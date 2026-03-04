/**
 * InMemoryKlineManager
 *
 * Replaces KlineManager + SupabaseKlineStore entirely.
 * Fetches klines directly from Binance and writes them into the shared
 * KlineCache (in-process memory). No database involved.
 *
 * - On first sight of a symbol+interval: bootstraps full history (historyDays).
 * - On subsequent polls: fetches only the tail since the last known candle.
 * - All projects share the same KlineCache instance — one fetch serves everyone.
 * - Invalid/failing symbols are backed off exponentially (max 6h) so one bad
 *   symbol can't stall the whole loop.
 */

import { KlineCache }         from "./KlineCache.js";
import { fetchBinanceKlines } from "./binance.js";
import { KlineInterval }      from "./types.js";

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function uniq<T>(arr: T[]): T[] {
  return [...new Set(arr)];
}

export type GetActiveSymbolsAndIntervals = () => Promise<{ symbols: string[]; intervals: string[] }>;

export type InMemoryKlineManagerOpts = {
  cache:            KlineCache;
  exchange:         "binance";
  historyDays:      number;
  pollEverySeconds: number;
  maxConcurrency:   number;
  getActive:        GetActiveSymbolsAndIntervals;
  logger?:          (msg: string, extra?: any) => void;
};

type SeriesState = {
  latestOpenTime: number | null;  // null = not bootstrapped yet
};

const INTERVAL_MS: Record<KlineInterval, number> = {
  "1m":  60_000,
  "3m":  3  * 60_000,
  "5m":  5  * 60_000,
  "15m": 15 * 60_000,
  "30m": 30 * 60_000,
  "1h":  60 * 60_000,
  "2h":  2  * 60 * 60_000,
  "4h":  4  * 60 * 60_000,
  "6h":  6  * 60 * 60_000,
  "8h":  8  * 60 * 60_000,
  "12h": 12 * 60 * 60_000,
  "1d":  24 * 60 * 60_000,
};

export class InMemoryKlineManager {
  private opts:    InMemoryKlineManagerOpts;
  private stopped = false;

  // key: `${symbol}|${interval}`
  private state        = new Map<string, SeriesState>();
  private invalidUntil = new Map<string, number>();
  private invalidBackoffMs = new Map<string, number>();
  private inFlight     = new Set<string>();

  constructor(opts: InMemoryKlineManagerOpts) {
    this.opts = opts;
  }

  stop() { this.stopped = true; }

  async start(): Promise<void> {
    this.log(`InMemoryKlineManager starting (historyDays=${this.opts.historyDays}, pollEvery=${this.opts.pollEverySeconds}s)`);

    while (!this.stopped) {
      try {
        const { symbols, intervals } = await this.opts.getActive();

        // Always include 1m so the broker always has a fresh mark price
        const allIntervals = uniq([...intervals, "1m"]) as KlineInterval[];
        const allSymbols   = uniq(symbols.map((s) => s.trim().toUpperCase()).filter(Boolean));

        if (allSymbols.length && allIntervals.length) {
          await this.syncAll(allSymbols, allIntervals);
        }
      } catch (e: any) {
        this.log(`tick error`, e);
      }

      await sleep(this.opts.pollEverySeconds * 1000);
    }
  }

  private async syncAll(symbols: string[], intervals: KlineInterval[]): Promise<void> {
    const now = Date.now();

    // Build work queue: all symbol × interval pairs, skip backed-off ones
    const queue: Array<{ symbol: string; interval: KlineInterval }> = [];
    for (const symbol of symbols) {
      for (const interval of intervals) {
        const key   = `${symbol}|${interval}`;
        const until = this.invalidUntil.get(key);
        if (until && until > now) continue;
        if (!this.isValidSymbol(symbol)) continue;
        queue.push({ symbol, interval });
      }
    }

    // Process with limited concurrency
    const { maxConcurrency } = this.opts;
    const workers: Promise<void>[] = [];

    for (let i = 0; i < maxConcurrency; i++) {
      workers.push((async () => {
        while (queue.length && !this.stopped) {
          const item = queue.shift();
          if (!item) break;
          const key = `${item.symbol}|${item.interval}`;
          if (this.inFlight.has(key)) continue;
          this.inFlight.add(key);
          try {
            await this.syncOne(item.symbol, item.interval);
          } finally {
            this.inFlight.delete(key);
          }
          await sleep(150); // gentle pacing to avoid Binance rate limits
        }
      })());
    }

    await Promise.all(workers);
  }

  private async syncOne(symbol: string, interval: KlineInterval): Promise<void> {
    const key   = `${symbol}|${interval}`;
    const state = this.state.get(key);
    const now   = Date.now();

    try {
      if (!state || state.latestOpenTime === null) {
        // First time — bootstrap full history
        this.log(`bootstrap ${symbol} ${interval} (${this.opts.historyDays}d)`);
        const historyMs  = this.opts.historyDays * 24 * 60 * 60 * 1000;
        const klines     = await this.fetchPaged(symbol, interval, now - historyMs, now);
        if (klines.length) {
          this.opts.cache.upsert(klines);
          this.state.set(key, { latestOpenTime: klines[klines.length - 1]!.open_time });
          this.log(`bootstrapped ${symbol} ${interval}: ${klines.length} candles`);
        } else {
          this.state.set(key, { latestOpenTime: null });
          this.log(`bootstrap empty for ${symbol} ${interval}`);
        }
      } else {
        // Incremental — only fetch candles since last known
        const iMs      = INTERVAL_MS[interval] ?? 60_000;
        const startTime = state.latestOpenTime + iMs;

        // Nothing new yet
        if (startTime > now - iMs) return;

        const klines = await this.fetchPaged(symbol, interval, startTime, now);
        if (klines.length) {
          this.opts.cache.upsert(klines);
          this.state.set(key, { latestOpenTime: klines[klines.length - 1]!.open_time });
          this.log(`synced ${symbol} ${interval}: +${klines.length} candles`);
        }
      }

      // Clear any backoff on success
      this.invalidUntil.delete(key);
      this.invalidBackoffMs.delete(key);

    } catch (err: any) {
      const prev = this.invalidBackoffMs.get(key) ?? 5 * 60_000;
      const next = Math.min(prev * 2, 6 * 60 * 60_000); // cap at 6h
      this.invalidBackoffMs.set(key, next);
      this.invalidUntil.set(key, Date.now() + next);
      const msg = err?.message ?? String(err);
      this.log(`skip ${symbol} ${interval}: ${msg} (retry in ${Math.round(next / 60_000)}m)`);
    }
  }

  private async fetchPaged(
    symbol:    string,
    interval:  KlineInterval,
    startTime: number,
    endTime:   number,
  ) {
    const out: Awaited<ReturnType<typeof fetchBinanceKlines>> = [];
    let cursor = startTime;
    const iMs  = INTERVAL_MS[interval] ?? 60_000;

    for (let i = 0; i < 1000 && cursor <= endTime && !this.stopped; i++) {
      const chunk = await fetchBinanceKlines({ symbol, interval, startTime: cursor, endTime, limit: 1000 });
      if (!chunk.length) break;
      out.push(...chunk);
      const lastOpen = chunk[chunk.length - 1]!.open_time;
      const next = lastOpen + iMs;
      if (next <= cursor) break;
      cursor = next;
      if (chunk.length < 1000) break;
      await sleep(120);
    }

    return out;
  }

  private isValidSymbol(s: string): boolean {
    return /^[A-Z0-9]{3,30}$/.test(s);
  }

  private log(msg: string, extra?: any): void {
    if (this.opts.logger) {
      this.opts.logger(msg, extra);
    } else if (extra !== undefined) {
      console.log(`[KLINES] ${msg}`, extra);
    } else {
      console.log(`[KLINES] ${msg}`);
    }
  }
}
