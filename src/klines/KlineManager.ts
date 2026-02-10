import { fetchBinanceKlines } from "./binance.js";
import { Kline, KlineInterval } from "./types.js";
import { KlineStore, SeriesKey } from "./store.js";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function uniq<T>(arr: T[]): T[] {
  return Array.from(new Set(arr));
}

export type ActiveSymbolsProvider = () => Promise<string[]>;

export type KlineManagerOpts = {
  exchange: "binance";
  interval: KlineInterval;
  historyDays: number; // e.g. 30
  pollEverySeconds: number; // e.g. 60
  maxConcurrency: number; // e.g. 3
  store: KlineStore;
  getActiveSymbols: ActiveSymbolsProvider;
  /** Legacy log hook (string only). */
  onLog?: (msg: string) => void;
  /** Preferred log hook (supports extra object). */
  logger?: (msg: string, extra?: any) => void;
};

/**
 * Global Kline Manager (Option B):
 * - Figures out the active symbol set from live projects
 * - Ensures a bounded window of historical klines per symbol
 * - On each tick, fetches only the missing tail since last stored candle
 * - Periodically trims old candles beyond historyDays
 */
export class KlineManager {
  private opts: KlineManagerOpts;
  private stopped = false;
  private inFlight = new Set<string>();
  private lastTrimAt = 0;

  constructor(opts: KlineManagerOpts) {
    this.opts = opts;
  }

  stop() {
    this.stopped = true;
  }

  async start() {
    this.log(`KlineManager starting (interval=${this.opts.interval}, historyDays=${this.opts.historyDays}, pollEvery=${this.opts.pollEverySeconds}s)`);
    while (!this.stopped) {
      try {
        const symbols = uniq((await this.opts.getActiveSymbols()).map((s) => s.trim().toUpperCase()).filter(Boolean));
        if (symbols.length) {
          await this.syncSymbols(symbols);
        }
      } catch (e) {
        this.log(`KlineManager tick error`, e);
      }
      await sleep(this.opts.pollEverySeconds * 1000);
    }
  }

  private log(msg: string, extra?: any) {
    if (this.opts.logger) return this.opts.logger(msg, extra);
    if (this.opts.onLog) return this.opts.onLog(extra ? `${msg} ${safeJson(extra)}` : msg);
    if (extra) console.log(`[KlineManager] ${msg}`, extra);
    else console.log(`[KlineManager] ${msg}`);
  }

  private keyStr(sym: string) {
    return `${this.opts.exchange}:${sym}:${this.opts.interval}`;
  }

  private async syncSymbols(symbols: string[]) {
    const { maxConcurrency } = this.opts;
    const queue = [...symbols];
    const workers: Promise<void>[] = [];

    for (let i = 0; i < maxConcurrency; i++) {
      workers.push(
        (async () => {
          while (queue.length && !this.stopped) {
            const sym = queue.shift();
            if (!sym) break;
            const key = this.keyStr(sym);
            if (this.inFlight.has(key)) continue;
            this.inFlight.add(key);
            try {
              await this.syncOne(sym);
            } finally {
              this.inFlight.delete(key);
            }
            // gentle pacing to avoid Binance bans
            await sleep(150);
          }
        })()
      );
    }

    await Promise.all(workers);

    // Trim at most once per hour to keep it cheap
    const now = Date.now();
    if (now - this.lastTrimAt > 60 * 60 * 1000) {
      this.lastTrimAt = now;
      for (const sym of symbols) {
        const key: SeriesKey = { exchange: this.opts.exchange, symbol: sym, interval: this.opts.interval };
        try {
          // Keep cache bounded: drop candles older than our history window
          await this.opts.store.trimOld(key, now - this.opts.historyDays * 24 * 60 * 60 * 1000);
        } catch (e) {
          this.log(`trim error for ${sym}`, e);
        }
      }
    }
  }

  private async syncOne(symbol: string) {
    const key: SeriesKey = { exchange: this.opts.exchange, symbol, interval: this.opts.interval };

    const latest = await this.opts.store.getLatestOpenTime(key);
    const now = Date.now();
    const historyMs = this.opts.historyDays * 24 * 60 * 60 * 1000;

    // If we have nothing, bootstrap last N days.
    if (!latest) {
      this.log(`bootstrap ${symbol} (${this.opts.historyDays}d)`);
      await this.bootstrapHistory(symbol, now - historyMs, now);
      return;
    }

    // Fetch only the tail since latest candle (plus 1 interval).
    // Binance returns candles with open_time >= startTime.
    const startTime = latest + this.intervalMs(this.opts.interval);
    if (startTime > now - this.intervalMs(this.opts.interval)) {
      // We are up to date.
      return;
    }

    const fetched = await this.fetchPaged(symbol, startTime, now);
    if (!fetched.length) return;

    await this.opts.store.upsertMany(fetched);
    this.log(`synced ${symbol}: +${fetched.length} klines`);
  }

  private async bootstrapHistory(symbol: string, startTime: number, endTime: number) {
    const fetched = await this.fetchPaged(symbol, startTime, endTime);
    if (!fetched.length) {
      this.log(`bootstrap empty for ${symbol}`);
      return;
    }
    await this.opts.store.upsertMany(fetched);
    this.log(`bootstrapped ${symbol}: ${fetched.length} klines`);
  }

  private async fetchPaged(symbol: string, startTime: number, endTime: number): Promise<Kline[]> {
    // Binance max 1000 candles per request.
    const out: Kline[] = [];
    let cursor = startTime;
    const maxLoops = 1000; // safety
    for (let i = 0; i < maxLoops; i++) {
      if (cursor > endTime) break;
      const chunk = await fetchBinanceKlines({
        symbol,
        interval: this.opts.interval,
        startTime: cursor,
        endTime,
        limit: 1000,
      });
      if (!chunk.length) break;

      out.push(...chunk);

      // Move cursor to next candle open
      const lastOpen = chunk[chunk.length - 1].open_time;
      const next = lastOpen + this.intervalMs(this.opts.interval);
      if (next <= cursor) break;
      cursor = next;

      // If Binance returns less than limit, we likely caught up.
      if (chunk.length < 1000) break;

      // small delay
      await sleep(120);
    }
    return out;
  }

  private intervalMs(interval: KlineInterval): number {
    const m = (n: number) => n * 60 * 1000;
    const h = (n: number) => n * 60 * 60 * 1000;
    const d = (n: number) => n * 24 * 60 * 60 * 1000;

    switch (interval) {
      case "1m":
        return m(1);
      case "3m":
        return m(3);
      case "5m":
        return m(5);
      case "15m":
        return m(15);
      case "30m":
        return m(30);
      case "1h":
        return h(1);
      case "2h":
        return h(2);
      case "4h":
        return h(4);
      case "6h":
        return h(6);
      case "8h":
        return h(8);
      case "12h":
        return h(12);
      case "1d":
        return d(1);
      default:
        return m(1);
    }
  }
}
