import { Kline } from "./types.js";

export type CacheKey = string; // `${exchange}|${symbol}|${interval}`

export type KlineSeries = {
  exchange: string;
  symbol:   string;
  interval: string;
  openTimes: number[];
  opens:     number[];
  highs:     number[];
  lows:      number[];
  closes:    number[];
  volumes:   number[];
};

function keyOf(exchange: string, symbol: string, interval: string): CacheKey {
  return `${exchange}|${symbol}|${interval}`;
}

export class KlineCache {
  private maxCandles: number;
  private map = new Map<CacheKey, KlineSeries>();

  constructor(opts: { maxCandles?: number } = {}) {
    this.maxCandles = Math.max(50, opts.maxCandles ?? 5000);
  }

  getSeries(exchange: string, symbol: string, interval: string): KlineSeries | null {
    return this.map.get(keyOf(exchange, symbol, interval)) ?? null;
  }

  getCloses(exchange: string, symbol: string, interval: string): number[] {
    return this.getSeries(exchange, symbol, interval)?.closes ?? [];
  }

  /**
   * Write klines directly into the in-memory cache.
   * New candles are appended; existing ones (same open_time) are updated in-place.
   * Series is kept trimmed to maxCandles.
   */
  upsert(klines: Kline[]): void {
    if (!klines.length) return;

    const groups = new Map<CacheKey, Kline[]>();
    for (const k of klines) {
      const key = keyOf(k.exchange, k.symbol, k.interval);
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(k);
    }

    for (const [key, incoming] of groups) {
      incoming.sort((a, b) => a.open_time - b.open_time);

      const existing = this.map.get(key);

      if (!existing) {
        const trimmed = incoming.slice(-this.maxCandles);
        this.map.set(key, {
          exchange:  incoming[0].exchange,
          symbol:    incoming[0].symbol,
          interval:  incoming[0].interval,
          openTimes: trimmed.map((k) => k.open_time),
          opens:     trimmed.map((k) => k.open),
          highs:     trimmed.map((k) => k.high),
          lows:      trimmed.map((k) => k.low),
          closes:    trimmed.map((k) => k.close),
          volumes:   trimmed.map((k) => k.volume),
        });
        continue;
      }

      // Build index for fast lookup
      const timeIndex = new Map<number, number>();
      for (let i = 0; i < existing.openTimes.length; i++) {
        timeIndex.set(existing.openTimes[i]!, i);
      }

      const toAppend: Kline[] = [];
      for (const k of incoming) {
        const idx = timeIndex.get(k.open_time);
        if (idx !== undefined) {
          // Update in-place (last candle may still be forming)
          existing.opens[idx]   = k.open;
          existing.highs[idx]   = k.high;
          existing.lows[idx]    = k.low;
          existing.closes[idx]  = k.close;
          existing.volumes[idx] = k.volume;
        } else {
          toAppend.push(k);
        }
      }

      if (toAppend.length) {
        existing.openTimes.push(...toAppend.map((k) => k.open_time));
        existing.opens.push(...toAppend.map((k) => k.open));
        existing.highs.push(...toAppend.map((k) => k.high));
        existing.lows.push(...toAppend.map((k) => k.low));
        existing.closes.push(...toAppend.map((k) => k.close));
        existing.volumes.push(...toAppend.map((k) => k.volume));
      }

      // Trim oldest candles if over limit
      if (existing.openTimes.length > this.maxCandles) {
        const excess = existing.openTimes.length - this.maxCandles;
        existing.openTimes.splice(0, excess);
        existing.opens.splice(0, excess);
        existing.highs.splice(0, excess);
        existing.lows.splice(0, excess);
        existing.closes.splice(0, excess);
        existing.volumes.splice(0, excess);
      }
    }
  }

  /** True if series exists and has enough candles to run indicators. */
  isReady(exchange: string, symbol: string, interval: string, minCandles = 2): boolean {
    const s = this.getSeries(exchange, symbol, interval);
    return s !== null && s.openTimes.length >= minCandles;
  }

  /** Age in ms of the latest candle open_time, or Infinity if no data. */
  ageMs(exchange: string, symbol: string, interval: string): number {
    const s = this.getSeries(exchange, symbol, interval);
    if (!s || s.openTimes.length === 0) return Infinity;
    return Date.now() - s.openTimes[s.openTimes.length - 1]!;
  }

  clear(): void {
    this.map.clear();
  }
}
