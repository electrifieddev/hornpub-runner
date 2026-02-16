import type { SupabaseClient } from "@supabase/supabase-js";

export type CacheKey = string; // `${exchange}|${symbol}|${interval}`

export type KlineSeries = {
  exchange: string;
  symbol: string;
  interval: string;
  openTimes: number[];
  opens: number[];
  highs: number[];
  lows: number[];
  closes: number[];
  volumes: number[];
};

function keyOf(exchange: string, symbol: string, interval: string): CacheKey {
  return `${exchange}|${symbol}|${interval}`;
}

export class KlineCache {
  private supabase: SupabaseClient;
  private table: string;
  private maxCandles: number;
  private map = new Map<CacheKey, KlineSeries>();

  constructor(opts: { supabase: SupabaseClient; table?: string; maxCandles?: number }) {
    this.supabase = opts.supabase;
    this.table = opts.table ?? "market_klines";
    this.maxCandles = Math.max(50, opts.maxCandles ?? 5000);
  }

  getSeries(exchange: string, symbol: string, interval: string): KlineSeries | null {
    return this.map.get(keyOf(exchange, symbol, interval)) ?? null;
  }

  getCloses(exchange: string, symbol: string, interval: string): number[] {
    return this.getSeries(exchange, symbol, interval)?.closes ?? [];
  }

  /**
   * Preload candles into memory so indicator functions can be synchronous.
   * Loads up to `maxCandles` most recent candles, ordered oldest->newest.
   */
  async preload(
    exchange: string,
    symbol: string,
    interval: string,
    opts?: { maxCandles?: number }
  ): Promise<KlineSeries | null> {
    const key = keyOf(exchange, symbol, interval);

    // Allow callers to request a smaller in-memory series size.
    const limit = Math.max(50, Math.min(this.maxCandles, opts?.maxCandles ?? this.maxCandles));

    const { data, error } = await this.supabase
      .from(this.table)
      // Load full OHLCV so indicators can resolve multiple `source` series.
      .select("open_time, open, high, low, close, volume")
      .eq("exchange", exchange)
      .eq("symbol", symbol)
      .eq("interval", interval)
      .order("open_time", { ascending: false })
      .limit(limit);

    if (error) throw error;
    if (!data || data.length === 0) {
      this.map.set(key, { exchange, symbol, interval, openTimes: [], opens: [], highs: [], lows: [], closes: [], volumes: [] });
      return this.map.get(key)!;
    }

    // We fetched DESC, reverse to ASC for indicator math.
    const rows = [...data].reverse();
    const openTimes: number[] = new Array(rows.length);
    const opens: number[] = new Array(rows.length);
    const highs: number[] = new Array(rows.length);
    const lows: number[] = new Array(rows.length);
    const closes: number[] = new Array(rows.length);
    const volumes: number[] = new Array(rows.length);

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] as any;
      openTimes[i] = Number(r.open_time);
      opens[i] = Number(r.open);
      highs[i] = Number(r.high);
      lows[i] = Number(r.low);
      closes[i] = Number(r.close);
      volumes[i] = Number(r.volume);
    }

    const series: KlineSeries = { exchange, symbol, interval, openTimes, opens, highs, lows, closes, volumes };
    this.map.set(key, series);
    return series;
  }

  clear(): void {
    this.map.clear();
  }
}
