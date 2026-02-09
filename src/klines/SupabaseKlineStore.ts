import type { SupabaseClient } from "@supabase/supabase-js";
import { Kline } from "./types";
import { KlineStore, SeriesKey } from "./store";

/**
 * Persists klines into Postgres.
 * Expected table: market_klines
 * Primary key: (exchange, symbol, interval, open_time)
 */
export class SupabaseKlineStore implements KlineStore {
  private supabase: SupabaseClient<any>;
  constructor(supabase: SupabaseClient<any>) {
    this.supabase = supabase;
  }

  async getLatestOpenTime(key: SeriesKey): Promise<number | null> {
    const { data, error } = await this.supabase
      .from("market_klines")
      .select("open_time")
      .eq("exchange", key.exchange)
      .eq("symbol", key.symbol)
      .eq("interval", key.interval)
      .order("open_time", { ascending: false })
      .limit(1);

    if (error) throw error;
    const row = data?.[0] as any;
    return row?.open_time ?? null;
  }

  async upsertMany(klines: Kline[]): Promise<void> {
    if (!klines.length) return;
    // Supabase will chunk large payloads; we also chunk to be safe.
    const chunkSize = 500;
    for (let i = 0; i < klines.length; i += chunkSize) {
      const chunk = klines.slice(i, i + chunkSize).map((k) => ({
        exchange: k.exchange,
        symbol: k.symbol,
        interval: k.interval,
        open_time: k.open_time,
        open: k.open,
        high: k.high,
        low: k.low,
        close: k.close,
        volume: k.volume,
        close_time: k.close_time,
      }));

      const { error } = await this.supabase
        .from("market_klines")
        .upsert(chunk, { onConflict: "exchange,symbol,interval,open_time" });

      if (error) throw error;
    }
  }

  async trimOld(key: SeriesKey, minOpenTime: number): Promise<void> {
    const { error } = await this.supabase
      .from("market_klines")
      .delete()
      .eq("exchange", key.exchange)
      .eq("symbol", key.symbol)
      .eq("interval", key.interval)
      .lt("open_time", minOpenTime);

    if (error) throw error;
  }
}
