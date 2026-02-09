import { Exchange, Kline, KlineInterval } from "./types.js";

export type SeriesKey = { exchange: Exchange; symbol: string; interval: KlineInterval };

export interface KlineStore {
  getLatestOpenTime(key: SeriesKey): Promise<number | null>;
  upsertMany(klines: Kline[]): Promise<void>;
  trimOld(key: SeriesKey, minOpenTime: number): Promise<void>;
}
