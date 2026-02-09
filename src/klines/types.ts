export type Exchange = "binance";

export type KlineInterval =
  | "1m"
  | "3m"
  | "5m"
  | "15m"
  | "30m"
  | "1h"
  | "2h"
  | "4h"
  | "6h"
  | "8h"
  | "12h"
  | "1d";

export type Kline = {
  exchange: Exchange;
  symbol: string; // e.g. BTCUSDT
  interval: KlineInterval;

  open_time: number; // ms epoch
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  close_time: number; // ms epoch
};
