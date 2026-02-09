import { Kline, KlineInterval } from "./types";

const BINANCE_BASE = "https://api.binance.com";

function toNum(x: any): number {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

/**
 * Binance Klines API:
 * GET /api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=...&endTime=...&limit=1000
 */
export async function fetchBinanceKlines(args: {
  symbol: string;
  interval: KlineInterval;
  startTime?: number; // ms
  endTime?: number; // ms
  limit?: number; // 1..1000
}): Promise<Kline[]> {
  const { symbol, interval } = args;
  const limit = Math.min(Math.max(args.limit ?? 1000, 1), 1000);

  const url = new URL("/api/v3/klines", BINANCE_BASE);
  url.searchParams.set("symbol", symbol);
  url.searchParams.set("interval", interval);
  url.searchParams.set("limit", String(limit));
  if (args.startTime != null) url.searchParams.set("startTime", String(args.startTime));
  if (args.endTime != null) url.searchParams.set("endTime", String(args.endTime));

  const res = await fetch(url.toString(), {
    headers: {
      "accept": "application/json",
      "user-agent": "hornpub-runner/1.0",
    },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Binance klines HTTP ${res.status}: ${body.slice(0, 200)}`);
  }

  const raw = (await res.json()) as any[];
  // Each entry:
  // [ openTime, open, high, low, close, volume, closeTime, quoteAssetVolume, numberOfTrades, ...]
  const out: Kline[] = [];
  for (const row of raw) {
    if (!Array.isArray(row) || row.length < 7) continue;
    out.push({
      exchange: "binance",
      symbol,
      interval,
      open_time: toNum(row[0]),
      open: toNum(row[1]),
      high: toNum(row[2]),
      low: toNum(row[3]),
      close: toNum(row[4]),
      volume: toNum(row[5]),
      close_time: toNum(row[6]),
    });
  }
  return out;
}
