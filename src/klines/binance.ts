import { Kline, KlineInterval } from "./types.js";

const BINANCE_BASE = "https://api.binance.com";

// Return NaN for invalid/non-finite values so callers can detect and skip bad data.
// Returning 0 previously created fake zero-price candles that poisoned indicator calculations.
function toNum(x: any): number {
  const n = Number(x);
  return Number.isFinite(n) ? n : Number.NaN;
}

const FETCH_TIMEOUT_MS = 15_000; // 15 s — enough for Binance but prevents indefinite hangs

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

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  let res: Response;
  try {
    res = await fetch(url.toString(), {
      signal: controller.signal,
      headers: {
        "accept": "application/json",
        "user-agent": "hornpub-runner/1.0",
      },
    });
  } catch (err: any) {
    if (err?.name === "AbortError") {
      throw new Error(`Binance klines request timed out after ${FETCH_TIMEOUT_MS}ms (${symbol} ${interval})`);
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }

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
    const open_time = toNum(row[0]);
    const open = toNum(row[1]);
    const high = toNum(row[2]);
    const low = toNum(row[3]);
    const close = toNum(row[4]);
    const volume = toNum(row[5]);
    const close_time = toNum(row[6]);
    // Skip any candle where price fields are not finite — a zero or NaN price would
    // corrupt downstream indicators (ATR, VWAP, etc.) with silent bad values.
    if (!Number.isFinite(open_time) || !Number.isFinite(open) || !Number.isFinite(high) ||
        !Number.isFinite(low) || !Number.isFinite(close)) continue;
    out.push({ exchange: "binance", symbol, interval, open_time, open, high, low, close, volume, close_time });
  }
  return out;
}
