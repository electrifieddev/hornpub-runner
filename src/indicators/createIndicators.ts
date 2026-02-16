import type { KlineCache } from "../klines/KlineCache.js";
import type {
  EmaCrossParams,
  IndicatorContext,
  MacdCrossParams,
  RSIParams,
  SmaCrossParams,
} from "./types.js";
import {
  atr as atrCalc,
  bbandsLatest,
  crossDown,
  crossUp,
  ema,
  macdLatest,
  rsi as rsiCalc,
  sma,
  wma,
} from "./math.js";

function getCloses(cache: KlineCache, ctx: IndicatorContext, tf: string): number[] {
  const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
  return series?.closes ?? [];
}

type SourceName =
  | "Close"
  | "Open"
  | "High"
  | "Low"
  | "Volume"
  | "HL2"
  | "HLC3"
  | "Typical Price"
  | "OHLC4";

function normalizeSource(raw: unknown): SourceName {
  const s = String(raw ?? "Close").trim();
  // Accept a few UI aliases.
  if (s.toLowerCase() === "typical price") return "Typical Price";
  if (s.toLowerCase() === "hlc3") return "HLC3";
  return (s as SourceName) ?? "Close";
}

function resolveSourceSeries(cache: KlineCache, ctx: IndicatorContext, tf: string, sourceRaw: unknown): number[] {
  const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
  if (!series) return [];
  const source = normalizeSource(sourceRaw);
  switch (source) {
    case "Close":
      return series.closes;
    case "Open":
      return series.opens;
    case "High":
      return series.highs;
    case "Low":
      return series.lows;
    case "Volume":
      return series.volumes;
    case "HL2":
      return series.highs.map((h, i) => (h + series.lows[i]!) / 2);
    case "HLC3":
    case "Typical Price":
      return series.highs.map((h, i) => (h + series.lows[i]! + series.closes[i]!) / 3);
    case "OHLC4":
      return series.opens.map((o, i) => (o + series.highs[i]! + series.lows[i]! + series.closes[i]!) / 4);
    default:
      return series.closes;
  }
}

function lastFinite(series: number[]): number {
  for (let i = series.length - 1; i >= 0; i--) {
    const v = series[i];
    if (Number.isFinite(v)) return v;
  }
  return Number.NaN;
}

// Accept both call styles to keep integration flexible:
//   createIndicators(cache, ctx)
//   createIndicators({ cache, ctx })
export function createIndicators(cache: KlineCache, ctx: IndicatorContext): ReturnType<typeof _createIndicators>;
export function createIndicators(opts: { cache: KlineCache; ctx: IndicatorContext }): ReturnType<typeof _createIndicators>;
export function createIndicators(arg1: KlineCache | { cache: KlineCache; ctx: IndicatorContext }, arg2?: IndicatorContext) {
  const cache = (arg1 as any)?.cache ? (arg1 as any).cache : (arg1 as KlineCache);
  const ctx = (arg1 as any)?.ctx ? (arg1 as any).ctx : (arg2 as IndicatorContext);
  return _createIndicators(cache, ctx);
}

function _createIndicators(cache: KlineCache, ctx: IndicatorContext) {
  // Per-run/per-symbol cache for computed indicator arrays.
  const seriesCache = new Map<string, number[]>();
  const indCache = new Map<string, number[]>();
  const numCache = new Map<string, number>();
  const objCache = new Map<string, any>();
  const warnOnce = new Set<string>();

  function getSource(tf: string, source: unknown): number[] {
    const key = `${tf}|src|${String(source ?? "Close")}`;
    const hit = seriesCache.get(key);
    if (hit) return hit;
    const v = resolveSourceSeries(cache, ctx, tf, source);
    seriesCache.set(key, v);
    return v;
  }

  function cachedArray(key: string, compute: () => number[]): number[] {
    const hit = indCache.get(key);
    if (hit) return hit;
    const v = compute();
    indCache.set(key, v);
    return v;
  }

  return {
    // === New Blockly runtime surface (Checkpoint 1) ===
    EMA: (p: { tf: string; source?: unknown; length: number }): number => {
      const values = getSource(p.tf, p.source ?? "Close");
      const n = Math.max(1, Math.floor(Number(p.length ?? 0)));
      if (values.length < n) return Number.NaN;
      const key = `${p.tf}|EMA|${String(p.source ?? "Close")}|${n}`;
      const arr = cachedArray(key, () => ema(values, n));
      return lastFinite(arr);
    },

    SMA: (p: { tf: string; source?: unknown; length: number }): number => {
      const values = getSource(p.tf, p.source ?? "Close");
      const n = Math.max(1, Math.floor(Number(p.length ?? 0)));
      if (values.length < n) return Number.NaN;
      const key = `${p.tf}|SMA|${String(p.source ?? "Close")}|${n}`;
      const arr = cachedArray(key, () => sma(values, n));
      return lastFinite(arr);
    },

    WMA: (p: { tf: string; source?: unknown; length: number }): number => {
      const values = getSource(p.tf, p.source ?? "Close");
      const n = Math.max(1, Math.floor(Number(p.length ?? 0)));
      if (values.length < n) return Number.NaN;
      const key = `${p.tf}|WMA|${String(p.source ?? "Close")}|${n}`;
      const arr = cachedArray(key, () => wma(values, n));
      return lastFinite(arr);
    },

    // === New Blockly runtime surface (Checkpoint 2) ===
    RSI: (p: RSIParams): number => {
      const period = Math.max(1, Math.floor(Number((p as any).period ?? 0)));
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const smoothingRaw = (p as any).smoothing ?? "Wilder";
      const smoothing = String(smoothingRaw).toLowerCase();
      if (smoothing && smoothing !== "wilder" && smoothing !== "rma" && smoothing !== "wilders") {
        const k = `rsi_smoothing_${smoothing}`;
        if (!warnOnce.has(k)) {
          warnOnce.add(k);
          // eslint-disable-next-line no-console
          console.warn(`[indicators] RSI smoothing '${String(smoothingRaw)}' not supported yet; falling back to Wilder.`);
        }
      }

      const key = `${tf}|RSI|${String(source)}|${period}`;
      const hit = numCache.get(key);
      if (hit !== undefined) return hit;

      const values = getSource(tf, source);
      const v = rsiCalc(values, period);
      numCache.set(key, v);
      return v;
    },

    ATR: (p: { tf: string; period: number }): number => {
      const tf = String((p as any).tf ?? "1m");
      const period = Math.max(1, Math.floor(Number((p as any).period ?? 0)));
      const key = `${tf}|ATR|${period}`;
      const hit = numCache.get(key);
      if (hit !== undefined) return hit;

      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series) return Number.NaN;
      const v = atrCalc(series.highs, series.lows, series.closes, period);
      numCache.set(key, v);
      return v;
    },

    // === New Blockly runtime surface (Checkpoint 3) ===
    MACD: (p: { tf: string; source?: unknown; fast: number; slow: number; signal: number }): { macd: number; signal: number; histogram: number } => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const fast = Math.max(1, Math.floor(Number((p as any).fast ?? 12)));
      const slow = Math.max(1, Math.floor(Number((p as any).slow ?? 26)));
      const signal = Math.max(1, Math.floor(Number((p as any).signal ?? 9)));
      const key = `${tf}|MACD|${String(source)}|${fast}|${slow}|${signal}`;
      const hit = objCache.get(key);
      if (hit) return hit;
      const values = getSource(tf, source);
      if (values.length < Math.max(fast, slow) + signal) {
        const out = { macd: Number.NaN, signal: Number.NaN, histogram: Number.NaN };
        objCache.set(key, out);
        return out;
      }
      const out = macdLatest(values, fast, slow, signal);
      objCache.set(key, out);
      return out;
    },

    BBANDS: (p: { tf: string; source?: unknown; length: number; mult: number }): { upper: number; middle: number; lower: number } => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const length = Math.max(1, Math.floor(Number((p as any).length ?? 20)));
      const mult = Number((p as any).mult ?? 2);
      const key = `${tf}|BBANDS|${String(source)}|${length}|${mult}`;
      const hit = objCache.get(key);
      if (hit) return hit;
      const values = getSource(tf, source);
      const out = bbandsLatest(values, length, mult);
      objCache.set(key, out);
      return out;
    },

    VWAP: (p: { tf: string; source?: unknown }): number => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Typical Price";
      const key = `${tf}|VWAP|${String(source)}`;
      const hit = numCache.get(key);
      if (hit !== undefined) return hit;

      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series) return Number.NaN;
      const prices = getSource(tf, source);
      const vols = series.volumes;
      const len = Math.min(prices.length, vols.length);
      if (len === 0) return Number.NaN;
      let pv = 0;
      let vSum = 0;
      for (let i = 0; i < len; i++) {
        const price = prices[i]!;
        const vol = vols[i]!;
        if (!Number.isFinite(price) || !Number.isFinite(vol)) continue;
        pv += price * vol;
        vSum += vol;
      }
      const out = vSum === 0 ? Number.NaN : pv / vSum;
      numCache.set(key, out);
      return out;
    },

    // === New Blockly runtime surface (Checkpoint 4: breakout) ===
    BREAKOUT_UP: (p: { tf: string; source?: unknown; lookback: number; level?: number }): boolean => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const lookback = Math.max(1, Math.floor(Number((p as any).lookback ?? 0)));
      const levelRaw = (p as any).level;
      const level = levelRaw === undefined || levelRaw === null ? null : Number(levelRaw);

      const values = getSource(tf, source);
      if (values.length < 2) return false;
      const curr = values[values.length - 1]!;
      if (!Number.isFinite(curr)) return false;

      // If a level is provided, treat it as an explicit threshold.
      if (level !== null && Number.isFinite(level)) return curr > level;

      // Otherwise compare against prior `lookback` bars (exclude current bar).
      const end = values.length - 1;
      const start = Math.max(0, end - lookback);
      if (end - start < 1) return false;
      let mx = -Infinity;
      for (let i = start; i < end; i++) {
        const v = values[i]!;
        if (Number.isFinite(v) && v > mx) mx = v;
      }
      return Number.isFinite(mx) ? curr > mx : false;
    },

    BREAKOUT_DOWN: (p: { tf: string; source?: unknown; lookback: number; level?: number }): boolean => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const lookback = Math.max(1, Math.floor(Number((p as any).lookback ?? 0)));
      const levelRaw = (p as any).level;
      const level = levelRaw === undefined || levelRaw === null ? null : Number(levelRaw);

      const values = getSource(tf, source);
      if (values.length < 2) return false;
      const curr = values[values.length - 1]!;
      if (!Number.isFinite(curr)) return false;

      if (level !== null && Number.isFinite(level)) return curr < level;

      const end = values.length - 1;
      const start = Math.max(0, end - lookback);
      if (end - start < 1) return false;
      let mn = Infinity;
      for (let i = start; i < end; i++) {
        const v = values[i]!;
        if (Number.isFinite(v) && v < mn) mn = v;
      }
      return Number.isFinite(mn) ? curr < mn : false;
    },

    EMA_CROSS_UP: (p: EmaCrossParams): boolean => {
      const closes = getCloses(cache, ctx, p.tf);
      const fast = ema(closes, p.fast);
      const slow = ema(closes, p.slow);
      return crossUp(fast, slow);
    },

    SMA_CROSS_UP: (p: SmaCrossParams): boolean => {
      const closes = getCloses(cache, ctx, p.tf);
      const fast = sma(closes, p.fast);
      const slow = sma(closes, p.slow);
      return crossUp(fast, slow);
    },

    MACD_CROSS_UP: (p: MacdCrossParams): boolean => {
      const closes = getCloses(cache, ctx, p.tf);
      const fast = ema(closes, p.fast);
      const slow = ema(closes, p.slow);
      if (fast.length === 0 || slow.length === 0) return false;
      const macdLine: number[] = [];
      const len = Math.min(fast.length, slow.length);
      for (let i = 0; i < len; i++) macdLine.push(fast[i]! - slow[i]!);
      const signal = ema(macdLine, p.signal);
      return crossUp(macdLine, signal);
    },

    // Legacy helper retained for compatibility (not used by new Blockly blocks)
    EMA_CROSS_DOWN: (p: EmaCrossParams): boolean => {
      const closes = getCloses(cache, ctx, p.tf);
      const fast = ema(closes, p.fast);
      const slow = ema(closes, p.slow);
      return crossDown(fast, slow);
    },
  };
}

export default createIndicators;
