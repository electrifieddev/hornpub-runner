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

    ATR: (p: { tf: string; period?: number; length?: number; smoothing?: string }): number => {
      const tf = String((p as any).tf ?? "1m");
      // Accept both `period` (backend convention) and `length` (Blockly generator emits this)
      const period = Math.max(1, Math.floor(Number((p as any).period ?? (p as any).length ?? 0)));
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

    // === B-18: Missing indicators used by Blockly generators ===

    /** Returns the latest value of any price source (Close, Open, High, Low, HL2, etc.) */
    PRICE: (p: { tf: string; source?: unknown }): number => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const values = getSource(tf, source);
      return lastFinite(values);
    },

    /** Returns the latest volume bar */
    VOLUME: (p: { tf: string }): number => {
      const tf = String((p as any).tf ?? "1m");
      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series) return Number.NaN;
      return lastFinite(series.volumes);
    },

    /**
     * Bollinger Bands — alias for BBANDS but accepts `std` instead of `mult` so
     * the Blockly generator emission  `BOLLINGER({ tf, source, length, std })`
     * maps correctly.
     */
    BOLLINGER: (p: { tf: string; source?: unknown; length: number; std?: number; mult?: number }): { upper: number; middle: number; lower: number } => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const length = Math.max(1, Math.floor(Number((p as any).length ?? 20)));
      // Accept both `std` (Blockly) and `mult` (internal) for multiplier
      const mult = Number((p as any).std ?? (p as any).mult ?? 2);
      const key = `${tf}|BOLLINGER|${String(source)}|${length}|${mult}`;
      const hit = objCache.get(key);
      if (hit) return hit;
      const values = getSource(tf, source);
      const out = bbandsLatest(values, length, mult);
      objCache.set(key, out);
      return out;
    },

    /**
     * Volatility regime: returns "high" | "low" | "normal" based on ATR vs its own SMA.
     * mode: "high" → current ATR > mult × atrSMA; "low" → current ATR < (1/mult) × atrSMA
     */
    VOLATILITY_REGIME: (p: { tf: string; atrLength?: number; mult?: number; mode?: string }): boolean => {
      const tf = String((p as any).tf ?? "1m");
      const atrLength = Math.max(1, Math.floor(Number((p as any).atrLength ?? 14)));
      const mult = Number((p as any).mult ?? 1.5);
      const mode = String((p as any).mode ?? "high").toLowerCase();

      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series) return false;

      const currentAtr = atrCalc(series.highs, series.lows, series.closes, atrLength);
      if (!Number.isFinite(currentAtr)) return false;

      // Build an ATR series to compute its SMA.  We need at minimum 2*atrLength bars.
      const len = Math.min(series.highs.length, series.lows.length, series.closes.length);
      if (len < atrLength * 2 + 1) return false;

      // Rolling ATR approximation: compute ATR over trailing windows for SMA.
      const atrSeries: number[] = [];
      const step = Math.max(1, Math.floor(atrLength / 2));
      for (let end = atrLength * 2; end <= len; end += step) {
        const h = series.highs.slice(end - atrLength * 2, end);
        const l = series.lows.slice(end - atrLength * 2, end);
        const c = series.closes.slice(end - atrLength * 2, end);
        const v = atrCalc(h, l, c, atrLength);
        if (Number.isFinite(v)) atrSeries.push(v);
      }

      if (atrSeries.length === 0) return false;
      const atrSmaArr = sma(atrSeries, Math.min(14, atrSeries.length));
      const atrSma = lastFinite(atrSmaArr);
      if (!Number.isFinite(atrSma)) return false;

      if (mode === "high") return currentAtr > mult * atrSma;
      if (mode === "low") return currentAtr < (1 / mult) * atrSma;
      return false;
    },

    /** Highest value of a source series over the last `length` bars (inclusive of current) */
    HIGHEST: (p: { tf: string; source?: unknown; length: number }): number => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const length = Math.max(1, Math.floor(Number((p as any).length ?? 1)));
      const values = getSource(tf, source);
      if (values.length === 0) return Number.NaN;
      const start = Math.max(0, values.length - length);
      let mx = -Infinity;
      for (let i = start; i < values.length; i++) {
        const v = values[i]!;
        if (Number.isFinite(v) && v > mx) mx = v;
      }
      return Number.isFinite(mx) ? mx : Number.NaN;
    },

    /** Lowest value of a source series over the last `length` bars (inclusive of current) */
    LOWEST: (p: { tf: string; source?: unknown; length: number }): number => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const length = Math.max(1, Math.floor(Number((p as any).length ?? 1)));
      const values = getSource(tf, source);
      if (values.length === 0) return Number.NaN;
      const start = Math.max(0, values.length - length);
      let mn = Infinity;
      for (let i = start; i < values.length; i++) {
        const v = values[i]!;
        if (Number.isFinite(v) && v < mn) mn = v;
      }
      return Number.isFinite(mn) ? mn : Number.NaN;
    },

    /** Highest High over the last `length` bars */
    HIGHEST_HIGH: (p: { tf: string; length: number }): number => {
      const tf = String((p as any).tf ?? "1m");
      const length = Math.max(1, Math.floor(Number((p as any).length ?? 1)));
      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series) return Number.NaN;
      const start = Math.max(0, series.highs.length - length);
      let mx = -Infinity;
      for (let i = start; i < series.highs.length; i++) {
        const v = series.highs[i]!;
        if (Number.isFinite(v) && v > mx) mx = v;
      }
      return Number.isFinite(mx) ? mx : Number.NaN;
    },

    /** Lowest Low over the last `length` bars */
    LOWEST_LOW: (p: { tf: string; length: number }): number => {
      const tf = String((p as any).tf ?? "1m");
      const length = Math.max(1, Math.floor(Number((p as any).length ?? 1)));
      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series) return Number.NaN;
      const start = Math.max(0, series.lows.length - length);
      let mn = Infinity;
      for (let i = start; i < series.lows.length; i++) {
        const v = series.lows[i]!;
        if (Number.isFinite(v) && v < mn) mn = v;
      }
      return Number.isFinite(mn) ? mn : Number.NaN;
    },

    /**
     * Returns the value of `series` `bars` bars ago.
     * `series` should be a number produced by any indicator call — but since the
     * sandbox surface exposes scalar values (not arrays), PREV stores the rolling
     * history itself using a per-key seriesCache.
     *
     * Usage:  PREV({ series: EMA({...}), bars: 1 })
     * Note:   The value accumulates across ticks because seriesCache is rebuilt
     *         fresh each tick.  For genuine multi-tick memory, strategies should
     *         rely on CROSS_UP / CROSS_DOWN.  PREV is most useful for confirming
     *         the prior bar's value within a single tick.
     */
    PREV: (p: { series: number; bars?: number }): number => {
      const bars = Math.max(1, Math.floor(Number((p as any).bars ?? 1)));
      const currVal = Number((p as any).series);
      // Build a rolling buffer under a deterministic key (call-site ordering).
      const prevCallKey = `prev_call_counter`;
      const callCount = (numCache.get(prevCallKey) ?? 0) + 1;
      numCache.set(prevCallKey, callCount);
      const bufKey = `PREV_buf_${callCount}`;

      let buf = seriesCache.get(bufKey);
      if (!buf) {
        buf = [];
        seriesCache.set(bufKey, buf);
      }
      // Append current value.
      if (Number.isFinite(currVal)) buf.push(currVal);

      // Return the value `bars` positions from the end (before the current append).
      const idx = buf.length - 1 - bars;
      if (idx < 0) return Number.NaN;
      return buf[idx]!;
    },


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

    SMA_CROSS_DOWN: (p: SmaCrossParams): boolean => {
      const closes = getCloses(cache, ctx, p.tf);
      const fast = sma(closes, p.fast);
      const slow = sma(closes, p.slow);
      return crossDown(fast, slow);
    },

    MACD_CROSS_DOWN: (p: MacdCrossParams): boolean => {
      const closes = getCloses(cache, ctx, p.tf);
      const fast = ema(closes, p.fast);
      const slow = ema(closes, p.slow);
      if (fast.length === 0 || slow.length === 0) return false;
      const macdLine: number[] = [];
      const len = Math.min(fast.length, slow.length);
      for (let i = 0; i < len; i++) macdLine.push(fast[i]! - slow[i]!);
      const signal = ema(macdLine, p.signal);
      return crossDown(macdLine, signal);
    },
  };
}

export default createIndicators;
