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

// Module-level persistent store for PREV indicator buffers.
// Keyed by `${projectId}|${exchange}|${symbol}|prev_${callIndex}`.
// Lives outside _createIndicators so it survives across ticks — each call to
// _createIndicators is a fresh scope, but PREV needs multi-tick history to work.
// Maximum of 50 values per buffer keeps memory bounded.
const _prevBuffers = new Map<string, number[]>();
const PREV_MAX_BUF = 50;

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

    VWAP: (p: { tf: string; source?: unknown; reset?: string }): number => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Typical Price";
      // B-22: respect the reset parameter emitted by the Blockly generator.
      // "Session" (default) resets at midnight UTC — matching exchange-displayed VWAP.
      // "None" uses all loaded candles (legacy cumulative behaviour).
      const resetRaw = String((p as any).reset ?? "Session").toLowerCase();
      const sessionReset = resetRaw !== "none";

      const key = `${tf}|VWAP|${String(source)}|${sessionReset ? "session" : "all"}`;
      const hit = numCache.get(key);
      if (hit !== undefined) return hit;

      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series) return Number.NaN;
      const prices = getSource(tf, source);
      const vols = series.volumes;
      const openTimes = series.openTimes;
      const len = Math.min(prices.length, vols.length, openTimes.length);
      if (len === 0) return Number.NaN;

      // Determine the session start (midnight UTC of the most recent candle's day).
      let sessionStartMs = 0;
      if (sessionReset) {
        const lastOpenMs = openTimes[len - 1]!;
        const d = new Date(lastOpenMs);
        sessionStartMs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
      }

      let pv = 0;
      let vSum = 0;
      for (let i = 0; i < len; i++) {
        if (sessionReset && openTimes[i]! < sessionStartMs) continue;
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

      // Accept both short form ("high"/"low") and full label form ("High Volatility"/"Low Volatility")
      if (mode === "high" || mode.includes("high")) return currentAtr > mult * atrSma;
      if (mode === "low" || mode.includes("low")) return currentAtr < (1 / mult) * atrSma;
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
     * history itself in a module-level map that persists across ticks.
     *
     * Usage:  PREV({ series: EMA({...}), bars: 1 })
     *
     * Each distinct call-site in a strategy gets its own buffer, identified by
     * call order (the Nth PREV call in a given tick).  Buffers are capped at
     * PREV_MAX_BUF entries to bound memory.
     */
    PREV: (p: { series: number; bars?: number }): number => {
      const bars = Math.max(1, Math.floor(Number((p as any).bars ?? 1)));
      const currVal = Number((p as any).series);

      // Stable per-call-site index within this tick (resets each _createIndicators call,
      // but the buffer key below includes it so each site has its own persistent buffer).
      const prevCallKey = `prev_call_counter`;
      const callCount = (numCache.get(prevCallKey) ?? 0) + 1;
      numCache.set(prevCallKey, callCount);

      // Persistent buffer key — includes projectId so multi-project runners don't collide.
      const ns = ctx.projectId ?? ctx.exchange;
      const bufKey = `${ns}|${ctx.symbol}|prev_${callCount}`;

      let buf = _prevBuffers.get(bufKey);
      if (!buf) {
        buf = [];
        _prevBuffers.set(bufKey, buf);
      }

      // Append the current value, then cap the buffer length to avoid unbounded growth.
      if (Number.isFinite(currVal)) {
        buf.push(currVal);
        if (buf.length > PREV_MAX_BUF) buf.splice(0, buf.length - PREV_MAX_BUF);
      }

      // Return the value `bars` positions back from the latest (before current append).
      // buf now contains the appended current value, so index from end is: length-1 = current,
      // length-1-bars = `bars` ticks ago.
      const idx = buf.length - 1 - bars;
      if (idx < 0) return Number.NaN;
      return buf[idx]!;
    },


    BREAKOUT_UP: (p: { tf: string; source?: unknown; lookback: number; level?: number; confirm?: number }): boolean => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const lookback = Math.max(1, Math.floor(Number((p as any).lookback ?? 0)));
      const levelRaw = (p as any).level;
      const level = levelRaw === undefined || levelRaw === null ? null : Number(levelRaw);
      // confirm: number of consecutive closing bars that must all be above the threshold.
      // Defaults to 1 (current bar only), matching the previous single-bar behaviour.
      const confirm = Math.max(1, Math.floor(Number((p as any).confirm ?? 1)));

      const values = getSource(tf, source);
      if (values.length < confirm + 1) return false;

      // Determine the threshold against which we confirm.
      let threshold: number;
      if (level !== null && Number.isFinite(level as number)) {
        threshold = level as number;
      } else {
        // Use the highest value over the `lookback` bars that precede the confirmation window.
        const windowEnd = values.length - confirm; // exclusive end of the pre-confirm history
        const histStart = Math.max(0, windowEnd - lookback);
        if (windowEnd - histStart < 1) return false;
        let mx = -Infinity;
        for (let i = histStart; i < windowEnd; i++) {
          const v = values[i]!;
          if (Number.isFinite(v) && v > mx) mx = v;
        }
        if (!Number.isFinite(mx)) return false;
        threshold = mx;
      }

      // All `confirm` closing bars (most recent first) must exceed the threshold.
      for (let i = values.length - confirm; i < values.length; i++) {
        const v = values[i]!;
        if (!Number.isFinite(v) || v <= threshold) return false;
      }
      return true;
    },

    BREAKOUT_DOWN: (p: { tf: string; source?: unknown; lookback: number; level?: number; confirm?: number }): boolean => {
      const tf = String((p as any).tf ?? "1m");
      const source = (p as any).source ?? "Close";
      const lookback = Math.max(1, Math.floor(Number((p as any).lookback ?? 0)));
      const levelRaw = (p as any).level;
      const level = levelRaw === undefined || levelRaw === null ? null : Number(levelRaw);
      const confirm = Math.max(1, Math.floor(Number((p as any).confirm ?? 1)));

      const values = getSource(tf, source);
      if (values.length < confirm + 1) return false;

      let threshold: number;
      if (level !== null && Number.isFinite(level as number)) {
        threshold = level as number;
      } else {
        const windowEnd = values.length - confirm;
        const histStart = Math.max(0, windowEnd - lookback);
        if (windowEnd - histStart < 1) return false;
        let mn = Infinity;
        for (let i = histStart; i < windowEnd; i++) {
          const v = values[i]!;
          if (Number.isFinite(v) && v < mn) mn = v;
        }
        if (!Number.isFinite(mn)) return false;
        threshold = mn;
      }

      // All `confirm` closing bars must be below the threshold.
      for (let i = values.length - confirm; i < values.length; i++) {
        const v = values[i]!;
        if (!Number.isFinite(v) || v >= threshold) return false;
      }
      return true;
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

    // ── Candle Pattern Detection (OHLC-based, deterministic) ──────────────

    /**
     * CANDLE_PATTERN: detects common single/two-bar candle patterns from OHLC data.
     * pattern: "Doji" | "Bullish Engulfing" | "Bearish Engulfing" | "Hammer" | "Shooting Star"
     * Returns true if the pattern is detected on the most recent completed candle.
     */
    CANDLE_PATTERN: (p: { tf: string; pattern: string; dojiThreshold?: number }): boolean => {
      const tf = String((p as any).tf ?? "1m");
      const pattern = String((p as any).pattern ?? "Doji");
      const dojiPct = Number((p as any).dojiThreshold ?? 0.1); // body < 10% of range
      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series || series.opens.length < 2) return false;

      const n = series.opens.length;
      const o = series.opens[n - 1]!;
      const h = series.highs[n - 1]!;
      const l = series.lows[n - 1]!;
      const c = series.closes[n - 1]!;
      // Prior candle
      const po = series.opens[n - 2]!;
      const ph = series.highs[n - 2]!;
      const pl = series.lows[n - 2]!;
      const pc = series.closes[n - 2]!;

      const range = h - l;
      if (!Number.isFinite(range) || range <= 0) return false;
      const body = Math.abs(c - o);

      switch (pattern) {
        case "Doji": {
          // Body is <= dojiPct of full high-low range
          return body / range <= dojiPct;
        }
        case "Bullish Engulfing": {
          // Prior candle: bearish (close < open). Current: bullish body engulfs prior body.
          const priorBearish = pc < po;
          const currBullish = c > o;
          return priorBearish && currBullish && o <= pc && c >= po;
        }
        case "Bearish Engulfing": {
          // Prior candle: bullish (close > open). Current: bearish body engulfs prior body.
          const priorBullish = pc > po;
          const currBearish = c < o;
          return priorBullish && currBearish && o >= pc && c <= po;
        }
        case "Hammer": {
          // Small body in upper third of range, long lower shadow (>= 2x body), minimal upper shadow
          const lowerShadow = Math.min(o, c) - l;
          const upperShadow = h - Math.max(o, c);
          return body / range <= 0.33 && lowerShadow >= 2 * body && upperShadow <= body;
        }
        case "Shooting Star": {
          // Small body in lower third of range, long upper shadow (>= 2x body), minimal lower shadow
          const lowerShadow = Math.min(o, c) - l;
          const upperShadow = h - Math.max(o, c);
          return body / range <= 0.33 && upperShadow >= 2 * body && lowerShadow <= body;
        }
        default:
          return false;
      }
    },

    /**
     * VOLUME_SPIKE: returns true if current bar's volume exceeds avgMultiplier × SMA(volume, avgPeriod).
     * Default: current volume > 2× its 20-bar average.
     */
    VOLUME_SPIKE: (p: { tf: string; avgPeriod?: number; avgMultiplier?: number }): boolean => {
      const tf = String((p as any).tf ?? "1m");
      const avgPeriod = Math.max(1, Math.floor(Number((p as any).avgPeriod ?? 20)));
      const mult = Number((p as any).avgMultiplier ?? 2);
      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series || series.volumes.length < avgPeriod + 1) return false;
      const vols = series.volumes;
      const currentVol = vols[vols.length - 1]!;
      // Average of the prior avgPeriod bars (excluding current)
      const start = vols.length - 1 - avgPeriod;
      let sum = 0;
      let cnt = 0;
      for (let i = Math.max(0, start); i < vols.length - 1; i++) {
        const v = vols[i]!;
        if (Number.isFinite(v)) { sum += v; cnt++; }
      }
      if (cnt === 0) return false;
      const avg = sum / cnt;
      if (!Number.isFinite(avg) || avg <= 0) return false;
      return Number.isFinite(currentVol) && currentVol >= mult * avg;
    },

    /**
     * PRICE_CHANGE_PCT: returns the percentage price change over the last `bars` bars.
     * e.g. PRICE_CHANGE_PCT({ tf: "4h", bars: 4 }) > 2  ← price up >2% in last 4×4h candles
     */
    PRICE_CHANGE_PCT: (p: { tf: string; bars?: number }): number => {
      const tf = String((p as any).tf ?? "1m");
      const bars = Math.max(1, Math.floor(Number((p as any).bars ?? 1)));
      const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
      if (!series || series.closes.length < bars + 1) return Number.NaN;
      const closes = series.closes;
      const current = closes[closes.length - 1]!;
      const prior = closes[closes.length - 1 - bars]!;
      if (!Number.isFinite(current) || !Number.isFinite(prior) || prior === 0) return Number.NaN;
      return ((current - prior) / prior) * 100;
    },

    /**
     * TAKE_PROFIT: returns true if current mark price is >= entry_price × (1 + pct/100).
     * Intended to be used in the block workflow to conditionally sell:
     *   if TAKE_PROFIT({ pct: 5 }) → SELL 100%
     * The block does NOT automatically sell — it only evaluates the condition so the user
     * can wire it to a SELL block in their strategy.
     */
    TAKE_PROFIT: (p: { pct: number; entryPrice?: number }): boolean => {
      const pct = Number((p as any).pct ?? 0);
      if (!Number.isFinite(pct) || pct <= 0) return false;
      const explicitEntry = (p as any)?.entryPrice;
      // entryPrice can be passed explicitly or looked up from the open position
      // via the kline cache (mark price). For block usage the engine exposes entry
      // via IN_POSITION / openPos; here we rely on the caller supplying entryPrice
      // or use a sentinel NaN to indicate no position.
      if (explicitEntry !== undefined) {
        const ep = Number(explicitEntry);
        const markPrice = series_getMarkPriceFromCache(cache, ctx);
        if (!Number.isFinite(ep) || ep <= 0 || !markPrice) return false;
        return markPrice >= ep * (1 + pct / 100);
      }
      return false; // Without entryPrice the block-level check uses HP.takeProfitCheck()
    },

    /**
     * STOP_LOSS: returns true if current mark price is <= entry_price × (1 - pct/100).
     * Same usage pattern as TAKE_PROFIT above.
     */
    STOP_LOSS: (p: { pct: number; entryPrice?: number }): boolean => {
      const pct = Number((p as any).pct ?? 0);
      if (!Number.isFinite(pct) || pct <= 0) return false;
      const explicitEntry = (p as any)?.entryPrice;
      if (explicitEntry !== undefined) {
        const ep = Number(explicitEntry);
        const markPrice = series_getMarkPriceFromCache(cache, ctx);
        if (!Number.isFinite(ep) || ep <= 0 || !markPrice) return false;
        return markPrice <= ep * (1 - pct / 100);
      }
      return false;
    },
  };
}

/** Internal helper: read latest close from cache (same logic as PaperBroker.getMarkPrice). */
function series_getMarkPriceFromCache(cache: KlineCache, ctx: IndicatorContext): number | null {
  for (const tf of ["1m", "5m", "15m", "1h", "4h"]) {
    const closes = cache.getCloses(ctx.exchange, ctx.symbol, tf);
    if (!closes || closes.length === 0) continue;
    const v = closes[closes.length - 1];
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

export default createIndicators;
