import type { KlineCache } from "../klines/KlineCache.js";
import type {
  EmaCrossParams,
  IndicatorContext,
  MacdCrossParams,
  RSIParams,
  SmaCrossParams,
} from "./types.js";
import { crossUp, ema, rsi as rsiCalc, sma } from "./math.js";

function getCloses(cache: KlineCache, ctx: IndicatorContext, tf: string): number[] {
  const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
  return series?.closes ?? [];
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
  return {
    RSI: (p: RSIParams): number => {
      const closes = getCloses(cache, ctx, p.tf);
      return rsiCalc(closes, p.period);
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
  };
}

export default createIndicators;
