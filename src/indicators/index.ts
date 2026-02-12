import type { KlineCache } from "../klines/KlineCache.js";
import type {
  IndicatorContext,
  RSIParams,
  EmaCrossParams,
  SmaCrossParams,
  MacdCrossParams,
} from "./types.js";
import { ema, sma, rsi as rsiCalc, macd } from "./math.js";

function getCloses(cache: KlineCache, ctx: IndicatorContext, tf: string): number[] {
  const series = cache.getSeries(ctx.exchange, ctx.symbol, tf);
  return series?.closes ?? [];
}

export function makeIndicators(cache: KlineCache, ctx: IndicatorContext) {
  return {
    RSI: (params: RSIParams): number => {
      const closes = getCloses(cache, ctx, params.tf);
      return rsiCalc(closes, params.period);
    },
    EMA_CROSS_UP: (params: EmaCrossParams): boolean => {
      const closes = getCloses(cache, ctx, params.tf);
      const fastArr = ema(closes, params.fast);
      const slowArr = ema(closes, params.slow);
      if (fastArr.length < 2 || slowArr.length < 2) return false;
      const i = fastArr.length - 1;
      const prev = fastArr[i - 1] - slowArr[i - 1];
      const cur = fastArr[i] - slowArr[i];
      return prev <= 0 && cur > 0;
    },
    SMA_CROSS_UP: (params: SmaCrossParams): boolean => {
      const closes = getCloses(cache, ctx, params.tf);
      const fastArr = sma(closes, params.fast);
      const slowArr = sma(closes, params.slow);
      if (fastArr.length < 2 || slowArr.length < 2) return false;
      const i = fastArr.length - 1;
      const prev = fastArr[i - 1] - slowArr[i - 1];
      const cur = fastArr[i] - slowArr[i];
      return prev <= 0 && cur > 0;
    },
    MACD_CROSS_UP: (params: MacdCrossParams): boolean => {
      const closes = getCloses(cache, ctx, params.tf);
      const { macd: macdLine, signal: signalLine } = macd(closes, params.fast, params.slow, params.signal);
      if (macdLine.length < 2 || signalLine.length < 2) return false;
      const i = macdLine.length - 1;
      const prev = macdLine[i - 1] - signalLine[i - 1];
      const cur = macdLine[i] - signalLine[i];
      return prev <= 0 && cur > 0;
    },
  };
}
