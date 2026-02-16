export function sma(values: number[], period: number): number[] {
  if (period <= 0) return [];
  const out: number[] = new Array(values.length).fill(NaN);
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += values[i];
    if (i >= period) sum -= values[i - period];
    if (i >= period - 1) out[i] = sum / period;
  }
  return out;
}

export function ema(values: number[], period: number): number[] {
  if (period <= 0) return [];
  const out: number[] = new Array(values.length).fill(NaN);
  const k = 2 / (period + 1);
  // Seed with SMA of first period
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (!Number.isFinite(v)) continue;
    if (i < period) {
      sum += v;
      if (i === period - 1) {
        out[i] = sum / period;
      }
      continue;
    }
    if (i === period) {
      // first EMA point uses previous SMA seed
      out[i] = (v - out[i - 1]) * k + out[i - 1];
      continue;
    }
    const prev = out[i - 1];
    out[i] = (v - prev) * k + prev;
  }
  // Fill earlier values with NaN (already)
  // Forward-calculate EMA starting from first defined point
  // Find first defined index
  let first = out.findIndex((x) => Number.isFinite(x));
  if (first >= 0) {
    for (let i = first + 1; i < values.length; i++) {
      if (!Number.isFinite(values[i])) continue;
      const prev = out[i - 1];
      if (!Number.isFinite(prev)) continue;
      out[i] = (values[i] - prev) * k + prev;
    }
  }
  return out;
}

export function wma(values: number[], period: number): number[] {
  const n = Math.max(1, Math.floor(period));
  if (n <= 0) return [];
  const out: number[] = new Array(values.length).fill(NaN);
  const denom = (n * (n + 1)) / 2;
  for (let i = n - 1; i < values.length; i++) {
    let sum = 0;
    let w = 1;
    for (let j = i - n + 1; j <= i; j++) {
      const v = values[j];
      if (!Number.isFinite(v)) {
        sum = NaN;
        break;
      }
      sum += v * w;
      w++;
    }
    if (Number.isFinite(sum)) out[i] = sum / denom;
  }
  return out;
}

/**
 * Relative Strength Index (RSI) using Wilder's smoothing.
 * Returns an array aligned with input (first period values are NaN).
 */
/**
 * Relative Strength Index (RSI) using Wilder's smoothing.
 * Returns ONLY the latest RSI value.
 */
export function rsi(values: number[], period: number): number {
  const n = Math.max(1, Math.floor(period));
  if (values.length < n + 1) return Number.NaN;

  let gain = 0;
  let loss = 0;
  for (let i = 1; i <= n; i++) {
    const diff = values[i] - values[i - 1];
    if (diff >= 0) gain += diff;
    else loss -= diff;
  }

  let avgGain = gain / n;
  let avgLoss = loss / n;

  const rs0 = avgLoss === 0 ? Infinity : avgGain / avgLoss;
  let lastRsi = 100 - 100 / (1 + rs0);

  for (let i = n + 1; i < values.length; i++) {
    const diff = values[i] - values[i - 1];
    const g = diff > 0 ? diff : 0;
    const l = diff < 0 ? -diff : 0;
    avgGain = (avgGain * (n - 1) + g) / n;
    avgLoss = (avgLoss * (n - 1) + l) / n;
    const rs = avgLoss === 0 ? Infinity : avgGain / avgLoss;
    lastRsi = 100 - 100 / (1 + rs);
  }
  return lastRsi;
}

/**
 * Average True Range (ATR) using Wilder's smoothing.
 * Returns ONLY the latest ATR value.
 */
export function atr(highs: number[], lows: number[], closes: number[], period: number): number {
  const n = Math.max(1, Math.floor(period));
  // Need previous close to compute true range.
  const len = Math.min(highs.length, lows.length, closes.length);
  if (len < n + 1) return Number.NaN;

  // True range starts from index 1.
  let trSum = 0;
  for (let i = 1; i <= n; i++) {
    const h = highs[i]!;
    const l = lows[i]!;
    const pc = closes[i - 1]!;
    const tr = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
    trSum += tr;
  }

  let lastAtr = trSum / n;
  for (let i = n + 1; i < len; i++) {
    const h = highs[i]!;
    const l = lows[i]!;
    const pc = closes[i - 1]!;
    const tr = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
    lastAtr = (lastAtr * (n - 1) + tr) / n;
  }
  return lastAtr;
}

/** Returns true if seriesA crosses above seriesB on the most recent candle. */
export function crossUp(seriesA: number[], seriesB: number[]): boolean {
  const a = last2Defined(seriesA);
  const b = last2Defined(seriesB);
  if (!a || !b) return false;
  return a[0] <= b[0] && a[1] > b[1];
}

/** Returns true if seriesA crosses below seriesB on the most recent candle. */
export function crossDown(seriesA: number[], seriesB: number[]): boolean {
  const a = last2Defined(seriesA);
  const b = last2Defined(seriesB);
  if (!a || !b) return false;
  return a[0] >= b[0] && a[1] < b[1];
}

export function macd(values: number[], fast = 12, slow = 26, signal = 9): { macd: number[]; signal: number[] } {
  const fastEma = ema(values, fast);
  const slowEma = ema(values, slow);
  const macdLine = values.map((_, i) => fastEma[i] - slowEma[i]);
  const signalLine = ema(macdLine, signal);
  return { macd: macdLine, signal: signalLine };
}

export function macdLatest(
  values: number[],
  fast = 12,
  slow = 26,
  signal = 9
): { macd: number; signal: number; histogram: number } {
  const fastEma = ema(values, fast);
  const slowEma = ema(values, slow);
  const len = Math.min(fastEma.length, slowEma.length);
  if (len === 0) return { macd: Number.NaN, signal: Number.NaN, histogram: Number.NaN };
  const macdLine: number[] = new Array(len);
  for (let i = 0; i < len; i++) macdLine[i] = fastEma[i]! - slowEma[i]!;
  const signalLine = ema(macdLine, signal);
  const m = lastFinite(macdLine);
  const s = lastFinite(signalLine);
  return { macd: m, signal: s, histogram: m - s };
}

export function bbandsLatest(
  values: number[],
  length = 20,
  mult = 2
): { upper: number; middle: number; lower: number } {
  const n = Math.max(1, Math.floor(length));
  if (values.length < n) return { upper: Number.NaN, middle: Number.NaN, lower: Number.NaN };
  const end = values.length - 1;
  let sum = 0;
  for (let i = end - n + 1; i <= end; i++) sum += values[i]!;
  const mean = sum / n;
  let varSum = 0;
  for (let i = end - n + 1; i <= end; i++) {
    const d = values[i]! - mean;
    varSum += d * d;
  }
  const std = Math.sqrt(varSum / n);
  const k = Number(mult);
  return { upper: mean + k * std, middle: mean, lower: mean - k * std };
}

function lastFinite(series: number[]): number {
  for (let i = series.length - 1; i >= 0; i--) {
    const v = series[i];
    if (Number.isFinite(v)) return v;
  }
  return Number.NaN;
}

export function last2Defined(series: number[]): [number, number] | null {
  for (let i = series.length - 1; i >= 1; i--) {
    const a = series[i - 1];
    const b = series[i];
    if (Number.isFinite(a) && Number.isFinite(b)) return [a, b];
  }
  return null;
}
