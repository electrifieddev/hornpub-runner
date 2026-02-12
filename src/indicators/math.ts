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

/** Returns true if seriesA crosses above seriesB on the most recent candle. */
export function crossUp(seriesA: number[], seriesB: number[]): boolean {
  const a = last2Defined(seriesA);
  const b = last2Defined(seriesB);
  if (!a || !b) return false;
  return a[0] <= b[0] && a[1] > b[1];
}

export function macd(values: number[], fast = 12, slow = 26, signal = 9): { macd: number[]; signal: number[] } {
  const fastEma = ema(values, fast);
  const slowEma = ema(values, slow);
  const macdLine = values.map((_, i) => fastEma[i] - slowEma[i]);
  const signalLine = ema(macdLine, signal);
  return { macd: macdLine, signal: signalLine };
}

export function last2Defined(series: number[]): [number, number] | null {
  for (let i = series.length - 1; i >= 1; i--) {
    const a = series[i - 1];
    const b = series[i];
    if (Number.isFinite(a) && Number.isFinite(b)) return [a, b];
  }
  return null;
}
