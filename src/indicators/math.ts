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

export function last2Defined(series: number[]): [number, number] | null {
  for (let i = series.length - 1; i >= 1; i--) {
    const a = series[i - 1];
    const b = series[i];
    if (Number.isFinite(a) && Number.isFinite(b)) return [a, b];
  }
  return null;
}
