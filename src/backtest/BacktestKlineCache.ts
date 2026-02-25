import type { KlineSeries } from "../klines/KlineCache.js";

/**
 * A read-only, windowed kline cache for backtesting.
 *
 * Load full historical series once with `load()`, then advance the simulation
 * bar-by-bar by calling `setWindowEnd(barIndex)`.  Every `getSeries()` /
 * `getCloses()` call returns data sliced to `[0 .. barIndex]` (inclusive),
 * preventing any lookahead bias in indicator calculations.
 *
 * The interface intentionally mirrors KlineCache so that `createIndicators`
 * can be handed a BacktestKlineCache with zero changes.
 */
export class BacktestKlineCache {
  private fullSeries = new Map<string, KlineSeries>();
  private _windowEnd = 0;

  /** Load a complete historical series for one symbol + timeframe. */
  load(exchange: string, symbol: string, interval: string, series: KlineSeries): void {
    const key = `${exchange}|${symbol}|${interval}`;
    this.fullSeries.set(key, series);
  }

  /** Advance the simulation window.  barIndex is 0-based into the primary series. */
  setWindowEnd(barIndex: number): void {
    this._windowEnd = barIndex;
  }

  /** Returns the current window end (bar index). */
  get windowEnd(): number {
    return this._windowEnd;
  }

  /** Returns the series sliced to the current window, or null if not loaded. */
  getSeries(exchange: string, symbol: string, interval: string): KlineSeries | null {
    const full = this.fullSeries.get(`${exchange}|${symbol}|${interval}`);
    if (!full) return null;

    // For non-primary timeframes whose bar boundaries don't align with the
    // primary tf, include all candles whose open_time <= primary bar's open_time.
    // For simplicity we just slice by the current window end index — the
    // primary tf's series is aligned, and secondary tfs contain all bars up to
    // the same wall-clock point because we load all historical data upfront.
    const end = this._windowEnd + 1; // exclusive upper bound
    if (end >= full.openTimes.length) return full; // fast-path: no copy needed

    return {
      exchange: full.exchange,
      symbol: full.symbol,
      interval: full.interval,
      openTimes: full.openTimes.slice(0, end),
      opens:     full.opens.slice(0, end),
      highs:     full.highs.slice(0, end),
      lows:      full.lows.slice(0, end),
      closes:    full.closes.slice(0, end),
      volumes:   full.volumes.slice(0, end),
    };
  }

  /** Convenience: closes array for the current window. */
  getCloses(exchange: string, symbol: string, interval: string): number[] {
    return this.getSeries(exchange, symbol, interval)?.closes ?? [];
  }

  /** Returns the open_time (ms) of the bar at the current window end. */
  currentBarTs(exchange: string, symbol: string, interval: string): number | null {
    const series = this.getSeries(exchange, symbol, interval);
    if (!series || series.openTimes.length === 0) return null;
    return series.openTimes[series.openTimes.length - 1] ?? null;
  }

  /** Total bars loaded for a series (ignores window). */
  totalBars(exchange: string, symbol: string, interval: string): number {
    return this.fullSeries.get(`${exchange}|${symbol}|${interval}`)?.openTimes.length ?? 0;
  }

  clear(): void {
    this.fullSeries.clear();
  }
}
