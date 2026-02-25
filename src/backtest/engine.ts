import type { KlineSeries } from "../klines/KlineCache.js";
import { BacktestKlineCache } from "./BacktestKlineCache.js";
import { BacktestBroker, type BacktestTradeRecord } from "./BacktestBroker.js";
import createIndicators from "../indicators/createIndicators.js";
import { runInSandbox } from "../engine/Sandbox.js";

// ── Types ────────────────────────────────────────────────────────────────────

export type BacktestInput = {
  /** Compiled strategy JS (same as generated_js in the project). */
  code: string;
  /** Full historical series, keyed by timeframe string (e.g. "1h"). */
  allSeries: Record<string, KlineSeries>;
  symbol: string;
  exchange: string;
  /** Project ID used for indicator buffer namespacing. */
  projectId: string;
  /** Primary timeframe — drives the bar loop. */
  primaryTf: string;
  startCapital: number;
  /** Number of bars to use as a warmup / indicator burn-in (excluded from results). */
  warmupBars?: number;
};

export type EquityPoint = {
  ts: number;       // bar open_time ms
  equity: number;
  drawdown: number; // as a fraction, e.g. -0.05 = -5%
};

export type BacktestResult = {
  summary: {
    startCapital: number;
    endCapital: number;
    totalReturnUsd: number;
    totalReturnPct: number;
    totalTrades: number;     // sell fills only
    winRate: number;         // fraction of profitable sell fills
    maxDrawdownPct: number;  // worst peak-to-trough as a %
    sharpeRatio: number;     // annualised, using bar returns
    barsSimulated: number;
    warmupBars: number;
    primaryTf: string;
    symbol: string;
  };
  equityCurve: EquityPoint[];
  trades: BacktestTradeRecord[];
  errors: string[];
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function safeNumber(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : Number.NaN;
}

/** Compute annualised Sharpe ratio from a series of bar-level returns. */
function sharpe(returns: number[], barsPerYear: number): number {
  if (returns.length < 2) return 0;
  const n = returns.length;
  const mean = returns.reduce((s, r) => s + r, 0) / n;
  const variance = returns.reduce((s, r) => s + (r - mean) ** 2, 0) / (n - 1);
  const std = Math.sqrt(variance);
  if (std === 0) return 0;
  return (mean / std) * Math.sqrt(barsPerYear);
}

const TF_BARS_PER_YEAR: Record<string, number> = {
  "1m":  525_600,
  "3m":  175_200,
  "5m":  105_120,
  "15m":  35_040,
  "30m":  17_520,
  "1h":    8_760,
  "2h":    4_380,
  "4h":    2_190,
  "6h":    1_460,
  "8h":    1_095,
  "12h":     730,
  "1d":      365,
};

// ── Main entry point ─────────────────────────────────────────────────────────

export async function runBacktest(input: BacktestInput): Promise<BacktestResult> {
  const {
    code,
    allSeries,
    symbol,
    exchange,
    projectId,
    primaryTf,
    startCapital,
    warmupBars = 0,
  } = input;

  const primarySeries = allSeries[primaryTf];
  if (!primarySeries || primarySeries.openTimes.length === 0) {
    return makeEmptyResult(input, ["No primary series data available."]);
  }

  const totalBars = primarySeries.openTimes.length;
  const startBar = Math.max(0, warmupBars);

  // ── Initialise cache and broker ──────────────────────────────────────────

  const cache = new BacktestKlineCache();
  for (const [tf, series] of Object.entries(allSeries)) {
    cache.load(exchange, symbol, tf, series);
  }

  const broker = new BacktestBroker(startCapital);

  // ── Persistent state that mirrors the live runner ────────────────────────

  /** crossPrev state — must persist across bars (same as live). */
  const crossPrev = new Map<string, { a: number; b: number }>();

  /** PREV indicator buffers — keyed by call-site, reused across bars. */
  const prevBuffers = new Map<string, number[]>();
  const PREV_MAX_BUF = 50;

  const equityCurve: EquityPoint[] = [];
  const errors: string[] = [];

  let peakEquity = startCapital;

  // ── Bar loop ─────────────────────────────────────────────────────────────

  for (let barIdx = 0; barIdx < totalBars; barIdx++) {
    const barTs = primarySeries.openTimes[barIdx]!;
    const barClose = primarySeries.closes[barIdx]!;

    // Advance the window so indicators see candles [0..barIdx].
    cache.setWindowEnd(barIdx);

    // Give the broker the current mark price before strategy runs.
    broker.setCurrentBar(barTs, barClose);

    // Skip warmup bars from equity recording and trade execution.
    if (barIdx < startBar) {
      peakEquity = Math.max(peakEquity, broker.getEquity());
      continue;
    }

    // ── Build sandbox API for this bar ────────────────────────────────────

    const context = { exchange, symbol, projectId };
    const indicators = createIndicators(cache as any, context);

    let crossCallIdx = 0;

    const CROSS_UP = (a: any, b: any) => {
      const currA = safeNumber((a as any)?.a ?? a);
      const currB = safeNumber((a as any)?.b ?? b);
      const strict = (a as any)?.strict === true;
      const idx = ++crossCallIdx;
      const key = `${projectId}|${symbol}|up|${idx}`;
      const prev = crossPrev.get(key);
      crossPrev.set(key, { a: currA, b: currB });
      if (!prev) return false;
      return (strict ? prev.a < prev.b : prev.a <= prev.b) && currA > currB;
    };

    const CROSS_DOWN = (a: any, b: any) => {
      const currA = safeNumber((a as any)?.a ?? a);
      const currB = safeNumber((a as any)?.b ?? b);
      const strict = (a as any)?.strict === true;
      const idx = ++crossCallIdx;
      const key = `${projectId}|${symbol}|down|${idx}`;
      const prev = crossPrev.get(key);
      crossPrev.set(key, { a: currA, b: currB });
      if (!prev) return false;
      return (strict ? prev.a > prev.b : prev.a >= prev.b) && currA < currB;
    };

    // Position snapshot (synchronous — no DB round-trip in backtest).
    const openPos = broker.getPositionSync();
    const positionRef = { current: openPos };

    const refreshPositionRef = async () => {
      positionRef.current = broker.getPositionSync();
    };

    const IN_POSITION = () => Boolean(positionRef.current?.id);

    const BARS_SINCE_ENTRY = () => {
      if (!positionRef.current?.entryTs) return Number.POSITIVE_INFINITY;
      const entryTs = positionRef.current.entryTs;
      // Count how many primary bars have opened since the entry bar.
      let count = 0;
      for (let i = barIdx; i >= 0; i--) {
        if ((primarySeries.openTimes[i] ?? 0) < entryTs) break;
        count++;
      }
      return Math.max(0, count - 1);
    };

    const COOLDOWN_OK = (p: { bars: number; scope?: string }) => {
      const bars = Math.max(0, Math.floor(Number((p as any)?.bars ?? 0)));
      return BARS_SINCE_ENTRY() >= bars;
    };

    const TAKE_PROFIT = (p: { pct: number }): boolean => {
      const pct = Number((p as any)?.pct ?? 0);
      if (!pct || !positionRef.current) return false;
      const ep = positionRef.current.entryPrice;
      const mp = broker.getMarkPricePublic();
      if (!mp || !ep) return false;
      return mp >= ep * (1 + pct / 100);
    };

    const STOP_LOSS = (p: { pct: number }): boolean => {
      const pct = Number((p as any)?.pct ?? 0);
      if (!pct || !positionRef.current) return false;
      const ep = positionRef.current.entryPrice;
      const mp = broker.getMarkPricePublic();
      if (!mp || !ep) return false;
      return mp <= ep * (1 - pct / 100);
    };

    const HP = {
      buy: async (a: any, b?: any) => {
        const usd = typeof a === "number" ? a : typeof b === "number" ? b : Number(a?.usd ?? 0);
        await broker.buy({ usd });
        await refreshPositionRef();
      },
      sell: async (a: any, b?: any) => {
        const pct = typeof a === "number" ? a : typeof b === "number" ? b : Number(a?.pct ?? 100);
        await broker.sell({ pct });
        await refreshPositionRef();
      },
      log: async (_msg: string) => { /* no-op */ },
      takeProfitCheck: async (a: any) => {
        const triggered = await broker.takeProfitCheck({ pct: Number(a?.pct ?? a) });
        if (triggered) await refreshPositionRef();
        return triggered;
      },
      stopLossCheck: async (a: any) => {
        const triggered = await broker.stopLossCheck({ pct: Number(a?.pct ?? a) });
        if (triggered) await refreshPositionRef();
        return triggered;
      },
    };

    // Inject our PREV override into the indicators — we need isolation per
    // backtest run to prevent the module-level _prevBuffers from mixing
    // across concurrent/sequential runs.
    // We monkey-patch the PREV function onto our local indicators object.
    let prevCallCounter = 0;
    (indicators as any).PREV = (p: { series: number; bars?: number }) => {
      const bars = Math.max(1, Math.floor(Number((p as any).bars ?? 1)));
      const currVal = Number((p as any).series);
      const callCount = ++prevCallCounter;
      const bufKey = `${projectId}|${symbol}|prev_${callCount}`;
      let buf = prevBuffers.get(bufKey);
      if (!buf) {
        buf = [];
        prevBuffers.set(bufKey, buf);
      }
      if (Number.isFinite(currVal)) {
        buf.push(currVal);
        if (buf.length > PREV_MAX_BUF) buf.splice(0, buf.length - PREV_MAX_BUF);
      }
      const idx = buf.length - 1 - bars;
      if (idx < 0) return Number.NaN;
      return buf[idx]!;
    };

    // ── Execute strategy in sandbox ───────────────────────────────────────

    try {
      await runInSandbox(
        code,
        {
          ...indicators,
          CROSS_UP,
          CROSS_DOWN,
          COOLDOWN_OK,
          IN_POSITION,
          BARS_SINCE_ENTRY,
          TAKE_PROFIT,
          STOP_LOSS,
          HP,
          context,
        },
        { timeoutMs: 2000 }
      );
    } catch (e: any) {
      // Record error but keep simulating — don't abort the whole run.
      const msg = `Bar ${barIdx} (${new Date(barTs).toISOString()}): ${e?.message ?? String(e)}`;
      errors.push(msg);
    }

    // ── Record equity for this bar ────────────────────────────────────────

    const equity = broker.getEquity();
    peakEquity = Math.max(peakEquity, equity);
    const drawdown = peakEquity > 0 ? (equity - peakEquity) / peakEquity : 0;

    equityCurve.push({ ts: barTs, equity, drawdown });
  }

  // ── Compute summary statistics ────────────────────────────────────────────

  const finalEquity = broker.getEquity();
  const trades = broker.getTrades();
  const sellTrades = trades.filter((t) => t.side === "sell");
  const wins = sellTrades.filter((t) => t.realizedPnl > 0).length;

  const maxDrawdownPct =
    equityCurve.length > 0
      ? Math.min(0, ...equityCurve.map((p) => p.drawdown)) * 100
      : 0;

  // Bar returns for Sharpe.
  const returns: number[] = [];
  for (let i = 1; i < equityCurve.length; i++) {
    const prev = equityCurve[i - 1]!.equity;
    const curr = equityCurve[i]!.equity;
    if (prev > 0) returns.push((curr - prev) / prev);
  }

  const barsPerYear = TF_BARS_PER_YEAR[primaryTf] ?? 8_760;
  const sharpeRatio = sharpe(returns, barsPerYear);

  return {
    summary: {
      startCapital,
      endCapital: finalEquity,
      totalReturnUsd: finalEquity - startCapital,
      totalReturnPct: startCapital > 0 ? ((finalEquity - startCapital) / startCapital) * 100 : 0,
      totalTrades: sellTrades.length,
      winRate: sellTrades.length > 0 ? wins / sellTrades.length : 0,
      maxDrawdownPct,
      sharpeRatio,
      barsSimulated: Math.max(0, totalBars - startBar),
      warmupBars: startBar,
      primaryTf,
      symbol,
    },
    equityCurve,
    trades,
    errors,
  };
}

function makeEmptyResult(input: BacktestInput, errors: string[]): BacktestResult {
  return {
    summary: {
      startCapital: input.startCapital,
      endCapital: input.startCapital,
      totalReturnUsd: 0,
      totalReturnPct: 0,
      totalTrades: 0,
      winRate: 0,
      maxDrawdownPct: 0,
      sharpeRatio: 0,
      barsSimulated: 0,
      warmupBars: input.warmupBars ?? 0,
      primaryTf: input.primaryTf,
      symbol: input.symbol,
    },
    equityCurve: [],
    trades: [],
    errors,
  };
}
