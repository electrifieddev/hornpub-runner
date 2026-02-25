/**
 * In-memory paper broker for backtesting.
 *
 * Mirrors the PaperBroker public API (buy / sell / getPosition /
 * getMarkPricePublic / takeProfitCheck / stopLossCheck) but never touches
 * Supabase — all state is held in memory and returned as a result object at
 * the end of the simulation.
 *
 * Assumptions:
 *  - Fills execute at the bar's close price (mark price set by the engine).
 *  - No fees (can be added later via a feeBps parameter).
 *  - Capital is tracked in the quote currency (USD / USDT).
 */

export type BacktestTradeRecord = {
  side: "buy" | "sell";
  price: number;
  qty: number;
  usdValue: number;
  ts: number;          // bar open_time ms
  realizedPnl: number; // 0 for buy; filled on sell
  equityAfter: number; // equity snapshot immediately after the fill
  positionId: string;
  meta?: Record<string, any>;
};

export type BacktestPositionState = {
  id: string;
  qty: number;
  entryPrice: number;
  entryTs: number;
  entry_price?: number; // alias for compatibility with live broker shape
  entry_time?: number;
};

export class BacktestBroker {
  private capital: number;
  private position: BacktestPositionState | null = null;
  private trades: BacktestTradeRecord[] = [];
  private posIdCounter = 0;

  /** Set by the engine before each bar's strategy execution. */
  private markPrice = 0;
  private barTs = 0;

  constructor(startCapital: number) {
    this.capital = startCapital;
  }

  // ── Called by the engine before running each bar ────────────────────────

  setCurrentBar(ts: number, markPrice: number): void {
    this.barTs = ts;
    this.markPrice = markPrice;
  }

  // ── PaperBroker-compatible public API ───────────────────────────────────

  getMarkPricePublic(): number | null {
    return this.markPrice > 0 ? this.markPrice : null;
  }

  async getPosition(): Promise<BacktestPositionState | null> {
    if (!this.position) return null;
    return {
      ...this.position,
      entry_price: this.position.entryPrice,
      entry_time: this.position.entryTs,
    };
  }

  /** Synchronous variant used inside the engine for equity / state reads. */
  getPositionSync(): BacktestPositionState | null {
    return this.position;
  }

  async log(_level: string, _msg: string, _meta?: any): Promise<void> {
    // no-op in backtest mode
  }

  async buy(args: { usd: number }): Promise<void> {
    const usd = Number(args?.usd ?? 0);
    if (!Number.isFinite(usd) || usd <= 0) return;
    const price = this.markPrice;
    if (!price) return;

    // Cap spend to available cash.
    const spend = Math.min(usd, this.capital);
    if (spend <= 0) return;

    const qty = spend / price;

    if (this.position) {
      // Volume-weighted average into existing position.
      const totalQty = this.position.qty + qty;
      const avgEntry =
        (this.position.qty * this.position.entryPrice + qty * price) / totalQty;
      this.position = { ...this.position, qty: totalQty, entryPrice: avgEntry };
    } else {
      const id = `bt_pos_${++this.posIdCounter}`;
      this.position = {
        id,
        qty,
        entryPrice: price,
        entryTs: this.barTs,
      };
    }

    this.capital -= spend;

    this.trades.push({
      side: "buy",
      price,
      qty,
      usdValue: spend,
      ts: this.barTs,
      realizedPnl: 0,
      equityAfter: this.getEquity(),
      positionId: this.position!.id,
    });
  }

  async sell(args: { pct: number }): Promise<void> {
    if (!this.position) return;
    const pct = Number(args?.pct ?? 100);
    if (!Number.isFinite(pct) || pct <= 0) return;
    const price = this.markPrice;
    if (!price) return;

    const closeFrac = Math.min(1, pct / 100);
    const closeQty = this.position.qty * closeFrac;
    const realizedPnl = (price - this.position.entryPrice) * closeQty;
    const proceeds = closeQty * price;

    this.capital += proceeds;

    const posId = this.position.id;
    const remainingQty = this.position.qty - closeQty;

    if (remainingQty <= 1e-12) {
      this.position = null;
    } else {
      this.position = { ...this.position, qty: remainingQty };
    }

    this.trades.push({
      side: "sell",
      price,
      qty: closeQty,
      usdValue: proceeds,
      ts: this.barTs,
      realizedPnl,
      equityAfter: this.getEquity(),
      positionId: posId,
    });
  }

  async takeProfitCheck(args: { pct: number }): Promise<boolean> {
    const pct = Number(args?.pct ?? 0);
    if (!Number.isFinite(pct) || pct <= 0 || !this.position) return false;
    const ep = this.position.entryPrice;
    const mp = this.markPrice;
    if (!mp || !ep) return false;
    if (mp >= ep * (1 + pct / 100)) {
      await this.sell({ pct: 100 });
      return true;
    }
    return false;
  }

  async stopLossCheck(args: { pct: number }): Promise<boolean> {
    const pct = Number(args?.pct ?? 0);
    if (!Number.isFinite(pct) || pct <= 0 || !this.position) return false;
    const ep = this.position.entryPrice;
    const mp = this.markPrice;
    if (!mp || !ep) return false;
    if (mp <= ep * (1 - pct / 100)) {
      await this.sell({ pct: 100 });
      return true;
    }
    return false;
  }

  // ── Result accessors ─────────────────────────────────────────────────────

  /** Current total equity = cash + open position mark value. */
  getEquity(): number {
    const posValue = this.position ? this.position.qty * this.markPrice : 0;
    return this.capital + posValue;
  }

  getCapital(): number {
    return this.capital;
  }

  getTrades(): BacktestTradeRecord[] {
    return this.trades;
  }
}
