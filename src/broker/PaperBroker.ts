import type { SupabaseClient } from "@supabase/supabase-js";
import type { KlineCache } from "../klines/KlineCache.js";

type LogLevel = "info" | "warn" | "error";

export type PaperBrokerCtx = {
  userId: string;
  projectId: string;
  runId: string;
  symbol: string;
  exchange: string; // e.g. "binance"
  tf: string; // default timeframe used for pricing
};

export type BuyArgs = { usd: number };
export type SellArgs = { pct: number };

export class PaperBroker {
  private sb: SupabaseClient;
  private cache: KlineCache;
  private ctx: PaperBrokerCtx;

  constructor(opts: { supabase: SupabaseClient; cache: KlineCache; ctx: PaperBrokerCtx }) {
    this.sb = opts.supabase;
    this.cache = opts.cache;
    this.ctx = opts.ctx;
  }

  /** Latest close for pricing. Returns null if we don't have candles yet. */
  private getMarkPrice(): number | null {
    const closes = this.cache.getCloses(this.ctx.exchange, this.ctx.symbol, this.ctx.tf);
    if (!closes || closes.length === 0) return null;
    const v = closes[closes.length - 1];
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  // Intentionally public so sandbox-exposed HP.log can forward to it.
  async log(level: LogLevel, message: string, meta: Record<string, any> = {}) {
    // Keep schema-flexible: put details in meta (jsonb).
    await this.sb.from("project_logs").insert({
      user_id: this.ctx.userId,
      project_id: this.ctx.projectId,
      level,
      message,
      meta: { ...meta, run_id: this.ctx.runId, symbol: this.ctx.symbol, exchange: this.ctx.exchange },
    });
  }

  private async getOpenPosition() {
    const { data, error } = await this.sb
      .from("project_positions")
      .select("*")
      .eq("project_id", this.ctx.projectId)
      .eq("symbol", this.ctx.symbol)
      .eq("status", "open")
      .maybeSingle();
    if (error) throw error;
    return data as any | null;
  }

  /**
   * Paper buy: opens a LONG position if none exists.
   * If a position exists, it logs and does nothing.
   */
  async buy(args: BuyArgs) {
    const usd = Number(args?.usd ?? 0);
    if (!Number.isFinite(usd) || usd <= 0) {
      await this.log("warn", "BUY skipped: invalid usd", { usd });
      return;
    }

    const existing = await this.getOpenPosition();
    if (existing) {
      await this.log("info", "BUY skipped: position already open", { existing_id: existing.id, usd });
      return;
    }

    const price = this.getMarkPrice();
    if (!price) {
      await this.log("warn", "BUY skipped: no price available", { usd });
      return;
    }

    const qty = usd / price;
    const now = new Date().toISOString();

    const { error } = await this.sb.from("project_positions").insert({
      user_id: this.ctx.userId,
      project_id: this.ctx.projectId,
      symbol: this.ctx.symbol,
      side: "long",
      status: "open",
      qty,
      entry_price: price,
      entry_time: now,
      exit_price: null,
      exit_time: null,
      realized_pnl: null,
    });
    if (error) {
      // If multiple runners / concurrent BUY calls happen, DB uniqueness is the final guard.
      // Treat "already open" as a no-op instead of failing the whole run.
      // Postgres unique violation: 23505
      const anyErr: any = error;
      if (anyErr?.code === "23505") {
        await this.log("info", "BUY skipped: position already open (db constraint)", { usd, price, qty });
        return;
      }
      throw error;
    }

    await this.log("info", "BUY executed (paper)", { usd, price, qty });
  }

  /**
   * Paper sell: closes (or partially reduces) the current LONG position.
   * If no open position exists, it logs and does nothing.
   */
  async sell(args: SellArgs) {
    const pct = Number(args?.pct ?? 0);
    if (!Number.isFinite(pct) || pct <= 0) {
      await this.log("warn", "SELL skipped: invalid pct", { pct });
      return;
    }

    const pos = await this.getOpenPosition();
    if (!pos) {
      await this.log("info", "SELL skipped: no open position", { pct });
      return;
    }

    const price = this.getMarkPrice();
    if (!price) {
      await this.log("warn", "SELL skipped: no price available", { pct, position_id: pos.id });
      return;
    }

    const entryPrice = Number(pos.entry_price ?? 0);
    const qty = Number(pos.qty ?? 0);
    if (!Number.isFinite(entryPrice) || !Number.isFinite(qty) || qty <= 0) {
      await this.log("error", "SELL failed: invalid stored position", { position_id: pos.id });
      return;
    }

    const closeFrac = Math.min(1, pct / 100);
    const closeQty = qty * closeFrac;
    const remainingQty = qty - closeQty;
    const realized = (price - entryPrice) * closeQty;
    const now = new Date().toISOString();

    if (remainingQty <= 1e-12) {
      // Full close
      const { error } = await this.sb
        .from("project_positions")
        .update({
          status: "closed",
          exit_price: price,
          exit_time: now,
          realized_pnl: realized,
        })
        .eq("id", pos.id);
      if (error) throw error;

      await this.log("info", "SELL executed (paper, close)", {
        pct,
        price,
        qty_closed: closeQty,
        realized_pnl: realized,
        position_id: pos.id,
      });
      return;
    }

    // Partial close (keep position open with reduced qty)
    const prevRealized = Number(pos.realized_pnl ?? 0) || 0;
    const { error } = await this.sb
      .from("project_positions")
      .update({
        qty: remainingQty,
        exit_price: price,
        exit_time: now,
        realized_pnl: prevRealized + realized,
      })
      .eq("id", pos.id);
    if (error) throw error;

    await this.log("info", "SELL executed (paper, partial)", {
      pct,
      price,
      qty_closed: closeQty,
      qty_remaining: remainingQty,
      realized_pnl_add: realized,
      position_id: pos.id,
    });
  }
}
