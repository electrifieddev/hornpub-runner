import type { SupabaseClient } from "@supabase/supabase-js";
import type { KlineCache } from "../klines/KlineCache.js";

type LogLevel = "info" | "warn" | "error";

export type PaperBrokerCtx = {
  userId: string;
  projectId: string;
  runId: string;
  symbol: string;
  exchange: string;
  tf: string;
};

export type BuyArgs = { usd: number };
export type SellArgs = { pct: number };

/**
 * Returned by buy() / sell() so callers can build TRADE_EXECUTED logs
 * and attach trigger_log_id / executed_log_id to the trade row.
 */
export type TradeResult = {
  tradeId:    string | null;
  fillPrice:  number;
  filledQty:  number;
  fee:        number;
  feeAsset:   string;
  status:     "SUCCESS" | "REJECTED" | "PARTIAL";
  orderId:    string | null;
  /** Position snapshot captured AFTER the trade was recorded */
  positionAfter?: Record<string, any> | null;
  /** Non-null if the trade was skipped / failed before reaching the exchange */
  skipReason?: string;
};

export class PaperBroker {
  private sb:    SupabaseClient;
  private cache: KlineCache;
  private ctx:   PaperBrokerCtx;

  constructor(opts: { supabase: SupabaseClient; cache: KlineCache; ctx: PaperBrokerCtx }) {
    this.sb    = opts.supabase;
    this.cache = opts.cache;
    this.ctx   = opts.ctx;
  }

  // ─── Price ────────────────────────────────────────────────────────────────

  private getMarkPrice(): number | null {
    const candidates = [this.ctx.tf, "1m", "5m", "15m", "1h", "4h"];
    for (const tf of candidates) {
      const closes = this.cache.getCloses(this.ctx.exchange, this.ctx.symbol, tf);
      if (!closes || closes.length === 0) continue;
      const v = closes[closes.length - 1];
      const n = Number(v);
      if (Number.isFinite(n)) return n;
    }
    return null;
  }

  getMarkPricePublic(): number | null {
    return this.getMarkPrice();
  }

  async getPosition(): Promise<any | null> {
    return this.getOpenPosition();
  }

  // ─── Logging ──────────────────────────────────────────────────────────────

  /**
   * Insert a log row.
   * If detail_json is provided it is stored in the new column.
   * Returns the inserted row ID so callers can link it to a trade.
   */
  async log(
    level:       LogLevel,
    message:     string,
    meta:        Record<string, any> = {},
    detail_json?: Record<string, any> | null,
  ): Promise<string | null> {
    const payload: Record<string, any> = {
      user_id:    this.ctx.userId,
      project_id: this.ctx.projectId,
      level,
      message,
      meta: { ...meta, run_id: this.ctx.runId, symbol: this.ctx.symbol, exchange: this.ctx.exchange },
    };
    if (detail_json !== undefined) {
      payload.detail_json = detail_json;
    }

    const { data, error } = await this.sb
      .from("project_logs")
      .insert(payload)
      .select("id")
      .single();

    if (error) {
      console.error("project_logs insert error:", error.message);
      return null;
    }
    return (data as any)?.id ?? null;
  }

  // ─── Trade record ─────────────────────────────────────────────────────────

  private async insertTrade(args: {
    side:           "buy" | "sell";
    qty:            number;
    price:          number;
    realizedPnl?:   number;
    fee?:           number;
    ts:             string;
    positionId?:    string | null;
    meta?:          Record<string, any>;
    triggerLogId?:  string | null;
    executedLogId?: string | null;
  }): Promise<string | null> {
    const { data, error } = await this.sb
      .from("project_trades")
      .insert({
        user_id:         this.ctx.userId,
        project_id:      this.ctx.projectId,
        run_id:          this.ctx.runId,
        symbol:          this.ctx.symbol,
        side:            args.side,
        qty:             args.qty,
        price:           args.price,
        fee:             args.fee ?? 0,
        ts:              args.ts,
        position_id:     args.positionId ?? null,
        realized_pnl:    args.realizedPnl ?? 0,
        meta:            { ...(args.meta ?? {}), mode: "paper" },
        trigger_log_id:  args.triggerLogId  ?? null,
        executed_log_id: args.executedLogId ?? null,
      })
      .select("id")
      .single();

    if (error) {
      await this.log("warn", "TRADE log failed", {
        side:         args.side,
        qty:          args.qty,
        price:        args.price,
        realized_pnl: args.realizedPnl ?? 0,
        error:        String((error as any)?.message ?? error),
      });
      return null;
    }
    return (data as any)?.id ?? null;
  }

  /**
   * Attach trigger_log_id and/or executed_log_id to an existing trade row.
   * Called by the runner after both logs have been inserted.
   */
  async attachLogIds(
    tradeId:        string,
    triggerLogId:   string | null,
    executedLogId:  string | null,
  ): Promise<void> {
    const update: Record<string, any> = {};
    if (triggerLogId  !== null) update.trigger_log_id  = triggerLogId;
    if (executedLogId !== null) update.executed_log_id = executedLogId;
    if (Object.keys(update).length === 0) return;
    await this.sb.from("project_trades").update(update).eq("id", tradeId);
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

  // ─── BUY ──────────────────────────────────────────────────────────────────

  /**
   * Paper buy: opens or merges into a LONG position.
   * Returns a TradeResult so the runner can build TRADE_EXECUTED detail_json.
   */
  async buy(args: BuyArgs): Promise<TradeResult> {
    const usd = Number(args?.usd ?? 0);
    if (!Number.isFinite(usd) || usd <= 0) {
      await this.log("warn", "BUY skipped: invalid usd", { usd });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "invalid usd" };
    }

    const price = this.getMarkPrice();
    if (!price) {
      await this.log("warn", "BUY skipped: no price available", { usd });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "no price available" };
    }

    const newQty = usd / price;
    const now    = new Date().toISOString();
    const existing = await this.getOpenPosition();

    let tradeId: string | null = null;
    let positionAfter: Record<string, any> | null = null;

    if (existing) {
      const existingQty   = Number(existing.qty ?? 0);
      const existingEntry = Number(existing.entry_price ?? 0);

      if (!Number.isFinite(existingQty) || existingQty <= 0 || !Number.isFinite(existingEntry)) {
        await this.log("error", "BUY merge failed: invalid stored position", { position_id: existing.id });
        return { tradeId: null, fillPrice: price, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "invalid stored position" };
      }

      const totalQty = existingQty + newQty;
      const avgEntry = (existingQty * existingEntry + newQty * price) / totalQty;

      const { error } = await this.sb
        .from("project_positions")
        .update({ qty: totalQty, entry_price: avgEntry })
        .eq("id", existing.id);
      if (error) throw error;

      tradeId = await this.insertTrade({
        side: "buy", qty: newQty, price,
        realizedPnl: 0, fee: 0, ts: now,
        positionId: existing.id,
        meta: { usd, reason: "strategy", kind: "add_to_position", avg_entry: avgEntry },
      });

      positionAfter = { qty: totalQty, entry_price: avgEntry, position_id: existing.id };


    } else {
      const { data, error } = await this.sb
        .from("project_positions")
        .insert({
          user_id:     this.ctx.userId,
          project_id:  this.ctx.projectId,
          symbol:      this.ctx.symbol,
          side:        "long",
          status:      "open",
          qty:         newQty,
          entry_price: price,
          entry_time:  now,
          exit_price:  null,
          exit_time:   null,
          realized_pnl: null,
        })
        .select("id")
        .maybeSingle();

      if (error) {
        const anyErr: any = error;
        if (anyErr?.code === "23505") {
          const racePos = await this.getOpenPosition();
          if (racePos) {
            const eQty   = Number(racePos.qty ?? 0);
            const eEntry = Number(racePos.entry_price ?? 0);
            if (Number.isFinite(eQty) && eQty > 0 && Number.isFinite(eEntry)) {
              const totalQty = eQty + newQty;
              const avgEntry = (eQty * eEntry + newQty * price) / totalQty;
              const { error: mergeErr } = await this.sb
                .from("project_positions")
                .update({ qty: totalQty, entry_price: avgEntry })
                .eq("id", racePos.id);
              if (mergeErr) throw mergeErr;
              tradeId = await this.insertTrade({
                side: "buy", qty: newQty, price,
                realizedPnl: 0, fee: 0, ts: now,
                positionId: racePos.id,
                meta: { usd, reason: "strategy", kind: "add_to_position_race_merge", avg_entry: avgEntry },
              });
              positionAfter = { qty: totalQty, entry_price: avgEntry, position_id: racePos.id };
              return { tradeId, fillPrice: price, filledQty: newQty, fee: 0, feeAsset: "USDT", status: "SUCCESS", orderId: null, positionAfter };
            }
          }
          await this.log("warn", "BUY skipped: duplicate constraint hit but position unreadable on re-fetch", { usd, price, qty: newQty });
          return { tradeId: null, fillPrice: price, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "duplicate constraint / race" };
        }
        throw error;
      }

      const positionId = (data as any)?.id ?? null;
      tradeId = await this.insertTrade({
        side: "buy", qty: newQty, price,
        realizedPnl: 0, fee: 0, ts: now,
        positionId,
        meta: { usd, reason: "strategy", kind: "open_position" },
      });

      positionAfter = { qty: newQty, entry_price: price, position_id: positionId };

    }

    return { tradeId, fillPrice: price, filledQty: newQty, fee: 0, feeAsset: "USDT", status: "SUCCESS", orderId: null, positionAfter };
  }

  // ─── SELL ─────────────────────────────────────────────────────────────────

  /**
   * Paper sell: closes or partially reduces the LONG position.
   * Returns a TradeResult so the runner can build TRADE_EXECUTED detail_json.
   */
  async sell(args: SellArgs): Promise<TradeResult> {
    const pct = Number(args?.pct ?? 0);
    if (!Number.isFinite(pct) || pct <= 0) {
      await this.log("warn", "SELL skipped: invalid pct", { pct });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "invalid pct" };
    }

    const pos = await this.getOpenPosition();
    if (!pos) {
      await this.log("warn", "SELL skipped: no open position", { pct });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "no open position" };
    }

    const price = this.getMarkPrice();
    if (!price) {
      await this.log("warn", "SELL skipped: no price available", { pct, position_id: pos.id });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "no price available" };
    }

    const entryPrice = Number(pos.entry_price ?? 0);
    const qty        = Number(pos.qty ?? 0);
    if (!Number.isFinite(entryPrice) || !Number.isFinite(qty) || qty <= 0) {
      await this.log("error", "SELL failed: invalid stored position", { position_id: pos.id });
      return { tradeId: null, fillPrice: price, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "invalid stored position" };
    }

    const closeFrac    = Math.min(1, pct / 100);
    const closeQty     = qty * closeFrac;
    const remainingQty = qty - closeQty;
    const realized     = (price - entryPrice) * closeQty;
    const now          = new Date().toISOString();

    let tradeId: string | null = null;
    let positionAfter: Record<string, any> | null = null;

    if (remainingQty <= 1e-12) {
      const { error } = await this.sb
        .from("project_positions")
        .delete()
        .eq("id", pos.id);
      if (error) throw error;

      tradeId = await this.insertTrade({
        side: "sell", qty: closeQty, price,
        realizedPnl: realized, fee: 0, ts: now,
        positionId: pos.id,
        meta: { pct, kind: "close", reason: "strategy", exit_price: price, exit_time: now, realized_pnl: realized },
      });

      positionAfter = { qty: 0, status: "closed", exit_price: price, position_id: pos.id };
    } else {
      const prevRealized = Number(pos.realized_pnl ?? 0) || 0;
      const { error } = await this.sb
        .from("project_positions")
        .update({ qty: remainingQty, realized_pnl: prevRealized + realized })
        .eq("id", pos.id);
      if (error) throw error;

      tradeId = await this.insertTrade({
        side: "sell", qty: closeQty, price,
        realizedPnl: realized, fee: 0, ts: now,
        positionId: pos.id,
        meta: { pct, kind: "partial", reason: "strategy" },
      });

      positionAfter = { qty: remainingQty, entry_price: entryPrice, position_id: pos.id };
    }

    return { tradeId, fillPrice: price, filledQty: closeQty, fee: 0, feeAsset: "USDT", status: "SUCCESS", orderId: null, positionAfter };
  }
}
