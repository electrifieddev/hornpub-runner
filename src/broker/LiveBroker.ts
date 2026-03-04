import type { SupabaseClient } from "@supabase/supabase-js";
import type { KlineCache } from "../klines/KlineCache.js";

// ─── HMAC-SHA256 using Web Crypto API (Node 18+ native, no extra deps) ───────

async function hmacSha256Hex(secret: string, message: string): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(message));
  return Array.from(new Uint8Array(sig))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// ─── Types ────────────────────────────────────────────────────────────────────

type LogLevel = "info" | "warn" | "error";

export type LiveBrokerCtx = {
  userId:    string;
  projectId: string;
  runId:     string;
  symbol:    string;
  exchange:  string;
  tf:        string;
  apiKey:    string;
  apiSecret: string;
};

export type BuyArgs  = { usd: number };
export type SellArgs = { pct: number };

/**
 * Returned by buy() / sell() so the runner can build TRADE_EXECUTED logs
 * and attach trigger_log_id / executed_log_id to the trade row.
 */
export type TradeResult = {
  tradeId:       string | null;
  fillPrice:     number;
  filledQty:     number;
  fee:           number;
  feeAsset:      string;
  status:        "SUCCESS" | "REJECTED" | "PARTIAL";
  orderId:       string | null;
  positionAfter?: Record<string, any> | null;
  skipReason?:   string;
};

// ─── Binance API helpers ──────────────────────────────────────────────────────

const BINANCE_BASE       = "https://api.binance.com";
const REQUEST_TIMEOUT_MS = 15_000;
const FEE_RATE           = Number(process.env.BINANCE_FEE_RATE ?? "0.001");

interface BinanceOrderResponse {
  orderId:              number;
  clientOrderId:        string;
  symbol:               string;
  status:               string;
  side:                 "BUY" | "SELL";
  type:                 string;
  executedQty:          string;
  cummulativeQuoteQty:  string;
  fills: Array<{
    price:            string;
    qty:              string;
    commission:       string;
    commissionAsset:  string;
  }>;
  transactTime: number;
}

interface BinanceAccountBalance { asset: string; free: string; locked: string; }

interface BinanceSymbolInfo {
  symbol:     string;
  status:     string;
  baseAsset:  string;
  quoteAsset: string;
  filters: Array<{
    filterType:   string;
    minQty?:      string;
    maxQty?:      string;
    stepSize?:    string;
    minNotional?: string;
    notional?:    string;
    minPrice?:    string;
    maxPrice?:    string;
    tickSize?:    string;
  }>;
}

async function buildSignedUrl(path: string, params: Record<string, string | number>, secret: string): Promise<string> {
  const ts = Date.now();
  const qs = new URLSearchParams({
    ...Object.fromEntries(Object.entries(params).map(([k, v]) => [k, String(v)])),
    timestamp: String(ts),
  });
  const signature = await hmacSha256Hex(secret, qs.toString());
  qs.set("signature", signature);
  return `${BINANCE_BASE}${path}?${qs.toString()}`;
}

async function binanceFetch(url: string, opts: RequestInit = {}): Promise<any> {
  const controller = new AbortController();
  const timeoutId  = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(url, { ...opts, signal: controller.signal });
  } catch (err: any) {
    if (err?.name === "AbortError") throw new Error(`Binance request timed out after ${REQUEST_TIMEOUT_MS}ms`);
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
  const body = await res.json();
  if (!res.ok) {
    const msg = (body as any)?.msg ?? JSON.stringify(body);
    throw new Error(`Binance API error ${res.status}: ${msg}`);
  }
  return body;
}

// ─── LiveBroker ───────────────────────────────────────────────────────────────

export class LiveBroker {
  private sb:    SupabaseClient;
  private cache: KlineCache;
  private ctx:   LiveBrokerCtx;

  private symbolInfoCache: BinanceSymbolInfo | null = null;

  constructor(opts: { supabase: SupabaseClient; cache: KlineCache; ctx: LiveBrokerCtx }) {
    this.sb    = opts.supabase;
    this.cache = opts.cache;
    this.ctx   = opts.ctx;
  }

  // ─── Price ────────────────────────────────────────────────────────────────

  private getMarkPrice(): number | null {
    const candidates = ["1m", "3m", "5m", "15m", this.ctx.tf, "1h", "4h"];
    for (const tf of candidates) {
      const closes = this.cache.getCloses(this.ctx.exchange, this.ctx.symbol, tf);
      if (!closes || closes.length === 0) continue;
      const v = closes[closes.length - 1];
      const n = Number(v);
      if (Number.isFinite(n)) return n;
    }
    return null;
  }

  getMarkPricePublic(): number | null { return this.getMarkPrice(); }

  async getPosition(): Promise<any | null> { return this.getOpenPosition(); }

  // ─── Logging ──────────────────────────────────────────────────────────────

  /**
   * Insert a structured log row.
   * detail_json (if provided) is stored in the dedicated JSONB column.
   * Returns the inserted row ID for linking to trades.
   */
  async log(
    level:        LogLevel,
    message:      string,
    meta:         Record<string, any>         = {},
    detail_json?: Record<string, any> | null,
  ): Promise<string | null> {
    const payload: Record<string, any> = {
      user_id:    this.ctx.userId,
      project_id: this.ctx.projectId,
      level,
      message,
      meta: { ...meta, run_id: this.ctx.runId, symbol: this.ctx.symbol, exchange: this.ctx.exchange, mode: "live" },
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
    feeAsset?:      string;
    ts:             string;
    positionId?:    string | null;
    orderId?:       string | number | null;
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
        position_id:     args.positionId  ?? null,
        realized_pnl:    args.realizedPnl ?? 0,
        meta: {
          ...(args.meta ?? {}),
          mode:      "live",
          order_id:  args.orderId  ?? null,
          fee_asset: args.feeAsset ?? null,
        },
        trigger_log_id:  args.triggerLogId  ?? null,
        executed_log_id: args.executedLogId ?? null,
      })
      .select("id")
      .single();

    if (error) {
      await this.log("warn", "TRADE log failed", {
        side:  args.side,
        qty:   args.qty,
        price: args.price,
        error: String((error as any)?.message ?? error),
      });
      return null;
    }
    return (data as any)?.id ?? null;
  }

  /**
   * Attach trigger_log_id and/or executed_log_id to an existing trade row.
   */
  async attachLogIds(
    tradeId:       string,
    triggerLogId:  string | null,
    executedLogId: string | null,
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
      .eq("symbol",     this.ctx.symbol)
      .eq("status",     "open")
      .maybeSingle();
    if (error) throw error;
    return data as any | null;
  }

  // ─── Binance helpers ──────────────────────────────────────────────────────

  private async getSymbolInfo(): Promise<BinanceSymbolInfo> {
    if (this.symbolInfoCache) return this.symbolInfoCache;
    const url  = `${BINANCE_BASE}/api/v3/exchangeInfo?symbol=${this.ctx.symbol}`;
    const data = await binanceFetch(url, { headers: { "X-MBX-APIKEY": this.ctx.apiKey } });
    const info: BinanceSymbolInfo = data.symbols?.[0];
    if (!info) throw new Error(`Symbol ${this.ctx.symbol} not found in Binance exchange info`);
    this.symbolInfoCache = info;
    return info;
  }

  private async quantizeQty(qty: number): Promise<number> {
    const info      = await this.getSymbolInfo();
    const lotFilter = info.filters.find((f) => f.filterType === "LOT_SIZE");
    if (!lotFilter) return qty;
    const stepSize  = parseFloat(lotFilter.stepSize ?? "0");
    const minQty    = parseFloat(lotFilter.minQty   ?? "0");
    if (stepSize <= 0) return qty;
    const quantized = Math.floor(qty / stepSize) * stepSize;
    const decimals  = (lotFilter.stepSize ?? "").split(".")[1]?.replace(/0+$/, "").length ?? 8;
    const rounded   = parseFloat(quantized.toFixed(decimals));
    return rounded < minQty ? 0 : rounded;
  }

  private async getMinNotional(): Promise<number> {
    const info = await this.getSymbolInfo();
    const f    = info.filters.find((f) => f.filterType === "NOTIONAL") ??
                 info.filters.find((f) => f.filterType === "MIN_NOTIONAL");
    return parseFloat(f?.minNotional ?? f?.notional ?? "10");
  }

  private async getFreeBalance(asset: string): Promise<number> {
    const url      = await buildSignedUrl("/api/v3/account", {}, this.ctx.apiSecret);
    const data     = await binanceFetch(url, { headers: { "X-MBX-APIKEY": this.ctx.apiKey } });
    const balances: BinanceAccountBalance[] = data.balances ?? [];
    const bal      = balances.find((b) => b.asset === asset);
    return parseFloat(bal?.free ?? "0");
  }

  private async getBaseAsset(): Promise<string> {
    const info = await this.getSymbolInfo();
    return info.baseAsset;
  }

  private calcFillStats(fills: BinanceOrderResponse["fills"]): {
    avgPrice:  number;
    totalQty:  number;
    totalFee:  number;
    feeAsset:  string;
  } {
    let totalQty  = 0, totalCost = 0, totalFee = 0, feeAsset = "";
    for (const fill of fills) {
      const qty   = parseFloat(fill.qty);
      const price = parseFloat(fill.price);
      const fee   = parseFloat(fill.commission);
      totalQty  += qty;
      totalCost += qty * price;
      totalFee  += fee;
      feeAsset   = fill.commissionAsset;
    }
    return { avgPrice: totalQty > 0 ? totalCost / totalQty : 0, totalQty, totalFee, feeAsset };
  }

  private async placeBuyOrder(usd: number): Promise<BinanceOrderResponse> {
    const minNotional = await this.getMinNotional();
    if (usd < minNotional) throw new Error(`Buy amount $${usd.toFixed(2)} is below minimum notional $${minNotional}`);
    const url = await buildSignedUrl("/api/v3/order", {
      symbol: this.ctx.symbol, side: "BUY", type: "MARKET", quoteOrderQty: usd.toFixed(2),
    }, this.ctx.apiSecret);
    return binanceFetch(url, { method: "POST", headers: { "X-MBX-APIKEY": this.ctx.apiKey } });
  }

  private async placeSellOrder(qty: number): Promise<BinanceOrderResponse> {
    const quantized = await this.quantizeQty(qty);
    if (quantized <= 0) throw new Error(`Sell qty ${qty} rounds to zero after LOT_SIZE quantization`);
    const qtyStr = quantized.toFixed(8).replace(/\.?0+$/, "");
    const url    = await buildSignedUrl("/api/v3/order", {
      symbol: this.ctx.symbol, side: "SELL", type: "MARKET", quantity: qtyStr,
    }, this.ctx.apiSecret);
    return binanceFetch(url, { method: "POST", headers: { "X-MBX-APIKEY": this.ctx.apiKey } });
  }

  // ─── BUY ──────────────────────────────────────────────────────────────────

  async buy(args: BuyArgs): Promise<TradeResult> {
    const usd = Number(args?.usd ?? 0);
    if (!Number.isFinite(usd) || usd <= 0) {
      await this.log("warn", "BUY skipped: invalid usd", { usd });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "invalid usd" };
    }

    let usdtBalance: number;
    try {
      usdtBalance = await this.getFreeBalance("USDT");
    } catch (e: any) {
      await this.log("error", "BUY failed: could not fetch USDT balance", { error: e?.message });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: `balance fetch failed: ${e?.message}` };
    }

    if (usdtBalance < usd) {
      await this.log("warn", "BUY skipped: insufficient USDT balance", { required: usd, available: usdtBalance });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "insufficient USDT balance" };
    }

    let order: BinanceOrderResponse;
    try {
      order = await this.placeBuyOrder(usd);
    } catch (e: any) {
      await this.log("error", `BUY order failed: ${e?.message}`, { usd });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: e?.message };
    }

    const isPartial  = order.status === "PARTIALLY_FILLED";
    const tradeStatus: TradeResult["status"] = order.status === "FILLED" ? "SUCCESS" : isPartial ? "PARTIAL" : "REJECTED";

    const fills = order.fills ?? [];
    const { avgPrice, totalQty, totalFee, feeAsset } = this.calcFillStats(fills);
    const effectiveQty = totalQty;
    const fillPrice    = avgPrice || this.getMarkPrice() || 0;
    const now          = new Date(order.transactTime ?? Date.now()).toISOString();
    const feeUsd       = feeAsset === "USDT" ? totalFee : feeAsset === "BNB" ? totalFee : totalFee * fillPrice;
    const orderId      = String(order.orderId);

    const existing = await this.getOpenPosition();
    let tradeId:     string | null            = null;
    let positionAfter: Record<string, any> | null = null;

    if (existing) {
      const existingQty   = Number(existing.qty ?? 0);
      const existingEntry = Number(existing.entry_price ?? 0);
      if (!Number.isFinite(existingQty) || existingQty <= 0 || !Number.isFinite(existingEntry)) {
        await this.log("error", "BUY merge failed: invalid stored position", { position_id: existing.id });
        return { tradeId: null, fillPrice, filledQty: effectiveQty, fee: feeUsd, feeAsset, status: "REJECTED", orderId, skipReason: "invalid stored position" };
      }

      const totalQtyMerged = existingQty + effectiveQty;
      const avgEntry       = (existingQty * existingEntry + effectiveQty * fillPrice) / totalQtyMerged;
      const prevRealized   = Number(existing.realized_pnl ?? 0) || 0;

      const { error } = await this.sb
        .from("project_positions")
        .update({ qty: totalQtyMerged, entry_price: avgEntry, realized_pnl: prevRealized - feeUsd })
        .eq("id", existing.id);
      if (error) throw error;

      tradeId = await this.insertTrade({
        side: "buy", qty: effectiveQty, price: fillPrice, realizedPnl: 0,
        fee: feeUsd, feeAsset, ts: now, positionId: existing.id, orderId: order.orderId,
        meta: { usd, reason: "strategy", kind: "add_to_position", avg_entry: avgEntry, raw_fee: totalFee, binance_status: order.status },
      });

      positionAfter = { qty: totalQtyMerged, entry_price: avgEntry, position_id: existing.id };

    } else {
      const { data, error } = await this.sb
        .from("project_positions")
        .insert({
          user_id:     this.ctx.userId, project_id: this.ctx.projectId,
          symbol:      this.ctx.symbol, side: "long", status: "open",
          qty:         effectiveQty,    entry_price: fillPrice, entry_time: now,
          exit_price:  null,            exit_time:   null,      realized_pnl: -feeUsd,
        })
        .select("id")
        .maybeSingle();

      if (error) {
        const anyErr: any = error;
        if (anyErr?.code === "23505") {
          const racePos = await this.getOpenPosition();
          if (racePos) {
            const eQty = Number(racePos.qty ?? 0), eEntry = Number(racePos.entry_price ?? 0);
            if (Number.isFinite(eQty) && eQty > 0 && Number.isFinite(eEntry)) {
              const tQty   = eQty + effectiveQty;
              const avgEntry = (eQty * eEntry + effectiveQty * fillPrice) / tQty;
              const prevRaceRealized = Number(racePos.realized_pnl ?? 0) || 0;
              const { error: mergeErr } = await this.sb
                .from("project_positions")
                .update({ qty: tQty, entry_price: avgEntry, realized_pnl: prevRaceRealized - feeUsd })
                .eq("id", racePos.id);
              if (mergeErr) throw mergeErr;
              tradeId = await this.insertTrade({
                side: "buy", qty: effectiveQty, price: fillPrice, realizedPnl: 0,
                fee: feeUsd, feeAsset, ts: now, positionId: racePos.id, orderId: order.orderId,
                meta: { usd, kind: "add_to_position_race_merge", avg_entry: avgEntry },
              });
              positionAfter = { qty: tQty, entry_price: avgEntry, position_id: racePos.id };
              return { tradeId, fillPrice, filledQty: effectiveQty, fee: feeUsd, feeAsset, status: tradeStatus, orderId, positionAfter };
            }
          }
          await this.log("warn", "BUY: duplicate constraint but position unreadable on re-fetch", { usd, fill_price: fillPrice });
          return { tradeId: null, fillPrice, filledQty: 0, fee: feeUsd, feeAsset, status: "REJECTED", orderId, skipReason: "race condition / unreadable position" };
        }
        throw error;
      }

      const positionId = (data as any)?.id ?? null;
      tradeId = await this.insertTrade({
        side: "buy", qty: effectiveQty, price: fillPrice, realizedPnl: 0,
        fee: feeUsd, feeAsset, ts: now, positionId, orderId: order.orderId,
        meta: { usd, reason: "strategy", kind: "open_position", raw_fee: totalFee, binance_status: order.status },
      });

      positionAfter = { qty: effectiveQty, entry_price: fillPrice, position_id: positionId };
    }

    return { tradeId, fillPrice, filledQty: effectiveQty, fee: feeUsd, feeAsset, status: tradeStatus, orderId, positionAfter };
  }

  // ─── SELL ─────────────────────────────────────────────────────────────────

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

    const entryPrice = Number(pos.entry_price ?? 0);
    const posQty     = Number(pos.qty ?? 0);
    if (!Number.isFinite(entryPrice) || !Number.isFinite(posQty) || posQty <= 0) {
      await this.log("error", "SELL failed: invalid stored position", { position_id: pos.id });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "invalid stored position" };
    }

    const closeFrac      = Math.min(1, pct / 100);
    const intendedCloseQty = posQty * closeFrac;

    let baseAsset: string, availableQty: number;
    try {
      baseAsset    = await this.getBaseAsset();
      availableQty = await this.getFreeBalance(baseAsset);
    } catch (e: any) {
      await this.log("error", `SELL failed: could not fetch ${this.ctx.symbol} balance`, { error: e?.message });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: `balance fetch failed: ${e?.message}` };
    }

    const actualCloseQty = Math.min(intendedCloseQty, availableQty * 0.9999);
    const quantized      = await this.quantizeQty(actualCloseQty);
    if (quantized <= 0) {
      await this.log("warn", "SELL skipped: qty rounds to zero after quantization", {
        pct, intended_qty: intendedCloseQty, available: availableQty, position_id: pos.id,
      });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: "qty rounds to zero" };
    }

    let order: BinanceOrderResponse;
    try {
      order = await this.placeSellOrder(quantized);
    } catch (e: any) {
      await this.log("error", `SELL order failed: ${e?.message}`, { pct, qty: quantized, position_id: pos.id });
      return { tradeId: null, fillPrice: 0, filledQty: 0, fee: 0, feeAsset: "USDT", status: "REJECTED", orderId: null, skipReason: e?.message };
    }

    const isPartial    = order.status === "PARTIALLY_FILLED";
    const tradeStatus: TradeResult["status"] = order.status === "FILLED" ? "SUCCESS" : isPartial ? "PARTIAL" : "REJECTED";

    const fills = order.fills ?? [];
    const { avgPrice, totalQty: filledQty, totalFee, feeAsset } = this.calcFillStats(fills);
    const fillPrice    = avgPrice || this.getMarkPrice() || 0;
    const now          = new Date(order.transactTime ?? Date.now()).toISOString();
    const feeUsd       = feeAsset === "USDT" ? totalFee : totalFee * fillPrice;
    const grossPnl     = (fillPrice - entryPrice) * filledQty;
    const realized     = grossPnl - feeUsd;
    const remainingQty = posQty - filledQty;
    const prevRealized = Number(pos.realized_pnl ?? 0) || 0;
    const orderId      = String(order.orderId);

    let tradeId:     string | null            = null;
    let positionAfter: Record<string, any> | null = null;

    if (remainingQty <= 1e-8) {
      tradeId = await this.insertTrade({
        side: "sell", qty: filledQty, price: fillPrice, realizedPnl: realized,
        fee: feeUsd, feeAsset, ts: now, positionId: pos.id, orderId: order.orderId,
        meta: { pct, kind: "close", reason: "strategy", gross_pnl: grossPnl, raw_fee: totalFee, binance_status: order.status, exit_price: fillPrice, exit_time: now, realized_pnl: prevRealized + realized },
      });

      const { error } = await this.sb
        .from("project_positions")
        .delete()
        .eq("id", pos.id);
      if (error) throw error;

      positionAfter = { qty: 0, status: "closed", exit_price: fillPrice, position_id: pos.id, realized_pnl: prevRealized + realized };

    } else {
      const { error } = await this.sb
        .from("project_positions")
        .update({ qty: remainingQty, realized_pnl: prevRealized + realized })
        .eq("id", pos.id);
      if (error) throw error;

      tradeId = await this.insertTrade({
        side: "sell", qty: filledQty, price: fillPrice, realizedPnl: realized,
        fee: feeUsd, feeAsset, ts: now, positionId: pos.id, orderId: order.orderId,
        meta: { pct, kind: "partial", reason: "strategy", gross_pnl: grossPnl, raw_fee: totalFee, binance_status: order.status },
      });

      positionAfter = { qty: remainingQty, entry_price: entryPrice, position_id: pos.id };
    }

    return { tradeId, fillPrice, filledQty, fee: feeUsd, feeAsset, status: tradeStatus, orderId, positionAfter };
  }

  // ─── Connectivity check ───────────────────────────────────────────────────

  async testConnectivity(): Promise<{ ok: boolean; error?: string }> {
    try {
      const url  = await buildSignedUrl("/api/v3/account", {}, this.ctx.apiSecret);
      const data = await binanceFetch(url, { headers: { "X-MBX-APIKEY": this.ctx.apiKey } });
      if ((data as any)?.canTrade !== true) return { ok: false, error: "Binance account does not have trading permission" };
      return { ok: true };
    } catch (e: any) {
      return { ok: false, error: e?.message ?? String(e) };
    }
  }
}
