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
  userId: string;
  projectId: string;
  runId: string;
  symbol: string;
  exchange: string;
  tf: string;
  /** Binance API key (read from env or project settings) */
  apiKey: string;
  /** Binance API secret (read from env or project settings) */
  apiSecret: string;
};

export type BuyArgs = { usd: number };
export type SellArgs = { pct: number };

// ─── Binance API helpers ──────────────────────────────────────────────────────

const BINANCE_BASE = "https://api.binance.com";
const REQUEST_TIMEOUT_MS = 15_000;

/**
 * Binance trading fee for standard spot accounts.
 * Default maker/taker fee is 0.1%. With BNB discount it's 0.075%.
 * We use 0.1% as the safe default; operators can override via env.
 */
const FEE_RATE = Number(process.env.BINANCE_FEE_RATE ?? "0.001"); // 0.1%

interface BinanceOrderResponse {
  orderId: number;
  clientOrderId: string;
  symbol: string;
  status: string; // e.g. "FILLED", "PARTIALLY_FILLED", "NEW"
  side: "BUY" | "SELL";
  type: string;
  executedQty: string;
  cummulativeQuoteQty: string; // total quote (USD) spent/received
  fills: Array<{
    price: string;
    qty: string;
    commission: string;
    commissionAsset: string;
  }>;
  transactTime: number;
}

interface BinanceAccountBalance {
  asset: string;
  free: string;
  locked: string;
}

interface BinanceSymbolInfo {
  symbol: string;
  status: string;
  baseAsset: string;
  quoteAsset: string;
  filters: Array<{
    filterType: string;
    minQty?: string;
    maxQty?: string;
    stepSize?: string;
    minNotional?: string;
    notional?: string;
    minPrice?: string;
    maxPrice?: string;
    tickSize?: string;
  }>;
}

/** Build a signed URL for Binance private endpoints */
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
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
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
  private sb: SupabaseClient;
  private cache: KlineCache;
  private ctx: LiveBrokerCtx;

  // Cache exchange info to avoid hammering /exchangeInfo on every order
  private symbolInfoCache: BinanceSymbolInfo | null = null;

  constructor(opts: { supabase: SupabaseClient; cache: KlineCache; ctx: LiveBrokerCtx }) {
    this.sb = opts.supabase;
    this.cache = opts.cache;
    this.ctx = opts.ctx;
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

  // ─── Public position accessor ─────────────────────────────────────────────

  async getPosition(): Promise<any | null> {
    return this.getOpenPosition();
  }

  // ─── Logging ──────────────────────────────────────────────────────────────

  async log(level: LogLevel, message: string, meta: Record<string, any> = {}) {
    await this.sb.from("project_logs").insert({
      user_id: this.ctx.userId,
      project_id: this.ctx.projectId,
      level,
      message,
      meta: { ...meta, run_id: this.ctx.runId, symbol: this.ctx.symbol, exchange: this.ctx.exchange, mode: "live" },
    });
  }

  // ─── Trade record ─────────────────────────────────────────────────────────

  private async insertTrade(args: {
    side: "buy" | "sell";
    qty: number;
    price: number;
    realizedPnl?: number;
    fee?: number;
    feeAsset?: string;
    ts: string;
    positionId?: string | null;
    orderId?: string | number | null;
    meta?: Record<string, any>;
  }) {
    const { error } = await this.sb.from("project_trades").insert({
      user_id: this.ctx.userId,
      project_id: this.ctx.projectId,
      run_id: this.ctx.runId,
      symbol: this.ctx.symbol,
      side: args.side,
      qty: args.qty,
      price: args.price,
      fee: args.fee ?? 0,
      ts: args.ts,
      position_id: args.positionId ?? null,
      realized_pnl: args.realizedPnl ?? 0,
      meta: {
        ...(args.meta ?? {}),
        mode: "live",
        order_id: args.orderId ?? null,
        fee_asset: args.feeAsset ?? null,
      },
    });
    if (error) {
      await this.log("warn", "TRADE log failed", {
        side: args.side,
        qty: args.qty,
        price: args.price,
        error: String((error as any)?.message ?? error),
      });
    }
  }

  // ─── DB position helpers ──────────────────────────────────────────────────

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

  // ─── Binance helpers ──────────────────────────────────────────────────────

  /** Get LOT_SIZE filter (step size, min qty) and MIN_NOTIONAL for the symbol */
  private async getSymbolInfo(): Promise<BinanceSymbolInfo> {
    if (this.symbolInfoCache) return this.symbolInfoCache;

    const url = `${BINANCE_BASE}/api/v3/exchangeInfo?symbol=${this.ctx.symbol}`;
    const data = await binanceFetch(url, {
      headers: { "X-MBX-APIKEY": this.ctx.apiKey },
    });
    const info: BinanceSymbolInfo = data.symbols?.[0];
    if (!info) throw new Error(`Symbol ${this.ctx.symbol} not found in Binance exchange info`);
    this.symbolInfoCache = info;
    return info;
  }

  /** Round qty to Binance LOT_SIZE step and enforce min qty */
  private async quantizeQty(qty: number): Promise<number> {
    const info = await this.getSymbolInfo();
    const lotFilter = info.filters.find((f) => f.filterType === "LOT_SIZE");
    if (!lotFilter) return qty;

    const stepSize = parseFloat(lotFilter.stepSize ?? "0");
    const minQty = parseFloat(lotFilter.minQty ?? "0");

    if (stepSize <= 0) return qty;

    // Round down to nearest step
    const quantized = Math.floor(qty / stepSize) * stepSize;
    // Count decimal places in stepSize to avoid floating point noise
    const decimals = (lotFilter.stepSize ?? "").split(".")[1]?.replace(/0+$/, "").length ?? 8;
    const rounded = parseFloat(quantized.toFixed(decimals));

    return rounded < minQty ? 0 : rounded;
  }

  /** Get minimum notional value for this symbol */
  private async getMinNotional(): Promise<number> {
    const info = await this.getSymbolInfo();
    // Binance uses NOTIONAL or MIN_NOTIONAL filter
    const f =
      info.filters.find((f) => f.filterType === "NOTIONAL") ??
      info.filters.find((f) => f.filterType === "MIN_NOTIONAL");
    return parseFloat(f?.minNotional ?? f?.notional ?? "10");
  }

  /** Get free balance for a given asset */
  private async getFreeBalance(asset: string): Promise<number> {
    const url = await buildSignedUrl("/api/v3/account", {}, this.ctx.apiSecret);
    const data = await binanceFetch(url, {
      headers: { "X-MBX-APIKEY": this.ctx.apiKey },
    });
    const balances: BinanceAccountBalance[] = data.balances ?? [];
    const bal = balances.find((b) => b.asset === asset);
    return parseFloat(bal?.free ?? "0");
  }

  /** Get base asset name from symbol (e.g. BTCUSDT → BTC) */
  private async getBaseAsset(): Promise<string> {
    const info = await this.getSymbolInfo();
    return info.baseAsset;
  }

  /** Calculate weighted average fill price and total fees from order fills */
  private calcFillStats(fills: BinanceOrderResponse["fills"]): {
    avgPrice: number;
    totalQty: number;
    totalFee: number;
    feeAsset: string;
  } {
    let totalQty = 0;
    let totalCost = 0;
    let totalFee = 0;
    let feeAsset = "";

    for (const fill of fills) {
      const qty = parseFloat(fill.qty);
      const price = parseFloat(fill.price);
      const fee = parseFloat(fill.commission);
      totalQty += qty;
      totalCost += qty * price;
      totalFee += fee;
      feeAsset = fill.commissionAsset;
    }

    return {
      avgPrice: totalQty > 0 ? totalCost / totalQty : 0,
      totalQty,
      totalFee,
      feeAsset,
    };
  }

  /**
   * Place a MARKET BUY order on Binance using quoteOrderQty (USD amount).
   * This is the preferred method as Binance handles the qty calculation.
   */
  private async placeBuyOrder(usd: number): Promise<BinanceOrderResponse> {
    const minNotional = await this.getMinNotional();
    if (usd < minNotional) {
      throw new Error(`Buy amount $${usd.toFixed(2)} is below minimum notional $${minNotional}`);
    }

    // Use quoteOrderQty so Binance does the conversion and fills the exact USD amount
    const url = await buildSignedUrl(
      "/api/v3/order",
      {
        symbol: this.ctx.symbol,
        side: "BUY",
        type: "MARKET",
        quoteOrderQty: usd.toFixed(2),
      },
      this.ctx.apiSecret
    );

    return binanceFetch(url, {
      method: "POST",
      headers: { "X-MBX-APIKEY": this.ctx.apiKey },
    });
  }

  /**
   * Place a MARKET SELL order on Binance using quantity.
   * Fees are taken from the quote asset (USDT) on spot.
   */
  private async placeSellOrder(qty: number): Promise<BinanceOrderResponse> {
    const quantized = await this.quantizeQty(qty);
    if (quantized <= 0) {
      throw new Error(`Sell qty ${qty} rounds to zero after LOT_SIZE quantization`);
    }

    // Format qty precisely — avoid scientific notation
    const qtyStr = quantized.toFixed(8).replace(/\.?0+$/, "");

    const url = await buildSignedUrl(
      "/api/v3/order",
      {
        symbol: this.ctx.symbol,
        side: "SELL",
        type: "MARKET",
        quantity: qtyStr,
      },
      this.ctx.apiSecret
    );

    return binanceFetch(url, {
      method: "POST",
      headers: { "X-MBX-APIKEY": this.ctx.apiKey },
    });
  }

  // ─── BUY ──────────────────────────────────────────────────────────────────

  /**
   * Live buy: executes a real MARKET BUY on Binance, then records the fill in Supabase.
   *
   * Fee handling:
   *   - On Binance spot, the fee for a BUY is deducted from the BASE asset received
   *     (e.g. buying BTCUSDT: you get slightly less BTC due to the 0.1% fee).
   *   - We store the actual filled qty (post-fee) from the order fills.
   *   - We record fee separately so PnL calculations are accurate.
   *
   * Position merging works identically to PaperBroker (VWAP).
   */
  async buy(args: BuyArgs) {
    const usd = Number(args?.usd ?? 0);
    if (!Number.isFinite(usd) || usd <= 0) {
      await this.log("warn", "BUY skipped: invalid usd", { usd });
      return;
    }

    // Pre-flight: check USDT balance
    let usdtBalance: number;
    try {
      usdtBalance = await this.getFreeBalance("USDT");
    } catch (e: any) {
      await this.log("error", "BUY failed: could not fetch USDT balance", { error: e?.message });
      return;
    }

    if (usdtBalance < usd) {
      await this.log("warn", "BUY skipped: insufficient USDT balance", {
        required: usd,
        available: usdtBalance,
      });
      return;
    }

    // Execute the order
    let order: BinanceOrderResponse;
    try {
      order = await this.placeBuyOrder(usd);
    } catch (e: any) {
      await this.log("error", `BUY order failed: ${e?.message}`, { usd });
      return;
    }

    if (order.status !== "FILLED") {
      await this.log("warn", `BUY order not immediately filled (status: ${order.status})`, {
        order_id: order.orderId,
        usd,
      });
      // We still proceed with best-effort fill data
    }

    const fills = order.fills ?? [];
    const { avgPrice, totalQty, totalFee, feeAsset } = this.calcFillStats(fills);

    // Effective qty after fee (if fee was taken in base asset, totalQty already reflects it)
    // When BNB is the fee asset, totalQty is the gross qty and we need to note the BNB fee separately.
    // In all cases, totalQty from fills is what we actually received.
    const effectiveQty = totalQty;
    const fillPrice = avgPrice || this.getMarkPrice() || 0;

    const now = new Date(order.transactTime ?? Date.now()).toISOString();

    // Fee in USD terms (approximate for reporting)
    const feeUsd = feeAsset === "USDT"
      ? totalFee
      : feeAsset === "BNB"
      ? totalFee // BNB fee — no BNB/USD price available, store raw BNB amount
      : totalFee * fillPrice; // Base asset fee: qty * price

    const existing = await this.getOpenPosition();

    if (existing) {
      const existingQty = Number(existing.qty ?? 0);
      const existingEntry = Number(existing.entry_price ?? 0);

      if (!Number.isFinite(existingQty) || existingQty <= 0 || !Number.isFinite(existingEntry)) {
        await this.log("error", "BUY merge failed: invalid stored position", { position_id: existing.id });
        return;
      }

      const totalQtyMerged = existingQty + effectiveQty;
      const avgEntry = (existingQty * existingEntry + effectiveQty * fillPrice) / totalQtyMerged;

      const { error } = await this.sb
        .from("project_positions")
        .update({ qty: totalQtyMerged, entry_price: avgEntry })
        .eq("id", existing.id);
      if (error) throw error;

      await this.insertTrade({
        side: "buy",
        qty: effectiveQty,
        price: fillPrice,
        realizedPnl: 0,
        fee: feeUsd,
        feeAsset,
        ts: now,
        positionId: existing.id,
        orderId: order.orderId,
        meta: {
          usd,
          reason: "strategy",
          kind: "add_to_position",
          avg_entry: avgEntry,
          raw_fee: totalFee,
          binance_status: order.status,
        },
      });

      await this.log("info", "BUY executed (live, added to position)", {
        usd,
        fill_price: fillPrice,
        qty: effectiveQty,
        total_qty: totalQtyMerged,
        avg_entry: avgEntry,
        fee: totalFee,
        fee_asset: feeAsset,
        order_id: order.orderId,
        position_id: existing.id,
      });
      return;
    }

    // Open a fresh position
    const { data, error } = await this.sb
      .from("project_positions")
      .insert({
        user_id: this.ctx.userId,
        project_id: this.ctx.projectId,
        symbol: this.ctx.symbol,
        side: "long",
        status: "open",
        qty: effectiveQty,
        entry_price: fillPrice,
        entry_time: now,
        exit_price: null,
        exit_time: null,
        realized_pnl: null,
      })
      .select("id")
      .maybeSingle();

    if (error) {
      // Handle race condition (same as PaperBroker)
      const anyErr: any = error;
      if (anyErr?.code === "23505") {
        const racePos = await this.getOpenPosition();
        if (racePos) {
          const existingQty = Number(racePos.qty ?? 0);
          const existingEntry = Number(racePos.entry_price ?? 0);
          if (Number.isFinite(existingQty) && existingQty > 0 && Number.isFinite(existingEntry)) {
            const totalQtyMerged = existingQty + effectiveQty;
            const avgEntry = (existingQty * existingEntry + effectiveQty * fillPrice) / totalQtyMerged;
            const { error: mergeErr } = await this.sb
              .from("project_positions")
              .update({ qty: totalQtyMerged, entry_price: avgEntry })
              .eq("id", racePos.id);
            if (mergeErr) throw mergeErr;
            await this.insertTrade({
              side: "buy",
              qty: effectiveQty,
              price: fillPrice,
              realizedPnl: 0,
              fee: feeUsd,
              feeAsset,
              ts: now,
              positionId: racePos.id,
              orderId: order.orderId,
              meta: { usd, kind: "add_to_position_race_merge", avg_entry: avgEntry },
            });
            await this.log("info", "BUY executed (live, merged into race-concurrent position)", {
              usd, fill_price: fillPrice, qty: effectiveQty, avg_entry: avgEntry, order_id: order.orderId,
            });
            return;
          }
        }
        await this.log("warn", "BUY: duplicate constraint but position unreadable on re-fetch", { usd, fill_price: fillPrice });
        return;
      }
      throw error;
    }

    await this.insertTrade({
      side: "buy",
      qty: effectiveQty,
      price: fillPrice,
      realizedPnl: 0,
      fee: feeUsd,
      feeAsset,
      ts: now,
      positionId: (data as any)?.id ?? null,
      orderId: order.orderId,
      meta: { usd, reason: "strategy", kind: "open_position", raw_fee: totalFee, binance_status: order.status },
    });

    await this.log("info", "BUY executed (live, new position)", {
      usd,
      fill_price: fillPrice,
      qty: effectiveQty,
      fee: totalFee,
      fee_asset: feeAsset,
      order_id: order.orderId,
    });
  }

  // ─── SELL ─────────────────────────────────────────────────────────────────

  /**
   * Live sell: executes a real MARKET SELL on Binance, then updates the Supabase position.
   *
   * Fee handling:
   *   - On Binance spot, the fee for a SELL is deducted from the QUOTE asset (USDT) received.
   *   - We fetch the actual base-asset balance to ensure we don't try to sell more than we hold.
   *   - pct is applied to the position's recorded qty, but capped by available balance.
   *
   * The realized PnL is calculated net of fees:
   *   realizedPnl = (exitPrice - entryPrice) * closeQty - totalFee (in USD)
   */
  async sell(args: SellArgs) {
    const pct = Number(args?.pct ?? 0);
    if (!Number.isFinite(pct) || pct <= 0) {
      await this.log("warn", "SELL skipped: invalid pct", { pct });
      return;
    }

    const pos = await this.getOpenPosition();
    if (!pos) {
      await this.log("warn", "SELL skipped: no open position", { pct });
      return;
    }

    const entryPrice = Number(pos.entry_price ?? 0);
    const posQty = Number(pos.qty ?? 0);
    if (!Number.isFinite(entryPrice) || !Number.isFinite(posQty) || posQty <= 0) {
      await this.log("error", "SELL failed: invalid stored position", { position_id: pos.id });
      return;
    }

    const closeFrac = Math.min(1, pct / 100);
    const intendedCloseQty = posQty * closeFrac;

    // Fetch actual base asset balance to avoid over-selling
    // (e.g. if a prior fill deducted fee from base asset and qty drifted)
    let baseAsset: string;
    let availableQty: number;
    try {
      baseAsset = await this.getBaseAsset();
      availableQty = await this.getFreeBalance(baseAsset);
    } catch (e: any) {
      await this.log("error", `SELL failed: could not fetch ${this.ctx.symbol} balance`, { error: e?.message });
      return;
    }

    // Cap to available balance (subtract small buffer to handle rounding)
    const actualCloseQty = Math.min(intendedCloseQty, availableQty * 0.9999);

    const quantized = await this.quantizeQty(actualCloseQty);
    if (quantized <= 0) {
      await this.log("warn", "SELL skipped: qty rounds to zero after quantization", {
        pct,
        intended_qty: intendedCloseQty,
        available: availableQty,
        position_id: pos.id,
      });
      return;
    }

    // Execute the order
    let order: BinanceOrderResponse;
    try {
      order = await this.placeSellOrder(quantized);
    } catch (e: any) {
      await this.log("error", `SELL order failed: ${e?.message}`, { pct, qty: quantized, position_id: pos.id });
      return;
    }

    const fills = order.fills ?? [];
    const { avgPrice, totalQty: filledQty, totalFee, feeAsset } = this.calcFillStats(fills);
    const fillPrice = avgPrice || this.getMarkPrice() || 0;
    const now = new Date(order.transactTime ?? Date.now()).toISOString();

    // Fee in USD (SELL fees are typically paid in USDT)
    const feeUsd = feeAsset === "USDT" ? totalFee : totalFee * fillPrice;

    // Realized PnL net of fees
    const grossPnl = (fillPrice - entryPrice) * filledQty;
    const realized = grossPnl - feeUsd;

    const remainingQty = posQty - filledQty;

    if (remainingQty <= 1e-8) {
      // Full close
      const { error } = await this.sb
        .from("project_positions")
        .update({
          status: "closed",
          exit_price: fillPrice,
          exit_time: now,
          realized_pnl: realized,
        })
        .eq("id", pos.id);
      if (error) throw error;

      await this.insertTrade({
        side: "sell",
        qty: filledQty,
        price: fillPrice,
        realizedPnl: realized,
        fee: feeUsd,
        feeAsset,
        ts: now,
        positionId: pos.id,
        orderId: order.orderId,
        meta: {
          pct,
          kind: "close",
          reason: "strategy",
          gross_pnl: grossPnl,
          raw_fee: totalFee,
          binance_status: order.status,
        },
      });

      await this.log("info", "SELL executed (live, close)", {
        pct,
        fill_price: fillPrice,
        qty_closed: filledQty,
        gross_pnl: grossPnl,
        fee: totalFee,
        fee_asset: feeAsset,
        realized_pnl: realized,
        order_id: order.orderId,
        position_id: pos.id,
      });
      return;
    }

    // Partial close
    const prevRealized = Number(pos.realized_pnl ?? 0) || 0;
    const { error } = await this.sb
      .from("project_positions")
      .update({
        qty: remainingQty,
        realized_pnl: prevRealized + realized,
      })
      .eq("id", pos.id);
    if (error) throw error;

    await this.insertTrade({
      side: "sell",
      qty: filledQty,
      price: fillPrice,
      realizedPnl: realized,
      fee: feeUsd,
      feeAsset,
      ts: now,
      positionId: pos.id,
      orderId: order.orderId,
      meta: {
        pct,
        kind: "partial",
        reason: "strategy",
        gross_pnl: grossPnl,
        raw_fee: totalFee,
        binance_status: order.status,
      },
    });

    await this.log("info", "SELL executed (live, partial)", {
      pct,
      fill_price: fillPrice,
      qty_closed: filledQty,
      qty_remaining: remainingQty,
      gross_pnl: grossPnl,
      fee: totalFee,
      fee_asset: feeAsset,
      realized_pnl_add: realized,
      order_id: order.orderId,
      position_id: pos.id,
    });
  }

  // ─── Connectivity check ───────────────────────────────────────────────────

  /**
   * Validate that the API keys are working and have SPOT trading permission.
   * Called at project start to fail fast with a clear error message.
   */
  async testConnectivity(): Promise<{ ok: boolean; error?: string }> {
    try {
      const url = await buildSignedUrl("/api/v3/account", {}, this.ctx.apiSecret);
      const data = await binanceFetch(url, {
        headers: { "X-MBX-APIKEY": this.ctx.apiKey },
      });
      const canTrade = (data as any)?.canTrade === true;
      if (!canTrade) {
        return { ok: false, error: "Binance account does not have trading permission" };
      }
      return { ok: true };
    } catch (e: any) {
      return { ok: false, error: e?.message ?? String(e) };
    }
  }
}
