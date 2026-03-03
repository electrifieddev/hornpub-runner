/**
 * src/index.ts  (hornpub-runner)
 *
 * Changes in this revision:
 *  - log() now returns the inserted log row ID (string | null).
 *  - HP.buy / HP.sell capture a trigger snapshot at decision-time (exact
 *    mark price + position state at call time) and emit two structured logs:
 *      1) TRADE_TRIGGER  (detail_json.kind = "trade_trigger")
 *      2) TRADE_EXECUTED (detail_json.kind = "trade_executed")
 *    Both log IDs are stored on the project_trades row via trigger_log_id /
 *    executed_log_id.  This is fully backwards-compatible — old trades that
 *    have no log IDs continue to render without those columns.
 *  - Slippage is computed immediately after execution using the trigger price
 *    that was captured before the order was placed.
 *  - No indicators are re-evaluated; the snapshot is captured from broker
 *    state (mark price, position cache) at the moment HP.buy/sell is invoked.
 *
 * All other logic is unchanged from the previous revision.
 */

import { createClient, SupabaseClient } from "@supabase/supabase-js";
import { KlineManager }       from "./klines/KlineManager.js";
import { SupabaseKlineStore } from "./klines/SupabaseKlineStore.js";
import { KlineCache }         from "./klines/KlineCache.js";
import createIndicators       from "./indicators/createIndicators.js";
import { runInSandbox }       from "./engine/Sandbox.js";
import { PaperBroker }        from "./broker/PaperBroker.js";
import { LiveBroker }         from "./broker/LiveBroker.js";
import { decryptString, isCiphertext } from "./encryption.js";

// ─── Condition row types ──────────────────────────────────────────────────────

/**
 * A single evaluated condition row stored inside detail_json.rows[].
 * Matches the table format shown in the trade detail modal:
 *   Condition | Value | Rule | Result
 */
export type ConditionRow = {
  condition: string;
  value:     string;
  rule:      string;
  result:    boolean;
};

/**
 * Per-trade accumulator.  Instantiate one at the start of each symbol run,
 * push rows as conditions are evaluated, then snapshot when HP.buy/sell fires.
 *
 * Rows are keyed by condition label so that repeated indicator calls (e.g.
 * EMA called multiple times) overwrite rather than duplicate.
 */
class RowsCollector {
  private _map = new Map<string, ConditionRow>();

  push(row: ConditionRow): void {
    this._map.set(row.condition, row);
  }

  snapshot(): ConditionRow[] {
    return [...this._map.values()];
  }

  clear(): void {
    this._map.clear();
  }
}

// ─── Settings helpers ─────────────────────────────────────────────────────────

type TradeHours      = { start: string; end: string };
type ProjectSettings = {
  mode?:               string;
  wallet_id?:          string;
  trade_hours?:        TradeHours;
  disable_weekends?:   boolean;
  max_trades_per_day?: number;
  advanced_logging?:   boolean;
};

function isWithinTradeHours(hours: TradeHours | undefined): boolean {
  if (!hours?.start || !hours?.end) return true;
  const now  = new Date();
  const hhmm = (h: string) => {
    const [hh, mm] = h.split(":").map(Number);
    return (hh ?? 0) * 60 + (mm ?? 0);
  };
  const nowMins   = now.getUTCHours() * 60 + now.getUTCMinutes();
  const startMins = hhmm(hours.start);
  const endMins   = hhmm(hours.end);
  if (startMins <= endMins) return nowMins >= startMins && nowMins < endMins;
  return nowMins >= startMins || nowMins < endMins;
}

function isWeekend(): boolean {
  return [0, 6].includes(new Date().getUTCDay());
}

async function countTradesToday(supabase: SupabaseClient, projectId: string, symbol: string): Promise<number> {
  const start = new Date();
  start.setUTCHours(0, 0, 0, 0);
  const { count } = await supabase
    .from("project_trades")
    .select("id", { count: "exact", head: true })
    .eq("project_id", projectId)
    .eq("symbol", symbol)
    .eq("side", "buy")
    .gte("ts", start.toISOString());
  return count ?? 0;
}

// ─── Supabase client ──────────────────────────────────────────────────────────

const SUPABASE_URL              = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ─── Global kline cache ───────────────────────────────────────────────────────

const klineCache = new KlineCache({ supabase, table: "market_klines" });
const crossPrev  = new Map<string, { a: number; b: number }>();

function safeNumber(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : Number.NaN;
}

async function getLastTradeTs(projectId: string, symbol: string): Promise<string | null> {
  const { data, error } = await supabase
    .from("project_trades")
    .select("ts")
    .eq("project_id", projectId)
    .eq("symbol", symbol)
    .order("ts", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) return null;
  return (data as any)?.ts ?? null;
}

function barsSinceTimestamp(exchange: string, symbol: string, tf: string, tsIso: string | null): number {
  if (!tsIso) return Number.POSITIVE_INFINITY;
  const tsMs = Date.parse(tsIso);
  if (!Number.isFinite(tsMs)) return Number.POSITIVE_INFINITY;
  const series = klineCache.getSeries(exchange, symbol, tf);
  if (!series || series.openTimes.length === 0) return Number.POSITIVE_INFINITY;
  const openTimes = series.openTimes;
  let idx = 0;
  while (idx < openTimes.length && openTimes[idx]! < tsMs) idx++;
  const lastIdx = openTimes.length - 1;
  return Math.max(0, lastIdx - Math.min(idx, lastIdx));
}

function extractTimeframesFromCode(code: string): string[] {
  const manifestMatch = /^\/\/\s*@hornpub-timeframes:\s*([^\n]+)/m.exec(code);
  if (manifestMatch) {
    const tfs = manifestMatch[1].split(",").map((s) => s.trim()).filter(Boolean);
    if (tfs.length > 0) return tfs;
  }
  const out = new Set<string>();
  const re  = /\btf\s*:\s*["'']([^"'']+)["']/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(code))) out.add(m[1]);
  if (out.size === 0) out.add("1m");
  return [...out];
}

type Project = {
  id:               string;
  owner_id:         string;
  generated_js:     string | null;
  interval_seconds: number;
  mode?:            string;
  settings_json?:   Record<string, any>;
};

// ─── Log helper ───────────────────────────────────────────────────────────────

/**
 * Insert a project log row.
 * Returns the inserted log ID (used to link trigger/execution logs to trades).
 */
async function log(
  projectId:    string,
  ownerId:      string,
  level:        string,
  message:      string,
  meta:         Record<string, any>         = {},
  detail_json?: Record<string, any> | null,
): Promise<string | null> {
  const payload: Record<string, any> = {
    project_id: projectId,
    user_id:    ownerId,
    level,
    message,
    meta,
  };
  if (detail_json !== undefined) {
    payload.detail_json = detail_json;
  }

  const { data, error } = await supabase
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

// ─── Trade detail logging helpers ────────────────────────────────────────────

/**
 * Compute slippage between the mark price at trigger time and the actual fill.
 *
 * Convention:
 *   BUY:  positive slippage_pct  = paid more than expected (worse for user)
 *   SELL: positive slippage_pct  = received less than expected (worse for user)
 */
function computeSlippage(
  side:         "BUY" | "SELL",
  triggerPrice: number,
  filledPrice:  number,
): {
  basis:          "trigger_price";
  trigger_price:  number;
  filled_price:   number;
  slippage_abs:   number;
  slippage_pct:   number;
} {
  const slippage_abs =
    side === "BUY"
      ? filledPrice - triggerPrice
      : triggerPrice - filledPrice;

  const slippage_pct =
    triggerPrice > 0
      ? (slippage_abs / triggerPrice) * 100
      : 0;

  return {
    basis:         "trigger_price",
    trigger_price: triggerPrice,
    filled_price:  filledPrice,
    slippage_abs,
    slippage_pct,
  };
}

/**
 * Insert TRADE_TRIGGER log (before order placement).
 * Captures the exact mark price and position snapshot at decision time.
 * Returns the log ID.
 */
async function insertTriggerLog(opts: {
  projectId:       string;
  ownerId:         string;
  side:            "BUY" | "SELL";
  symbol:          string;
  interval:        string;
  priceAtTrigger:  number;
  positionBefore:  Record<string, any> | null;
  runId:           string;
  rows:            ConditionRow[];
}): Promise<string | null> {
  const { projectId, ownerId, side, symbol, interval, priceAtTrigger, positionBefore, runId, rows } = opts;

  const allPassed = rows.length === 0 || rows.every((r) => r.result);
  const summary   = allPassed
    ? `All entry conditions met. Executing ${side}.`
    : `Some conditions failed. Executing ${side} anyway.`;

  const detail_json: Record<string, any> = {
    kind:             "trade_trigger",
    side,
    symbol,
    interval,
    price_at_trigger: priceAtTrigger,
    summary,
    rows,
    // Keep legacy context block for backwards compat
    context: {
      position_before: positionBefore
        ? {
            qty:         positionBefore.qty         ?? null,
            entry_price: positionBefore.entry_price ?? null,
            entry_time:  positionBefore.entry_time  ?? null,
            position_id: positionBefore.id          ?? null,
          }
        : null,
    },
  };

  return log(
    projectId,
    ownerId,
    "info",
    `TRADE_TRIGGER ${side} ${symbol} @ ${priceAtTrigger}`,
    { run_id: runId, symbol, exchange: "binance" },
    detail_json,
  );
}

/**
 * Insert TRADE_EXECUTED log (after order fills back).
 * Slippage is computed here immediately while triggerPrice is still in scope.
 * Returns the log ID.
 */
async function insertExecutedLog(opts: {
  projectId:      string;
  ownerId:        string;
  side:           "BUY" | "SELL";
  symbol:         string;
  triggerPrice:   number;
  result:         import("./broker/PaperBroker.js").TradeResult;
  requestedQty:   number;
  requestedUsd?:  number;
  runId:          string;
}): Promise<string | null> {
  const { projectId, ownerId, side, symbol, triggerPrice, result, requestedQty, requestedUsd, runId } = opts;

  const slippage = result.status !== "REJECTED"
    ? computeSlippage(side, triggerPrice, result.fillPrice)
    : null;

  const detail_json: Record<string, any> = {
    kind:             "trade_executed",
    side,
    symbol,
    order_type:       "MARKET",
    requested_qty:    requestedQty,
    filled_qty:       result.filledQty,
    requested_price:  null, // MARKET orders have no requested price
    filled_price:     result.fillPrice,
    status:           result.status,
    exchange_order_id: result.orderId,
    fees: {
      amount: result.fee,
      asset:  result.feeAsset,
    },
    slippage,
    position_after: result.positionAfter ?? null,
    ...(result.skipReason ? { error: { reason: result.skipReason } } : {}),
  };

  const statusLabel = result.status === "SUCCESS" ? "filled" : result.status.toLowerCase();
  return log(
    projectId,
    ownerId,
    "info",
    `TRADE_EXECUTED ${side} ${symbol} (${statusLabel})`,
    { run_id: runId, symbol, exchange: "binance" },
    detail_json,
  );
}

// ─── Wallet credentials ───────────────────────────────────────────────────────

function resolveWalletCredentials(wallet: Record<string, any>): { apiKey: string; apiSecret: string } {
  const encKey    = wallet.api_key_enc    as string | null | undefined;
  const encSecret = wallet.api_secret_enc as string | null | undefined;

  if (encKey && encSecret) {
    if (!isCiphertext(encKey) || !isCiphertext(encSecret)) {
      throw new Error("api_key_enc / api_secret_enc look malformed. Re-save the wallet to re-encrypt.");
    }
    return { apiKey: decryptString(encKey), apiSecret: decryptString(encSecret) };
  }

  const legacyKey    = wallet.external_key as string | null | undefined;
  const legacySecret = wallet.address      as string | null | undefined;
  if (legacyKey && legacySecret) {
    console.warn("[encryption] Wallet is using legacy plaintext columns. Please re-save API keys.");
    return { apiKey: legacyKey, apiSecret: legacySecret };
  }

  throw new Error("Wallet has neither encrypted nor legacy credentials. Re-save API keys.");
}

// ─── Project runner ───────────────────────────────────────────────────────────

async function runProject(p: Project) {
  const projectMode = p.mode === "live" ? "live" : "paper";

  const { data: runRow, error: runErr } = await supabase
    .from("project_runs")
    .insert({ project_id: p.id, user_id: p.owner_id, mode: projectMode, status: "running" })
    .select("id")
    .single();

  if (runErr) throw runErr;
  const runId = runRow.id;

  let symbolFailures = 0;
  let symbolTotal    = 0;

  try {
    await log(p.id, p.owner_id, "info", "Run started.", { run_id: runId });

    const { data: projRow, error: projErr } = await supabase
      .from("projects")
      .select("symbols, generated_js, settings_json")
      .eq("id", p.id)
      .single();
    if (projErr) throw projErr;

    const settings: ProjectSettings & { binance_api_key?: string; binance_api_secret?: string } =
      ((projRow as any)?.settings_json ?? {}) as any;

    const resolvedMode    = (settings.mode ?? p.mode) === "live" ? "live" : "paper";
    const advancedLogging = settings.advanced_logging === true;

    await supabase.from("project_runs").update({ mode: resolvedMode }).eq("id", runId);

    if (!isWithinTradeHours(settings.trade_hours)) {
      const h = settings.trade_hours;
      await log(p.id, p.owner_id, "info",
        `Run skipped: outside trade hours (${h?.start ?? "?"}–${h?.end ?? "?"} UTC).`,
        { run_id: runId }
      );
      await supabase.from("project_runs")
        .update({ status: "skipped", finished_at: new Date().toISOString(), summary: "Outside trade hours" })
        .eq("id", runId);
      return;
    }

    if (settings.disable_weekends && isWeekend()) {
      await log(p.id, p.owner_id, "info", "Run skipped: weekend trading disabled.", { run_id: runId });
      await supabase.from("project_runs")
        .update({ status: "skipped", finished_at: new Date().toISOString(), summary: "Weekend trading disabled" })
        .eq("id", runId);
      return;
    }

    const symbols = [
      ...new Set(
        ((projRow?.symbols ?? []) as string[])
          .map((s) => s.trim().toUpperCase())
          .filter(Boolean)
      ),
    ];

    const freshJs = String((projRow as any)?.generated_js ?? p.generated_js ?? "").trim();
    if (!freshJs) {
      await log(p.id, p.owner_id, "warn", "No generated_js found. Skipping run.");
      await supabase.from("project_runs")
        .update({ status: "skipped", finished_at: new Date().toISOString(), summary: "No code compiled" })
        .eq("id", runId);
      return;
    }

    const timeframes = extractTimeframesFromCode(freshJs) ?? ["1m"];
    const primaryTf  = timeframes[0] ?? "1m";

    for (const symbol of symbols) {
      symbolTotal++;

      let primaryPreloadOk = true;
      for (const tf of timeframes) {
        const isPrimary = tf === primaryTf;
        try {
          await klineCache.preload("binance", symbol, tf, {
            maxCandles: Number(process.env.INDICATOR_MAX_CANDLES ?? 5000),
          });
        } catch (e: any) {
          const msg = e?.message ?? String(e);
          if (isPrimary) {
            primaryPreloadOk = false;
            await log(p.id, p.owner_id, "warn",
              `Primary klines unavailable for ${symbol} ${tf} — skipping symbol: ${msg}`,
              { run_id: runId, symbol, tf, exchange: "binance" }
            );
          } else {
            await log(p.id, p.owner_id, "warn",
              `Secondary klines unavailable for ${symbol} ${tf} — indicators for this tf will return NaN: ${msg}`,
              { run_id: runId, symbol, tf, exchange: "binance" }
            );
          }
        }
      }
      if (!primaryPreloadOk) continue;

      const maxTrades = settings.max_trades_per_day;
      if (maxTrades !== undefined && Number.isFinite(maxTrades) && maxTrades > 0) {
        const todayCount = await countTradesToday(supabase, p.id, symbol);
        if (todayCount >= maxTrades) {
          if (advancedLogging) {
            await log(p.id, p.owner_id, "info",
              `SKIP ${symbol}: max trades per day reached (${todayCount}/${maxTrades}).`,
              { run_id: runId, symbol, trades_today: todayCount, max_trades_per_day: maxTrades }
            );
          }
          continue;
        }
      }

      const context    = { exchange: "binance", symbol, projectId: p.id };
      const indicators = createIndicators(klineCache, context);

      // ── Per-symbol condition rows collector ──────────────────────────────
      // Rows are accumulated as the strategy evaluates conditions, then
      // snapshotted at the moment HP.buy / HP.sell fires.
      const rowsCollector = new RowsCollector();

      // ── HP.recordCondition feeds rows from generated strategy JS ─────────────
      // The generated JS now calls HP.recordCondition({condition, value, rule, result})
      // for every boolean condition block at evaluation time, so no indicator
      // wrappers are needed here.


      const brokerCtxBase = {
        userId:    p.owner_id,
        projectId: p.id,
        runId,
        symbol,
        exchange: "binance",
        tf:       primaryTf,
      };

      let binanceApiKey    = settings.binance_api_key    ?? process.env.BINANCE_API_KEY    ?? "";
      let binanceApiSecret = settings.binance_api_secret ?? process.env.BINANCE_API_SECRET ?? "";

      if (settings.wallet_id) {
        const { data: wallet, error: walletErr } = await supabase
          .from("wallets")
          .select("api_key_enc, api_secret_enc, external_key, address")
          .eq("id", settings.wallet_id)
          .eq("owner_id", p.owner_id)
          .maybeSingle();

        if (walletErr || !wallet) {
          const errMsg = walletErr
            ? `Failed to load wallet ${settings.wallet_id}: ${walletErr.message}`
            : `Wallet ${settings.wallet_id} not found or not owned by this user`;
          await log(p.id, p.owner_id, "error", errMsg, { run_id: runId, symbol });
        } else {
          try {
            const creds  = resolveWalletCredentials(wallet);
            binanceApiKey    = creds.apiKey;
            binanceApiSecret = creds.apiSecret;
          } catch (decryptErr: any) {
            console.error(`[runner] Decryption failed for wallet ${settings.wallet_id}: ${decryptErr?.message}`);
            await log(p.id, p.owner_id, "error",
              `[SECURITY] Credential decryption failed for wallet ${settings.wallet_id}: ` +
              `${decryptErr?.message} — falling back to paper mode for ${symbol}.`,
              { run_id: runId, symbol, wallet_id: settings.wallet_id }
            );
            binanceApiKey    = "";
            binanceApiSecret = "";
          }
        }
      }

      const isLiveMode = resolvedMode === "live";
      let broker: PaperBroker | LiveBroker;

      if (isLiveMode) {
        if (!binanceApiKey || !binanceApiSecret) {
          await log(p.id, p.owner_id, "error",
            `Live mode requires valid API credentials for ${symbol} — falling back to paper mode.`,
            { run_id: runId, symbol }
          );
          broker = new PaperBroker({ supabase, cache: klineCache, ctx: brokerCtxBase });
        } else {
          const liveBroker = new LiveBroker({
            supabase,
            cache: klineCache,
            ctx: { ...brokerCtxBase, apiKey: binanceApiKey, apiSecret: binanceApiSecret },
          });

          const { ok, error: connErr } = await liveBroker.testConnectivity();
          if (!ok) {
            await log(p.id, p.owner_id, "error",
              `Binance connectivity check failed for ${symbol}: ${connErr} — skipping symbol.`,
              { run_id: runId, symbol }
            );
            symbolFailures++;
            binanceApiKey    = "";
            binanceApiSecret = "";
            continue;
          }

          broker = liveBroker;
          if (advancedLogging) {
            await log(p.id, p.owner_id, "info",
              `[LIVE] Binance connectivity OK for ${symbol}`,
              { run_id: runId, symbol, key_fingerprint: binanceApiKey.slice(0, 8) + "…" }
            );
          }

          binanceApiKey    = "";
          binanceApiSecret = "";
        }
      } else {
        broker = new PaperBroker({ supabase, cache: klineCache, ctx: brokerCtxBase });
      }

      // ── Position snapshot helpers ────────────────────────────────────────

      const { data: openPosInitial } = await supabase
        .from("project_positions")
        .select("id, entry_time, entry_price, qty")
        .eq("project_id", p.id)
        .eq("symbol", symbol)
        .eq("status", "open")
        .maybeSingle();

      const lastTradeTs  = await getLastTradeTs(p.id, symbol);
      const positionRef  = { current: openPosInitial as typeof openPosInitial | null };

      async function refreshPositionRef() {
        const { data } = await supabase
          .from("project_positions")
          .select("id, entry_time, entry_price, qty")
          .eq("project_id", p.id)
          .eq("symbol", symbol)
          .eq("status", "open")
          .maybeSingle();
        positionRef.current = data ?? null;
      }

      let crossCallIdx = 0;

      const IN_POSITION     = () => Boolean(positionRef.current?.id);
      const BARS_SINCE_ENTRY = () => {
        if (!positionRef.current?.entry_time) return Number.POSITIVE_INFINITY;
        return barsSinceTimestamp("binance", symbol, primaryTf, String(positionRef.current.entry_time));
      };

      const COOLDOWN_OK = (p2: { bars: number; scope?: string }) => {
        const bars      = Math.max(0, Math.floor(Number((p2 as any)?.bars ?? 0)));
        const scopeRaw  = String((p2 as any)?.scope ?? "last_trade").toLowerCase();
        const referenceTs: string | null =
          scopeRaw === "entry"
            ? positionRef.current?.entry_time ? String(positionRef.current.entry_time) : null
            : lastTradeTs;
        return barsSinceTimestamp("binance", symbol, primaryTf, referenceTs) >= bars;
      };

      const POSITION_VALUE = (): number => {
        if (!positionRef.current) return Number.NaN;
        const qty       = Number(positionRef.current.qty ?? NaN);
        const markPrice = broker.getMarkPricePublic();
        if (!Number.isFinite(qty) || qty <= 0 || !markPrice) return Number.NaN;
        return qty * markPrice;
      };

      const TAKE_PROFIT = (p2: { pct: number }): boolean => {
        const pct        = Number((p2 as any)?.pct ?? 0);
        if (!Number.isFinite(pct) || pct <= 0 || !positionRef.current) return false;
        const entryPrice = Number(positionRef.current.entry_price ?? 0);
        if (!Number.isFinite(entryPrice) || entryPrice <= 0) return false;
        const markPrice  = broker.getMarkPricePublic();
        if (!markPrice) return false;
        const triggered  = markPrice >= entryPrice * (1 + pct / 100);
        if (triggered && advancedLogging) {
          void broker.log("info",
            `TAKE_PROFIT condition met: price ${markPrice.toFixed(6)} >= entry ${entryPrice.toFixed(6)} +${pct}%`,
            { trigger: "take_profit", pct, mark_price: markPrice, entry_price: entryPrice }
          );
        }
        return triggered;
      };

      const STOP_LOSS = (p2: { pct: number }): boolean => {
        const pct        = Number((p2 as any)?.pct ?? 0);
        if (!Number.isFinite(pct) || pct <= 0 || !positionRef.current) return false;
        const entryPrice = Number(positionRef.current.entry_price ?? 0);
        if (!Number.isFinite(entryPrice) || entryPrice <= 0) return false;
        const markPrice  = broker.getMarkPricePublic();
        if (!markPrice) return false;
        const triggered  = markPrice <= entryPrice * (1 - pct / 100);
        if (triggered && advancedLogging) {
          void broker.log("info",
            `STOP_LOSS condition met: price ${markPrice.toFixed(6)} <= entry ${entryPrice.toFixed(6)} -${pct}%`,
            { trigger: "stop_loss", pct, mark_price: markPrice, entry_price: entryPrice }
          );
        }
        return triggered;
      };

      const CROSS_UP = (a: any, b: any) => {
        const currA  = safeNumber((a as any)?.a ?? a);
        const currB  = safeNumber((a as any)?.b ?? b);
        const strict = (a as any)?.strict === true || (a as any)?.strict === "true";
        const idx    = ++crossCallIdx;
        const key    = `${p.id}|${symbol}|up|${idx}`;
        const prev   = crossPrev.get(key);
        crossPrev.set(key, { a: currA, b: currB });
        if (!prev) return false;
        const crossed = (strict ? prev.a < prev.b : prev.a <= prev.b) && currA > currB;
        return crossed;
      };

      const CROSS_DOWN = (a: any, b: any) => {
        const currA  = safeNumber((a as any)?.a ?? a);
        const currB  = safeNumber((a as any)?.b ?? b);
        const strict = (a as any)?.strict === true || (a as any)?.strict === "true";
        const idx    = ++crossCallIdx;
        const key    = `${p.id}|${symbol}|down|${idx}`;
        const prev   = crossPrev.get(key);
        crossPrev.set(key, { a: currA, b: currB });
        if (!prev) return false;
        const crossed = (strict ? prev.a > prev.b : prev.a >= prev.b) && currA < currB;
        return crossed;
      };

      if (advancedLogging) {
        try {
          const markPrice = broker.getMarkPricePublic();
          const meta: Record<string, any> = {
            run_id:      runId,
            symbol,
            mark_price:  markPrice,
            in_position: Boolean(positionRef.current?.id),
          };
          if (positionRef.current) {
            meta.entry_price = Number(positionRef.current.entry_price ?? 0);
            meta.entry_time  = positionRef.current.entry_time;
            const ep = Number(positionRef.current.entry_price ?? 0);
            if (markPrice && Number.isFinite(ep) && ep > 0) {
              meta.unrealized_pnl_pct = ((markPrice - ep) / ep * 100).toFixed(3) + "%";
            }
          }
          await log(p.id, p.owner_id, "info", `[ADV] Tick for ${symbol}`, meta);
        } catch {
          // Never fail the strategy run due to advanced logging errors.
        }
      }

      // ── HP surface ────────────────────────────────────────────────────────
      //
      // TRIGGER SNAPSHOT INTEGRITY:
      //   The mark price and position snapshot captured here are taken from
      //   broker state at the EXACT MOMENT HP.buy/HP.sell is invoked by the
      //   strategy.  The strategy has already evaluated its conditions and
      //   decided to act — this is decision-time.  We do NOT re-evaluate any
      //   indicators.  The values come directly from:
      //     - broker.getMarkPricePublic()  — last close from the kline cache
      //     - positionRef.current          — last DB read of the open position
      //   Both are captured synchronously before any async work begins.

      const HP = {
        // recordCondition: called by generated strategy JS for every boolean
        // condition block.  The block generator emits an IIFE that evaluates
        // the real indicator value, computes the result, then calls this so
        // the runner has accurate condition data at exactly decision-time.
        recordCondition: (row: ConditionRow): void => {
          rowsCollector.push(row);
        },

        buy: async (a: any, b?: any) => {
          const usd = typeof a === "number" ? a : typeof b === "number" ? b : Number(a?.usd ?? 0);

          // ── 1) Capture trigger snapshot synchronously at decision-time ───
          const priceAtTrigger = broker.getMarkPricePublic() ?? 0;
          const positionBefore = positionRef.current
            ? { ...positionRef.current }   // shallow copy; sufficient for snapshot
            : null;

          // ── 1b) Append guard-condition rows at decision time ─────────────
          //    Trade-hours and max-trades were already evaluated above in the
          //    symbol loop (we only reach HP.buy if they passed).  Append
          //    informational rows so the table is complete.
          if (settings.trade_hours?.start && settings.trade_hours?.end) {
            const now    = new Date();
            const hhmm   = `${String(now.getUTCHours()).padStart(2, "0")}:${String(now.getUTCMinutes()).padStart(2, "0")}`;
            const rule   = `within ${settings.trade_hours.start}–${settings.trade_hours.end} (UTC)`;
            rowsCollector.push({ condition: "Active window (UTC)", value: hhmm, rule, result: true });
          }
          if (settings.max_trades_per_day !== undefined && Number.isFinite(settings.max_trades_per_day)) {
            const todayCount = await countTradesToday(supabase, p.id, symbol);
            rowsCollector.push({
              condition: "Max trades per day",
              value:     String(todayCount),
              rule:      `< ${settings.max_trades_per_day}`,
              result:    todayCount < settings.max_trades_per_day,
            });
          }

          // Snapshot rows — captured at exact decision time
          const triggerRows = rowsCollector.snapshot();
          rowsCollector.clear();

          // ── 2) Insert TRADE_TRIGGER log ──────────────────────────────────
          const triggerLogId = await insertTriggerLog({
            projectId: p.id,
            ownerId:   p.owner_id,
            side:      "BUY",
            symbol,
            interval:  primaryTf,
            priceAtTrigger,
            positionBefore,
            runId,
            rows:      triggerRows,
          });

          // ── 3) Execute the order ─────────────────────────────────────────
          const result = await broker.buy({ usd });

          // ── 4) Compute slippage & insert TRADE_EXECUTED log ──────────────
          //    Slippage is computed here, immediately after execution,
          //    while priceAtTrigger is still in the closure.
          const requestedQty = priceAtTrigger > 0 ? usd / priceAtTrigger : 0;
          const executedLogId = await insertExecutedLog({
            projectId:    p.id,
            ownerId:      p.owner_id,
            side:         "BUY",
            symbol,
            triggerPrice: priceAtTrigger,
            result,
            requestedQty,
            requestedUsd: usd,
            runId,
          });

          // ── 5) Attach both log IDs to the trade row ──────────────────────
          if (result.tradeId && (triggerLogId || executedLogId)) {
            await broker.attachLogIds(result.tradeId, triggerLogId, executedLogId);
          }

          await refreshPositionRef();
        },

        sell: async (a: any, b?: any) => {
          const pct = typeof a === "number" ? a : typeof b === "number" ? b : Number(a?.pct ?? 100);

          // ── 1) Capture trigger snapshot synchronously at decision-time ───
          const priceAtTrigger = broker.getMarkPricePublic() ?? 0;
          const positionBefore = positionRef.current
            ? { ...positionRef.current }
            : null;
          const posQty = Number(positionRef.current?.qty ?? 0);

          // ── 1b) Append guard-condition rows ──────────────────────────────
          if (settings.trade_hours?.start && settings.trade_hours?.end) {
            const now    = new Date();
            const hhmm   = `${String(now.getUTCHours()).padStart(2, "0")}:${String(now.getUTCMinutes()).padStart(2, "0")}`;
            const rule   = `within ${settings.trade_hours.start}–${settings.trade_hours.end} (UTC)`;
            rowsCollector.push({ condition: "Active window (UTC)", value: hhmm, rule, result: true });
          }

          const triggerRows = rowsCollector.snapshot();
          rowsCollector.clear();

          // ── 2) Insert TRADE_TRIGGER log ──────────────────────────────────
          const triggerLogId = await insertTriggerLog({
            projectId:    p.id,
            ownerId:      p.owner_id,
            side:         "SELL",
            symbol,
            interval:     primaryTf,
            priceAtTrigger,
            positionBefore,
            runId,
            rows:         triggerRows,
          });

          // ── 3) Execute the order ─────────────────────────────────────────
          const result = await broker.sell({ pct });

          // ── 4) Insert TRADE_EXECUTED log ─────────────────────────────────
          const closeFrac    = Math.min(1, pct / 100);
          const requestedQty = posQty * closeFrac;
          const executedLogId = await insertExecutedLog({
            projectId:    p.id,
            ownerId:      p.owner_id,
            side:         "SELL",
            symbol,
            triggerPrice: priceAtTrigger,
            result,
            requestedQty,
            runId,
          });

          // ── 5) Attach both log IDs to the trade row ──────────────────────
          if (result.tradeId && (triggerLogId || executedLogId)) {
            await broker.attachLogIds(result.tradeId, triggerLogId, executedLogId);
          }

          await refreshPositionRef();
        },

        log: async (msg: string) => broker.log("info", String(msg)),

        takeProfitCheck: async (a: any): Promise<boolean> => {
          const pct = Number((a as any)?.pct ?? a);
          if (!Number.isFinite(pct) || pct <= 0) return false;
          const pos = await broker.getPosition();
          if (!pos) return false;
          const markPrice  = broker.getMarkPricePublic();
          if (!markPrice) return false;
          const entryPrice = Number(pos.entry_price ?? 0);
          if (!Number.isFinite(entryPrice) || entryPrice <= 0) return false;
          const tpPrice = entryPrice * (1 + pct / 100);
          if (markPrice >= tpPrice) {
            if (advancedLogging) {
              await broker.log("info",
                `TAKE_PROFIT triggered: price ${markPrice.toFixed(6)} >= TP level ${tpPrice.toFixed(6)} (entry ${entryPrice.toFixed(6)} +${pct}%)`,
                { trigger: "take_profit", pct, mark_price: markPrice, tp_price: tpPrice, entry_price: entryPrice }
              );
            }
            // Use HP.sell so trigger/executed logs are created here too
            await HP.sell(100);
            return true;
          }
          return false;
        },

        stopLossCheck: async (a: any): Promise<boolean> => {
          const pct = Number((a as any)?.pct ?? a);
          if (!Number.isFinite(pct) || pct <= 0) return false;
          const pos = await broker.getPosition();
          if (!pos) return false;
          const markPrice  = broker.getMarkPricePublic();
          if (!markPrice) return false;
          const entryPrice = Number(pos.entry_price ?? 0);
          if (!Number.isFinite(entryPrice) || entryPrice <= 0) return false;
          const slPrice = entryPrice * (1 - pct / 100);
          if (markPrice <= slPrice) {
            if (advancedLogging) {
              await broker.log("info",
                `STOP_LOSS triggered: price ${markPrice.toFixed(6)} <= SL level ${slPrice.toFixed(6)} (entry ${entryPrice.toFixed(6)} -${pct}%)`,
                { trigger: "stop_loss", pct, mark_price: markPrice, sl_price: slPrice, entry_price: entryPrice }
              );
            }
            await HP.sell(100);
            return true;
          }
          return false;
        },
      };

      // ── Run the strategy ─────────────────────────────────────────────────

      try {
        await runInSandbox(
          freshJs,
          {
            ...indicators,
            CROSS_UP,
            CROSS_DOWN,
            COOLDOWN_OK,
            IN_POSITION,
            BARS_SINCE_ENTRY,
            POSITION_VALUE,
            TAKE_PROFIT,
            STOP_LOSS,
            HP,
            context,
          },
          { timeoutMs: 5000 }
        );
      } catch (e: any) {
        symbolFailures++;
        await log(p.id, p.owner_id, "error",
          `Strategy error for ${symbol}: ${e?.message ?? String(e)}`,
          { run_id: runId, symbol, exchange: "binance" }
        );
      }
    }

    await log(p.id, p.owner_id, "info", "Run finished OK.", { run_id: runId });

    const runStatus  = symbolFailures > 0 && symbolTotal > 0 ? "partial_error" : "ok";
    const runSummary = symbolFailures > 0 ? `${symbolFailures}/${symbolTotal} symbols failed` : undefined;

    await supabase.from("project_runs")
      .update({ status: runStatus, finished_at: new Date().toISOString(), ...(runSummary ? { summary: runSummary } : {}) })
      .eq("id", runId);

    await supabase.from("projects")
      .update({ last_run_status: runStatus, last_run_error: runSummary ?? null })
      .eq("id", p.id);

  } catch (e: any) {
    const msg = e?.message ?? String(e);
    await log(p.id, p.owner_id, "error", `Run failed: ${msg}`, { run_id: runId });
    await supabase.from("project_runs")
      .update({ status: "error", finished_at: new Date().toISOString(), error: msg })
      .eq("id", runId);
    await supabase.from("projects")
      .update({ last_run_status: "error", last_run_error: msg })
      .eq("id", p.id);
  }
}

// ─── Tick + main ──────────────────────────────────────────────────────────────

async function tick() {
  klineCache.clear();

  const { data, error } = await supabase.rpc("claim_due_projects", { p_limit: 5 });
  if (error) {
    console.error("claim_due_projects error:", error.message);
    return;
  }

  const projects         = (data ?? []) as Project[];
  const activeProjectIds = new Set(projects.map((p) => p.id));

  for (const p of projects) {
    try {
      await runProject(p);
    } catch (e: any) {
      console.error(`[tick] runProject threw (project ${p.id}):`, e?.message ?? e);
    }
  }

  for (const key of crossPrev.keys()) {
    const projectId = key.split("|")[0];
    if (projectId && !activeProjectIds.has(projectId)) crossPrev.delete(key);
  }
}

async function main() {
  console.log("Hornpub runner started.");

  const klineStore = new SupabaseKlineStore(supabase);

  const pollEverySeconds = (() => {
    if (process.env.KLINE_REFRESH_EVERY_SECONDS !== undefined) {
      return Math.max(10, Number(process.env.KLINE_REFRESH_EVERY_SECONDS));
    }
    return Math.max(10, Math.floor(Number(process.env.KLINE_REFRESH_EVERY_MS ?? 60_000) / 1000));
  })();

  const klineManagerOpts = {
    store: klineStore,
    exchange: "binance" as const,
    historyDays:    Number(process.env.KLINE_RETENTION_DAYS ?? 30),
    pollEverySeconds,
    maxConcurrency: Number(process.env.KLINE_MAX_CONCURRENCY ?? 3),
    getActiveSymbols: async () => {
      const statuses = (process.env.ACTIVE_PROJECT_STATUSES ?? "live,running").split(",").map((s) => s.trim()).filter(Boolean);
      const { data, error } = await supabase.from("projects").select("symbols,status").in("status", statuses);
      if (error) throw error;
      const syms: string[] = [];
      for (const row of data ?? []) {
        if (Array.isArray((row as any).symbols)) syms.push(...((row as any).symbols as string[]));
      }
      return syms;
    },
    logger: (msg: string, extra?: any) => {
      if (extra !== undefined) console.log(`[KLINES] ${msg}`, extra);
      else console.log(`[KLINES] ${msg}`);
    },
  };

  const VALID_INTERVALS  = new Set<string>(["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d"]);
  const BASE_INTERVALS   = ["1m"];
  const forcedIntervals  = (process.env.KLINE_INTERVALS ?? "").split(",").map((s) => s.trim()).filter((s) => VALID_INTERVALS.has(s));

  async function getActiveTimeframes(): Promise<string[]> {
    const statuses = (process.env.ACTIVE_PROJECT_STATUSES ?? "live,running").split(",").map((s) => s.trim()).filter(Boolean);
    const { data, error } = await supabase.from("projects").select("generated_js").in("status", statuses);
    if (error) { console.error("[KLINES] Could not query active project timeframes:", error.message); return []; }
    const found = new Set<string>();
    for (const row of data ?? []) {
      const js = String((row as any).generated_js ?? "").trim();
      if (!js) continue;
      for (const tf of extractTimeframesFromCode(js)) {
        if (VALID_INTERVALS.has(tf)) found.add(tf);
      }
    }
    return [...found];
  }

  const runningManagers = new Map<string, KlineManager>();

  function ensureManagers(intervals: string[]) {
    for (const interval of intervals) {
      if (runningManagers.has(interval)) continue;
      console.log(`[KLINES] Starting KlineManager for interval: ${interval}`);
      const m = new KlineManager({ ...klineManagerOpts, interval: interval as any });
      runningManagers.set(interval, m);
      m.start().catch((e) => console.error(`[KLINES] KlineManager(${interval}) threw unexpectedly:`, e));
    }
  }

  ensureManagers([...BASE_INTERVALS, ...forcedIntervals]);
  try { ensureManagers(await getActiveTimeframes()); }
  catch (e: any) { console.error("[KLINES] Initial timeframe discovery failed:", e?.message ?? e); }

  const tfRefreshMs = Math.max(60_000, Number(process.env.KLINE_TF_REFRESH_SECONDS ?? 300) * 1000);
  let lastTfRefresh = Date.now();

  while (true) {
    await tick();
    if (Date.now() - lastTfRefresh >= tfRefreshMs) {
      lastTfRefresh = Date.now();
      try { ensureManagers(await getActiveTimeframes()); }
      catch (e: any) { console.error("[KLINES] Timeframe refresh failed:", e?.message ?? e); }
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
