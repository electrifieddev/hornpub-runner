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
import { KlineCache }              from "./klines/KlineCache.js";
import { InMemoryKlineManager }    from "./klines/InMemoryKlineManager.js";
import createIndicators       from "./indicators/createIndicators.js";
import { runInSandbox }       from "./engine/Sandbox.js";
import { PaperBroker }        from "./broker/PaperBroker.js";
import { LiveBroker }         from "./broker/LiveBroker.js";
import { decryptString, isCiphertext } from "./encryption.js";

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

/**
 * One row in the trigger condition table.
 * Stored as detail_json.trigger.rows — an ordered array so there are no key collisions.
 */
type ConditionRow = {
  condition:    string;   // human label, e.g. "RSI(14)" or "AND" / "OR"
  value:        string;   // actual value at decision time, e.g. "27.84"
  rule:         string;   // exact comparison, e.g. "RSI(14) < 30", or "—" for context rows
  result:       boolean | null;
  // null         → context row (RHS value display only, never flipped)
  // true/false   → actual runtime boolean — always the real evaluated result
  grouped?:     boolean;  // true = sub-condition inside an AND/OR group; __flipFrom skips it
  groupSummary?: "AND" | "OR"; // present on group summary rows; __flipFrom skips these too
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

const klineCache = new KlineCache({ maxCandles: Number(process.env.INDICATOR_MAX_CANDLES ?? 5000) });
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
  conditionRows:   ConditionRow[];  // exact conditions evaluated, in evaluation order
}): Promise<string | null> {
  const { projectId, ownerId, side, symbol, interval, priceAtTrigger, positionBefore, runId, conditionRows } = opts;
  const primaryReason = side === "BUY" ? "BUY signal detected" : "SELL signal detected";

  const detail_json: Record<string, any> = {
    kind:             "trade_trigger",
    side,
    symbol,
    interval,
    price_at_trigger: priceAtTrigger,
    trigger: {
      primary_reason: primaryReason,
      // rows: ordered array of every condition evaluated at decision time.
      // Each entry is { condition, value, rule, result } — truthful because rule strings
      // are co-generated with the actual indicator call by the same Blockly forBlock function.
      rows: conditionRows,
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
    },
  };

  return log(
    projectId,
    ownerId,
    "info",
    `Result: ${primaryReason}`,
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

  return log(
    projectId,
    ownerId,
    "info",
    `Trade executed: ${side} ${symbol}`,
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

      // Check that the primary timeframe klines are ready in memory.
      // InMemoryKlineManager bootstraps in the background — if not ready yet, skip and retry next run.
      if (!klineCache.isReady("binance", symbol, primaryTf)) {
        await log(p.id, p.owner_id, "info",
          `Klines not ready yet for ${symbol} ${primaryTf} — skipping until bootstrapped`,
          { run_id: runId, symbol, tf: primaryTf, exchange: "binance" }
        );
        continue;
      }

      // Staleness guard: skip if latest candle is too old (KlineManager may be lagging)
      const staleThresholdMs = 3 * 60 * 1000; // 3 minutes
      const ageMs = klineCache.ageMs("binance", symbol, "1m");
      if (ageMs > staleThresholdMs) {
        await log(p.id, p.owner_id, "warn",
          `Stale klines for ${symbol} (${Math.round(ageMs / 60_000)}m old) — skipping`,
          { run_id: runId, symbol, exchange: "binance" }
        );
        continue;
      }

      const maxTrades = settings.max_trades_per_day;
      let todayCount = 0;
      if (maxTrades !== undefined && Number.isFinite(maxTrades) && maxTrades > 0) {
        todayCount = await countTradesToday(supabase, p.id, symbol);
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

      // ── Per-tick condition tracker ────────────────────────────────────────
      // Populated in two phases:
      //   1) System conditions (trade hours, max trades) — added here, before sandbox
      //   2) User strategy conditions — added by HP.__cond inside the sandbox
      // HP.buy/sell takes a snapshot when called, so successive calls in one tick
      // each get only the conditions accumulated up to that point.
      const conditionRows: ConditionRow[] = [];

      // System condition: active trade window
      if (settings.trade_hours?.start && settings.trade_hours?.end) {
        const now     = new Date();
        const hh      = String(now.getUTCHours()).padStart(2, "0");
        const mm      = String(now.getUTCMinutes()).padStart(2, "0");
        conditionRows.push({
          condition: "Active Window (UTC)",
          value:     `${hh}:${mm}`,
          rule:      `within ${settings.trade_hours.start}–${settings.trade_hours.end}`,
          result:    true, // must be true: we would have returned early otherwise
        });
      }

      // System condition: max daily trades (only log if the limit is configured)
      if (maxTrades !== undefined && Number.isFinite(maxTrades) && maxTrades > 0) {
        // todayCount was already fetched above for the guard — reuse it
        conditionRows.push({
          condition: "Max Trades Today",
          value:     String(todayCount),
          rule:      `< ${maxTrades}`,
          result:    true, // must be true: we would have `continue`d otherwise
        });
      }

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
        return (strict ? prev.a < prev.b : prev.a <= prev.b) && currA > currB;
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
        return (strict ? prev.a > prev.b : prev.a >= prev.b) && currA < currB;
      };



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

      let tradeActionTaken = false;

      const HP = {
        // ── HP.__cond ──────────────────────────────────────────────────────────
        // Called by Blockly-generated strategy code for every boolean condition.
        // The rule string is co-generated with the actual indicator call in the same
        // Blockly forBlock function — same field values, same function. They cannot diverge.
        //
        // Usage in generated JS (emitted by BlocklyEditor forBlock generators):
        //   ((_v) => HP.__cond("RSI(14)", String(+_v.toFixed(2)), "RSI(14) < 30", _v < 30))(RSI({period:14}))
        //
        // Returns the boolean `result` unchanged so it can be used in `if` expressions.
        __cond: (condition: string, value: any, rule: string, result: boolean): boolean => {
          conditionRows.push({
            condition,
            value:  String(value ?? ""),
            rule,
            result: Boolean(result),
          });
          return Boolean(result);
        },

        // ── HP.__ctx ───────────────────────────────────────────────────────────
        // Appends a context-only row (rule = "—", result = null) used to show the
        // RHS indicator value when two indicators are compared, e.g. EMA(21) = 61050.
        // These rows are display-only — __flipFrom skips them.
        __ctx: (condition: string, value: any): void => {
          conditionRows.push({
            condition,
            value:  String(value ?? ""),
            rule:   "—",
            result: null as any,
          });
        },

        // ── HP.__group ─────────────────────────────────────────────────────────
        // Called by the logic_operation (AND/OR) override. Evaluates the compound
        // thunk, marks all rows emitted during evaluation as `grouped: true`, then
        // appends a group summary row storing the ACTUAL runtime boolean result.
        //
        // Group summary rows are NEVER flipped by __flipFrom — they are runtime
        // facts ("this AND evaluated to false") not judgments. This is correct even
        // in nested AND/OR inside else branches: the inner summary always reflects
        // what actually happened at runtime.
        //
        // Individual sub-rows: also never flipped (grouped: true). Their results
        // are always their real runtime values.
        __group: (op: "AND" | "OR", startIdx: number, evaluate: () => boolean): boolean => {
          const result = Boolean(evaluate());
          for (let i = startIdx; i < conditionRows.length; i++) {
            conditionRows[i]!.grouped = true;
          }
          conditionRows.push({
            condition:    op,
            value:        String(result),   // "true" or "false" — the raw runtime fact
            rule:         "—",
            result:       result,           // actual boolean, never inverted
            groupSummary: op,
          });
          return result;
        },

        // ── HP.__checkpoint ────────────────────────────────────────────────────
        // Returns the current length of conditionRows — used by the controls_if
        // override to mark "where we were before this condition was evaluated".
        __checkpoint: (): number => {
          return conditionRows.length;
        },

        // ── HP.__flipFrom ──────────────────────────────────────────────────────
        // Called by the controls_if override when an else/else-if branch is taken.
        // Inverts the result and operator of leaf condition rows recorded since `idx`
        // so the log accurately reflects "why this branch was entered".
        //
        // Skipped rows (never mutated):
        //   - result === null   → context rows (RHS display only)
        //   - grouped === true  → sub-conditions inside AND/OR groups (always accurate)
        //   - groupSummary set  → AND/OR summary rows (runtime facts, never inverted)
        //
        // Only plain leaf boolean comparisons are flipped.
        __flipFrom: (idx: number): void => {
          const OPS: [string, string][] = [
            [" >= ", " < "], [" <= ", " > "],
            [" > ",  " <= "], [" < ",  " >= "],
            [" == ", " != "], [" != ", " == "],
          ];
          function invertOp(s: string): string {
            for (const [op, inv] of OPS) {
              if (s.includes(op)) return s.replace(op, inv);
            }
            return s;
          }
          for (let i = idx; i < conditionRows.length; i++) {
            const row = conditionRows[i]!;
            if (row.result === null) continue;       // context rows — display only
            if (row.grouped)        continue;        // AND/OR sub-rows — always accurate
            if (row.groupSummary)   continue;        // AND/OR summaries — runtime facts
            row.result = !row.result;
            row.rule   = invertOp(row.rule);
          }
        },

        // ── HP.__negate ────────────────────────────────────────────────────────
        // Called by the logic_negate override (NOT block). Flips the last recorded
        // condition row's result and rule to reflect the negation, then returns !val.
        __negate: (val: boolean): boolean => {
          if (conditionRows.length > 0) {
            const last = conditionRows[conditionRows.length - 1]!;
            last.result = !last.result;
            last.rule   = last.rule.startsWith("NOT (")
              ? last.rule.slice(5, -1)       // double-NOT → unwrap
              : `NOT (${last.rule})`;
          }
          return !val;
        },

        buy: async (a: any, b?: any) => {
          const usd = typeof a === "number" ? a : typeof b === "number" ? b : Number(a?.usd ?? 0);
          tradeActionTaken = true;

          // ── 1) Capture trigger snapshot synchronously at decision-time ───
          const priceAtTrigger = broker.getMarkPricePublic() ?? 0;
          const positionBefore = positionRef.current
            ? { ...positionRef.current }   // shallow copy; sufficient for snapshot
            : null;

          // Snapshot condition rows accumulated up to this moment, then reset
          // so that a second HP.buy in the same tick gets a fresh slate.
          const conditionRowsSnapshot = [...conditionRows];
          conditionRows.length = 0;

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
            conditionRows: conditionRowsSnapshot,
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
          tradeActionTaken = true;

          // ── 1) Capture trigger snapshot synchronously at decision-time ───
          const priceAtTrigger = broker.getMarkPricePublic() ?? 0;
          const positionBefore = positionRef.current
            ? { ...positionRef.current }
            : null;
          const posQty = Number(positionRef.current?.qty ?? 0);

          // Snapshot and reset condition rows
          const conditionRowsSnapshot = [...conditionRows];
          conditionRows.length = 0;

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
            conditionRows: conditionRowsSnapshot,
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

      if (!tradeActionTaken) {
        await log(p.id, p.owner_id, "info", "Result: No trade conditions met",
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

  const VALID_INTERVALS = new Set<string>(["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d"]);

  const pollEverySeconds = (() => {
    if (process.env.KLINE_REFRESH_EVERY_SECONDS !== undefined) {
      return Math.max(10, Number(process.env.KLINE_REFRESH_EVERY_SECONDS));
    }
    return Math.max(10, Math.floor(Number(process.env.KLINE_REFRESH_EVERY_MS ?? 60_000) / 1000));
  })();

  // Single shared in-memory kline manager — no database involved.
  // Fetches directly from Binance, writes into the shared KlineCache.
  // All projects read from the same cache instance.
  const klineManager = new InMemoryKlineManager({
    cache:            klineCache,
    exchange:         "binance",
    historyDays:      Number(process.env.KLINE_RETENTION_DAYS ?? 30),
    pollEverySeconds,
    maxConcurrency:   Number(process.env.KLINE_MAX_CONCURRENCY ?? 3),
    getActive: async () => {
      const statuses = (process.env.ACTIVE_PROJECT_STATUSES ?? "live,running").split(",").map((s) => s.trim()).filter(Boolean);
      const { data, error } = await supabase.from("projects").select("symbols, generated_js").in("status", statuses);
      if (error) throw error;

      const symbols  = new Set<string>();
      const intervals = new Set<string>();

      for (const row of data ?? []) {
        if (Array.isArray((row as any).symbols)) {
          for (const s of (row as any).symbols as string[]) {
            if (s) symbols.add(s.trim().toUpperCase());
          }
        }
        const js = String((row as any).generated_js ?? "").trim();
        if (js) {
          for (const tf of extractTimeframesFromCode(js)) {
            if (VALID_INTERVALS.has(tf)) intervals.add(tf);
          }
        }
      }

      return { symbols: [...symbols], intervals: [...intervals] };
    },
    logger: (msg: string, extra?: any) => {
      if (extra !== undefined) console.log(`[KLINES] ${msg}`, extra);
      else console.log(`[KLINES] ${msg}`);
    },
  });

  klineManager.start().catch((e) => console.error("[KLINES] InMemoryKlineManager threw unexpectedly:", e));

  while (true) {
    await tick();
    await new Promise((r) => setTimeout(r, 2000));
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
