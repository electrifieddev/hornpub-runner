import { createClient, SupabaseClient } from "@supabase/supabase-js";
import { KlineManager } from "./klines/KlineManager.js";
import { SupabaseKlineStore } from "./klines/SupabaseKlineStore.js";
import { KlineCache } from "./klines/KlineCache.js";
import createIndicators from "./indicators/createIndicators.js";
import { runInSandbox } from "./engine/Sandbox.js";
import { PaperBroker } from "./broker/PaperBroker.js";

// ─── Settings helpers ────────────────────────────────────────────────────────

type TradeHours = { start: string; end: string };
type ProjectSettings = {
  trade_hours?: TradeHours;
  disable_weekends?: boolean;
  max_trades_per_day?: number;
  advanced_logging?: boolean;
};

/** Returns true when the current UTC time is within the configured trade window. */
function isWithinTradeHours(hours: TradeHours | undefined): boolean {
  if (!hours?.start || !hours?.end) return true; // no restriction configured
  const now = new Date();
  const hhmm = (h: string) => {
    const [hh, mm] = h.split(":").map(Number);
    return (hh ?? 0) * 60 + (mm ?? 0);
  };
  const nowMins = now.getUTCHours() * 60 + now.getUTCMinutes();
  const startMins = hhmm(hours.start);
  const endMins = hhmm(hours.end);
  if (startMins <= endMins) {
    return nowMins >= startMins && nowMins < endMins;
  }
  // Wraps midnight (e.g. 22:00–06:00)
  return nowMins >= startMins || nowMins < endMins;
}

/** Returns true if today (UTC) is Saturday or Sunday. */
function isWeekend(): boolean {
  return [0, 6].includes(new Date().getUTCDay()); // 0=Sun, 6=Sat
}

/** Count trades executed today (UTC) for this project+symbol. */
async function countTradesToday(supabase: SupabaseClient, projectId: string, symbol: string): Promise<number> {
  const start = new Date();
  start.setUTCHours(0, 0, 0, 0);
  const { count } = await supabase
    .from("project_trades")
    .select("id", { count: "exact", head: true })
    .eq("project_id", projectId)
    .eq("symbol", symbol)
    .gte("ts", start.toISOString());
  return count ?? 0;
}

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// Global in-memory kline cache used by synchronous indicators.
// IMPORTANT: indicators must not hit the DB at runtime; preload happens before each symbol execution.
const klineCache = new KlineCache({ supabase, table: "market_klines" });

// Cross detection requires prior values across ticks.
// Keyed by project+symbol+callIndex to keep multiple CROSS_* calls stable per strategy.
const crossPrev = new Map<string, { a: number; b: number }>();

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
  // Find the first candle whose open_time >= ts.
  let idx = 0;
  while (idx < openTimes.length && openTimes[idx]! < tsMs) idx++;
  const lastIdx = openTimes.length - 1;
  return Math.max(0, lastIdx - Math.min(idx, lastIdx));
}

function extractTimeframesFromCode(code: string): string[] {
  // B-03 fix: prefer the authoritative manifest comment emitted by the Blockly generator:
  //   // @hornpub-timeframes: 1m,4h,15m
  // This is more reliable than regex-scanning object literals, which misses variable
  // references or any format that doesn't match  tf: "..."  exactly.
  const manifestMatch = /^\/\/\s*@hornpub-timeframes:\s*([^\n]+)/m.exec(code);
  if (manifestMatch) {
    const tfs = manifestMatch[1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (tfs.length > 0) return tfs;
  }

  // Fallback: scan for tf: "..." / tf: '...' inside object-literal params.
  const out = new Set<string>();
  const re = /\btf\s*:\s*["'']([^"'']+)["']/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(code))) out.add(m[1]);
  if (out.size === 0) out.add("1m");
  return [...out];
}

type Project = {
  id: string;
  owner_id: string; // projects table uses owner_id
  generated_js: string | null; // you chose generated_js
  interval_seconds: number;
};

async function log(
  projectId: string,
  ownerId: string,
  level: string,
  message: string,
  meta: Record<string, any> = {}
) {
  const { error } = await supabase.from("project_logs").insert({
    project_id: projectId,
    user_id: ownerId, // project_logs expects user_id
    level,
    message,
    meta,
  });
  if (error) {
    // Don't crash the whole runner just because logging failed.
    console.error("project_logs insert error:", error.message);
  }
}

async function runProject(p: Project) {
  // Create run record (project_runs expects user_id)
  const { data: runRow, error: runErr } = await supabase
    .from("project_runs")
    .insert({ project_id: p.id, user_id: p.owner_id, mode: "paper", status: "running" })
    .select("id")
    .single();

  if (runErr) throw runErr;
  const runId = runRow.id;

  let symbolFailures = 0;
  let symbolTotal = 0;

  try {
    await log(p.id, p.owner_id, "info", "Run started.", { run_id: runId });

    // Load symbols from projects table (claim_due_projects doesn't guarantee returning them)
    const { data: projRow, error: projErr } = await supabase
      .from("projects")
      .select("symbols, generated_js, settings_json")
      .eq("id", p.id)
      .single();
    if (projErr) throw projErr;

    // ── Load and enforce project settings ──────────────────────────────────
    const settings: ProjectSettings = ((projRow as any)?.settings_json ?? {}) as ProjectSettings;
    const advancedLogging = settings.advanced_logging === true;

    // Trade hours enforcement (UTC)
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

    // Weekend enforcement
    if (settings.disable_weekends && isWeekend()) {
      await log(p.id, p.owner_id, "info", "Run skipped: weekend trading disabled.", { run_id: runId });
      await supabase.from("project_runs")
        .update({ status: "skipped", finished_at: new Date().toISOString(), summary: "Weekend trading disabled" })
        .eq("id", runId);
      return;
    }

    const symbols = [...new Set(((projRow?.symbols ?? []) as string[]).map((s) => s.trim().toUpperCase()).filter(Boolean))];
    // B-05: use generated_js from the fresh project row if the RPC didn't return it
    const freshJs = String((projRow as any)?.generated_js ?? p.generated_js ?? "").trim();
    if (!freshJs) {
      await log(p.id, p.owner_id, "warn", "No generated_js found. Skipping run.");
      await supabase
        .from("project_runs")
        .update({ status: "skipped", finished_at: new Date().toISOString(), summary: "No code compiled" })
        .eq("id", runId);
      return;
    }

    // B-24: derive the primary strategy timeframe from the code for BARS_SINCE_ENTRY / COOLDOWN_OK
    const timeframes = extractTimeframesFromCode(freshJs) ?? ["1m"];
    const primaryTf = timeframes[0] ?? "1m";

    for (const symbol of symbols) {
      symbolTotal++;
      // Preload the cache for all required timeframes *before* executing the strategy.
      // Hard-skip the symbol only if the PRIMARY timeframe fails (indicators would be useless).
      // Secondary timeframe failures are logged as warnings but execution continues — the
      // indicators for that timeframe will return NaN, which strategies should handle gracefully.
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
            await log(p.id, p.owner_id, "warn", `Primary klines unavailable for ${symbol} ${tf} — skipping symbol: ${msg}`,
              { run_id: runId, symbol, tf, exchange: "binance" }
            );
          } else {
            await log(p.id, p.owner_id, "warn", `Secondary klines unavailable for ${symbol} ${tf} — indicators for this tf will return NaN: ${msg}`,
              { run_id: runId, symbol, tf, exchange: "binance" }
            );
          }
        }
      }
      if (!primaryPreloadOk) continue;

      // Max trades per day enforcement (per symbol)
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

      const context = { exchange: "binance", symbol };
      const indicators = createIndicators(klineCache, context);

      const broker = new PaperBroker({
        supabase,
        cache: klineCache,
        ctx: {
          userId: p.owner_id,
          projectId: p.id,
          runId,
          symbol,
          exchange: "binance",
          // Primary timeframe for mark price. PaperBroker.getMarkPrice() also
          // falls back through 1m → 5m → 15m → 1h → 4h if this tf has no data.
          tf: primaryTf,
        },
      });

      // Broker surface exposed to user strategies.
      // Supports BOTH:
      //   await HP.buy({ usd: 100 })
      //   await HP.buy("BTCUSDT", 100)   (legacy)
      //   await HP.sell({ pct: 100 })
      //   await HP.sell("BTCUSDT", 100)  (legacy pct)
      const HP = {
        buy: async (a: any, b?: any) => {
          const usd = typeof a === "number" ? a : typeof b === "number" ? b : Number(a?.usd ?? 0);
          // ignore symbol argument (engine runs per symbol)
          await broker.buy({ usd });
        },
        sell: async (a: any, b?: any) => {
          const pct = typeof a === "number" ? a : typeof b === "number" ? b : Number(a?.pct ?? 100);
          await broker.sell({ pct });
        },
        log: async (msg: string) => broker.log("info", String(msg)),
        /**
         * TAKE_PROFIT: sells 100% of the position if the current price is >= entry * (1 + pct/100).
         * @param p.pct - percentage gain threshold (e.g. 5 means +5%)
         * Returns true if TP was triggered.
         */
        takeProfitCheck: async (a: any): Promise<boolean> => {
          const pct = Number((a as any)?.pct ?? a);
          if (!Number.isFinite(pct) || pct <= 0) return false;
          const pos = await broker.getPosition();
          if (!pos) return false;
          const markPrice = broker.getMarkPricePublic();
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
            await broker.sell({ pct: 100 });
            return true;
          }
          return false;
        },
        /**
         * STOP_LOSS: sells 100% of the position if the current price is <= entry * (1 - pct/100).
         * @param p.pct - percentage loss threshold (e.g. 3 means -3%)
         * Returns true if SL was triggered.
         */
        stopLossCheck: async (a: any): Promise<boolean> => {
          const pct = Number((a as any)?.pct ?? a);
          if (!Number.isFinite(pct) || pct <= 0) return false;
          const pos = await broker.getPosition();
          if (!pos) return false;
          const markPrice = broker.getMarkPricePublic();
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
            await broker.sell({ pct: 100 });
            return true;
          }
          return false;
        },
      };

      // === New Blockly runtime surface (Checkpoint 4: events/cooldown/state) ===
      // Snapshot position/trade state once per symbol execution (no per-call DB queries).
      const { data: openPos } = await supabase
        .from("project_positions")
        .select("id, entry_time, entry_price")
        .eq("project_id", p.id)
        .eq("symbol", symbol)
        .eq("status", "open")
        .maybeSingle();

      const lastTradeTs = await getLastTradeTs(p.id, symbol);

      // Cross detection uses a stable call index per execution.
      let crossCallIdx = 0;

      const IN_POSITION = () => Boolean(openPos?.id);

      // B-24: use the strategy's primary timeframe so bar counts match the chart the user sees.
      const BARS_SINCE_ENTRY = () => {
        if (!openPos?.entry_time) return Number.POSITIVE_INFINITY;
        return barsSinceTimestamp("binance", symbol, primaryTf, String(openPos.entry_time));
      };

      // B-06: implement scope variants. "entry" measures bars since position entry;
      //        "last_trade" (and any unknown scope) measures bars since the last trade.
      const COOLDOWN_OK = (p2: { bars: number; scope?: string }) => {
        const bars = Math.max(0, Math.floor(Number((p2 as any)?.bars ?? 0)));
        const scopeRaw = String((p2 as any)?.scope ?? "last_trade").toLowerCase();
        let referenceTs: string | null;
        if (scopeRaw === "entry") {
          referenceTs = openPos?.entry_time ? String(openPos.entry_time) : null;
        } else {
          // "last_trade" and any future / unknown scope variants fall back to lastTradeTs.
          referenceTs = lastTradeTs;
        }
        // B-24: use the primary strategy timeframe so bar counts are chart-accurate.
        const since = barsSinceTimestamp("binance", symbol, primaryTf, referenceTs);
        return since >= bars;
      };

      // Blocks-level TAKE_PROFIT / STOP_LOSS: evaluate condition against open position.
      // These are boolean checks only — the user must wire a SELL block after them.
      const TAKE_PROFIT = (p2: { pct: number }): boolean => {
        const pct = Number((p2 as any)?.pct ?? 0);
        if (!Number.isFinite(pct) || pct <= 0 || !openPos) return false;
        const entryPrice = Number(openPos.entry_price ?? 0);
        if (!Number.isFinite(entryPrice) || entryPrice <= 0) return false;
        const markPrice = broker.getMarkPricePublic();
        if (!markPrice) return false;
        const triggered = markPrice >= entryPrice * (1 + pct / 100);
        if (triggered && advancedLogging) {
          void broker.log("info",
            `TAKE_PROFIT condition met: price ${markPrice.toFixed(6)} >= entry ${entryPrice.toFixed(6)} +${pct}%`,
            { trigger: "take_profit", pct, mark_price: markPrice, entry_price: entryPrice }
          );
        }
        return triggered;
      };

      const STOP_LOSS = (p2: { pct: number }): boolean => {
        const pct = Number((p2 as any)?.pct ?? 0);
        if (!Number.isFinite(pct) || pct <= 0 || !openPos) return false;
        const entryPrice = Number(openPos.entry_price ?? 0);
        if (!Number.isFinite(entryPrice) || entryPrice <= 0) return false;
        const markPrice = broker.getMarkPricePublic();
        if (!markPrice) return false;
        const triggered = markPrice <= entryPrice * (1 - pct / 100);
        if (triggered && advancedLogging) {
          void broker.log("info",
            `STOP_LOSS condition met: price ${markPrice.toFixed(6)} <= entry ${entryPrice.toFixed(6)} -${pct}%`,
            { trigger: "stop_loss", pct, mark_price: markPrice, entry_price: entryPrice }
          );
        }
        return triggered;
      };

      // B-21: strict mode uses a strict inequality for the previous tick's relationship,
      //        so a flat re-touch of the crossing level does not re-trigger the signal.
      const CROSS_UP = (a: any, b: any) => {
        const currA = safeNumber((a as any)?.a ?? a);
        const currB = safeNumber((a as any)?.b ?? b);
        const strict = (a as any)?.strict === true || (a as any)?.strict === "true";
        const idx = ++crossCallIdx;
        const key = `${p.id}|${symbol}|up|${idx}`;
        const prev = crossPrev.get(key);
        crossPrev.set(key, { a: currA, b: currB });
        if (!prev) return false;
        // Non-strict: prev.a <= prev.b (was below or equal); strict: prev.a < prev.b (was strictly below)
        return (strict ? prev.a < prev.b : prev.a <= prev.b) && currA > currB;
      };

      const CROSS_DOWN = (a: any, b: any) => {
        const currA = safeNumber((a as any)?.a ?? a);
        const currB = safeNumber((a as any)?.b ?? b);
        const strict = (a as any)?.strict === true || (a as any)?.strict === "true";
        const idx = ++crossCallIdx;
        const key = `${p.id}|${symbol}|down|${idx}`;
        const prev = crossPrev.get(key);
        crossPrev.set(key, { a: currA, b: currB });
        if (!prev) return false;
        // Non-strict: prev.a >= prev.b (was above or equal); strict: prev.a > prev.b (was strictly above)
        return (strict ? prev.a > prev.b : prev.a >= prev.b) && currA < currB;
      };

      // Advanced logging: emit indicator snapshot before strategy runs
      if (advancedLogging) {
        try {
          const markPrice = broker.getMarkPricePublic();
          const meta: Record<string, any> = {
            run_id: runId,
            symbol,
            mark_price: markPrice,
            in_position: Boolean(openPos?.id),
          };
          if (openPos) {
            meta.entry_price = Number(openPos.entry_price ?? 0);
            meta.entry_time = openPos.entry_time;
            const ep = Number(openPos.entry_price ?? 0);
            if (markPrice && Number.isFinite(ep) && ep > 0) {
              meta.unrealized_pnl_pct = ((markPrice - ep) / ep * 100).toFixed(3) + "%";
            }
          }
          await log(p.id, p.owner_id, "info", `[ADV] Tick for ${symbol}`, meta);
        } catch {
          // Never fail the strategy run due to advanced logging errors.
        }
      }

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
            TAKE_PROFIT,
            STOP_LOSS,
            HP,
            context,
          },
          { timeoutMs: 5000 }
        );
      } catch (e: any) {
        // Log per-symbol failure but continue to the next symbol.
        symbolFailures++;
        await log(p.id, p.owner_id, "error", `Strategy error for ${symbol}: ${e?.message ?? String(e)}`,
          { run_id: runId, symbol, exchange: "binance" }
        );
      }
    }

    await log(p.id, p.owner_id, "info", "Run finished OK.", { run_id: runId });

    const runStatus = symbolFailures > 0 && symbolTotal > 0 ? "partial_error" : "ok";
    const runSummary = symbolFailures > 0
      ? `${symbolFailures}/${symbolTotal} symbols failed`
      : undefined;

    await supabase
      .from("project_runs")
      .update({
        status: runStatus,
        finished_at: new Date().toISOString(),
        ...(runSummary ? { summary: runSummary } : {}),
      })
      .eq("id", runId);

    await supabase
      .from("projects")
      .update({
        last_run_status: runStatus,
        last_run_error: runSummary ?? null,
      })
      .eq("id", p.id);
  } catch (e: any) {
    const msg = e?.message ?? String(e);
    await log(p.id, p.owner_id, "error", `Run failed: ${msg}`,
      { run_id: runId }
    );

    await supabase
      .from("project_runs")
      .update({
        status: "error",
        finished_at: new Date().toISOString(),
        error: msg,
      })
      .eq("id", runId);

    await supabase
      .from("projects")
      .update({
        last_run_status: "error",
        last_run_error: msg,
      })
      .eq("id", p.id);
  }
}

async function tick() {
  // B-08: clear the in-memory kline cache at the start of each tick so stale data from
  //        a prior error (where preload() was skipped) can never bleed into the next run.
  klineCache.clear();

  const { data, error } = await supabase.rpc("claim_due_projects", { p_limit: 5 });
  if (error) {
    console.error("claim_due_projects error:", error.message);
    return;
  }

  const projects = (data ?? []) as Project[];
  const activeProjectIds = new Set(projects.map((p) => p.id));

  for (const p of projects) {
    try {
      await runProject(p);
    } catch (e: any) {
      // runProject has its own outer try/catch that updates project_runs to "error".
      // This guard is a last resort in case the run-row insert itself throws — e.g.
      // a Supabase outage at the very start of runProject. Without it the whole
      // main loop would crash and all projects would stop running.
      console.error(`[tick] runProject threw outside its own try/catch (project ${p.id}):`, e?.message ?? e);
    }
  }

  // B-07: evict crossPrev entries for projects that were NOT in this tick's batch.
  //        Entries for active projects are kept so CROSS_UP/DOWN can detect transitions
  //        between ticks.  Entries for removed/paused projects are pruned to bound memory.
  if (activeProjectIds.size > 0) {
    for (const key of crossPrev.keys()) {
      const projectId = key.split("|")[0];
      if (projectId && !activeProjectIds.has(projectId)) {
        crossPrev.delete(key);
      }
    }
  }
}

async function main() {
  console.log("Hornpub runner started.");

  // --- Global market data (Option B): refresh shared klines once, then all projects read the same cache
  const klineStore = new SupabaseKlineStore(supabase);

  // B-17: Accept KLINE_REFRESH_EVERY_SECONDS as the canonical env var name.
  //        Fall back to KLINE_REFRESH_EVERY_MS for backward compatibility with existing
  //        deployment configs (divide by 1000 as the old code did).
  const pollEverySeconds = (() => {
    if (process.env.KLINE_REFRESH_EVERY_SECONDS !== undefined) {
      return Math.max(10, Number(process.env.KLINE_REFRESH_EVERY_SECONDS));
    }
    // Legacy path: the old variable was in milliseconds despite its name implying seconds.
    return Math.max(10, Math.floor(Number(process.env.KLINE_REFRESH_EVERY_MS ?? 60_000) / 1000));
  })();

  const klineManagerOpts = {
    store: klineStore,
    exchange: "binance" as const,
    historyDays: Number(process.env.KLINE_RETENTION_DAYS ?? 30),
    pollEverySeconds,
    maxConcurrency: Number(process.env.KLINE_MAX_CONCURRENCY ?? 3),
    getActiveSymbols: async () => {
      const statuses = (process.env.ACTIVE_PROJECT_STATUSES ?? "live,running").split(",").map((s) => s.trim()).filter(Boolean);
      const { data, error } = await supabase
        .from("projects")
        .select("symbols,status")
        .in("status", statuses);
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

  // B-26: Instantiate one KlineManager per supported interval so indicator functions that
  //        reference non-1m timeframes find populated cache entries instead of NaN.
  //        The set of supported intervals is configurable via KLINE_INTERVALS (comma-separated),
  //        defaulting to "1m" until operators explicitly opt into multi-timeframe support.
  const supportedIntervals = (process.env.KLINE_INTERVALS ?? "1m")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const klineManagers = supportedIntervals.map(
    (interval) => new KlineManager({ ...klineManagerOpts, interval: interval as any })
  );
  klineManagers.forEach((m) => m.start().catch((e) => console.error("[KLINES] start() threw unexpectedly:", e)));
  while (true) {
    await tick();
    await new Promise((r) => setTimeout(r, 2000));
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
