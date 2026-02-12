import { createClient } from "@supabase/supabase-js";
import { KlineManager } from "./klines/KlineManager.js";
import { SupabaseKlineStore } from "./klines/SupabaseKlineStore.js";
import { KlineCache } from "./klines/KlineCache.js";
import createIndicators from "./indicators/createIndicators.js";
import { runInSandbox } from "./engine/Sandbox.js";
import { PaperBroker } from "./broker/PaperBroker.js";

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

function extractTimeframesFromCode(code: string): string[] {
  // Very small heuristic: find tf: "1m" in object-literal params.
  const out = new Set<string>();
  const re = /\btf\s*:\s*["']([^"']+)["']/g;
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

async function log(projectId: string, ownerId: string, level: string, message: string) {
  const { error } = await supabase.from("project_logs").insert({
    project_id: projectId,
    user_id: ownerId, // project_logs expects user_id
    level,
    message,
  });
  if (error) {
    // Don't crash the whole runner just because logging failed.
    console.error("project_logs insert error:", error.message);
  }
}

async function hasOpenPosition(projectId: string, symbol: string) {
  const { data, error } = await supabase
    .from("project_positions")
    .select("id")
    .eq("project_id", projectId)
    .eq("symbol", symbol)
    .eq("status", "open")
    .limit(1);

  if (error) throw error;
  return (data?.length ?? 0) > 0;
}

async function openPosition(projectId: string, ownerId: string, symbol: string, usd: number) {
  // DB unique index should prevent duplicates if you created it.
  const { error } = await supabase.from("project_positions").insert({
    project_id: projectId,
    user_id: ownerId, // project_positions expects user_id
    symbol,
    status: "open",
    // add more fields later (entry_price, qty, etc.)
    usd,
  });
  if (error) throw error;
}

async function closePosition(projectId: string, symbol: string) {
  const { error } = await supabase
    .from("project_positions")
    .update({ status: "closed" })
    .eq("project_id", projectId)
    .eq("symbol", symbol)
    .eq("status", "open");
  if (error) throw error;
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

  try {
    if (!p.generated_js || !p.generated_js.trim()) {
      await log(p.id, p.owner_id, "warn", "No generated_js found. Skipping run.");
      await supabase
        .from("project_runs")
        .update({
          status: "skipped",
          finished_at: new Date().toISOString(),
          summary: "No code compiled",
        })
        .eq("id", runId);
      return;
    }

    await log(p.id, p.owner_id, "info", "Run started.");

    // Load symbols from projects table (claim_due_projects doesn't guarantee returning them)
    const { data: projRow, error: projErr } = await supabase
      .from("projects")
      .select("symbols")
      .eq("id", p.id)
      .single();
    if (projErr) throw projErr;
    const symbols = (projRow?.symbols ?? []) as string[];

    const timeframes = extractTimeframesFromCode(p.generated_js) ?? ["1m"];

    for (const symbol of symbols) {
      // Preload the cache for all required timeframes *before* executing the strategy.
      // If a symbol is invalid or has no data, skip it but keep the run alive.
      let preloadOk = true;
      for (const tf of timeframes) {
        try {
          await klineCache.preload("binance", symbol, tf, {
            maxCandles: Number(process.env.INDICATOR_MAX_CANDLES ?? 5000),
          });
        } catch (e: any) {
          preloadOk = false;
          await log(p.id, p.owner_id, "warn", `Klines unavailable for ${symbol} ${tf}: ${e?.message ?? String(e)}`);
        }
      }
      if (!preloadOk) continue;

      const context = { exchange: "binance", symbol };
      const indicators = createIndicators({ cache: klineCache, ctx: context });

      const broker = new PaperBroker(supabase, klineCache, {
        userId: p.owner_id,
        projectId: p.id,
        runId,
        symbol,
        exchange: "binance",
        // Used only to get a "latest price" for sizing/PNL. Defaults to 1m.
        tf: "1m",
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
      };

      await runInSandbox(p.generated_js, {
        ...indicators,
        HP,
        context,
      }, { timeoutMs: 5000 });
    }

    await log(p.id, p.owner_id, "info", "Run finished OK.");

    await supabase
      .from("project_runs")
      .update({
        status: "ok",
        finished_at: new Date().toISOString(),
      })
      .eq("id", runId);

    await supabase
      .from("projects")
      .update({
        last_run_status: "ok",
        last_run_error: null,
      })
      .eq("id", p.id);
  } catch (e: any) {
    const msg = e?.message ?? String(e);
    await log(p.id, p.owner_id, "error", `Run failed: ${msg}`);

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
  const { data, error } = await supabase.rpc("claim_due_projects", { p_limit: 5 });
  if (error) {
    console.error("claim_due_projects error:", error.message);
    return;
  }

  const projects = (data ?? []) as Project[];
  for (const p of projects) {
    await runProject(p);
  }
}

async function main() {
  console.log("Hornpub runner started.");

  // --- Global market data (Option B): refresh shared klines once, then all projects read the same cache
  const klineStore = new SupabaseKlineStore(supabase);
  const klineManager = new KlineManager({
    store: klineStore,
    exchange: "binance",
    interval: "1m",
	    // Keep a bounded amount of history in the global cache
	    historyDays: Number(process.env.KLINE_RETENTION_DAYS ?? 30),
	    // How often the manager refreshes active symbols & fetches new klines
	    pollEverySeconds: Math.max(10, Math.floor(Number(process.env.KLINE_REFRESH_EVERY_MS ?? 60_000) / 1000)),
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
	    logger: (msg: string) => console.log(`[KLINES] ${msg}`),
  });
  klineManager.start();
  while (true) {
    await tick();
    await new Promise((r) => setTimeout(r, 2000));
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
