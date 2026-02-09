import { createClient } from "@supabase/supabase-js";
import { KlineManager } from "./klines/KlineManager.js";
import { SupabaseKlineStore } from "./klines/SupabaseKlineStore.js";
import vm from "node:vm";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

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

    // Expose a tiny API to strategy code
    const HP = {
      buy: async (symbol: string, usd: number) => {
        if (await hasOpenPosition(p.id, symbol)) {
          await log(p.id, p.owner_id, "info", `BUY blocked: already open for ${symbol}`);
          return;
        }
        await openPosition(p.id, p.owner_id, symbol, usd);
        await log(p.id, p.owner_id, "info", `BUY executed: ${symbol} $${usd}`);
      },
      sell: async (symbol: string) => {
        if (!(await hasOpenPosition(p.id, symbol))) {
          await log(p.id, p.owner_id, "info", `SELL blocked: no open position for ${symbol}`);
          return;
        }
        await closePosition(p.id, symbol);
        await log(p.id, p.owner_id, "info", `SELL executed: ${symbol}`);
      },
      log: async (msg: string) => log(p.id, p.owner_id, "info", msg),
    };

    // Run in VM context
    // NOTE: for now this is “trusted code”. Later we sandbox harder.
    const context = vm.createContext({ HP });
    const wrapped = `(async () => { ${p.generated_js}\n })()`;
    const script = new vm.Script(wrapped);

    await script.runInContext(context, { timeout: 5000 });

    await log(p.id, p.owner_id, "info", "Run finished OK.");

    await supabase
      .from("project_runs")
      .update({
        status: "ok",
        finished_at: new Date().toISOString(),
        summary: "OK",
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
