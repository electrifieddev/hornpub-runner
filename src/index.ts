import { createClient } from "@supabase/supabase-js";
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
  compiled_js: string | null;
  interval_seconds: number;
};

async function log(projectId: string, level: string, message: string) {
  await supabase.from("project_logs").insert({
    project_id: projectId,
    level,
    message,
  });
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

async function openPosition(projectId: string, symbol: string, usd: number) {
  // DB unique index prevents duplicates even if called twice.
  const { error } = await supabase.from("project_positions").insert({
    project_id: projectId,
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
  // Create run record
  const { data: runRow, error: runErr } = await supabase
    .from("project_runs")
    .insert({ project_id: p.id, status: "running" })
    .select("id")
    .single();

  if (runErr) throw runErr;
  const runId = runRow.id;

  try {
    if (!p.compiled_js || !p.compiled_js.trim()) {
      await log(p.id, "warn", "No compiled_js found. Skipping run.");
      await supabase.from("project_runs").update({
        status: "skipped",
        finished_at: new Date().toISOString(),
        summary: "No code compiled"
      }).eq("id", runId);
      return;
    }

    await log(p.id, "info", "Run started.");

    // Expose a tiny API to strategy code
    const HP = {
      buy: async (symbol: string, usd: number) => {
        if (await hasOpenPosition(p.id, symbol)) {
          await log(p.id, "info", `BUY blocked: already open for ${symbol}`);
          return;
        }
        await openPosition(p.id, symbol, usd);
        await log(p.id, "info", `BUY executed: ${symbol} $${usd}`);
      },
      sell: async (symbol: string) => {
        if (!(await hasOpenPosition(p.id, symbol))) {
          await log(p.id, "info", `SELL blocked: no open position for ${symbol}`);
          return;
        }
        await closePosition(p.id, symbol);
        await log(p.id, "info", `SELL executed: ${symbol}`);
      },
      log: async (msg: string) => log(p.id, "info", msg),
    };

    // Run in VM context
    // NOTE: for now this is “trusted code”. Later we sandbox harder.
    const context = vm.createContext({ HP });
    const wrapped = `(async () => { ${p.compiled_js}\n })()`;
    const script = new vm.Script(wrapped);

    await script.runInContext(context, { timeout: 5000 });

    await log(p.id, "info", "Run finished OK.");

    await supabase.from("project_runs").update({
      status: "ok",
      finished_at: new Date().toISOString(),
      summary: "OK"
    }).eq("id", runId);

    await supabase.from("projects").update({
      last_run_status: "ok",
      last_run_error: null
    }).eq("id", p.id);

  } catch (e: any) {
    const msg = e?.message ?? String(e);
    await log(p.id, "error", `Run failed: ${msg}`);

    await supabase.from("project_runs").update({
      status: "error",
      finished_at: new Date().toISOString(),
      error: msg
    }).eq("id", runId);

    await supabase.from("projects").update({
      last_run_status: "error",
      last_run_error: msg
    }).eq("id", p.id);
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
  while (true) {
    await tick();
    await new Promise((r) => setTimeout(r, 2000));
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
