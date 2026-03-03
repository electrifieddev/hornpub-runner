/**
 * src/api/tradeLogDetail.ts
 *
 * Two REST endpoint handlers for the trade detail feature.
 *
 * These are written as framework-agnostic async functions that accept a
 * Supabase client + route params and return a typed result. Wire them
 * into your existing Express / Hono / Next.js API router as needed.
 *
 * Endpoints:
 *   GET /api/projects/:projectId/logs/:logId
 *   GET /api/projects/:projectId/trades/:tradeId/detail
 */

import type { SupabaseClient } from "@supabase/supabase-js";

// ─── Shared types ─────────────────────────────────────────────────────────────

export type ApiResult<T> =
  | { ok: true;  data: T }
  | { ok: false; status: number; error: string };

// ─── 1. GET /api/projects/:projectId/logs/:logId ──────────────────────────────

export type LogDetailResponse = {
  id:          string;
  project_id:  string;
  user_id:     string;
  level:       string;
  message:     string;
  meta:        Record<string, any> | null;
  detail_json: Record<string, any> | null;
  created_at:  string;
};

/**
 * Returns a single log row including detail_json.
 * Validates that the log belongs to the given project (ownership guard).
 */
export async function getLogDetail(
  supabase:  SupabaseClient,
  projectId: string,
  logId:     string,
  userId:    string,
): Promise<ApiResult<LogDetailResponse>> {
  if (!projectId || !logId) {
    return { ok: false, status: 400, error: "projectId and logId are required" };
  }

  // Verify the project belongs to the requesting user
  const { data: project, error: projErr } = await supabase
    .from("projects")
    .select("id")
    .eq("id",       projectId)
    .eq("owner_id", userId)
    .maybeSingle();

  if (projErr) return { ok: false, status: 500, error: projErr.message };
  if (!project) return { ok: false, status: 404, error: "Project not found" };

  const { data: logRow, error: logErr } = await supabase
    .from("project_logs")
    .select("id, project_id, user_id, level, message, meta, detail_json, created_at")
    .eq("id",         logId)
    .eq("project_id", projectId)
    .maybeSingle();

  if (logErr)  return { ok: false, status: 500, error: logErr.message };
  if (!logRow) return { ok: false, status: 404, error: "Log not found" };

  return { ok: true, data: logRow as LogDetailResponse };
}

// ─── 2. GET /api/projects/:projectId/trades/:tradeId/detail ──────────────────

export type TradeDetailResponse = {
  trade:         Record<string, any>;
  trigger_log:   LogDetailResponse | null;
  executed_log:  LogDetailResponse | null;
};

/**
 * Returns the trade row plus the two linked log rows (trigger + executed).
 * Missing logs (old trades without IDs, or logs deleted) are returned as null.
 */
export async function getTradeDetail(
  supabase:  SupabaseClient,
  projectId: string,
  tradeId:   string,
  userId:    string,
): Promise<ApiResult<TradeDetailResponse>> {
  if (!projectId || !tradeId) {
    return { ok: false, status: 400, error: "projectId and tradeId are required" };
  }

  // Verify project ownership
  const { data: project, error: projErr } = await supabase
    .from("projects")
    .select("id")
    .eq("id",       projectId)
    .eq("owner_id", userId)
    .maybeSingle();

  if (projErr) return { ok: false, status: 500, error: projErr.message };
  if (!project) return { ok: false, status: 404, error: "Project not found" };

  // Fetch the trade row (includes trigger_log_id + executed_log_id)
  const { data: trade, error: tradeErr } = await supabase
    .from("project_trades")
    .select("*")
    .eq("id",         tradeId)
    .eq("project_id", projectId)
    .maybeSingle();

  if (tradeErr) return { ok: false, status: 500, error: tradeErr.message };
  if (!trade)   return { ok: false, status: 404, error: "Trade not found" };

  const tradeRow = trade as Record<string, any>;

  // Fetch linked logs in parallel (graceful null on missing IDs)
  const [triggerLog, executedLog] = await Promise.all([
    fetchLogOrNull(supabase, projectId, tradeRow.trigger_log_id),
    fetchLogOrNull(supabase, projectId, tradeRow.executed_log_id),
  ]);

  return {
    ok: true,
    data: {
      trade:        tradeRow,
      trigger_log:  triggerLog,
      executed_log: executedLog,
    },
  };
}

// ─── Internal helper ──────────────────────────────────────────────────────────

async function fetchLogOrNull(
  supabase:  SupabaseClient,
  projectId: string,
  logId:     string | null | undefined,
): Promise<LogDetailResponse | null> {
  if (!logId) return null;

  const { data, error } = await supabase
    .from("project_logs")
    .select("id, project_id, user_id, level, message, meta, detail_json, created_at")
    .eq("id",         logId)
    .eq("project_id", projectId)
    .maybeSingle();

  if (error || !data) return null;
  return data as LogDetailResponse;
}

// ─── Express-style route wiring example ──────────────────────────────────────
//
// import express from "express";
// import { createClient } from "@supabase/supabase-js";
// import { getLogDetail, getTradeDetail } from "./api/tradeLogDetail.js";
//
// const router = express.Router();
// const supabase = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!);
//
// // Middleware that sets req.userId from your auth layer (JWT / session)
//
// router.get("/projects/:projectId/logs/:logId", async (req, res) => {
//   const result = await getLogDetail(supabase, req.params.projectId, req.params.logId, req.userId);
//   if (!result.ok) return res.status(result.status).json({ error: result.error });
//   res.json(result.data);
// });
//
// router.get("/projects/:projectId/trades/:tradeId/detail", async (req, res) => {
//   const result = await getTradeDetail(supabase, req.params.projectId, req.params.tradeId, req.userId);
//   if (!result.ok) return res.status(result.status).json({ error: result.error });
//   res.json(result.data);
// });
