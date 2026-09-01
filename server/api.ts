import express, { type Express, type NextFunction, type Request, type RequestHandler, type Response } from "express";

import type { AppConfig } from "./config.js";
import { dashboardAuth } from "./auth.js";
import { DashboardData } from "./dashboard.js";
import { isResearchLaneId, isResearchPool, isResearchStage } from "./files.js";
import { LogStore } from "./logger.js";
import { LarkSettingsStore, LarkSettingsValidationError } from "./lark-settings.js";
import { asString } from "./redaction.js";
import { JobRunner } from "./runner.js";
import { WorkflowScheduler } from "./scheduler.js";

const SAFE_RESEARCH_RUN_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$/;

export interface ApiDependencies {
  readonly config: AppConfig;
  readonly dashboard: DashboardData;
  readonly runner: JobRunner;
  readonly scheduler: WorkflowScheduler;
  readonly logger: LogStore;
  readonly larkSettings: LarkSettingsStore;
  readonly startedAt: number;
}

function queryString(request: Request, key: string): string | null {
  const value: unknown = request.query[key];
  return asString(value);
}

function queryLimit(request: Request, fallback = 50): number {
  const raw = queryString(request, "limit");
  const value = raw ? Number.parseInt(raw, 10) : fallback;
  if (!Number.isSafeInteger(value) || value < 1) return fallback;
  return Math.min(value, 200);
}

function queryPositiveInteger(request: Request, key: string, fallback: number, maximum: number): number | null {
  const raw = queryString(request, key);
  if (raw === null) return fallback;
  if (!/^\d+$/.test(raw)) return null;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) return null;
  return value;
}

function asyncRoute(
  handler: (request: Request, response: Response, next: NextFunction) => Promise<void>,
): RequestHandler {
  return (request, response, next): void => {
    void handler(request, response, next).catch(next);
  };
}

export function createApp(deps: ApiDependencies): Express {
  const app = express();
  app.disable("x-powered-by");

  app.get("/api/health", (_request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.json({
      status: "ok",
      service: "liangjian-funnel-control-plane",
      uptimeSeconds: Math.floor((Date.now() - deps.startedAt) / 1000),
      timestamp: new Date().toISOString(),
      timezone: deps.config.timezone,
    });
  });

  app.use("/api", dashboardAuth(deps.config.dashboardToken));
  app.use("/api/settings/lark", express.json({ limit: "4kb", strict: true }));
  app.get("/api/settings/lark", asyncRoute(async (_request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.json(await deps.larkSettings.status());
  }));
  app.put("/api/settings/lark", asyncRoute(async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    try {
      response.json(await deps.larkSettings.save((request.body as { webhookUrl?: unknown } | undefined)?.webhookUrl));
    } catch (error) {
      if (error instanceof LarkSettingsValidationError) {
        response.status(400).json({ error: error.reasonCode, message: error.message });
        return;
      }
      throw error;
    }
  }));
  app.delete("/api/settings/lark", asyncRoute(async (_request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.json(await deps.larkSettings.clear());
  }));
  app.get("/api/overview", asyncRoute(async (_request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.json(await deps.dashboard.overview());
  }));
  app.get("/api/runs", asyncRoute(async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.json(await deps.dashboard.runs(queryLimit(request)));
  }));
  app.get("/api/runs/:runId", asyncRoute(async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    const runId = typeof request.params.runId === "string" ? request.params.runId : "";
    const detail = await deps.dashboard.run(runId);
    if (!detail) {
      response.status(404).json({ error: "RUN_NOT_FOUND" });
      return;
    }
    response.json(detail);
  }));
  app.get("/api/research/runs/:runId/lanes/:laneId/stages/:stage", asyncRoute(async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    const runId = typeof request.params.runId === "string" ? request.params.runId : "";
    const laneId = typeof request.params.laneId === "string" ? request.params.laneId : "";
    const stage = typeof request.params.stage === "string" ? request.params.stage : "";
    const pool = queryString(request, "pool") ?? "approved";
    const page = queryPositiveInteger(request, "page", 1, Number.MAX_SAFE_INTEGER);
    const pageSize = queryPositiveInteger(request, "pageSize", 50, 100);
    if (!SAFE_RESEARCH_RUN_ID.test(runId) || !isResearchLaneId(laneId) || !isResearchStage(stage) || !isResearchPool(pool) || page === null || pageSize === null) {
      response.status(400).json({ error: "INVALID_STAGE_DETAIL_QUERY" });
      return;
    }
    const detail = await deps.dashboard.stageDetail(
      runId,
      laneId,
      stage,
      pool,
      page,
      pageSize,
      queryString(request, "q") ?? "",
      queryString(request, "reason") ?? "",
    );
    if (!detail) {
      response.status(404).json({ error: "RESEARCH_STAGE_NOT_FOUND" });
      return;
    }
    response.json(detail);
  }));
  app.get("/api/logs", asyncRoute(async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    const level = queryString(request, "level");
    const acceptedLevel = level === "debug" || level === "info" || level === "warn" || level === "error" ? level : undefined;
    const job = queryString(request, "job") ?? undefined;
    response.json(await deps.dashboard.logs(queryLimit(request), acceptedLevel, job));
  }));
  app.get("/api/logs/stream", (request, response) => {
    response.status(200);
    response.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    response.setHeader("Cache-Control", "no-cache, no-transform");
    response.setHeader("Connection", "keep-alive");
    response.write(": connected\n\n");
    let closed = false;
    const send = (event: unknown): void => {
      if (closed) return;
      try {
        response.write(`event: log\ndata: ${JSON.stringify(event)}\n\n`);
      } catch {
        closed = true;
      }
    };
    void deps.dashboard.logs(30).then((payload) => {
      if (closed) return;
      const logs = typeof payload === "object" && payload !== null && "logs" in payload
        ? (payload as { readonly logs?: unknown }).logs
        : null;
      if (Array.isArray(logs)) {
        for (const event of logs.slice().reverse()) send(event);
      }
    }).catch(() => {
      // The stream remains available for live events when historical files are unavailable.
    });
    const unsubscribe = deps.logger.subscribe(send);
    const heartbeat = setInterval(() => {
      if (closed) return;
      try {
        response.write(": heartbeat\n\n");
      } catch {
        closed = true;
      }
    }, 15_000);
    const cleanup = (): void => {
      if (closed) return;
      closed = true;
      clearInterval(heartbeat);
      unsubscribe();
    };
    request.on("close", cleanup);
    response.on("close", cleanup);
  });

  app.use("/api", (_request, response) => {
    response.status(404).json({ error: "API_NOT_FOUND" });
  });

  app.use(express.static(deps.config.webDist, { index: "index.html", fallthrough: true }));
  app.get(/.*/, (request, response) => {
    if (request.path.startsWith("/api/")) {
      response.status(404).json({ error: "API_NOT_FOUND" });
      return;
    }
    response.sendFile("index.html", { root: deps.config.webDist }, (error: Error | undefined) => {
      if (error && !response.headersSent) response.status(404).json({ error: "WEB_NOT_BUILT" });
    });
  });

  app.use((error: unknown, _request: Request, response: Response, _next: NextFunction): void => {
    if (response.headersSent) return;
    response.status(500).json({ error: "INTERNAL_ERROR" });
  });
  return app;
}
