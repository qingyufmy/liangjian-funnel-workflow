import express, { type Router } from "express";
import { parseTeamUiQuery, TeamUiQueryError, TeamUiBusyError, type TeamUiQuery } from "./team-ui-readonly.js";

export interface TeamUiReader {
  read(query: TeamUiQuery): Promise<unknown>;
  /** Optional on test readers; production reader persists bounded/redacted5xx. */
  recordFailure?(query: TeamUiQuery, error: unknown, responseBody: {error:string}): Promise<unknown>;
}

/** Mounted behind the existing internal auth, never a business/runner dependency. */
export function createTeamUiRouter(reader: TeamUiReader, day = (): string =>
  new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date())): Router {
  const router = express.Router();
  router.use((request, response, next) => {
    response.setHeader("Cache-Control", "no-store");
    response.setHeader("X-Content-Type-Options", "nosniff");
    if (!["GET", "HEAD"].includes(request.method)) {
      response.setHeader("Allow", "GET, HEAD");
      response.status(405).json({ error: "METHOD_NOT_ALLOWED" });
      return;
    }
    next();
  });
  router.get('/session',(_request,response)=>response.json({schema:'team-ui-session/1',poll:true,interval_ms:10000}));
  // Mounted after Team auth/flag and GET/HEAD gate. No DB, worker or business
  // heartbeat: it proves only this HTTP service can answer right now.
  router.get('/ping',(_request,response)=>response.json({schema:'team-ui-ping/1',reachable:true}));
  router.use((request, response) => {
    let query: TeamUiQuery | null;
    try {
      query = parseTeamUiQuery(request.path, new URL(request.originalUrl, "http://internal.invalid").searchParams, day());
    } catch (error) {
      response.status(400).json({ error: error instanceof TeamUiQueryError ? error.message : "INVALID_QUERY" });
      return;
    }
    if (!query) { response.status(404).json({ error: "UI_API_NOT_FOUND" }); return; }
    void reader.read(query).then(result => {
      if (['dashboard','system/health','summary'].includes(query!.kind) && result && typeof result==='object') {
        response.json({...result,node:{pid:process.pid,uptime_seconds:process.uptime(),heartbeat_at:new Date().toISOString(),source:'SERVING_NODE_PROCESS_NOT_TRADING_ACCEPTANCE'}});
      } else response.json(result);
    }).catch(async error => {
      const body={ error: error instanceof TeamUiBusyError ? "TEAM_UI_BUSY" : "TEAM_UI_SOURCE_UNAVAILABLE" };
      try { await reader.recordFailure?.(query!,error,body); } catch { /* Diagnostics never disclose raw exceptions or recursively fail the API. */ }
      response.status(503).json(body);
    });
  });
  return router;
}
