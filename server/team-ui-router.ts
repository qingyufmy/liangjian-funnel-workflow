import express, { type Router } from "express";
import { parseTeamUiQuery, TeamUiQueryError, TeamUiBusyError, type TeamUiQuery } from "./team-ui-readonly.js";

export interface TeamUiReader { read(query: TeamUiQuery): Promise<unknown> }

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
  router.use((request, response) => {
    let query: TeamUiQuery | null;
    try {
      query = parseTeamUiQuery(request.path, new URL(request.originalUrl, "http://internal.invalid").searchParams, day());
    } catch (error) {
      response.status(400).json({ error: error instanceof TeamUiQueryError ? error.message : "INVALID_QUERY" });
      return;
    }
    if (!query) { response.status(404).json({ error: "UI_API_NOT_FOUND" }); return; }
    void reader.read(query).then(result => response.json(result)).catch(error => {
      response.status(503).json({ error: error instanceof TeamUiBusyError ? "TEAM_UI_BUSY" : "TEAM_UI_SOURCE_UNAVAILABLE" });
    });
  });
  return router;
}
