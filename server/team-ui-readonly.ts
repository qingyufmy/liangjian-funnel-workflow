import { execFile } from "node:child_process";
import { resolve } from "node:path";
import { promisify } from "node:util";

const runFile = promisify(execFile);
const roles = new Set(["a1", "a2", "pm", "tr", "rk", "rv"]);
export interface TeamUiQuery {
  readonly kind: string;
  readonly trade_date: string;
  readonly role?: string;
  readonly limit: number;
  readonly offset: number;
  readonly days: number;
  readonly since_id?: number;
}
export class TeamUiQueryError extends Error {}

function integer(query: URLSearchParams, key: string, fallback: number, min: number, max: number): number {
  const values = query.getAll(key);
  if (values.length === 0) return fallback;
  if (values.length !== 1 || !/^\d+$/.test(values[0] ?? "")) throw new TeamUiQueryError("INVALID_QUERY");
  const value = Number(values[0]);
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new TeamUiQueryError("INVALID_QUERY");
  return value;
}

/** Validated before process/file access. No user-controlled paths or SQL. */
export function parseTeamUiQuery(path: string, query: URLSearchParams, today: string): TeamUiQuery | null {
  const day = /^\/day\/(\d{4}-\d{2}-\d{2})\/(tasks|feed|plans|fills)$/.exec(path);
  const role = /^\/role\/([a-z0-9]+)\/(today|stats)$/.exec(path);
  let kind: string;
  let tradeDate = today;
  let person: string | undefined;
  if (day) { kind = day[2]!; tradeDate = day[1]!; }
  else if (role) {
    if (!roles.has(role[1]!)) return null;
    person = role[1]!; kind = `role-${role[2]}`;
  } else if (["/events", "/review/scoreboard", "/review/lessons", "/settings"].includes(path)) kind = path.slice(1);
  else return null;
  if (query.getAll("trade_date").length > 1) throw new TeamUiQueryError("INVALID_QUERY");
  if (!day && query.has("trade_date")) tradeDate = query.get("trade_date")!;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(tradeDate) || !Number.isFinite(Date.parse(`${tradeDate}T00:00:00Z`)) ||
      new Date(`${tradeDate}T00:00:00Z`).toISOString().slice(0, 10) !== tradeDate) throw new TeamUiQueryError("INVALID_DATE");
  if (day && query.has("trade_date") && query.get("trade_date") !== tradeDate) throw new TeamUiQueryError("CONFLICTING_DATE");
  if (query.has("role")) {
    const value = query.get("role")!;
    if (query.getAll("role").length !== 1 || !roles.has(value) || (person && person !== value)) throw new TeamUiQueryError("INVALID_ROLE");
    person = value;
  }
  const offset=integer(query,'offset',0,0,1_000_000);
  if(kind==='events'&&offset!==0)throw new TeamUiQueryError('INVALID_CURSOR');
  return { kind, trade_date: tradeDate, ...(person ? { role: person } : {}),
    ...(kind === 'events' ? {since_id:integer(query,'since_id',0,0,Number.MAX_SAFE_INTEGER)} : {}),
    limit: integer(query, "limit", 50, 1, 500), offset, days: integer(query, "days", 20, 1, 20) };
}

export class TeamUiBusyError extends Error {}

/** Two asynchronous child workers maximum; no unbounded request queue. */
export class TeamUiReadPool {
  private readonly pending = new Map<string, Promise<unknown>>();
  private readonly cache = new Map<string, { readonly until: number; readonly value: unknown }>();
  private active = 0;
  private peak = 0;
  private hits = 0;

  constructor(private readonly worker: (query: TeamUiQuery) => Promise<unknown>, private readonly now = Date.now) {}

  read(query: TeamUiQuery): Promise<unknown> {
    // Explicit field order makes semantically equal queries share one worker/cache.
    const key = JSON.stringify([query.kind,query.trade_date,query.role ?? null,query.limit,query.offset,query.days,query.since_id ?? 0]);
    const cached = this.cache.get(key);
    if (cached && cached.until > this.now()) { this.hits += 1; return Promise.resolve(cached.value); }
    if (cached) this.cache.delete(key);
    const existing = this.pending.get(key);
    if (existing) return existing;
    if (this.active >= 2) return Promise.reject(new TeamUiBusyError("TEAM_UI_BUSY"));
    this.active += 1; this.peak = Math.max(this.peak, this.active);
    const operation = Promise.resolve().then(() => this.worker(query)).then(value => {
      this.cache.set(key, {until:this.now()+2_000,value});
      while (this.cache.size > 64) this.cache.delete(this.cache.keys().next().value!);
      return value;
    }).finally(() => { this.active -= 1; this.pending.delete(key); });
    this.pending.set(key, operation);
    return operation;
  }

  diagnostics(): {active: number; peak: number; cacheEntries: number; cacheHits: number} {
    return {active:this.active,peak:this.peak,cacheEntries:this.cache.size,cacheHits:this.hits};
  }
}

/** Same VM Python executable configured for Node; asynchronous stdlib-only child.
 * 450ms child deadline leaves margin inside the 500ms HTTP budget. */
export class TeamUiReadOnly extends TeamUiReadPool {
  constructor(rootDir: string, pythonBin: string) {
    super(async query => {
      const { stdout } = await runFile(pythonBin,
        ["-B", resolve(rootDir, "scripts/team_ui_readonly.py"), rootDir, JSON.stringify(query)],
        { cwd: rootDir, timeout: 450, maxBuffer: 512 * 1024, windowsHide: true });
      const result: unknown = JSON.parse(stdout);
      if (!result || typeof result !== "object" || "error" in result) throw new Error("TEAM_UI_SOURCE_UNAVAILABLE");
      return result;
    });
  }
}
