import { readdir, readFile, stat } from "node:fs/promises";
import { basename, isAbsolute, join, relative, resolve, sep } from "node:path";

import type { AppConfig } from "./config.js";
import { asArray, asJsonRecord, asString, redactText, sanitizeJson } from "./redaction.js";
import { runReadOnlyStatus } from "./runner.js";
import { LogStore } from "./logger.js";
import type { JsonRecord, JsonValue, StatusSnapshot } from "./types.js";

const MAX_JSON_BYTES = 8 * 1024 * 1024;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$/;

function laneBelongsToRun(name: string, runId: string): boolean {
  return name.startsWith(`${runId}_lane_`) || name.startsWith(`research_${runId}_lane_`);
}

export function resolveWithinRoot(rootDir: string, relativePath: string): string | null {
  if (!relativePath || relativePath.includes("\0")) return null;
  const root = resolve(rootDir);
  const candidate = resolve(root, relativePath);
  const child = relative(root, candidate);
  if (child === "" || (child !== ".." && !child.startsWith(`..${sep}`) && !child.startsWith("../") && !child.startsWith("..\\") && !child.includes(":\\"))) {
    return candidate;
  }
  return null;
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function readJson(path: string): Promise<JsonRecord | null> {
  try {
    const metadata = await stat(path);
    if (!metadata.isFile() || metadata.size > MAX_JSON_BYTES) return null;
    const parsed: unknown = JSON.parse(await readFile(path, { encoding: "utf8" }));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function listFiles(rootDir: string, directory: string, pattern: RegExp): Promise<{ path: string; name: string; mtimeMs: number }[]> {
  const safeDirectory = resolveWithinRoot(rootDir, directory);
  if (!safeDirectory) return [];
  try {
    const entries = await readdir(safeDirectory, { withFileTypes: true });
    const output: { path: string; name: string; mtimeMs: number }[] = [];
    for (const entry of entries) {
      if (!entry.isFile() || !pattern.test(entry.name)) continue;
      const path = resolveWithinRoot(rootDir, join(directory, entry.name));
      if (!path) continue;
      try {
        const metadata = await stat(path);
        output.push({ path, name: entry.name, mtimeMs: metadata.mtimeMs });
      } catch {
        // Ignore files rotated or removed while the directory is being read.
      }
    }
    return output.sort((left, right) => right.mtimeMs - left.mtimeMs || right.name.localeCompare(left.name));
  } catch {
    return [];
  }
}

function relativeDisplay(rootDir: string, path: string): string {
  const value = relative(resolve(rootDir), resolve(path));
  return value.split(sep).join("/");
}

function safeDisplayPath(rootDir: string, value: string | null): string | null {
  if (!value) return null;
  const candidate = isAbsolute(value) ? resolve(value) : resolveWithinRoot(rootDir, value);
  if (!candidate || !resolveWithinRoot(rootDir, relativeDisplay(rootDir, candidate))) return null;
  return relativeDisplay(rootDir, candidate);
}

function recordString(record: JsonRecord, key: string): string | null {
  return asString(record[key]);
}

function recordArray(record: JsonRecord, key: string): readonly unknown[] {
  return asArray(record[key]) ?? [];
}

function safeLimit(value: number, fallback = 50): number {
  if (!Number.isSafeInteger(value) || value < 1) return fallback;
  return Math.min(value, 200);
}

export interface WorkflowRunSummary {
  readonly runId: string;
  readonly slot: string | null;
  readonly status: string | null;
  readonly path: string;
  readonly mtimeMs: number;
  readonly snapshot: JsonValue | null;
  readonly researchMarkdown: string | null;
}

export interface WorkflowRunDetail extends WorkflowRunSummary {
  readonly payload: JsonValue;
  readonly lanes: readonly JsonValue[];
}

export interface DataSourceSummary {
  readonly provider: string;
  readonly status: string | null;
  readonly generatedAt: string | null;
  readonly path: string;
  readonly checks: JsonValue | null;
}

export class ProjectFiles {
  private readonly statusReader: PythonStatusReader;

  public constructor(
    private readonly config: AppConfig,
    logger: LogStore,
  ) {
    this.statusReader = new PythonStatusReader(config, logger);
  }

  public async status(): Promise<StatusSnapshot> {
    return this.statusReader.read();
  }

  public async listRuns(limit = 50): Promise<WorkflowRunSummary[]> {
    const files = await listFiles(this.config.rootDir, "outputs/runs", /^[A-Za-z0-9][A-Za-z0-9._-]*\.json$/);
    const summaries: WorkflowRunSummary[] = [];
    for (const file of files.slice(0, safeLimit(limit))) {
      const payload = await readJson(file.path);
      if (!payload) continue;
      const runId = recordString(payload, "run_id") ?? basename(file.name, ".json");
      if (!SAFE_ID.test(runId)) continue;
      const summary: WorkflowRunSummary = {
        runId,
        slot: recordString(payload, "slot"),
        status: recordString(payload, "status"),
        path: relativeDisplay(this.config.rootDir, file.path),
        mtimeMs: file.mtimeMs,
        snapshot: sanitizeJson(payload.snapshot ?? null),
        researchMarkdown: safeDisplayPath(this.config.rootDir, recordString(payload, "research_markdown")),
      };
      summaries.push(summary);
    }
    return summaries;
  }

  public async getRun(runId: string): Promise<WorkflowRunDetail | null> {
    if (!SAFE_ID.test(runId)) return null;
    const relativePath = `outputs/runs/${runId}.json`;
    const path = resolveWithinRoot(this.config.rootDir, relativePath);
    if (!path) return null;
    const payload = await readJson(path);
    if (!payload) return null;
    let mtimeMs = 0;
    try {
      mtimeMs = (await stat(path)).mtimeMs;
    } catch {
      return null;
    }
    const laneFiles = await listFiles(this.config.rootDir, "outputs/research", /^[A-Za-z0-9][A-Za-z0-9._-]*_lane_[A-Za-z0-9._-]+\.json$/);
    const lanes: JsonValue[] = [];
    for (const laneFile of laneFiles) {
      if (!laneBelongsToRun(laneFile.name, runId)) continue;
      const lane = await readJson(laneFile.path);
      if (lane) lanes.push(sanitizeJson(lane));
    }
    const summary: WorkflowRunSummary = {
      runId,
      slot: recordString(payload, "slot"),
      status: recordString(payload, "status"),
      path: relativeDisplay(this.config.rootDir, path),
      mtimeMs,
      snapshot: sanitizeJson(payload.snapshot ?? null),
      researchMarkdown: safeDisplayPath(this.config.rootDir, recordString(payload, "research_markdown")),
    };
    return { ...summary, payload: sanitizeJson(payload), lanes };
  }

  public async latestResearchLanes(runId: string | null): Promise<JsonValue[] | null> {
    if (!runId || !SAFE_ID.test(runId)) return null;
    const files = await listFiles(this.config.rootDir, "outputs/research", /^[A-Za-z0-9][A-Za-z0-9._-]*_lane_[A-Za-z0-9._-]+\.json$/);
    const lanes: JsonValue[] = [];
    for (const file of files) {
      if (!laneBelongsToRun(file.name, runId)) continue;
      const payload = await readJson(file.path);
      if (payload) lanes.push(sanitizeJson(payload));
    }
    return lanes;
  }

  public async monitor(): Promise<{ readonly latest: JsonValue | null; readonly effectiveSignals: string | null; readonly events: readonly JsonValue[] }> {
    const latestPath = resolveWithinRoot(this.config.rootDir, "outputs/monitor/latest.json");
    const signalPath = resolveWithinRoot(this.config.rootDir, "outputs/monitor/effective_signals.md");
    const latest = latestPath ? await readJson(latestPath) : null;
    let effectiveSignals: string | null = null;
    if (signalPath) {
      try {
        const metadata = await stat(signalPath);
        if (metadata.isFile() && metadata.size <= MAX_JSON_BYTES) {
          effectiveSignals = redactText((await readFile(signalPath, { encoding: "utf8" })).slice(-20_000), 20_000);
        }
      } catch {
        effectiveSignals = null;
      }
    }
    const events: JsonValue[] = [];
    if (latest) {
      for (const lane of recordArray(latest, "lanes")) {
        if (!isRecord(lane)) continue;
        for (const event of recordArray(lane, "events")) {
          if (!isRecord(event) || event.effective !== true) continue;
          events.push(sanitizeJson(event));
        }
      }
    }
    return { latest: sanitizeJson(latest), effectiveSignals: effectiveSignals ? effectiveSignals : null, events };
  }

  public async dataSources(): Promise<DataSourceSummary[]> {
    const files = await listFiles(this.config.rootDir, "outputs/capabilities", /^[A-Za-z0-9][A-Za-z0-9._+\-]*\.json$/);
    const latest = new Map<string, DataSourceSummary>();
    for (const file of files) {
      const payload = await readJson(file.path);
      if (!payload) continue;
      const provider = recordString(payload, "provider") ?? basename(file.name, ".json");
      if (latest.has(provider)) continue;
      const checks = recordArray(payload, "checks").map((item) => sanitizeJson(item));
      latest.set(provider, {
        provider,
        status: recordString(payload, "overall_status") ?? recordString(payload, "status"),
        generatedAt: recordString(payload, "generated_at"),
        path: relativeDisplay(this.config.rootDir, file.path),
        checks: checks.length > 0 ? checks : null,
      });
    }
    return [...latest.values()];
  }

  public async latestSchedulerFiles(): Promise<string[]> {
    const files = await listFiles(this.config.rootDir, "outputs/scheduler", /^scheduler-\d{4}-\d{2}-\d{2}\.log$/);
    return files.slice(0, 10).map((file) => relativeDisplay(this.config.rootDir, file.path));
  }
}

export class PythonStatusReader {
  private cached: StatusSnapshot | null = null;
  private pending: Promise<StatusSnapshot> | null = null;

  public constructor(
    private readonly config: AppConfig,
    private readonly logger: LogStore,
  ) {}

  public async read(): Promise<StatusSnapshot> {
    const now = Date.now();
    if (this.cached && now - Date.parse(this.cached.fetchedAt) < this.config.statusCacheMs) return this.cached;
    if (this.pending) return this.pending;
    this.pending = this.readFresh().finally(() => {
      this.pending = null;
    });
    return this.pending;
  }

  private async readFresh(): Promise<StatusSnapshot> {
    const fetchedAt = new Date().toISOString();
    const result = await runReadOnlyStatus(this.config, this.logger);
    if (result.exitCode !== 0 || result.timedOut) {
      const snapshot: StatusSnapshot = {
        availability: "degraded",
        data: null,
        fetchedAt,
        reason: result.timedOut ? "STATUS_TIMEOUT" : "STATUS_NON_ZERO_EXIT",
      };
      this.cached = snapshot;
      return snapshot;
    }
    try {
      const parsed: unknown = JSON.parse(result.stdout);
      const data = asJsonRecord(parsed);
      if (!data) throw new Error("invalid status envelope");
      const safeData = asJsonRecord(sanitizeJson(data));
      const snapshot: StatusSnapshot = { availability: "ok", data: safeData, fetchedAt, reason: null };
      this.cached = snapshot;
      return snapshot;
    } catch {
      const snapshot: StatusSnapshot = {
        availability: "degraded",
        data: null,
        fetchedAt,
        reason: "STATUS_INVALID_JSON",
      };
      this.cached = snapshot;
      return snapshot;
    }
  }
}
