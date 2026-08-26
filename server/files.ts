import { readdir, readFile, stat } from "node:fs/promises";
import { basename, isAbsolute, join, relative, resolve, sep, win32 } from "node:path";

import type { AppConfig } from "./config.js";
import { asArray, asJsonRecord, asString, redactText, sanitizeJson } from "./redaction.js";
import { runReadOnlyStatus } from "./runner.js";
import { LogStore } from "./logger.js";
import type {
  JsonRecord,
  JsonValue,
  StatusSnapshot,
  WorkflowProgressLane,
  WorkflowProgressStage,
  WorkflowProgressStatus,
  WorkflowProgressSummary,
} from "./types.js";

const MAX_JSON_BYTES = 8 * 1024 * 1024;
const MAX_WORKFLOW_PROGRESS_BYTES = 256 * 1024;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$/;
const SAFE_PROGRESS_TOKEN = /^[\p{L}\p{N}][\p{L}\p{N} ._:/+\-]{0,119}$/u;
const MAX_PROGRESS_COUNT = 10_000_000;
const MAX_PROGRESS_DURATION_MS = 10 ** 12;

const PROGRESS_STATUS = new Set<WorkflowProgressStatus>([
  "RUNNING",
  "COMPLETED",
  "READY",
  "PARTIAL",
  "BLOCKED",
  "FAILED",
  "IDLE",
  "UNKNOWN",
]);

const PROGRESS_PHASES = new Set([
  "STARTING",
  "UNIVERSE_SYNC",
  "UNIVERSESYNC",
  "MARKET_FACT_SYNC",
  "MARKETFACTSYNC",
  "CNINFO_SYNC",
  "DATA_SYNC",
  "SNAPSHOT",
  "SNAPSHOT_RESUMED",
  "A1",
  "A2",
  "A3",
  "RESEARCH_A1",
  "RESEARCHA1",
  "RESEARCH_A2",
  "RESEARCHA2",
  "RESEARCH_A3",
  "RESEARCHA3",
  "RESEARCH",
  "PERSIST",
  "COMPLETE",
  "COMPLETED",
  "DATA_READY",
  "DATA_PARTIAL",
  "FAILED",
  "DONE",
  "READY",
  "BOOTSTRAP",
  "UNKNOWN",
]);

const NUMERIC_PROGRESS_KEYS = new Set([
  "processed",
  "processed_count",
  "completed",
  "completed_count",
  "current",
  "done",
  "total",
  "total_count",
  "universe_total",
  "cache_hits",
  "cacheHit",
  "hits",
  "cache_misses",
  "cacheMisses",
  "misses",
  "failures",
  "failed",
  "failure_count",
  "elapsed_ms",
  "elapsedMs",
  "elapsed_seconds",
  "eta_ms",
  "etaMs",
  "eta_seconds",
  "remaining_ms",
  "estimated_remaining_ms",
  "batch_processed",
  "batch_completed",
  "current_batch",
  "processed_batches",
  "batch_total",
  "batch_count",
  "total_batches",
]);

type MetricKind = "count" | "duration";

interface MetricValue {
  readonly value: number | null;
  readonly invalid: boolean;
}

function laneBelongsToRun(name: string, runId: string): boolean {
  return name.startsWith(`${runId}_lane_`) || name.startsWith(`research_${runId}_lane_`);
}

export function resolveWithinRoot(rootDir: string, relativePath: string): string | null {
  if (!relativePath || relativePath.includes("\0")) return null;
  const portablePath = relativePath.replaceAll("\\", "/");
  if (
    portablePath.startsWith("/")
    || win32.isAbsolute(relativePath)
    || /^[A-Za-z]:/.test(portablePath)
    || portablePath.split("/").includes("..")
  ) return null;
  const root = resolve(rootDir);
  const candidate = resolve(root, portablePath);
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

function progressString(value: unknown, maxLength = 120): string | null {
  if (typeof value !== "string") return null;
  const candidate = value.trim();
  if (!candidate || candidate.length > maxLength || !SAFE_PROGRESS_TOKEN.test(candidate)) return null;
  const lowered = candidate.toLowerCase();
  if (/(?:bearer|authorization|api[_ -]?key|access[_ -]?key|secret|password|passwd|credential|token|cookie|sk-[a-z0-9])/.test(lowered)) return null;
  return candidate;
}

function progressStatus(value: unknown): WorkflowProgressStatus {
  const token = progressString(value)?.toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  return token && PROGRESS_STATUS.has(token as WorkflowProgressStatus)
    ? token as WorkflowProgressStatus
    : "UNKNOWN";
}

function progressPhase(value: unknown): string | null {
  const token = progressString(value)?.toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  if (!token) return null;
  const aliases: Record<string, string> = {
    UNIVERSESYNC: "UNIVERSE_SYNC",
    MARKETFACTSYNC: "MARKET_FACT_SYNC",
    RESEARCHA1: "RESEARCH_A1",
    RESEARCHA2: "RESEARCH_A2",
    RESEARCHA3: "RESEARCH_A3",
  };
  const canonical = aliases[token] ?? token;
  return PROGRESS_PHASES.has(token) || PROGRESS_PHASES.has(canonical) ? canonical : "UNKNOWN";
}

function progressTime(value: unknown): string | null {
  if (typeof value !== "string" || value.length > 80) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

function progressId(value: unknown): string | null {
  const candidate = progressString(value, 200);
  return candidate && SAFE_ID.test(candidate) ? candidate : null;
}

function progressLaneId(value: unknown): string | null {
  const token = progressString(value, 80);
  if (!token) return null;
  const compact = token.toLowerCase().replaceAll("_", "");
  const laneMatch = /^lane([1-9][0-9]*)$/.exec(compact);
  return laneMatch ? `lane_${laneMatch[1]}` : null;
}

function progressNumber(value: unknown, kind: MetricKind): number | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) return null;
  const limit = kind === "duration" ? MAX_PROGRESS_DURATION_MS : MAX_PROGRESS_COUNT;
  if (value > limit) return null;
  return kind === "count" ? Math.floor(value) : value;
}

function metricFromSources(sources: readonly (JsonRecord | null)[], keys: readonly string[], kind: MetricKind): MetricValue {
  for (const source of sources) {
    if (!source) continue;
    for (const key of keys) {
      if (!(key in source)) continue;
      const value = source[key];
      const parsed = progressNumber(value, kind);
      return { value: parsed, invalid: parsed === null };
    }
  }
  return { value: null, invalid: false };
}

function durationFromSources(
  sources: readonly (JsonRecord | null)[],
  millisecondKeys: readonly string[],
  secondKeys: readonly string[],
): MetricValue {
  const milliseconds = metricFromSources(sources, millisecondKeys, "duration");
  if (milliseconds.value !== null || milliseconds.invalid) return milliseconds;
  const seconds = metricFromSources(sources, secondKeys, "count");
  if (seconds.value === null) return seconds;
  const value = progressNumber(seconds.value * 1000, "duration");
  return { value, invalid: value === null };
}

function nestedRecord(source: JsonRecord | null, keys: readonly string[]): JsonRecord | null {
  if (!source) return null;
  for (const key of keys) {
    const value = source[key];
    if (isRecord(value)) return value;
  }
  return null;
}

function validateProgressShape(source: JsonRecord): boolean {
  const stringKeys = ["status", "phase", "current_phase", "run_id", "runId", "updated_at", "updatedAt", "time", "started_at", "startedAt"];
  if (stringKeys.some((key) => key in source && typeof source[key] !== "string")) return false;
  const collection = source.lanes ?? source.lane_progress ?? source.laneProgress;
  if (!validateProgressCollection(collection, false)) return false;
  for (const key of ["progress", "metrics", "data", "data_sync", "dataSync"]) {
    if (key in source && !isRecord(source[key])) return false;
    if (isRecord(source[key]) && !validateProgressMetrics(source[key])) return false;
  }
  return validateProgressMetrics(source);
}

function validateProgressMetrics(source: JsonRecord): boolean {
  for (const key of NUMERIC_PROGRESS_KEYS) {
    if (!(key in source)) continue;
    const value = source[key];
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) return false;
  }
  return true;
}

function validateProgressCollection(value: unknown, stages: boolean): boolean {
  if (value === undefined) return true;
  const entries = Array.isArray(value)
    ? value.map((item) => [null, item] as const)
    : isRecord(value) ? Object.entries(value) : null;
  if (!entries || entries.length > (stages ? 16 : 12)) return false;
  for (const [key, item] of entries) {
    if (key !== null && !SAFE_PROGRESS_TOKEN.test(key)) return false;
    if (!isRecord(item) || !validateProgressMetrics(item)) return false;
    if (!stages && !progressLaneId(item.lane_id ?? item.laneId ?? item.id ?? key)) return false;
    if (stages && !validateProgressCollection(item.stages ?? item.stage_progress ?? item.stageProgress, true)) return false;
    if (!stages && !validateProgressCollection(item.stages ?? item.stage_progress ?? item.stageProgress, true)) return false;
  }
  return true;
}

function collectionEntries(value: unknown): Array<{ readonly key: string | null; readonly value: JsonRecord }> {
  if (Array.isArray(value)) {
    return value
      .filter((item): item is JsonRecord => isRecord(item))
      .slice(0, 12)
      .map((item) => ({ key: null, value: item }));
  }
  if (!isRecord(value)) return [];
  return Object.entries(value)
    .filter(([key]) => SAFE_PROGRESS_TOKEN.test(key))
    .slice(0, 12)
    .flatMap(([key, item]) => isRecord(item) ? [{ key, value: item }] : []);
}

function normalizeStage(stageKey: string | null, stage: JsonRecord): WorkflowProgressStage {
  const stageName = progressPhase(stage.stage ?? stage.stage_id ?? stage.stageId ?? stage.name ?? stageKey) ?? "UNKNOWN";
  const nested = nestedRecord(stage, ["progress", "batch", "batch_progress", "batchProgress", "counts"]);
  const sources = nested ? [stage, nested] : [stage];
  const processed = metricFromSources(sources, ["processed", "processed_count", "completed", "completed_count", "current", "done"], "count");
  const total = metricFromSources(sources, ["total", "total_count", "universe_total"], "count");
  const batchProcessed = metricFromSources(sources, ["batch_processed", "batch_completed", "completed_batches", "current_batch", "processed_batches", "processed", "completed"], "count");
  const batchTotal = metricFromSources(sources, ["batch_total", "batch_count", "total_batches", "total"], "count");
  return {
    stage: stageName,
    status: progressString(stage.status) ? progressStatus(stage.status) : null,
    processed: processed.value,
    total: total.value,
    batchProcessed: batchProcessed.value,
    batchTotal: batchTotal.value,
    updatedAt: progressTime(stage.updated_at ?? stage.updatedAt ?? stage.time),
  };
}

function normalizeLane(laneKey: string | null, lane: JsonRecord): WorkflowProgressLane | null {
  const laneId = progressLaneId(lane.lane_id ?? lane.laneId ?? lane.id ?? laneKey);
  if (!laneId) return null;
  const nested = nestedRecord(lane, ["progress", "batch", "batch_progress", "batchProgress", "counts"]);
  const sources = nested ? [lane, nested] : [lane];
  const processed = metricFromSources(sources, ["processed", "processed_count", "completed", "completed_count", "current", "done"], "count");
  const total = metricFromSources(sources, ["total", "total_count", "universe_total"], "count");
  const batchProcessed = metricFromSources(sources, ["batch_processed", "batch_completed", "completed_batches", "current_batch", "processed_batches", "processed", "completed"], "count");
  const batchTotal = metricFromSources(sources, ["batch_total", "batch_count", "total_batches", "total"], "count");
  const stageCollection = lane.stages ?? lane.stage_progress ?? lane.stageProgress;
  const stages = collectionEntries(stageCollection)
    .map(({ key, value }) => normalizeStage(key, value))
    .filter((stage, index, all) => all.findIndex((item) => item.stage === stage.stage) === index)
    .slice(0, 16);
  const directStage = lane.current_stage ?? lane.currentStage ?? lane.stage;
  if (stages.length === 0 && progressPhase(directStage)) {
    stages.push(normalizeStage(null, { ...lane, stage: directStage }));
  }
  const currentStageName = progressPhase(directStage);
  const currentStage = stages.find((stage) => stage.stage === currentStageName) ?? stages[0];
  return {
    laneId,
    model: progressString(lane.model, 120),
    status: progressString(lane.status) ? progressStatus(lane.status) : null,
    currentStage: progressPhase(directStage),
    processed: processed.value,
    total: total.value,
    batchProcessed: batchProcessed.value ?? currentStage?.batchProcessed ?? null,
    batchTotal: batchTotal.value ?? currentStage?.batchTotal ?? null,
    updatedAt: progressTime(lane.updated_at ?? lane.updatedAt ?? lane.time),
    stages,
  };
}

function invalidProgress(issue: "OVERSIZE" | "INVALID_JSON" | "INVALID_SHAPE"): WorkflowProgressSummary {
  return {
    status: issue === "OVERSIZE" ? "BLOCKED" : "INVALID",
    issue,
    runId: null,
    phase: null,
    processed: null,
    total: null,
    cacheHits: null,
    cacheMisses: null,
    failures: null,
    elapsedMs: null,
    etaMs: null,
    updatedAt: null,
    lanes: [],
  };
}

function normalizeWorkflowProgress(source: JsonRecord): WorkflowProgressSummary | null {
  if (!validateProgressShape(source)) return invalidProgress("INVALID_SHAPE");
  const nestedProgress = nestedRecord(source, ["progress", "metrics"]);
  const nestedData = nestedRecord(source, ["data", "data_sync", "dataSync"]);
  const metricSourcesList = [source, nestedProgress, nestedData].filter((item): item is JsonRecord => item !== null);
  const hasKnownField = [
    "status", "phase", "current_phase", "run_id", "runId", "lanes", "lane_progress", "laneProgress",
    "progress", "metrics", "data", "data_sync", "dataSync",
  ].some((key) => key in source) || [...NUMERIC_PROGRESS_KEYS].some((key) => key in source);
  if (!hasKnownField) return invalidProgress("INVALID_SHAPE");
  const processed = metricFromSources(metricSourcesList, ["processed", "processed_count", "completed", "completed_count", "current", "done"], "count");
  const total = metricFromSources(metricSourcesList, ["total", "total_count", "universe_total"], "count");
  const cacheHits = metricFromSources(metricSourcesList, ["cache_hits", "cacheHit", "hits"], "count");
  const cacheMisses = metricFromSources(metricSourcesList, ["cache_misses", "cacheMisses", "misses"], "count");
  const failures = metricFromSources(metricSourcesList, ["failures", "failed", "failure_count"], "count");
  const elapsedMs = durationFromSources(metricSourcesList, ["elapsed_ms", "elapsedMs", "elapsed"], ["elapsed_seconds"]);
  const etaMs = durationFromSources(metricSourcesList, ["eta_ms", "etaMs", "remaining_ms", "estimated_remaining_ms"], ["eta_seconds"]);
  const lanes = collectionEntries(source.lanes ?? source.lane_progress ?? source.laneProgress)
    .map(({ key, value }) => normalizeLane(key, value))
    .filter((lane): lane is WorkflowProgressLane => lane !== null);
  const statusValue = source.status ?? nestedProgress?.status ?? nestedData?.status;
  const phaseValue = source.phase ?? source.current_phase ?? nestedProgress?.phase ?? nestedData?.phase;
  return {
    status: progressStatus(statusValue),
    issue: null,
    runId: progressId(source.run_id ?? source.runId),
    phase: progressPhase(phaseValue),
    processed: processed.value,
    total: total.value,
    cacheHits: cacheHits.value,
    cacheMisses: cacheMisses.value,
    failures: failures.value,
    elapsedMs: elapsedMs.value,
    etaMs: etaMs.value,
    updatedAt: progressTime(source.updated_at ?? source.updatedAt ?? source.time ?? nestedProgress?.updated_at ?? nestedData?.updated_at),
    lanes,
  };
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

  /**
   * Read only the bounded, allow-listed projection of the Python progress file.
   * Missing progress is normal before the first run; malformed or oversized
   * files become a fixed diagnostic summary and never bubble their contents.
   */
  public async workflowProgress(): Promise<WorkflowProgressSummary | null> {
    const progressPath = resolveWithinRoot(this.config.rootDir, "state/workflow_progress.json");
    if (!progressPath) return null;
    let metadata;
    try {
      metadata = await stat(progressPath);
    } catch {
      return null;
    }
    if (!metadata.isFile()) return invalidProgress("INVALID_SHAPE");
    if (metadata.size > MAX_WORKFLOW_PROGRESS_BYTES) return invalidProgress("OVERSIZE");
    let contents: string;
    try {
      contents = await readFile(progressPath, { encoding: "utf8" });
    } catch {
      return invalidProgress("INVALID_JSON");
    }
    if (Buffer.byteLength(contents, "utf8") > MAX_WORKFLOW_PROGRESS_BYTES) return invalidProgress("OVERSIZE");
    let parsed: unknown;
    try {
      parsed = JSON.parse(contents);
    } catch {
      return invalidProgress("INVALID_JSON");
    }
    if (!isRecord(parsed)) return invalidProgress("INVALID_SHAPE");
    return normalizeWorkflowProgress(parsed);
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
