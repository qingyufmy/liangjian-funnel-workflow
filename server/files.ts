import { createReadStream } from "node:fs";
import { readdir, readFile, stat } from "node:fs/promises";
import { basename, isAbsolute, join, relative, resolve, sep, win32 } from "node:path";
import { createInterface } from "node:readline";

import type { AppConfig } from "./config.js";
import { asArray, asJsonRecord, asString, redactText, sanitizeJson } from "./redaction.js";
import { runReadOnlyStatus } from "./runner.js";
import { LogStore } from "./logger.js";
import type {
  JsonRecord,
  JsonValue,
  ResearchPool,
  ResearchStage,
  ResearchStageDetail,
  ResearchStageDetailItem,
  ResearchStageDetailPlan,
  ResearchStageDetailPool,
  ResearchDecisionFacts,
  StatusSnapshot,
  WorkflowProgressIssue,
  WorkflowProgressDiagnostics,
  WorkflowProgressLane,
  WorkflowProgressStage,
  WorkflowProgressStatus,
  WorkflowProgressSummary,
  LaneOutcomeContract,
  RunOutcomeContract,
  StageOutcomeContract,
  OutcomeJobStatus,
} from "./types.js";

const MAX_JSON_BYTES = 8 * 1024 * 1024;
const MAX_RESEARCH_JSON_BYTES = 64 * 1024 * 1024;
const MAX_SNAPSHOT_JSON_BYTES = 64 * 1024 * 1024;
const MAX_WORKFLOW_PROGRESS_BYTES = 256 * 1024;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$/;
const SAFE_SNAPSHOT_ID = /^[A-Za-z0-9][A-Za-z0-9._+\-]{0,200}$/;
const SAFE_PROGRESS_TOKEN = /^[\p{L}\p{N}][\p{L}\p{N} ._:/+\-]{0,119}$/u;
const MAX_PROGRESS_COUNT = 10_000_000;
const MAX_PROGRESS_DURATION_MS = 10 ** 12;
const MAX_PROGRESS_DIAGNOSTIC_FIELDS = 20;
const MAX_PROGRESS_DIAGNOSTIC_ITEMS = 10_000;
const SAFE_PROGRESS_DIAGNOSTIC_FIELDS = new Set([
  "envelope",
  "analysis_summary",
  "structural_themes",
  "industry_chain_graph",
  "taxonomy_links",
  "industry_theme_mappings",
  "canonical_monthly_decisions",
  "monthly_industry_decisions",
  "monthly_rotation_coverage",
  "a1_contract",
  "local_screen_summary",
  "active_research_pool",
  "monitor_pool",
  "active_themes",
  "focus_pool",
  "watch_only_pool",
  "core_watch_pool",
  "secondary_watch_pool",
  "rejected_candidates",
  "source_health",
  "unresolved_questions",
]);
const SAFE_PROGRESS_DIAGNOSTIC_TYPES = new Set([
  "object",
  "array",
  "list",
  "dict",
  "tuple",
  "string",
  "str",
  "number",
  "int",
  "float",
  "boolean",
  "bool",
  "null",
  "none",
  "nonetype",
]);

const RESEARCH_LANE_IDS = new Set(["lane_1", "lane_2", "lane_3"]);
const RESEARCH_STAGES = new Set<ResearchStage>(["A1", "A2", "A3"]);
const RESEARCH_POOLS = new Set<ResearchPool>(["approved", "watch", "rejected"]);
const RESEARCH_POOL_LABELS: Record<ResearchStage, Record<ResearchPool, string>> = {
  A1: { approved: "晋级研究", watch: "持续观察", rejected: "淘汰" },
  A2: { approved: "聚焦候选", watch: "仅观察", rejected: "淘汰" },
  A3: { approved: "核心计划", watch: "次级观察", rejected: "淘汰" },
};
const RESEARCH_POOL_KEYS: Record<ResearchStage, Record<ResearchPool, string>> = {
  A1: { approved: "active_research_pool", watch: "monitor_pool", rejected: "rejected_candidates" },
  A2: { approved: "focus_pool", watch: "watch_only_pool", rejected: "rejected_candidates" },
  A3: { approved: "core_watch_pool", watch: "secondary_watch_pool", rejected: "rejected_candidates" },
};
const DETAIL_PAGE_SIZE_MAX = 100;
const DETAIL_TEXT_MAX_LENGTH = 1_000;
const DETAIL_INDEX_LINE_MAX = 1024 * 1024;

const PROGRESS_STATUS = new Set<WorkflowProgressStatus>([
  "RUNNING",
  "STALE",
  "COMPLETED",
  "READY",
  "READY_DEGRADED",
  "PARTIAL",
  "BLOCKED",
  "FAILED",
  "SUCCEEDED",
  "CANCELLED",
  "VALIDATED",
  "VALIDATED_NO_OPPORTUNITY",
  "VALIDATED_NO_ACTION",
  "VALIDATED_NO_SETUP",
  "DEGRADED_UNDERFILLED_DATA_GAP",
  "VALIDATED_UNDERFILLED_MARKET",
  "NOT_RUN",
  "IDLE",
  "UNKNOWN",
  "INVALID",
]);

const PROGRESS_PHASES = new Set([
  "STARTING",
  "UNIVERSE_SYNC",
  "UNIVERSESYNC",
  "MARKET_FACT_SYNC",
  "MARKETFACTSYNC",
  "CNINFO_SYNC",
  "CNINFO_PDF_SYNC",
  "OPEN_MACRO_SYNC",
  "DATA_SYNC",
  "SNAPSHOT",
  "SNAPSHOT_RESUMED",
  "MACRO_DISCOVERY",
  "RESEARCH_MACRO_DISCOVERY",
  "A1_LOCAL_SCREEN",
  "RESEARCH_A1_LOCAL_SCREEN",
  "A1_LLM_REVIEW",
  "RESEARCH_A1_LLM_REVIEW",
  "A2_LOCAL_ROLE",
  "RESEARCH_A2_LOCAL_ROLE",
  "A2_LLM_REVIEW",
  "RESEARCH_A2_LLM_REVIEW",
  "A3_LOCAL_TECHNICAL",
  "RESEARCH_A3_LOCAL_TECHNICAL",
  "A3_LLM_REVIEW",
  "RESEARCH_A3_LLM_REVIEW",
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
  "processed_symbols",
  "total_symbols",
  "selected_symbols",
  "monitor_symbols",
  "rejected_symbols",
  "industry_count",
  "monthly_decision_count",
  "theme_count",
  "node_count",
  "mapping_count",
  "cache_hits",
  "cacheHit",
  "hits",
  "cache_misses",
  "cacheMisses",
  "misses",
  "failures",
  "failed",
  "failure_count",
  "documents_succeeded",
  "documentsSucceeded",
  "documents_failed",
  "documentsFailed",
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
  "rss_current_mb",
  "rss_peak_mb",
  "system_mem_available_mb",
  "swap_used_mb",
  "disk_free_mb",
  "disk_free_ratio",
  "open_file_descriptors",
]);

type MetricKind = "count" | "duration";

interface MetricValue {
  readonly value: number | null;
  readonly invalid: boolean;
}

function laneBelongsToRun(name: string, runId: string): boolean {
  return name.startsWith(`${runId}_lane_`) || name.startsWith(`research_${runId}_lane_`);
}

export function isResearchLaneId(value: string): value is "lane_1" | "lane_2" | "lane_3" {
  return RESEARCH_LANE_IDS.has(value);
}

export function isResearchStage(value: string): value is ResearchStage {
  return RESEARCH_STAGES.has(value as ResearchStage);
}

export function isResearchPool(value: string): value is ResearchPool {
  return RESEARCH_POOLS.has(value as ResearchPool);
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

async function readJson(path: string, maxBytes = MAX_JSON_BYTES): Promise<JsonRecord | null> {
  try {
    const metadata = await stat(path);
    if (!metadata.isFile() || metadata.size > maxBytes) return null;
    const parsed: unknown = JSON.parse(await readFile(path, { encoding: "utf8" }));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

const OUTCOME_SCHEMA_VERSION = "research-outcome/3.0.0" as const;
const LEGACY_OUTCOME_SCHEMA_VERSION = "research-outcome/2.0.0" as const;
const OUTCOME_LIFECYCLE = new Set(["QUEUED", "RUNNING", "TERMINAL"]);
const OUTCOME_JOB_STATUS = new Set(["QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "STALE"]);
const OUTCOME_QUALITY = new Set(["VALIDATED", "DEGRADED", "BLOCKED", "FAILED", "CANCELLED"]);
const OUTCOME_SUFFICIENCY = new Set(["SUFFICIENT", "PARTIAL", "INSUFFICIENT", "NOT_APPLICABLE"]);
const OUTCOME_OPPORTUNITY = new Set(["PRESENT", "ABSENT", "UNKNOWN", "NOT_APPLICABLE"]);
const OUTCOME_ACTIONABILITY = new Set(["ACTIONABLE", "NO_ACTION", "UNKNOWN", "NOT_APPLICABLE"]);
const OUTCOME_PUBLICATION = new Set(["READY", "NOT_APPLICABLE", "BLOCKED", "PUBLISHED"]);
const OUTCOME_REASON = /^[A-Z][A-Z0-9_.:-]{0,119}$/;
const OUTCOME_COUNT_MAX = 10_000_000;

function outcomeText(value: unknown, maxLength = 200): string | null {
  if (typeof value !== "string") return null;
  const text = value.trim();
  return text ? text.slice(0, maxLength) : null;
}

function outcomeToken(value: unknown, allowed: ReadonlySet<string>): string | null {
  const token = outcomeText(value)?.toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  return token && allowed.has(token) ? token : null;
}

function outcomeCount(value: unknown): number | null {
  if (typeof value === "boolean" || typeof value !== "number" || !Number.isFinite(value)) return null;
  if (value < 0 || value > OUTCOME_COUNT_MAX) return null;
  return Math.floor(value);
}

function outcomeCounts(value: unknown, fallbacks: JsonRecord | null = null): Record<string, number> {
  const output: Record<string, number> = {};
  const source = isRecord(value) ? value : null;
  if (source) {
    for (const [key, raw] of Object.entries(source)) {
      const safeKey = outcomeText(key, 80);
      const parsed = outcomeCount(raw);
      if (safeKey && parsed !== null) output[safeKey] = parsed;
    }
  }
  if (fallbacks) {
    const aliases: Readonly<Record<string, readonly string[]>> = {
      input: ["input", "input_count", "inputCount"],
      evaluated: ["evaluated", "evaluated_count", "evaluatedCount"],
      selected: ["selected", "selected_count", "selectedCount", "output_count", "outputCount"],
    };
    for (const [target, keys] of Object.entries(aliases)) {
      if (output[target] !== undefined) continue;
      for (const key of keys) {
        const parsed = outcomeCount(fallbacks[key]);
        if (parsed !== null) {
          output[target] = parsed;
          break;
        }
      }
    }
  }
  return Object.fromEntries(Object.entries(output).sort(([left], [right]) => left.localeCompare(right)));
}

function outcomeCoverage(value: unknown): Record<string, number | string | null> {
  if (!isRecord(value)) return {};
  const output: Record<string, number | string | null> = {};
  for (const [key, raw] of Object.entries(value)) {
    const safeKey = outcomeText(key, 80);
    if (!safeKey) continue;
    if (raw === null) output[safeKey] = null;
    else if (typeof raw === "string") output[safeKey] = raw.slice(0, 200);
    else if (typeof raw === "number" && Number.isFinite(raw)) output[safeKey] = raw;
  }
  return Object.fromEntries(Object.entries(output).sort(([left], [right]) => left.localeCompare(right)));
}

function outcomeReasons(value: unknown): string[] {
  const values = typeof value === "string" ? [value] : Array.isArray(value) ? value : [];
  const output: string[] = [];
  for (const raw of values) {
    if (typeof raw !== "string") continue;
    const token = raw.trim().toUpperCase();
    if (token && OUTCOME_REASON.test(token) && !output.includes(token)) output.push(token);
  }
  return output;
}

function outcomeSource(value: JsonRecord): JsonRecord {
  const nested = value.outcome_v3 ?? value.outcomeV3 ?? value.outcome_v2 ?? value.outcomeV2 ?? value.outcome;
  return isRecord(nested) ? nested : value;
}

function outcomeJobStatus(status: string): "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED" | "STALE" {
  if (["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED"].includes(status)) return "QUEUED";
  if (["RUNNING", "RETRYING", "STARTED", "IN_PROGRESS"].includes(status)) return "RUNNING";
  if (["CANCELLED", "CANCELED"].includes(status)) return "CANCELLED";
  if (["FAILED", "BLOCKED_MODEL", "MODEL_FAILED", "MODEL_CALL_FAILED"].includes(status)) return "FAILED";
  if (status === "STALE") return "STALE";
  return "SUCCEEDED";
}

function outcomeSufficiency(
  status: string,
  counts: Readonly<Record<string, number>>,
  coverage: Readonly<Record<string, number | string | null>>,
  reasons: readonly string[],
): "SUFFICIENT" | "PARTIAL" | "INSUFFICIENT" | "NOT_APPLICABLE" {
  if (["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED", "RUNNING"].includes(status)) return "NOT_APPLICABLE";
  if (["BLOCKED_DATA_COVERAGE", "DEGRADED_UNDERFILLED_DATA_GAP", "DATA_GAP", "BLOCKED_EVIDENCE_GAP", "BLOCKED_TECHNICAL_DATA"].includes(status)) return "INSUFFICIENT";
  if (reasons.some((reason) => /COVERAGE|UNAVAILABLE|INSUFFICIENT|EVIDENCE_GAP|DATA_GAP/.test(reason))) return "INSUFFICIENT";
  const coverageState = outcomeIsCoverageInsufficient(counts, coverage);
  if (coverageState === true) return "INSUFFICIENT";
  if (coverageState === false) return "SUFFICIENT";
  if (["VALIDATED", "VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP", "VALIDATED_UNDERFILLED_MARKET", "READY", "READY_DEGRADED", "READY_TO_PUBLISH", "PUBLISHED"].includes(status)) return "PARTIAL";
  return "NOT_APPLICABLE";
}

function stageHasDataGap(stage: StageOutcomeContract): boolean {
  if (stage.quality_state === "FAILED" || stage.quality_state === "CANCELLED") return false;
  if (stage.data_sufficiency_state === "INSUFFICIENT") return true;
  return stage.quality_state === "BLOCKED" && stage.reason_codes.some((reason) => /COVERAGE|UNAVAILABLE|INSUFFICIENT|EVIDENCE_GAP|DATA_GAP/.test(reason));
}

function laneHasDataGap(lane: LaneOutcomeContract): boolean {
  return lane.data_sufficiency_state === "INSUFFICIENT" && (lane.stages.some(stageHasDataGap) || lane.reason_codes.some((reason) => /COVERAGE|UNAVAILABLE|INSUFFICIENT|EVIDENCE_GAP|DATA_GAP/.test(reason)));
}

function actionabilityForOpportunity(value: StageOutcomeContract["opportunity_state"]): StageOutcomeContract["actionability_state"] {
  return value === "PRESENT" ? "ACTIONABLE" : value === "ABSENT" ? "NO_ACTION" : value === "UNKNOWN" ? "UNKNOWN" : "NOT_APPLICABLE";
}

function opportunityForActionability(value: StageOutcomeContract["actionability_state"]): StageOutcomeContract["opportunity_state"] {
  return value === "ACTIONABLE" ? "PRESENT" : value === "NO_ACTION" ? "ABSENT" : value === "UNKNOWN" ? "UNKNOWN" : "NOT_APPLICABLE";
}

function outcomeLegacyStatus(value: JsonRecord, source: JsonRecord): string {
  return (outcomeText(source.legacy_status) ?? outcomeText(source.status) ?? outcomeText(value.status) ?? "UNKNOWN").toUpperCase();
}

function outcomeDerivedReason(status: string, stage: string): string | null {
  if (status === "VALIDATED_NO_OPPORTUNITY") return stage === "A2" ? "A2_NO_FOCUS_OPPORTUNITY" : "NO_OPPORTUNITY";
  if (status === "VALIDATED_NO_ACTION") return "A3_NO_ACTION";
  if (status === "VALIDATED_NO_SETUP") return "A3_NO_TECHNICAL_SETUP";
  if (status === "VALIDATED_UNDERFILLED_MARKET") return "POOL_UNDERFILLED_MARKET";
  if (status === "DEGRADED_UNDERFILLED_DATA_GAP" || status === "BLOCKED_DATA_COVERAGE") return "DATA_COVERAGE_INSUFFICIENT";
  if (status === "DATA_GAP") return "DATA_GAP";
  if (status === "BLOCKED_EVIDENCE_GAP") return "EVIDENCE_GAP";
  if (["BLOCKED_MODEL", "MODEL_FAILED", "MODEL_CALL_FAILED"].includes(status)) return "MODEL_CALL_FAILED";
  if (status === "BLOCKED_TECHNICAL_DATA") return "TECHNICAL_DATA_UNAVAILABLE";
  if (status === "NOT_RUN_UPSTREAM_BLOCKED") return "UPSTREAM_STAGE_BLOCKED";
  if (["CANCELLED", "CANCELED"].includes(status)) return "RUN_CANCELLED";
  return null;
}

function outcomeIsCoverageInsufficient(counts: Readonly<Record<string, number>>, coverage: Readonly<Record<string, number | string | null>>): boolean | null {
  const required = typeof coverage.required === "number" ? coverage.required : null;
  const actual = typeof coverage.actual === "number" ? coverage.actual : null;
  if (required !== null && actual !== null) return actual < required;
  const input = counts.input;
  const evaluated = counts.evaluated;
  if (input !== undefined && evaluated !== undefined && input > 0) return evaluated < input;
  return null;
}

function legacyStageOutcome(value: JsonRecord, stage: string): StageOutcomeContract {
  const source = outcomeSource(value);
  const legacyStatus = outcomeLegacyStatus(value, source);
  const fallbackCounts = {
    ...value,
    input_count: source.input_count ?? source.inputCount,
    evaluated_count: source.evaluated_count ?? source.evaluatedCount,
    selected_count: source.selected_count
      ?? source.selectedCount
      ?? source.output_count
      ?? source.outputCount
      ?? (Array.isArray(value.symbols) ? value.symbols.length : undefined),
  } satisfies JsonRecord;
  const counts = outcomeCounts(source.counts, fallbackCounts);
  const coverage = outcomeCoverage(source.data_coverage ?? source.dataCoverage);
  const reasons = outcomeReasons(source.reason_codes ?? source.reasonCodes ?? value.reason_codes ?? value.reasonCodes);
  const derived = outcomeDerivedReason(legacyStatus, stage);
  if (derived && !reasons.includes(derived)) reasons.push(derived);
  const queued = ["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED"].includes(legacyStatus);
  const running = legacyStatus === "RUNNING";
  const lifecycle = running ? "RUNNING" : queued ? "QUEUED" : "TERMINAL";
  const failed = ["FAILED", "BLOCKED_MODEL", "MODEL_FAILED", "MODEL_CALL_FAILED"].includes(legacyStatus);
  const blocked = ["BLOCKED", "BLOCKED_DATA_COVERAGE", "BLOCKED_EVIDENCE_GAP", "BLOCKED_TECHNICAL_DATA", "NOT_RUN_UPSTREAM_BLOCKED", "EXPIRED"].includes(legacyStatus);
  const cancelled = ["CANCELLED", "CANCELED"].includes(legacyStatus);
  const degraded = ["DEGRADED", "DEGRADED_UNDERFILLED_DATA_GAP", "DATA_GAP", "READY_DEGRADED", "VALIDATED_UNDERFILLED_MARKET"].includes(legacyStatus);
  const upstreamDataGap = stage === "A3" && legacyStatus === "NOT_RUN_UPSTREAM_BLOCKED" && reasons.some((reason) => /COVERAGE|UNAVAILABLE|INSUFFICIENT|EVIDENCE_GAP|DATA_GAP/.test(reason));
  const quality = cancelled ? "CANCELLED" : failed ? "FAILED" : upstreamDataGap ? "DEGRADED" : blocked ? "BLOCKED" : degraded ? "DEGRADED" : ["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED", "RUNNING"].includes(legacyStatus) ? "DEGRADED" : "VALIDATED";
  let opportunity: StageOutcomeContract["opportunity_state"] = "UNKNOWN";
  if (["NOT_RUN_UPSTREAM_BLOCKED", "PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED", "RUNNING"].includes(legacyStatus)) opportunity = "NOT_APPLICABLE";
  else if (failed || blocked || cancelled) opportunity = "UNKNOWN";
  else if (["VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"].includes(legacyStatus)) opportunity = "ABSENT";
  else if (["READY", "READY_DEGRADED", "READY_TO_PUBLISH", "PUBLISHED", "VALIDATED_UNDERFILLED_MARKET"].includes(legacyStatus)) opportunity = "PRESENT";
  else if ((counts.selected ?? 0) > 0) opportunity = "PRESENT";
  else if (counts.selected === 0 && outcomeIsCoverageInsufficient(counts, coverage) === false) opportunity = "ABSENT";
  const publication: StageOutcomeContract["publication_state"] = upstreamDataGap ? "NOT_APPLICABLE" : legacyStatus === "PUBLISHED"
    ? "PUBLISHED"
    : ["READY", "READY_DEGRADED", "READY_TO_PUBLISH", "DATA_GAP"].includes(legacyStatus)
      ? "READY"
      : failed || blocked || cancelled || legacyStatus === "NOT_RUN_UPSTREAM_BLOCKED" ? "BLOCKED" : "NOT_APPLICABLE";
  const explicitNoOpportunity = ["VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"].includes(legacyStatus);
  const missingEvaluation = counts.input === undefined || counts.evaluated === undefined;
  const insufficient = explicitNoOpportunity && (missingEvaluation || counts.input === 0 || outcomeIsCoverageInsufficient(counts, coverage) === true);
  const finalReasons = insufficient && !reasons.includes("DATA_COVERAGE_INSUFFICIENT")
    ? [...reasons, "DATA_COVERAGE_INSUFFICIENT"]
    : reasons;
  const finalOpportunity = insufficient ? "UNKNOWN" : opportunity;
  const finalQuality = insufficient ? "BLOCKED" : quality;
  const finalCoverage = insufficient && Object.keys(coverage).length === 0
    ? { sufficiency: "INSUFFICIENT" }
    : coverage;
  const stageOpportunity = stage === "A1" ? finalOpportunity : "NOT_APPLICABLE";
  const focusOpportunity = stage === "A2" ? finalOpportunity : "NOT_APPLICABLE";
  const actionability = stage === "A3" ? actionabilityForOpportunity(finalOpportunity) : "NOT_APPLICABLE";
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    stage,
    job_status: outcomeJobStatus(legacyStatus),
    lifecycle_state: lifecycle,
    quality_state: finalQuality,
    data_sufficiency_state: outcomeSufficiency(legacyStatus, counts, finalCoverage, finalReasons),
    research_opportunity_state: stageOpportunity,
    focus_opportunity_state: focusOpportunity,
    actionability_state: actionability,
    opportunity_state: finalOpportunity,
    publication_state: publication,
    reason_codes: finalReasons,
    counts,
    data_coverage: finalCoverage,
    legacy_status: legacyStatus,
  };
}

/** Return only the safe four-axis stage outcome; legacy records are projected once here. */
export function normalizeStageOutcome(value: unknown, fallbackStage = "UNKNOWN"): StageOutcomeContract | null {
  if (!isRecord(value)) return null;
  const source = outcomeSource(value);
  const stage = outcomeText(source.stage) ?? outcomeText(value.stage) ?? fallbackStage;
  const lifecycle = outcomeToken(source.lifecycle_state ?? source.lifecycleState, OUTCOME_LIFECYCLE);
  const quality = outcomeToken(source.quality_state ?? source.qualityState, OUTCOME_QUALITY);
  const legacyOpportunity = outcomeToken(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY);
  const researchOpportunity = outcomeToken(source.research_opportunity_state ?? source.researchOpportunityState, OUTCOME_OPPORTUNITY);
  const focusOpportunity = outcomeToken(source.focus_opportunity_state ?? source.focusOpportunityState, OUTCOME_OPPORTUNITY);
  const actionability = outcomeToken(source.actionability_state ?? source.actionabilityState, OUTCOME_ACTIONABILITY);
  const publication = outcomeToken(source.publication_state ?? source.publicationState, OUTCOME_PUBLICATION);
  if (lifecycle && quality && publication && (legacyOpportunity || researchOpportunity || focusOpportunity || actionability)) {
    const counts = outcomeCounts(source.counts);
    const coverage = outcomeCoverage(source.data_coverage ?? source.dataCoverage);
    const reasons = outcomeReasons(source.reason_codes ?? source.reasonCodes);
    const legacyStatus = outcomeLegacyStatus(value, source);
    const stageOpportunity = legacyOpportunity
      ?? (stage === "A1" ? researchOpportunity : stage === "A2" ? focusOpportunity : opportunityForActionability((actionability ?? "NOT_APPLICABLE") as StageOutcomeContract["actionability_state"]));
    if (!stageOpportunity) return null;
    const finalReasons = [...reasons];
    const explicitNoOpportunity = ["VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"].includes(legacyStatus);
    const missingEvaluation = counts.input === undefined || counts.evaluated === undefined;
    const insufficient = explicitNoOpportunity && (missingEvaluation || counts.input === 0 || outcomeIsCoverageInsufficient(counts, coverage) === true);
    if (insufficient && !finalReasons.includes("DATA_COVERAGE_INSUFFICIENT")) finalReasons.push("DATA_COVERAGE_INSUFFICIENT");
    const finalOpportunity = insufficient ? "UNKNOWN" : stageOpportunity;
    const finalCoverage = insufficient && Object.keys(coverage).length === 0 ? { sufficiency: "INSUFFICIENT" } : coverage;
    const dataGapOnly = quality === "BLOCKED" && (outcomeToken(source.data_sufficiency_state ?? source.dataSufficiencyState, OUTCOME_SUFFICIENCY) === "INSUFFICIENT" || finalReasons.some((reason) => /COVERAGE|UNAVAILABLE|INSUFFICIENT|EVIDENCE_GAP|DATA_GAP/.test(reason)));
    const upstreamDataGap = stage === "A3" && legacyStatus === "NOT_RUN_UPSTREAM_BLOCKED" && finalReasons.some((reason) => /COVERAGE|UNAVAILABLE|INSUFFICIENT|EVIDENCE_GAP|DATA_GAP/.test(reason));
    const projectedQuality = (dataGapOnly || upstreamDataGap) ? "DEGRADED" : (insufficient ? "BLOCKED" : quality);
    const projectedPublication = upstreamDataGap ? "NOT_APPLICABLE" : dataGapOnly && publication === "BLOCKED" ? "READY" : publication;
    return {
      schema_version: OUTCOME_SCHEMA_VERSION,
      stage,
      job_status: outcomeToken(source.job_status ?? source.jobStatus, OUTCOME_JOB_STATUS) as StageOutcomeContract["job_status"] ?? outcomeJobStatus(legacyStatus),
      lifecycle_state: lifecycle as StageOutcomeContract["lifecycle_state"],
      quality_state: projectedQuality as StageOutcomeContract["quality_state"],
      data_sufficiency_state: outcomeToken(source.data_sufficiency_state ?? source.dataSufficiencyState, OUTCOME_SUFFICIENCY) as StageOutcomeContract["data_sufficiency_state"] ?? outcomeSufficiency(legacyStatus, counts, finalCoverage, finalReasons),
      research_opportunity_state: (stage === "A1" ? finalOpportunity : researchOpportunity ?? "NOT_APPLICABLE") as StageOutcomeContract["research_opportunity_state"],
      focus_opportunity_state: (stage === "A2" ? finalOpportunity : focusOpportunity ?? "NOT_APPLICABLE") as StageOutcomeContract["focus_opportunity_state"],
      actionability_state: (stage === "A3" ? actionabilityForOpportunity(finalOpportunity as StageOutcomeContract["opportunity_state"]) : actionability ?? "NOT_APPLICABLE") as StageOutcomeContract["actionability_state"],
      opportunity_state: finalOpportunity as StageOutcomeContract["opportunity_state"],
      publication_state: projectedPublication as StageOutcomeContract["publication_state"],
      reason_codes: finalReasons,
      counts,
      data_coverage: finalCoverage,
      legacy_status: legacyStatus,
    };
  }
  const hasLegacySignal = source.status !== undefined || value.status !== undefined || source.legacy_status !== undefined || source.counts !== undefined;
  return hasLegacySignal ? legacyStageOutcome(value, stage) : null;
}

function aggregateLegacyLaneOutcome(value: JsonRecord, laneId: string, model: string | null, stages: readonly StageOutcomeContract[]): LaneOutcomeContract {
  const status = outcomeLegacyStatus(value, value);
  const last = stages[stages.length - 1] ?? legacyStageOutcome(value, "A3");
  const hasFailed = stages.some((stage) => stage.quality_state === "FAILED");
  const hasBlocked = stages.some((stage) => stage.quality_state === "BLOCKED" && !stageHasDataGap(stage));
  const hasDataGap = stages.some(stageHasDataGap);
  const hasDegraded = stages.some((stage) => stage.quality_state === "DEGRADED");
  const quality: LaneOutcomeContract["quality_state"] = hasFailed ? "FAILED" : hasBlocked ? "BLOCKED" : hasDegraded || hasDataGap ? "DEGRADED" : last.quality_state;
  const lifecycle: LaneOutcomeContract["lifecycle_state"] = stages.some((stage) => stage.lifecycle_state === "RUNNING") ? "RUNNING" : stages.some((stage) => stage.lifecycle_state === "QUEUED") ? "QUEUED" : "TERMINAL";
  const opportunity: LaneOutcomeContract["opportunity_state"] = stages.some((stage) => stage.opportunity_state === "PRESENT") ? "PRESENT" : stages.some((stage) => stage.opportunity_state === "UNKNOWN") ? "UNKNOWN" : stages.some((stage) => stage.opportunity_state === "ABSENT") ? "ABSENT" : "NOT_APPLICABLE";
  const publication: LaneOutcomeContract["publication_state"] = quality === "FAILED" || quality === "BLOCKED" ? "BLOCKED" : stages.some((stage) => stage.publication_state === "PUBLISHED") ? "PUBLISHED" : lifecycle === "TERMINAL" ? "READY" : "NOT_APPLICABLE";
  const reasons = outcomeReasons(value.reason_codes ?? value.reasonCodes);
  for (const stage of stages) for (const reason of stage.reason_codes) if (!reasons.includes(reason)) reasons.push(reason);
  const a1 = stages.find((stage) => stage.stage === "A1");
  const a2 = stages.find((stage) => stage.stage === "A2");
  const a3 = stages.find((stage) => stage.stage === "A3");
  const laneJobStatus: LaneOutcomeContract["job_status"] = hasFailed ? "FAILED" : stages.some((stage) => stage.job_status === "CANCELLED") ? "CANCELLED" : lifecycle === "RUNNING" ? "RUNNING" : lifecycle === "QUEUED" ? "QUEUED" : "SUCCEEDED";
  const sufficiencies = stages.map((stage) => stage.data_sufficiency_state);
  const dataSufficiency: LaneOutcomeContract["data_sufficiency_state"] = sufficiencies.includes("INSUFFICIENT") ? "INSUFFICIENT" : sufficiencies.includes("PARTIAL") ? "PARTIAL" : sufficiencies.includes("SUFFICIENT") ? "SUFFICIENT" : "NOT_APPLICABLE";
  const legacyLaneStatus = quality === "DEGRADED" && status === "BLOCKED" ? "READY_DEGRADED" : status || last.legacy_status;
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    lane_id: laneId,
    model,
    job_status: laneJobStatus,
    lifecycle_state: lifecycle,
    quality_state: quality,
    data_sufficiency_state: dataSufficiency,
    research_opportunity_state: a1?.research_opportunity_state ?? "NOT_APPLICABLE",
    focus_opportunity_state: a2?.focus_opportunity_state ?? "NOT_APPLICABLE",
    actionability_state: a3?.actionability_state ?? "NOT_APPLICABLE",
    opportunity_state: opportunity,
    publication_state: publication,
    reason_codes: reasons,
    counts: { stage_count: stages.length, completed_stages: stages.filter((stage) => stage.lifecycle_state === "TERMINAL").length },
    data_coverage: {},
    legacy_status: legacyLaneStatus,
    stages,
  };
}

/** Return only the safe four-axis lane outcome, preserving stage outcomes when present. */
export function normalizeLaneOutcome(value: unknown, fallbackLaneId = "UNKNOWN", fallbackModel: string | null = null): LaneOutcomeContract | null {
  if (!isRecord(value)) return null;
  const source = outcomeSource(value);
  const laneId = outcomeText(source.lane_id ?? source.laneId) ?? outcomeText(value.lane) ?? fallbackLaneId;
  const model = outcomeText(source.model) ?? outcomeText(value.model) ?? fallbackModel;
  const rawStages = Array.isArray(source.stages) ? source.stages : Array.isArray(value.stages) ? value.stages : [];
  const stages = rawStages.map((item, index) => normalizeStageOutcome(item, ["A1", "A2", "A3"][index] ?? "UNKNOWN")).filter((item): item is StageOutcomeContract => item !== null);
  const lifecycle = outcomeToken(source.lifecycle_state ?? source.lifecycleState, OUTCOME_LIFECYCLE);
  const quality = outcomeToken(source.quality_state ?? source.qualityState, OUTCOME_QUALITY);
  const opportunity = outcomeToken(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY);
  const researchOpportunity = outcomeToken(source.research_opportunity_state ?? source.researchOpportunityState, OUTCOME_OPPORTUNITY);
  const focusOpportunity = outcomeToken(source.focus_opportunity_state ?? source.focusOpportunityState, OUTCOME_OPPORTUNITY);
  const actionability = outcomeToken(source.actionability_state ?? source.actionabilityState, OUTCOME_ACTIONABILITY);
  const publication = outcomeToken(source.publication_state ?? source.publicationState, OUTCOME_PUBLICATION);
  if (lifecycle && quality && publication && (opportunity || researchOpportunity || focusOpportunity || actionability)) {
    const legacyStatus = outcomeLegacyStatus(value, source);
    const a1 = stages.find((stage) => stage.stage === "A1");
    const a2 = stages.find((stage) => stage.stage === "A2");
    const a3 = stages.find((stage) => stage.stage === "A3");
    const derivedOpportunity = opportunity ?? researchOpportunity ?? a1?.research_opportunity_state ?? "NOT_APPLICABLE";
    const dataGapOnly = quality === "BLOCKED" && stages.some(stageHasDataGap) && !stages.some((stage) => stage.quality_state === "FAILED" || stage.quality_state === "CANCELLED");
    const projectedQuality = dataGapOnly ? "DEGRADED" : quality;
    const projectedPublication = dataGapOnly && publication === "BLOCKED" && lifecycle === "TERMINAL" ? "READY" : publication;
    const sufficiency = outcomeToken(source.data_sufficiency_state ?? source.dataSufficiencyState, OUTCOME_SUFFICIENCY)
      ?? (stages.some((stage) => stage.data_sufficiency_state === "INSUFFICIENT") ? "INSUFFICIENT" : stages.some((stage) => stage.data_sufficiency_state === "PARTIAL") ? "PARTIAL" : stages.some((stage) => stage.data_sufficiency_state === "SUFFICIENT") ? "SUFFICIENT" : outcomeSufficiency(legacyStatus, outcomeCounts(source.counts), outcomeCoverage(source.data_coverage ?? source.dataCoverage), outcomeReasons(source.reason_codes ?? source.reasonCodes)));
    return {
      schema_version: OUTCOME_SCHEMA_VERSION,
      lane_id: laneId,
      model,
      job_status: outcomeToken(source.job_status ?? source.jobStatus, OUTCOME_JOB_STATUS) as LaneOutcomeContract["job_status"] ?? (stages.some((stage) => stage.job_status === "FAILED") ? "FAILED" : lifecycle === "RUNNING" ? "RUNNING" : lifecycle === "QUEUED" ? "QUEUED" : "SUCCEEDED"),
      lifecycle_state: lifecycle as LaneOutcomeContract["lifecycle_state"],
      quality_state: projectedQuality as LaneOutcomeContract["quality_state"],
      data_sufficiency_state: sufficiency as LaneOutcomeContract["data_sufficiency_state"],
      research_opportunity_state: (researchOpportunity ?? a1?.research_opportunity_state ?? "NOT_APPLICABLE") as LaneOutcomeContract["research_opportunity_state"],
      focus_opportunity_state: (focusOpportunity ?? a2?.focus_opportunity_state ?? "NOT_APPLICABLE") as LaneOutcomeContract["focus_opportunity_state"],
      actionability_state: (actionability ?? a3?.actionability_state ?? "NOT_APPLICABLE") as LaneOutcomeContract["actionability_state"],
      opportunity_state: derivedOpportunity as LaneOutcomeContract["opportunity_state"],
      publication_state: projectedPublication as LaneOutcomeContract["publication_state"],
      reason_codes: outcomeReasons(source.reason_codes ?? source.reasonCodes),
      counts: outcomeCounts(source.counts),
      data_coverage: outcomeCoverage(source.data_coverage ?? source.dataCoverage),
      legacy_status: dataGapOnly && outcomeLegacyStatus(value, source) === "BLOCKED" ? "READY_DEGRADED" : outcomeLegacyStatus(value, source),
      stages,
    };
  }
  const hasLegacySignal = source.status !== undefined || value.status !== undefined || source.legacy_status !== undefined || stages.length > 0;
  return hasLegacySignal ? aggregateLegacyLaneOutcome(value, laneId, model, stages) : null;
}

/** Return only the safe v3 run outcome; v2/legacy rows are projected once here. */
export function normalizeRunOutcome(value: unknown): RunOutcomeContract | null {
  if (!isRecord(value)) return null;
  const source = outcomeSource(value);
  const rawLanes = Array.isArray(source.lanes) ? source.lanes : Array.isArray(value.lanes) ? value.lanes : [];
  const lanes = rawLanes.map((item) => normalizeLaneOutcome(item)).filter((item): item is LaneOutcomeContract => item !== null);
  const lifecycle = outcomeToken(source.lifecycle_state ?? source.lifecycleState, OUTCOME_LIFECYCLE);
  const quality = outcomeToken(source.quality_state ?? source.qualityState, OUTCOME_QUALITY);
  const opportunity = outcomeToken(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY);
  const researchOpportunity = outcomeToken(source.research_opportunity_state ?? source.researchOpportunityState, OUTCOME_OPPORTUNITY);
  const focusOpportunity = outcomeToken(source.focus_opportunity_state ?? source.focusOpportunityState, OUTCOME_OPPORTUNITY);
  const actionability = outcomeToken(source.actionability_state ?? source.actionabilityState, OUTCOME_ACTIONABILITY);
  const publication = outcomeToken(source.publication_state ?? source.publicationState, OUTCOME_PUBLICATION);
  if (lifecycle && quality && publication && (opportunity || researchOpportunity || focusOpportunity || actionability)) {
    const primaryIds = (Array.isArray(source.primary_lane_ids) ? source.primary_lane_ids : Array.isArray(source.primaryLaneIds) ? source.primaryLaneIds : ["lane_1"])
      .map((item) => outcomeText(item, 80)).filter((item): item is string => item !== null);
    const primaryLane = lanes.find((lane) => primaryIds.includes(lane.lane_id));
    const primaryResearch = researchOpportunity ?? primaryLane?.research_opportunity_state ?? "NOT_APPLICABLE";
    const primaryFocus = focusOpportunity ?? primaryLane?.focus_opportunity_state ?? "NOT_APPLICABLE";
    const primaryActionability = actionability ?? primaryLane?.actionability_state ?? "NOT_APPLICABLE";
    const primaryDataGap = lanes.filter((lane) => primaryIds.includes(lane.lane_id)).some(laneHasDataGap);
    const primaryTechnicalFailure = lanes.filter((lane) => primaryIds.includes(lane.lane_id)).some((lane) => lane.quality_state === "FAILED" || lane.quality_state === "CANCELLED");
    const projectedQuality = quality === "BLOCKED" && primaryDataGap && !primaryTechnicalFailure ? "DEGRADED" : quality;
    const projectedPublication = projectedQuality === "DEGRADED" && publication === "BLOCKED" && lifecycle === "TERMINAL" ? "READY" : publication;
    return {
      schema_version: OUTCOME_SCHEMA_VERSION,
      run_id: outcomeText(source.run_id ?? source.runId) ?? outcomeText(value.run_id ?? value.runId),
      job_status: outcomeToken(source.job_status ?? source.jobStatus, OUTCOME_JOB_STATUS) as RunOutcomeContract["job_status"] ?? (quality === "FAILED" ? "FAILED" : quality === "CANCELLED" ? "CANCELLED" : lifecycle === "RUNNING" ? "RUNNING" : lifecycle === "QUEUED" ? "QUEUED" : "SUCCEEDED"),
      lifecycle_state: lifecycle as RunOutcomeContract["lifecycle_state"],
      quality_state: projectedQuality as RunOutcomeContract["quality_state"],
      data_sufficiency_state: outcomeToken(source.data_sufficiency_state ?? source.dataSufficiencyState, OUTCOME_SUFFICIENCY) as RunOutcomeContract["data_sufficiency_state"] ?? (primaryLane?.data_sufficiency_state ?? "NOT_APPLICABLE"),
      research_opportunity_state: primaryResearch as RunOutcomeContract["research_opportunity_state"],
      focus_opportunity_state: primaryFocus as RunOutcomeContract["focus_opportunity_state"],
      actionability_state: primaryActionability as RunOutcomeContract["actionability_state"],
      publication_state: projectedPublication as RunOutcomeContract["publication_state"],
      reason_codes: outcomeReasons(source.reason_codes ?? source.reasonCodes),
      counts: outcomeCounts(source.counts),
      data_coverage: outcomeCoverage(source.data_coverage ?? source.dataCoverage),
      legacy_status: outcomeLegacyStatus(value, source),
      primary_lane_ids: primaryIds.length ? primaryIds : ["lane_1"],
      comparison_status: outcomeText(source.comparison_status ?? source.comparisonStatus)?.toUpperCase() ?? "NOT_RUN",
      lanes,
    };
  }
  const hasLegacySignal = source.status !== undefined || value.status !== undefined || source.legacy_status !== undefined || lanes.length > 0;
  if (!hasLegacySignal) return null;
  const status = outcomeLegacyStatus(value, source);
  const primary = lanes.filter((lane) => lane.lane_id === "lane_1");
  const selected = primary[0] ?? (lanes[0] ?? null);
  const fallbackStage = legacyStageOutcome(value, "A3");
  const lane = selected ?? aggregateLegacyLaneOutcome(value, "lane_1", null, [fallbackStage]);
  const reasons = outcomeReasons(source.reason_codes ?? source.reasonCodes ?? value.reason_codes ?? value.reasonCodes);
  for (const reason of lane.reason_codes) if (!reasons.includes(reason)) reasons.push(reason);
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    run_id: outcomeText(source.run_id ?? source.runId) ?? outcomeText(value.run_id ?? value.runId),
    job_status: lane.job_status,
    lifecycle_state: lane.lifecycle_state,
    quality_state: lane.quality_state,
    data_sufficiency_state: lane.data_sufficiency_state,
    research_opportunity_state: lane.research_opportunity_state,
    focus_opportunity_state: lane.focus_opportunity_state,
    actionability_state: lane.actionability_state,
    opportunity_state: lane.opportunity_state,
    publication_state: lane.publication_state,
    reason_codes: reasons,
    counts: outcomeCounts(source.counts, value),
    data_coverage: outcomeCoverage(source.data_coverage ?? source.dataCoverage),
    legacy_status: status,
    primary_lane_ids: ["lane_1"],
    comparison_status: lanes.length > 1 ? "READY" : "NOT_RUN",
    lanes,
  };
}

function boundedText(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const text = value.trim();
  return text ? text.slice(0, DETAIL_TEXT_MAX_LENGTH) : null;
}

function boundedStringList(value: unknown): string[] {
  const output: string[] = [];
  const visit = (item: unknown, depth: number): void => {
    if (depth > 2 || output.length >= 100) return;
    if (typeof item === "string") {
      const text = boundedText(item);
      if (text && !output.includes(text)) output.push(text);
      return;
    }
    // Prompt fields are allowed to be either a scalar string or an array of
    // strings.  Flatten one extra array level for hand-authored/legacy
    // artifacts, but never stringify arbitrary objects into the response.
    if (Array.isArray(item)) {
      for (const child of item) visit(child, depth + 1);
    }
  };
  visit(value, 0);
  return output;
}

function hasAnyKey(value: JsonRecord, keys: readonly string[]): boolean {
  return keys.some((key) => Object.prototype.hasOwnProperty.call(value, key));
}

function hasPresentKey(value: JsonRecord, keys: readonly string[]): boolean {
  return keys.some((key) => Object.prototype.hasOwnProperty.call(value, key) && value[key] !== null && value[key] !== undefined);
}

function boundedJsonValue(value: unknown): JsonValue | null {
  if (value === undefined) return null;
  const safe = sanitizeJson(value);
  const limit = (item: JsonValue, depth: number): JsonValue => {
    if (depth > 5) return "[TRUNCATED]";
    if (typeof item === "string") return item.slice(0, DETAIL_TEXT_MAX_LENGTH);
    if (Array.isArray(item)) return item.slice(0, 50).map((child) => limit(child, depth + 1));
    if (item !== null && typeof item === "object") {
      const result: JsonRecord = {};
      for (const [key, child] of Object.entries(item).slice(0, 40)) {
        result[key.slice(0, 120)] = limit(child as JsonValue, depth + 1);
      }
      return result as JsonValue;
    }
    return item;
  };
  return limit(safe, 0);
}

function firstJsonValue(record: JsonRecord, keys: readonly string[]): JsonValue | null {
  for (const key of keys) {
    if (!Object.prototype.hasOwnProperty.call(record, key)) continue;
    const value = boundedJsonValue(record[key]);
    if (value !== null) return value;
  }
  return null;
}

function firstString(record: JsonRecord, keys: readonly string[]): string | null {
  for (const key of keys) {
    const value = boundedText(record[key]);
    if (value) return value;
  }
  return null;
}

function firstNumber(record: JsonRecord, keys: readonly string[]): number | null {
  for (const key of keys) {
    const value = numberValue(record[key]);
    if (value !== null) return value;
  }
  return null;
}

function normalizedSymbol(value: JsonRecord): string | null {
  const direct = firstString(value, ["symbol", "thscode"]);
  if (direct) return direct;
  const code = firstString(value, ["code", "ticker"]);
  const exchange = firstString(value, ["exchange", "market"]);
  return code ? (exchange ? `${code}.${exchange.toUpperCase()}` : code) : null;
}

function rawArray(value: unknown): readonly unknown[] {
  return Array.isArray(value) ? value : [];
}

function asSafeJson(value: unknown): JsonValue | null {
  return value === undefined ? null : sanitizeJson(value);
}

function industryText(value: JsonRecord): string | null {
  const direct = firstString(value, ["industry", "industry_name", "primary_industry"]);
  if (direct) return direct;
  const industries = rawArray(value.ths_industries);
  const names = industries
    .map((item) => isRecord(item) ? firstString(item, ["industry_name", "name", "industry"]) : null)
    .filter((item): item is string => item !== null);
  return names.length ? [...new Set(names)].join(" / ") : null;
}

function collectReasonCodes(value: JsonRecord): string[] {
  return collectTextFields(value, [
    "reason_codes",
    "reasonCodes",
    "reason_code",
    "reasonCode",
    "system_reason_codes",
    "systemReasonCodes",
    "decision_reason_codes",
    "decisionReasonCodes",
  ]);
}

function collectTextFields(value: JsonRecord, keys: readonly string[]): string[] {
  const output: string[] = [];
  for (const key of keys) {
    for (const item of boundedStringList(value[key])) {
      if (!output.includes(item)) output.push(item);
    }
  }
  return output;
}

function collectStructuredTextFields(
  value: JsonRecord,
  keys: readonly string[],
  objectKeys: readonly string[] = ["text", "summary", "reason", "description", "claim", "condition", "evidence"],
): string[] {
  const output = collectTextFields(value, keys);
  for (const key of keys) {
    const values = rawArray(value[key]);
    for (const item of values) {
      if (!isRecord(item)) continue;
      for (const objectKey of objectKeys) {
        for (const text of boundedStringList(item[objectKey])) {
          if (!output.includes(text)) output.push(text);
        }
      }
    }
  }
  return output.slice(0, 100);
}

function optionalRecord(value: unknown): JsonRecord | null {
  return isRecord(value) ? value : null;
}

function lineageValue(value: JsonRecord): JsonValue | null {
  const fields: JsonRecord = {};
  const sources = [value, optionalRecord(value.lineage)].filter((item): item is JsonRecord => Boolean(item));
  const mappings: readonly [string, readonly string[]][] = [
    ["candidateId", ["candidate_id", "candidateId"]],
    ["upstreamCandidateId", ["upstream_candidate_id", "upstreamCandidateId"]],
    ["parentCandidateId", ["parent_candidate_id", "parentCandidateId"]],
    ["candidateOrigin", ["candidate_origin", "candidateOrigin", "origin"]],
    ["origin", ["origin"]],
    ["themeId", ["theme_id", "themeId"]],
    ["parentThemeId", ["parent_theme_id", "parentThemeId"]],
    ["nodeId", ["node_id", "nodeId", "industry_chain_node", "industryChainNode"]],
    ["marketRole", ["market_role", "marketRole"]],
    ["route", ["a2_route", "selection_route", "route"]],
  ];
  for (const [outputKey, keys] of mappings) {
    for (const source of sources) {
      const field = firstString(source, keys);
      if (field) {
        fields[outputKey] = field;
        break;
      }
    }
  }
  return Object.keys(fields).length ? sanitizeJson(fields) : null;
}

function timeframeStates(value: JsonRecord): JsonValue | null {
  const states: JsonRecord = {};
  const stateKeys = [
    "weekly_state", "daily_state", "m120_state", "m15_state", "m5_state",
    "state_120m", "state_15m", "state_m120", "state_m15", "state_m5",
    "weekly", "daily", "m120", "m15", "m5",
  ] as const;
  for (const key of stateKeys) {
    if (value[key] !== undefined) states[key] = value[key];
  }
  if (value.ma_analysis !== undefined) states.maAnalysis = value.ma_analysis;
  return Object.keys(states).length ? sanitizeJson(states) : null;
}

function planValue(value: JsonRecord): ResearchStageDetailPlan | null {
  const source = optionalRecord(value.plan) ?? value;
  const trigger = optionalRecord(source.trigger_zone ?? source.triggerZone);
  const triggerZone = trigger
    ? sanitizeJson({ low: numberValue(trigger.low), high: numberValue(trigger.high) })
    : null;
  const confirmationConditions = collectStructuredTextFields(source, ["confirmation_conditions", "confirmationConditions"]);
  const scenarios = source.scenarios === undefined ? null : asSafeJson(source.scenarios);
  const timeframe = timeframeStates(source);
  const plan: ResearchStageDetailPlan = {
    setupType: firstString(source, ["setup_type", "setupType"]),
    planHash: firstString(source, ["plan_hash", "planHash"]),
    triggerZone,
    invalidationLevel: firstNumber(source, ["invalidation_level", "invalidationLevel"]),
    rewardRisk: firstNumber(source, ["reward_risk", "rewardRisk"]),
    stopDistancePct: firstNumber(source, ["stop_distance_pct", "stopDistancePct"]),
    riskUnit: firstNumber(source, ["risk_unit", "riskUnit"]) ?? firstString(source, ["risk_unit", "riskUnit"]),
    firstResistance: firstNumber(source, ["first_resistance", "firstResistance"]),
    noChaseCondition: firstString(source, ["no_chase_condition", "noChaseCondition"]),
    technicalScore: firstNumber(source, ["technical_score", "technicalScore"]),
    counterTrendProbe: typeof source.counter_trend_probe === "boolean" ? source.counter_trend_probe : typeof source.counterTrendProbe === "boolean" ? source.counterTrendProbe : null,
    overExtended: typeof source.over_extended === "boolean" ? source.over_extended : typeof source.overExtended === "boolean" ? source.overExtended : null,
    atrExtension: firstNumber(source, ["atr_extension", "atrExtension"]),
    maBiasMax: firstNumber(source, ["ma_bias_max", "maBiasMax"]),
    relativeStrengthRank: firstNumber(source, ["relative_strength_rank", "relativeStrengthRank"]),
    allowedTimeWindows: asSafeJson(source.allowed_time_windows ?? source.allowedTimeWindows),
    maAnalysis: asSafeJson(source.ma_analysis ?? source.maAnalysis),
    klinePattern: firstString(source, ["kline_pattern", "klinePattern"]),
    factorSnapshotHash: firstString(source, ["factor_snapshot_hash", "factorSnapshotHash"]),
    configHash: firstString(source, ["config_hash", "configHash"]),
    planId: firstString(source, ["plan_id", "planId"]),
    planExpiry: firstString(source, ["plan_expiry", "planExpiry"]),
    confirmationConditions,
    scenarios,
    timeframeStates: timeframe,
  };
  return plan.setupType || plan.planHash || plan.triggerZone || plan.invalidationLevel !== null || plan.rewardRisk !== null
    || plan.stopDistancePct !== null || plan.riskUnit !== null || plan.firstResistance !== null
    || plan.noChaseCondition || plan.technicalScore !== null || plan.counterTrendProbe !== null
    || plan.overExtended !== null || plan.atrExtension !== null || plan.maBiasMax !== null
    || plan.relativeStrengthRank !== null || plan.allowedTimeWindows || plan.maAnalysis || plan.klinePattern
    || plan.factorSnapshotHash || plan.configHash || plan.planId || plan.planExpiry
    || plan.confirmationConditions.length || plan.scenarios || plan.timeframeStates
    ? plan
    : null;
}

interface NameCatalogEntry {
  readonly name: string;
  readonly source: "lane_a1" | "snapshot";
  readonly theme: string | null;
  readonly industry: string | null;
}

function addNamesFromArray(catalog: Map<string, NameCatalogEntry>, value: unknown, source: NameCatalogEntry["source"]): void {
  for (const item of rawArray(value)) {
    if (!isRecord(item)) continue;
    const symbol = normalizedSymbol(item);
    const name = firstString(item, ["company_name", "name", "sec_name"]);
    if (!symbol || !name) continue;
    const entry: NameCatalogEntry = {
      name,
      source,
      theme: firstString(item, ["primary_theme", "theme", "theme_id", "themeId"]),
      industry: industryText(item),
    };
    const previous = catalog.get(symbol);
    if (!previous) {
      catalog.set(symbol, entry);
    } else if ((!previous.theme && entry.theme) || (!previous.industry && entry.industry)) {
      catalog.set(symbol, {
        ...previous,
        theme: previous.theme ?? entry.theme,
        industry: previous.industry ?? entry.industry,
      });
    }
  }
}

type FactField = readonly [string, readonly string[]];

const FACTOR_CONTAINERS = ["a2_factor_scores", "factor_scores", "factors", "factor_details"] as const;

function firstFact(value: JsonRecord, keys: readonly string[], nestedKeys: readonly string[] = []): JsonValue | null {
  const direct = firstJsonValue(value, keys);
  if (direct !== null) return direct;
  if (!nestedKeys.length) return null;
  for (const containerKey of FACTOR_CONTAINERS) {
    const container = optionalRecord(value[containerKey]);
    if (!container) continue;
    const nested = firstJsonValue(container, nestedKeys);
    if (nested !== null) return nested;
  }
  return null;
}

function factObject(value: JsonRecord, fields: readonly FactField[]): JsonValue | null {
  const result: JsonRecord = {};
  for (const [outputKey, keys] of fields) {
    const fact = firstJsonValue(value, keys);
    if (fact !== null) result[outputKey] = fact;
  }
  return Object.keys(result).length ? result as JsonValue : null;
}

/**
 * Project only decision facts that the UI and audit contract understand.
 * Keeping this list explicit is important: lane artifacts may contain model
 * payloads, but those payloads must never become an accidental API surface.
 */
function decisionFactsValue(value: JsonRecord): ResearchDecisionFacts {
  const industryChainRole = firstFact(
    value,
    ["industry_chain_role", "industryChainRole", "chain_role", "chainRole"],
    ["industry_chain_role", "chain_role"],
  ) ?? factObject(value, [
    ["nodeId", ["industry_chain_node", "industryChainNode", "node_id", "nodeId"]],
    ["valueChainPosition", ["value_chain_position", "valueChainPosition"]],
    ["route", ["a2_route", "selection_route", "route"]],
  ]);
  const marketRole = firstFact(value, ["market_role", "marketRole", "leader_role", "leaderRole"] , ["market_role", "leader_role"]);
  const supplyChainRole = firstFact(value, ["supply_chain_role", "supplyChainRole", "value_chain_position", "valueChainPosition"]);
  const businessExposure = firstFact(
    value,
    ["business_exposure", "businessExposure", "business_exposure_facts", "businessExposureFacts", "revenue_exposure_pct", "revenueExposurePct", "gross_profit_exposure_pct", "grossProfitExposurePct", "maximum_revenue_exposure_pct", "maximumRevenueExposurePct", "business_purity", "businessPurity"],
  );
  const financialTransmission = firstFact(value, ["financial_transmission", "financialTransmission", "financial_features", "financialFeatures"]);
  const capitalFlow = firstFact(
    value,
    ["capital_flow", "capitalFlow", "fund_flow", "fundFlow", "capital_flow_score", "capitalFlowScore", "capital_score", "capitalScore", "fund_flow_score", "fundFlowScore", "net_inflow", "netInflow", "main_net_inflow", "mainNetInflow"],
    ["capital_flow", "fund_flow", "capital_score"],
  );
  const tierStructure = firstFact(
    value,
    ["tier_structure", "tierStructure", "tier_position", "tierPosition", "tier", "ladder", "tier_table", "tierTable", "tier_height", "tierHeight", "tier_width", "tierWidth", "consecutive_limit_ups", "consecutiveLimitUps", "limit_up_count", "limitUpCount"],
    ["tier_structure", "tier", "ladder"],
  );
  const leaderStructure = firstFact(
    value,
    ["leader_structure", "leaderStructure", "leader_subtype", "leaderSubtype", "identifiability_score", "identifiabilityScore"],
    ["leader_structure", "leader_subtype", "identifiability_score"],
  );
  const crowding = firstFact(
    value,
    ["crowding", "crowding_score", "crowdingScore", "chase_risk_level", "chaseRiskLevel", "crowding_flags", "crowdingFlags", "overcrowding", "overCrowding"],
    ["crowding", "crowding_score"],
  );
  const technicalCycle = firstFact(
    value,
    ["technical_cycle", "technicalCycle", "technical_state", "technicalState"],
    ["technical_cycle", "technical_state"],
  ) ?? factObject(value, [
    ["technicalScore", ["technical_score", "technicalScore"]],
    ["maAnalysis", ["ma_analysis", "maAnalysis"]],
    ["klinePattern", ["kline_pattern", "klinePattern"]],
    ["weekly", ["weekly_state", "weeklyState", "weekly"]],
    ["daily", ["daily_state", "dailyState", "daily"]],
    ["m120", ["state_120m", "state120m", "m120_state", "m120State", "m120"]],
    ["m15", ["state_15m", "state15m", "m15_state", "m15State", "m15"]],
    ["m5", ["m5_state", "m5State", "m5"]],
  ]);
  const weeklyConfirmation = firstFact(
    value,
    ["weekly_confirmation", "weeklyConfirmation", "weekly_confirmation_score", "weeklyConfirmationScore", "weekly_momentum_state", "weeklyMomentumState", "weekly_state", "weeklyState"],
    ["weekly_confirmation", "weekly_confirmation_score"],
  );
  const indexChainResonance = firstFact(
    value,
    ["index_chain_resonance", "indexChainResonance", "index_chain_resonance_score", "indexChainResonanceScore", "chain_resonance_score", "chainResonanceScore"],
    ["index_chain_resonance", "chain_resonance_score"],
  );
  return {
    industryChainRole,
    marketRole,
    supplyChainRole,
    businessExposure,
    financialTransmission,
    capitalFlow,
    tierStructure,
    leaderStructure,
    crowding,
    technicalCycle,
    weeklyConfirmation,
    indexChainResonance,
  };
}

function sourceReferenceValues(value: JsonRecord): JsonValue[] {
  const output: JsonValue[] = [];
  const seen = new Set<string>();
  const keys = [
    "source_refs", "sourceRefs", "source_ref", "sourceRef",
    "supporting_source_refs", "supportingSourceRefs", "base_source_refs", "baseSourceRefs",
    "theme_source_refs", "themeSourceRefs", "node_source_refs", "nodeSourceRefs",
    "evidence_ref", "evidenceRef",
  ] as const;
  const append = (raw: unknown, depth: number): void => {
    if (depth > 2 || output.length >= 100) return;
    if (Array.isArray(raw)) {
      for (const child of raw) append(child, depth + 1);
      return;
    }
    const safe = boundedJsonValue(raw);
    if (safe === null) return;
    const key = JSON.stringify(safe);
    if (key && !seen.has(key)) {
      seen.add(key);
      output.push(safe);
    }
  };
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(value, key)) append(value[key], 0);
  }
  return output;
}

function detailMissingFields(
  value: JsonRecord,
  stage: ResearchStage,
  pool: ResearchPool,
  item: {
    readonly name: string | null;
    readonly theme: string | null;
    readonly industry: string | null;
    readonly score: number | null;
    readonly selectionReasons: readonly string[];
    readonly reasonCodes: readonly string[];
    readonly evidence: readonly string[];
    readonly risks: readonly string[];
    readonly invalidation: readonly string[];
    readonly sourceRefs: readonly JsonValue[];
    readonly lineage: JsonValue | null;
    readonly decisionFacts: ResearchDecisionFacts;
    readonly plan: ResearchStageDetailPlan | null;
  },
): string[] {
  const missing: string[] = [];
  const mark = (field: string, available: boolean): void => {
    if (!available && !missing.includes(field)) missing.push(field);
  };
  if (pool === "rejected") {
    mark("name", Boolean(item.name));
    mark("reasonCodes", item.reasonCodes.length > 0 || hasAnyKey(value, [
      "reason_codes", "reasonCodes", "reason_code", "reasonCode", "system_reason_codes", "systemReasonCodes",
      "veto_triggered", "vetoTriggered",
    ]));
    return missing;
  }
  // These are display/audit fields, not eligibility gates.  A missing value
  // is surfaced to the UI instead of being replaced with a guess.
  mark("name", Boolean(item.name));
  mark("themeOrIndustry", Boolean(item.theme || item.industry));
  mark("score", item.score !== null);
  mark("selectionReasonsOrReasonCodes", item.selectionReasons.length > 0 || item.reasonCodes.length > 0);
  if (stage === "A1") {
    if (hasAnyKey(value, ["business_exposure", "businessExposure", "business_exposure_facts", "businessExposureFacts"])) {
      mark("decisionFacts.businessExposure", item.decisionFacts.businessExposure !== null);
    }
    if (hasAnyKey(value, ["financial_transmission", "financialTransmission", "financial_features", "financialFeatures"])) {
      mark("decisionFacts.financialTransmission", item.decisionFacts.financialTransmission !== null);
    }
    if (hasAnyKey(value, [
      "source_refs", "sourceRefs", "source_ref", "sourceRef", "supporting_source_refs", "supportingSourceRefs",
      "base_source_refs", "baseSourceRefs", "theme_source_refs", "themeSourceRefs", "node_source_refs", "nodeSourceRefs",
      "evidence_ref", "evidenceRef",
    ])) mark("sourceRefs", item.sourceRefs.length > 0 || hasPresentKey(value, [
      "source_refs", "sourceRefs", "source_ref", "sourceRef", "supporting_source_refs", "supportingSourceRefs",
      "base_source_refs", "baseSourceRefs", "theme_source_refs", "themeSourceRefs", "node_source_refs", "nodeSourceRefs",
      "evidence_ref", "evidenceRef",
    ]));
  }
  if (stage === "A2") {
    mark("industry", Boolean(item.industry));
    mark("decisionFacts.marketRole", item.decisionFacts.marketRole !== null);
    mark("decisionFacts.supplyChainRole", item.decisionFacts.supplyChainRole !== null);
    mark("decisionFacts.capitalFlow", item.decisionFacts.capitalFlow !== null);
    mark("decisionFacts.tierStructure", item.decisionFacts.tierStructure !== null);
    mark("decisionFacts.leaderStructure", item.decisionFacts.leaderStructure !== null);
    mark("decisionFacts.crowding", item.decisionFacts.crowding !== null);
    mark("decisionFacts.indexChainResonance", item.decisionFacts.indexChainResonance !== null);
  }
  if (stage === "A3") {
    mark("decisionFacts.technicalCycle", item.decisionFacts.technicalCycle !== null);
    mark("decisionFacts.weeklyConfirmation", item.decisionFacts.weeklyConfirmation !== null);
  }
  // Only an A3 approved row is expected to contain an executable plan.  A3
  // watch/rejected rows are valid without one and remain auditable through
  // their reason/evidence fields.
  if (stage === "A3" && pool === "approved") mark("plan", item.plan !== null);
  return missing;
}

function rootChild(rootDir: string, candidate: string): string | null {
  const root = resolve(rootDir);
  const path = resolve(candidate);
  const child = relative(root, path);
  if (child === "" || child === ".." || child.startsWith(`..${sep}`) || child.startsWith("../") || child.startsWith("..\\") || isAbsolute(child)) return null;
  return path;
}

function resolveSafeReference(rootDir: string, value: string): string | null {
  if (!value || value.includes("\0")) return null;
  if (isAbsolute(value)) return rootChild(rootDir, value);
  const safe = resolveWithinRoot(rootDir, value);
  return safe ? rootChild(rootDir, safe) : null;
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

function progressJobStatus(value: unknown): OutcomeJobStatus | null {
  const token = progressString(value)?.toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  return token && OUTCOME_JOB_STATUS.has(token) ? token as OutcomeJobStatus : null;
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

const TERMINAL_PROGRESS_STATUSES = new Set<WorkflowProgressStatus>([
  "COMPLETED",
  "READY",
  "READY_DEGRADED",
  "PARTIAL",
  "BLOCKED",
  "FAILED",
  "SUCCEEDED",
  "CANCELLED",
  "VALIDATED",
  "VALIDATED_NO_OPPORTUNITY",
  "VALIDATED_NO_ACTION",
  "VALIDATED_NO_SETUP",
  "DEGRADED_UNDERFILLED_DATA_GAP",
  "VALIDATED_UNDERFILLED_MARKET",
  "NOT_RUN",
]);

interface ProgressTerminalContext {
  readonly terminal: boolean;
  readonly status: WorkflowProgressStatus;
  readonly jobStatus: OutcomeJobStatus | null;
}

function terminalProgressJobStatus(
  status: WorkflowProgressStatus,
  jobStatus: OutcomeJobStatus | null,
  phase: string | null,
): OutcomeJobStatus | null {
  if (jobStatus && jobStatus !== "RUNNING" && jobStatus !== "QUEUED") return jobStatus;
  if (status === "FAILED" || (status === "BLOCKED" && phase === "FAILED")) return "FAILED";
  if (status === "CANCELLED") return "CANCELLED";
  if (TERMINAL_PROGRESS_STATUSES.has(status)) return "SUCCEEDED";
  return jobStatus;
}

function terminalProgressStatus(status: WorkflowProgressStatus): WorkflowProgressStatus {
  return status === "RUNNING" ? "COMPLETED" : status;
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

function progressDiagnosticType(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const candidate = value.trim().toLowerCase();
  return SAFE_PROGRESS_DIAGNOSTIC_TYPES.has(candidate) ? candidate : null;
}

function progressDiagnosticFields(value: unknown): string[] {
  const values = asArray(value);
  if (!values) return [];
  const fields: string[] = [];
  for (const item of values) {
    if (typeof item !== "string") continue;
    const field = item.trim();
    if (!SAFE_PROGRESS_DIAGNOSTIC_FIELDS.has(field) || fields.includes(field)) continue;
    fields.push(field);
    if (fields.length >= MAX_PROGRESS_DIAGNOSTIC_FIELDS) break;
  }
  return fields;
}

function progressDiagnosticCount(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Math.min(MAX_PROGRESS_COUNT, Math.max(0, Math.floor(value)));
}

function normalizeProgressDiagnostics(value: unknown): WorkflowProgressDiagnostics | null {
  if (!isRecord(value)) return null;

  const rawShape = isRecord(value.last_invalid_output_shape)
    ? value.last_invalid_output_shape
    : isRecord(value.lastInvalidOutputShape) ? value.lastInvalidOutputShape : null;
  let lastInvalidOutputShape: WorkflowProgressDiagnostics["lastInvalidOutputShape"] = null;
  if (rawShape) {
    const fields = progressDiagnosticFields(rawShape.fields);
    const shape = {
      type: progressDiagnosticType(rawShape.type),
      fields,
      unknownFieldCount: progressDiagnosticCount(rawShape.unknown_field_count ?? rawShape.unknownFieldCount),
      envelopeUnknownFieldCount: progressDiagnosticCount(rawShape.envelope_unknown_field_count ?? rawShape.envelopeUnknownFieldCount),
    };
    if (shape.type !== null || fields.length > 0 || shape.unknownFieldCount !== null || shape.envelopeUnknownFieldCount !== null) {
      lastInvalidOutputShape = shape;
    }
  }

  const semanticAttempts = progressDiagnosticCount(value.semantic_attempts ?? value.semanticAttempts);
  const themeCount = progressDiagnosticCount(value.theme_count ?? value.themeCount);
  const nodeCount = progressDiagnosticCount(value.node_count ?? value.nodeCount);
  const mappingCount = progressDiagnosticCount(value.mapping_count ?? value.mappingCount);
  const expectedMappingCount = progressDiagnosticCount(value.expected_mapping_count ?? value.expectedMappingCount);
  let missingMappingCount = progressDiagnosticCount(value.missing_mapping_count ?? value.missingMappingCount);
  if (missingMappingCount === null) {
    const codes = asArray(value.missing_mapping_codes ?? value.missingMappingCodes);
    if (codes) {
      let count = 0;
      for (let index = 0; index < Math.min(codes.length, MAX_PROGRESS_DIAGNOSTIC_ITEMS); index += 1) {
        const code = codes[index];
        if (typeof code === "string" && code.trim()) count += 1;
      }
      missingMappingCount = count;
    }
  }
  const hasSafeValue = lastInvalidOutputShape !== null
    || [semanticAttempts, themeCount, nodeCount, mappingCount, expectedMappingCount, missingMappingCount].some((item) => item !== null);
  if (!hasSafeValue) return null;
  return {
    lastInvalidOutputShape,
    semanticAttempts,
    themeCount,
    nodeCount,
    mappingCount,
    expectedMappingCount,
    missingMappingCount,
  };
}

function validateProgressShape(source: JsonRecord): boolean {
  const stringKeys = ["status", "job_status", "jobStatus", "phase", "current_phase", "run_id", "runId", "updated_at", "updatedAt", "time", "started_at", "startedAt", "phase_started_at", "phaseStartedAt"];
  if (stringKeys.some((key) => key in source && typeof source[key] !== "string")) return false;
  const collection = source.lanes ?? source.lane_progress ?? source.laneProgress;
  if (!validateProgressCollection(collection, false)) return false;
  for (const key of ["progress", "metrics", "data", "data_sync", "dataSync", "resources"]) {
    if (key in source && !isRecord(source[key])) return false;
    if (isRecord(source[key]) && !validateProgressMetrics(source[key])) return false;
  }
  return validateProgressMetrics(source);
}

function validateProgressMetrics(source: JsonRecord): boolean {
  for (const key of NUMERIC_PROGRESS_KEYS) {
    if (!(key in source)) continue;
    const value = source[key];
    if (value === null) continue;
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

function normalizeStage(
  stageKey: string | null,
  stage: JsonRecord,
  terminalContext: ProgressTerminalContext | null = null,
): WorkflowProgressStage {
  const stageName = progressPhase(stage.stage ?? stage.stage_id ?? stage.stageId ?? stage.name ?? stageKey) ?? "UNKNOWN";
  const nested = nestedRecord(stage, ["progress", "batch", "batch_progress", "batchProgress", "counts"]);
  const sources = nested ? [stage, nested] : [stage];
  const processed = metricFromSources(sources, ["processed_symbols", "processed", "processed_count", "completed", "completed_count", "current", "done"], "count");
  const total = metricFromSources(sources, ["total_symbols", "total", "total_count", "universe_total"], "count");
  const batchProcessed = metricFromSources(sources, ["batch_processed", "batch_completed", "completed_batches", "current_batch", "processed_batches", "processed", "completed"], "count");
  const batchTotal = metricFromSources(sources, ["batch_total", "batch_count", "total_batches", "total"], "count");
  const selected = metricFromSources(sources, ["selected_symbols"], "count");
  const monitor = metricFromSources(sources, ["monitor_symbols"], "count");
  const rejected = metricFromSources(sources, ["rejected_symbols"], "count");
  const industryCount = metricFromSources(sources, ["industry_count"], "count");
  const monthlyDecisionCount = metricFromSources(sources, ["monthly_decision_count"], "count");
  const themeCount = metricFromSources(sources, ["theme_count"], "count");
  const nodeCount = metricFromSources(sources, ["node_count"], "count");
  const mappingCount = metricFromSources(sources, ["mapping_count"], "count");
  const diagnostics = normalizeProgressDiagnostics(stage.diagnostics);
  const rawStatus = progressString(stage.status) ? progressStatus(stage.status) : null;
  const hasIncompleteAggregate = (
    processed.value !== null && total.value !== null && total.value > 0 && processed.value < total.value
  ) || (
    batchProcessed.value !== null && batchTotal.value !== null && batchTotal.value > 0 && batchProcessed.value < batchTotal.value
  );
  const rawStageStatus = rawStatus;
  const stageStatus = rawStageStatus === "COMPLETED" && hasIncompleteAggregate
    ? terminalContext?.terminal ? "PARTIAL" : "RUNNING"
    : terminalContext?.terminal && rawStageStatus === "RUNNING"
      ? terminalProgressStatus(terminalContext.status)
      : rawStageStatus;
  const rawStageJobStatus = progressJobStatus(stage.job_status ?? stage.jobStatus);
  const stageJobStatus = terminalContext?.terminal
    && (rawStageJobStatus === null || rawStageJobStatus === "RUNNING" || rawStageJobStatus === "QUEUED")
    ? terminalContext.jobStatus
    : rawStageJobStatus;
  return {
    stage: stageName,
    // A completed latest batch does not mean the aggregate stage is complete.
    // Do not present an incomplete 10/248 aggregate as a completed stage.
    status: stageStatus,
    jobStatus: stageJobStatus,
    processed: processed.value,
    total: total.value,
    batchProcessed: batchProcessed.value,
    batchTotal: batchTotal.value,
    selected: selected.value,
    monitor: monitor.value,
    rejected: rejected.value,
    industryCount: industryCount.value,
    monthlyDecisionCount: monthlyDecisionCount.value,
    themeCount: themeCount.value,
    nodeCount: nodeCount.value,
    mappingCount: mappingCount.value,
    diagnostics,
    updatedAt: progressTime(stage.updated_at ?? stage.updatedAt ?? stage.time),
  };
}

function normalizeLane(
  laneKey: string | null,
  lane: JsonRecord,
  terminalContext: ProgressTerminalContext | null = null,
): WorkflowProgressLane | null {
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
    .map(({ key, value }) => normalizeStage(key, value, terminalContext))
    .filter((stage, index, all) => all.findIndex((item) => item.stage === stage.stage) === index)
    .slice(0, 16);
  const directStage = lane.current_stage ?? lane.currentStage ?? lane.stage;
  if (stages.length === 0 && progressPhase(directStage)) {
    stages.push(normalizeStage(null, { ...lane, stage: directStage }));
  }
  const currentStageName = progressPhase(directStage);
  const currentStage = stages.find((stage) => stage.stage === currentStageName) ?? stages[0];
  const rawLaneStatus = progressString(lane.status) ? progressStatus(lane.status) : null;
  const rawLaneJobStatus = progressJobStatus(lane.job_status ?? lane.jobStatus);
  const laneStatus = terminalContext?.terminal
    && (rawLaneStatus === null || rawLaneStatus === "RUNNING")
    ? terminalProgressStatus(terminalContext.status)
    : rawLaneStatus;
  const laneJobStatus = terminalContext?.terminal
    && (rawLaneJobStatus === null || rawLaneJobStatus === "RUNNING" || rawLaneJobStatus === "QUEUED")
    ? terminalContext.jobStatus
    : rawLaneJobStatus;
  return {
    laneId,
    model: progressString(lane.model, 120),
    status: laneStatus,
    jobStatus: laneJobStatus,
    // A terminal top-level run has no active lane stage, even if the last
    // progress event was emitted before the final outcome was persisted.
    currentStage: terminalContext?.terminal ? null : progressPhase(directStage),
    processed: processed.value ?? currentStage?.processed ?? null,
    total: total.value ?? currentStage?.total ?? null,
    batchProcessed: batchProcessed.value ?? currentStage?.batchProcessed ?? null,
    batchTotal: batchTotal.value ?? currentStage?.batchTotal ?? null,
    industryCount: currentStage?.industryCount ?? null,
    monthlyDecisionCount: currentStage?.monthlyDecisionCount ?? null,
    themeCount: currentStage?.themeCount ?? null,
    nodeCount: currentStage?.nodeCount ?? null,
    mappingCount: currentStage?.mappingCount ?? null,
    updatedAt: progressTime(lane.updated_at ?? lane.updatedAt ?? lane.time),
    stages,
  };
}

function invalidProgress(issue: WorkflowProgressIssue): WorkflowProgressSummary {
  return {
    status: issue === "OVERSIZE" ? "BLOCKED" : "INVALID",
    jobStatus: null,
    issue,
    stale: false,
    staleIssue: null,
    runId: null,
    phase: null,
    processed: null,
    total: null,
    cacheHits: null,
    cacheMisses: null,
    failures: null,
    currentSymbol: null,
    currentDocument: null,
    documentsSucceeded: null,
    documentsFailed: null,
    elapsedMs: null,
    etaMs: null,
    phaseStartedAt: null,
    updatedAt: null,
    lanes: [],
    resources: null,
  };
}

function nodeErrorCode(error: unknown): string | null {
  if (!isRecord(error)) return null;
  return typeof error.code === "string" ? error.code : null;
}

function normalizeWorkflowProgress(source: JsonRecord): WorkflowProgressSummary | null {
  if (!validateProgressShape(source)) return invalidProgress("INVALID_SHAPE");
  const nestedProgress = nestedRecord(source, ["progress", "metrics"]);
  const nestedData = nestedRecord(source, ["data", "data_sync", "dataSync"]);
  const metricSourcesList = [source, nestedProgress, nestedData].filter((item): item is JsonRecord => item !== null);
  const hasKnownField = [
    "status", "job_status", "jobStatus", "phase", "current_phase", "run_id", "runId", "lanes", "lane_progress", "laneProgress",
    "progress", "metrics", "data", "data_sync", "dataSync",
  ].some((key) => key in source) || [...NUMERIC_PROGRESS_KEYS].some((key) => key in source);
  if (!hasKnownField) return invalidProgress("INVALID_SHAPE");
  const processed = metricFromSources(metricSourcesList, ["processed", "processed_count", "completed", "completed_count", "current", "done"], "count");
  const total = metricFromSources(metricSourcesList, ["total", "total_count", "universe_total"], "count");
  const cacheHits = metricFromSources(metricSourcesList, ["cache_hits", "cacheHit", "hits"], "count");
  const cacheMisses = metricFromSources(metricSourcesList, ["cache_misses", "cacheMisses", "misses"], "count");
  const failures = metricFromSources(metricSourcesList, ["failures", "failed", "failure_count"], "count");
  const documentsSucceeded = metricFromSources(metricSourcesList, ["documents_succeeded", "documentsSucceeded"], "count");
  const documentsFailed = metricFromSources(metricSourcesList, ["documents_failed", "documentsFailed"], "count");
  const elapsedMs = durationFromSources(metricSourcesList, ["elapsed_ms", "elapsedMs", "elapsed"], ["elapsed_seconds"]);
  const etaMs = durationFromSources(metricSourcesList, ["eta_ms", "etaMs", "remaining_ms", "estimated_remaining_ms"], ["eta_seconds"]);
  const statusValue = source.status ?? nestedProgress?.status ?? nestedData?.status;
  const jobStatusValue = source.job_status ?? source.jobStatus ?? nestedProgress?.job_status ?? nestedProgress?.jobStatus;
  const phaseValue = source.phase ?? source.current_phase ?? nestedProgress?.phase ?? nestedData?.phase;
  const status = progressStatus(statusValue);
  const jobStatus = progressJobStatus(jobStatusValue);
  const phase = progressPhase(phaseValue);
  const terminalContext: ProgressTerminalContext = {
    terminal: TERMINAL_PROGRESS_STATUSES.has(status),
    status,
    jobStatus: terminalProgressJobStatus(status, jobStatus, phase),
  };
  const lanes = collectionEntries(source.lanes ?? source.lane_progress ?? source.laneProgress)
    .map(({ key, value }) => normalizeLane(key, value, terminalContext))
    .filter((lane): lane is WorkflowProgressLane => lane !== null);
  const resourceSource = nestedRecord(source, ["resources"]);
  const resourceValue = resourceSource ? {
    rssCurrentMb: metricFromSources([resourceSource], ["rss_current_mb"], "duration").value,
    rssPeakMb: metricFromSources([resourceSource], ["rss_peak_mb"], "duration").value,
    systemMemAvailableMb: metricFromSources([resourceSource], ["system_mem_available_mb"], "duration").value,
    swapUsedMb: metricFromSources([resourceSource], ["swap_used_mb"], "duration").value,
    diskFreeMb: metricFromSources([resourceSource], ["disk_free_mb"], "duration").value,
    diskFreeRatio: metricFromSources([resourceSource], ["disk_free_ratio"], "duration").value,
    openFileDescriptors: metricFromSources([resourceSource], ["open_file_descriptors"], "count").value,
  } : null;
  return {
    status,
    jobStatus: terminalContext.terminal ? terminalContext.jobStatus : jobStatus,
    issue: null,
    stale: false,
    staleIssue: null,
    runId: progressId(source.run_id ?? source.runId),
    phase,
    processed: processed.value,
    total: total.value,
    cacheHits: cacheHits.value,
    cacheMisses: cacheMisses.value,
    failures: failures.value,
    currentSymbol: progressString(nestedData?.current_symbol ?? nestedData?.currentSymbol, 32),
    currentDocument: progressString(nestedData?.current_document ?? nestedData?.currentDocument, 120),
    documentsSucceeded: documentsSucceeded.value,
    documentsFailed: documentsFailed.value,
    elapsedMs: elapsedMs.value,
    etaMs: etaMs.value,
    phaseStartedAt: progressTime(source.phase_started_at ?? source.phaseStartedAt),
    updatedAt: progressTime(source.updated_at ?? source.updatedAt ?? source.time ?? nestedProgress?.updated_at ?? nestedData?.updated_at),
    lanes,
    resources: resourceValue,
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
  readonly outcome: RunOutcomeContract | null;
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

function normalizeResearchItem(
  value: unknown,
  pool: ResearchPool,
  stage: ResearchStage,
  names: ReadonlyMap<string, NameCatalogEntry>,
): ResearchStageDetailItem | null {
  if (!isRecord(value)) return null;
  const symbol = normalizedSymbol(value);
  if (!symbol) return null;
  const modelName = firstString(value, ["company_name", "companyName", "name", "sec_name", "secName", "stock_name", "stockName", "security_name", "securityName"]);
  const catalogName = names.get(symbol);
  const name = modelName ?? catalogName?.name ?? null;
  const declaredNameSource = firstString(value, ["name_source", "nameSource"]);
  const declaredSource = declaredNameSource
    ? ({
      model: "model",
      lane_a1: "lane_a1",
      snapshot: "snapshot",
      snapshot_index: "snapshot",
      index: "snapshot",
    } as const)[declaredNameSource.toLocaleLowerCase()]
    : undefined;
  const nameSource: ResearchStageDetailItem["nameSource"] = modelName
    ? declaredSource ?? "model"
    : catalogName?.source ?? "unavailable";
  const score = stage === "A1"
    ? firstNumber(value, ["structural_score", "structuralScore", "score"])
    : stage === "A2"
      ? firstNumber(value, ["theme_score", "themeScore", "identifiability_score", "identifiabilityScore", "score"])
      : firstNumber(value, ["technical_score", "technicalScore", "score"]);
  const selectionReasons = stage === "A1"
    ? collectStructuredTextFields(value, ["core_thesis", "coreThesis", "selection_reasons", "selectionReasons", "supporting_evidence", "supportingEvidence", "why_selected", "whySelected", "rationale"])
    : stage === "A2"
      ? collectStructuredTextFields(value, ["selection_reasons", "selectionReasons", "role_evidence", "roleEvidence", "supporting_evidence", "supportingEvidence", "why_selected", "whySelected", "rationale"])
      : collectStructuredTextFields(value, ["selection_reasons", "selectionReasons", "confirmation_conditions", "confirmationConditions", "setup_type", "setupType", "ma_context", "maContext", "why_selected", "whySelected", "rationale"]);
  const riskReasons = stage === "A1"
    ? collectStructuredTextFields(value, ["bear_case", "bearCase", "risk_reasons", "riskReasons", "risk_flags", "riskFlags", "risk_warnings", "riskWarnings"])
    : stage === "A2"
      ? collectStructuredTextFields(value, ["risk_reasons", "riskReasons", "risk_flags", "riskFlags", "risk_warnings", "riskWarnings", "contradicting_evidence", "contradictingEvidence"])
      : collectStructuredTextFields(value, ["risk_reasons", "riskReasons", "risk_flags", "riskFlags", "risk_warnings", "riskWarnings", "veto_triggered", "vetoTriggered"]);
  const evidence = collectStructuredTextFields(value, ["evidence", "role_evidence", "roleEvidence", "supporting_evidence", "supportingEvidence", "core_thesis", "coreThesis", "bottleneck_evidence", "bottleneckEvidence"]);
  const risks = collectStructuredTextFields(value, ["bear_case", "bearCase", "risk_reasons", "riskReasons", "risk_flags", "riskFlags", "risk_warnings", "riskWarnings", "contradicting_evidence", "contradictingEvidence"]);
  const invalidation = collectStructuredTextFields(value, ["invalidation_conditions", "invalidationConditions", "invalidation", "veto_triggered", "vetoTriggered"]);
  const sourceRefs = sourceReferenceValues(value);
  const scoreBreakdown = asSafeJson(value.score_breakdown ?? value.scoreBreakdown);
  const factorCoverage = asSafeJson(value.factor_coverage ?? value.factorCoverage ?? value.critical_factor_coverage ?? value.criticalFactorCoverage);
  const theme = firstString(value, ["primary_theme", "primaryTheme", "theme", "theme_id", "themeId"])
    ?? catalogName?.theme
    ?? null;
  const industry = industryText(value) ?? catalogName?.industry ?? null;
  const lineage = lineageValue(value);
  const plan = planValue(value);
  const item: ResearchStageDetailItem = {
    symbol,
    name,
    nameSource,
    detailState: "PARTIAL",
    missingFields: [],
    status: firstString(value, ["status", "state", "decision_status", "decisionStatus"]),
    pool,
    theme,
    industry,
    route: firstString(value, ["a2_route", "a2Route", "selection_route", "selectionRoute", "route"]),
    bottleneckStatus: firstString(value, ["bottleneck_status", "bottleneckStatus"]),
    factorCoverage,
    score,
    reasonCodes: collectReasonCodes(value),
    selectionReasons,
    riskReasons,
    evidence,
    risks,
    invalidation,
    scoreBreakdown,
    sourceRefs,
    lineage,
    decisionFacts: decisionFactsValue(value),
    plan,
  };
  const missingFields = detailMissingFields(value, stage, pool, item);
  return {
    ...item,
    detailState: missingFields.length ? "PARTIAL" : "COMPLETE",
    missingFields,
  };
}

function stageOutput(stage: JsonRecord): JsonRecord | null {
  return optionalRecord(stage.output);
}

function researchLaneFileNames(runId: string, laneId: string): readonly string[] {
  return [`research_${runId}_${laneId}.json`, `${runId}_${laneId}.json`];
}

interface WorkflowProgressFileSystem {
  readonly stat: (path: string) => Promise<{ readonly isFile: () => boolean; readonly size: number; readonly mtimeMs?: number }>;
  readonly readFile: (path: string) => Promise<string>;
}

export interface ProjectFilesOptions {
  /**
   * Small dependency seam for deterministic tests of atomic-replace and
   * permission races. Production callers use node:fs/promises by default.
   */
  readonly workflowProgressFs?: Partial<WorkflowProgressFileSystem>;
}

export class ProjectFiles {
  private readonly statusReader: PythonStatusReader;
  private readonly workflowProgressFs: WorkflowProgressFileSystem;
  private lastWorkflowProgress: WorkflowProgressSummary | null = null;

  public constructor(
    private readonly config: AppConfig,
    logger: LogStore,
    options: ProjectFilesOptions = {},
  ) {
    this.statusReader = new PythonStatusReader(config, logger);
    this.workflowProgressFs = {
      stat: options.workflowProgressFs?.stat ?? (async (path) => stat(path)),
      readFile: options.workflowProgressFs?.readFile ?? (async (path) => readFile(path, { encoding: "utf8" })),
    };
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
        outcome: normalizeRunOutcome(payload),
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
      const lane = await readJson(laneFile.path, MAX_RESEARCH_JSON_BYTES);
      if (lane) {
        const projected = { ...lane };
        const outcome = normalizeLaneOutcome(lane, outcomeText(lane.lane, 80) ?? "UNKNOWN", outcomeText(lane.model));
        if (outcome) projected.outcome_v2 = outcome;
        lanes.push(sanitizeJson(projected));
      }
    }
    const summary: WorkflowRunSummary = {
      runId,
      slot: recordString(payload, "slot"),
      status: recordString(payload, "status"),
      path: relativeDisplay(this.config.rootDir, path),
      mtimeMs,
      snapshot: sanitizeJson(payload.snapshot ?? null),
      researchMarkdown: safeDisplayPath(this.config.rootDir, recordString(payload, "research_markdown")),
      outcome: normalizeRunOutcome(payload),
    };
    return { ...summary, payload: sanitizeJson(payload), lanes };
  }

  public async latestResearchLanes(runId: string | null): Promise<JsonValue[] | null> {
    if (!runId || !SAFE_ID.test(runId)) return null;
    const files = await listFiles(this.config.rootDir, "outputs/research", /^[A-Za-z0-9][A-Za-z0-9._-]*_lane_[A-Za-z0-9._-]+\.json$/);
    const lanes: JsonValue[] = [];
    for (const file of files) {
      if (!laneBelongsToRun(file.name, runId)) continue;
      const payload = await readJson(file.path, MAX_RESEARCH_JSON_BYTES);
      if (payload) {
        const projected = { ...payload };
        const outcome = normalizeLaneOutcome(payload, outcomeText(payload.lane, 80) ?? "UNKNOWN", outcomeText(payload.model));
        if (outcome) projected.outcome_v2 = outcome;
        lanes.push(sanitizeJson(projected));
      }
    }
    return lanes;
  }

  private async researchLane(runId: string, laneId: string): Promise<JsonRecord | null> {
    if (!SAFE_ID.test(runId) || !isResearchLaneId(laneId)) return null;
    for (const fileName of researchLaneFileNames(runId, laneId)) {
      const path = resolveWithinRoot(this.config.rootDir, join("outputs/research", fileName));
      if (!path) continue;
      const payload = await readJson(path, MAX_RESEARCH_JSON_BYTES);
      if (payload && payload.lane === laneId) return payload;
    }
    return null;
  }

  private async researchNameCatalog(runId: string, lane: JsonRecord): Promise<Map<string, NameCatalogEntry>> {
    const catalog = new Map<string, NameCatalogEntry>();
    const stages = rawArray(lane.stages);
    const a1 = stages.find((item) => isRecord(item) && item.stage === "A1");
    const a1Output = a1 && isRecord(a1) ? stageOutput(a1) : null;
    if (a1Output) {
      for (const key of Object.values(RESEARCH_POOL_KEYS.A1)) addNamesFromArray(catalog, a1Output[key], "lane_a1");
    }

    const snapshotPayload = await this.researchSnapshot(runId, lane);
    if (snapshotPayload) {
      const data = optionalRecord(snapshotPayload.data);
      for (const source of [snapshotPayload, data]) {
        if (!source) continue;
        addNamesFromArray(catalog, source.trade_candidates, "snapshot");
        addNamesFromArray(catalog, source.universe_candidates, "snapshot");
        addNamesFromArray(catalog, source.g0_candidates, "snapshot");
      }
    }
    return catalog;
  }

  private async researchSnapshot(runId: string, lane: JsonRecord): Promise<JsonRecord | null> {
    const runPath = resolveWithinRoot(this.config.rootDir, join("outputs/runs", `${runId}.json`));
    const run = runPath ? await readJson(runPath) : null;
    const snapshot = run ? optionalRecord(run.snapshot) : null;
    const snapshotReference = snapshot ? firstString(snapshot, ["path"]) : null;
    let snapshotPath = snapshotReference ? resolveSafeReference(this.config.rootDir, snapshotReference) : null;
    if (!snapshotPath) {
      for (const stage of rawArray(lane.stages)) {
        if (!isRecord(stage)) continue;
        const envelope = optionalRecord(stageOutput(stage)?.envelope);
        const snapshotId = firstString(stage, ["snapshot_id", "snapshotId"])
          ?? rawArray(envelope?.input_snapshot_ids).map((item) => boundedText(item)).find((item): item is string => Boolean(item));
        if (!snapshotId || !SAFE_SNAPSHOT_ID.test(snapshotId)) continue;
        snapshotPath = resolveWithinRoot(this.config.rootDir, join("storage/snapshots", `${snapshotId}.json`));
        if (snapshotPath) break;
      }
    }
    return snapshotPath ? readJson(snapshotPath, MAX_SNAPSHOT_JSON_BYTES) : null;
  }

  private async researchInputCount(runId: string, lane: JsonRecord, stages: readonly unknown[], stageIndex: number): Promise<number | null> {
    if (stageIndex > 0) {
      const previous = stages[stageIndex - 1];
      return isRecord(previous) && Array.isArray(previous.symbols) ? rawArray(previous.symbols).length : null;
    }
    const runPath = resolveWithinRoot(this.config.rootDir, join("outputs/runs", `${runId}.json`));
    const run = runPath ? await readJson(runPath) : null;
    const snapshot = run ? optionalRecord(run.snapshot) : null;
    const runCount = snapshot ? numberValue(snapshot.selected_count ?? snapshot.selectedCount) : null;
    if (runCount !== null) return runCount;
    const snapshotPayload = await this.researchSnapshot(runId, lane);
    const data = snapshotPayload ? optionalRecord(snapshotPayload.data) : null;
    const manifest = data ? optionalRecord(data.snapshot_manifest ?? data.snapshotManifest) : null;
    return manifest ? numberValue(manifest.selected_count ?? manifest.selectedCount) : null;
  }

  public async researchStageDetail(
    runId: string,
    laneId: string,
    stage: string,
    pool: string,
    page = 1,
    pageSize = 50,
    query = "",
    reason = "",
  ): Promise<ResearchStageDetail | null> {
    if (!SAFE_ID.test(runId) || !isResearchLaneId(laneId) || !isResearchStage(stage) || !isResearchPool(pool)) return null;
    const indexed = await this.indexedResearchStageDetail(runId, laneId, stage, pool, page, pageSize, query, reason);
    if (indexed) return indexed;
    const lane = await this.researchLane(runId, laneId);
    if (!lane) return null;
    const stages = rawArray(lane.stages);
    const stageIndex = stages.findIndex((item) => isRecord(item) && item.stage === stage);
    const stageRecord = stageIndex >= 0 ? stages[stageIndex] : undefined;
    if (!isRecord(stageRecord)) return null;
    const stageKey = stage as ResearchStage;
    const poolKey = pool as ResearchPool;
    const output = stageOutput(stageRecord);
    const names = await this.researchNameCatalog(runId, lane);
    const normalizedPools = new Map<ResearchPool, ResearchStageDetailItem[]>();
    for (const candidatePool of ["approved", "watch", "rejected"] as const) {
      const rawItems = output ? rawArray(output[RESEARCH_POOL_KEYS[stageKey][candidatePool]]) : [];
      const items = rawItems
        .map((item) => normalizeResearchItem(item, candidatePool, stageKey, names))
        .filter((item): item is ResearchStageDetailItem => item !== null);
      normalizedPools.set(candidatePool, items);
    }
    const allPools: ResearchStageDetailPool[] = (["approved", "watch", "rejected"] as const).map((candidatePool) => ({
      id: candidatePool,
      label: RESEARCH_POOL_LABELS[stageKey][candidatePool],
      count: normalizedPools.get(candidatePool)?.length ?? 0,
    }));
    const selectedItems = normalizedPools.get(poolKey) ?? [];
    const reasonOptions: string[] = [];
    const reasonSet = new Set<string>();
    for (const item of selectedItems) {
      for (const code of item.reasonCodes) {
        if (reasonSet.has(code)) continue;
        reasonSet.add(code);
        reasonOptions.push(code);
      }
    }
    const search = boundedText(query)?.toLocaleLowerCase() ?? "";
    const reasonFilter = boundedText(reason) ?? "";
    const filtered = selectedItems.filter((item) => {
      const queryMatch = !search || item.symbol.toLocaleLowerCase().includes(search) || (item.name?.toLocaleLowerCase().includes(search) ?? false);
      const reasonMatch = !reasonFilter || item.reasonCodes.includes(reasonFilter);
      return queryMatch && reasonMatch;
    });
    const safePage = Number.isSafeInteger(page) && page > 0 ? page : 1;
    const safePageSize = Number.isSafeInteger(pageSize) && pageSize > 0 ? Math.min(pageSize, DETAIL_PAGE_SIZE_MAX) : 50;
    const offset = (safePage - 1) * safePageSize;
    const symbols = rawArray(stageRecord.symbols);
    const outputCount = Array.isArray(stageRecord.symbols) ? symbols.length : null;
    const inputCount = await this.researchInputCount(runId, lane, stages, stageIndex);
    return {
      runId,
      laneId,
      model: firstString(lane, ["model"]) ?? firstString(stageRecord, ["model"]),
      stage: stageKey,
      status: firstString(stageRecord, ["status"]),
      latencyMs: numberValue(stageRecord.latency_ms ?? stageRecord.latencyMs),
      inputCount,
      outputCount,
      pools: allPools,
      pool: poolKey,
      page: safePage,
      pageSize: safePageSize,
      total: filtered.length,
      reasonOptions,
      outcome: normalizeStageOutcome(stageRecord, stageKey),
      items: filtered.slice(offset, offset + safePageSize),
    };
  }

  private async indexedResearchStageDetail(
    runId: string,
    laneId: string,
    stage: string,
    pool: string,
    page: number,
    pageSize: number,
    query: string,
    reason: string,
  ): Promise<ResearchStageDetail | null> {
    const stem = `research_${runId}_${laneId}`;
    const manifestPath = resolveWithinRoot(this.config.rootDir, join("outputs/research", `${stem}.decisions.json`));
    const dataPath = resolveWithinRoot(this.config.rootDir, join("outputs/research", `${stem}.decisions.ndjson`));
    if (!manifestPath || !dataPath) return null;
    const manifest = await readJson(manifestPath);
    if (
      !manifest
      || manifest.schema_version !== "research-stage-decision-index/1.0.0"
      || manifest.run_id !== runId
      || manifest.lane_id !== laneId
      || manifest.data_file !== `${stem}.decisions.ndjson`
    ) return null;
    try {
      const metadata = await stat(dataPath);
      if (!metadata.isFile()) return null;
    } catch {
      return null;
    }
    const stageKey = stage as ResearchStage;
    const poolKey = pool as ResearchPool;
    const counts = optionalRecord(optionalRecord(manifest.counts)?.[stageKey]);
    const allPools: ResearchStageDetailPool[] = (["approved", "watch", "rejected"] as const).map((candidatePool) => ({
      id: candidatePool,
      label: RESEARCH_POOL_LABELS[stageKey][candidatePool],
      count: numberValue(counts?.[candidatePool]) ?? 0,
    }));
    const optionsRecord = optionalRecord(optionalRecord(manifest.reason_options)?.[stageKey]);
    const reasonOptions = rawArray(optionsRecord?.[poolKey])
      .map((value) => boundedText(value))
      .filter((value): value is string => Boolean(value));
    const safePage = Number.isSafeInteger(page) && page > 0 ? page : 1;
    const safePageSize = Number.isSafeInteger(pageSize) && pageSize > 0 ? Math.min(pageSize, DETAIL_PAGE_SIZE_MAX) : 50;
    const offset = (safePage - 1) * safePageSize;
    const search = boundedText(query)?.toLocaleLowerCase() ?? "";
    const reasonFilter = boundedText(reason) ?? "";
    // Use the same lane/snapshot name catalog as the legacy path.  The index
    // is an acceleration layer, not a separate response contract; rows that
    // omit a name must resolve identically regardless of which artifact was
    // selected.
    const lane = await this.researchLane(runId, laneId);
    const names = lane ? await this.researchNameCatalog(runId, lane) : new Map<string, NameCatalogEntry>();
    const items: ResearchStageDetailItem[] = [];
    let total = 0;
    const input = createReadStream(dataPath, { encoding: "utf8" });
    const lines = createInterface({ input, crlfDelay: Infinity });
    try {
      for await (const line of lines) {
        if (!line || Buffer.byteLength(line, "utf8") > DETAIL_INDEX_LINE_MAX) continue;
        let parsed: unknown;
        try {
          parsed = JSON.parse(line);
        } catch {
          continue;
        }
        if (!isRecord(parsed) || parsed.stage !== stageKey || parsed.pool !== poolKey || !isRecord(parsed.item)) continue;
        const normalized = normalizeResearchItem(parsed.item, poolKey, stageKey, names);
        if (!normalized) continue;
        const queryMatch = !search
          || normalized.symbol.toLocaleLowerCase().includes(search)
          || (normalized.name?.toLocaleLowerCase().includes(search) ?? false);
        const reasonMatch = !reasonFilter || normalized.reasonCodes.includes(reasonFilter);
        if (!queryMatch || !reasonMatch) continue;
        if (total >= offset && items.length < safePageSize) items.push(normalized);
        total += 1;
      }
    } finally {
      lines.close();
      input.destroy();
    }
    const stageMeta = optionalRecord(optionalRecord(manifest.stages)?.[stageKey]);
    return {
      runId,
      laneId,
      model: boundedText(manifest.model),
      stage: stageKey,
      status: boundedText(stageMeta?.status),
      latencyMs: numberValue(stageMeta?.latency_ms),
      inputCount: numberValue(stageMeta?.input_count),
      outputCount: numberValue(stageMeta?.output_count),
      pools: allPools,
      pool: poolKey,
      page: safePage,
      pageSize: safePageSize,
      total,
      reasonOptions,
      outcome: normalizeStageOutcome(stageMeta ?? {}, stageKey),
      items,
    };
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

  private workflowProgressFailure(issue: WorkflowProgressIssue): WorkflowProgressSummary {
    if (!this.lastWorkflowProgress) return invalidProgress(issue);
    return {
      ...this.lastWorkflowProgress,
      issue: null,
      stale: true,
      staleIssue: issue,
    };
  }

  /**
   * Read only the bounded, allow-listed projection of the Python progress file.
   * Missing progress is normal before the first run. After a successful read,
   * transient stat/read/parse failures serve the last allow-listed projection
   * with a fixed stale diagnostic; the source document is never cached.
   */
  public async workflowProgress(): Promise<WorkflowProgressSummary | null> {
    const progressPath = resolveWithinRoot(this.config.rootDir, "state/workflow_progress.json");
    if (!progressPath) return null;
    let metadata;
    try {
      metadata = await this.workflowProgressFs.stat(progressPath);
    } catch (error) {
      // ENOENT is the expected pre-first-run state. If a previously valid
      // file disappears during an atomic replace, report it as stale data.
      if (nodeErrorCode(error) === "ENOENT" && !this.lastWorkflowProgress) return null;
      return this.workflowProgressFailure("UNREADABLE");
    }
    if (!metadata.isFile()) return this.workflowProgressFailure("INVALID_SHAPE");
    if (metadata.size > MAX_WORKFLOW_PROGRESS_BYTES) return this.workflowProgressFailure("OVERSIZE");
    let contents: string;
    try {
      contents = await this.workflowProgressFs.readFile(progressPath);
    } catch {
      return this.workflowProgressFailure("UNREADABLE");
    }
    if (Buffer.byteLength(contents, "utf8") > MAX_WORKFLOW_PROGRESS_BYTES) return this.workflowProgressFailure("OVERSIZE");
    let parsed: unknown;
    try {
      parsed = JSON.parse(contents);
    } catch {
      return this.workflowProgressFailure("INVALID_JSON");
    }
    if (!isRecord(parsed)) return this.workflowProgressFailure("INVALID_SHAPE");
    const normalized = normalizeWorkflowProgress(parsed);
    if (!normalized || normalized.issue) return this.workflowProgressFailure(normalized?.issue ?? "INVALID_SHAPE");
    const mtimeMs = metadata.mtimeMs;
    const heartbeatTimedOut = (normalized.jobStatus === "RUNNING" || normalized.status === "RUNNING")
      && typeof mtimeMs === "number"
      && Number.isFinite(mtimeMs)
      && Date.now() - mtimeMs > this.config.workflowProgressStaleMs;
    const fresh: WorkflowProgressSummary = heartbeatTimedOut
      ? { ...normalized, status: "STALE", jobStatus: "STALE", stale: true, staleIssue: "HEARTBEAT_TIMEOUT" }
      : { ...normalized, stale: false, staleIssue: null };
    this.lastWorkflowProgress = fresh;
    return fresh;
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
