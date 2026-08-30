export type HealthTone = "healthy" | "running" | "warning" | "error" | "unknown";

type JsonRecord = Record<string, unknown>;

/** Canonical backend outcome v3; the UI must not infer business axes. */
export type {
  LaneOutcomeContract,
  OutcomeActionabilityState,
  OutcomeCounts,
  OutcomeDataCoverage,
  OutcomeDataSufficiencyState,
  OutcomeJobStatus,
  OutcomeLifecycleState,
  OutcomeOpportunityState,
  OutcomePublicationState,
  OutcomeQualityState,
  RunOutcomeContract,
  StageOutcomeContract,
} from "./generated/research-outcome-v3";

import type {
  LaneOutcomeContract,
  OutcomeActionabilityState,
  OutcomeCounts,
  OutcomeDataCoverage,
  OutcomeDataSufficiencyState,
  OutcomeJobStatus,
  OutcomeLifecycleState,
  OutcomeOpportunityState,
  OutcomePublicationState,
  OutcomeQualityState,
  RunOutcomeContract,
  StageOutcomeContract,
} from "./generated/research-outcome-v3";

const OUTCOME_SCHEMA_VERSION = "research-outcome/3.0.0";
const LEGACY_OUTCOME_SCHEMA_VERSION = "research-outcome/2.0.0";
const OUTCOME_LIFECYCLE = new Set(["QUEUED", "RUNNING", "TERMINAL"]);
const OUTCOME_JOB_STATUS = new Set(["QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "STALE"]);
const OUTCOME_QUALITY = new Set(["VALIDATED", "DEGRADED", "BLOCKED", "FAILED", "CANCELLED"]);
const OUTCOME_SUFFICIENCY = new Set(["SUFFICIENT", "PARTIAL", "INSUFFICIENT", "NOT_APPLICABLE"]);
const OUTCOME_OPPORTUNITY = new Set(["PRESENT", "ABSENT", "UNKNOWN", "NOT_APPLICABLE"]);
const OUTCOME_ACTIONABILITY = new Set(["ACTIONABLE", "NO_ACTION", "UNKNOWN", "NOT_APPLICABLE"]);
const OUTCOME_PUBLICATION = new Set(["READY", "NOT_APPLICABLE", "BLOCKED", "PUBLISHED"]);

function outcomeRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function outcomeString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function outcomeEnum(value: unknown, allowed: ReadonlySet<string>): string | null {
  const token = outcomeString(value)?.toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  return token && allowed.has(token) ? token : null;
}

function outcomeJobStatus(value: unknown, fallback?: unknown): OutcomeJobStatus {
  const token = outcomeEnum(value, OUTCOME_JOB_STATUS) ?? outcomeEnum(fallback, OUTCOME_JOB_STATUS);
  if (token) return token as OutcomeJobStatus;
  const legacy = outcomeString(fallback)?.toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  if (["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED"].includes(legacy ?? "")) return "QUEUED";
  if (["RUNNING", "RETRYING", "STARTED", "IN_PROGRESS"].includes(legacy ?? "")) return "RUNNING";
  if (["CANCELLED", "CANCELED"].includes(legacy ?? "")) return "CANCELLED";
  if (["FAILED", "BLOCKED_MODEL", "MODEL_FAILED", "MODEL_CALL_FAILED"].includes(legacy ?? "")) return "FAILED";
  if (legacy === "STALE") return "STALE";
  return "SUCCEEDED";
}

function outcomeSufficiency(value: unknown): OutcomeDataSufficiencyState | null {
  return outcomeEnum(value, OUTCOME_SUFFICIENCY) as OutcomeDataSufficiencyState | null;
}

function actionabilityForOpportunity(value: OutcomeOpportunityState): OutcomeActionabilityState {
  return value === "PRESENT" ? "ACTIONABLE" : value === "ABSENT" ? "NO_ACTION" : value === "UNKNOWN" ? "UNKNOWN" : "NOT_APPLICABLE";
}

function opportunityForActionability(value: OutcomeActionabilityState): OutcomeOpportunityState {
  return value === "ACTIONABLE" ? "PRESENT" : value === "NO_ACTION" ? "ABSENT" : value === "UNKNOWN" ? "UNKNOWN" : "NOT_APPLICABLE";
}

function outcomeReasonCodes(value: unknown): string[] {
  const values = typeof value === "string" ? [value] : Array.isArray(value) ? value : [];
  return values
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim().toUpperCase())
    .filter((item, index, all) => /^[A-Z][A-Z0-9_.:-]{0,119}$/.test(item) && all.indexOf(item) === index);
}

function outcomeCounts(value: unknown): OutcomeCounts {
  const record = outcomeRecord(value);
  if (!record) return {};
  const counts: Record<string, number> = {};
  for (const [key, raw] of Object.entries(record)) {
    if (typeof raw !== "number" || !Number.isFinite(raw) || raw < 0) continue;
    counts[key] = Math.floor(raw);
  }
  return counts;
}

function outcomeCoverage(value: unknown): OutcomeDataCoverage {
  const record = outcomeRecord(value);
  if (!record) return {};
  const coverage: Record<string, number | string | null> = {};
  for (const [key, raw] of Object.entries(record)) {
    if (raw === null || typeof raw === "string" || (typeof raw === "number" && Number.isFinite(raw))) coverage[key] = raw;
  }
  return coverage;
}

function outcomeSource(value: Record<string, unknown>): Record<string, unknown> {
  const nested = value.outcome_v3 ?? value.outcomeV3 ?? value.outcome_v2 ?? value.outcomeV2 ?? value.outcome;
  return outcomeRecord(nested) ?? value;
}

function outcomeLifecycle(value: unknown, fallback: string): OutcomeLifecycleState | null {
  const token = outcomeEnum(value, OUTCOME_LIFECYCLE);
  if (token) return token as OutcomeLifecycleState;
  const legacy = fallback.toUpperCase();
  if (["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED"].includes(legacy)) return "QUEUED";
  if (["RUNNING", "RETRYING", "STARTED", "IN_PROGRESS"].includes(legacy)) return "RUNNING";
  if (fallback) return "TERMINAL";
  return null;
}

function outcomeQuality(value: unknown, fallback: string): OutcomeQualityState | null {
  const token = outcomeEnum(value, OUTCOME_QUALITY);
  if (token) return token as OutcomeQualityState;
  if (["CANCELLED", "CANCELED"].includes(fallback)) return "CANCELLED";
  if (["FAILED", "BLOCKED_MODEL", "MODEL_FAILED", "MODEL_CALL_FAILED"].includes(fallback)) return "FAILED";
  if (["BLOCKED", "BLOCKED_DATA_COVERAGE", "BLOCKED_EVIDENCE_GAP", "BLOCKED_TECHNICAL_DATA", "EXPIRED", "NOT_RUN_UPSTREAM_BLOCKED"].includes(fallback)) return "BLOCKED";
  if (["DEGRADED", "DEGRADED_UNDERFILLED_DATA_GAP", "READY_DEGRADED", "VALIDATED_UNDERFILLED_MARKET"].includes(fallback)) return "DEGRADED";
  if (["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED", "RUNNING"].includes(fallback)) return "DEGRADED";
  if (fallback) return "VALIDATED";
  return null;
}

function outcomePublication(value: unknown, fallback: string): OutcomePublicationState | null {
  const token = outcomeEnum(value, OUTCOME_PUBLICATION);
  if (token) return token as OutcomePublicationState;
  if (fallback === "PUBLISHED") return "PUBLISHED";
  if (["READY", "READY_DEGRADED", "READY_TO_PUBLISH", "DATA_GAP"].includes(fallback)) return "READY";
  if (["BLOCKED", "BLOCKED_DATA_COVERAGE", "BLOCKED_EVIDENCE_GAP", "BLOCKED_MODEL", "BLOCKED_TECHNICAL_DATA", "FAILED", "EXPIRED", "CANCELLED", "CANCELED", "NOT_RUN_UPSTREAM_BLOCKED"].includes(fallback)) return "BLOCKED";
  return "NOT_APPLICABLE";
}

function outcomeStatus(value: JsonRecord, source: JsonRecord): string {
  return (outcomeString(source.legacy_status) ?? outcomeString(source.status) ?? outcomeString(value.status) ?? "").toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
}

function outcomeDerivedOpportunity(status: string, counts: OutcomeCounts, coverage: OutcomeDataCoverage): OutcomeOpportunityState {
  if (["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED", "RUNNING", "NOT_RUN_UPSTREAM_BLOCKED"].includes(status)) return "NOT_APPLICABLE";
  if (["BLOCKED", "BLOCKED_DATA_COVERAGE", "BLOCKED_EVIDENCE_GAP", "BLOCKED_MODEL", "BLOCKED_TECHNICAL_DATA", "FAILED", "EXPIRED", "CANCELLED", "CANCELED", "DATA_GAP"].includes(status)) return "UNKNOWN";
  if (["VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"].includes(status)) return "ABSENT";
  if (["READY", "READY_DEGRADED", "READY_TO_PUBLISH", "PUBLISHED", "VALIDATED_UNDERFILLED_MARKET"].includes(status)) return "PRESENT";
  if ((counts.selected ?? 0) > 0) return "PRESENT";
  const required = typeof coverage.required === "number" ? coverage.required : null;
  const actual = typeof coverage.actual === "number" ? coverage.actual : null;
  if (counts.selected === 0 && ((required !== null && actual !== null && actual >= required) || (counts.input !== undefined && counts.evaluated !== undefined && counts.input > 0 && counts.evaluated >= counts.input))) return "ABSENT";
  return "UNKNOWN";
}

function outcomeSufficiencyFor(status: string, counts: OutcomeCounts, coverage: OutcomeDataCoverage, reasons: readonly string[]): OutcomeDataSufficiencyState {
  const explicit = outcomeSufficiency(coverage.sufficiency ?? null);
  if (explicit) return explicit;
  if (["PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED", "RUNNING"].includes(status)) return "NOT_APPLICABLE";
  if (["BLOCKED_DATA_COVERAGE", "DEGRADED_UNDERFILLED_DATA_GAP", "DATA_GAP", "BLOCKED_EVIDENCE_GAP", "BLOCKED_TECHNICAL_DATA"].includes(status)) return "INSUFFICIENT";
  if (reasons.some((reason) => /COVERAGE|UNAVAILABLE|INSUFFICIENT|EVIDENCE_GAP|DATA_GAP/.test(reason))) return "INSUFFICIENT";
  if (typeof coverage.required === "number" && typeof coverage.actual === "number") return coverage.actual >= coverage.required ? "SUFFICIENT" : "INSUFFICIENT";
  if (counts.input !== undefined && counts.evaluated !== undefined && counts.input > 0) return counts.evaluated >= counts.input ? "SUFFICIENT" : "INSUFFICIENT";
  if (["VALIDATED", "VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP", "VALIDATED_UNDERFILLED_MARKET", "READY", "READY_DEGRADED", "READY_TO_PUBLISH", "PUBLISHED"].includes(status)) return "PARTIAL";
  return "NOT_APPLICABLE";
}

function legacyStageOutcome(value: JsonRecord, stage: string): StageOutcomeContract | null {
  const source = outcomeSource(value);
  const status = outcomeStatus(value, source);
  const counts: Record<string, number> = { ...outcomeCounts(source.counts ?? value.counts) };
  for (const [target, keys] of Object.entries({ input: ["input_count", "inputCount"], evaluated: ["evaluated_count", "evaluatedCount"], selected: ["selected_count", "selectedCount", "output_count", "outputCount"] })) {
    if (counts[target] !== undefined) continue;
    for (const key of keys) {
      if (typeof source[key] === "number" && Number.isFinite(source[key])) { counts[target] = Math.max(0, Math.floor(source[key] as number)); break; }
      if (typeof value[key] === "number" && Number.isFinite(value[key])) { counts[target] = Math.max(0, Math.floor(value[key] as number)); break; }
    }
  }
  if (counts.selected === undefined && Array.isArray(value.symbols)) counts.selected = value.symbols.length;
  const coverage = outcomeCoverage(source.data_coverage ?? source.dataCoverage);
  const reasons = outcomeReasonCodes(source.reason_codes ?? source.reasonCodes ?? value.reason_codes ?? value.reasonCodes);
  const derivedReason = status === "VALIDATED_NO_OPPORTUNITY" ? (stage === "A2" ? "A2_NO_FOCUS_OPPORTUNITY" : "NO_OPPORTUNITY") : status === "VALIDATED_NO_ACTION" ? "A3_NO_ACTION" : status === "VALIDATED_NO_SETUP" ? "A3_NO_TECHNICAL_SETUP" : status === "DATA_GAP" ? "DATA_GAP" : null;
  if (derivedReason && !reasons.includes(derivedReason)) reasons.push(derivedReason);
  const lifecycle = outcomeLifecycle(null, status);
  const quality = outcomeQuality(null, status);
  const publication = outcomePublication(null, status);
  if (!lifecycle || !quality || !publication || !status) return null;
  const opportunity = outcomeDerivedOpportunity(status, counts, coverage);
  const explicitNoOpportunity = ["VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"].includes(status);
  const insufficient = explicitNoOpportunity && (counts.input === undefined || counts.evaluated === undefined || counts.input === 0 || (counts.input !== undefined && counts.evaluated !== undefined && counts.evaluated < counts.input));
  const finalOpportunity = insufficient ? "UNKNOWN" : opportunity;
  const finalQuality = insufficient ? "BLOCKED" : quality;
  if (insufficient && !reasons.includes("DATA_COVERAGE_INSUFFICIENT")) reasons.push("DATA_COVERAGE_INSUFFICIENT");
  const stageResearch = stage === "A1" ? finalOpportunity : "NOT_APPLICABLE";
  const stageFocus = stage === "A2" ? finalOpportunity : "NOT_APPLICABLE";
  const stageAction = stage === "A3" ? actionabilityForOpportunity(finalOpportunity) : "NOT_APPLICABLE";
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    stage,
    job_status: outcomeJobStatus(null, status),
    lifecycle_state: lifecycle,
    quality_state: finalQuality,
    data_sufficiency_state: insufficient ? "INSUFFICIENT" : outcomeSufficiencyFor(status, counts, coverage, reasons),
    research_opportunity_state: stageResearch,
    focus_opportunity_state: stageFocus,
    actionability_state: stageAction,
    opportunity_state: finalOpportunity,
    publication_state: insufficient ? "BLOCKED" : publication,
    reason_codes: reasons,
    counts,
    data_coverage: insufficient && !Object.keys(coverage).length ? { sufficiency: "INSUFFICIENT" } : coverage,
    legacy_status: status,
  };
}

/** Parse the backend projection without deriving a result from a stock count. */
export function readStageOutcome(value: unknown): StageOutcomeContract | null {
  const record = outcomeRecord(value);
  const source = outcomeRecord(record?.outcome_v3) ?? outcomeRecord(record?.outcomeV3) ?? outcomeRecord(record?.outcome_v2) ?? outcomeRecord(record?.outcomeV2) ?? outcomeRecord(record?.outcome) ?? record;
  if (!source) return null;
  const stage = outcomeString(source.stage) ?? outcomeString(record?.stage) ?? "UNKNOWN";
  const status = outcomeStatus(record ?? {}, source);
  const counts = outcomeCounts(source.counts);
  const coverage = outcomeCoverage(source.data_coverage ?? source.dataCoverage);
  const reasons = outcomeReasonCodes(source.reason_codes ?? source.reasonCodes);
  const lifecycle = outcomeLifecycle(source.lifecycle_state ?? source.lifecycleState, status);
  const quality = outcomeQuality(source.quality_state ?? source.qualityState, status);
  const publication = outcomePublication(source.publication_state ?? source.publicationState, status);
  const rawOpportunity = outcomeEnum(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null;
  const research = outcomeEnum(source.research_opportunity_state ?? source.researchOpportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null;
  const focus = outcomeEnum(source.focus_opportunity_state ?? source.focusOpportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null;
  const action = outcomeEnum(source.actionability_state ?? source.actionabilityState, OUTCOME_ACTIONABILITY) as OutcomeActionabilityState | null;
  const opportunity = rawOpportunity ?? (stage === "A1" ? research : stage === "A2" ? focus : opportunityForActionability(action ?? "NOT_APPLICABLE"));
  if (!lifecycle || !quality || !publication || !opportunity || (!status && !source.lifecycle_state && !source.quality_state)) return legacyStageOutcome(record ?? {}, stage);
  const explicitNoOpportunity = ["VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"].includes(status);
  const insufficient = explicitNoOpportunity && (counts.input === undefined || counts.evaluated === undefined || counts.input === 0 || (counts.input !== undefined && counts.evaluated !== undefined && counts.evaluated < counts.input));
  const finalOpportunity = insufficient ? "UNKNOWN" : opportunity;
  const finalReasons = [...reasons];
  if (insufficient && !finalReasons.includes("DATA_COVERAGE_INSUFFICIENT")) finalReasons.push("DATA_COVERAGE_INSUFFICIENT");
  const finalQuality = insufficient ? "BLOCKED" : quality;
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    stage,
    job_status: outcomeJobStatus(source.job_status ?? source.jobStatus, status),
    lifecycle_state: lifecycle,
    quality_state: finalQuality,
    data_sufficiency_state: outcomeEnum(source.data_sufficiency_state ?? source.dataSufficiencyState, OUTCOME_SUFFICIENCY) as OutcomeDataSufficiencyState ?? (insufficient ? "INSUFFICIENT" : outcomeSufficiencyFor(status, counts, coverage, finalReasons)),
    research_opportunity_state: (stage === "A1" ? finalOpportunity : research ?? "NOT_APPLICABLE") as OutcomeOpportunityState,
    focus_opportunity_state: (stage === "A2" ? finalOpportunity : focus ?? "NOT_APPLICABLE") as OutcomeOpportunityState,
     actionability_state: (stage === "A3" ? action ?? actionabilityForOpportunity(finalOpportunity) : action ?? "NOT_APPLICABLE") as OutcomeActionabilityState,
    opportunity_state: finalOpportunity,
    publication_state: publication,
    reason_codes: finalReasons,
    counts,
    data_coverage: insufficient && !Object.keys(coverage).length ? { sufficiency: "INSUFFICIENT" } : coverage,
    legacy_status: status || "UNKNOWN",
  };
}

/** Parse a lane outcome and its nested stage projections, if supplied. */
export function readLaneOutcome(value: unknown): LaneOutcomeContract | null {
  const record = outcomeRecord(value);
  const root = record ?? {};
  const source = outcomeSource(root);
  if (!source) return null;
  const rawStages = Array.isArray(source.stages) ? source.stages : [];
  const stages = rawStages.map((item, index) => {
    const stageRecord = outcomeRecord(item);
    if (!stageRecord) return null;
    return readStageOutcome({ ...stageRecord, stage: stageRecord.stage ?? ["A1", "A2", "A3"][index] ?? "UNKNOWN" });
  }).filter((item): item is StageOutcomeContract => item !== null);
  const status = outcomeStatus(root, source);
  const lifecycleFromStages = stages.some((stage) => stage.lifecycle_state === "RUNNING") ? "RUNNING" : stages.some((stage) => stage.lifecycle_state === "QUEUED") ? "QUEUED" : stages.length ? "TERMINAL" : null;
  const qualityFromStages = stages.some((stage) => stage.quality_state === "FAILED") ? "FAILED" : stages.some((stage) => stage.quality_state === "BLOCKED") ? "BLOCKED" : stages.some((stage) => stage.quality_state === "DEGRADED") ? "DEGRADED" : stages.length ? "VALIDATED" : null;
  const lifecycle = (outcomeEnum(source.lifecycle_state ?? source.lifecycleState, OUTCOME_LIFECYCLE) as OutcomeLifecycleState | null) ?? lifecycleFromStages ?? outcomeLifecycle(null, status);
  const quality = (outcomeEnum(source.quality_state ?? source.qualityState, OUTCOME_QUALITY) as OutcomeQualityState | null) ?? qualityFromStages ?? outcomeQuality(null, status);
  const publication = (outcomeEnum(source.publication_state ?? source.publicationState, OUTCOME_PUBLICATION) as OutcomePublicationState | null) ?? (quality === "FAILED" || quality === "BLOCKED" ? "BLOCKED" : lifecycle === "TERMINAL" ? "READY" : "NOT_APPLICABLE");
  const a1 = stages.find((stage) => stage.stage === "A1");
  const a2 = stages.find((stage) => stage.stage === "A2");
  const a3 = stages.find((stage) => stage.stage === "A3");
  const rawOpportunity = outcomeEnum(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null;
  const research = (outcomeEnum(source.research_opportunity_state ?? source.researchOpportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null) ?? a1?.research_opportunity_state ?? "NOT_APPLICABLE";
  const focus = (outcomeEnum(source.focus_opportunity_state ?? source.focusOpportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null) ?? a2?.focus_opportunity_state ?? "NOT_APPLICABLE";
  const action = (outcomeEnum(source.actionability_state ?? source.actionabilityState, OUTCOME_ACTIONABILITY) as OutcomeActionabilityState | null) ?? a3?.actionability_state ?? "NOT_APPLICABLE";
  const opportunity = rawOpportunity ?? research;
  const counts = outcomeCounts(source.counts);
  const coverage = outcomeCoverage(source.data_coverage ?? source.dataCoverage);
  const reasons = outcomeReasonCodes(source.reason_codes ?? source.reasonCodes);
  const sufficiency = (outcomeEnum(source.data_sufficiency_state ?? source.dataSufficiencyState, OUTCOME_SUFFICIENCY) as OutcomeDataSufficiencyState | null) ?? (stages.some((stage) => stage.data_sufficiency_state === "INSUFFICIENT") ? "INSUFFICIENT" : stages.some((stage) => stage.data_sufficiency_state === "PARTIAL") ? "PARTIAL" : stages.some((stage) => stage.data_sufficiency_state === "SUFFICIENT") ? "SUFFICIENT" : outcomeSufficiencyFor(status, counts, coverage, reasons));
  const hasSignal = Boolean(status || source.lifecycle_state || source.quality_state || source.publication_state || stages.length || rawOpportunity || research || focus || action);
  if (!lifecycle || !quality || !publication || !opportunity || !hasSignal) return null;
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    lane_id: outcomeString(source.lane_id ?? source.laneId) ?? outcomeString(root.lane) ?? "UNKNOWN",
    model: outcomeString(source.model) ?? outcomeString(root.model),
    job_status: outcomeJobStatus(source.job_status ?? source.jobStatus, status),
    lifecycle_state: lifecycle,
    quality_state: quality,
    data_sufficiency_state: sufficiency,
    research_opportunity_state: research,
    focus_opportunity_state: focus,
    actionability_state: action,
    opportunity_state: opportunity,
    publication_state: publication,
    reason_codes: reasons,
    counts,
    data_coverage: coverage,
    legacy_status: status || "UNKNOWN",
    stages,
  };
}

/** Parse a run outcome; legacy runs simply return null and retain their old status. */
export function readRunOutcome(value: unknown): RunOutcomeContract | null {
  const record = outcomeRecord(value);
  const root = record ?? {};
  const source = outcomeSource(root);
  if (!source) return null;
  const rawLanes = Array.isArray(source.lanes) ? source.lanes : [];
  const lanes = rawLanes.map((item) => readLaneOutcome(item)).filter((item): item is LaneOutcomeContract => item !== null);
  const status = outcomeStatus(root, source);
  const primaryIds = (Array.isArray(source.primary_lane_ids) ? source.primary_lane_ids : Array.isArray(source.primaryLaneIds) ? source.primaryLaneIds : ["lane_1"])
    .map(outcomeString).filter((item): item is string => item !== null);
  const primary = lanes.filter((lane) => primaryIds.includes(lane.lane_id));
  const lifecycleFromLanes = lanes.some((lane) => lane.lifecycle_state === "RUNNING") ? "RUNNING" : lanes.some((lane) => lane.lifecycle_state === "QUEUED") ? "QUEUED" : lanes.length ? "TERMINAL" : null;
  const qualityFromLanes = primary.some((lane) => lane.quality_state === "FAILED") ? "FAILED" : primary.some((lane) => lane.quality_state === "BLOCKED") ? "BLOCKED" : primary.some((lane) => lane.quality_state === "DEGRADED") ? "DEGRADED" : primary.length ? "VALIDATED" : null;
  const lifecycle = (outcomeEnum(source.lifecycle_state ?? source.lifecycleState, OUTCOME_LIFECYCLE) as OutcomeLifecycleState | null) ?? lifecycleFromLanes ?? outcomeLifecycle(null, status);
  const quality = (outcomeEnum(source.quality_state ?? source.qualityState, OUTCOME_QUALITY) as OutcomeQualityState | null) ?? qualityFromLanes ?? outcomeQuality(null, status);
  const publication = (outcomeEnum(source.publication_state ?? source.publicationState, OUTCOME_PUBLICATION) as OutcomePublicationState | null) ?? (quality === "FAILED" || quality === "BLOCKED" ? "BLOCKED" : lifecycle === "TERMINAL" ? "READY" : "NOT_APPLICABLE");
  const rawOpportunity = outcomeEnum(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null;
  const research = (outcomeEnum(source.research_opportunity_state ?? source.researchOpportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null) ?? primary.find((lane) => lane.research_opportunity_state !== "NOT_APPLICABLE")?.research_opportunity_state ?? "NOT_APPLICABLE";
  const focus = (outcomeEnum(source.focus_opportunity_state ?? source.focusOpportunityState, OUTCOME_OPPORTUNITY) as OutcomeOpportunityState | null) ?? primary.find((lane) => lane.focus_opportunity_state !== "NOT_APPLICABLE")?.focus_opportunity_state ?? "NOT_APPLICABLE";
  const action = (outcomeEnum(source.actionability_state ?? source.actionabilityState, OUTCOME_ACTIONABILITY) as OutcomeActionabilityState | null) ?? primary.find((lane) => lane.actionability_state !== "NOT_APPLICABLE")?.actionability_state ?? "NOT_APPLICABLE";
  const opportunity = rawOpportunity ?? research;
  const counts = outcomeCounts(source.counts);
  const coverage = outcomeCoverage(source.data_coverage ?? source.dataCoverage);
  const reasons = outcomeReasonCodes(source.reason_codes ?? source.reasonCodes);
  const sufficiency = (outcomeEnum(source.data_sufficiency_state ?? source.dataSufficiencyState, OUTCOME_SUFFICIENCY) as OutcomeDataSufficiencyState | null) ?? (primary.some((lane) => lane.data_sufficiency_state === "INSUFFICIENT") ? "INSUFFICIENT" : primary.some((lane) => lane.data_sufficiency_state === "PARTIAL") ? "PARTIAL" : primary.some((lane) => lane.data_sufficiency_state === "SUFFICIENT") ? "SUFFICIENT" : outcomeSufficiencyFor(status, counts, coverage, reasons));
  const hasSignal = Boolean(status || source.lifecycle_state || source.quality_state || source.publication_state || lanes.length || rawOpportunity || research || focus || action);
  if (!lifecycle || !quality || !publication || !opportunity || !hasSignal) return null;
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    job_status: outcomeJobStatus(source.job_status ?? source.jobStatus, status),
    run_id: outcomeString(source.run_id ?? source.runId) ?? outcomeString(root.run_id ?? root.runId),
    lifecycle_state: lifecycle,
    quality_state: quality,
    data_sufficiency_state: sufficiency,
    research_opportunity_state: research,
    focus_opportunity_state: focus,
    actionability_state: action,
    ...(rawOpportunity ? { opportunity_state: rawOpportunity } : {}),
    publication_state: publication,
    reason_codes: reasons,
    counts,
    data_coverage: coverage,
    legacy_status: status || "UNKNOWN",
    primary_lane_ids: primaryIds.length ? primaryIds : ["lane_1"],
    comparison_status: (outcomeString(source.comparison_status ?? source.comparisonStatus) ?? "NOT_RUN").toUpperCase(),
    lanes,
  };
}

export interface StageSummary {
  stage: string;
  label?: string;
  status: string;
  symbolCount?: number | null;
  latencyMs?: number | null;
  reasonCodes?: string[];
  /** Canonical backend projection. Legacy ``status`` remains for compatibility only. */
  outcome?: StageOutcomeContract | null;
  outcome_v2?: StageOutcomeContract | null;
}

export type StagePoolId = "approved" | "watch" | "rejected";

export interface StageDetailPool {
  id: StagePoolId;
  label: string;
  count: number;
}

export interface StageDetailPlan {
  setupType?: string | null;
  planHash?: string | null;
  triggerZone?: { low?: number | null; high?: number | null } | null;
  invalidationLevel?: number | null;
  rewardRisk?: number | null;
  stopDistancePct?: number | null;
  riskUnit?: string | number | null;
  firstResistance?: number | null;
  noChaseCondition?: string | null;
  technicalScore?: number | null;
  counterTrendProbe?: boolean | null;
  overExtended?: boolean | null;
  atrExtension?: number | null;
  maBiasMax?: number | null;
  relativeStrengthRank?: number | null;
  allowedTimeWindows?: unknown;
  maAnalysis?: unknown;
  klinePattern?: string | null;
  factorSnapshotHash?: string | null;
  configHash?: string | null;
  planId?: string | null;
  planExpiry?: string | null;
  confirmationConditions?: string[];
  scenarios?: unknown;
  timeframeStates?: Record<string, unknown>;
}

export interface StageDetailDecisionFacts {
  industryChainRole?: unknown;
  marketRole?: unknown;
  supplyChainRole?: unknown;
  businessExposure?: unknown;
  financialTransmission?: unknown;
  capitalFlow?: unknown;
  tierStructure?: unknown;
  leaderStructure?: unknown;
  crowding?: unknown;
  technicalCycle?: unknown;
  weeklyConfirmation?: unknown;
  indexChainResonance?: unknown;
}

export interface StageDetailItem {
  symbol: string;
  name?: string | null;
  nameSource?: string | null;
  detailState?: "COMPLETE" | "PARTIAL";
  missingFields?: string[];
  status?: string | null;
  pool: StagePoolId;
  theme?: string | null;
  industry?: string | null;
  route?: string | null;
  bottleneckStatus?: string | null;
  factorCoverage?: Record<string, unknown> | null;
  score?: number | null;
  reasonCodes: string[];
  selectionReasons: string[];
  riskReasons: string[];
  evidence: string[];
  risks: string[];
  invalidation: string[];
  scoreBreakdown?: Record<string, unknown> | null;
  sourceRefs: unknown[];
  lineage?: Record<string, unknown> | null;
  decisionFacts?: StageDetailDecisionFacts | null;
  plan?: StageDetailPlan | null;
}

export interface StageDetailResponse {
  runId: string;
  laneId: string;
  model?: string | null;
  stage: string;
  status?: string | null;
  latencyMs?: number | null;
  inputCount?: number | null;
  outputCount?: number | null;
  pools: StageDetailPool[];
  pool: StagePoolId;
  page: number;
  pageSize: number;
  total: number;
  reasonOptions: string[];
  outcome?: StageOutcomeContract | null;
  items: StageDetailItem[];
}

export interface LaneSummary {
  laneId: string;
  model: string;
  status: string;
  updatedAt?: string | null;
  stages: StageSummary[];
  reasonCodes?: string[] | null;
  outcome?: LaneOutcomeContract | null;
  outcome_v2?: LaneOutcomeContract | null;
  comparison?: boolean;
}

export interface ScheduleItem {
  id?: string;
  label: string;
  time?: string;
  cron?: string;
  status?: string;
  nextRunAt?: string | null;
}

export interface LogEntry {
  id?: string;
  timestamp: string;
  level: string;
  job?: string | null;
  runId?: string | null;
  message: string;
  durationMs?: number | null;
}

export interface EffectiveEvent {
  time?: string | null;
  minuteEnd?: string | null;
  laneId?: string | null;
  symbol?: string | null;
  action?: string | null;
  reasonCode?: string | null;
  effective?: boolean;
}

export interface DataSourceSummary {
  id: string;
  label: string;
  status: string;
  checkedAt?: string | null;
  detail?: string | null;
}

export interface AccountSummary {
  accountId: string;
  model: string;
  status: string;
  cash?: number | null;
  equity?: number | null;
  positions?: number | null;
}

export interface WorkflowSummary {
  runId?: string | null;
  status: string;
  slot?: string | null;
  tradeDate?: string | null;
  updatedAt?: string | null;
  snapshotId?: string | null;
  selectedCount?: number | null;
  fullUniverseCount?: number | null;
  lanes: LaneSummary[];
  /** Canonical run projection; old ``status`` is retained for compatibility. */
  outcome?: RunOutcomeContract | null;
  outcome_v2?: RunOutcomeContract | null;
  acceptance?: RunOutcomeContract | null;
}

export interface ServiceSummary {
  status: string;
  version?: string | null;
  uptimeSeconds?: number | null;
  host?: string | null;
  timezone?: string | null;
  stateHealthy?: boolean | null;
  configurationReady?: boolean | null;
  deploymentReady?: boolean | null;
  blockers?: string[];
  schedulerEnabled?: boolean | null;
}

export interface MonitorSummary {
  status: string;
  checkedAt?: string | null;
  effectiveEventCount?: number;
  activePlanCount?: number;
  events: EffectiveEvent[];
}

export interface WorkflowProgressStage {
  stage: string;
  status: string | null;
  processed: number | null;
  total: number | null;
  batchProcessed: number | null;
  batchTotal: number | null;
  selected: number | null;
  monitor: number | null;
  rejected: number | null;
  industryCount: number | null;
  monthlyDecisionCount: number | null;
  themeCount: number | null;
  nodeCount: number | null;
  mappingCount: number | null;
  diagnostics: WorkflowProgressDiagnostics | null;
  updatedAt: string | null;
}

export interface WorkflowProgressDiagnosticShape {
  type: string | null;
  fields: string[];
  unknownFieldCount: number | null;
  envelopeUnknownFieldCount: number | null;
}

export interface WorkflowProgressDiagnostics {
  lastInvalidOutputShape: WorkflowProgressDiagnosticShape | null;
  semanticAttempts: number | null;
  themeCount: number | null;
  nodeCount: number | null;
  mappingCount: number | null;
  expectedMappingCount: number | null;
  missingMappingCount: number | null;
}

export interface WorkflowProgressLane {
  laneId: string;
  model: string | null;
  status: string | null;
  currentStage: string | null;
  processed: number | null;
  total: number | null;
  batchProcessed: number | null;
  batchTotal: number | null;
  industryCount: number | null;
  monthlyDecisionCount: number | null;
  themeCount: number | null;
  nodeCount: number | null;
  mappingCount: number | null;
  updatedAt: string | null;
  stages: WorkflowProgressStage[];
}

export interface WorkflowProgressResources {
  rssCurrentMb: number | null;
  rssPeakMb: number | null;
  systemMemAvailableMb: number | null;
  swapUsedMb: number | null;
  diskFreeMb: number | null;
  diskFreeRatio: number | null;
  openFileDescriptors: number | null;
}

export interface WorkflowProgressSummary {
  status: "RUNNING" | "STALE" | "COMPLETED" | "READY" | "PARTIAL" | "BLOCKED" | "FAILED" | "IDLE" | "UNKNOWN" | "INVALID";
  issue: "OVERSIZE" | "UNREADABLE" | "INVALID_JSON" | "INVALID_SHAPE" | "HEARTBEAT_TIMEOUT" | null;
  stale: boolean;
  staleIssue: "OVERSIZE" | "UNREADABLE" | "INVALID_JSON" | "INVALID_SHAPE" | "HEARTBEAT_TIMEOUT" | null;
  runId: string | null;
  phase: string | null;
  processed: number | null;
  total: number | null;
  cacheHits: number | null;
  cacheMisses: number | null;
  failures: number | null;
  currentSymbol: string | null;
  currentDocument: string | null;
  documentsSucceeded: number | null;
  documentsFailed: number | null;
  elapsedMs: number | null;
  etaMs: number | null;
  phaseStartedAt: string | null;
  updatedAt: string | null;
  lanes: WorkflowProgressLane[];
  resources: WorkflowProgressResources | null;
}

export interface OverviewResponse {
  generatedAt: string;
  service: ServiceSummary;
  activeJob?: {
    job: string;
    runId?: string | null;
    startedAt?: string | null;
    status?: string;
  } | null;
  schedule: ScheduleItem[];
  latestWorkflow: WorkflowSummary;
  workflowProgress: WorkflowProgressSummary | null;
  monitor: MonitorSummary;
  accounts: AccountSummary[];
  planCounts: Record<string, number>;
  dataSources: DataSourceSummary[];
  recentEffectiveEvents: EffectiveEvent[];
  recentLogs: LogEntry[];
}

export interface RunSummary {
  runId: string;
  status: string;
  slot?: string | null;
  updatedAt?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  job?: string | null;
  exitCode?: number | null;
  durationMs?: number | null;
  laneCount?: number | null;
  mtimeMs?: number | null;
}

export interface RunsResponse {
  runs: RunSummary[];
}

export interface LogsResponse {
  logs: LogEntry[];
}

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}
