export type HealthTone = "healthy" | "running" | "warning" | "error" | "unknown";

/** Canonical backend outcome contract; the UI must not infer these axes. */
export type OutcomeLifecycleState = "QUEUED" | "RUNNING" | "TERMINAL";
export type OutcomeQualityState = "VALIDATED" | "DEGRADED" | "BLOCKED" | "FAILED" | "CANCELLED";
export type OutcomeOpportunityState = "PRESENT" | "ABSENT" | "UNKNOWN" | "NOT_APPLICABLE";
export type OutcomePublicationState = "READY" | "NOT_APPLICABLE" | "BLOCKED" | "PUBLISHED";

export interface OutcomeCounts {
  readonly [key: string]: number;
}

export interface OutcomeDataCoverage {
  readonly [key: string]: number | string | null;
}

const OUTCOME_SCHEMA_VERSION = "research-outcome/2.0.0";
const OUTCOME_LIFECYCLE = new Set(["QUEUED", "RUNNING", "TERMINAL"]);
const OUTCOME_QUALITY = new Set(["VALIDATED", "DEGRADED", "BLOCKED", "FAILED", "CANCELLED"]);
const OUTCOME_OPPORTUNITY = new Set(["PRESENT", "ABSENT", "UNKNOWN", "NOT_APPLICABLE"]);
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

/** Parse the backend projection without deriving a result from a stock count. */
export function readStageOutcome(value: unknown): StageOutcomeContract | null {
  const record = outcomeRecord(value);
  const source = outcomeRecord(record?.outcome_v2) ?? outcomeRecord(record?.outcomeV2) ?? outcomeRecord(record?.outcome) ?? record;
  if (!source) return null;
  const lifecycle = outcomeEnum(source.lifecycle_state ?? source.lifecycleState, OUTCOME_LIFECYCLE);
  const quality = outcomeEnum(source.quality_state ?? source.qualityState, OUTCOME_QUALITY);
  const opportunity = outcomeEnum(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY);
  const publication = outcomeEnum(source.publication_state ?? source.publicationState, OUTCOME_PUBLICATION);
  if (!lifecycle || !quality || !opportunity || !publication) return null;
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    stage: outcomeString(source.stage) ?? "UNKNOWN",
    lifecycle_state: lifecycle as OutcomeLifecycleState,
    quality_state: quality as OutcomeQualityState,
    opportunity_state: opportunity as OutcomeOpportunityState,
    publication_state: publication as OutcomePublicationState,
    reason_codes: outcomeReasonCodes(source.reason_codes ?? source.reasonCodes),
    counts: outcomeCounts(source.counts),
    data_coverage: outcomeCoverage(source.data_coverage ?? source.dataCoverage),
    legacy_status: (outcomeString(source.legacy_status) ?? outcomeString(source.status) ?? "UNKNOWN").toUpperCase(),
  };
}

/** Parse a lane outcome and its nested stage projections, if supplied. */
export function readLaneOutcome(value: unknown): LaneOutcomeContract | null {
  const record = outcomeRecord(value);
  const root = record ?? {};
  const source = outcomeRecord(root.outcome_v2) ?? outcomeRecord(root.outcomeV2) ?? outcomeRecord(root.outcome) ?? record;
  if (!source) return null;
  const lifecycle = outcomeEnum(source.lifecycle_state ?? source.lifecycleState, OUTCOME_LIFECYCLE);
  const quality = outcomeEnum(source.quality_state ?? source.qualityState, OUTCOME_QUALITY);
  const opportunity = outcomeEnum(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY);
  const publication = outcomeEnum(source.publication_state ?? source.publicationState, OUTCOME_PUBLICATION);
  if (!lifecycle || !quality || !opportunity || !publication) return null;
  const rawStages = Array.isArray(source.stages) ? source.stages : [];
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    lane_id: outcomeString(source.lane_id ?? source.laneId) ?? outcomeString(root.lane) ?? "UNKNOWN",
    model: outcomeString(source.model) ?? outcomeString(root.model),
    lifecycle_state: lifecycle as OutcomeLifecycleState,
    quality_state: quality as OutcomeQualityState,
    opportunity_state: opportunity as OutcomeOpportunityState,
    publication_state: publication as OutcomePublicationState,
    reason_codes: outcomeReasonCodes(source.reason_codes ?? source.reasonCodes),
    counts: outcomeCounts(source.counts),
    data_coverage: outcomeCoverage(source.data_coverage ?? source.dataCoverage),
    legacy_status: (outcomeString(source.legacy_status) ?? outcomeString(source.status) ?? "UNKNOWN").toUpperCase(),
    stages: rawStages.map(readStageOutcome).filter((item): item is StageOutcomeContract => item !== null),
  };
}

/** Parse a run outcome; legacy runs simply return null and retain their old status. */
export function readRunOutcome(value: unknown): RunOutcomeContract | null {
  const record = outcomeRecord(value);
  const root = record ?? {};
  const source = outcomeRecord(root.outcome_v2) ?? outcomeRecord(root.outcomeV2) ?? outcomeRecord(root.outcome) ?? record;
  if (!source) return null;
  const lifecycle = outcomeEnum(source.lifecycle_state ?? source.lifecycleState, OUTCOME_LIFECYCLE);
  const quality = outcomeEnum(source.quality_state ?? source.qualityState, OUTCOME_QUALITY);
  const opportunity = outcomeEnum(source.opportunity_state ?? source.opportunityState, OUTCOME_OPPORTUNITY);
  const publication = outcomeEnum(source.publication_state ?? source.publicationState, OUTCOME_PUBLICATION);
  if (!lifecycle || !quality || !opportunity || !publication) return null;
  const rawLanes = Array.isArray(source.lanes) ? source.lanes : [];
  const primaryIds = (Array.isArray(source.primary_lane_ids) ? source.primary_lane_ids : Array.isArray(source.primaryLaneIds) ? source.primaryLaneIds : ["lane_1"])
    .map(outcomeString).filter((item): item is string => item !== null);
  return {
    schema_version: OUTCOME_SCHEMA_VERSION,
    run_id: outcomeString(source.run_id ?? source.runId) ?? outcomeString(root.run_id ?? root.runId),
    lifecycle_state: lifecycle as OutcomeLifecycleState,
    quality_state: quality as OutcomeQualityState,
    opportunity_state: opportunity as OutcomeOpportunityState,
    publication_state: publication as OutcomePublicationState,
    reason_codes: outcomeReasonCodes(source.reason_codes ?? source.reasonCodes),
    counts: outcomeCounts(source.counts),
    data_coverage: outcomeCoverage(source.data_coverage ?? source.dataCoverage),
    legacy_status: (outcomeString(source.legacy_status) ?? outcomeString(source.status) ?? "UNKNOWN").toUpperCase(),
    primary_lane_ids: primaryIds.length ? primaryIds : ["lane_1"],
    comparison_status: (outcomeString(source.comparison_status ?? source.comparisonStatus) ?? "NOT_RUN").toUpperCase(),
    lanes: rawLanes.map(readLaneOutcome).filter((item): item is LaneOutcomeContract => item !== null),
  };
}

export interface StageOutcomeContract {
  readonly schema_version: "research-outcome/2.0.0";
  readonly stage: string;
  readonly lifecycle_state: OutcomeLifecycleState;
  readonly quality_state: OutcomeQualityState;
  readonly opportunity_state: OutcomeOpportunityState;
  readonly publication_state: OutcomePublicationState;
  readonly reason_codes: readonly string[];
  readonly counts: OutcomeCounts;
  readonly data_coverage: OutcomeDataCoverage;
  readonly legacy_status: string;
}

export interface LaneOutcomeContract {
  readonly schema_version: "research-outcome/2.0.0";
  readonly lane_id: string;
  readonly model: string | null;
  readonly lifecycle_state: OutcomeLifecycleState;
  readonly quality_state: OutcomeQualityState;
  readonly opportunity_state: OutcomeOpportunityState;
  readonly publication_state: OutcomePublicationState;
  readonly reason_codes: readonly string[];
  readonly counts: OutcomeCounts;
  readonly data_coverage: OutcomeDataCoverage;
  readonly legacy_status: string;
  readonly stages: readonly StageOutcomeContract[];
}

export interface RunOutcomeContract {
  readonly schema_version: "research-outcome/2.0.0";
  readonly run_id: string | null;
  readonly lifecycle_state: OutcomeLifecycleState;
  readonly quality_state: OutcomeQualityState;
  readonly opportunity_state: OutcomeOpportunityState;
  readonly publication_state: OutcomePublicationState;
  readonly reason_codes: readonly string[];
  readonly counts: OutcomeCounts;
  readonly data_coverage: OutcomeDataCoverage;
  readonly legacy_status: string;
  readonly primary_lane_ids: readonly string[];
  readonly comparison_status: string;
  readonly lanes: readonly LaneOutcomeContract[];
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
  triggerZone?: { low?: number | null; high?: number | null } | null;
  invalidationLevel?: number | null;
  rewardRisk?: number | null;
  stopDistancePct?: number | null;
  riskUnit?: number | null;
  planId?: string | null;
  planExpiry?: string | null;
  confirmationConditions?: string[];
  scenarios?: unknown;
  timeframeStates?: Record<string, unknown>;
}

export interface StageDetailItem {
  symbol: string;
  name?: string | null;
  nameSource?: string | null;
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
