export type JsonPrimitive = string | number | boolean | null;

export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type JsonRecord = { [key: string]: unknown };

/**
 * Canonical research outcome contract (research-outcome/2.0.0).
 *
 * ``legacy_status`` is an additive compatibility projection.  Consumers must
 * render the four axes and reason codes supplied by Python instead of
 * deriving business meaning from a status string.
 */
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

export type JobName = "morning" | "close" | "monitor" | "features";

export type JobStatus = "running" | "succeeded" | "failed" | "skipped" | "terminated";

export interface JobDefinition {
  readonly name: JobName;
  readonly command: string;
  readonly label: string;
  readonly schedule: string;
}

export interface JobRunRecord {
  readonly runId: string;
  readonly job: JobName;
  readonly command: string;
  readonly startedAt: string;
  readonly finishedAt: string | null;
  readonly exitCode: number | null;
  readonly signal: string | null;
  readonly durationMs: number | null;
  readonly status: JobStatus;
  readonly reason: string | null;
}

export interface LogEvent {
  readonly id: number;
  readonly timestamp: string;
  readonly level: "debug" | "info" | "warn" | "error";
  readonly job: string | null;
  readonly runId: string | null;
  readonly stream: "node" | "stdout" | "stderr" | "scheduler" | "workflow";
  readonly message: string;
}

export interface SchedulerSnapshot {
  readonly timezone: string;
  readonly running: boolean;
  readonly jobs: readonly {
    readonly job: JobName;
    readonly label: string;
    readonly schedule: string;
    readonly enabled: boolean;
    readonly lastDispatchAt: string | null;
  }[];
}

export interface StatusSnapshot {
  readonly availability: "ok" | "degraded";
  readonly data: JsonRecord | null;
  readonly fetchedAt: string;
  readonly reason: string | null;
}

export type WorkflowProgressStatus = "RUNNING" | "STALE" | "COMPLETED" | "READY" | "READY_DEGRADED" | "PARTIAL" | "BLOCKED" | "FAILED" | "IDLE" | "UNKNOWN" | "INVALID";

/**
 * Fixed, non-sensitive diagnostics for an unavailable or stale progress file.
 * These values are deliberately narrower than Node's filesystem errors so
 * the API never exposes an OS error message or the source document.
 */
export type WorkflowProgressIssue = "OVERSIZE" | "UNREADABLE" | "INVALID_JSON" | "INVALID_SHAPE" | "HEARTBEAT_TIMEOUT";

export interface WorkflowProgressDiagnosticShape {
  readonly type: string | null;
  readonly fields: readonly string[];
  readonly unknownFieldCount: number | null;
  readonly envelopeUnknownFieldCount: number | null;
}

/**
 * Safe, bounded diagnostics emitted by the Python progress writer.  This is
 * intentionally a structural projection; it never contains model text,
 * mapping codes, or arbitrary provider fields.
 */
export interface WorkflowProgressDiagnostics {
  readonly lastInvalidOutputShape: WorkflowProgressDiagnosticShape | null;
  readonly semanticAttempts: number | null;
  readonly themeCount: number | null;
  readonly nodeCount: number | null;
  readonly mappingCount: number | null;
  readonly expectedMappingCount: number | null;
  readonly missingMappingCount: number | null;
}

export interface WorkflowProgressStage {
  readonly stage: string;
  readonly status: string | null;
  readonly processed: number | null;
  readonly total: number | null;
  readonly batchProcessed: number | null;
  readonly batchTotal: number | null;
  readonly selected: number | null;
  readonly monitor: number | null;
  readonly rejected: number | null;
  readonly industryCount: number | null;
  readonly monthlyDecisionCount: number | null;
  readonly themeCount: number | null;
  readonly nodeCount: number | null;
  readonly mappingCount: number | null;
  readonly diagnostics: WorkflowProgressDiagnostics | null;
  readonly updatedAt: string | null;
}

export interface WorkflowProgressLane {
  readonly laneId: string;
  readonly model: string | null;
  readonly status: string | null;
  readonly currentStage: string | null;
  readonly processed: number | null;
  readonly total: number | null;
  readonly batchProcessed: number | null;
  readonly batchTotal: number | null;
  readonly industryCount: number | null;
  readonly monthlyDecisionCount: number | null;
  readonly themeCount: number | null;
  readonly nodeCount: number | null;
  readonly mappingCount: number | null;
  readonly updatedAt: string | null;
  readonly stages: readonly WorkflowProgressStage[];
}

export interface WorkflowProgressResources {
  readonly rssCurrentMb: number | null;
  readonly rssPeakMb: number | null;
  readonly systemMemAvailableMb: number | null;
  readonly swapUsedMb: number | null;
  readonly diskFreeMb: number | null;
  readonly diskFreeRatio: number | null;
  readonly openFileDescriptors: number | null;
}

/**
 * A deliberately small, allow-listed projection of state/workflow_progress.json.
 * The control plane must never expose the original progress document because it
 * is written by a separate process and may contain provider or model payloads.
 */
export interface WorkflowProgressSummary {
  readonly status: WorkflowProgressStatus;
  readonly issue: WorkflowProgressIssue | null;
  /** True when this is the last successful projection while the file is unavailable. */
  readonly stale: boolean;
  /** The fixed reason for serving the cached projection, if stale. */
  readonly staleIssue: WorkflowProgressIssue | null;
  readonly runId: string | null;
  readonly phase: string | null;
  readonly processed: number | null;
  readonly total: number | null;
  readonly cacheHits: number | null;
  readonly cacheMisses: number | null;
  readonly failures: number | null;
  readonly currentSymbol: string | null;
  readonly currentDocument: string | null;
  readonly documentsSucceeded: number | null;
  readonly documentsFailed: number | null;
  readonly elapsedMs: number | null;
  readonly etaMs: number | null;
  readonly phaseStartedAt: string | null;
  readonly updatedAt: string | null;
  readonly lanes: readonly WorkflowProgressLane[];
  readonly resources: WorkflowProgressResources | null;
}

export type ResearchStage = "A1" | "A2" | "A3";

export type ResearchPool = "approved" | "watch" | "rejected";

export interface ResearchStageDetailPlan {
  readonly setupType: string | null;
  readonly triggerZone: JsonValue | null;
  readonly invalidationLevel: number | null;
  readonly rewardRisk: number | null;
  readonly stopDistancePct: number | null;
  readonly riskUnit: number | null;
  readonly planId: string | null;
  readonly planExpiry: string | null;
  readonly confirmationConditions: readonly string[];
  readonly scenarios: JsonValue | null;
  readonly timeframeStates: JsonValue | null;
}

export interface ResearchStageDetailItem {
  readonly symbol: string;
  readonly name: string | null;
  readonly nameSource: "model" | "lane_a1" | "snapshot" | "unavailable";
  readonly status: string | null;
  readonly pool: ResearchPool;
  readonly theme: string | null;
  readonly industry: string | null;
  readonly route: string | null;
  readonly bottleneckStatus: string | null;
  readonly factorCoverage: JsonValue | null;
  readonly score: number | null;
  readonly reasonCodes: readonly string[];
  readonly selectionReasons: readonly string[];
  readonly riskReasons: readonly string[];
  readonly evidence: readonly string[];
  readonly risks: readonly string[];
  readonly invalidation: readonly string[];
  readonly scoreBreakdown: JsonValue | null;
  readonly sourceRefs: JsonValue;
  readonly lineage: JsonValue | null;
  readonly plan: ResearchStageDetailPlan | null;
}

export interface ResearchStageDetailPool {
  readonly id: ResearchPool;
  readonly label: string;
  readonly count: number;
}

export interface ResearchStageDetail {
  readonly runId: string;
  readonly laneId: string;
  readonly model: string | null;
  readonly stage: ResearchStage;
  readonly status: string | null;
  readonly latencyMs: number | null;
  readonly inputCount: number | null;
  readonly outputCount: number | null;
  readonly pools: readonly ResearchStageDetailPool[];
  readonly pool: ResearchPool;
  readonly page: number;
  readonly pageSize: number;
  readonly total: number;
  readonly reasonOptions: readonly string[];
  /** Safe four-axis projection; absent for legacy artifacts that lack a usable status. */
  readonly outcome?: StageOutcomeContract | null;
  readonly items: readonly ResearchStageDetailItem[];
}
