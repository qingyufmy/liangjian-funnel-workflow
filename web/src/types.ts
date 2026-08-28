export type HealthTone = "healthy" | "running" | "warning" | "error" | "unknown";

export interface StageSummary {
  stage: string;
  label?: string;
  status: string;
  symbolCount?: number | null;
  latencyMs?: number | null;
  reasonCodes?: string[];
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
  items: StageDetailItem[];
}

export interface LaneSummary {
  laneId: string;
  model: string;
  status: string;
  updatedAt?: string | null;
  stages: StageSummary[];
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
  updatedAt: string | null;
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
  updatedAt: string | null;
  stages: WorkflowProgressStage[];
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
