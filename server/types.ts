export type JsonPrimitive = string | number | boolean | null;

export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type JsonRecord = { [key: string]: unknown };

export type JobName = "morning" | "close" | "monitor";

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

export type WorkflowProgressStatus = "RUNNING" | "COMPLETED" | "READY" | "PARTIAL" | "BLOCKED" | "FAILED" | "IDLE" | "UNKNOWN" | "INVALID";

export interface WorkflowProgressStage {
  readonly stage: string;
  readonly status: string | null;
  readonly processed: number | null;
  readonly total: number | null;
  readonly batchProcessed: number | null;
  readonly batchTotal: number | null;
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
  readonly updatedAt: string | null;
  readonly stages: readonly WorkflowProgressStage[];
}

/**
 * A deliberately small, allow-listed projection of state/workflow_progress.json.
 * The control plane must never expose the original progress document because it
 * is written by a separate process and may contain provider or model payloads.
 */
export interface WorkflowProgressSummary {
  readonly status: WorkflowProgressStatus;
  readonly issue: "OVERSIZE" | "INVALID_JSON" | "INVALID_SHAPE" | null;
  readonly runId: string | null;
  readonly phase: string | null;
  readonly processed: number | null;
  readonly total: number | null;
  readonly cacheHits: number | null;
  readonly cacheMisses: number | null;
  readonly failures: number | null;
  readonly elapsedMs: number | null;
  readonly etaMs: number | null;
  readonly updatedAt: string | null;
  readonly lanes: readonly WorkflowProgressLane[];
}
