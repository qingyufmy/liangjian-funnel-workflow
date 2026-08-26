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
