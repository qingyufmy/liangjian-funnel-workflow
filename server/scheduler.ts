import { A1_MAINTENANCE_AT, FEATURE_MAINTENANCE_AT, JOB_DEFINITIONS, TIMEZONE } from "./config.js";
import { LogStore } from "./logger.js";
import { JobRunner } from "./runner.js";
import type { JobName, SchedulerSnapshot } from "./types.js";

interface ShanghaiClock {
  readonly date: string;
  readonly weekday: number;
  readonly hour: number;
  readonly minute: number;
  readonly second: number;
}

export interface SchedulerOptions {
  readonly now?: () => Date;
  readonly intervalMs?: number;
  readonly retryMs?: number;
  readonly comparisonEnabled?: boolean;
  readonly featureMaintenanceEnabled?: boolean;
}

export function shanghaiClock(value: Date): ShanghaiClock {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(value);
  const values = new Map(parts.map((part) => [part.type, part.value]));
  const date = `${values.get("year") ?? "0000"}-${values.get("month") ?? "00"}-${values.get("day") ?? "00"}`;
  const dateParts = date.split("-");
  const year = Number(dateParts[0] ?? "0");
  const month = Number(dateParts[1] ?? "0");
  const day = Number(dateParts[2] ?? "0");
  return {
    date,
    weekday: Number.isSafeInteger(year) && Number.isSafeInteger(month) && Number.isSafeInteger(day)
      ? new Date(Date.UTC(year, month - 1, day)).getUTCDay()
      : 0,
    hour: Number(values.get("hour") ?? "0"),
    minute: Number(values.get("minute") ?? "0"),
    second: Number(values.get("second") ?? "0"),
  };
}

export function isMonitorMinute(clock: ShanghaiClock): boolean {
  const minuteOfDay = clock.hour * 60 + clock.minute;
  const researchProtection = (minuteOfDay >= 9 * 60 + 25 && minuteOfDay <= 9 * 60 + 27)
    || (minuteOfDay >= 15 * 60 + 8 && minuteOfDay <= 15 * 60 + 11);
  return !researchProtection && (
    (minuteOfDay >= 9 * 60 + 31 && minuteOfDay <= 11 * 60 + 30)
    || (minuteOfDay >= 13 * 60 + 1 && minuteOfDay <= 15 * 60)
  );
}

function isFeatureMaintenanceMinute(clock: ShanghaiClock): boolean {
  const [hour, minute] = FEATURE_MAINTENANCE_AT.split(":").map((value) => Number(value));
  return clock.hour === hour && clock.minute === minute;
}

function isA1MaintenanceMinute(clock: ShanghaiClock): boolean {
  const [hour, minute] = A1_MAINTENANCE_AT.split(":").map((value) => Number(value));
  return clock.hour === hour && clock.minute === minute;
}

export class WorkflowScheduler {
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private readonly dispatched = new Map<JobName, string>();
  private readonly lastDispatch = new Map<JobName, string>();
  private readonly inFlight = new Set<JobName>();
  private readonly retryKeys = new Map<JobName, string>();
  private readonly retryTimers = new Map<JobName, NodeJS.Timeout>();
  private comparisonInFlight = false;
  private comparisonPendingTrigger = false;
  private comparisonRetryTimer: NodeJS.Timeout | null = null;
  private readonly now: () => Date;
  private readonly intervalMs: number;
  private readonly retryMs: number;
  private readonly comparisonEnabled: boolean;
  private readonly featureMaintenanceEnabled: boolean;

  public constructor(
    private readonly runner: JobRunner,
    private readonly logger: LogStore,
    options: SchedulerOptions = {},
  ) {
    this.now = options.now ?? (() => new Date());
    this.intervalMs = options.intervalMs ?? 1_000;
    this.retryMs = options.retryMs ?? 5_000;
    this.comparisonEnabled = options.comparisonEnabled ?? true;
    this.featureMaintenanceEnabled = options.featureMaintenanceEnabled ?? true;
  }

  public start(): void {
    if (this.timer) return;
    this.running = true;
    this.timer = setInterval(() => this.tick(), this.intervalMs);
    void this.tick();
    // Recover a request committed by the primary before a Node restart.  The
    // Python command is idempotent and never reruns the primary lane.
    if (this.comparisonEnabled) this.triggerComparison("startup");
    this.logger.info("Node 调度器已启动");
  }

  public stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    for (const timer of this.retryTimers.values()) clearTimeout(timer);
    this.retryTimers.clear();
    this.retryKeys.clear();
    this.inFlight.clear();
    if (this.comparisonRetryTimer) clearTimeout(this.comparisonRetryTimer);
    this.comparisonRetryTimer = null;
    this.comparisonInFlight = false;
    this.comparisonPendingTrigger = false;
    this.running = false;
    this.logger.info("Node 调度器已停止");
  }

  public async tick(value: Date = this.now()): Promise<void> {
    const clock = shanghaiClock(value);
    const key = `${clock.date}T${String(clock.hour).padStart(2, "0")}:${String(clock.minute).padStart(2, "0")}`;
    const due: JobName[] = [];
    // Sunday is a hard NOOP.  Saturday has exactly one maintenance slot;
    // research and intraday monitoring remain trading-day-only.
    if (clock.weekday === 0) return;
    if (clock.weekday === 6) {
      if (isFeatureMaintenanceMinute(clock)) due.push("features");
    } else {
      if (isFeatureMaintenanceMinute(clock)) due.push("features");
      if (clock.hour === 9 && clock.minute === 26) due.push("morning");
      if (clock.hour === 15 && clock.minute === 10) due.push("close");
      // Node provides a cheap weekday wake-up. Python owns the exchange
      // calendar and decides whether this date is the monthly full or weekly
      // incremental A1 maintenance boundary; ordinary weekdays are a NOOP.
      if (isA1MaintenanceMinute(clock)) due.push("a1");
      if (isMonitorMinute(clock)) due.push("monitor");
    }

    for (const job of due) {
      if (job === "features" && !this.featureMaintenanceEnabled) continue;
      if (this.dispatched.get(job) === key || this.inFlight.has(job) || this.retryKeys.get(job) === key) continue;
      this.dispatch(job, key, value);
    }
  }

  public snapshot(): SchedulerSnapshot {
    return {
      timezone: TIMEZONE,
      running: this.running,
      jobs: JOB_DEFINITIONS.map((definition) => ({
        job: definition.name,
        label: definition.label,
        schedule: definition.schedule,
        enabled: this.running
          && (definition.name !== "comparison" || this.comparisonEnabled)
          && (definition.name !== "features" || this.featureMaintenanceEnabled),
        lastDispatchAt: this.lastDispatch.get(definition.name) ?? null,
      })),
    };
  }

  private dispatch(job: JobName, key: string, value: Date): void {
    this.dispatched.set(job, key);
    this.lastDispatch.set(job, value.toISOString());
    this.inFlight.add(job);
    this.logger.info(`触发调度 ${job} at=${key}`, { job });
    void this.runner.run(job)
      .then((result) => {
        const shouldRetry = (job === "morning" || job === "close" || job === "a1")
          && result.status === "skipped"
          && result.reason?.startsWith("BUSY:") === true;
        if (shouldRetry) {
          this.dispatched.delete(job);
          this.scheduleResearchRetry(job, key);
        }
        // The current morning command is the deterministic pending-plan
        // review; the model-backed primary research hand-off is the close
        // command.  Do not manufacture a comparison request after a review.
        if (this.comparisonEnabled && job === "close" && result.status === "succeeded") {
          this.triggerComparison(`after-${job}`);
        }
      })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : "unknown scheduler error";
        this.logger.error(`调度执行异常 ${job}: ${message}`, { job });
      })
      .finally(() => {
        this.inFlight.delete(job);
      });
  }

  private triggerComparison(source: string): void {
    if (!this.comparisonEnabled) return;
    if (this.comparisonInFlight) {
      // A close can finish while recovery is still processing an older
      // request.  Remember that edge-trigger so the newly committed request
      // is picked up as soon as the current comparison child exits.
      this.comparisonPendingTrigger = true;
      return;
    }
    this.comparisonInFlight = true;
    this.logger.info(`检查对比模型持久请求 source=${source}`, { job: "comparison" });
    void this.runner.run("comparison")
      .then((result) => {
        if (result.status === "skipped" && result.reason?.startsWith("BUSY:") === true) {
          this.comparisonRetryTimer = setTimeout(() => {
            this.comparisonRetryTimer = null;
            this.triggerComparison("busy-retry");
          }, this.retryMs);
        }
      })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : "unknown comparison scheduler error";
        this.logger.error(`对比模型恢复异常: ${message}`, { job: "comparison" });
      })
      .finally(() => {
        this.comparisonInFlight = false;
        if (this.comparisonPendingTrigger && this.running && !this.comparisonRetryTimer) {
          this.comparisonPendingTrigger = false;
          this.triggerComparison("after-flight");
        }
      });
  }

  private scheduleResearchRetry(job: JobName, key: string): void {
    if (this.retryKeys.get(job) === key || this.retryTimers.has(job)) return;
    this.retryKeys.set(job, key);
    const timer = setTimeout(() => {
      this.retryTimers.delete(job);
      if (this.retryKeys.get(job) !== key) return;
      this.retryKeys.delete(job);
      const current = this.now();
      const clock = shanghaiClock(current);
      const dueMinute = job === "morning"
        ? 9 * 60 + 26
        : job === "close"
          ? 15 * 60 + 10
          : 18 * 60;
      const currentMinute = clock.hour * 60 + clock.minute;
      const withinRecoveryWindow = clock.date === key.slice(0, 10)
        && currentMinute >= dueMinute
        && currentMinute <= dueMinute + (job === "a1" ? 5 * 60 : 10);
      if (withinRecoveryWindow && clock.weekday !== 0 && clock.weekday !== 6) {
        this.dispatch(job, key, current);
      }
    }, this.retryMs);
    this.retryTimers.set(job, timer);
  }
}
