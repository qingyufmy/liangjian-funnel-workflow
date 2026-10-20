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

/**
 * Providers may publish the next minute's forming row a few seconds before
 * its close.  Wait for a small, deterministic settling window before asking
 * Python to freeze the minute snapshot.  The dispatch key remains the same
 * minute, so this is not catch-up or a second business attempt.
 */
export function isMonitorDispatchReady(clock: ShanghaiClock): boolean {
  return clock.second >= 3;
}

export function auctionEvidenceSlot(clock: ShanghaiClock): number | undefined {
  if (clock.hour !== 9 || clock.minute !== 25) return undefined;
  return [3, 8, 15, 24].find(second => clock.second >= second && clock.second < second + 2);
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
  private outcomesAfterCloseDate: string | null = null;
  private teamEodAfterCloseDate: string | null = null;
  private teamEodRecoveryCheckedDate: string | null = null;
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
    this.outcomesAfterCloseDate = null;
    this.teamEodAfterCloseDate = null;
    this.teamEodRecoveryCheckedDate = null;
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
    if (clock.weekday >= 1 && clock.weekday <= 5 && clock.hour*60+clock.minute>=15*60+10
        && this.teamEodRecoveryCheckedDate!==clock.date) {
      this.teamEodRecoveryCheckedDate=clock.date;
      if (this.runner.completedCloseForTeamEod?.(clock.date)) this.teamEodAfterCloseDate=clock.date;
    }
    if (clock.weekday === 6) {
      if (isFeatureMaintenanceMinute(clock)) due.push("features");
    } else {
      if (isFeatureMaintenanceMinute(clock)) due.push("features");
      if (clock.hour === 7 && clock.minute < 15) due.push("auction-base");
      if (clock.hour === 8 && clock.minute === 30) due.push("premarket");
      // Independent budgets and durable Python prerequisites. No delayed
      // catch-up/model retry can make a missed prior edge appear successful.
      if (clock.hour === 8 && clock.minute === 33) due.push("team-morning-engine");
      if (clock.hour === 8 && clock.minute === 35) due.push("team-morning-risk");
      if (clock.hour === 8 && clock.minute === 38) due.push("team-morning-pm");
      if (clock.hour*60+clock.minute>=8*60+45 && clock.hour*60+clock.minute<=17*60+55) due.push("team-internal");
      // Independent evidence only. Never late-catch-up a 09:26 quote as an
      // auction-final print, and never rerun the A2/A3 research to obtain it.
      if (auctionEvidenceSlot(clock) !== undefined) due.push("auction-evidence");
      if (clock.hour === 9 && clock.minute === 26) due.push("morning");
      // Auction refresh remains a manual runner/CLI command. Morning only
      // validates and activates the already-published previous-close plans.
      if (clock.hour === 11 && clock.minute === 35) due.push("a5-midday");
      if (clock.hour === 16 && clock.minute === 0) due.push("a5-close");
      if ((clock.hour===11 && clock.minute>=35) || clock.hour===12) due.push("team-a5-midday");
      // Customer A5: 16:00, BUSY retry through 17:00, max runtime 15m.
      // Team never takes its slot during that entire customer window.
      if (clock.hour===17 && clock.minute>=25 && clock.minute<=45) due.push("team-a5-close");
      if (clock.hour === 16 && clock.minute === 10) due.push("outcomes");
      if (clock.hour === 15 && clock.minute === 10) due.push("close");
      if (clock.hour === 15 && clock.minute >= 1 && clock.minute < 5) due.push("team-close-bar");
      // Owned paper settlement may recover a busy review slot or a Node
      // restart for ten minutes. Its persisted daily ledger is authoritative.
      if (clock.hour === 15 && clock.minute >= 30 && clock.minute < 40) due.push("paper-settle");
      // Node provides a cheap weekday wake-up. Python owns the exchange
      // calendar and decides whether this date is the monthly full or weekly
      // incremental A1 maintenance boundary; ordinary weekdays are a NOOP.
      if (isA1MaintenanceMinute(clock)) due.push("a1");
      if (isMonitorMinute(clock) && isMonitorDispatchReady(clock)) due.push("monitor");
    }

    for (const job of due) {
      const dispatchKey = job === "auction-evidence" ? `${clock.date}T09:25:${auctionEvidenceSlot(clock)}`
        : job === "paper-settle" ? `${clock.date}T15:30`
        : job === "team-close-bar" ? `${clock.date}T15:01`
        : job === "team-a5-midday" ? `${clock.date}Tteam-midday`
        : job === "team-a5-close" ? `${clock.date}Tteam-close`
        : job === "auction-base" ? `${clock.date}T07:00` : key;
      if (job === "features" && !this.featureMaintenanceEnabled) continue;
      if (this.dispatched.get(job) === dispatchKey || this.inFlight.has(job) || this.retryKeys.get(job) === dispatchKey) continue;
      if ((job === "paper-settle" || job === "team-close-bar") && this.lastDispatch.has(job)
          && value.getTime() - Date.parse(this.lastDispatch.get(job)!) < 5_000) continue;
      if (job.startsWith('team-a5-') && this.lastDispatch.has(job)
          && value.getTime() - Date.parse(this.lastDispatch.get(job)!) < 60_000) continue;
      this.dispatch(job, dispatchKey, value);
    }
    // The 16:10 pass can precede refreshed closing prices. Reconcile again
    // after a successful same-day research run, without replaying research.
    if (this.teamEodAfterCloseDate && this.teamEodAfterCloseDate !== clock.date) this.teamEodAfterCloseDate = null;
    if (this.teamEodAfterCloseDate === clock.date && clock.hour*60+clock.minute >= 17*60+20
        && !this.inFlight.has("team-eod")
        && (!this.lastDispatch.has("team-eod") || value.getTime() - Date.parse(this.lastDispatch.get("team-eod")!) >= 5_000)) {
      this.teamEodAfterCloseDate = null;
      this.dispatch("team-eod", `${clock.date}Tafter-close`, value);
    }
    if (this.outcomesAfterCloseDate && this.outcomesAfterCloseDate !== clock.date) {
      this.outcomesAfterCloseDate = null;
    }
    if (this.outcomesAfterCloseDate === clock.date && clock.hour >= 15
      && !this.inFlight.has("outcomes") && !this.retryTimers.has("outcomes")) {
      this.outcomesAfterCloseDate = null;
      this.dispatch("outcomes", `${clock.date}Tafter-close`, value);
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
          && definition.name !== "auction-refresh"
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
        if (job.startsWith('team-a5-') && ((result.status==='skipped' && result.reason?.startsWith('BUSY:'))
            || result.exitCode===75)) this.dispatched.delete(job);
        // Baseline failures require investigation, not repeated full syncs.
        // Only retry a worker-busy skip while the 07:00-07:14 window is open.
        if (job === "auction-base" && result.status === "skipped" && result.reason?.startsWith("BUSY:")) {
          this.dispatched.delete(job);
        }
        if ((job === "paper-settle" || job === "team-close-bar") && result.status === "skipped" && result.reason?.startsWith("BUSY:")) {
          this.dispatched.delete(job);
        }
        if (job === "team-eod" && result.status === "skipped" && result.reason?.startsWith("BUSY:")
            && shanghaiClock(this.now()).date === key.slice(0,10)) {
          this.teamEodAfterCloseDate = key.slice(0,10);
        }
        const shouldRetry = (job === "premarket" || job === "morning" || job === "close" || job === "a1" || job === "a5-midday" || job === "a5-close" || job === "outcomes")
          && result.status === "skipped"
          && result.reason?.startsWith("BUSY:") === true;
        if (shouldRetry) {
          this.dispatched.delete(job);
          this.scheduleResearchRetry(job, key);
        }
        if (job === "close" && result.status === "succeeded"
          && shanghaiClock(this.now()).date === key.slice(0, 10)) {
          this.outcomesAfterCloseDate = key.slice(0, 10);
          this.teamEodAfterCloseDate = key.slice(0, 10);
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
      const dueMinute = job === "premarket"
        ? 8 * 60 + 30
        : job === "morning"
        ? 9 * 60 + 26
        : job === "a5-midday"
        ? 11 * 60 + 35
        : job === "a5-close"
        ? 16 * 60
        : job === "outcomes"
        ? 16 * 60 + 10
        : job === "close"
          ? 15 * 60 + 10
          : 18 * 60;
      const currentMinute = clock.hour * 60 + clock.minute;
      const afterCloseOutcomes = job === "outcomes" && key.endsWith("Tafter-close");
      const withinRecoveryWindow = clock.date === key.slice(0, 10)
        && currentMinute >= (afterCloseOutcomes ? 15 * 60 : dueMinute)
        && currentMinute <= (afterCloseOutcomes ? 23 * 60 + 59 : dueMinute + (job === "premarket" ? 50 : job === "a1" ? 5 * 60 : job === "a5-midday" ? 70 : job === "a5-close" || job === "outcomes" ? 60 : 10));
      if (withinRecoveryWindow && clock.weekday !== 0 && clock.weekday !== 6) {
        this.dispatch(job, key, current);
      }
    }, this.retryMs);
    this.retryTimers.set(job, timer);
  }
}
