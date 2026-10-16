import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync, existsSync, openSync, readSync, closeSync } from "node:fs";
import path from "node:path";

import type { AppConfig } from "./config.js";
import { JOB_DEFINITIONS } from "./config.js";
import { redactText } from "./redaction.js";
import { LogStore } from "./logger.js";
import type { JobName, JobRunRecord } from "./types.js";
import { MonitorHealth, monitorReasonCounts } from "./monitor-health.js";

interface ActiveChild {
  readonly runId: string;
  readonly process: ChildProcess;
}

export interface ReadOnlyCommandResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly exitCode: number | null;
  readonly timedOut: boolean;
}

export function writeTeamEodHandoff(config: AppConfig, record: JobRunRecord): void {
  if (record.job !== 'close' || record.status !== 'succeeded' || record.exitCode !== 0 || !record.finishedAt) return;
  const day = new Intl.DateTimeFormat('en-CA', {timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(record.finishedAt));
  const directory=path.join(config.rootDir,'state','team_v2','eod-handoff');
  mkdirSync(directory,{recursive:true});
  try {writeFileSync(path.join(directory,day+'.json'),JSON.stringify({trade_date:day,source_close:record}),{flag:'wx',mode:0o600});}
  catch(error){if((error as NodeJS.ErrnoException).code!=='EEXIST')throw error;}
}

export function timeoutForJob(
  job: JobName,
  configuredTimeoutMs: number,
  a1TimeoutMs: number = configuredTimeoutMs,
): number | null {
  // Research is checkpointed and must have a wall-clock boundary; unlimited
  // orchestration previously left stale close processes blocking every later
  // schedule. Minute monitoring has a much tighter deadline so it releases
  // the single worker before the 09:26/15:10 research protection windows.
  if (job === "monitor") return Math.min(configuredTimeoutMs, 58_000);
  if (job === "auction-evidence") return Math.min(configuredTimeoutMs, 30_000);
  if (job === "paper-settle") return Math.min(configuredTimeoutMs, 60_000);
  if (job === "team-eod") return Math.min(configuredTimeoutMs, 120_000);
  if (job === "team-internal") return Math.min(configuredTimeoutMs, 8_000);
  if (job === "team-a5-midday" || job === "team-a5-close") return Math.min(configuredTimeoutMs, 240_000);
  if (job.startsWith("team-morning-")) return Math.min(configuredTimeoutMs, 120_000);
  if (job === "auction-base") return Math.min(configuredTimeoutMs, 60 * 60 * 1000);
  if (job === "auction-refresh") return Math.min(configuredTimeoutMs, 30 * 60 * 1000);
  // A5's model call has a 600s total budget. The parent must also allow
  // bounded fact collection, verification, persistence and delivery; equal
  // deadlines kill a valid model result before the review can be recorded.
  if (job === "a5-midday" || job === "a5-close") return Math.min(configuredTimeoutMs, 15 * 60 * 1000);
  // A full-market closed-day refresh alone has a 0.5s per-symbol provider
  // pacing floor. Keep the job bounded, but leave enough time for the
  // subsequent complete A2/A3 review rather than killing a valid partial
  // research run before it can atomically publish next-session plans.
  if (job === "close") return Math.min(configuredTimeoutMs, 90 * 60 * 1000);
  if (job === "a1") return a1TimeoutMs;
  return configuredTimeoutMs;
}

export function monitorSnapshotDelay(job: JobName, timeoutMs: number | null): number | null {
  return job === "monitor" && timeoutMs !== null && timeoutMs > 50_000 ? 50_000 : null;
}

export function ensureMonitorTimeoutSnapshot(directory: string, runId: string, elapsedMs: number): void {
  const target = path.join(directory, `${runId}.json`);
  if (existsSync(target)) return;
  const bounded = (file: string): string | null => {
    let fd: number | undefined;
    try { fd = openSync(file, "r"); const data = Buffer.alloc(128 * 1024);
      return data.subarray(0, readSync(fd, data, 0, data.length, 0)).toString("utf8");
    } catch { return null; } finally { if (fd !== undefined) closeSync(fd); }
  };
  let phase: Record<string, unknown> | null = null;
  try { const parsed = JSON.parse(bounded(path.join(directory, `${runId}.phase.json`)) ?? "null") as Record<string, unknown> | null;
    if (parsed?.run_id === runId) phase = parsed;
  } catch { /* Unknown is not startup/strategy success. */ }
  mkdirSync(directory, { recursive: true });
  writeFileSync(target, JSON.stringify({ run_id: runId, observed_at: new Date().toISOString(),
    reason: "PYTHON_SIGNAL_JSON_NOT_ACKNOWLEDGED", phase_checkpoint: phase,
    current_phase: phase?.current_phase ?? null, stack: bounded(path.join(directory, `${runId}.stacks.log`)),
    parent_elapsed_ms: elapsedMs, snapshot_is_parent_fallback: true, phase_is_last_checkpoint: true }),
    { flag: "wx", mode: 0o640 });
}

function readMonitorTrace(directory: string, runId: string): Record<string, unknown> {
  try {
    const receiptPath = path.join(directory, `${runId}.receipt.json`);
    const raw = readFileSync(receiptPath, "utf8");
    if (raw.length > 256 * 1024) return { status: "TRACE_TOO_LARGE" };
    const receipt = JSON.parse(raw) as Record<string, unknown>;
    if (receipt.run_id !== runId) return { status: "TRACE_RUN_ID_MISMATCH" };
    return { status: "RECORDED", receiptPath, phase_timings: receipt.phase_timings,
      business_reason_counts: receipt.business_reason_counts,
      business_status_observed: receipt.business_status_observed,
      business_incident_ids: receipt.business_incident_ids,
      elapsed_ms: receipt.elapsed_ms, timeoutSnapshotPath: path.join(directory, `${runId}.json`) };
  } catch {
    return { status: "NO_FINAL_RECEIPT", timeoutSnapshotPath: path.join(directory, `${runId}.json`) };
  }
}

export function waitForProcessExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve(true);
  return new Promise<boolean>((resolve) => {
    let settled = false;
    let timer: NodeJS.Timeout;
    const finish = (exited: boolean): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.off("close", onClose);
      resolve(exited);
    };
    const onClose = (): void => finish(true);
    timer = setTimeout(() => finish(false), timeoutMs);
    child.once("close", onClose);
  });
}

export function childEnvironment(
  job: JobName, timeoutMs: number | null, startedAtMs: number,
  base: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  if (!Number.isSafeInteger(startedAtMs) || startedAtMs < 0
    || (job === "close" && timeoutMs === null)
    || (timeoutMs !== null && (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0
      || !Number.isSafeInteger(startedAtMs + timeoutMs)))) {
    throw new Error("INVALID_PARENT_JOB_TIMING");
  }
  const env = { ...base };
  // Child-only deadline is authoritative for this invocation; never inherit
  // an old close deadline into unrelated jobs or mutate the server env.
  delete env.LIANGJIAN_PARENT_CLOSE_DEADLINE_MS;
  delete env.LIANGJIAN_PARENT_JOB_BUDGET_MS;
  delete env.LIANGJIAN_PARENT_JOB_STARTED_MS;
  delete env.LIANGJIAN_PARENT_RUN_ID;
  delete env.LIANGJIAN_MONITOR_TRACE_DIR;
  if (timeoutMs !== null) {
    env.LIANGJIAN_PARENT_JOB_BUDGET_MS = String(timeoutMs);
    env.LIANGJIAN_PARENT_JOB_STARTED_MS = String(startedAtMs);
  }
  if (job === "close" && timeoutMs !== null) {
    env.LIANGJIAN_PARENT_CLOSE_DEADLINE_MS = String(startedAtMs + timeoutMs);
  }
  return env;
}

function splitChunk(
  pending: string,
  chunk: Buffer | string,
): { readonly lines: string[]; readonly pending: string } {
  const combined = pending + chunk.toString();
  const parts = combined.split(/\r?\n/);
  return { lines: parts.slice(0, -1), pending: parts.at(-1) ?? "" };
}

function definitionFor(job: JobName) {
  return JOB_DEFINITIONS.find((item) => item.name === job) ?? null;
}

export class JobRunner {
  private readonly history: JobRunRecord[] = [];
  private readonly monitorHealth: MonitorHealth;
  // The optional comparison plane has its own child slot.  It is deliberately
  // independent from the priority slot so a slow Kimi/GLM run cannot block the
  // next morning/close research or the one-minute monitor.
  private active: ActiveChild | null = null;
  private comparison: ActiveChild | null = null;
  private review: ActiveChild | null = null;
  private notification: ActiveChild | null = null;

  public constructor(
    private readonly config: AppConfig,
    private readonly logger: LogStore,
  ) {
    this.monitorHealth = new MonitorHealth(path.join(config.nodeLogDir, "monitor-business-health.json"),
      path.join(path.dirname(config.rootDir), "liangjian-shadow", "internal_alert.env"),
      message => logger.warn(message, { job:"monitor" }));
  }

  public activeJob(): JobRunRecord | null {
    const current = this.history.find((item) => item.runId === (this.active?.runId ?? this.review?.runId ?? this.comparison?.runId ?? this.notification?.runId));
    return current ?? null;
  }

  public recentRuns(limit = 100): JobRunRecord[] {
    return this.history.slice(-limit).reverse();
  }

  public completedCloseForTeamEod(day: string): boolean {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return false;
    let fd: number | undefined;
    try {
      fd=openSync(path.join(this.config.rootDir,'state','team_v2','eod-handoff',day+'.json'),'r');
      const buffer=Buffer.alloc(8193);const size=readSync(fd,buffer,0,buffer.length,0);
      if(size>8192)return false;const raw=buffer.subarray(0,size).toString('utf8');
      const receipt=JSON.parse(raw) as {trade_date?:string;source_close?:JobRunRecord};const source=receipt.source_close;
      const date=(value:string)=>new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value));
      return receipt.trade_date===day && source?.job==='close' && source.status==='succeeded' && source.exitCode===0
        && typeof source.runId==='string' && !!source.runId && !!source.finishedAt && date(source.startedAt)===day && date(source.finishedAt)===day;
    } catch{return false;} finally {if(fd!==undefined)closeSync(fd);}
  }

  public async run(job: JobName): Promise<JobRunRecord> {
    const definition = definitionFor(job);
    if (!definition) {
      throw new Error("unsupported job");
    }
    const slot = job === "team-internal" ? "notification" : job === "comparison" ? "comparison" : job.startsWith("team-morning-") || job.startsWith("team-a5-") || job === "team-eod" || job === "auction-evidence" || job === "auction-refresh" || job === "a5-midday" || job === "a5-close" || job === "outcomes" || job === "paper-settle" ? "review" : "primary";
    const current = this.childFor(slot);
    if (current) return this.skipped(job, definition.command, current.runId);

    return this.runChild(job, definition.command, slot);
  }

  private skipped(job: JobName, command: string, activeRunId: string): JobRunRecord {
    const skipped: JobRunRecord = {
      runId: `skipped-${randomUUID()}`,
      job,
      command,
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      exitCode: null,
      signal: null,
      durationMs: 0,
      status: "skipped",
      reason: `BUSY:${activeRunId}`,
    };
    this.record(skipped);
    this.logger.warn(`任务互斥，跳过 ${command}; active=${activeRunId}`, { job });
    return skipped;
  }

  private childFor(slot: "primary" | "comparison" | "review" | "notification"): ActiveChild | null {
    return slot === "notification" ? this.notification : slot === "comparison" ? this.comparison : slot === "review" ? this.review : this.active;
  }

  private setChild(slot: "primary" | "comparison" | "review" | "notification", child: ActiveChild | null): void {
    if (slot === "comparison") this.comparison = child;
    else if (slot === "review") this.review = child;
    else if (slot === "notification") this.notification = child;
    else this.active = child;
  }

  private async runChild(
    job: JobName,
    command: string,
    slot: "primary" | "comparison" | "review" | "notification",
  ): Promise<JobRunRecord> {

    const runId = `${job}-${new Date().toISOString().replace(/[:.]/g, "-")}-${randomUUID().slice(0, 8)}`;
    const startedAt = new Date();
    const started: JobRunRecord = {
      runId,
      job,
      command,
      startedAt: startedAt.toISOString(),
      finishedAt: null,
      exitCode: null,
      signal: null,
      durationMs: null,
      status: "running",
      reason: null,
    };
    this.record(started);
    this.logger.info(`开始执行 ${command}`, { job, runId });

    const timeoutMs = timeoutForJob(job, this.config.jobTimeoutMs, this.config.a1JobTimeoutMs);
    const traceDir = path.join(this.config.rootDir, "outputs", "node", "timeout-snapshots");
    const env = childEnvironment(job, timeoutMs, startedAt.getTime());
    if (job === "monitor") {
      env.LIANGJIAN_PARENT_RUN_ID = runId;
      env.LIANGJIAN_MONITOR_TRACE_DIR = traceDir;
    }
    const child = spawn(this.config.pythonBin, ["-m", "liangjian_funnel", command], {
      cwd: this.config.rootDir,
      env,
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    this.setChild(slot, { runId, process: child });

    let stdoutPending = "";
    let stderrPending = "";
    let traceReady = false;
    let monitorReasons: Record<string, number> = {};
    child.stdout.on("data", (chunk: Buffer | string) => {
      if (job === "monitor") monitorReasons = { ...monitorReasons, ...monitorReasonCounts(stdoutPending + chunk.toString()) };
      const split = splitChunk(stdoutPending, chunk);
      stdoutPending = split.pending;
      for (const line of split.lines) this.logger.info(redactText(line, this.config.maxLogLineLength), { job, runId, stream: "stdout" });
    });
    child.stderr.on("data", (chunk: Buffer | string) => {
      if (job === "monitor") monitorReasons = { ...monitorReasons, ...monitorReasonCounts(stderrPending + chunk.toString()) };
      if (job === "a5-close" && /^A4_ENTRY_WINDOW_UNRESOLVED\r?$/m.test(stderrPending + chunk.toString()))
        monitorReasons.A4_ENTRY_WINDOW_UNRESOLVED = 1;
      const split = splitChunk(stderrPending, chunk);
      stderrPending = split.pending;
      for (const line of split.lines) {
        if (line === "MONITOR_TRACE_READY") { traceReady = true; continue; }
        this.logger.warn(redactText(line, this.config.maxLogLineLength), { job, runId, stream: "stderr" });
      }
    });

    const result = await new Promise<JobRunRecord>((resolve) => {
      let settled = false;
      let timedOut = false;
      let timer: NodeJS.Timeout | null = null;
      let escalationTimer: NodeJS.Timeout | null = null;
      let hardFinishTimer: NodeJS.Timeout | null = null;
      let snapshotTimer: NodeJS.Timeout | null = null;
      let snapshotRequested = false;
      const finish = (exitCode: number | null, signal: NodeJS.Signals | null): void => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        if (escalationTimer) clearTimeout(escalationTimer);
        if (hardFinishTimer) clearTimeout(hardFinishTimer);
        if (snapshotTimer) clearTimeout(snapshotTimer);
        if (stdoutPending) this.logger.info(redactText(stdoutPending, this.config.maxLogLineLength), { job, runId, stream: "stdout" });
        if (stderrPending) this.logger.warn(redactText(stderrPending, this.config.maxLogLineLength), { job, runId, stream: "stderr" });
        const finishedAt = new Date();
        const durationMs = finishedAt.getTime() - startedAt.getTime();
        if (job === "monitor" && snapshotRequested) {
          try { ensureMonitorTimeoutSnapshot(traceDir, runId, durationMs); }
          catch { this.logger.warn("MONITOR_SNAPSHOT_WRITE_FAILED", { job, runId }); }
        }
        const status = timedOut ? "terminated" : exitCode === 0 ? "succeeded" : "failed";
        const monitorTrace = job === "monitor" ? readMonitorTrace(traceDir, runId) : null;
        let monitorBusiness: Readonly<Record<string, unknown>> | null = null;
        if (monitorTrace) {
          const captured = monitorTrace.business_reason_counts;
          if (monitorTrace.business_status_observed === true) monitorReasons = {};
          if (captured && typeof captured === "object" && !Array.isArray(captured)) {
            for (const [code, count] of Object.entries(captured)) {
              if (typeof count === "number" && Number.isSafeInteger(count) && count > 0) monitorReasons[code] = count;
            }
          }
          if (monitorTrace.business_status_observed !== true && !Object.keys(monitorReasons).length)
            monitorReasons.MONITOR_BUSINESS_STATUS_UNAVAILABLE = 1;
          monitorBusiness = { ...this.monitorHealth.observe({ runId, startedAt: startedAt.toISOString(),
            finishedAt: finishedAt.toISOString(), exitCode }, monitorReasons,monitorTrace.business_incident_ids) };
        }
        if (job === "a5-close" && monitorReasons.A4_ENTRY_WINDOW_UNRESOLVED)
          monitorBusiness = { ...this.monitorHealth.observe({ runId, startedAt: startedAt.toISOString(),
            finishedAt: finishedAt.toISOString(), exitCode }, monitorReasons) };
        const record: JobRunRecord = {
          runId,
          job,
          command,
          startedAt: startedAt.toISOString(),
          finishedAt: finishedAt.toISOString(),
          exitCode,
          signal,
          durationMs,
          status,
          reason: timedOut
            ? "TIMEOUT"
            : exitCode === 0
              ? null
              : signal
                ? `SIGNAL_KILLED:${signal}`
                : "NON_ZERO_EXIT",
          ...(monitorTrace ? { monitorTrace } : {}),
          ...(monitorBusiness ? { monitorBusiness } : {}),
        };
        this.replaceRunning(record);
        try {writeTeamEodHandoff(this.config,record);}
        catch {this.logger.warn('TEAM_EOD_HANDOFF_WRITE_FAILED',{job,runId});}
        if (this.childFor(slot)?.runId === runId) this.setChild(slot, null);
        this.logger[status === "succeeded" ? "info" : "error"](
          `任务结束 ${command} status=${status} exit=${exitCode ?? "null"} signal=${signal ?? "none"} duration_ms=${durationMs}`,
          { job, runId },
        );
        if (monitorBusiness) this.logger[monitorBusiness.status === "READY" ? "info" : "warn"](
          `MONITOR_BUSINESS_HEALTH ${JSON.stringify(monitorBusiness)}`, { job, runId });
        resolve(record);
      };
      const snapshotDelay = monitorSnapshotDelay(job, timeoutMs);
      if (snapshotDelay !== null) {
        snapshotTimer = setTimeout(() => {
          if (settled) return;
          snapshotRequested = true;
          if (traceReady && process.platform !== "win32") {
            try {
              if (child.kill("SIGUSR1")) {
                this.logger.warn("MONITOR_50S_SNAPSHOT_REQUESTED", { job, runId });
                return;
              }
            } catch { /* Fall through to an honest missing-child-trace record. */ }
          }
          try {
            mkdirSync(traceDir, { recursive: true });
            writeFileSync(path.join(traceDir, `${runId}.json`), JSON.stringify({
              run_id: runId, observed_at: new Date().toISOString(), current_phase: null, phase_is_known: false,
              reason: "CHILD_TRACE_NOT_READY", stack: null, parent_elapsed_ms: Date.now() - startedAt.getTime(),
            }), { flag: "wx", mode: 0o640 });
          } catch { this.logger.warn("MONITOR_SNAPSHOT_WRITE_FAILED", { job, runId }); }
        }, snapshotDelay);
      }
      if (timeoutMs !== null) {
        timer = setTimeout(() => {
          timedOut = true;
          this.logger.error(`任务超时，终止 ${command}`, { job, runId });
          child.kill("SIGTERM");
          escalationTimer = setTimeout(() => {
            if (settled) return;
            this.logger.error(`SIGTERM 未结束任务，发送 SIGKILL ${command}`, { job, runId });
            child.kill("SIGKILL");
            hardFinishTimer = setTimeout(() => finish(null, "SIGKILL"), 2_000);
          }, 10_000);
        }, timeoutMs);
      }
      child.once("error", (error: Error) => {
        this.logger.error(`任务启动失败 ${command}: ${redactText(error.message)}`, { job, runId });
        finish(null, null);
      });
      child.once("close", (exitCode: number | null, signal: NodeJS.Signals | null) => finish(exitCode, signal));
    });
    return result;
  }

  public async stop(): Promise<void> {
    await Promise.all([
      this.stopChild(this.active),
      this.stopChild(this.comparison),
      this.stopChild(this.review),
      this.stopChild(this.notification),
    ]);
  }

  private async stopChild(active: ActiveChild | null): Promise<void> {
    if (!active) return;
    this.logger.warn(`正在终止活动任务 ${active.runId}`);
    active.process.kill("SIGTERM");
    const exitedAfterTerm = await waitForProcessExit(active.process, 10_000);
    if (exitedAfterTerm) return;
    this.logger.error(`SIGTERM 未结束任务，发送 SIGKILL ${active.runId}`);
    active.process.kill("SIGKILL");
    await waitForProcessExit(active.process, 2_000);
  }

  private record(record: JobRunRecord): void {
    this.history.push(record);
    while (this.history.length > this.config.maxMemoryLogs) this.history.shift();
  }

  private replaceRunning(record: JobRunRecord): void {
    const index = this.history.findIndex((item) => item.runId === record.runId);
    if (index >= 0) this.history[index] = record;
    else this.record(record);
  }

}

export async function runReadOnlyStatus(
  config: AppConfig,
  logger: LogStore,
): Promise<ReadOnlyCommandResult> {
  const child = spawn(config.pythonBin, ["-m", "liangjian_funnel", "status"], {
    cwd: config.rootDir,
    env: process.env,
    shell: false,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  let timedOut = false;
  const maxOutput = 2 * 1024 * 1024;
  child.stdout.on("data", (chunk: Buffer | string) => {
    if (stdout.length < maxOutput) stdout += chunk.toString().slice(0, maxOutput - stdout.length);
  });
  child.stderr.on("data", (chunk: Buffer | string) => {
    if (stderr.length < maxOutput) stderr += chunk.toString().slice(0, maxOutput - stderr.length);
  });
  const result = await new Promise<ReadOnlyCommandResult>((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      timedOut = true;
      child.kill("SIGTERM");
    }, config.statusTimeoutMs);
    const finish = (exitCode: number | null): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ stdout, stderr, exitCode, timedOut });
    };
    child.once("error", (error: Error) => {
      logger.warn(`读取 Python status 失败: ${redactText(error.message)}`, { job: "status" });
      finish(null);
    });
    child.once("close", (exitCode: number | null) => finish(exitCode));
  });
  return result;
}
