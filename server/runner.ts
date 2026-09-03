import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";

import type { AppConfig } from "./config.js";
import { JOB_DEFINITIONS } from "./config.js";
import { redactText } from "./redaction.js";
import { LogStore } from "./logger.js";
import type { JobName, JobRunRecord } from "./types.js";

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

export function timeoutForJob(
  job: JobName,
  configuredTimeoutMs: number,
  a1TimeoutMs: number = configuredTimeoutMs,
): number | null {
  // Research is checkpointed and must have a wall-clock boundary; unlimited
  // orchestration previously left stale close processes blocking every later
  // schedule. Minute monitoring has a much tighter deadline so it releases
  // the single worker before the 09:26/15:10 research protection windows.
  if (job === "monitor") return Math.min(configuredTimeoutMs, 55_000);
  if (job === "a5-midday" || job === "a5-close") return Math.min(configuredTimeoutMs, 10 * 60 * 1000);
  if (job === "a1") return a1TimeoutMs;
  return configuredTimeoutMs;
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
  // The optional comparison plane has its own child slot.  It is deliberately
  // independent from the priority slot so a slow Kimi/GLM run cannot block the
  // next morning/close research or the one-minute monitor.
  private active: ActiveChild | null = null;
  private comparison: ActiveChild | null = null;
  private review: ActiveChild | null = null;

  public constructor(
    private readonly config: AppConfig,
    private readonly logger: LogStore,
  ) {}

  public activeJob(): JobRunRecord | null {
    const current = this.history.find((item) => item.runId === (this.active?.runId ?? this.review?.runId ?? this.comparison?.runId));
    return current ?? null;
  }

  public recentRuns(limit = 100): JobRunRecord[] {
    return this.history.slice(-limit).reverse();
  }

  public async run(job: JobName): Promise<JobRunRecord> {
    const definition = definitionFor(job);
    if (!definition) {
      throw new Error("unsupported job");
    }
    const slot = job === "comparison" ? "comparison" : job === "a5-midday" || job === "a5-close" ? "review" : "primary";
    const current = slot === "comparison" ? this.comparison : slot === "review" ? this.review : this.active;
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

  private childFor(slot: "primary" | "comparison" | "review"): ActiveChild | null {
    return slot === "comparison" ? this.comparison : slot === "review" ? this.review : this.active;
  }

  private setChild(slot: "primary" | "comparison" | "review", child: ActiveChild | null): void {
    if (slot === "comparison") this.comparison = child;
    else if (slot === "review") this.review = child;
    else this.active = child;
  }

  private async runChild(
    job: JobName,
    command: string,
    slot: "primary" | "comparison" | "review",
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

    const child = spawn(this.config.pythonBin, ["-m", "liangjian_funnel", command], {
      cwd: this.config.rootDir,
      env: process.env,
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    this.setChild(slot, { runId, process: child });

    let stdoutPending = "";
    let stderrPending = "";
    child.stdout.on("data", (chunk: Buffer | string) => {
      const split = splitChunk(stdoutPending, chunk);
      stdoutPending = split.pending;
      for (const line of split.lines) this.logger.info(redactText(line, this.config.maxLogLineLength), { job, runId, stream: "stdout" });
    });
    child.stderr.on("data", (chunk: Buffer | string) => {
      const split = splitChunk(stderrPending, chunk);
      stderrPending = split.pending;
      for (const line of split.lines) this.logger.warn(redactText(line, this.config.maxLogLineLength), { job, runId, stream: "stderr" });
    });

    const result = await new Promise<JobRunRecord>((resolve) => {
      let settled = false;
      let timedOut = false;
      let timer: NodeJS.Timeout | null = null;
      let escalationTimer: NodeJS.Timeout | null = null;
      let hardFinishTimer: NodeJS.Timeout | null = null;
      const finish = (exitCode: number | null, signal: NodeJS.Signals | null): void => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        if (escalationTimer) clearTimeout(escalationTimer);
        if (hardFinishTimer) clearTimeout(hardFinishTimer);
        if (stdoutPending) this.logger.info(redactText(stdoutPending, this.config.maxLogLineLength), { job, runId, stream: "stdout" });
        if (stderrPending) this.logger.warn(redactText(stderrPending, this.config.maxLogLineLength), { job, runId, stream: "stderr" });
        const finishedAt = new Date();
        const durationMs = finishedAt.getTime() - startedAt.getTime();
        const status = timedOut ? "terminated" : exitCode === 0 ? "succeeded" : "failed";
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
        };
        this.replaceRunning(record);
        if (this.childFor(slot)?.runId === runId) this.setChild(slot, null);
        this.logger[status === "succeeded" ? "info" : "error"](
          `任务结束 ${command} status=${status} exit=${exitCode ?? "null"} signal=${signal ?? "none"} duration_ms=${durationMs}`,
          { job, runId },
        );
        resolve(record);
      };
      const timeoutMs = timeoutForJob(job, this.config.jobTimeoutMs, this.config.a1JobTimeoutMs);
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
