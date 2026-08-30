import { mkdtemp } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { expect, test, vi } from "vitest";

import { loadConfig } from "../../server/config.js";
import { LogStore } from "../../server/logger.js";
import { WorkflowScheduler } from "../../server/scheduler.js";
import type { JobName, JobRunRecord } from "../../server/types.js";

function result(
  job: JobName,
  status: JobRunRecord["status"] = "succeeded",
  reason: string | null = null,
): JobRunRecord {
  return {
    runId: `${job}-run`,
    job,
    command: job === "comparison" ? "run-comparison" : job,
    startedAt: new Date().toISOString(),
    finishedAt: new Date().toISOString(),
    exitCode: status === "succeeded" ? 0 : null,
    signal: null,
    durationMs: 1,
    status,
    reason,
  };
}

test("successful close dispatch starts comparison without changing the primary dispatch", async () => {
  const calls: JobName[] = [];
  const fakeRunner = {
    run: async (job: JobName): Promise<JobRunRecord> => {
      calls.push(job);
      return result(job);
    },
    activeJob: (): JobRunRecord | null => null,
  };
  const root = await mkdtemp(join(tmpdir(), "liangjian-primary-comparison-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger);

  await scheduler.tick(new Date("2026-08-26T07:10:10.000Z")); // 15:10 Asia/Shanghai
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["close", "comparison"]);
});

test("stable mode never starts optional comparison on startup or after close", async () => {
  const calls: JobName[] = [];
  const fakeRunner = {
    run: async (job: JobName): Promise<JobRunRecord> => {
      calls.push(job);
      return result(job);
    },
    activeJob: (): JobRunRecord | null => null,
  };
  const root = await mkdtemp(join(tmpdir(), "liangjian-stable-primary-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger, {
    now: () => new Date("2026-08-30T00:00:00.000Z"),
    intervalMs: 60_000,
    comparisonEnabled: false,
  });

  scheduler.start();
  await new Promise<void>((resolve) => setImmediate(resolve));
  await scheduler.tick(new Date("2026-08-31T07:10:10.000Z")); // 15:10 Asia/Shanghai
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["close"]);
  expect(scheduler.snapshot().jobs.find((job) => job.job === "comparison")?.enabled).toBe(false);
  scheduler.stop();
});

test("scheduler startup recovery requests only the optional comparison command", async () => {
  const calls: JobName[] = [];
  const fakeRunner = {
    run: async (job: JobName): Promise<JobRunRecord> => {
      calls.push(job);
      return result(job);
    },
    activeJob: (): JobRunRecord | null => null,
  };
  const root = await mkdtemp(join(tmpdir(), "liangjian-startup-recovery-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger, {
    now: () => new Date("2026-08-30T00:00:00.000Z"),
    intervalMs: 60_000,
  });

  scheduler.start();
  await new Promise<void>((resolve) => setImmediate(resolve));
  scheduler.stop();
  expect(calls).toEqual(["comparison"]);
});

test("busy comparison recovery retries without touching the primary slot", async () => {
  vi.useFakeTimers();
  let scheduler: WorkflowScheduler | undefined;
  try {
    const calls: JobName[] = [];
    let comparisonCalls = 0;
    const fakeRunner = {
      run: async (job: JobName): Promise<JobRunRecord> => {
        calls.push(job);
        if (job === "comparison" && comparisonCalls++ === 0) {
          return result(job, "skipped", "BUSY:comparison-active");
        }
        return result(job);
      },
      activeJob: (): JobRunRecord | null => null,
    };
    const root = await mkdtemp(join(tmpdir(), "liangjian-comparison-retry-"));
    const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
    scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger, {
      now: () => new Date("2026-08-30T00:00:00.000Z"),
      retryMs: 100,
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(calls).toEqual(["comparison"]);

    await vi.advanceTimersByTimeAsync(99);
    expect(calls).toEqual(["comparison"]);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toEqual(["comparison", "comparison"]);
  } finally {
    scheduler?.stop();
    vi.useRealTimers();
  }
});

test("an in-flight comparison remembers a close trigger and drains it after completion", async () => {
  const calls: JobName[] = [];
  const resolves: Array<(value: JobRunRecord | PromiseLike<JobRunRecord>) => void> = [];
  const fakeRunner = {
    run: (job: JobName): Promise<JobRunRecord> => {
      calls.push(job);
      return new Promise<JobRunRecord>((resolve) => resolves.push(resolve));
    },
    activeJob: (): JobRunRecord | null => null,
  };
  const root = await mkdtemp(join(tmpdir(), "liangjian-comparison-pending-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger, {
    now: () => new Date("2026-08-30T00:00:00.000Z"),
  });

  scheduler.start();
  await Promise.resolve();
  expect(calls).toEqual(["comparison"]);
  (scheduler as unknown as { triggerComparison: (source: string) => void }).triggerComparison("after-close");
  expect(calls).toEqual(["comparison"]);

  resolves.shift()?.(result("comparison"));
  for (let index = 0; index < 5; index += 1) await Promise.resolve();
  expect(calls).toEqual(["comparison", "comparison"]);

  resolves.shift()?.(result("comparison"));
  for (let index = 0; index < 3; index += 1) await Promise.resolve();
  scheduler.stop();
});

test("comparison runner errors are logged and release the recovery gate", async () => {
  const calls: JobName[] = [];
  let comparisonCalls = 0;
  const fakeRunner = {
    run: async (job: JobName): Promise<JobRunRecord> => {
      calls.push(job);
      if (job === "comparison" && comparisonCalls++ === 0) throw new Error("comparison backend unavailable");
      return result(job);
    },
    activeJob: (): JobRunRecord | null => null,
  };
  const root = await mkdtemp(join(tmpdir(), "liangjian-comparison-error-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger, {
    now: () => new Date("2026-08-30T00:00:00.000Z"),
  });

  scheduler.start();
  for (let index = 0; index < 5; index += 1) await Promise.resolve();
  expect(calls).toEqual(["comparison"]);
  expect(logger.memory().some((event) => event.level === "error" && event.message.includes("comparison backend unavailable"))).toBe(true);

  (scheduler as unknown as { triggerComparison: (source: string) => void }).triggerComparison("manual-recovery");
  for (let index = 0; index < 5; index += 1) await Promise.resolve();
  expect(calls).toEqual(["comparison", "comparison"]);
  scheduler.stop();
});
