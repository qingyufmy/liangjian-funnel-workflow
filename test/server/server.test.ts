import { mkdtemp, mkdir, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { expect, test } from "vitest";
import type { Request } from "express";

import { tokenMatches } from "../../server/auth.js";
import { loadConfig } from "../../server/config.js";
import { ProjectFiles, resolveWithinRoot } from "../../server/files.js";
import { LogStore } from "../../server/logger.js";
import { redactText, sanitizeJson } from "../../server/redaction.js";
import { JobRunner, timeoutForJob, waitForProcessExit } from "../../server/runner.js";
import { WorkflowScheduler } from "../../server/scheduler.js";
import type { JobRunRecord } from "../../server/types.js";

function requestWithAuthorization(value: string | undefined): Request {
  return {
    header: (name: string): string | undefined => name.toLowerCase() === "authorization" ? value : undefined,
  } as unknown as Request;
}

test("redacts bearer, API keys, and private reasoning fields", () => {
  const text = redactText("Authorization: Bearer abc123 api_key=secret sk-test_123");
  expect(text).not.toContain("abc123");
  expect(text).not.toContain("secret");
  expect(text).not.toContain("sk-test_123");
  const safe = sanitizeJson({ token: "sk-hidden", reasoning_content: "private", nested: "Bearer hidden" });
  expect(safe).toEqual({ token: "[REDACTED]", nested: "Bearer [REDACTED]" });
});

test("rejects path traversal and accepts only paths inside root", () => {
  const root = join(tmpdir(), "liangjian-root");
  expect(resolveWithinRoot(root, "outputs/runs/a.json")).toBe(join(root, "outputs", "runs", "a.json"));
  expect(resolveWithinRoot(root, "../secrets.env")).toBeNull();
  expect(resolveWithinRoot(root, "outputs\\..\\..\\secrets.env")).toBeNull();
  expect(resolveWithinRoot(root, "C:\\secrets.env")).toBeNull();
  expect(resolveWithinRoot(root, "/etc/passwd")).toBeNull();
});

test("requires exact bearer token and does not compare different lengths", () => {
  const token = "dashboard-secret";
  expect(tokenMatches(requestWithAuthorization(`Bearer ${token}`), token)).toBe(true);
  expect(tokenMatches(requestWithAuthorization("Bearer dashboard-secre"), token)).toBe(false);
  expect(tokenMatches(requestWithAuthorization("Basic dashboard-secret"), token)).toBe(false);
});

test("uses BaoTa host and port variables and supports a test-only scheduler disable", () => {
  const config = loadConfig({
    HOST: "127.0.0.2",
    PORT: "4321",
    LIANGJIAN_PYTHON_BIN: "python3",
    LIANGJIAN_SCHEDULER_ENABLED: "false",
  }, join(tmpdir(), "liangjian-config"));
  expect(config.host).toBe("127.0.0.2");
  expect(config.port).toBe(4321);
  expect(config.schedulerEnabled).toBe(false);
});

test("reads and sorts fixed workflow run files without accepting arbitrary paths", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-control-plane-"));
  await mkdir(join(root, "outputs", "runs"), { recursive: true });
  const oldPath = join(root, "outputs", "runs", "old.json");
  const newPath = join(root, "outputs", "runs", "new.json");
  await writeFile(oldPath, JSON.stringify({ run_id: "old", slot: "close", status: "BLOCKED" }));
  await writeFile(newPath, JSON.stringify({ run_id: "new", slot: "morning", status: "READY" }));
  await utimes(oldPath, 1_700_000_000, 1_700_000_000);
  await utimes(newPath, 1_700_000_100, 1_700_000_100);
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const logger = new LogStore(config);
  const files = new ProjectFiles(config, logger);
  const runs = await files.listRuns(10);
  expect(runs.map((run) => run.runId)).toEqual(["new", "old"]);
  expect(await files.getRun("../old")).toBeNull();
});

test("scheduler dispatches each due minute once", async () => {
  const calls: string[] = [];
  const fakeRunner = {
    run: async (job: "morning" | "close" | "monitor"): Promise<JobRunRecord> => {
      calls.push(job);
      return {
        runId: job,
        job,
        command: job,
        startedAt: new Date().toISOString(),
        finishedAt: new Date().toISOString(),
        exitCode: 0,
        signal: null,
        durationMs: 0,
        status: "succeeded",
        reason: null,
      };
    },
    activeJob: (): JobRunRecord | null => null,
  };
  const root = await mkdtemp(join(tmpdir(), "liangjian-scheduler-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger);
  await scheduler.tick(new Date("2026-08-26T01:26:10.000Z"));
  await scheduler.tick(new Date("2026-08-26T01:26:30.000Z"));
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["morning", "monitor"]);
});

test("scheduler retries a research job skipped by an active monitor in the same minute", async () => {
  const calls: string[] = [];
  let morningAttempts = 0;
  const fakeRunner = {
    run: async (job: "morning" | "close" | "monitor"): Promise<JobRunRecord> => {
      calls.push(job);
      if (job === "morning" && morningAttempts++ === 0) {
        return {
          runId: "morning-skipped",
          job,
          command: job,
          startedAt: new Date().toISOString(),
          finishedAt: new Date().toISOString(),
          exitCode: null,
          signal: null,
          durationMs: 0,
          status: "skipped",
          reason: "BUSY:monitor-run",
        };
      }
      return {
        runId: job,
        job,
        command: job,
        startedAt: new Date().toISOString(),
        finishedAt: new Date().toISOString(),
        exitCode: 0,
        signal: null,
        durationMs: 0,
        status: "succeeded",
        reason: null,
      };
    },
    activeJob: (): JobRunRecord | null => null,
  };
  const root = await mkdtemp(join(tmpdir(), "liangjian-retry-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const now = new Date("2026-08-26T01:26:10.000Z");
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as JobRunner, logger, {
    now: () => now,
    retryMs: 10,
  });
  await scheduler.tick(now);
  await new Promise<void>((resolve) => setTimeout(resolve, 50));
  expect(calls).toEqual(["morning", "monitor", "morning"]);
});

test("process-exit wait returns after timeout so shutdown can escalate to SIGKILL", async () => {
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 1000)"], { stdio: "ignore", windowsHide: true });
  const exited = await waitForProcessExit(child, 5);
  expect(exited).toBe(false);
  child.kill("SIGKILL");
  expect(await waitForProcessExit(child, 2_000)).toBe(true);
});

test("full-market close research has no control-plane total timeout", () => {
  expect(timeoutForJob("close", 1234)).toBeNull();
  expect(timeoutForJob("morning", 1234)).toBe(1234);
  expect(timeoutForJob("monitor", 1234)).toBe(1234);
});
