import { createServer } from "node:http";
import * as childProcess from "node:child_process";
import { afterEach, expect, test, vi } from "vitest";
import type { Request, Response } from "express";
import * as api from "../../server/api.js";
import { ProjectFiles } from "../../server/files.js";
import type { ApiDependencies } from "../../server/api.js";
import type { JobRunRecord } from "../../server/types.js";

vi.mock("node:child_process", async (original) => ({
  ...await original<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("CHILD_PROCESS_MUST_NOT_RUN"); }),
}));
afterEach(() => vi.restoreAllMocks());

function dependencies(token: string | null = null) {
  const job: JobRunRecord = { runId: "a5-original", job: "a5-close", command: "run-a5-close",
    startedAt: "2026-10-12T08:00:02.113Z", finishedAt: "2026-10-12T08:06:01.947Z",
    status: "succeeded", exitCode: 0, signal: null, durationMs: 359834, reason: null };
  const recentRuns = vi.fn(() => [job]);
  const status = vi.spyOn(ProjectFiles.prototype, "status").mockImplementation(async () => {
    throw new Error("STATUS_MUST_NOT_RUN");
  });
  const overview = vi.fn(() => { throw new Error("DASHBOARD_MUST_NOT_RUN"); });
  const run = vi.fn(() => { throw new Error("RUNNER_MUST_NOT_RUN"); });
  const deps = { config: { dashboardToken: token, timezone: "Asia/Shanghai", webDist: process.cwd() },
    runner: { recentRuns, run }, dashboard: { overview }, scheduler: {}, logger: {}, larkSettings: {},
    startedAt: Date.now() } as unknown as ApiDependencies;
  return { deps, job, recentRuns, run, status, overview };
}

async function withServer(deps: ApiDependencies, work: (url: string) => Promise<void>) {
  const server = createServer(api.createApp(deps));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("INVALID_LOCAL_FIXTURE_PORT");
  try { await work(`http://127.0.0.1:${address.port}/api/shadow/job-runs-readonly`); }
  finally { server.closeAllConnections(); await new Promise<void>((resolve) => server.close(() => resolve())); }
}

test("loopback GET returns exact original DTO and stable process birth, with zero status/spawn", async () => {
  const f = dependencies();
  const before = JSON.stringify(f.job);
  await withServer(f.deps, async (url) => {
    const first = await fetch(url);
    expect(first.status).toBe(200);
    expect(first.headers.get("cache-control")).toBe("no-store");
    const raw = await first.json();
    expect(raw.recentJobRuns).toEqual([f.job]);
    expect(raw.node.pid).toBe(process.pid);
    expect(Number.isFinite(raw.node.startedAtEpochMs)).toBe(true);
    expect(raw.node.startedAtEpochMs).toBeLessThanOrEqual(Date.parse(raw.node.observedAt));
    const second = await fetch(url).then((r) => r.json());
    expect(second.node.startedAtEpochMs).toBe(raw.node.startedAtEpochMs);
    expect(f.recentRuns).toHaveBeenCalledWith(1000);
  });
  expect(JSON.stringify(f.job)).toBe(before);
  expect(f.status).not.toHaveBeenCalled(); expect(f.overview).not.toHaveBeenCalled();
  expect(f.run).not.toHaveBeenCalled(); expect(childProcess.spawn).not.toHaveBeenCalled();
});

test.each(["POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])("%s cannot read or launch the route", async (method) => {
  const f = dependencies();
  await withServer(f.deps, async (url) => {
    expect((await fetch(url, { method })).status).toBe(405);
  });
  expect(f.recentRuns).not.toHaveBeenCalled();
  expect(f.status).not.toHaveBeenCalled(); expect(childProcess.spawn).not.toHaveBeenCalled();
});

test("existing dashboard bearer auth remains required when configured", async () => {
  const f = dependencies("fixture-token-only");
  await withServer(f.deps, async (url) => {
    expect((await fetch(url)).status).toBe(401);
    expect((await fetch(url, { headers: { Authorization: "Bearer fixture-token-only" } })).status).toBe(200);
  });
  expect(f.recentRuns).toHaveBeenCalledTimes(1);
});

test.each(["192.168.0.1", "::ffff:192.168.0.1", "10.0.0.2", "", undefined])(
  "remote socket %s is denied even when forwarding headers claim loopback", (remoteAddress) => {
    const f = dependencies();
    const request = { method: "GET", socket: { remoteAddress },
      headers: { "x-forwarded-for": "127.0.0.1", host: "127.0.0.1:3210" } } as unknown as Request;
    const status = vi.fn().mockReturnThis(), json = vi.fn(), setHeader = vi.fn();
    const response = { status, json, setHeader } as unknown as Response;
    expect(typeof api.shadowJobRunsReadOnlyRoute).toBe("function");
    api.shadowJobRunsReadOnlyRoute(f.deps.runner, 123.25)(request, response, vi.fn());
    expect(status).toHaveBeenCalledWith(403); expect(f.recentRuns).not.toHaveBeenCalled();
    expect(f.status).not.toHaveBeenCalled(); expect(childProcess.spawn).not.toHaveBeenCalled();
  });

test.each(["127.0.0.1", "::1", "::ffff:127.0.0.1"])("only literal local peer %s is accepted", (remoteAddress) => {
  const f = dependencies();
  const request = { method: "GET", socket: { remoteAddress }, headers: {} } as unknown as Request;
  const status = vi.fn().mockReturnThis(), json = vi.fn(), setHeader = vi.fn();
  const response = { status, json, setHeader } as unknown as Response;
  expect(typeof api.shadowJobRunsReadOnlyRoute).toBe("function");
  api.shadowJobRunsReadOnlyRoute(f.deps.runner, 123.25)(request, response, vi.fn());
  expect(json).toHaveBeenCalledWith(expect.objectContaining({ recentJobRuns: [f.job],
    node: expect.objectContaining({ pid: process.pid, startedAtEpochMs: 123.25 }) }));
  expect(status).not.toHaveBeenCalled();
});
