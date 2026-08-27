import { mkdtemp, mkdir, utimes, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { expect, test } from "vitest";
import type { Request } from "express";

import { tokenMatches } from "../../server/auth.js";
import { createApp } from "../../server/api.js";
import { loadConfig } from "../../server/config.js";
import { DashboardData } from "../../server/dashboard.js";
import { ProjectFiles, resolveWithinRoot } from "../../server/files.js";
import { LogStore } from "../../server/logger.js";
import { redactText, sanitizeJson } from "../../server/redaction.js";
import { JobRunner, timeoutForJob, waitForProcessExit } from "../../server/runner.js";
import { WorkflowScheduler } from "../../server/scheduler.js";
import type { JobRunRecord } from "../../server/types.js";

async function createResearchDetailFixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "liangjian-stage-detail-"));
  await mkdir(join(root, "outputs", "research"), { recursive: true });
  await mkdir(join(root, "outputs", "runs"), { recursive: true });
  await mkdir(join(root, "storage", "snapshots"), { recursive: true });
  await writeFile(join(root, "outputs", "runs", "fixture-run.json"), JSON.stringify({
    run_id: "fixture-run",
    snapshot: { path: "storage/snapshots/fixture-snapshot.json", selected_count: 7 },
  }));
  await writeFile(join(root, "storage", "snapshots", "fixture-snapshot.json"), JSON.stringify({
    data: {
      trade_candidates: [{ symbol: "600002.SZ", name: "快照补全" }],
    },
  }));
  await writeFile(join(root, "storage", "snapshots", "snapshot-replay+0800.json"), JSON.stringify({
    data: {
      snapshot_manifest: { selected_count: 1 },
      universe_candidates: [{ symbol: "600004.SH", name: "回放快照名称" }],
    },
  }));
  await writeFile(join(root, "outputs", "research", "research_fixture-run_lane_1.json"), JSON.stringify({
    lane: "lane_1",
    model: "fixture-model",
    stages: [
      {
        stage: "A1",
        status: "VALIDATED",
        latency_ms: 123,
        symbols: ["600001.SH", "600002.SZ", "600003.SH"],
        output: {
          active_research_pool: [{
            symbol: "600001.SH",
            company_name: "模型名称",
            primary_theme: "人工智能",
            ths_industries: [{ industry_name: "软件开发" }],
            structural_score: 88.5,
            core_thesis: ["收入增长", "现金流改善"],
            bear_case: ["估值偏高"],
            invalidation_conditions: ["收入增速跌破20%"],
            score_breakdown: { financial_quality: 90 },
            reason_codes: ["QUALITY_PASS"],
            source_refs: ["fixture-source"],
          }],
          monitor_pool: [{
            symbol: "600002.SZ",
            reason_codes: ["WAIT_CONFIRMATION"],
            evidence: ["等待趋势确认"],
          }],
          rejected_candidates: [{
            symbol: "600003.SH",
            reason_codes: ["LOW_PROFITABILITY"],
            veto_triggered: "盈利能力不足",
          }],
        },
      },
      {
        stage: "A2",
        status: "VALIDATED",
        latency_ms: 456,
        symbols: ["600001.SH", "600002.SZ"],
        output: {
          focus_pool: [{ symbol: "600001.SH", theme_score: 72, selection_reasons: ["主题强度"] }],
          watch_only_pool: [{ symbol: "600002.SZ", reason_codes: ["WAIT_CONFIRMATION"] }],
          rejected_candidates: [{ symbol: "600003.SH", reason_codes: ["THEME_WEAK"] }],
        },
      },
      {
        stage: "A3",
        status: "VALIDATED",
        latency_ms: 789,
        symbols: ["600001.SH"],
        output: {
          core_watch_pool: [{
            symbol: "600001.SH",
            technical_score: 81,
            setup_type: "BREAKOUT_RETEST",
            trigger_zone: { low: 10, high: 11 },
            invalidation_level: 9.5,
            reward_risk: 2.4,
            stop_distance_pct: 4.5,
            risk_unit: 0.01,
            plan_id: "fixture-plan",
            plan_expiry: "2026-08-27T15:00:00+08:00",
            confirmation_conditions: ["VWAP_RECLAIM"],
            daily_state: "UPTREND",
            m5_state: "BREAKOUT",
            scenarios: { normal_open_plan: { action: "ENTER" } },
          }],
          secondary_watch_pool: [],
          rejected_candidates: [{ symbol: "600003.SH", reason_codes: ["REWARD_RISK_TOO_LOW"] }],
        },
      },
    ],
  }));
  await writeFile(join(root, "outputs", "research", "research_replay-run_lane_1.json"), JSON.stringify({
    lane: "lane_1",
    model: "fixture-model",
    stages: [{
      stage: "A1",
      status: "VALIDATED",
      snapshot_id: "snapshot-replay+0800",
      symbols: [],
      output: { active_research_pool: [], monitor_pool: [], rejected_candidates: [{ symbol: "600004.SH", reason_codes: ["TEST_REJECT"] }] },
    }],
  }));
  return root;
}

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

test("projects paginated research stage pools with names, reasons, and allow-listed detail", async () => {
  const root = await createResearchDetailFixture();
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));

  const approved = await files.researchStageDetail("fixture-run", "lane_1", "A1", "approved", 1, 1, "模型名称", "QUALITY_PASS");
  expect(approved).toMatchObject({
    runId: "fixture-run",
    laneId: "lane_1",
    model: "fixture-model",
    stage: "A1",
    latencyMs: 123,
    outputCount: 3,
    inputCount: 7,
    pool: "approved",
    page: 1,
    pageSize: 1,
    total: 1,
    reasonOptions: ["QUALITY_PASS"],
  });
  expect(approved?.pools).toEqual([
    { id: "approved", label: "晋级研究", count: 1 },
    { id: "watch", label: "持续观察", count: 1 },
    { id: "rejected", label: "淘汰", count: 1 },
  ]);
  expect(approved?.items[0]).toMatchObject({
    symbol: "600001.SH",
    name: "模型名称",
    nameSource: "model",
    theme: "人工智能",
    industry: "软件开发",
    score: 88.5,
    reasonCodes: ["QUALITY_PASS"],
    selectionReasons: ["收入增长", "现金流改善"],
    riskReasons: ["估值偏高"],
  });
  expect(JSON.stringify(approved)).not.toContain("raw");

  const snapshotName = await files.researchStageDetail("fixture-run", "lane_1", "A1", "watch", 1, 50, "600002", "WAIT_CONFIRMATION");
  expect(snapshotName?.items[0]).toMatchObject({ symbol: "600002.SZ", name: "快照补全", nameSource: "snapshot" });

  const laneName = await files.researchStageDetail("fixture-run", "lane_1", "A2", "approved", 1, 50, "600001", "");
  expect(laneName).toMatchObject({ inputCount: 3, outputCount: 2, reasonOptions: [] });
  expect(laneName?.items[0]).toMatchObject({ symbol: "600001.SH", name: "模型名称", nameSource: "lane_a1" });
  expect(laneName?.pools).toEqual([
    { id: "approved", label: "聚焦候选", count: 1 },
    { id: "watch", label: "仅观察", count: 1 },
    { id: "rejected", label: "淘汰", count: 1 },
  ]);

  const a3 = await files.researchStageDetail("fixture-run", "lane_1", "A3", "approved", 1, 50);
  expect(a3).toMatchObject({ inputCount: 2, outputCount: 1, reasonOptions: [] });
  expect(a3?.pools).toEqual([
    { id: "approved", label: "核心计划", count: 1 },
    { id: "watch", label: "次级观察", count: 0 },
    { id: "rejected", label: "淘汰", count: 1 },
  ]);
  expect(a3?.items[0]).toMatchObject({
    symbol: "600001.SH",
    plan: {
      setupType: "BREAKOUT_RETEST",
      triggerZone: { low: 10, high: 11 },
      invalidationLevel: 9.5,
      rewardRisk: 2.4,
      stopDistancePct: 4.5,
      planId: "fixture-plan",
      timeframeStates: { daily_state: "UPTREND", m5_state: "BREAKOUT" },
    },
  });

  const replay = await files.researchStageDetail("replay-run", "lane_1", "A1", "rejected", 1, 50);
  expect(replay).toMatchObject({ inputCount: 1, outputCount: 0, total: 1 });
  expect(replay?.items[0]).toMatchObject({ symbol: "600004.SH", name: "回放快照名称", nameSource: "snapshot" });
});

test("rejects invalid stage detail parameters and preserves dashboard authentication", async () => {
  const root = await createResearchDetailFixture();
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3", LIANGJIAN_DASHBOARD_TOKEN: "fixture-token" }, root);
  const logger = new LogStore(config);
  const runner = new JobRunner(config, logger);
  const scheduler = new WorkflowScheduler(runner, logger);
  const dashboard = new DashboardData(config, new ProjectFiles(config, logger), runner, scheduler, logger);
  const server = createServer(createApp({ config, dashboard, runner, scheduler, logger, startedAt: Date.now() }));
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("fixture server did not bind to a TCP port");
  const base = `http://127.0.0.1:${address.port}`;
  try {
    const unauthorized = await fetch(`${base}/api/research/runs/fixture-run/lanes/lane_1/stages/A1?pool=approved`);
    expect(unauthorized.status).toBe(401);
    const invalidLane = await fetch(`${base}/api/research/runs/fixture-run/lanes/lane_9/stages/A1?pool=approved`, {
      headers: { Authorization: "Bearer fixture-token" },
    });
    expect(invalidLane.status).toBe(400);
    const invalidPool = await fetch(`${base}/api/research/runs/fixture-run/lanes/lane_1/stages/A1?pool=all`, {
      headers: { Authorization: "Bearer fixture-token" },
    });
    expect(invalidPool.status).toBe(400);
    const valid = await fetch(`${base}/api/research/runs/fixture-run/lanes/lane_1/stages/A1?pool=watch&page=1&pageSize=1&q=快照补全`, {
      headers: { Authorization: "Bearer fixture-token" },
    });
    expect(valid.status).toBe(200);
    await expect(valid.json()).resolves.toMatchObject({ pool: "watch", total: 1, items: [{ symbol: "600002.SZ", name: "快照补全" }] });
    const missing = await fetch(`${base}/api/research/runs/missing/lanes/lane_1/stages/A1?pool=approved`, {
      headers: { Authorization: "Bearer fixture-token" },
    });
    expect(missing.status).toBe(404);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

test("returns null when the persisted workflow progress file is missing", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-missing-"));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  expect(await files.workflowProgress()).toBeNull();
});

test("projects a valid workflow progress document without exposing unknown fields", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-valid-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    run_id: "close-20260826",
    status: "running",
    phase: "A1",
    processed: 100,
    total: 5000,
    cache_hits: 90,
    cache_misses: 10,
    failures: 0,
    elapsed_ms: 12_345,
    eta_ms: 67_890,
    updated_at: "2026-08-26T07:10:00.000Z",
    api_key: "sk-should-not-appear",
    reasoning_content: "private model output",
    lanes: {
      lane_1: {
        model: "deepseek-v4-pro-0813",
        status: "running",
        current_stage: "A1",
        processed: 50,
        total: 2500,
        batch_processed: 2,
        batch_total: 10,
        stages: [{ stage: "A1", status: "running", processed: 50, total: 2500, batch_processed: 2, batch_total: 10 }],
      },
    },
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  const progress = await files.workflowProgress();
  expect(progress).toMatchObject({
    status: "RUNNING",
    issue: null,
    runId: "close-20260826",
    phase: "A1",
    processed: 100,
    total: 5000,
    cacheHits: 90,
    cacheMisses: 10,
    failures: 0,
    elapsedMs: 12_345,
    etaMs: 67_890,
    updatedAt: "2026-08-26T07:10:00.000Z",
  });
  expect(progress?.lanes[0]).toMatchObject({ laneId: "lane_1", model: "deepseek-v4-pro-0813", currentStage: "A1", batchProcessed: 2, batchTotal: 10 });
  expect(JSON.stringify(progress)).not.toContain("sk-should-not-appear");
  expect(JSON.stringify(progress)).not.toContain("private model output");
});

test("returns a fixed invalid summary for malformed workflow progress JSON", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-malformed-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), "{\"status\":\"running\",\"secret\":");
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  await expect(files.workflowProgress()).resolves.toMatchObject({ status: "INVALID", issue: "INVALID_JSON", stale: false, staleIssue: null, lanes: [] });
});

test("distinguishes an unreadable progress file from malformed JSON", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-unreadable-"));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config), {
    workflowProgressFs: {
      stat: async () => { throw Object.assign(new Error("permission denied: secret path"), { code: "EACCES" }); },
      readFile: async () => "{}",
    },
  });
  const progress = await files.workflowProgress();
  expect(progress).toMatchObject({ status: "INVALID", issue: "UNREADABLE", stale: false, staleIssue: null, lanes: [] });
  expect(JSON.stringify(progress)).not.toContain("permission denied");
  expect(JSON.stringify(progress)).not.toContain("secret path");
});

test("serves the last valid progress projection while the file is broken, then clears stale state on recovery", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-stale-"));
  let unreadable = false;
  let malformed = false;
  const validDocument = JSON.stringify({
    run_id: "stale-run",
    status: "RUNNING",
    phase: "RESEARCH_A1",
    data: { processed: 6, total: 248 },
    lanes: {
      LANE_1: {
        model: "fixture-model",
        status: "RUNNING",
        stages: { A1: { status: "RUNNING", completed_batches: 6, total_batches: 248 } },
      },
    },
  });
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config), {
    workflowProgressFs: {
      stat: async () => ({ isFile: () => true, size: validDocument.length }),
      readFile: async () => {
        if (unreadable) throw Object.assign(new Error("EACCES private detail"), { code: "EACCES" });
        if (malformed) return JSON.stringify({ status: "RUNNING", secret: "private model output" }).slice(0, -1);
        return validDocument;
      },
    },
  });

  const fresh = await files.workflowProgress();
  expect(fresh).toMatchObject({ runId: "stale-run", phase: "RESEARCH_A1", stale: false, staleIssue: null, issue: null });
  expect(fresh?.lanes[0]).toMatchObject({ laneId: "lane_1", batchProcessed: 6, batchTotal: 248 });

  malformed = true;
  const staleMalformed = await files.workflowProgress();
  expect(staleMalformed).toMatchObject({ runId: "stale-run", phase: "RESEARCH_A1", stale: true, issue: null, staleIssue: "INVALID_JSON" });
  expect(JSON.stringify(staleMalformed)).not.toContain("private model output");

  malformed = false;
  unreadable = true;
  const staleUnreadable = await files.workflowProgress();
  expect(staleUnreadable).toMatchObject({ runId: "stale-run", stale: true, issue: null, staleIssue: "UNREADABLE" });

  unreadable = false;
  const recovered = await files.workflowProgress();
  expect(recovered).toMatchObject({ runId: "stale-run", phase: "RESEARCH_A1", stale: false, issue: null, staleIssue: null });
});

test("returns a blocked summary when workflow progress exceeds the safe file size", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-oversize-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({ status: "running", note: "x".repeat(300_000) }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  const progress = await files.workflowProgress();
  expect(progress).toMatchObject({ status: "BLOCKED", issue: "OVERSIZE", lanes: [] });
  expect(JSON.stringify(progress)).not.toContain("x".repeat(100));
});

test("returns a fixed invalid summary when workflow progress has the wrong JSON shape", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-shape-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({ status: "running", lanes: "not-an-array" }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  await expect(files.workflowProgress()).resolves.toMatchObject({ status: "INVALID", issue: "INVALID_SHAPE", lanes: [] });
});

test("reads the Python progress writer shape including second-based timing and stage batches", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-python-shape-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    schema_version: 1,
    run_id: "close-20260826",
    job: "run-close",
    status: "RUNNING",
    phase: "RESEARCH_A1",
    started_at: "2026-08-26T15:00:00+08:00",
    updated_at: "2026-08-26T15:01:00+08:00",
    elapsed_seconds: 60,
    eta_seconds: null,
    data: { processed: 5000, total: 5000, cache_hits: 4900, cache_misses: 100, failures: 0 },
    lanes: {
      LANE_1: { model: "deepseek-v4-pro-0813", status: "RUNNING", stages: { A1: { status: "RUNNING", completed_batches: 4, total_batches: 20 } } },
    },
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  const progress = await files.workflowProgress();
  expect(progress).toMatchObject({ phase: "RESEARCH_A1", processed: 5000, total: 5000, cacheHits: 4900, cacheMisses: 100, elapsedMs: 60_000, etaMs: null });
  expect(progress?.lanes[0]).toMatchObject({ laneId: "lane_1", batchProcessed: 4, batchTotal: 20 });
});

test("projects deterministic V2 stock counters separately from model batches", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-v2-shape-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    run_id: "close-v2",
    status: "RUNNING",
    phase: "RESEARCH_A1_LOCAL_SCREEN",
    lanes: {
      LANE_1: {
        model: "deepseek-v4-pro-0813",
        status: "RUNNING",
        current_stage: "A1_LOCAL_SCREEN",
        stages: {
          A1_LOCAL_SCREEN: {
            status: "COMPLETED",
            completed_batches: 1,
            total_batches: 1,
            processed_symbols: 3886,
            total_symbols: 3886,
            selected_symbols: 36,
            monitor_symbols: 3800,
            rejected_symbols: 50,
          },
        },
      },
    },
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  const progress = await files.workflowProgress();
  expect(progress?.lanes[0]).toMatchObject({ processed: 3886, total: 3886 });
  expect(progress?.lanes[0]?.stages[0]).toMatchObject({
    stage: "A1_LOCAL_SCREEN",
    processed: 3886,
    total: 3886,
    batchProcessed: 1,
    batchTotal: 1,
    selected: 36,
    monitor: 3800,
    rejected: 50,
  });
});

test("does not label an incomplete aggregate stage as completed", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-incomplete-stage-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    run_id: "running-batches",
    status: "RUNNING",
    phase: "RESEARCH_A1",
    lanes: {
      LANE_1: {
        model: "deepseek-v4-pro-0813",
        status: "RUNNING",
        stages: { A1: { status: "COMPLETED", completed_batches: 10, total_batches: 248 } },
      },
    },
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));

  const progress = await files.workflowProgress();
  expect(progress?.lanes[0]?.stages[0]).toMatchObject({ status: "RUNNING", batchProcessed: 10, batchTotal: 248 });
});

test("projects allow-listed CNINFO PDF progress without exposing document payloads", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-cninfo-pdf-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    schema_version: 1,
    run_id: "close-20260826",
    job: "close",
    status: "RUNNING",
    phase: "CNINFO_PDF_SYNC",
    phase_started_at: "2026-08-26T21:52:38+08:00",
    updated_at: "2026-08-26T21:54:38+08:00",
    elapsed_seconds: 120,
    eta_seconds: 600,
    data: {
      processed: 25,
      total: 150,
      cache_hits: 20,
      cache_misses: 5,
      failures: 2,
      current_symbol: "600519.SH",
      current_document: "ANNUAL_REPORT_2025_120001",
      documents_succeeded: 23,
      documents_failed: 2,
      raw_text: "private PDF contents",
    },
    lanes: {},
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  const progress = await files.workflowProgress();
  expect(progress).toMatchObject({
    phase: "CNINFO_PDF_SYNC",
    processed: 25,
    total: 150,
    cacheHits: 20,
    cacheMisses: 5,
    failures: 2,
    currentSymbol: "600519.SH",
    currentDocument: "ANNUAL_REPORT_2025_120001",
    documentsSucceeded: 23,
    documentsFailed: 2,
    etaMs: 600_000,
    phaseStartedAt: "2026-08-26T13:52:38.000Z",
  });
  expect(JSON.stringify(progress)).not.toContain("private PDF contents");
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
