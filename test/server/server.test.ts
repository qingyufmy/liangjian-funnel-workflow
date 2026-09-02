import { mkdtemp, mkdir, readFile, stat, utimes, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { expect, test } from "vitest";
import type { Request } from "express";

import { tokenMatches } from "../../server/auth.js";
import { createApp } from "../../server/api.js";
import { loadConfig } from "../../server/config.js";
import { DashboardData, normalizeA4Replay, summarizeMonitorDispatch } from "../../server/dashboard.js";
import { normalizeLaneOutcome, normalizeRunOutcome, normalizeStageOutcome, ProjectFiles, resolveWithinRoot } from "../../server/files.js";
import { LogStore } from "../../server/logger.js";
import { LarkSettingsStore } from "../../server/lark-settings.js";
import { redactText, sanitizeJson } from "../../server/redaction.js";
import { JobRunner, timeoutForJob, waitForProcessExit } from "../../server/runner.js";
import { isMonitorDispatchReady, isMonitorMinute, WorkflowScheduler } from "../../server/scheduler.js";
import type { JobName, JobRunRecord } from "../../server/types.js";

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
          focus_pool: [{
            symbol: "600001.SH",
            theme_score: 72,
            industry: "软件开发",
            industry_chain_role: "BOTTLENECK_NODE",
            market_role: "CORE_ARMY",
            supply_chain_role: "SUPPLIES_SCARCE_LAYER",
            capital_flow_score: 80,
            tier_structure: "COMPLETE",
            leader_structure: "CORE_ARMY",
            crowding_score: 40,
            index_chain_resonance_score: 75,
            selection_reasons: ["主题强度"],
            source_refs: ["a2-source"],
          }],
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
            plan_priority: "P1",
            priority_reasons: ["QUALIFIED_STANDARD", "STRONG_SETUP:PLATFORM_BREAKOUT"],
            reference_price: 10.8,
            reference_price_as_of: "2026-08-26T15:00:00+08:00",
            technical_cycle: "UPTREND",
            weekly_confirmation: { score: 80, state: "PERSISTENT" },
            trigger_zone: { low: 10, high: 11 },
            invalidation_level: 9.5,
            reward_risk: 2.4,
            stop_distance_pct: 4.5,
            risk_unit: 0.01,
            plan_id: "fixture-plan",
            plan_expiry: "2026-08-27T15:00:00+08:00",
            plan_hash: "fixture-plan-hash",
            first_resistance: 11.5,
            no_chase_condition: "高开超过5%不追",
            pressure_reduce_price: 11.5,
            pressure_basis: "FIRST_RESISTANCE",
            allowed_time_windows: [{ from: "09:32", to: "14:45" }],
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

test("stores Lark webhook locally while returning only masked status", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-lark-settings-"));
  const store = new LarkSettingsStore(root);
  const webhook = "https://open.larksuite.com/open-apis/bot/v2/hook/test-runtime-token";
  const status = await store.save(webhook);

  expect(status).toMatchObject({ configured: true, masked: "••••oken" });
  expect(JSON.stringify(status)).not.toContain(webhook);
  expect(await readFile(store.path, "utf8")).toContain(webhook);
  if (process.platform !== "win32") expect((await stat(store.path)).mode & 0o777).toBe(0o600);

  await expect(store.save("https://example.test/hook/secret")).rejects.toThrow("Webhook 格式无效");
  await expect(store.clear()).resolves.toEqual({ configured: false, masked: null, updatedAt: null });
});

test("normalizes canonical outcomes and keeps validated empty opportunity distinct from unavailable data", () => {
  const canonical = normalizeStageOutcome({
    outcome_v2: {
      schema_version: "research-outcome/3.0.0",
      stage: "A2",
      lifecycle_state: "TERMINAL",
      quality_state: "VALIDATED",
      opportunity_state: "ABSENT",
      publication_state: "READY",
      reason_codes: ["A2_NO_FOCUS_OPPORTUNITY"],
      counts: { input: 53, evaluated: 53, selected: 0 },
      data_coverage: { required: 53, actual: 53 },
      legacy_status: "VALIDATED_NO_OPPORTUNITY",
    },
  });
  expect(canonical).toMatchObject({
    stage: "A2",
    lifecycle_state: "TERMINAL",
    quality_state: "VALIDATED",
    opportunity_state: "ABSENT",
    counts: { input: 53, evaluated: 53, selected: 0 },
  });

  const unavailable = normalizeStageOutcome({
    stage: "A2",
    status: "BLOCKED_DATA_COVERAGE",
    input_count: 53,
    evaluated_count: 0,
    selected_count: 0,
  });
  expect(unavailable).toMatchObject({
    quality_state: "BLOCKED",
    opportunity_state: "UNKNOWN",
    reason_codes: ["DATA_COVERAGE_INSUFFICIENT"],
  });
});

test("normalizes lane and run outcomes without letting optional comparison lanes change the primary axes", () => {
  const lane = normalizeLaneOutcome({
    lane: "lane_1",
    model: "fixture-model",
    stages: [{ stage: "A1", status: "VALIDATED", symbols: ["600001.SH"] }],
  });
  expect(lane).toMatchObject({ lane_id: "lane_1", model: "fixture-model", opportunity_state: "PRESENT" });
  const run = normalizeRunOutcome({
    run_id: "fixture-run",
    outcome_v2: {
      schema_version: "research-outcome/3.0.0",
      run_id: "fixture-run",
      lifecycle_state: "TERMINAL",
      quality_state: "VALIDATED",
      opportunity_state: "PRESENT",
      publication_state: "READY",
      reason_codes: [],
      counts: { expected_lanes: 3, recorded_lanes: 3, required_lanes: 1, ready_required_lanes: 1 },
      data_coverage: {},
      legacy_status: "READY_DEGRADED",
      primary_lane_ids: ["lane_1"],
      comparison_status: "BLOCKED",
      lanes: [
        { ...lane, lane_id: "lane_1" },
        { ...lane, lane_id: "lane_2", quality_state: "FAILED", publication_state: "BLOCKED" },
      ],
    },
  });
  expect(run).toMatchObject({
    run_id: "fixture-run",
    quality_state: "VALIDATED",
    publication_state: "READY",
    comparison_status: "BLOCKED",
    primary_lane_ids: ["lane_1"],
  });
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
  expect(config.a1JobTimeoutMs).toBe(6 * 60 * 60 * 1000);
});

test("starts A4 only when a continuous-auction minute bar can be closed", () => {
  const clock = (hour: number, minute: number) => ({ date: "2026-08-31", weekday: 1, hour, minute, second: 0 });
  expect(isMonitorMinute(clock(9, 30))).toBe(false);
  expect(isMonitorMinute(clock(9, 31))).toBe(true);
  expect(isMonitorMinute(clock(13, 0))).toBe(false);
  expect(isMonitorMinute(clock(13, 1))).toBe(true);
  expect(isMonitorMinute(clock(15, 0))).toBe(true);
});

test("delays each A4 minute dispatch until the provider settling window", () => {
  const clock = (second: number) => ({ date: "2026-08-31", weekday: 1, hour: 13, minute: 58, second });
  expect(isMonitorMinute(clock(0))).toBe(true);
  expect(isMonitorDispatchReady(clock(0))).toBe(false);
  expect(isMonitorDispatchReady(clock(2))).toBe(false);
  expect(isMonitorDispatchReady(clock(3))).toBe(true);
  expect(isMonitorDispatchReady(clock(59))).toBe(true);
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

test("reads the bounded A4 replay artifact separately from live monitor events", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-a4-monitor-"));
  await mkdir(join(root, "outputs", "monitor"), { recursive: true });
  await mkdir(join(root, "outputs", "evaluation"), { recursive: true });
  await writeFile(join(root, "outputs", "monitor", "latest.json"), JSON.stringify({
    time: "2026-08-28T10:00:00+08:00",
    lanes: [{ events: [{ effective: true, symbol: "600001.SH", action: "BUY_SIGNAL" }] }],
  }));
  await writeFile(join(root, "outputs", "evaluation", "a4_replay_latest.json"), JSON.stringify({
    schema_version: "liangjian-a4-replay/1.0.0",
    mode: "TEST_ONLY_COUNTERFACTUAL",
    trade_date: "2026-08-28",
    effective_events: [{ effective: true, symbol: "000859.SZ", action: "LLM_VETO" }],
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  const monitor = await files.monitor();
  expect(monitor.events).toHaveLength(1);
  expect(monitor.replay).toMatchObject({ mode: "TEST_ONLY_COUNTERFACTUAL", trade_date: "2026-08-28" });
});

test("joins A4 replay event, test plan, and matching paper fill for the workbench", () => {
  const replay = normalizeA4Replay({
    schema_version: "liangjian-a4-replay/1.0.0",
    mode: "TEST_ONLY_COUNTERFACTUAL",
    test_plan: {
      plan_id: "test-plan",
      symbol: "000859.SZ",
      name: "国风新材",
      test_risk_unit: "PROBE",
      trigger_low: 9.04,
      trigger_high: 9.05,
      stop_level: 8.68,
    },
    effective_events: [{
      plan_id: "test-plan",
      symbol: "000859.SZ",
      action: "BUY_SIGNAL",
      reason_code: "DETERMINISTIC_TRIGGER_PASS",
    }],
    fills: [{ symbol: "000859.SZ", action: "BUY", qty: 9100, price: 9.049, bar_end: "2026-08-28T09:51:00+08:00" }],
  }) as Record<string, unknown>;
  expect(replay.effectiveEvents).toMatchObject([{
    testOnly: true,
    plan: { planId: "test-plan", name: "国风新材", riskUnit: "PROBE" },
    simulation: { status: "FILLED", action: "BUY", qty: 9100, price: 9.049 },
  }]);
});

test("keeps a newer monitor failure visible instead of projecting the stale latest file as empty scope", () => {
  const runId = "monitor-failed";
  const logs = [
    { id: 1, timestamp: "2026-09-01T01:32:00.000Z", level: "info" as const, job: "monitor" as const, runId, stream: "node" as const, message: "开始执行 run-monitor" },
    { id: 2, timestamp: "2026-09-01T01:32:00.100Z", level: "info" as const, job: "monitor" as const, runId, stream: "stdout" as const, message: '        "reason_code": "MINUTE_CACHE_CONFLICT",' },
    { id: 3, timestamp: "2026-09-01T01:32:00.100Z", level: "info" as const, job: "monitor" as const, runId, stream: "stdout" as const, message: '        "symbol": "000713.SZ"' },
    { id: 4, timestamp: "2026-09-01T01:32:00.100Z", level: "info" as const, job: "monitor" as const, runId, stream: "stdout" as const, message: '      "status": "FAILED"' },
    { id: 5, timestamp: "2026-09-01T01:32:00.200Z", level: "error" as const, job: "monitor" as const, runId, stream: "node" as const, message: "任务结束 run-monitor status=failed exit=2 signal=none duration_ms=200" },
  ];
  const summary = summarizeMonitorDispatch({
    latest: {
      time: "2026-09-01T09:31:00+08:00",
      lanes: [{ blocked: false, events: [{ action: "EMPTY_SCOPE", effective: false, reason_code: "EMPTY_SCOPE" }] }],
    },
    logs,
    activePlanCount: 2,
  });
  expect(summary).toMatchObject({
    status: "FAILED",
    latestRunId: runId,
    lastReasonCode: "MINUTE_CACHE_CONFLICT",
    affectedPlanCount: 2,
    affectedSymbols: ["000713.SZ"],
    failureCount: 1,
  });
});

test("classifies a successful monitor with no trigger separately from an empty scope", () => {
  const summary = summarizeMonitorDispatch({
    latest: {
      time: "2026-09-01T05:32:00+08:00",
      lanes: [{ blocked: false, events: [{ action: "NO_ACTION", effective: false, reason_code: "STRATEGY_TRIGGER_NOT_MET" }] }],
    },
    logs: [],
    activePlanCount: 1,
  });
  expect(summary.status).toBe("SUCCEEDED_NO_ACTION");
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
    outcome: {
      schema_version: "research-outcome/3.0.0",
      stage: "A1",
      lifecycle_state: "TERMINAL",
      quality_state: "VALIDATED",
      opportunity_state: "PRESENT",
    },
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
    detailState: "COMPLETE",
    missingFields: [],
    decisionFacts: {
      industryChainRole: null,
      businessExposure: null,
      capitalFlow: null,
      tierStructure: null,
      leaderStructure: null,
      crowding: null,
      technicalCycle: null,
      weeklyConfirmation: null,
      indexChainResonance: null,
    },
  });
  expect(JSON.stringify(approved)).not.toContain("raw");

  const snapshotName = await files.researchStageDetail("fixture-run", "lane_1", "A1", "watch", 1, 50, "600002", "WAIT_CONFIRMATION");
  expect(snapshotName?.items[0]).toMatchObject({ symbol: "600002.SZ", name: "快照补全", nameSource: "snapshot" });

  const laneName = await files.researchStageDetail("fixture-run", "lane_1", "A2", "approved", 1, 50, "600001", "");
  expect(laneName).toMatchObject({ inputCount: 3, outputCount: 2, reasonOptions: [] });
  expect(laneName?.items[0]).toMatchObject({
    symbol: "600001.SH",
    name: "模型名称",
    nameSource: "lane_a1",
    detailState: "COMPLETE",
    missingFields: [],
    decisionFacts: {
      industryChainRole: "BOTTLENECK_NODE",
      marketRole: "CORE_ARMY",
      supplyChainRole: "SUPPLIES_SCARCE_LAYER",
      capitalFlow: 80,
      tierStructure: "COMPLETE",
      leaderStructure: "CORE_ARMY",
      crowding: 40,
      indexChainResonance: 75,
    },
  });
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
    detailState: "COMPLETE",
    missingFields: [],
    decisionFacts: {
      technicalCycle: "UPTREND",
      weeklyConfirmation: { score: 80, state: "PERSISTENT" },
    },
    plan: {
      setupType: "BREAKOUT_RETEST",
      planPriority: "P1",
      priorityReasons: ["QUALIFIED_STANDARD", "STRONG_SETUP:PLATFORM_BREAKOUT"],
      planHash: "fixture-plan-hash",
      referencePrice: 10.8,
      referencePriceAsOf: "2026-08-26T15:00:00+08:00",
      triggerZone: { low: 10, high: 11 },
      invalidationLevel: 9.5,
      rewardRisk: 2.4,
      firstResistance: 11.5,
      noChaseCondition: "高开超过5%不追",
      pressureReducePrice: 11.5,
      pressureBasis: "FIRST_RESISTANCE",
      allowedTimeWindows: [{ from: "09:32", to: "14:45" }],
      stopDistancePct: 4.5,
      planId: "fixture-plan",
      timeframeStates: { daily_state: "UPTREND", m5_state: "BREAKOUT" },
    },
  });

  const replay = await files.researchStageDetail("replay-run", "lane_1", "A1", "rejected", 1, 50);
  expect(replay).toMatchObject({ inputCount: 1, outputCount: 0, total: 1 });
  expect(replay?.items[0]).toMatchObject({ symbol: "600004.SH", name: "回放快照名称", nameSource: "snapshot" });
});

test("streams the stage decision index without reading the full lane or snapshot", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-stage-index-"));
  const researchDir = join(root, "outputs", "research");
  await mkdir(researchDir, { recursive: true });
  const stem = "research_index-run_lane_1";
  const rows = Array.from({ length: 250 }, (_, index) => JSON.stringify({
    stage: "A1",
    pool: "rejected",
    symbol: `${String(index + 1).padStart(6, "0")}.SZ`,
    item: {
      symbol: `${String(index + 1).padStart(6, "0")}.SZ`,
      name: `股票${index + 1}`,
      reason_codes: [index % 2 ? "LOW_SCORE" : "NO_EVIDENCE"],
    },
  })).join("\n") + "\n";
  await writeFile(join(researchDir, `${stem}.decisions.ndjson`), rows);
  await writeFile(join(researchDir, `${stem}.decisions.json`), JSON.stringify({
    schema_version: "research-stage-decision-index/1.0.0",
    run_id: "index-run",
    lane_id: "lane_1",
    model: "fixture-model",
    data_file: `${stem}.decisions.ndjson`,
    counts: { A1: { approved: 0, watch: 0, rejected: 250 } },
    stages: { A1: { status: "VALIDATED", latency_ms: 55, input_count: 4017, output_count: 120 } },
    reason_options: { A1: { approved: [], watch: [], rejected: ["LOW_SCORE", "NO_EVIDENCE"] } },
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));
  const detail = await files.researchStageDetail(
    "index-run", "lane_1", "A1", "rejected", 2, 10, "股票", "LOW_SCORE",
  );
  expect(detail).toMatchObject({
    model: "fixture-model",
    status: "VALIDATED",
    latencyMs: 55,
    inputCount: 4017,
    outputCount: 120,
    total: 125,
    page: 2,
    pageSize: 10,
    reasonOptions: ["LOW_SCORE", "NO_EVIDENCE"],
  });
  expect(detail?.items).toHaveLength(10);
  expect(detail?.items[0]).toMatchObject({ symbol: "000022.SZ", name: "股票22", reasonCodes: ["LOW_SCORE"] });
});

test("rejects invalid stage detail parameters and preserves dashboard authentication", async () => {
  const root = await createResearchDetailFixture();
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3", LIANGJIAN_DASHBOARD_TOKEN: "fixture-token" }, root);
  const logger = new LogStore(config);
  const runner = new JobRunner(config, logger);
  const scheduler = new WorkflowScheduler(runner, logger);
  const dashboard = new DashboardData(config, new ProjectFiles(config, logger), runner, scheduler, logger);
  const larkSettings = new LarkSettingsStore(root);
  const server = createServer(createApp({ config, dashboard, runner, scheduler, logger, larkSettings, startedAt: Date.now() }));
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
    const unauthorizedSettings = await fetch(`${base}/api/settings/lark`);
    expect(unauthorizedSettings.status).toBe(401);
    const configured = await fetch(`${base}/api/settings/lark`, {
      method: "PUT",
      headers: { Authorization: "Bearer fixture-token", "Content-Type": "application/json" },
      body: JSON.stringify({ webhookUrl: "https://open.larksuite.com/open-apis/bot/v2/hook/api-fixture-token" }),
    });
    expect(configured.status).toBe(200);
    const configuredBody = await configured.json() as Record<string, unknown>;
    expect(configuredBody).toMatchObject({ configured: true, masked: "••••oken" });
    expect(JSON.stringify(configuredBody)).not.toContain("api-fixture-token");
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

test("projects only safe bounded stage diagnostics without exposing model content", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-diagnostics-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    run_id: "diagnostics-run",
    status: "RUNNING",
    phase: "RESEARCH_MACRO_DISCOVERY",
    lanes: {
      LANE_1: {
        model: "fixture-model",
        status: "FAILED",
        current_stage: "MACRO_DISCOVERY",
        stages: {
          MACRO_DISCOVERY: {
            status: "FAILED",
            diagnostics: {
              last_invalid_output_shape: {
                type: "object",
                fields: ["envelope", "private_field", "structural_themes"],
                unknown_field_count: 99_999_999,
                envelope_unknown_field_count: -4,
                raw_model_content: "must-not-appear",
              },
              semantic_attempts: 2,
              theme_count: 8,
              node_count: 40,
              mapping_count: 14,
              expected_mapping_count: 20,
              missing_mapping_codes: ["884001.TI", "", "private-mapping-code"],
              arbitrary: "must-not-appear",
            },
          },
        },
      },
    },
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));

  const progress = await files.workflowProgress();
  expect(progress?.lanes[0]?.stages[0]?.diagnostics).toEqual({
    lastInvalidOutputShape: {
      type: "object",
      fields: ["envelope", "structural_themes"],
      unknownFieldCount: 10_000_000,
      envelopeUnknownFieldCount: 0,
    },
    semanticAttempts: 2,
    themeCount: 8,
    nodeCount: 40,
    mappingCount: 14,
    expectedMappingCount: 20,
    missingMappingCount: 2,
  });
  const serialized = JSON.stringify(progress);
  expect(serialized).not.toContain("private_field");
  expect(serialized).not.toContain("raw_model_content");
  expect(serialized).not.toContain("private-mapping-code");
  expect(serialized).not.toContain("must-not-appear");
});

test("marks a running progress file as stale when its filesystem heartbeat times out", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-heartbeat-timeout-"));
  await mkdir(join(root, "state"), { recursive: true });
  const progressPath = join(root, "state", "workflow_progress.json");
  await writeFile(progressPath, JSON.stringify({
    run_id: "heartbeat-timeout",
    status: "RUNNING",
    phase: "A1",
    updated_at: "2026-01-01T00:00:00.000Z",
  }));
  const oldTime = new Date(Date.now() - 10_000);
  await utimes(progressPath, oldTime, oldTime);
  const config = loadConfig({
    LIANGJIAN_PYTHON_BIN: "python3",
    LIANGJIAN_WORKFLOW_PROGRESS_STALE_MS: "1000",
  }, root);
  const files = new ProjectFiles(config, new LogStore(config));

  await expect(files.workflowProgress()).resolves.toMatchObject({
    status: "STALE",
    issue: null,
    stale: true,
    staleIssue: "HEARTBEAT_TIMEOUT",
    runId: "heartbeat-timeout",
  });
});

test("keeps a recently modified running progress file in the running state", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-heartbeat-fresh-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    run_id: "heartbeat-fresh",
    status: "RUNNING",
    phase: "A1",
    updated_at: "2026-01-01T00:00:00.000Z",
  }));
  const config = loadConfig({
    LIANGJIAN_PYTHON_BIN: "python3",
    LIANGJIAN_WORKFLOW_PROGRESS_STALE_MS: "60000",
  }, root);
  const files = new ProjectFiles(config, new LogStore(config));

  await expect(files.workflowProgress()).resolves.toMatchObject({
    status: "RUNNING",
    issue: null,
    stale: false,
    staleIssue: null,
    runId: "heartbeat-fresh",
  });
});

test("does not mark an old completed progress file as stale", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-heartbeat-completed-"));
  await mkdir(join(root, "state"), { recursive: true });
  const progressPath = join(root, "state", "workflow_progress.json");
  await writeFile(progressPath, JSON.stringify({
    run_id: "heartbeat-completed",
    status: "COMPLETED",
    phase: "COMPLETE",
    updated_at: "2026-01-01T00:00:00.000Z",
  }));
  const oldTime = new Date(Date.now() - 10_000);
  await utimes(progressPath, oldTime, oldTime);
  const config = loadConfig({
    LIANGJIAN_PYTHON_BIN: "python3",
    LIANGJIAN_WORKFLOW_PROGRESS_STALE_MS: "1000",
  }, root);
  const files = new ProjectFiles(config, new LogStore(config));

  await expect(files.workflowProgress()).resolves.toMatchObject({
    status: "COMPLETED",
    issue: null,
    stale: false,
    staleIssue: null,
    runId: "heartbeat-completed",
  });
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
  expect(fresh).toMatchObject({ runId: "stale-run", phase: "RESEARCH_A1", status: "RUNNING", stale: false, staleIssue: null, issue: null });
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

test("projects macro discovery industry, decision, theme, node and mapping counters", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-macro-discovery-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    run_id: "close-contract-v3",
    status: "RUNNING",
    phase: "RESEARCH_MACRO_DISCOVERY",
    resources: {
      rss_current_mb: 612.5,
      rss_peak_mb: 820.25,
      system_mem_available_mb: 1450,
      swap_used_mb: 118,
      disk_free_mb: 9216,
      disk_free_ratio: 0.24,
      open_file_descriptors: 31,
    },
    lanes: {
      LANE_1: {
        model: "deepseek-v4-pro-0813",
        status: "RUNNING",
        current_stage: "MACRO_DISCOVERY",
        stages: {
          MACRO_DISCOVERY: {
            status: "RUNNING",
            completed_batches: 0,
            total_batches: 1,
            industry_count: 40,
            monthly_decision_count: 20,
            theme_count: 8,
            node_count: 40,
            mapping_count: 14,
          },
        },
      },
    },
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));

  const progress = await files.workflowProgress();
  expect(progress?.lanes[0]).toMatchObject({
    currentStage: "MACRO_DISCOVERY",
    industryCount: 40,
    monthlyDecisionCount: 20,
    themeCount: 8,
    nodeCount: 40,
    mappingCount: 14,
  });
  expect(progress?.lanes[0]?.stages[0]).toMatchObject({
    industryCount: 40,
    monthlyDecisionCount: 20,
    themeCount: 8,
    nodeCount: 40,
    mappingCount: 14,
  });
  expect(progress?.resources).toMatchObject({
    rssCurrentMb: 612.5,
    rssPeakMb: 820.25,
    systemMemAvailableMb: 1450,
    diskFreeMb: 9216,
    openFileDescriptors: 31,
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

test("projects stale running lanes as terminal when the overall progress is complete", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-progress-terminal-lane-"));
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "state", "workflow_progress.json"), JSON.stringify({
    run_id: "terminal-lane",
    status: "READY_DEGRADED",
    job_status: "RUNNING",
    phase: "COMPLETED",
    lanes: {
      LANE_1: {
        model: "deepseek-v4-pro-0813",
        status: "RUNNING",
        job_status: "RUNNING",
        current_stage: "A2_LLM_REVIEW",
        stages: {
          A1_LLM_REVIEW: { status: "COMPLETED", completed_batches: 10, total_batches: 248 },
          A2_LLM_REVIEW: { status: "DEGRADED_UNDERFILLED_DATA_GAP", reason_codes: ["A2_FACTOR_COVERAGE_BELOW_MINIMUM"] },
        },
      },
    },
  }));
  const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
  const files = new ProjectFiles(config, new LogStore(config));

  const progress = await files.workflowProgress();
  expect(progress).toMatchObject({ status: "READY_DEGRADED", jobStatus: "SUCCEEDED", phase: "COMPLETED" });
  expect(progress?.lanes[0]).toMatchObject({
    laneId: "lane_1",
    status: "READY_DEGRADED",
    jobStatus: "SUCCEEDED",
    currentStage: null,
  });
  expect(progress?.lanes[0]?.stages).toEqual(expect.arrayContaining([
    expect.objectContaining({ stage: "A1_LLM_REVIEW", status: "PARTIAL", jobStatus: "SUCCEEDED" }),
    expect.objectContaining({ stage: "A2_LLM_REVIEW", status: "DEGRADED_UNDERFILLED_DATA_GAP", jobStatus: "SUCCEEDED" }),
  ]));
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
      daily_updates: 0,
      financial_refreshes: 0,
      deferred_financial_refreshes: 0,
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
    dailyUpdates: 0,
    financialRefreshes: 0,
    deferredFinancialRefreshes: 0,
    etaMs: 600_000,
    phaseStartedAt: "2026-08-26T13:52:38.000Z",
  });
  expect(JSON.stringify(progress)).not.toContain("private PDF contents");
});

test("scheduler gives research exclusive dispatch in its protection minute", async () => {
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
  expect(calls).toEqual(["morning"]);
});

test("dispatches A4 once at the settled second without catch-up", async () => {
  const calls: string[] = [];
  const fakeRunner = {
    run: async (job: "morning" | "close" | "monitor"): Promise<JobRunRecord> => {
      calls.push(job);
      return {
        runId: `${job}-1`,
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
  const root = await mkdtemp(join(tmpdir(), "liangjian-monitor-settle-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger, { comparisonEnabled: false });

  // 2026-08-31T05:58Z = 13:58 Asia/Shanghai. The minute key is the same
  // across the settling window; second zero must not dispatch or catch up.
  await scheduler.tick(new Date("2026-08-31T05:58:00.000Z"));
  expect(calls).toEqual([]);
  await scheduler.tick(new Date("2026-08-31T05:58:03.000Z"));
  await new Promise<void>((resolve) => setImmediate(resolve));
  await scheduler.tick(new Date("2026-08-31T05:58:04.000Z"));
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["monitor"]);
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
  expect(calls).toEqual(["morning", "morning"]);
});

test("scheduler dispatches weekday incremental and Saturday full maintenance but never Sunday", async () => {
  const calls: string[] = [];
  const fakeRunner = {
    run: async (job: JobName): Promise<JobRunRecord> => {
      calls.push(job);
      return {
        runId: job,
        job,
        command: job === "features" ? "maintain-features" : job,
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
  const root = await mkdtemp(join(tmpdir(), "liangjian-feature-scheduler-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as import("../../server/runner.js").JobRunner, logger);

  // 2026-08-27T19:30Z = Friday 03:30 Asia/Shanghai.
  await scheduler.tick(new Date("2026-08-27T19:30:10.000Z"));
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["features"]);

  // 2026-08-28T19:30Z = Saturday 03:30 Asia/Shanghai.
  await scheduler.tick(new Date("2026-08-28T19:30:10.000Z"));
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["features", "features"]);

  // 2026-08-29T19:30Z = Sunday 03:30 Asia/Shanghai.
  await scheduler.tick(new Date("2026-08-29T19:30:10.000Z"));
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["features", "features"]);
});

test("feature maintenance flag disables only the 03:30 job", async () => {
  const calls: string[] = [];
  const fakeRunner = {
    run: async (job: JobName): Promise<JobRunRecord> => {
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
  const root = await mkdtemp(join(tmpdir(), "liangjian-feature-disabled-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as JobRunner, logger, {
    featureMaintenanceEnabled: false,
    comparisonEnabled: false,
    now: () => new Date("2026-08-31T00:00:00.000Z"),
    intervalMs: 60_000,
  });
  scheduler.start();

  await scheduler.tick(new Date("2026-08-27T19:30:10.000Z"));
  await scheduler.tick(new Date("2026-08-31T01:26:10.000Z"));
  await new Promise<void>((resolve) => setImmediate(resolve));

  expect(calls).toEqual(["morning"]);
  expect(scheduler.snapshot().jobs.find((job) => job.job === "features")?.enabled).toBe(false);
  expect(scheduler.snapshot().jobs.find((job) => job.job === "morning")?.enabled).toBe(true);
  scheduler.stop();
});

test("scheduler wakes the Python-owned A1 cadence gate at 18:00 on weekdays only", async () => {
  const calls: JobName[] = [];
  const fakeRunner = {
    run: async (job: JobName): Promise<JobRunRecord> => {
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
  const root = await mkdtemp(join(tmpdir(), "liangjian-a1-scheduler-"));
  const logger = new LogStore(loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root));
  const scheduler = new WorkflowScheduler(fakeRunner as unknown as JobRunner, logger, {
    comparisonEnabled: false,
  });

  await scheduler.tick(new Date("2026-08-31T10:00:05.000Z")); // Monday 18:00 Asia/Shanghai
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["a1"]);

  await scheduler.tick(new Date("2026-09-05T10:00:05.000Z")); // Saturday
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(calls).toEqual(["a1"]);
});

test("process-exit wait returns after timeout so shutdown can escalate to SIGKILL", async () => {
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 1000)"], { stdio: "ignore", windowsHide: true });
  const exited = await waitForProcessExit(child, 5);
  expect(exited).toBe(false);
  child.kill("SIGKILL");
  expect(await waitForProcessExit(child, 2_000)).toBe(true);
});

test("all jobs have a bounded control-plane timeout", () => {
  expect(timeoutForJob("close", 1234)).toBe(1234);
  expect(timeoutForJob("morning", 1234)).toBe(1234);
  expect(timeoutForJob("monitor", 1234)).toBe(1234);
  expect(timeoutForJob("monitor", 90_000)).toBe(55_000);
  expect(timeoutForJob("a1", 90_000, 6 * 60 * 60 * 1000)).toBe(6 * 60 * 60 * 1000);
});
