import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, vi } from "vitest";

import {
  legacyResearchPresentation,
  normalizePersistedResearchPresentation,
  unifiedPresentationStatus,
} from "../../shared/research-presentation.js";
import { loadConfig } from "../../server/config.js";
import { ProjectFiles } from "../../server/files.js";
import { LogStore } from "../../server/logger.js";
import { summarizeMonitorDispatch } from "../../server/dashboard.js";


test("UI-01 and UI-02 keep A3 entry pending and A2 stock facts separate", () => {
  const a3 = legacyResearchPresentation({
    eligibility: "QUALIFIED",
    execution_permission: "REQUIRES_A3_A4_CONFIRMATION",
    reason_codes: ["A3_REWARD_RISK_BELOW_MINIMUM"],
    a4_deferred_conditions: ["LIVE_REWARD_RISK"],
    plan_expiry: "2026-09-18T15:00:00+08:00",
    price_discovery: true,
    strategy_facts: { observation_targets: { r2: 12, target_basis: "R_MULTIPLE_NO_RESISTANCE_REQUIRED" } },
  }, "A3", "approved", "2026-09-18T10:00:00+08:00");
  expect(a3.a3).toMatchObject({
    dailySetupState: "QUALIFIED",
    a4ConfirmationState: "REQUIRED",
    currentEntryEligibility: "PENDING_A4",
    target: { kind: "FIXED_R_OBSERVATION", claim: "OBSERVATION_NOT_MARKET_PROOF" },
  });

  const first = legacyResearchPresentation({ theme_score: 88, relative_strength_score: 91, market_role: "LEADER" }, "A2", "approved");
  const second = legacyResearchPresentation({ theme_score: 88, relative_strength_score: 63, market_role: "FOLLOWER" }, "A2", "approved");
  expect(first.a2?.themeStrength).toBe(88);
  expect(first.a2?.stockRelativeStrength).toBe(91);
  expect(second.a2?.stockRelativeStrength).toBe(63);
  expect(first.a2?.individualTotalScore).toBeNull();
});

test("UI-03 keeps missing coverage unknown and critical/noncritical data distinct", () => {
  expect(unifiedPresentationStatus({
    jobStatus: "SUCCEEDED", dataState: "INSUFFICIENT", opportunityState: "UNKNOWN",
    actionabilityState: "UNKNOWN", criticalData: true, coverage: null,
  })).toMatchObject({ overallState: "BLOCKED_DATA", coverage: null });
  expect(unifiedPresentationStatus({
    jobStatus: "SUCCEEDED", dataState: "PARTIAL", opportunityState: "ABSENT",
    actionabilityState: "NO_ACTION", criticalData: false, coverage: null,
  })).toMatchObject({ overallState: "SUCCEEDED_DEGRADED_NO_OPPORTUNITY", coverage: null });
});

test("UI-04 persisted projection is accepted but unknown fields and credentials are removed", () => {
  const normalized = normalizePersistedResearchPresentation({
    schema_version: "research-presentation/1.0.0",
    stage: "A2",
    pool: "approved",
    source_identities: ["A2:FROZEN:1"],
    display_score: null,
    score_meaning: "NO_INDIVIDUAL_TOTAL_SCORE",
    a2: { theme_strength: 80, stock_relative_strength: 77, market_role: "LEADER", research_path: "EMOTION", individual_total_score: null },
    api_key: "secret",
  });
  expect(normalized).toMatchObject({ sourceIdentities: ["A2:FROZEN:1"], a2: { marketRole: "LEADER" } });
  expect(JSON.stringify(normalized)).not.toContain("secret");
});

test("UI-05 100 dashboard reads consume local capability snapshots without remote fetch", async () => {
  const root = await mkdtemp(join(tmpdir(), "liangjian-ui05-"));
  const fetchSpy = vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("network forbidden"));
  try {
    const dir = join(root, "outputs", "capabilities");
    await mkdir(dir, { recursive: true });
    await writeFile(join(dir, "mootdx.json"), JSON.stringify({
      provider: "MOOTDX", generated_at: "2026-09-18T10:00:00+08:00", overall_status: "PASS",
      checks: [{ name: "minute_1m", status: "PASS", evidence: { data_as_of: "2026-09-18T09:59:00+08:00", coverage: 0.98, affected_paths: ["A4"] } }],
    }));
    const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
    const files = new ProjectFiles(config, new LogStore(config));
    for (let index = 0; index < 100; index += 1) {
      const sources = await files.dataSources();
      expect(sources[0]?.capabilities[0]).toMatchObject({ capability: "minute_1m", coverage: 0.98, affectedPaths: ["A4"] });
    }
    expect(fetchSpy).not.toHaveBeenCalled();
  } finally {
    fetchSpy.mockRestore();
    await rm(root, { recursive: true, force: true });
  }
});

test("UI-06 A4 dispatch exposes schedule, valid-through and independent scope states", () => {
  const summary = summarizeMonitorDispatch({
    latest: {
      time: "2026-09-18T10:39:30+08:00",
      execution_cutoff: "2026-09-18T10:38:00+08:00",
      observability: {
        scheduled_at: "2026-09-18T10:39:00+08:00",
        started_at: "2026-09-18T10:39:01+08:00",
        deadline_at: "2026-09-18T10:39:45+08:00",
        required_scope: ["300136.SZ", "600001.SH"],
        ready_scope: ["300136.SZ"],
        blocked_scope: ["600001.SH"],
        no_signal_scope: [],
        axes: { job_status: "SUCCEEDED", data_state: "MISSING", opportunity_state: "PRESENT", trade_eligibility: "BLOCKED_DATA" },
      },
      simulation: [{ status: "FILLED" }],
      lanes: [{ blocked: false, events: [{ action: "BUY_SIGNAL", effective: true }] }],
    },
    activePlanCount: 2,
  });
  expect(summary).toMatchObject({
    scheduledAt: "2026-09-18T10:39:00+08:00",
    startedAt: "2026-09-18T10:39:01+08:00",
    deadlineAt: "2026-09-18T10:39:45+08:00",
    dataValidThrough: "2026-09-18T10:38:00+08:00",
    evaluatedCount: 1,
    gapCount: 1,
    noOpportunityCount: 0,
    deadlineState: "WITHIN_DEADLINE",
    modelState: "NOT_RECORDED",
    signalState: "PRESENT",
    fillState: "FILLED",
  });
});
