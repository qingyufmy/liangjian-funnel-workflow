import { describe, expect, test } from "vitest";

import { collectWorkbenchIssues } from "../../web/src/issues.js";
import type { OverviewResponse, StageOutcomeContract } from "../../web/src/types.js";

function stageOutcome(overrides: Partial<StageOutcomeContract> = {}): StageOutcomeContract {
  return {
    schema_version: "research-outcome/3.0.0",
    stage: "A2",
    job_status: "SUCCEEDED",
    lifecycle_state: "TERMINAL",
    quality_state: "VALIDATED",
    data_sufficiency_state: "SUFFICIENT",
    research_opportunity_state: "NOT_APPLICABLE",
    focus_opportunity_state: "ABSENT",
    actionability_state: "NOT_APPLICABLE",
    opportunity_state: "ABSENT",
    publication_state: "READY",
    reason_codes: ["A2_NO_FOCUS_OPPORTUNITY"],
    counts: { input: 90, evaluated: 90, selected: 0 },
    data_coverage: { required: 90, actual: 90 },
    legacy_status: "VALIDATED_NO_OPPORTUNITY",
    ...overrides,
  };
}

function overview(outcome: StageOutcomeContract = stageOutcome()): OverviewResponse {
  return {
    generatedAt: "2026-08-30T08:00:00+08:00",
    service: { status: "healthy", stateHealthy: true, configurationReady: true, deploymentReady: true, blockers: [] },
    schedule: [],
    latestWorkflow: {
      runId: "run-1", status: "VALIDATED", updatedAt: "2026-08-30T08:00:00+08:00",
      lanes: [{ laneId: "lane_1", model: "deepseek", status: "VALIDATED", stages: [{ stage: outcome.stage, status: outcome.legacy_status, outcome }] }],
    },
    workflowProgress: null,
    monitor: { status: "READY", events: [] },
    accounts: [],
    planCounts: { ACTIVE_TODAY: 1 },
    dataSources: [{ id: "ths", label: "同花顺", status: "HEALTHY" }],
    recentEffectiveEvents: [],
    recentLogs: [],
  };
}

describe("collectWorkbenchIssues", () => {
  test("does not misclassify a validated empty opportunity as a fault", () => {
    expect(collectWorkbenchIssues(overview(), [])).toEqual([]);
  });

  test("does not flag partial-but-present legacy coverage as a fault", () => {
    const value = overview(stageOutcome({ data_sufficiency_state: "PARTIAL", opportunity_state: "PRESENT", focus_opportunity_state: "PRESENT" }));
    expect(collectWorkbenchIssues(value, [])).toEqual([]);
  });

  test("tracks a data-insufficient stage with durable location", () => {
    const value = overview(stageOutcome({
      quality_state: "BLOCKED",
      data_sufficiency_state: "INSUFFICIENT",
      opportunity_state: "UNKNOWN",
      focus_opportunity_state: "UNKNOWN",
      publication_state: "BLOCKED",
      reason_codes: ["A2_CAPITAL_FLOW_DATA_GAP"],
      data_coverage: { required: 90, actual: 22 },
    }));
    const [issue] = collectWorkbenchIssues(value, []);
    expect(issue).toMatchObject({ status: "OPEN", severity: "WARNING", runId: "run-1", laneId: "lane_1", stage: "A2", code: "A2_CAPITAL_FLOW_DATA_GAP" });
  });

  test("deduplicates recurring runtime errors and retains the time range", () => {
    const logs = [
      { timestamp: "2026-08-30T08:02:00+08:00", level: "error", job: "close", runId: "run-1", message: "provider request 429 failed" },
      { timestamp: "2026-08-30T08:01:00+08:00", level: "error", job: "close", runId: "run-1", message: "provider request 429 failed" },
    ];
    const [issue] = collectWorkbenchIssues(overview(), logs);
    expect(issue).toMatchObject({ status: "OBSERVING", source: "RUNTIME", occurrenceCount: 2, firstSeenAt: "2026-08-30T08:01:00+08:00", lastSeenAt: "2026-08-30T08:02:00+08:00" });
  });

  test("reports zero active plans as informational observation only", () => {
    const value = overview();
    value.planCounts = {};
    const [issue] = collectWorkbenchIssues(value, []);
    expect(issue).toMatchObject({ code: "NO_ACTIVE_A3_PLAN", severity: "INFO", status: "OBSERVING" });
  });

  test("keeps a sufficient underfilled market result out of the open fault count", () => {
    const value = overview(stageOutcome({
      quality_state: "DEGRADED",
      data_sufficiency_state: "SUFFICIENT",
      opportunity_state: "PRESENT",
      focus_opportunity_state: "PRESENT",
      reason_codes: ["A2_FOCUS_POOL_UNDERFILLED_MARKET"],
      counts: { input: 90, evaluated: 90, selected: 6 },
    }));
    const [issue] = collectWorkbenchIssues(value, []);
    expect(issue).toMatchObject({ code: "MARKET_OPPORTUNITY_UNDERFILLED", severity: "INFO", status: "OBSERVING" });
  });
});
