import { expect, test } from "vitest";
import { timeoutForJob, monitorSnapshotDelay } from "../../server/runner.js";
import { ensureMonitorTimeoutSnapshot } from "../../server/runner.js";
import { mkdtempSync, writeFileSync, readFileSync, unlinkSync, rmdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { normalizeStageOutcome } from "../../server/files.js";

test("monitor parent has 58 seconds, no other deadline changes", () => {
  expect(timeoutForJob("monitor", 90_000)).toBe(58_000);
  expect(timeoutForJob("monitor", 1_000)).toBe(1_000);
  expect(timeoutForJob("close", 9_000_000)).toBe(5_400_000);
});

test("A2 quantitative fallback is displayed as degraded, not validated model output", () => {
  const outcome = normalizeStageOutcome({ status: "A2_REVIEW_DEGRADED", stage: "A2", counts: { input: 30, evaluated: 30, selected: 30 } });
  expect(outcome?.quality_state).toBe("DEGRADED");
  expect(outcome?.job_status).toBe("SUCCEEDED");
});

test("snapshot precedes the monitor timeout and never fires for other jobs", () => {
  expect(monitorSnapshotDelay("monitor", 58_000)).toBe(50_000);
  expect(monitorSnapshotDelay("monitor", 49_000)).toBe(null);
  expect(monitorSnapshotDelay("close", 5_400_000)).toBe(null);
});

test("C-blocked Python retains native stack and last checkpoint without a false complete claim", () => {
  const folder = mkdtempSync(path.join(tmpdir(), "b2-trace-"));
  try {
    writeFileSync(path.join(folder, "run.phase.json"), JSON.stringify({ run_id: "run", current_phase: "persist" }));
    writeFileSync(path.join(folder, "run.stacks.log"), "Thread: isolated C writer\n");
    ensureMonitorTimeoutSnapshot(folder, "run", 58_000);
    const data = readFileSync(path.join(folder, "run.json"), "utf8");
    expect(JSON.parse(data)).toMatchObject({ current_phase: "persist", phase_is_last_checkpoint: true, snapshot_is_parent_fallback: true });
    expect(JSON.parse(data).stack).toContain("isolated C writer");
    ensureMonitorTimeoutSnapshot(folder, "run", 59_000);
    expect(readFileSync(path.join(folder, "run.json"), "utf8")).toBe(data);
  } finally {
    for (const name of ["run.phase.json", "run.stacks.log", "run.json"]) unlinkSync(path.join(folder, name));
    rmdirSync(folder);
  }
});
