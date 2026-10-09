import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { loadConfig } from "../../server/config.js";
import { normalizeWorkflowTiming, ProjectFiles } from "../../server/files.js";
import { LogStore } from "../../server/logger.js";

const entry = { kind: "PHASE", phase: "DATA_SYNC", lane_id: null, elapsed_seconds: 10,
  visits_count: 2, status: "RUNNING", started_at: "2026-10-10T00:00:00+08:00", ended_at: null };
const timing = { schema_version: "workflow-timing/1.0", python_elapsed_seconds: 10,
  run_wall_elapsed_seconds: 100, parent_elapsed_seconds: 30, budget_seconds: 100,
  budget_source: "NODE_TIMEOUT_FOR_JOB", parent_started_at: "2026-10-09T23:59:40+08:00",
  visits_dropped_count: 0, totals: [entry], visits: [entry] };

describe("WP5 safe stage timing DTO", () => {
  it("uses actual denominator and recomputes the ratio instead of trusting a provider value", () => {
    const out = normalizeWorkflowTiming({ ...timing, budget_used_ratio: 99, reasoning: "PRIVATE" });
    expect(out).toMatchObject({ budgetMs: 100000, parentElapsedMs: 30000, pythonElapsedMs: 10000,
      runWallElapsedMs: 100000, budgetUsedRatio: .3, stageTimesAreAdditive: false });
    expect(out?.totals[0]).toMatchObject({ elapsedMs: 10000, visitsCount: 2 });
    expect(JSON.stringify(out)).not.toContain("PRIVATE");
  });
  it("does not invent a default budget for legacy or invalid input", () => {
    expect(normalizeWorkflowTiming(null)).toBeNull();
    expect(normalizeWorkflowTiming({ ...timing, budget_seconds: "100" })?.budgetUsedRatio).toBeNull();
    expect(normalizeWorkflowTiming({ ...timing, budget_seconds: Infinity })?.budgetMs).toBeNull();
    expect(normalizeWorkflowTiming({ ...timing, budget_source: "MODEL_INFERRED" })?.budgetMs).toBeNull();
  });
  it("exports only bounded numeric entries and fixed tokens, not untrusted stage text", () => {
    const out = normalizeWorkflowTiming({ ...timing, visits: [
      { ...entry, phase: "private model response", secret: "PRIVATE" },
      { ...entry, elapsed_seconds: -1 }, { ...entry, elapsed_seconds: true },
      { ...entry, kind: "PRIVATE" },
    ] });
    expect(out?.visits).toHaveLength(1);
    expect(out?.visits[0].phase).toBe("UNKNOWN");
    expect(JSON.stringify(out)).not.toContain("PRIVATE");
    expect(JSON.stringify(out)).not.toContain("private model response");
  });
  it("preserves last recorded RUNNING timing across heartbeat failure without extrapolation", async () => {
    const root = await mkdtemp(join(tmpdir(), "wp5-timing-"));
    const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3", LIANGJIAN_WORKFLOW_PROGRESS_STALE_MS: "1000" }, root);
    const files = new ProjectFiles(config, new LogStore(config), { workflowProgressFs: {
      stat: async () => ({ isFile: () => true, size: 1000, mtimeMs: Date.now() - 5000 }),
      readFile: async () => JSON.stringify({ run_id: "r", status: "RUNNING", phase: "DATA_SYNC", timing }),
    } });
    const out = await files.workflowProgress();
    expect(out).toMatchObject({ status: "STALE", staleIssue: "HEARTBEAT_TIMEOUT" });
    expect(out?.timing?.visits[0]).toMatchObject({ status: "RUNNING", elapsedMs: 10000, endedAt: null });
  });
  it("recovers valid timing after broken progress JSON without losing cached duration", async () => {
    const root = await mkdtemp(join(tmpdir(), "wp5-timing-recovery-"));
    await mkdir(join(root, "state"));
    const path = join(root, "state/workflow_progress.json");
    const config = loadConfig({ LIANGJIAN_PYTHON_BIN: "python3" }, root);
    const files = new ProjectFiles(config, new LogStore(config));
    const body = { run_id: "r", status: "RUNNING", phase: "DATA_SYNC", timing };
    await writeFile(path, JSON.stringify(body));
    expect((await files.workflowProgress())?.timing?.totals[0].elapsedMs).toBe(10000);
    await writeFile(path, "{");
    const stale = await files.workflowProgress();
    expect(stale?.stale).toBe(true);
    expect(stale?.timing?.totals[0].elapsedMs).toBe(10000);
    await writeFile(path, JSON.stringify(body));
    expect((await files.workflowProgress())?.stale).toBe(false);
  });
});
