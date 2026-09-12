import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { loadConfig } from "../../server/config.js";
import { LogStore } from "../../server/logger.js";
import { WorkflowScheduler } from "../../server/scheduler.js";
import { JobRunner, timeoutForJob } from "../../server/runner.js";
import type { JobName } from "../../server/types.js";

test("auction research dispatches once and does not stop A4 minute dispatch", async () => {
  const root = await mkdtemp(join(tmpdir(), "auction-refresh-"));
  const calls: JobName[] = [];
  const runner = { run: async (job: JobName) => {
    calls.push(job);
    return { status: "succeeded", job };
  }};
  const logger = new LogStore(loadConfig({}, root));
  const scheduler = new WorkflowScheduler(runner as unknown as JobRunner, logger, { comparisonEnabled: false });
  try {
    for (const stamp of ["01:25:10", "01:26:10", "01:27:10", "01:29:10", "01:31:10"]) {
      await scheduler.tick(new Date(`2026-09-14T${stamp}Z`));
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    expect(calls.filter(x => x === "auction-refresh")).toHaveLength(1);
    expect(calls).toContain("morning");
    expect(calls).toContain("monitor");
    expect(timeoutForJob("auction-refresh", 3600_000)).toBe(1800_000);
  } finally {
    scheduler.stop();
    await (logger as unknown as { writeChain: Promise<void> }).writeChain;
    await rm(root, { recursive: true, force: true });
  }
});

test("auction research uses the review worker even if A4 primary is occupied", async () => {
  const root = await mkdtemp(join(tmpdir(), "auction-slot-"));
  try {
    const runner = new JobRunner(loadConfig({}, root), new LogStore(loadConfig({}, root)));
    const probe = runner as unknown as { active: unknown; runChild: (job: string, command: string, slot: string) => Promise<unknown> };
    probe.active = { runId: "A4-busy" };
    let observed = "";
    probe.runChild = async (_job, _command, slot) => { observed = slot; return {}; };
    await runner.run("auction-refresh");
    expect(observed).toBe("review");
  } finally { await rm(root, { recursive: true, force: true }); }
});
