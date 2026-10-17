import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { JOB_DEFINITIONS, loadConfig } from "../../server/config.js";
import { LogStore } from "../../server/logger.js";
import { JobRunner, timeoutForJob } from "../../server/runner.js";
import { WorkflowScheduler } from "../../server/scheduler.js";
import type { JobName } from "../../server/types.js";

test("opening source evidence has its own bounded command, not a research invocation", () => {
  expect(JOB_DEFINITIONS.find(row => row.name === ("auction-evidence" as JobName))?.command)
    .toBe("run-auction-evidence");
  expect(timeoutForJob("auction-evidence" as JobName, 5400000)).toBe(30000);
});

test("source evidence dispatches once by 09:25:30 and never catches up after the window", async () => {
  const root = await mkdtemp(join(tmpdir(), "b3-opening-"));
  const calls: string[] = [];
  const runner = { run: async (job: JobName) => {
    calls.push(job); return { status: "succeeded", job };
  }};
  const logger = new LogStore(loadConfig({}, root));
  const scheduler = new WorkflowScheduler(runner as unknown as JobRunner, logger, { comparisonEnabled: false });
  try {
    for (const stamp of ["01:24:59", "01:25:00", "01:25:10", "01:25:30", "01:25:31", "01:26:10", "01:31:10"]) {
      await scheduler.tick(new Date(`2026-10-16T${stamp}Z`));
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    expect(calls.filter(x => x === "auction-evidence")).toHaveLength(1);
    expect(calls).toContain("morning");
    expect(calls).toContain("auction-refresh");
    expect(calls).toContain("monitor");
    const late = new WorkflowScheduler(runner as unknown as JobRunner, logger, { comparisonEnabled: false });
    await late.tick(new Date("2026-10-16T01:25:31Z"));
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(calls.filter(x => x === "auction-evidence")).toHaveLength(1);
    late.stop();
  } finally {
    scheduler.stop();
    await (logger as unknown as { writeChain: Promise<void> }).writeChain;
    await rm(root, { recursive: true, force: true });
  }
});
