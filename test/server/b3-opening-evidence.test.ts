import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { JOB_DEFINITIONS, loadConfig } from "../../server/config.js";
import { LogStore } from "../../server/logger.js";
import { JobRunner, timeoutForJob } from "../../server/runner.js";
import { WorkflowScheduler, auctionEvidenceSlot, shanghaiClock } from "../../server/scheduler.js";
import type { JobName } from "../../server/types.js";

test("opening source evidence has its own bounded command, not a research invocation", () => {
  expect(JOB_DEFINITIONS.find(row => row.name === ("auction-evidence" as JobName))?.command)
    .toBe("run-auction-evidence");
  expect(timeoutForJob("auction-evidence" as JobName, 5400000)).toBe(30000);
});

test("source evidence dispatches four exclusive slots and never catches up after a slot", async () => {
  const root = await mkdtemp(join(tmpdir(), "b3-opening-"));
  const calls: string[] = [];
  const runner = { run: async (job: JobName) => {
    calls.push(job); return { status: "succeeded", job };
  }};
  const logger = new LogStore(loadConfig({}, root));
  const scheduler = new WorkflowScheduler(runner as unknown as JobRunner, logger, { comparisonEnabled: false });
  try {
    for (const stamp of ["01:24:59", "01:25:00", "01:25:03", "01:25:04", "01:25:08", "01:25:09", "01:25:15", "01:25:16", "01:25:24", "01:25:25", "01:25:30", "01:25:31", "01:26:10", "01:31:10"]) {
      await scheduler.tick(new Date(`2026-10-16T${stamp}Z`));
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    expect(calls.filter(x => x === "auction-evidence")).toHaveLength(4);
    expect(calls).toContain("morning");
    expect(calls).toContain("auction-refresh");
    expect(calls).toContain("monitor");
    const late = new WorkflowScheduler(runner as unknown as JobRunner, logger, { comparisonEnabled: false });
    await late.tick(new Date("2026-10-16T01:25:31Z"));
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(calls.filter(x => x === "auction-evidence")).toHaveLength(4);
    late.stop();
  } finally {
    scheduler.stop();
    await (logger as unknown as { writeChain: Promise<void> }).writeChain;
    await rm(root, { recursive: true, force: true });
  }
});

test("poll slots use explicit Shanghai clock, not host timezone or minute catch-up", () => {
  for (const second of [3,8,15,24]) {
    expect(auctionEvidenceSlot(shanghaiClock(new Date(`2026-10-19T01:25:${String(second).padStart(2,"0")}Z`)))).toBe(second);
  }
  for (const second of [0,2,5,7,10,14,17,23,26,30,31]) {
    expect(auctionEvidenceSlot(shanghaiClock(new Date(`2026-10-19T01:25:${String(second).padStart(2,"0")}Z`)))).toBeUndefined();
  }
});

test("in-flight poll misses the next slot without overlap or late catch-up", async () => {
  const root = await mkdtemp(join(tmpdir(), "opening-busy-"));
  let finish: (() => void) | undefined;
  let calls=0;
  const runner={run:async () => {
    calls++;
    if (calls===1) await new Promise<void>(resolve=>{finish=resolve;});
    return {status:"succeeded"};
  }};
  const logger=new LogStore(loadConfig({},root));
  const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{comparisonEnabled:false});
  try {
    await scheduler.tick(new Date("2026-10-19T01:25:03Z"));
    await scheduler.tick(new Date("2026-10-19T01:25:08Z"));
    expect(calls).toBe(1);
    finish?.(); await new Promise<void>(resolve=>setImmediate(resolve));
    await scheduler.tick(new Date("2026-10-19T01:25:10Z"));
    expect(calls).toBe(1);
    await scheduler.tick(new Date("2026-10-19T01:25:15Z"));
    await new Promise<void>(resolve=>setImmediate(resolve));
    expect(calls).toBe(2);
  } finally {
    scheduler.stop();
    await (logger as unknown as {writeChain:Promise<void>}).writeChain;
    await rm(root,{recursive:true,force:true});
  }
});
