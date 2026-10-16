import { EventEmitter } from "node:events";
import { mkdir, mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, test, vi } from "vitest";
import { loadConfig } from "../../server/config.js";
import { LogStore } from "../../server/logger.js";
import { JobRunner } from "../../server/runner.js";
import * as childProcess from "node:child_process";
vi.mock("node:child_process", async original => ({ ...await original<typeof import("node:child_process")>(), spawn:vi.fn() }));
afterEach(() => vi.restoreAllMocks());

test("actual runner binds current child business receipt into readonly DTO; no customer send", async () => {
  const root = await mkdtemp(join(tmpdir(),"liangjian-h1-runner-"));
  const config=loadConfig({},root), logger=new LogStore(config);
  const child=Object.assign(new EventEmitter(),{stdout:new EventEmitter(),stderr:new EventEmitter(),kill:vi.fn()});
  vi.mocked(childProcess.spawn).mockReturnValue(child as unknown as childProcess.ChildProcessWithoutNullStreams);
  const runner=new JobRunner(config,logger);
  try {
    const finished=runner.run("monitor");
    const run=runner.recentRuns(1)[0]!;
    const traceDir=join(root,"outputs/node/timeout-snapshots"); await mkdir(traceDir,{recursive:true});
    await writeFile(join(traceDir,`${run.runId}.receipt.json`),JSON.stringify({run_id:run.runId,
      business_status_observed:true,business_reason_counts:{STATE_CONFLICT_QUARANTINED:1},phase_timings:{},elapsed_ms:123}));
    child.emit("close",0,null);
    const result=await finished;
    expect(result.exitCode).toBe(0);
    expect(result.monitorBusiness).toMatchObject({status:"DEGRADED",reasonCounts:{STATE_CONFLICT_QUARANTINED:1}});
    expect(runner.recentRuns(1)[0]?.monitorBusiness).toEqual(result.monitorBusiness);
    expect(logger.memory().some(row=>row.message.includes("MONITOR_BUSINESS_HEALTH"))).toBe(true);
    // No webhook file in this sandbox; no credential lookup beyond its sibling path.
    await (runner as unknown as {monitorHealth:{flush():Promise<void>}}).monitorHealth.flush();
    expect(childProcess.spawn).toHaveBeenCalledTimes(1);
  } finally {
    await (logger as unknown as {writeChain:Promise<void>}).writeChain;
    await rm(root,{recursive:true,force:true});
  }
});
