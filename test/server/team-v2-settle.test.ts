import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { JOB_DEFINITIONS, loadConfig } from "../../server/config.js";
import { LogStore } from "../../server/logger.js";
import { JobRunner, timeoutForJob } from "../../server/runner.js";
import { WorkflowScheduler } from "../../server/scheduler.js";
import type { JobName } from "../../server/types.js";

test("paper settlement has a real isolated command, sixty seconds, and no monitor budget change", () => {
  expect(JOB_DEFINITIONS.find(x => x.name === ("paper-settle" as JobName))?.command).toBe("run-paper-settle");
  expect(timeoutForJob("paper-settle" as JobName, 5400000)).toBe(60000);
  expect(timeoutForJob("monitor", 5400000)).toBe(58000);
});

test("15:30 settlement can recover once from BUSY without duplicate or research dispatch", async () => {
  const root=await mkdtemp(join(tmpdir(),"team-settle-"));
  const calls:string[]=[];let settleCalls=0;
  const runner={run:async(job:JobName) => {calls.push(job); const busy=job==='paper-settle'&&++settleCalls===1; return {
    status:busy?"skipped":"succeeded",job,reason:busy?"BUSY:review":null};}};
  const logger=new LogStore(loadConfig({},root));
  const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{comparisonEnabled:false});
  try {
    for (const iso of ["2026-10-19T07:29:59Z","2026-10-19T07:30:00Z","2026-10-19T07:30:01Z",
      "2026-10-19T07:30:06Z","2026-10-19T07:31:00Z","2026-10-19T07:39:00Z","2026-10-19T07:40:00Z"]) {
      await scheduler.tick(new Date(iso));await new Promise<void>(resolve=>setImmediate(resolve));
    }
    expect(calls.filter(x=>x!=='team-internal')).toEqual(["paper-settle","paper-settle"]);
    expect(calls.filter(x=>x==='team-internal')).toHaveLength(5);
    await scheduler.tick(new Date("2026-10-18T07:30:00Z"));
    expect(calls).toHaveLength(7);
  } finally {scheduler.stop();await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test("paper settlement uses review slot without displacing active close", async () => {
  const root=await mkdtemp(join(tmpdir(),"team-settle-slot-"));
  const logger=new LogStore(loadConfig({},root));const runner=new JobRunner(loadConfig({},root),logger);
  let seen="";
  const internals=runner as unknown as {active:unknown;runChild:(job:string,command:string,slot:string)=>Promise<unknown>};
  internals.active={runId:"natural-close"};
  internals.runChild=async (_job,_command,slot)=>{seen=slot;return {};};
  try {await runner.run("paper-settle" as JobName);expect(seen).toBe("review");}
  finally {await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});
