import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { JOB_DEFINITIONS, loadConfig } from "../../server/config.js";
import { LogStore } from "../../server/logger.js";
import { JobRunner, timeoutForJob } from "../../server/runner.js";
import { WorkflowScheduler } from "../../server/scheduler.js";
import type { JobName } from "../../server/types.js";

const jobs:JobName[]=["team-morning-engine","team-morning-risk","team-morning-pm"];
test("three separate real CLI commands and 120s budgets preserve monitor",()=>{
  for (const name of jobs){
    expect(JOB_DEFINITIONS.find(j=>j.name===name)?.command).toBe(`run-${name}`);
    expect(timeoutForJob(name,5400000)).toBe(120000);
  }
  expect(timeoutForJob("monitor",5400000)).toBe(58000);
});
test("A6 completion dispatches PM immediately; no duplicate 08:38 clock or weekend catchup",async()=>{
  const root=await mkdtemp(join(tmpdir(),"team-morning-"));const config=loadConfig({},root);
  const logger=new LogStore(config);const calls:JobName[]=[];
  const runner={run:async(job:JobName)=>{calls.push(job);return {job,status:"succeeded"};}};
  let current=new Date('2026-10-19T00:00:00Z');
  const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{comparisonEnabled:false,now:()=>current});
  try {
    for(const day of ["2026-10-18","2026-10-19"]){
      for(const instant of ["00:32:59","00:33:00","00:33:02","00:34:00","00:35:00","00:35:59","00:38:00","00:38:01","00:40:00"]){
        current=new Date(`${day}T${instant}Z`);
        await scheduler.tick(current);await new Promise<void>(resolve=>setImmediate(resolve));
      }
    }
    expect(calls).toEqual(['runtime-integrity',...jobs]);
  } finally {scheduler.stop();await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test("PM follows actual A6 completion and never starts at or after 08:45",async()=>{
  for(const completedAt of ['00:36:12','00:45:00']){
    const root=await mkdtemp(join(tmpdir(),'pm-completion-'));const logger=new LogStore(loadConfig({},root));
    let current=new Date('2026-10-27T00:35:00Z');const calls:JobName[]=[];
    let finishRisk!:(value:unknown)=>void;
    const runner={run:(job:JobName)=>{calls.push(job);return job==='team-morning-risk'
      ?new Promise(resolve=>{finishRisk=resolve;}):Promise.resolve({job,status:'succeeded'});}};
    const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{comparisonEnabled:false,now:()=>current});
    try{
      await scheduler.tick(current);await new Promise<void>(resolve=>setImmediate(resolve));
      expect(calls).not.toContain('team-morning-pm');
      current=new Date(`2026-10-27T${completedAt}Z`);finishRisk({job:'team-morning-risk',status:'succeeded'});
      await new Promise<void>(resolve=>setImmediate(resolve));
      expect(calls.filter(j=>j==='team-morning-pm')).toHaveLength(completedAt==='00:36:12'?1:0);
      current=new Date('2026-10-27T00:38:00Z');await scheduler.tick(current);
      expect(calls.filter(j=>j==='team-morning-pm')).toHaveLength(completedAt==='00:36:12'?1:0);
    }finally{scheduler.stop();await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
  }
});
test("each edge uses review slot without displacing the primary process",async()=>{
  const root=await mkdtemp(join(tmpdir(),"team-morning-slot-"));const config=loadConfig({},root);
  const logger=new LogStore(config);const runner=new JobRunner(config,logger);const seen:string[]=[];
  const internals=runner as unknown as {active:unknown;runChild:(job:string,command:string,slot:string)=>Promise<unknown>};
  internals.active={runId:"natural-base"};internals.runChild=async(_job,_command,slot)=>{seen.push(slot);return {};};
  try {for(const job of jobs)await runner.run(job);expect(seen).toEqual(["review","review","review"]);}
  finally {await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});
