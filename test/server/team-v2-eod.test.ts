import { mkdtemp, rm,writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, test, vi } from 'vitest';
import { JOB_DEFINITIONS,loadConfig } from '../../server/config.js';
import { LogStore } from '../../server/logger.js';
import { JobRunner,timeoutForJob,writeTeamEodHandoff } from '../../server/runner.js';
import { WorkflowScheduler } from '../../server/scheduler.js';
import type { JobName,JobRunRecord } from '../../server/types.js';

const settle=async()=>{await new Promise<void>(resolve=>setImmediate(resolve));};

test('0327 direct runner Team requests cannot occupy the customer review window',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-priority-runner-'));const config=loadConfig({},root);
  const logger=new LogStore(config);const runner=new JobRunner(config,logger);const spawned:string[]=[];
  const internals=runner as unknown as {runChild:(job:string,command:string,slot:string)=>Promise<unknown>};
  internals.runChild=async(job)=>{spawned.push(job);return {job,status:'succeeded'};};
  try{
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-10-19T08:00:00Z'));
    for(const job of ['team-eod','team-a5-close'] as const)
      expect((await runner.run(job)).reason).toBe('CUSTOMER_A5_PRIORITY_WINDOW');
    expect((await runner.run('a5-close')).status).toBe('succeeded');
    expect(spawned).toEqual(['a5-close']);
  }finally{vi.useRealTimers();await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test('0327 customer A5 owns its whole retry/runtime window before Team review tasks',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-customer-priority-'));
  const logger=new LogStore(loadConfig({},root));const calls:JobName[]=[];const busy:JobName[]=[];
  let now=new Date('2026-10-19T07:10:00Z');let owner:JobName|null=null;
  let finishCustomer!:()=>void;const customer=new Promise<void>(resolve=>{finishCustomer=resolve;});
  const runner={run:async(job:JobName)=>{
    calls.push(job);
    if(['team-eod','team-a5-close','a5-close'].includes(job)){
      if(owner){busy.push(job);return {job,status:'skipped',reason:'BUSY:'+owner};}
      owner=job;if(job==='a5-close')await customer;owner=null;
    }
    return {job,status:'succeeded',exitCode:0,reason:null};
  }};
  const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{now:()=>now,comparisonEnabled:false});
  try{
    await scheduler.tick(now);await settle(); // successful natural close would make EOD due
    now=new Date('2026-10-19T08:00:00Z');await scheduler.tick(now);await settle();
    expect(owner).toBe('a5-close');expect(calls).not.toContain('team-eod');expect(calls).not.toContain('team-a5-close');
    now=new Date('2026-10-19T09:15:00Z');await scheduler.tick(now);await settle();
    expect(calls).not.toContain('team-eod');expect(calls).not.toContain('team-a5-close');
    finishCustomer();await settle();
    now=new Date('2026-10-19T09:20:00Z');await scheduler.tick(now);await settle();
    now=new Date('2026-10-19T09:25:00Z');await scheduler.tick(now);await settle();
    expect(calls.filter(x=>x==='a5-close')).toHaveLength(1);
    expect(calls.filter(x=>x==='team-eod')).toHaveLength(1);
    expect(calls.filter(x=>x==='team-a5-close')).toHaveLength(1);
    expect(busy).not.toContain('a5-close');
  }finally{finishCustomer();scheduler.stop();await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test('close dependency has actual isolated command and parent budget, monitor unchanged',()=>{
  expect(JOB_DEFINITIONS.find(d=>d.name==='team-eod')?.command).toBe('run-team-eod');
  expect(timeoutForJob('team-eod',5400000)).toBe(120000);
  expect(timeoutForJob('monitor',5400000)).toBe(58000);
});

test('successful close triggers owned EOD only after completion; failed close cannot fabricate it',async()=>{
  for(const successful of [true,false]){
    const root=await mkdtemp(join(tmpdir(),'team-eod-after-'));let now=new Date('2026-10-19T07:10:00Z');
    const logger=new LogStore(loadConfig({},root));const calls:string[]=[];
    let finish!:()=>void;const wait=new Promise<void>(resolve=>{finish=resolve;});
    const runner={run:async(job:JobName)=>{calls.push(job);if(job==='close')await wait;
      return {status:job==='close'&&!successful?'failed':'succeeded',job,reason:null};}};
    const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{now:()=>now,comparisonEnabled:false});
    try{
      await scheduler.tick(now);await settle();
      now=new Date('2026-10-19T07:20:00Z');await scheduler.tick(now);await settle();
      expect(calls).not.toContain('team-eod');finish();await settle();
      now=new Date('2026-10-19T07:20:01Z');await scheduler.tick(now);await settle();
      expect(calls).not.toContain('team-eod');
      now=new Date('2026-10-19T09:20:01Z');await scheduler.tick(now);await settle();
      expect(calls.filter(x=>x==='team-eod')).toHaveLength(successful?1:0);
      now=new Date('2026-10-19T09:21:01Z');await scheduler.tick(now);await settle();
      expect(calls.filter(x=>x==='close')).toHaveLength(1);
      expect(calls.filter(x=>x==='team-eod')).toHaveLength(successful?1:0);
    }finally{finish();scheduler.stop();await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
  }
});

test('EOD uses review slot even while primary is occupied',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-eod-slot-'));const config=loadConfig({},root);
  const logger=new LogStore(config);const runner=new JobRunner(config,logger);let seen='';
  const internals=runner as unknown as {active:unknown;runChild:(job:string,command:string,slot:string)=>Promise<unknown>};
  internals.active={runId:'another-primary'};
  internals.runChild=async(_job,_command,slot)=>{seen=slot;return {};};
  try{vi.useFakeTimers();vi.setSystemTime(new Date('2026-10-19T09:30:00Z'));
    await runner.run('team-eod');expect(seen).toBe('review');}
  finally{vi.useRealTimers();await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test('only BUSY can retry EOD, with five-second spacing and no repeated research',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-eod-busy-'));let now=new Date('2026-10-19T07:10:00Z');
  const logger=new LogStore(loadConfig({},root));const calls:string[]=[];let count=0;
  const runner={run:async(job:JobName)=>{calls.push(job);const busy=job==='team-eod'&&count++===0;
    return {status:busy?'skipped':'succeeded',job,reason:busy?'BUSY:review':null};}};
  const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{now:()=>now,comparisonEnabled:false});
  try{
    await scheduler.tick(now);await settle();
    for(const iso of ['2026-10-19T09:20:00Z','2026-10-19T09:20:01Z','2026-10-19T09:20:05Z','2026-10-19T09:20:10Z']){
      now=new Date(iso);await scheduler.tick(now);await settle();
    }
    expect(calls.filter(x=>x==='team-eod')).toHaveLength(2);
    expect(calls.filter(x=>x==='close')).toHaveLength(1);
  }finally{scheduler.stop();await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test('Node restart replays only same-day completed close handoff, never research',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-eod-restart-'));const config=loadConfig({},root);
  const logger=new LogStore(config);const reader=new JobRunner(config,logger);const calls:string[]=[];
  const now=new Date('2026-10-19T08:35:00Z');
  const source:JobRunRecord={runId:'original-close',job:'close',command:'run-close',startedAt:'2026-10-19T07:10:00Z',
    finishedAt:'2026-10-19T08:30:00Z',durationMs:4800000,exitCode:0,signal:null,status:'succeeded',reason:null};
  try{
    writeTeamEodHandoff(config,{...source,status:'failed',exitCode:2});
    expect(reader.completedCloseForTeamEod('2026-10-19')).toBe(false);
    writeTeamEodHandoff(config,source);writeTeamEodHandoff(config,{...source,runId:'later'});
    expect(reader.completedCloseForTeamEod('2026-10-19')).toBe(true);
    expect(reader.completedCloseForTeamEod('2026-10-20')).toBe(false);
    expect(reader.completedCloseForTeamEod('../bad')).toBe(false);
    const fake={completedCloseForTeamEod:(day:string)=>reader.completedCloseForTeamEod(day),
      run:async(job:JobName)=>{calls.push(job);return {...source,job};}};
    const scheduler=new WorkflowScheduler(fake as unknown as JobRunner,logger,{now:()=>now,comparisonEnabled:false});
    await scheduler.tick(now);await settle();await scheduler.tick(now);await settle();
    expect(calls).toEqual(['team-internal']);
    await scheduler.tick(new Date('2026-10-19T09:20:00Z'));await settle();
    expect(calls.filter(x=>x==='team-eod')).toHaveLength(1);
    expect(calls).not.toContain('team-a5-close');
    await scheduler.tick(new Date('2026-10-19T09:25:00Z'));await settle();
    expect(calls.filter(x=>x==='team-a5-close')).toHaveLength(1);scheduler.stop();
    await writeFile(join(root,'state','team_v2','eod-handoff','2026-10-19.json'),' '.repeat(8193));
    expect(reader.completedCloseForTeamEod('2026-10-19')).toBe(false);
  }finally{await (logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});
