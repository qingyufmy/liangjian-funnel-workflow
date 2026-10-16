import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {expect,test,vi} from 'vitest';
import {JOB_DEFINITIONS,loadConfig} from '../../server/config.js';
import {LogStore} from '../../server/logger.js';
import {JobRunner,timeoutForJob} from '../../server/runner.js';
import {WorkflowScheduler} from '../../server/scheduler.js';
import type {JobName} from '../../server/types.js';

test('internal command has its own notification slot and eight second parent',async()=>{
  expect(JOB_DEFINITIONS.find(x=>x.name==='team-internal')?.command).toBe('run-team-internal');
  expect(timeoutForJob('team-internal',5400000)).toBe(8000);
  expect(timeoutForJob('monitor',5400000)).toBe(58000);
  const root=await mkdtemp(join(tmpdir(),'team-internal-slot-'));
  const logger=new LogStore(loadConfig({},root));const runner=new JobRunner(loadConfig({},root),logger);
  const state=runner as unknown as {active:unknown;review:unknown;notification:unknown;
    runChild:(job:string,command:string,slot:string)=>Promise<unknown>};
  state.active={runId:'monitor'};state.review={runId:'risk-review'};
  let slot='';state.runChild=async(_j,_c,s)=>{slot=s;return {};};
  try{
    await runner.run('team-internal');expect(slot).toBe('notification');
    state.notification={runId:'internal-running'};
    expect((await runner.run('team-internal')).status).toBe('skipped');
    expect(state.active).toEqual({runId:'monitor'});expect(state.review).toEqual({runId:'risk-review'});
    state.active=null;await runner.run('monitor');expect(slot).toBe('primary');
  }finally{await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test('shutdown includes the independent notification child',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-internal-stop-'));const logger=new LogStore(loadConfig({},root));
  const runner=new JobRunner(loadConfig({},root),logger);const kill=vi.fn();
  (runner as unknown as {notification:unknown}).notification={runId:'n1',process:{kill,exitCode:0,signalCode:null}};
  try{await runner.stop();expect(kill).toHaveBeenCalledWith('SIGTERM');}
  finally{await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test('Shanghai weekday 08:45 first edge, once per minute, no weekend or late send',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-internal-clock-'));const logger=new LogStore(loadConfig({},root));
  const calls:JobName[]=[];const runner={run:async(job:JobName)=>{calls.push(job);return {status:'succeeded',job};}};
  const schedule=new WorkflowScheduler(runner as unknown as JobRunner,logger,{comparisonEnabled:false});
  const tick=async(iso:string)=>{await schedule.tick(new Date(iso));await new Promise<void>(r=>setImmediate(r));};
  try{
    await tick('2026-10-19T00:44:59Z');expect(calls).not.toContain('team-internal');
    await tick('2026-10-19T00:45:00Z');await tick('2026-10-19T00:45:40Z');
    expect(calls.filter(x=>x==='team-internal')).toHaveLength(1);
    await tick('2026-10-19T00:46:00Z');expect(calls.filter(x=>x==='team-internal')).toHaveLength(2);
    await tick('2026-10-19T09:20:00Z');expect(calls.filter(x=>x==='team-internal')).toHaveLength(3);
    await tick('2026-10-19T09:21:00Z');await tick('2026-10-18T00:45:00Z');
    expect(calls.filter(x=>x==='team-internal')).toHaveLength(3);
  }finally{schedule.stop();await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});
