import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {expect,test} from 'vitest';
import {loadConfig,JOB_DEFINITIONS} from '../../server/config.js';
import {LogStore} from '../../server/logger.js';
import {JobRunner,timeoutForJob} from '../../server/runner.js';
import {WorkflowScheduler} from '../../server/scheduler.js';
import type {JobName} from '../../server/types.js';

test('close-bar real command has sixty seconds in independent review slot',async()=>{
  expect(JOB_DEFINITIONS.find(x=>x.name==='team-close-bar')?.command).toBe('run-team-close-bar');
  expect(timeoutForJob('team-close-bar',5400000)).toBe(60000);
  expect(timeoutForJob('monitor',5400000)).toBe(58000);
  const root=await mkdtemp(join(tmpdir(),'team-close-bar-slot-'));const logger=new LogStore(loadConfig({},root));
  const runner=new JobRunner(loadConfig({},root),logger);
  const internals=runner as unknown as {active:unknown;runChild:(j:string,c:string,s:string)=>Promise<unknown>};
  internals.active={runId:'natural-close'};let slot='';internals.runChild=async(_j,_c,s)=>{slot=s;return {};};
  try{await runner.run('team-close-bar');expect(slot).toBe('review');}
  finally{await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test('15:01 close wakeup recovers BUSY only inside window and never wakes monitor/models',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-close-bar-clock-'));const logger=new LogStore(loadConfig({},root));
  const calls:JobName[]=[];let n=0;
  const runner={run:async(job:JobName)=>{calls.push(job);const busy=job==='team-close-bar'&&++n===1;
    return {job,status:busy?'skipped':'succeeded',reason:busy?'BUSY:review':null};}};
  const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{comparisonEnabled:false});
  try{
    for(const iso of ['2026-10-19T07:00:59Z','2026-10-19T07:01:00Z','2026-10-19T07:01:01Z',
      '2026-10-19T07:01:06Z','2026-10-19T07:03:00Z','2026-10-19T07:05:00Z']){
      await scheduler.tick(new Date(iso));await new Promise<void>(resolve=>setImmediate(resolve));
    }
    // Existing last 15:00 monitor stays intact; no monitor is added after it.
    expect(calls.filter(x=>x!=='team-internal')).toEqual(['monitor','team-close-bar','team-close-bar']);
    await scheduler.tick(new Date('2026-10-18T07:01:00Z'));
    expect(calls.filter(x=>x==='team-close-bar')).toHaveLength(2);
  }finally{scheduler.stop();await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});
