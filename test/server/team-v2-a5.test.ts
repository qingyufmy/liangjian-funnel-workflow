import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {expect,test,vi} from 'vitest';
import {loadConfig,JOB_DEFINITIONS} from '../../server/config.js';
import {LogStore} from '../../server/logger.js';
import {JobRunner,timeoutForJob} from '../../server/runner.js';
import {WorkflowScheduler} from '../../server/scheduler.js';
import type {JobName} from '../../server/types.js';

test('Team A5 separate command has 240s review budget and never changes customer A5 budget',async()=>{
  expect(JOB_DEFINITIONS.find(x=>x.name==='team-a5-close')?.command).toBe('run-team-a5-close');
  expect(timeoutForJob('team-a5-close',5400000)).toBe(240000);
  expect(timeoutForJob('a5-close',5400000)).toBe(900000);
  const root=await mkdtemp(join(tmpdir(),'team-coach-slot-'));const logger=new LogStore(loadConfig({},root));
  const runner=new JobRunner(loadConfig({},root),logger);
  const internals=runner as unknown as {active:unknown;review:unknown;runChild:(j:string,c:string,s:string)=>Promise<unknown>};
  internals.active={runId:'natural-close'};let slot='';internals.runChild=async(_j,_c,s)=>{slot=s;return {};};
  try{vi.useFakeTimers();vi.setSystemTime(new Date('2026-10-19T09:30:00Z'));
    await runner.run('team-a5-close');expect(slot).toBe('review');
    internals.review={runId:'customer-review'};expect((await runner.run('team-a5-close')).status).toBe('skipped');
  }finally{vi.useRealTimers();await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
});

test('only pre-model wait75 retries once/minute; model failure never retried or routed to v1',async()=>{
  for(const exit of [75,2]){
    const root=await mkdtemp(join(tmpdir(),'team-coach-wait-'));const logger=new LogStore(loadConfig({},root));
    let count=0;const calls:JobName[]=[];
    const runner={run:async(job:JobName)=>{calls.push(job);if(job==='team-a5-midday')count++;
      return {job,status:job==='team-a5-midday'&&count<3?'failed':'succeeded',
        exitCode:job==='team-a5-midday'&&count<3?exit:0};}};
    const scheduler=new WorkflowScheduler(runner as unknown as JobRunner,logger,{comparisonEnabled:false});
    try{
      for(const minute of ['03:35:00','03:35:30','03:36:00','03:37:00','03:38:00']){
        await scheduler.tick(new Date('2026-10-19T'+minute+'Z'));await new Promise<void>(r=>setImmediate(r));
      }
      expect(count).toBe(exit===75?3:1);
      expect(calls.filter(x=>x==='a5-midday')).toHaveLength(1);
    }finally{scheduler.stop();await(logger as unknown as {writeChain:Promise<void>}).writeChain;await rm(root,{recursive:true,force:true});}
  }
});
