import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, test } from 'vitest';
import { loadConfig } from '../../server/config.js';
import { LogStore } from '../../server/logger.js';
import { JobRunner, timeoutForJob } from '../../server/runner.js';
import { WorkflowScheduler } from '../../server/scheduler.js';
import type { JobName, JobRunRecord } from '../../server/types.js';

const result = (job: JobName, status: JobRunRecord['status'] = 'succeeded', reason: string | null = null): JobRunRecord => ({
  runId:job,job,command:job,startedAt:'2026-10-26T00:00:00Z',finishedAt:'2026-10-26T00:00:01Z',
  exitCode:status==='succeeded'?0:2,signal:null,durationMs:1,status,reason,
});

async function setup(run:(job:JobName)=>Promise<JobRunRecord>) {
  const root=await mkdtemp(join(tmpdir(),'integrity-schedule-'));
  const logger=new LogStore(loadConfig({},root));
  const scheduler=new WorkflowScheduler({run} as unknown as JobRunner,logger,{comparisonEnabled:false});
  return {scheduler,cleanup:async()=>{
    scheduler.stop();await (logger as unknown as {writeChain:Promise<void>}).writeChain;
    await rm(root,{recursive:true,force:true});
  }};
}

test('06:50 maintenance runs once before 07:00 preparation and never calls provider probe',async()=>{
  const calls:JobName[]=[];const s=await setup(async job=>{calls.push(job);return result(job);});
  try {
    await s.scheduler.tick(new Date('2026-10-25T22:49:59Z'));expect(calls).toEqual([]);
    await s.scheduler.tick(new Date('2026-10-25T22:50:00Z'));
    await s.scheduler.tick(new Date('2026-10-25T22:51:00Z'));
    await s.scheduler.tick(new Date('2026-10-25T23:00:00Z'));
    expect(calls).toEqual(['runtime-integrity','auction-base']);
    expect(timeoutForJob('runtime-integrity',5_400_000)).toBe(210_000);
  } finally {await s.cleanup();}
});

test('late Node start completes integrity before dispatching due work',async()=>{
  const calls:JobName[]=[];let finish!:(value:JobRunRecord)=>void;
  const s=await setup(async job=>{calls.push(job);return job==='runtime-integrity'?new Promise(resolve=>{finish=resolve;}):result(job);});
  try {
    const first=s.scheduler.tick(new Date('2026-10-25T23:03:00Z'));
    const overlapping=s.scheduler.tick(new Date('2026-10-25T23:03:01Z'));
    expect(calls).toEqual(['runtime-integrity']);
    finish(result('runtime-integrity'));await Promise.all([first,overlapping]);
    expect(calls).toEqual(['runtime-integrity','auction-base']);
  } finally {await s.cleanup();}
});

test('failed integrity never turns into success or a same-day scan retry',async()=>{
  const calls:JobName[]=[];const s=await setup(async job=>{calls.push(job);return result(job,'failed','INTEGRITY_FAILED');});
  try {
    await s.scheduler.tick(new Date('2026-10-25T22:50:00Z'));
    await s.scheduler.tick(new Date('2026-10-25T23:03:00Z'));
    await s.scheduler.tick(new Date('2026-10-26T01:31:03Z'));
    expect(calls).toEqual(['runtime-integrity']);
  } finally {await s.cleanup();}
});

test('worker busy can defer the attempt, without treating the skip as verified',async()=>{
  const calls:JobName[]=[];let count=0;
  const s=await setup(async job=>{calls.push(job);return job==='runtime-integrity'&&count++===0?result(job,'skipped','BUSY:a1'):result(job);});
  try {
    await s.scheduler.tick(new Date('2026-10-25T23:03:00Z'));expect(calls).toEqual(['runtime-integrity']);
    await s.scheduler.tick(new Date('2026-10-25T23:03:01Z'));
    expect(calls).toEqual(['runtime-integrity','runtime-integrity','auction-base']);
  } finally {await s.cleanup();}
});

test('Sunday remains NOOP, Saturday does not acquire a new integrity schedule',async()=>{
  const calls:JobName[]=[];const s=await setup(async job=>{calls.push(job);return result(job);});
  try {
    await s.scheduler.tick(new Date('2026-10-24T23:03:00Z'));
    await s.scheduler.tick(new Date('2026-10-23T23:03:00Z'));
    expect(calls).toEqual([]);
  } finally {await s.cleanup();}
});
