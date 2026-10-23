import { EventEmitter } from 'node:events';
import * as childProcess from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, test, vi } from 'vitest';
import { loadConfig } from '../../server/config.js';
import { LogStore } from '../../server/logger.js';
import { commandArgumentsForJob, JobRunner, timeoutForJob } from '../../server/runner.js';

vi.mock('node:child_process', async original => ({ ...await original<typeof import('node:child_process')>(), spawn: vi.fn() }));
afterEach(() => { vi.restoreAllMocks(); vi.clearAllMocks(); vi.useRealTimers(); });

test.each([[5_400_000,600_000,540],[600_000,600_000,540],[540_000,540_000,480],
  [120_000,120_000,60],[61_999,61_999,1],[61_000,61_000,1]])(
  'configured cap %i binds parent %i and child %i seconds', (configured,parent,child) => {
    const actual = timeoutForJob('runtime-integrity', configured);
    expect(actual).toBe(parent);
    expect(commandArgumentsForJob('runtime-integrity','run-runtime-integrity',actual))
      .toEqual(['-m','liangjian_funnel','run-runtime-integrity','--timeout-seconds',String(child)]);
    expect(child*1000).toBeLessThan(parent);
  });

test.each([60_999,60_000,1000,0,null,NaN])('too-small/invalid parent %s cannot start integrity', parent => {
  expect(() => commandArgumentsForJob('runtime-integrity','run-runtime-integrity',parent))
    .toThrow('RUNTIME_INTEGRITY_PARENT_BUDGET_TOO_SMALL');
});

test('provider-health, monitor and model jobs retain original caps and arguments', () => {
  expect(timeoutForJob('provider-health',5_400_000)).toBe(300_000);
  expect(timeoutForJob('monitor',5_400_000)).toBe(58_000);
  expect(timeoutForJob('a5-close',5_400_000)).toBe(900_000);
  for (const job of ['provider-health','monitor','a5-close'] as const)
    expect(commandArgumentsForJob(job,job,1000)).toEqual(['-m','liangjian_funnel',job]);
});

test.each([600_000,120_000,60_000])('actual runner spawn respects configured %i', async cap => {
  const root=await mkdtemp(join(tmpdir(),'integrity-budget-'));
  const config=loadConfig({LIANGJIAN_JOB_TIMEOUT_MS:String(cap)},root),logger=new LogStore(config);
  const child=Object.assign(new EventEmitter(),{stdout:new EventEmitter(),stderr:new EventEmitter(),kill:vi.fn()});
  vi.mocked(childProcess.spawn).mockReturnValue(child as unknown as childProcess.ChildProcessWithoutNullStreams);
  const runner=new JobRunner(config,logger);
  try {
    const pending=runner.run('runtime-integrity');
    if (cap < 61_000) {
      const result=await pending;
      expect(childProcess.spawn).not.toHaveBeenCalled();
      expect(result).toMatchObject({status:'failed',reason:'RUNTIME_INTEGRITY_PARENT_BUDGET_TOO_SMALL'});
      expect(runner.activeJob()).toBeNull();
      expect(runner.recentRuns(1)[0]).toEqual(result);
    } else {
      expect(childProcess.spawn).toHaveBeenCalledWith(config.pythonBin,
        ['-m','liangjian_funnel','run-runtime-integrity','--timeout-seconds',cap===600_000?'540':'60'],
        expect.objectContaining({env:expect.objectContaining({LIANGJIAN_PARENT_JOB_BUDGET_MS:String(cap)})}));
      child.emit('close',0,null);
      expect((await pending).status).toBe('succeeded');
    }
  } finally {
    await (logger as unknown as {writeChain:Promise<void>}).writeChain;
    await rm(root,{recursive:true,force:true});
  }
});

test('late zero exit after parent timeout cannot report success', async () => {
  const root=await mkdtemp(join(tmpdir(),'integrity-timeout-'));
  const config=loadConfig({},root),logger=new LogStore(config);
  const child=Object.assign(new EventEmitter(),{stdout:new EventEmitter(),stderr:new EventEmitter(),kill:vi.fn()});
  vi.mocked(childProcess.spawn).mockReturnValue(child as unknown as childProcess.ChildProcessWithoutNullStreams);
  const runner=new JobRunner(config,logger);
  vi.useFakeTimers();
  try {
    const pending=runner.run('runtime-integrity');
    await vi.advanceTimersByTimeAsync(600_000);
    expect(child.kill).toHaveBeenCalledWith('SIGTERM');
    child.emit('close',0,null);
    expect(await pending).toMatchObject({status:'terminated',reason:'TIMEOUT',exitCode:0});
  } finally {
    vi.useRealTimers();
    await (logger as unknown as {writeChain:Promise<void>}).writeChain;
    await rm(root,{recursive:true,force:true});
  }
});
