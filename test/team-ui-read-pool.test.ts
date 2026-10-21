import {expect,test,vi} from 'vitest';
import {TeamUiBusyError,TeamUiReadPool,type TeamUiQuery} from '../server/team-ui-readonly.js';

const q=(offset=0):TeamUiQuery=>({kind:'plans',trade_date:'2026-10-21',limit:50,offset,days:20});
test('identical in-flight query coalesces; third distinct worker rejected, not queued',async()=>{
  const releases:Array<(value:unknown)=>void>=[];
  const worker=vi.fn(()=>new Promise(done=>releases.push(done)));
  const pool=new TeamUiReadPool(worker);
  const a=pool.read(q(0)),same=pool.read(q(0)),b=pool.read(q(1));
  expect(same).toBe(a);
  await expect(pool.read(q(2))).rejects.toBeInstanceOf(TeamUiBusyError);
  expect(worker).toHaveBeenCalledTimes(2);
  expect(pool.diagnostics().active).toBe(2);
  releases[0]!({rows:[]});releases[1]!({rows:[]});await Promise.all([a,b]);
  expect(pool.diagnostics()).toMatchObject({active:0,peak:2});
});
test('two-second cache expires and never crosses query dates or pages',async()=>{
  let clock=0;const worker=vi.fn(async()=>({source:'original'}));const pool=new TeamUiReadPool(worker,()=>clock);
  const original=await pool.read(q());clock=1999;expect(await pool.read(q())).toBe(original);
  expect(worker).toHaveBeenCalledTimes(1);
  clock=2000;await pool.read(q());await pool.read(q(1));await pool.read({...q(),trade_date:'2026-10-20'});
  expect(worker).toHaveBeenCalledTimes(4);expect(pool.diagnostics().cacheHits).toBe(1);
});
test('failed workers are not cached and always release the concurrency slot',async()=>{
  const worker=vi.fn().mockRejectedValueOnce(new Error('fixture failure')).mockResolvedValueOnce({rows:[]});
  const pool=new TeamUiReadPool(worker);
  await expect(pool.read(q())).rejects.toThrow('fixture failure');expect(pool.diagnostics().active).toBe(0);
  await pool.read(q());expect(worker).toHaveBeenCalledTimes(2);
});
test('cache is bounded at 64 entries even across many valid queries',async()=>{
  const pool=new TeamUiReadPool(async()=>({rows:[]}));
  for(let i=0;i<100;i++)await pool.read(q(i));
  expect(pool.diagnostics().cacheEntries).toBe(64);
});
