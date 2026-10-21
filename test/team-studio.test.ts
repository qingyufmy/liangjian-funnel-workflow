import {test,expect} from 'vitest';
// @ts-expect-error Framework-free browser module.
import {studioFacts} from '../web/team/studio.js';
// @ts-expect-error Framework-free chart geometry.
import {chartModel} from '../web/team/plan-detail.js';
// @ts-expect-error Framework-free reducer.
import {emptyState} from '../web/team/model.js';
test('studio conclusion comes from supplied plan and latch facts, never fabricated health or zero ledger',()=>{
  expect(studioFacts(emptyState()).active).toBeNull();
  const s={...emptyState(),plans:[{status:'ACTIVE_TODAY'},{status:'INVALIDATED'}]};
  expect(studioFacts(s)).toMatchObject({active:1,invalid:1});expect(studioFacts(s).subtitle).toContain('账本待读取');
  expect(studioFacts({...s,risk:{latch_record:true}}).title).toContain('只记录保护');
  expect(studioFacts({...s,tasks:[{id:'scheduler-only',status:'running'}]}).tasks).toBeNull();
  expect(studioFacts({...s,tasks:[{id:'call',provenance:{table:'model_calls',id:'real-call',sha256:'a'.repeat(64)}}]}).tasks).toBe(1);
});
test('chart only draws supplied closed bars and never joins missing time intervals',()=>{
  expect(chartModel({bars:[]})).toBeNull();
  const source={zone_values:[10,11],stop:9,bars:[{at:'2026-10-21T10:00:00+08:00',close:10,closed:true},{at:'2026-10-21T10:05:00+08:00',close:11,closed:true},{at:'2026-10-21T10:15:00+08:00',close:10.5,closed:true},{at:'2026-10-21T10:20:00+08:00',close:20,closed:false}]};
  const before=JSON.stringify(source),m=chartModel(source);expect(m.coverage).toMatchObject({count:3,gapped:true});expect(m.segments).toHaveLength(2);expect(m.zone.h).toBeGreaterThan(0);expect(m.points.every((p:{y:number})=>p.y>=8&&p.y<=118)).toBe(true);expect(JSON.stringify(source)).toBe(before);
});
