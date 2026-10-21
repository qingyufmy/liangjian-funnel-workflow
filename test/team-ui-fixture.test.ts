import {test,expect} from 'vitest';
import {projectFrozenEvidence} from '../scripts/team_fixture_projection.mjs';
import {validateEvent,reduceEvent,emptyState} from '../web/team/model.js';
function evidence(){
  const plans=Array.from({length:33},(_,i)=>({category:'PLAN',id:`PLAN:${i}`,value:{plan_id:`p${i}`,symbol:`symbol${i}`,status:'PENDING_MORNING_REVIEW',cluster:'test',source_row_sha256:'a'.repeat(64),publication_facts:{name:`测试${i}`,strategy_profile:'TREND',trigger_low:10,trigger_high:11,stop_level:9}}}));
  return {morning:{trade_date:'2026-10-20',observed_at:'2026-10-20T08:33:00+08:00',snapshot:{account:{nav:'1000000',cash:'1000000',pnl_unrealized:'0'},positions:[]}},models:{model_call_evidence:[{catalog_json:JSON.stringify({items:plans}),catalog_sha256:'b'.repeat(64)}],model_calls:[]},latest:{at:'2026-10-20T15:32:00+08:00',plans:{states:{ACTIVE_TODAY:32,INVALIDATED:1},metadata:plans.map((p,i)=>({plan_id:p.value.plan_id,status:i?'ACTIVE_TODAY':'INVALIDATED',updated_at:'2026-10-20T09:26:00+08:00'}))},team_sessions:[],record_latch:[{body:{mode:'record',observed_at:'2026-10-20T13:23:00+08:00',reason_code:'TEST_REASON'},body_sha256:'c'.repeat(64)}],condition_details:[],paper_counts:{paper_orders:0,paper_fills:0,paper_lots:0},team_notification_counts:[]}};
}
const sources=Object.fromEntries(['morning','models','latest'].map(k=>[k,{id:k,sha256:'d'.repeat(64)}]));
test('source date and original plan cardinality are hard prerequisites',()=>{
  const x=evidence();x.morning.trade_date='2026-10-21';expect(()=>projectFrozenEvidence(x,sources)).toThrow('DATE');
  const y=evidence();y.models.model_call_evidence[0].catalog_json='{"items":[]}';expect(()=>projectFrozenEvidence(y,sources)).toThrow('COUNT');
});
test('replay excludes unknown model speech, secrets and unavailable return numbers',()=>{
  const x=evidence();(x.morning.snapshot.account as any).api_key='DO_NOT_PROJECT';
  const fixture=projectFrozenEvidence(x,sources),raw=JSON.stringify(fixture);
  expect(raw).not.toContain('DO_NOT_PROJECT');expect(fixture.kind).toBe('PARTIAL_FROZEN_EVIDENCE');
  expect(fixture.events.some(e=>['say','relay','theme','market'].includes(e.type))).toBe(false);
  expect(fixture.events.find(e=>e.type==='account').day_pct).toBeUndefined();
});
test('33 original plans, later invalidation, account and record lock replay independently',()=>{
  const fixture=projectFrozenEvidence(evidence(),sources);
  let state=emptyState();
  for(const ev of fixture.events){expect(validateEvent(ev)).toBe(ev);expect(ev.provenance.id).toBeTruthy();expect(ev.provenance.sha256).toMatch(/^[a-f0-9]{64}$/);state=reduceEvent(state,ev);}
  expect(state.plans).toHaveLength(33);expect(state.plans.filter(p=>p.status!=='INVALIDATED')).toHaveLength(32);
  expect(state.account.nav).toBe(1000000);expect(state.account.day_pct).toBeUndefined();expect(state.positions).toEqual([]);expect(state.risk.latch_record).toBe(true);
  expect(state.plans[0].status_source_id).toBe('execution_plans:p0');expect(state.plans[0].status_source_at).toBe('2026-10-20T09:26:00+08:00');expect(state.plans[0].status_source_sha256).toBe(sources.latest.sha256);
  expect(fixture.events.map(e=>Date.parse(e.at))).toEqual(fixture.events.map(e=>Date.parse(e.at)).sort((a,b)=>a-b));
});
test('A6 validation failure cannot render a risk stop before an actual risk event',()=>{
  const x=evidence();x.models.model_calls.push({call_id:'A6:morning',role:'A6',captured_at:'2026-10-20T08:36:00+08:00',validated:0,latency_ms:60000,issues_json:'["A6_CAPACITY_INVALID"]'} as never);
  const events=projectFrozenEvidence(x,sources).events;
  expect(events.find(e=>e.type==='role'&&e.at.includes('08:36')).state).toBe('idle');
  expect(events.filter(e=>e.type==='role'&&e.state==='stop').every(e=>e.at.includes('13:23'))).toBe(true);
});
test('activation record outside published domain fails closed, not invented',()=>{
  const x=evidence();x.latest.plans.metadata[0].plan_id='outside';expect(()=>projectFrozenEvidence(x,sources)).toThrow('UNKNOWN_PLAN');
});
