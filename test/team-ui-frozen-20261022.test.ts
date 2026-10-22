import {execFileSync} from 'node:child_process';
import {readFileSync,mkdtempSync,rmSync,writeFileSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename} from 'node:path';
import {expect,test} from 'vitest';
// @ts-expect-error Static browser JS.
import {applyDashboard} from '../web/team/dashboard.js';
// @ts-expect-error Static browser JS.
import {emptyState} from '../web/team/model.js';
// @ts-expect-error Static browser JS.
import {reasonText} from '../web/team/copy/reasons.js';
const capture=resolve('artifacts/frontend-20261022/frozen-ledger-rows.json');
const python=process.platform==='win32'?'python':'python3';
test.skipIf(!existsSync(capture))('all reason/required-condition codes in the approved 10-22 bounded capture have Chinese explanations',()=>{
 const original=JSON.parse(readFileSync(capture,'utf8').replace(/^\uFEFF/,'')),codes=new Set<string>();
 const visit=(x:unknown)=>{if(Array.isArray(x))for(const v of x)visit(v);else if(x&&typeof x==='object')for(const[k,v]of Object.entries(x)){if(['reason_codes','required_conditions'].includes(k)&&Array.isArray(v))for(const c of v)if(typeof c==='string'&&/^[A-Z0-9_:.]+$/.test(c))codes.add(c);visit(v);}};
 for(const table of ['state/workflow.sqlite3:monitor_events','state/workflow.sqlite3:execution_plans'])for(const row of original.tables[table])visit(JSON.parse(row.payload_json));
 expect(codes.size).toBeGreaterThan(30);for(const code of codes){expect(reasonText(code),code).not.toContain('尚无核实过');expect(reasonText(code),code).not.toContain(code);}
});
test.skipIf(!existsSync(capture))('real frozen API and browser state mapping preserve 40+ independently compared financial fields',()=>{
 const folder=mkdtempSync(join(tmpdir(),'team-ui-frozen-check-'));
 try{
  const output=join(folder,'report.json');execFileSync(python,['-B',resolve('scripts/team_ui_frozen.py'),capture,'--report',output]);
  const report=JSON.parse(readFileSync(output,'utf8')),original=JSON.parse(readFileSync(capture,'utf8').replace(/^\uFEFF/,'')),state=applyDashboard(emptyState(),report.projection);let fields=0;
  const equal=(actual:unknown,expected:unknown)=>{expect(actual).toEqual(expected);fields++;};
  const meta=Object.fromEntries(original.tables['state/team_v2/paper.db:paper_meta'].map((r:{key:string,value:string})=>[r.key,r.value]));
  equal(state.account.cash,Number(meta.cash));equal(state.account.realized,Number(meta.realized));equal(state.account.nav,Number(meta.cash));equal(state.positions,[]);
  for(const row of original.tables['state/workflow.sqlite3:execution_plans']){
   const plan=state.plans.find((p:{id:string})=>p.id===row.plan_id),payload=JSON.parse(row.payload_json);
   equal(plan.code,row.symbol);equal(plan.name,payload.company_name||payload.name);equal(plan.stored_status,row.status);equal(plan.status,'EXPIRED');equal(plan.zone_values[0],Number(payload.trigger_low));equal(plan.zone_values[1],Number(payload.trigger_high));equal(plan.stop,Number(payload.stop_level));equal(plan.target,Number(payload.first_resistance));equal(plan.risk_unit,payload.risk_unit);equal(plan.expires_at,row.expires_at);equal(plan.planned_rr,Number(payload.reward_risk));
  }
  for(const row of original.tables['state/team_v2/paper.db:paper_fills'].filter((r:{minute_end:string})=>r.minute_end.startsWith(original.day))){
   const fill=state.fills.find((f:{fill_id:string})=>f.fill_id===row.fill_id);for(const key of ['symbol','side','qty','price','fee','minute_end'])equal(fill[key],row[key]);
  }
  expect(fields).toBeGreaterThanOrEqual(40);expect(report.compared_fields).toBeGreaterThanOrEqual(40);expect(report.differences).toBe(0);
  expect(state.dashboard.data_mode).toBe('FROZEN_CAPTURE_NOT_REALTIME');expect(state.dashboard.metrics.model_calls).toBe(1);expect(state.dashboard.health.record_latch).toBe(false);expect(state.dashboard.health.mode).toBe('CONTROL');expect(state.dashboard.admissions).toHaveLength(2);expect(state.dashboard.risk.max_open).toBeNull();
  // A modified source row must fail hash binding before it is materialized.
  original.tables['state/team_v2/paper.db:paper_meta'].find((r:{key:string})=>r.key==='cash').value='123';
  const tampered=join(folder,'tampered.json');writeFileSync(tampered,JSON.stringify(original));expect(()=>execFileSync(python,['-B',resolve('scripts/team_ui_frozen.py'),tampered],{stdio:'pipe'})).toThrow();
 }finally{if(dirname(resolve(folder))!==resolve(tmpdir())||!basename(folder).startsWith('team-ui-frozen-check-'))throw Error('UNSAFE_TEST_CLEANUP');rmSync(folder,{recursive:true,force:true,maxRetries:10,retryDelay:50});}
});
