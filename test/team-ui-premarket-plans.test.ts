import {execFileSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,readFileSync,readdirSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename} from 'node:path';
import {createHash} from 'node:crypto';
import {beforeAll,afterAll,expect,test} from 'vitest';
const python=process.platform==='win32'?'python':'python3';let root:string,before:string;
const digest=()=>createHash('sha256').update(readFileSync(join(root,'state/workflow.sqlite3'))).digest('hex');
beforeAll(()=>{
 root=mkdtempSync(join(tmpdir(),'team-ui-premarket-'));mkdirSync(join(root,'state'));
 execFileSync(python,['-B','-c',`import sqlite3,json,sys
from pathlib import Path
c=sqlite3.connect(Path(sys.argv[1])/'state/workflow.sqlite3')
c.execute('CREATE TABLE execution_plans(plan_id,symbol,status,valid_from,expires_at,updated_at,payload_json)')
def put(id,valid,expiry,payload,status='PENDING_MORNING_REVIEW'):
 c.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?)',(id,'688018.SH',status,valid,expiry,'2026-10-21T16:17:14+08:00',json.dumps(payload)))
base={'company_name':'合成半导体','trigger_low':'12.3292','trigger_high':'12.4525','stop_level':'12.2','first_resistance':'14.1','minimum_reward_risk':'2.5','risk_unit':'PROBE'}
put('today-pending',None,'2026-10-22T15:00:00+08:00',base)
put('yesterday-pending',None,'2026-10-21T15:00:00+08:00',base)
put('tomorrow-pending',None,'2026-10-23T15:00:00+08:00',base)
put('old-invalid',None,'2026-10-21T15:00:00+08:00',base,'INVALIDATED')
put('today-active','2026-10-22T09:26:00+08:00','2026-10-22T15:00:00+08:00',dict(base,DATA_LIMITED=True,source_data_limits={'reason_codes':['PDF_SOURCE_UNAVAILABLE','https://SECRET.invalid','/PRIVATE_PATH'],'a6_explicit_review_required':True,'source_as_of':'2026-10-21T18:21:57+08:00','webhook':'SECRET'}),'ACTIVE_TODAY')
put('today-invalid','2026-10-22T09:26:00+08:00','2026-10-22T15:00:00+08:00',dict(base,trigger_low=True,trigger_high='NaN',stop_level='Infinity',first_resistance='1e999',minimum_reward_risk='not a number'),'INVALIDATED')
c.commit();c.close()`,root]);before=digest();
});
afterAll(()=>{if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-premarket-'))throw Error('UNSAFE_TEST_CLEANUP');rmSync(root,{recursive:true,force:true,maxRetries:10,retryDelay:50});});
function read(extra={}){return JSON.parse(execFileSync(python,['-B',resolve('scripts/team_ui_readonly.py'),root,JSON.stringify({kind:'plans',trade_date:'2026-10-22',limit:50,offset:0,days:20,...extra})],{encoding:'utf8'}));}
test('today pending plans remain visible before activation without leaking old or future batches',()=>{
 const r=read();expect(r.rows.map((x:{id:string})=>x.id)).toEqual(['today-active','today-invalid','today-pending']);
 expect(r.rows[2].status).toBe('PENDING_MORNING_REVIEW');expect(r.rows[2].day_scope_basis).toBe('UNACTIVATED_EXPIRY_DATE');
 expect(r.rows[0].day_scope_basis).toBe('ACTIVATION_DATE');expect(read({limit:1}).next_offset).toBe(1);expect(read({limit:1,offset:2}).rows[0].id).toBe('today-pending');
});
test('persisted decimal text and company name are projected, invalid values are not fabricated as zero',()=>{
 const r=read().rows;expect(r[2]).toMatchObject({name:'合成半导体',zone:[12.3292,12.4525],stop:12.2,target:14.1,minimum_rr:2.5,risk_unit:'PROBE'});
 expect(r[1]).toMatchObject({zone:[null,null],stop:null,target:null,minimum_rr:null});
});
test('data limitation is explicit, absence stays unknown and private source fields never escape',()=>{
 const r=read();expect(r.rows[0].data_limits).toMatchObject({data_limited:true,reason_codes:['PDF_SOURCE_UNAVAILABLE'],a6_explicit_review_required:true});
 expect(r.rows[2].data_limits).toMatchObject({data_limited:null,missing_reason:'SOURCE_GAP_STATUS_NOT_RECORDED'});
 expect(JSON.stringify(r)).not.toMatch(/SECRET|PRIVATE_PATH|webhook|https:/);
});
test('plan detail uses the same current-day pending scope and cannot read a neighboring day',()=>{
 expect(read({plan_id:'today-pending',limit:1}).rows[0].id).toBe('today-pending');
 expect(read({plan_id:'yesterday-pending',limit:1}).rows).toEqual([]);
});
test('all premarket reads preserve the original DB bytes and create no sidecars',()=>{
 read();expect(digest()).toBe(before);expect(readdirSync(join(root,'state'))).toEqual(['workflow.sqlite3']);
});
