import {execFileSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,readFileSync,rmSync,readdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename} from 'node:path';
import {createHash} from 'node:crypto';
import {afterAll,beforeAll,test,expect} from 'vitest';
import {parseTeamUiQuery,TeamUiReadPool} from '../server/team-ui-readonly.js';
const python=process.platform==='win32'?'python':'python3';let root:string,hash:string;
const digest=()=>createHash('sha256').update(readFileSync(join(root,'state/workflow.sqlite3'))).digest('hex');
beforeAll(()=>{root=mkdtempSync(join(tmpdir(),'team-ui-plan-detail-'));mkdirSync(join(root,'state'));execFileSync(python,['-B','-c',`import sqlite3,json,sys
from pathlib import Path
c=sqlite3.connect(Path(sys.argv[1])/'state/workflow.sqlite3')
c.execute('CREATE TABLE execution_plans(plan_id,symbol,status,valid_from,expires_at,updated_at,payload_json)')
for i in range(2):c.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?)',('p:'+str(i),'000000.TEST','ACTIVE_TODAY','2026-10-21T09:26:00+08:00',None,'2026-10-21T10:11:00+08:00',json.dumps({'name':'合成详情','trigger_low':10,'trigger_high':11,'stop_level':9,'first_resistance':14,'raw_response':'SECRET_SENTINEL'})))
c.execute('CREATE TABLE monitor_events(event_id,minute_end,payload_json)')
for i,end in enumerate(['10:00','10:05','10:20']):
 body={'plan_id':'p:1','strategy':{'closed_5m_end':'2026-10-21T'+end+':00+08:00','entry_confirmation':{'current_entry_close':10+i,'required_conditions':['ENTRY_RR'],'conditions':{'ENTRY_RR':{'status':'MET','value':99 if i==2 else 3,'threshold':{'minimum':2.5,'internal_path':'/SECRET_PATH','webhook':'SECRET_HOOK'}}}},'indicator_observations':{'volume_5m':{'closed_bar_end':'2026-10-21T'+end+':00+08:00','latest_volume':100}}}}
 c.execute('INSERT INTO monitor_events VALUES(?,?,?)',('event:'+str(i),'2026-10-21T10:11:00+08:00',json.dumps(body)))
body={'plan_id':'p:0','strategy':{'closed_5m_end':'2026-10-21T10:10:00+08:00','entry_confirmation':{'current_entry_close':11,'required_conditions':['ENTRY_RR'],'conditions':{'ENTRY_RR':{'status':'MET','source_as_of':'2026-10-21T10:30:00+08:00','value':777}}}}}
c.execute('INSERT INTO monitor_events VALUES(?,?,?)',('future-condition','2026-10-21T10:15:00+08:00',json.dumps(body)))
c.commit();c.close()`,root]);hash=digest();});
afterAll(()=>{if(!root)return;if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-plan-detail-'))throw Error('UNSAFE_TEST_CLEANUP');rmSync(root,{recursive:true,force:true,maxRetries:10,retryDelay:50});});
function read(plan_id:string){return JSON.parse(execFileSync(python,['-B',resolve('scripts/team_ui_readonly.py'),root,JSON.stringify({kind:'plans',trade_date:'2026-10-21',plan_id,limit:1,offset:0,days:20})],{encoding:'utf8'}));}
test('detail is date/plan scoped, excludes future closed bars and returns only allowlisted existing facts',()=>{
 const item=read('p:1').rows[0];expect(item.id).toBe('p:1');expect(item.target).toBe(14);expect(item.bars).toHaveLength(2);expect(item.bars.map((b:{at:string})=>b.at.slice(11,16))).toEqual(['10:00','10:05']);expect(item.rr).toBe(3);expect(item.conditions[0].threshold).toEqual({minimum:2.5});expect(JSON.stringify(item)).not.toMatch(/SECRET|raw_response|webhook|internal_path/);expect(read('p:0').rows[0].conditions[0]).toMatchObject({code:'ENTRY_RR',status:'MET',source_valid:false,at:'2026-10-21T10:30:00+08:00'});expect(read('not-found').rows).toEqual([]);
});
test('detail route input and worker cache cannot mix plan IDs',async()=>{
 for(const q of ['plan_id=../../private','plan_id=p:1&plan_id=p:0','plan_id=','plan_id=a%27%20OR%201=1'])expect(()=>parseTeamUiQuery('/day/2026-10-21/plans',new URLSearchParams(q),'2026-10-21')).toThrow();
 expect(()=>parseTeamUiQuery('/events',new URLSearchParams('plan_id=p:1'),'2026-10-21')).toThrow();
 const pool=new TeamUiReadPool(async q=>q.plan_id);const base={kind:'plans',trade_date:'2026-10-21',limit:1,offset:0,days:20};expect(await pool.read({...base,plan_id:'p:1'})).toBe('p:1');expect(await pool.read({...base,plan_id:'p:0'})).toBe('p:0');
});
test('detail reads do not mutate DB or create WAL/sidecars',()=>{read('p:1');expect(digest()).toBe(hash);expect(readdirSync(join(root,'state'))).toEqual(['workflow.sqlite3']);});
