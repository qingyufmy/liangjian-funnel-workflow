import {execFileSync} from 'node:child_process';
import {expect,test} from 'vitest';
// @ts-expect-error Static browser module.
import {healthEventText} from '../web/team/health.js';
const python=process.platform==='win32'?'python':'python3';
const setup=String.raw`
import sys,json,sqlite3,tempfile,hashlib
from pathlib import Path
from contextlib import closing
sys.path.insert(0,'scripts')
from team_ui_health import project_health,digest
from team_ui_readonly import connect
case=sys.argv[1]
with tempfile.TemporaryDirectory(prefix='team-ui-health-') as folder:
 root=Path(folder);(root/'state/team_v2').mkdir(parents=True);(root/'outputs/node').mkdir(parents=True)
 with closing(sqlite3.connect(root/'state/workflow.sqlite3')) as d:
  d.execute('CREATE TABLE monitor_events(event_id,minute_end,action,effective,payload_json)')
  d.execute('CREATE TABLE notification_deliveries(delivery_id,kind,source_id,status,attempt_count,last_reason_code,created_at,sent_at)')
  d.execute("INSERT INTO notification_deliveries VALUES('n','TEST','source','SENT',1,NULL,'2026-10-22T09:32:00+08:00','2026-10-22T09:32:01+08:00')")
  def event(i,day,action='NO_ACTION',effective=0,confirmed=False,closed='2026-10-22T10:00:00+08:00'):
   body={'plan_id':'p','secret':'never-export-customer-body','strategy':{'closed_5m_end':closed,'entry_confirmation':{'confirmed':confirmed,'pattern':'A'}}}
   d.execute('INSERT INTO monitor_events VALUES(?,?,?,?,?)',(str(i),day+'T10:00:00+08:00',action,effective,json.dumps(body)))
  if case=='partial':
   for i in range(1202):event(i,'2026-10-22')
  else:
   event(0,'2026-10-21');event(1,'2026-10-22','FORCED_RISK_EXIT',1);event(2,'2026-10-22','START_CONFIRMATION',0,False)
   event(3,'2026-10-22',confirmed=True);event(4,'2026-10-22',confirmed=True)
  d.commit()
 with closing(sqlite3.connect(root/'state/team_v2/team.db')) as d:
  d.executescript('CREATE TABLE risk_meetings(meeting_id,source_call_id,account_id,trade_date,observed_at,result_json);CREATE TABLE pm_decisions(meeting_id,source_call_id,account_id,trade_date,result_json);CREATE TABLE trader_sessions(decision_id,source_call_id,account_id,trade_date,result_json);')
  d.execute('INSERT INTO risk_meetings VALUES(?,?,?,?,?,?)',('r','A6:morning:2026-10-22','local-paper','2026-10-22','2026-10-22T08:35:01+08:00',json.dumps({'status':'DEGRADED','reason_codes':['A6_REVIEW_DEGRADED']})))
  d.execute('INSERT INTO pm_decisions VALUES(?,?,?,?,?)',('pm','A3:morning:2026-10-22','local-paper','2026-10-22',json.dumps({'status':'READY','model_validated':False,'fallback_observed_at':'2026-10-22T08:38:01+08:00','decision_authority':'SERVER_FALLBACK_NOT_MODEL_VALIDATED'})))
  review={key:0 for key in ['confirmed_count','candidate_count','selected_count','actual_model_objects','failed_review_count','timeout_count']}
  d.execute('INSERT INTO trader_sessions VALUES(?,?,?,?,?)',('t','A4:t','local-paper','2026-10-22',json.dumps({'status':'NO_MODEL_REQUIRED','candidate_review':review})));d.commit()
 with closing(sqlite3.connect(root/'state/team_v2/model.db')) as d:
  d.execute('CREATE TABLE model_call_claims(call_id,state,failure_code)');d.execute('INSERT INTO model_call_claims VALUES(?,?,?)',('A6:morning:2026-10-22','CLAIMED',None));d.commit()
 lines=[{'timestamp':'2026-10-22T00:35:01Z','job':'team-morning-risk','message':'HTTP Error 429 Bearer never-export-secret'},
 {'timestamp':'2026-10-22T04:44:01Z','job':None,'message':json.dumps({'event':'NODE_PROCESS_START','pid':123})},
 {'timestamp':'2026-10-22T04:44:02Z','job':'customer-card','message':'HTTP Error 403 never-export-customer-body'}]
 (root/'outputs/node/node-2026-10-22.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in lines),encoding='utf8')
 if case in ('ops','tampered'):
  body={'schema':'team-ui-health-events/1','producer':'PINNED_READ_ONLY_OPS','trade_date':'2026-10-22','captured_at':'2026-10-22T10:59:00+08:00','events':[{'at':'2026-10-22T10:58:00+08:00','kind':'MODEL_ROLE_CHANGED','model_roles':{'research':'deepseek/deepseek-v4-pro','endpoint':'forbidden'},'source':{'id':'actual-ops.json','sha256':'a'*64}}]}
  envelope={'body':body,'body_sha256':digest(body)}
  if case=='tampered':envelope['body']['events'][0]['at']='2026-10-22T10:57:00+08:00'
  (root/'state/team_v2/ui_health_events.json').write_text(json.dumps(envelope),encoding='utf8')
 paths=list(root.rglob('*.db'))+list(root.rglob('*.sqlite3'));before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
 result=project_health(root,'2026-10-22',connect)
 assert before=={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
 print(json.dumps(result))
`;
function run(kind:string){return JSON.parse(execFileSync(python,['-B','-c',setup,kind],{encoding:'utf8'}));}

test('real journals distinguish failed morning, PM fallback, claim unknown, source times and omit all bodies',()=>{
 const h=run('normal');expect(h.events.find((e:{kind:string})=>e.kind==='RISK_MEETING')).toMatchObject({at:'2026-10-22T08:35:01+08:00',status:'DEGRADED'});
 expect(h.events.find((e:{kind:string})=>e.kind==='PM_MEETING')).toMatchObject({status:'READY',model_validated:false,decision_authority:'SERVER_FALLBACK_NOT_MODEL_VALIDATED'});
 expect(h.events.find((e:{kind:string})=>e.kind==='MODEL_CLAIM')).toMatchObject({at:null,status:'CLAIMED'});
 expect(h.events.filter((e:{kind:string})=>e.kind==='PROVIDER_HTTP_ERROR')).toHaveLength(1);expect(JSON.stringify(h)).not.toContain('never-export');expect(h.notifications).toMatchObject({sent:1,failed:0,complete:true});
});
test('effective signal and confirmed shape counts use real flags not order count or START_CONFIRMATION',()=>{
 const h=run('normal');expect(h.monitor).toMatchObject({complete:true,signals:1,confirmations:1,day_rows:4});expect(h.team_reviews).toMatchObject({session_rows:1,confirmed_observations:0,model_objects:0});
});
test('tail without prior-day boundary and over1200 rows never claims full-day zero',()=>{
 const h=run('partial');expect(h.monitor).toMatchObject({complete:false,signals:null,confirmations:null,reason:'MONITOR_TAIL_DOES_NOT_COVER_DAY'});
});
test('normal production receipt reader consumes actual OPS metadata, not frozen-only and rejects tampered hash',()=>{
 const h=run('ops'),change=h.events.find((e:{kind:string})=>e.kind==='MODEL_ROLE_CHANGED');expect(change.model_roles).toEqual({research:'deepseek/deepseek-v4-pro'});expect(change.source.sha256).toBe('a'.repeat(64));expect(h.ops.status).toBe('AVAILABLE');
 const bad=run('tampered');expect(bad.events.some((e:{kind:string})=>e.kind==='MODEL_ROLE_CHANGED')).toBe(false);expect(bad.gaps).toContain('OPS_HEALTH_HISTORY_INVALID');
});
test('health text never turns orphanCLAIMED/fallback/observed outage into current active or model pass',()=>{
 expect(healthEventText({kind:'MODEL_CLAIM',status:'CLAIMED'})).toContain('不代表当前运行');expect(healthEventText({kind:'PM_MEETING',status:'READY',decision_authority:'SERVER_FALLBACK_NOT_MODEL_VALIDATED'})).toContain('不是模型校验通过');
 expect(healthEventText({kind:'NODE_OFFLINE_OBSERVED'})).toContain('不是精确故障起点');expect(healthEventText({kind:'MODEL_CLAIM',status:'FAILED',reason_code:'TEAM_MODEL_CALL_FAILED'})).not.toContain('429');
});
