import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename} from 'node:path';
import {expect,test} from 'vitest';
import {parseTeamUiQuery} from '../server/team-ui-readonly.js';
const python=process.platform==='win32'?'python':'python3';
const script=String.raw`import json,sys,hashlib
from pathlib import Path
from datetime import datetime,timezone,timedelta
sys.path.insert(0,str(Path.cwd()/'scripts'))
from team_ui_runtime import read_runtime_receipt
root=Path(sys.argv[1]);case=sys.argv[2]
(root/'.git').mkdir();(root/'.git/HEAD').write_text('a'*40)
(root/'config').mkdir();(root/'state/team_v2').mkdir(parents=True)
sources=[]
for name,raw in [('.env',b'PUBLIC_FIXTURE_MODE=control\nSECRET_FIXTURE_DO_NOT_EXPORT=private\n'),('config/risk_limits.yaml',b'public limits fixture'),('config/team_v2_deployment.yaml',b'public deployment fixture')]:
 (root/name).write_bytes(raw);sources.append(dict(path=name,sha256=hashlib.sha256(raw).hexdigest()))
now=datetime.now(timezone.utc);day=now.astimezone(timezone(timedelta(hours=8))).date().isoformat()
body=dict(pid=4321,process_start_ticks='5678',head='a'*40,observed_at=now.isoformat(),settings=dict(a4_trader_mode='control',broker_adapter='local_paper',monitor_model='deepseek-v4.1-flash',team_intraday_model='deepseek-v4.1-flash',research_models=['deepseek/deepseek-v4-pro'],review_model='deepseek/deepseek-v4-pro'),sources=sources,risk_limits=dict(max_new_entries=3,max_cluster_signals=2,min_cash_reserve_pct='0.10',probe_max_position_pct='0.10',no_new_entry_after='14:30',private_note='DO_NOT_EXPORT'))
query=dict(trade_date=day,_runtime=dict(pid=4321,process_start_ticks='5678'))
if case=='pid_reused':query['_runtime']['process_start_ticks']='9999'
if case=='wrong_pid':query['_runtime']['pid']=4322
if case=='missing_node':query.pop('_runtime')
if case=='future':body['observed_at']=(now+timedelta(minutes=5)).isoformat()
if case=='missing_config_source':body['sources'].pop()
if case=='path_scope':body['sources'][0]['path']='../private'
if case=='bad_model':body['settings']['monitor_model']='https://private.invalid/token'
if case=='historical':query['trade_date']='2020-01-01'
hashbody=lambda x:hashlib.sha256(json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
receipt=dict(schema='team-ui-runtime-status/1',body=body,body_sha256=hashbody(body))
if case=='tampered':receipt['body']['settings']['a4_trader_mode']='record'
(root/'state/team_v2/ui_runtime_status.json').write_text(json.dumps(receipt))
if case=='checkout_changed':(root/'.git/HEAD').write_text('b'*40)
if case=='config_changed':(root/'.env').write_text('different config')
before={p.as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
result=read_runtime_receipt(root,query)
after={p.as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
print(json.dumps(dict(result=result,source_unchanged=before==after)))`;
for(const scenario of ['valid','pid_reused','wrong_pid','missing_node','future','missing_config_source','path_scope','bad_model','historical','tampered','checkout_changed','config_changed'])test(`runtime receipt ${scenario} keeps actual process/config binding and no source writes`,()=>{
 const folder=mkdtempSync(join(tmpdir(),'team-ui-runtime-'));
 try{const data=JSON.parse(execFileSync(python,['-B','-c',script,folder,scenario],{encoding:'utf8'}));expect(data.source_unchanged).toBe(true);
  if(scenario==='valid'){expect(data.result).toMatchObject({status:'VERIFIED_PROCESS_AND_CONFIG_BOUND',pid:4321,configured_limits:{max_new_entries:3,probe_max_position_pct:.1}});expect(data.result.limits_basis).toContain('NOT_PER_ORDER');}
  else expect(data.result.status).toBe('UNVERIFIED');
  expect(JSON.stringify(data)).not.toMatch(/SECRET_FIXTURE|private_note|DO_NOT_EXPORT|private.invalid|PUBLIC_FIXTURE_MODE/);
 }finally{if(dirname(resolve(folder))!==resolve(tmpdir())||!basename(folder).startsWith('team-ui-runtime-'))throw Error('UNSAFE_TEST_CLEANUP');rmSync(folder,{recursive:true,force:true,maxRetries:10,retryDelay:50});}
});
test('HTTP query cannot inject serving PID or process start evidence',()=>{
 const parsed=parseTeamUiQuery('/dashboard',new URLSearchParams('_runtime=pretend&pid=4321&process_start_ticks=5678'),'2026-10-22');expect(parsed).not.toHaveProperty('_runtime');expect(parsed).not.toHaveProperty('pid');
});
