"""Bounded existing-evidence export. No model/provider calls and no remote writes."""
import datetime, hashlib, json, pathlib, subprocess, sys
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8')
ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / 'artifacts/observations-20261016'
CODE = r'''
import collections,datetime,hashlib,json,pathlib,sqlite3,subprocess,urllib.request
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
r=pathlib.Path.cwd();assert str(r)=='/www1/Agu/liangjian-funnel-workflow'
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
now=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));assert now.date().isoformat()=='2026-10-16'
s=Settings.from_env(root=r)
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
with op.open('http://127.0.0.1:3210/api/shadow/job-runs-readonly',timeout=5) as f:j=json.loads(f.read())
keep=('runId','job','status','startedAt','finishedAt','exitCode','durationMs')
out={'actual_at':now.isoformat(),'node':j.get('node'),'jobs':[{k:v.get(k) for k in keep} for v in j.get('recentJobRuns',[])],'writes':0,'new_provider_calls':0,'new_model_calls':0}
c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row
c.execute('PRAGMA query_only=ON');c.execute('BEGIN')
def rows(q,args=()):return [dict(x) for x in c.execute(q,args)]
out['tables']=rows("SELECT name,sql FROM sqlite_master WHERE type='table' AND (name LIKE '%a4%' OR name IN ('execution_plans','monitor_events','workflow_runs','virtual_fills','virtual_positions','a5_daily_reviews'))")
out['today_plans']=rows('SELECT * FROM execution_plans WHERE expires_at>=? AND expires_at<? ORDER BY plan_id',('2026-10-16','2026-10-17'))
out['events']=rows('SELECT * FROM monitor_events WHERE minute_end>=? AND minute_end<? ORDER BY minute_end,event_id',('2026-10-16','2026-10-17'))
out['lifecycles']=rows('SELECT * FROM a4_signal_lifecycles WHERE trade_date=? OR (updated_at>=? AND updated_at<?)',('2026-10-16','2026-10-16','2026-10-17'))
out['recent_plan_rows']=rows('SELECT * FROM execution_plans WHERE expires_at>=? AND expires_at<? ORDER BY expires_at,plan_id',('2026-09-15','2026-10-17'))
out['recent_runs']=rows('SELECT * FROM workflow_runs WHERE trade_date>=? ORDER BY created_at',('2026-09-15',))
out['today_reviews']=rows('SELECT review_id,review_kind,status,input_hash,markdown_path,created_at FROM a5_daily_reviews WHERE trade_date=?',('2026-10-16',))
out['fills']=rows('SELECT * FROM virtual_fills WHERE bar_end>=? AND bar_end<?',('2026-10-16','2026-10-17'))
out['simulation_intents']=rows('SELECT * FROM simulation_intents WHERE created_at>=? AND created_at<?',('2026-10-16','2026-10-17'))
c.rollback();c.close()
log=r/'outputs/node/node-2026-10-16.jsonl';chunks=collections.defaultdict(list);meta=[]
targets={v['runId'] for v in j.get('recentJobRuns',[]) if v.get('job')=='monitor'}
for line in log.open():
 try:v=json.loads(line)
 except ValueError:continue
 if v.get('runId') in targets and v.get('stream')=='stdout':chunks[v['runId']].append(v.get('message',''))
 if v.get('job')=='monitor' and v.get('stream')!='stdout':
  # Fixed event metadata only: omit arbitrary text/argv/env.
  meta.append({k:v.get(k) for k in ('timestamp','event','level','job','runId','phase','exitCode','durationMs') if k in v})
out['monitor_stdout']={};out['node_event_metadata']=meta
for rid,parts in chunks.items():
 text='\n'.join(parts);parsed=[];i=0
 while i<len(text):
  if text[i]!='{':i+=1;continue
  try:body,end=json.JSONDecoder().raw_decode(text[i:])
  except ValueError:i+=1;continue
  if isinstance(body,dict):parsed.append(body)
  i+=end
 out['monitor_stdout'][rid]=parsed
out['local_evidence_files']={}
for name,folder in [('facts',s.fact_store_dir),('research',r/'outputs/research'),('progress',r/'outputs')]:
 paths=[]
 for p in folder.glob('*'):
  if p.is_file() and ('2026-10-16' in p.name or 'progress' in p.name):paths.append({'path':str(p.relative_to(r)),'bytes':p.stat().st_size})
 out['local_evidence_files'][name]=paths
out['fact_subdirectories']=[str(p.relative_to(r)) for p in s.fact_store_dir.iterdir() if p.is_dir()]
out['monitor_trace']={v['runId']:v.get('monitorTrace') for v in j.get('recentJobRuns',[]) if v.get('job')=='monitor'}
out['fact_file_catalog']=[]
for folder in (s.fact_store_dir/'a4_live_market'/'2026-10-16',s.fact_store_dir/'board_reference',s.fact_store_dir/'a2_market',s.fact_store_dir/'snapshots'):
 for p in folder.glob('**/*'):
  if p.is_file():out['fact_file_catalog'].append({'path':str(p.relative_to(r)),'bytes':p.stat().st_size})
out['head']=subprocess.check_output(['runuser','-u','www','--','git','rev-parse','HEAD'],text=True).strip()
print(json.dumps(out,ensure_ascii=True))
'''
OUT.mkdir(parents=True, exist_ok=True)
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));prefix=at.strftime('%Y%m%dT%H%M%S%f')+'-zero-signal'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && .venv/bin/python -B -'],input=CODE.encode(),capture_output=True,timeout=45)
for suffix,data in [('stdout.json',p.stdout),('stderr.log',p.stderr)]:
 with (OUT/(prefix+'-'+suffix)).open('xb') as f:f.write(data)
receipt={'actual_at':at.isoformat(),'exit_code':p.returncode,'remote_code_sha256':hashlib.sha256(CODE.encode()).hexdigest(),'stdout_sha256':hashlib.sha256(p.stdout).hexdigest(),'production_writes':0}
with (OUT/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
print(json.dumps({'prefix':prefix,**receipt}));print(p.stderr.decode(errors='replace')[-1800:] if p.returncode else 'READONLY_EXPORT_SAVED')
sys.exit(p.returncode)
