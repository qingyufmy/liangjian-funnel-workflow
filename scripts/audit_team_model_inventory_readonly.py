"""Bounded E1 source inventory and original A4 metadata, never model inference."""
import argparse
import base64
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
AS_OF = '2026-10-16'
EXPECTED = '2b077f393321c1858f17546682191caeb8eaa41f'
REMOTE = r'''
import datetime,hashlib,json,os,pathlib,re,sqlite3,subprocess,time,urllib.request
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
r=pathlib.Path.cwd();assert str(r)=='/www1/Agu/liangjian-funnel-workflow'
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip();assert head==EXPECTED
now=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));assert now.date().isoformat()==AS_OF
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
with op.open('http://127.0.0.1:3210/api/shadow/job-runs-readonly',timeout=3) as f:b=json.loads(f.read(4*1024*1024))
active=[v['job'] for v in b['recentJobRuns'] if str(v.get('status','')).upper()=='RUNNING'];assert not active,'ACTIVE_WORKFLOW_PRESERVED'
mem={line.split(':',1)[0]:int(line.split()[1]) for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:')}
assert mem['MemAvailable']>=800*1024,'RESOURCE_GATE'
s=Settings.from_env(root=r);start=time.monotonic();deadline=start+12
out={'actual_at':now.isoformat(),'head':head,'active_jobs':active,'mem_kib':mem,'files':[],
     'small_manifests':[],'research_json_prefixes':[],'a4_event_summary':None,'errors':[],
     'production_writes':0,'models':0,'source_calls':0}
total=0;count=0
roots=[s.workflow_output_dir/'research',s.workflow_output_dir/'audits',s.workflow_output_dir/'a4']
for root in roots:
 if not root.exists():continue
 stack=[(root,0)]
 while stack and time.monotonic()<deadline:
  directory,depth=stack.pop()
  with os.scandir(directory) as entries:
   for entry in entries:
    count+=1
    if count>4000 or time.monotonic()>deadline:break
    if entry.is_dir(follow_symlinks=False):
     if depth<2:stack.append((pathlib.Path(entry.path),depth+1))
     continue
    if not entry.is_file(follow_symlinks=False):continue
    p=pathlib.Path(entry.path);match=re.search(r'(2026-\d\d-\d\d)',p.name)
    if not match or not '2026-09-11'<=match[1]<=AS_OF:continue
    st=p.stat();item={'path':str(p.relative_to(r)),'bytes':st.st_size,'inode':st.st_ino,'mtime_ns':st.st_mtime_ns}
    out['files'].append(item)
    size=(st.st_size if p.name.endswith('.decisions.json') and st.st_size<=65536 else
          min(st.st_size,16384) if p.suffix=='.json' and '.decisions.' not in p.name else 0)
    if not size or total+size>2*1024*1024:continue
    with p.open('rb') as handle:raw=handle.read(size)
    total+=len(raw);after=p.stat()
    if (st.st_ino,st.st_size,st.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):
     out['errors'].append({'path':item['path'],'code':'SOURCE_IDENTITY_CHANGED'});continue
    meta={**item,'read_bytes':len(raw),'sha256_of_read_bytes':hashlib.sha256(raw).hexdigest(),'full_file':len(raw)==st.st_size}
    if p.name.endswith('.decisions.json'):
     data=json.loads(raw);out['small_manifests'].append({**meta,'manifest':data})
    else:
     # A large lane file can start with nested outcome/snapshot evidence before
     # stages. A textual prefix is not a stage parser and makes no stage claim.
     out['research_json_prefixes'].append({**meta,'stage_metadata':'NOT_PARSED_PREFIX_NOT_A_JSON_OBJECT'})
 if count>4000:out['errors'].append({'code':'FILE_ENUMERATION_BOUND'});break
out['inventory_entries_examined']=count;out['file_bytes_read']=total
if time.monotonic()<deadline:
 c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row
 c.execute('PRAGMA query_only=ON');c.set_progress_handler(lambda:int(time.monotonic()>deadline),1000);c.execute('BEGIN')
 try:
  out['a4_event_summary']=[dict(row) for row in c.execute("SELECT substr(minute_end,1,10) trade_date,action,reason_code,count(*) rows,count(DISTINCT minute_end||'|'||lane_id) minute_lanes FROM monitor_events WHERE minute_end>=? AND minute_end<? GROUP BY trade_date,action,reason_code ORDER BY trade_date,action,reason_code",('2026-09-11','2026-10-17'))]
  out['a4_sql_schema']=[dict(row) for row in c.execute("SELECT name,sql FROM sqlite_master WHERE tbl_name='monitor_events'")]
 except sqlite3.OperationalError:out['errors'].append({'code':'A4_METADATA_QUERY_INTERRUPTED_OR_UNAVAILABLE'})
 finally:c.rollback();c.close()
out['elapsed_seconds']=round(time.monotonic()-start,3)
out['limits']=['A4_EVENT_ROWS_NOT_MODEL_CALLS','MANIFESTS_DERIVED_NOT_RAW_MODEL_OUTPUTS','RESEARCH_PREFIX_ONLY_FIRST_STAGE_NOT_FULL_REPORT','TWO_MIB_READ_BOUND','TWELVE_SECOND_WORK_BOUND','OLDER_SOURCE_LOCATION_NOT_PROVEN_ABSENT']
print(json.dumps(out,ensure_ascii=True))
'''

# Reuse exactly the host, original HEAD, idle-workflow and resource gates above.
# Only the source collection after these gates differs in this second probe.
REMOTE_ATTEMPTS = REMOTE[:REMOTE.index('s=Settings.from_env(root=r);')] + r'''
from collections import Counter
s=Settings.from_env(root=r);start=time.monotonic();deadline=start+15
out={'actual_at':now.isoformat(),'head':head,'active_jobs':active,'mem_kib':mem,
     'attempts':[],'skipped':[],'a4_original_decision_projections':[],'metadata_tables':[],
     'production_writes':0,'models':0,'source_calls':0}
total=0
for stage in (() if A4_ONLY else ('a2','a3')):
 folder=s.workflow_output_dir/'research'/(stage+'_review_attempts')
 if not folder.exists():out['skipped'].append({'stage':stage,'code':'ATTEMPT_FOLDER_ABSENT'});continue
 for run in sorted(folder.iterdir()):
  match=re.search(r'(2026-\d\d-\d\d)',run.name)
  if not run.is_dir() or not match or not '2026-09-11'<=match[1]<=AS_OF:continue
  for p in sorted(run.glob('*/*.json')):
   st=p.stat();rel=str(p.relative_to(r))
   if REMAINING_ONLY and rel not in REMAINING_PATHS:continue
   per_file=4*1024*1024 if REMAINING_ONLY else 2*1024*1024
   if time.monotonic()>deadline or st.st_size>per_file or total+st.st_size>20*1024*1024:
    out['skipped'].append({'path':rel,'bytes':st.st_size,'code':'READ_BUDGET'});continue
   raw=p.read_bytes();total+=len(raw);after=p.stat()
   if (st.st_ino,st.st_size,st.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):
    out['skipped'].append({'path':rel,'code':'SOURCE_IDENTITY_CHANGED'});continue
   data=json.loads(raw)
   item={k:data.get(k) for k in ('schema_version','run_id','lane','model','snapshot_id','snapshot_hash',
       'prompt_hash','input_hash','semantic_attempt','record_hash','validation_reasons','policy_demotions')}
   item.update(path=rel,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),inode=st.st_ino,mtime_ns=st.st_mtime_ns)
   body={k:v for k,v in data.items() if k!='record_hash'}
   computed=hashlib.sha256(json.dumps(body,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
   item['record_hash_verified']=computed==data.get('record_hash')
   model=data.get('model_output');post=data.get('post_policy_output')
   fields=('focus_decisions','reject_decisions','rotation_reviews','theme_reviews','core_watch_pool','secondary_watch_pool','rejected_candidates')
   item['model_pool_counts']={key:len(model[key]) for key in fields if isinstance(model,dict) and isinstance(model.get(key),list)}
   # Decision identifiers and categorical verdict only, not rationale text.
   def decisions(value):
    found=[]
    if not isinstance(value,dict):return found
    for pool in fields:
     for row in value.get(pool,[]) if isinstance(value.get(pool),list) else []:
      if isinstance(row,dict):found.append({'pool':pool,**{key:row.get(key) for key in
          ('symbol','review_status','rotation_direction_id','decision','stage','new_entry_policy') if key in row}})
    return found
   item['model_decisions']=decisions(model);item['post_policy_decisions']=decisions(post)
   out['attempts'].append(item)
c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row
c.execute('PRAGMA query_only=ON');c.set_progress_handler(lambda:int(time.monotonic()>deadline),1000);c.execute('BEGIN')
try:
 out['metadata_tables']=[dict(row) for row in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%model%' OR name LIKE '%research%' OR name LIKE '%review%')")]
 rows=c.execute("SELECT event_id,lane_id,minute_end,action,reason_code,payload_json FROM monitor_events WHERE minute_end>=? AND minute_end<? AND action IN ('BUY_SIGNAL','LLM_VETO') ORDER BY minute_end LIMIT 50",('2026-09-11','2026-10-17')).fetchall()
 for row in rows:
  payload=json.loads(row['payload_json']);strategy=payload.get('strategy',{})
  out['a4_original_decision_projections'].append({**{key:row[key] for key in ('event_id','lane_id','minute_end','action','reason_code')},
    'payload_sha256':hashlib.sha256(row['payload_json'].encode()).hexdigest(),'payload_keys':sorted(payload),
    'plan_id':payload.get('plan_id'),'llm_veto':payload.get('llm_veto'),'llm_reason_code':payload.get('llm_reason_code'),
    'nested_llm_review':strategy.get('llm_review') if isinstance(strategy,dict) else None,
    'nested_llm_reason_code':strategy.get('llm_reason_code') if isinstance(strategy,dict) else None})
except sqlite3.OperationalError:out['skipped'].append({'code':'SQL_DEADLINE_OR_UNAVAILABLE'})
finally:c.rollback();c.close()
out['bytes_read_from_attempt_files']=total;out['elapsed_seconds']=round(time.monotonic()-start,3)
out['limits']=['ATTEMPT_RECORDS_ARE_SEMANTIC_ATTEMPTS_NOT_PROVIDER_HTTP_ATTEMPTS','A4_EVENTS_NOT_MODEL_CALLS',
 'MISSING_OLD_FILES_NOT_ZERO','NO_RAW_MODEL_RATIONALE_OR_PROMPT_DUMP','POST_POLICY_NOT_INDEPENDENT_ENGINE_VERDICT',
 'TWO_MIB_PER_FILE_TWENTY_MIB_TOTAL_FIFTEEN_SECONDS_BOUND']
print(json.dumps(out,ensure_ascii=True))
'''


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--attempts',action='store_true')
    parser.add_argument('--remaining',action='store_true');parser.add_argument('--a4-only',action='store_true');args=parser.parse_args()
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    if now.date().isoformat() != AS_OF: raise ValueError('SAME_REAL_DAY_REQUIRED')
    known = subprocess.check_output(['ssh-keygen', '-F', '192.168.0.254'], text=True)
    pins = [base64.b64encode(hashlib.sha256(base64.b64decode(line.split()[2])).digest()).decode().rstrip('=')
            for line in known.splitlines() if line.strip() and not line.startswith('#')]
    assert 'LnPSC/kfZ7jQtzj55siqgWOkW0xy/6cGyoAwglS6yQ0' in pins
    config = subprocess.check_output(['ssh', '-G', 'aurum-vm'], text=True, stderr=subprocess.DEVNULL)
    assert 'hostname 192.168.0.254' in config.splitlines()
    remaining_paths=[
        'outputs/research/a2_review_attempts/2026-10-13-auction-refresh-092622/lane_1/attempt-1-7125c65a73fa52c6.json',
        'outputs/research/a2_review_attempts/2026-10-16-auction-refresh-092646/lane_1/attempt-1-df2f16056dad47f4.json',
        'outputs/research/a2_review_attempts/2026-10-16-auction-refresh-092646/lane_1/attempt-2-052e50e92e0fbc98.json']
    detail=args.attempts or args.remaining or args.a4_only
    code = (f'AS_OF={AS_OF!r}\nEXPECTED={EXPECTED!r}\nA4_ONLY={args.a4_only!r}\n'
        f'REMAINING_ONLY={args.remaining!r}\nREMAINING_PATHS={remaining_paths!r}\n'
        +(REMOTE_ATTEMPTS if detail else REMOTE))
    out = ROOT/'artifacts/llm-audit-20261016';out.mkdir(parents=True, exist_ok=True)
    suffix='-model-review-remaining-readonly' if args.remaining else '-model-a4-details-readonly' if args.a4_only else '-model-review-attempts-readonly' if detail else '-model-source-inventory-readonly'
    prefix = now.strftime('%Y%m%dT%H%M%S%f')+suffix
    result = subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8',
        'aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && sudo -n -u www .venv/bin/python -B -'],
        input=code.encode(),capture_output=True,timeout=25)
    for suffix,raw in [('stdout.json',result.stdout),('stderr.log',result.stderr)]:
        with (out/(prefix+'-'+suffix)).open('xb') as handle:handle.write(raw)
    receipt={'actual_at':now.isoformat(),'exit_code':result.returncode,'host_pin_strict':True,
        'remote_code_sha256':hashlib.sha256(code.encode()).hexdigest(),'stdout_sha256':hashlib.sha256(result.stdout).hexdigest(),
        'production_writes':0,'model_calls':0}
    with (out/(prefix+'-receipt.json')).open('x') as handle:json.dump(receipt,handle,indent=2)
    print(json.dumps({'prefix':prefix,**receipt}))
    if result.returncode == 0:
        data=json.loads(result.stdout)
        if detail:
            print(json.dumps({'actual_at':data['actual_at'],'elapsed_seconds':data['elapsed_seconds'],
                'attempts':len(data['attempts']),'skipped':len(data['skipped']),
                'a4_original_decision_projections':len(data['a4_original_decision_projections'])}))
        else:
            print(json.dumps({key:data[key] for key in ('actual_at','elapsed_seconds',
                'file_bytes_read','inventory_entries_examined','errors')}))
            print(json.dumps({'files':len(data['files']),'small_manifests':len(data['small_manifests']),
                'a4_metadata_groups':None if data['a4_event_summary'] is None else len(data['a4_event_summary'])}))
    else: print('INVENTORY_FAILED_OR_SAFE_GATE_SEE_PRESERVED_STDERR')
    sys.exit(result.returncode)


if __name__ == '__main__': main()
