"""E1 small original summaries only; no model calls or heavy monitor scans."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar

REMOTE = r'''
import codecs,datetime,hashlib,json,pathlib,re,sqlite3,subprocess,time,urllib.request
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
root=pathlib.Path.cwd()
assert str(root)=='/www1/Agu/liangjian-funnel-workflow'
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head==EXPECTED_HEAD
s=Settings.from_env(root=root)
started=time.monotonic();cutoff=started+8
now=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));assert now.date().isoformat()==AS_OF
out={'actual_at':now.isoformat(),'production_head':head,'trading_days':TRADING_DAYS,
     'calendar_version':CALENDAR_VERSION,'production_writes':0,'models_called':0,'source_calls':0,
     'research_markdown_stages':[],'files':[],'skipped':[]}
c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');c.execute('BEGIN')
c.set_progress_handler(lambda: int(time.monotonic()>cutoff),1000)
try:
 out['research_run_rows']=[dict(row) for row in c.execute('SELECT run_id,trade_date,slot,model,status,created_at,updated_at FROM research_runs WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date,run_id',(TRADING_DAYS[0],AS_OF))]
 out['a5_persisted_reviews']=[dict(row) for row in c.execute('SELECT review_id,trade_date,review_kind,cutoff_at,status,model,input_hash,prompt_hash,output_hash,latency_ms,attempts,created_at FROM a5_daily_reviews WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date,review_id',(TRADING_DAYS[0],AS_OF))]
finally:c.rollback();c.close()
lane_re=re.compile(r'^### (\S+) · (\S+)\s*$')
stage_re=re.compile(r'^- (A[123])：`([^`]+)`；原因：`([^`]*)`；请求次数：`(\d+)`；耗时：`([^`]+)`')
run_re=re.compile(r'^- run_id：`([^`]+)`',re.M)
generated_re=re.compile(r'^- 运行时间：`([^`]+)`',re.M)
total=0
for p in sorted((s.workflow_output_dir/'research').glob('research_*.md')):
 if time.monotonic()>cutoff:out['skipped'].append({'reason':'WALL_BUDGET_REACHED'});break
 # File-name date selects candidates only; report provenance below is mandatory.
 named=re.search(r'(\d{4}-\d{2}-\d{2})',p.name)
 if not named or not TRADING_DAYS[0]<=named[1]<=AS_OF:continue
 st=p.stat()
 if total+min(st.st_size,32768)>2*1024*1024:
  out['skipped'].append({'path':str(p.relative_to(root)),'reason':'BOUNDED_METADATA_READ_LIMIT','bytes':st.st_size});continue
 with p.open('rb') as f:raw=f.read(32768)
 total+=len(raw);after=p.stat()
 if (st.st_ino,st.st_size,st.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):
  out['skipped'].append({'path':str(p.relative_to(root)),'reason':'SOURCE_IDENTITY_CHANGED'});continue
 text=codecs.getincrementaldecoder('utf-8-sig')().decode(raw,final=len(raw)==st.st_size);run=run_re.search(text);generated=generated_re.search(text)
 if not run or not generated:continue
 source={'path':str(p.relative_to(root)),'prefix_sha256':hashlib.sha256(raw).hexdigest(),'prefix_bytes':len(raw),'file_bytes':st.st_size,'prefix_only':len(raw)<st.st_size,
         'run_id':run[1],'generated_at':generated[1],'basis':'GENERATED_STAGE_AUDIT_PROJECTION_NOT_RAW_PROVIDER_LOG'}
 out['files'].append(source);lane=None;model=None
 for line in text.splitlines():
  match=lane_re.match(line)
  if match:lane,model=match.groups();continue
  match=stage_re.match(line)
  if not match or lane is None:continue
  stage,status,reasons,attempts,latency=match.groups()
  out['research_markdown_stages'].append({'run_id':run[1],'generated_at':generated[1],'lane':lane,'model':model,
    'stage':stage,'status':status,'reason_codes':reasons,'stage_request_attempts':int(attempts),
    'latency_ms':int(latency[:-3]) if re.fullmatch(r'\d+ ms',latency) else None,
    'source_path':source['path'],'source_prefix_sha256':source['prefix_sha256']})
out['bytes_read_from_research_markdown']=total
out['coverage_limits']=['A4_NOT_SCANNED_DURING_ACTIVE_A1','A5_FAILED_CALLS_WITHOUT_PERSISTED_REVIEW_NOT_COUNTED',
 'RESEARCH_AGGREGATE_STAGE_ATTEMPTS_NOT_INDEPENDENT_LOGICAL_CALL_IDS','PASS_VETO_FOCUS_AND_ENGINE_AGREEMENT_PENDING_RAW_RESPONSE_EXPORT',
 'MISSING_FILES_OR_ROWS_ARE_UNKNOWN_NOT_ZERO','TWO_MIB_FILE_READ_BUDGET','ONLY_FIRST_32768_BYTES_PER_MARKDOWN_PREFIX_HASH_NOT_FULL_FILE_HASH']
out['elapsed_seconds']=round(time.monotonic()-started,3)
print(json.dumps(out,ensure_ascii=True))
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--as-of', type=date.fromisoformat, required=True)
    parser.add_argument('--expected-head', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    if datetime.now(ZoneInfo('Asia/Shanghai')).date() != args.as_of:
        raise ValueError('AUDIT_SAME_REAL_DAY_REQUIRED')
    calendar = ExchangeTradingCalendar(); day = args.as_of
    if not calendar.is_trading_day(day):day = calendar.previous_trading_day(day)
    days = [day]
    for _ in range(19):days.append(calendar.previous_trading_day(days[-1]))
    values = dict(TRADING_DAYS=[d.isoformat() for d in reversed(days)], AS_OF=args.as_of.isoformat(),
        EXPECTED_HEAD=args.expected_head, CALENDAR_VERSION=importlib.metadata.version('exchange_calendars'))
    code = '\n'.join(key+'='+repr(value) for key, value in values.items())+'\n'+REMOTE
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    prefix = now.strftime('%Y%m%dT%H%M%S%f')+'-model-usage-small-readonly'
    args.output_dir.mkdir(parents=True,exist_ok=True)
    result = subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8',
        'aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && sudo -n -u www .venv/bin/python -B -'],
        input=code.encode(),capture_output=True,timeout=20)
    for suffix,raw in [('stdout.json',result.stdout),('stderr.log',result.stderr)]:
        with (args.output_dir/(prefix+'-'+suffix)).open('xb') as f:f.write(raw)
    receipt={'actual_at':now.isoformat(),'exit_code':result.returncode,'remote_code_sha256':hashlib.sha256(code.encode()).hexdigest(),
        'stdout_sha256':hashlib.sha256(result.stdout).hexdigest(),'production_writes':0,'model_calls':0}
    with (args.output_dir/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
    print(json.dumps({'prefix':prefix,**receipt}))
    if result.returncode==0:
        data=json.loads(result.stdout)
        print(json.dumps({key:data[key] for key in ('trading_days','elapsed_seconds','bytes_read_from_research_markdown')}))
        print(json.dumps({'stage_audits':len(data['research_markdown_stages']),'a5_persisted_reviews':len(data['a5_persisted_reviews']),'skipped':len(data['skipped'])}))
    sys.exit(result.returncode)


if __name__=='__main__':main()
