"""Seal existing PIT daily versions needed by original A3 stage enrichment.

No provider acquisition, model, workflow/store construction, or production write.
The base snapshots intentionally did not persist their stage technical overlays.
"""
import argparse,base64,gzip,hashlib,json,subprocess,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a2_frozen_dryrun import source
from run_team_a5_frozen_dryrun import save

REMOTE=r'''
import gzip,json,sqlite3,subprocess,sys,time,urllib.request
from datetime import datetime,timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
request=json.loads(sys.stdin.readline());started=datetime.now(ZoneInfo('Asia/Shanghai'))
assert Path.cwd()==Path('/www1/Agu/liangjian-funnel-workflow')
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()=='2b077f393321c1858f17546682191caeb8eaa41f'
assert started.date().isoformat()=='2026-10-17'
assert int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')))>=800*1024
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
with op.open('http://127.0.0.1:3210/api/shadow/job-runs-readonly',timeout=3) as f:jobs=json.loads(f.read(4*1024*1024))
assert not any(str(j.get('status','')).upper()=='RUNNING' for j in jobs['recentJobRuns']),'ACTIVE_JOB_PRESERVED'
s=Settings.from_env(root=Path.cwd());deadline=time.monotonic()+45
c=sqlite3.connect(s.fact_cache_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row
c.execute('PRAGMA query_only=ON');c.set_progress_handler(lambda:int(time.monotonic()>deadline),1000);c.execute('BEGIN')
result=[]
for item in request:
 cutoff=datetime.fromisoformat(item['as_of']).astimezone(timezone.utc).isoformat(timespec='microseconds')
 assert item['day'] in {'2026-10-14','2026-10-15','2026-10-16'} and len(item['symbols'])<=100
 for symbol in item['symbols']:
  rows=c.execute("WITH ranked AS (SELECT payload_json,bar_timestamp,fetched_at,content_hash,ROW_NUMBER() OVER (PARTITION BY bar_timestamp ORDER BY fetched_at DESC,content_hash DESC) AS n FROM daily_bars WHERE symbol=? AND adjust='none' AND bar_timestamp<=? AND fetched_at<=?) SELECT payload_json,bar_timestamp,fetched_at,content_hash FROM ranked WHERE n=1 ORDER BY bar_timestamp DESC LIMIT 800",(symbol,cutoff,cutoff)).fetchall()
  result.append(dict(day=item['day'],symbol=symbol,cutoff=cutoff,versions=[dict(r) for r in reversed(rows)]))
c.rollback();c.close()
body=dict(actual_at=started.isoformat(),finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
 request=request,rows=result,models=0,providers=0,production_writes=0,source='EXISTING_FACT_CACHE_PIT_REVISIONS_NOT_NEW_ACQUISITION')
sys.stdout.buffer.write(gzip.compress(json.dumps(body,ensure_ascii=True,separators=(',',':')).encode(),mtime=0))
'''

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 output=a.output.resolve()
 if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_DIRECTORY_REQUIRED')
 from liangjian_funnel.pipeline.research.common import _build_a3_candidate_domain
 request=[]
 for day in ('2026-10-14','2026-10-15','2026-10-16'):
  snapshot,_,a2,proof=source(a.sources.resolve(),day);_,origins=_build_a3_candidate_domain(a2['output'])
  request.append(dict(day=day,as_of=snapshot['as_of'],symbols=sorted(origins),source_proofs=proof))
 config=subprocess.check_output(['ssh','-G','aurum-vm'],text=True,stderr=subprocess.DEVNULL)
 if 'hostname 192.168.0.254' not in config.splitlines():raise ValueError('SSH_HOST_INVALID')
 known=subprocess.check_output(['ssh-keygen','-F','192.168.0.254'],text=True)
 pins={base64.b64encode(hashlib.sha256(base64.b64decode(r.split()[2])).digest()).decode().rstrip('=') for r in known.splitlines() if r and not r.startswith('#')}
 if 'LnPSC/kfZ7jQtzj55siqgWOkW0xy/6cGyoAwglS6yQ0' not in pins:raise ValueError('SSH_PIN_INVALID')
 output.mkdir(parents=True)
 # The script and exact request are public source facts; credentials are never read.
 wrapper='import sys,io\nsys.stdin=io.StringIO('+repr(json.dumps(request))+'+"\\n")\nexec('+repr(REMOTE)+')\n'
 command=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm',
  'cd /www1/Agu/liangjian-funnel-workflow && runuser -u www -- nice -n 10 .venv/bin/python -B -']
 started=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
 with (output/'source.json.gz').open('xb') as out,(output/'ssh.stderr.log').open('xb') as err:
  proc=subprocess.run(command,input=wrapper.encode(),stdout=out,stderr=err,timeout=65)
 save(output/'COMMAND.json',dict(actual_started_at=started,actual_finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),exit_code=proc.returncode,command=command))
 if proc.returncode:raise ValueError('PIT_EXPORT_FAILED')
 raw=(output/'source.json.gz').read_bytes();body=json.loads(gzip.decompress(raw))
 if body['request']!=request:raise ValueError('PIT_REQUEST_BINDING_INVALID')
 counts=[dict(day=r['day'],symbol=r['symbol'],count=len(r['versions'])) for r in body['rows']]
 receipt=dict(status='SEALED_NOT_A3_PARITY_PROVEN',sha256=hashlib.sha256(raw).hexdigest(),counts=counts,
  production_writes=0,models=0,provider_calls=0,actual_at=body['actual_at'])
 save(output/'RECEIPT.json',receipt);print(json.dumps(receipt));return 0

if __name__=='__main__':sys.exit(main())
