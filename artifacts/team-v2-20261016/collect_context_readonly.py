"""Export bounded pre-existing context files/schema, never obtain new market data."""
import datetime,hashlib,json,pathlib,subprocess,sys
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8')
ROOT=pathlib.Path(__file__).resolve().parents[2];OUT=ROOT/'artifacts/observations-20261016'
CODE=r'''
import datetime,hashlib,json,pathlib,sqlite3,subprocess
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
r=pathlib.Path.cwd();assert str(r)=='/www1/Agu/liangjian-funnel-workflow'
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));assert str(at.date())=='2026-10-16'
s=Settings.from_env(root=r);out={'actual_at':at.isoformat(),'files':[],'writes':0,'provider_calls':0,'model_calls':0}
paths=list((s.fact_store_dir/'board_reference'/'hithink').glob('*.json'))
paths += [p for p in (s.fact_store_dir/'a2_market').glob('*.json') if any(d in p.name for d in ('2026-10-12','2026-10-13','2026-10-14','2026-10-15','2026-10-16')) and not p.name.startswith('capital-flow')]
paths += list((s.fact_store_dir/'a4_live_market'/'2026-10-16').glob('1455.json'))
paths += [p for p in (r/'outputs/research').glob('*_A?.md') if any(d in p.name for d in ('2026-10-12','2026-10-13','2026-10-14','2026-10-15','2026-10-16'))]
budget=10*1024*1024
for p in sorted(paths):
 size=p.stat().st_size
 if size>800000 or size>budget:continue
 raw=p.read_bytes();budget-=len(raw)
 out['files'].append({'path':str(p.relative_to(r)),'sha256':hashlib.sha256(raw).hexdigest(),'text':raw.decode('utf-8')})
out['fact_db_schemas']={}
for name,path in [('fact',s.fact_cache_db_path),('feature',s.feature_store_db_path)]:
 if not path.exists():continue
 c=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON')
 out['fact_db_schemas'][name]=[dict(v) for v in c.execute("SELECT name,sql FROM sqlite_master WHERE type='table'")]
 c.close()
print(json.dumps(out,ensure_ascii=True))
'''
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));prefix=at.strftime('%Y%m%dT%H%M%S%f')+'-context'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && .venv/bin/python -B -'],input=CODE.encode(),capture_output=True,timeout=30)
for suffix,raw in [('stdout.json',p.stdout),('stderr.log',p.stderr)]:
 with (OUT/(prefix+'-'+suffix)).open('xb') as f:f.write(raw)
receipt={'actual_at':at.isoformat(),'exit_code':p.returncode,'stdout_sha256':hashlib.sha256(p.stdout).hexdigest(),'remote_code_sha256':hashlib.sha256(CODE.encode()).hexdigest()}
with (OUT/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
print(json.dumps({'prefix':prefix,**receipt}));print(p.stderr.decode(errors='replace')[-1200:] if p.returncode else 'CONTEXT_ONLY_SAVED')
sys.exit(p.returncode)
