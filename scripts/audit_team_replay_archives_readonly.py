"""Bounded VM archive inventory only: no collectors, models or runtime stores."""
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys

REMOTE=r'''
from contextlib import closing
from datetime import datetime
import json,os,sqlite3,subprocess,time
from pathlib import Path
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
root=Path.cwd();assert str(root)=='/www1/Agu/liangjian-funnel-workflow'
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
now=datetime.now(ZoneInfo('Asia/Shanghai'));assert now.date().isoformat()=='2026-10-17'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head=='2b077f393321c1858f17546682191caeb8eaa41f','HEAD_CHANGED_REVIEW_BEFORE_EXPORT'
s=Settings.from_env(root=root);deadline=time.monotonic()+12
out=dict(actual_at=now.isoformat(),head=head,production_writes=0,provider_calls=0,model_calls=0,
    databases=[],archive_files=[],directories=[],limits=[])
for path in (s.state_db_path,s.minute_cache_dir/'minute_bars.sqlite3',s.fact_cache_db_path):
 if time.monotonic()>deadline:out['limits'].append('TIME_BOUND');break
 if not path.is_file():out['databases'].append(dict(path=str(path),status='MISSING'));continue
 item=dict(path=str(path),bytes=path.stat().st_size,tables=[])
 with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
  db.execute('PRAGMA query_only=ON');db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000);db.execute('BEGIN')
  tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
  for name in tables:
   if not name.replace('_','').isalnum():continue
   columns=[r[1] for r in db.execute('PRAGMA table_info('+name+')')]
   item['tables'].append(dict(name=name,columns=columns))
  db.rollback()
 out['databases'].append(item)
seen=0
for base in (s.fact_store_dir,s.workflow_output_dir,s.state_db_path.parent):
 if not base.is_dir():continue
 out['directories'].append(str(base))
 stack=[(base,0)]
 while stack and time.monotonic()<deadline and seen<15000:
  directory,depth=stack.pop()
  with os.scandir(directory) as entries:
   for e in entries:
    seen+=1
    if seen>=15000 or time.monotonic()>deadline:break
    if e.is_dir(follow_symlinks=False):
     if depth==0:out['directories'].append(e.path)
     if depth<4 and e.name not in {'.git','.venv','node_modules','lark_cards'}:stack.append((Path(e.path),depth+1))
     continue
    if not e.is_file(follow_symlinks=False):continue
    p=Path(e.path);relative=str(p.relative_to(root));lower=relative.lower()
    if any(word in lower for word in ('quote','full_market','a4_live_market','market_snapshot','intraday_market')):
     st=e.stat(follow_symlinks=False)
     out['archive_files'].append(dict(path=relative,bytes=st.st_size,inode=st.st_ino,mtime_ns=st.st_mtime_ns))
 if seen>=15000:out['limits'].append('ENTRY_BOUND')
 if time.monotonic()>deadline:out['limits'].append('TIME_BOUND')
out['entries_examined']=seen
# The broad metadata walk can hit its cap before reaching per-day summaries.
# Probe only these known producer directories separately; do not claim the
# bounded first walk proved the absence of another archive.
out['focused_files']=[]
for family in ('a2_market','a4_live_market','open_macro'):
 base=s.fact_store_dir/family
 if not base.is_dir():continue
 for p in sorted(base.glob('*')):
  if p.is_file() and p.suffix=='.json':
   st=p.stat();out['focused_files'].append(dict(path=str(p.relative_to(root)),bytes=st.st_size))
  elif p.is_dir() and '2026-10-12'<=p.name<='2026-10-16':
   for child in sorted(p.glob('*.json')):
    st=child.stat();out['focused_files'].append(dict(path=str(child.relative_to(root)),bytes=st.st_size))
out['cache_namespaces']=[]
with closing(sqlite3.connect(s.fact_cache_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
 db.execute('PRAGMA query_only=ON');namespace_deadline=time.monotonic()+3
 db.set_progress_handler(lambda:int(time.monotonic()>namespace_deadline),1000)
 try:out['cache_namespaces']=[r[0] for r in db.execute('SELECT DISTINCT namespace FROM cached_results LIMIT 100')]
 except sqlite3.OperationalError:out['limits'].append('CACHE_NAMESPACES_TIME_BOUND')
print(json.dumps(out,ensure_ascii=True,sort_keys=True))
'''


def main():
    config=subprocess.check_output(['ssh','-G','aurum-vm'],text=True,stderr=subprocess.DEVNULL)
    assert 'hostname 192.168.0.254' in config.splitlines()
    known=subprocess.check_output(['ssh-keygen','-F','192.168.0.254'],text=True)
    pins={'SHA256:'+base64.b64encode(hashlib.sha256(base64.b64decode(r.split()[2])).digest()).decode().rstrip('=')
        for r in known.splitlines() if r and not r.startswith('#')}
    assert 'SHA256:LnPSC/kfZ7jQtzj55siqgWOkW0xy/6cGyoAwglS6yQ0' in pins
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8',
        'aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && sudo -n -u www .venv/bin/python -B -'],
        input=REMOTE.encode(),capture_output=True,timeout=25)
    if result.returncode:
        # Never echo environment/provider exception contents into a bridge.
        print(json.dumps(dict(status='ARCHIVE_INVENTORY_FAILED',exit_code=result.returncode,
            stderr_sha256=hashlib.sha256(result.stderr).hexdigest())))
        return result.returncode
    body=json.loads(result.stdout)
    assert body['production_writes']==body['provider_calls']==body['model_calls']==0
    print(json.dumps(dict(body,remote_code_sha256=hashlib.sha256(REMOTE.encode()).hexdigest(),
        host_pin_strict=True),ensure_ascii=True,sort_keys=True))
    return 0


if __name__=='__main__':sys.exit(main())
