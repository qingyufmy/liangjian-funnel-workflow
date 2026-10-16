"""Seal original 10-16 11:15--11:21 snapshots locally without changing the VM."""
import datetime,hashlib,json,pathlib,subprocess,sys
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8')
ROOT=pathlib.Path(__file__).resolve().parents[1]
CODE=r'''
import datetime,hashlib,json,pathlib,sqlite3,subprocess,zlib
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
r=pathlib.Path.cwd();assert str(r)=='/www1/Agu/liangjian-funnel-workflow'
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));assert str(at.date())=='2026-10-16'
s=Settings.from_env(root=r);c=sqlite3.connect((s.minute_cache_dir/'minute_bars.sqlite3').resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');c.execute('BEGIN')
out={'actual_at':at.isoformat(),'production_writes':0,'model_calls':0,'source_calls':0,'snapshots':[]}
for row in c.execute('SELECT * FROM minute_decision_snapshots WHERE symbol=? AND decision_as_of>=? AND decision_as_of<=? ORDER BY decision_as_of,interval',('600621.SH','2026-10-16T11:15:00+08:00','2026-10-16T11:21:00+08:00')):
 raw=zlib.decompress(row['payload_zlib']);assert hashlib.sha256(raw).hexdigest()==row['payload_sha256']
 out['snapshots'].append({**{k:row[k] for k in row.keys() if k!='payload_zlib'},'original_payload_utf8':raw.decode('utf-8')})
c.rollback();c.close();print(json.dumps(out,ensure_ascii=True))
'''
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));out=ROOT/'artifacts/h1-20261016';out.mkdir(parents=True,exist_ok=True)
prefix=at.strftime('%Y%m%dT%H%M%S%f')+'-frozen-window'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && .venv/bin/python -B -'],input=CODE.encode(),capture_output=True,timeout=20)
for suffix,raw in [('stdout.json',p.stdout),('stderr.log',p.stderr)]:
 with (out/(prefix+'-'+suffix)).open('xb') as f:f.write(raw)
receipt={'actual_at':at.isoformat(),'exit_code':p.returncode,'remote_code_sha256':hashlib.sha256(CODE.encode()).hexdigest(),'stdout_sha256':hashlib.sha256(p.stdout).hexdigest()}
with (out/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
if p.returncode:print(p.stderr.decode(errors='replace')[-1000:]);sys.exit(p.returncode)
body=json.loads(p.stdout)
print(json.dumps({'prefix':prefix,**receipt,'snapshots':len(body['snapshots']),'window_ends':[{'as_of':x['decision_as_of'],'interval':x['interval'],'bars':len(json.loads(x['original_payload_utf8']))} for x in body['snapshots']]}))
