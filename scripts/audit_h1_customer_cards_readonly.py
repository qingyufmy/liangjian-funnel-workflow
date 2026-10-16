"""Read already-sent, sanitized card evidence; no Lark calls or corrections."""
import datetime, hashlib, json, pathlib, subprocess, sys
from zoneinfo import ZoneInfo
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.stdout.reconfigure(encoding='utf-8')
CODE=r'''
import datetime,hashlib,json,pathlib,sqlite3,subprocess
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
r=pathlib.Path.cwd()
assert str(r)=='/www1/Agu/liangjian-funnel-workflow'
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head=='2b077f393321c1858f17546682191caeb8eaa41f'
now=datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
assert now.date().isoformat()=='2026-10-16'
s=Settings.from_env(root=r)
archive_root=(s.state_db_path.parent/'lark_cards').resolve()
c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');c.execute('BEGIN')
rows=c.execute("SELECT delivery_id,delivery_key,kind,source_id,status,title,sent_at,created_at,payload_json FROM notification_deliveries WHERE kind IN ('A4_EFFECTIVE','A4_EXECUTION_RESULT') AND created_at>=? ORDER BY created_at LIMIT 2000",('2026-10-16T11:16:00+08:00',)).fetchall()
out={'actual_at':now.isoformat(),'head':head,'production_writes':0,'notification_calls':0,'rows':[]}
for row in rows:
 meta=json.loads(row['payload_json']);item={k:row[k] for k in row.keys() if k!='payload_json'}
 item['archive_status']='MISSING_REFERENCE'
 p=pathlib.Path(str(meta.get('card_evidence_path') or '')).resolve()
 if p.is_relative_to(archive_root) and p.is_file() and p.stat().st_size<=256*1024:
  raw=p.read_bytes();body=json.loads(raw)
  canonical=json.dumps(body['card'],ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
  item.update({'archive_status':'PRESENT','file_sha256':hashlib.sha256(raw).hexdigest(),
   'body_hash_match':hashlib.sha256(canonical).hexdigest()==meta.get('sanitized_card_sha256')==body.get('sanitized_card_sha256'),
   'delivery_key_hash_match':hashlib.sha256(row['delivery_key'].encode()).hexdigest()==body.get('delivery_key_hash'),
   'sanitized_not_wire_bytes':body.get('sanitized_not_wire_bytes'),'body':body})
 out['rows'].append(item)
c.rollback();c.close()
print(json.dumps(out,ensure_ascii=True))
'''
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
out=ROOT/'artifacts/h1-20261016';out.mkdir(parents=True,exist_ok=True)
prefix=at.strftime('%Y%m%dT%H%M%S%f')+'-customer-cards-readonly'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8',
 'aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && sudo -u www .venv/bin/python -B -'],input=CODE.encode(),capture_output=True,timeout=20)
for suffix,raw in [('stdout.json',p.stdout),('stderr.log',p.stderr)]:
 with (out/(prefix+'-'+suffix)).open('xb') as f:f.write(raw)
receipt={'actual_at':at.isoformat(),'exit_code':p.returncode,'remote_code_sha256':hashlib.sha256(CODE.encode()).hexdigest(),
 'stdout_sha256':hashlib.sha256(p.stdout).hexdigest(),'production_writes':0,'notification_calls':0}
with (out/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
if p.returncode:print(json.dumps(receipt));sys.exit(p.returncode)
body=json.loads(p.stdout)
for row in body['rows']:
 if '600621' in json.dumps(row,ensure_ascii=False):
  print(json.dumps(row,ensure_ascii=False))
print(json.dumps({'prefix':prefix,**receipt,'rows':len(body['rows'])}))
