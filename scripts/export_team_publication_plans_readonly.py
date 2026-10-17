"""Copy bounded original published plan rows for the three frozen role days.

Production reads one SQLite transaction only. The new local SQLite file is an
evidence copy, not a business DB; no model/provider/notification is called.
"""
import argparse
import base64
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
REMOTE=r'''
import hashlib,json,sqlite3,subprocess,time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from contextlib import closing
from liangjian_funnel.settings import Settings
root=Path.cwd();now=datetime.now(ZoneInfo('Asia/Shanghai'))
assert now.date().isoformat()=='2026-10-17'
assert root==Path('/www1/Agu/liangjian-funnel-workflow')
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head=='2b077f393321c1858f17546682191caeb8eaa41f'
s=Settings.from_env(root=root);deadline=time.monotonic()+10
with closing(sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
 db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000);db.execute('BEGIN')
 columns=[dict(r) for r in db.execute('PRAGMA table_info(execution_plans)')]
 rows=[dict(r) for r in db.execute("SELECT * FROM execution_plans WHERE lane_id='lane_1' AND substr(expires_at,1,10) IN ('2026-10-15','2026-10-16','2026-10-19') ORDER BY plan_id LIMIT 101")]
 assert len(rows)<=100
 db.rollback()
encoded=json.dumps(rows,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
print(json.dumps(dict(actual_collected_at=now.isoformat(),head=head,columns=columns,rows=rows,rows_sha256=hashlib.sha256(encoded).hexdigest(),production_writes=0,provider_calls=0,model_calls=0),ensure_ascii=True))
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    output=args.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_DIRECTORY_REQUIRED')
    config=subprocess.check_output(['ssh','-G','aurum-vm'],text=True,stderr=subprocess.DEVNULL)
    if 'hostname 192.168.0.254' not in config.splitlines():raise ValueError('SSH_HOST_INVALID')
    known=subprocess.check_output(['ssh-keygen','-F','192.168.0.254'],text=True)
    pins={base64.b64encode(hashlib.sha256(base64.b64decode(r.split()[2])).digest()).decode().rstrip('=') for r in known.splitlines() if r and not r.startswith('#')}
    if 'LnPSC/kfZ7jQtzj55siqgWOkW0xy/6cGyoAwglS6yQ0' not in pins:raise ValueError('HOST_PIN_INVALID')
    output.mkdir(parents=True)
    cmd=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm',
         'cd /www1/Agu/liangjian-funnel-workflow && runuser -u www -- .venv/bin/python -B -']
    proc=subprocess.run(cmd,input=REMOTE.encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=25)
    with (output/'command.json').open('x') as f:json.dump(dict(command=cmd,exit_code=proc.returncode,actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()),f)
    with (output/'ssh.stderr.log').open('xb') as f:f.write(proc.stderr)
    with (output/'SOURCE.json').open('xb') as f:f.write(proc.stdout)
    if proc.returncode:raise ValueError('READONLY_PLAN_EXPORT_FAILED')
    body=json.loads(proc.stdout);raw=json.dumps(body['rows'],ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
    if hashlib.sha256(raw).hexdigest()!=body['rows_sha256']:raise ValueError('PLAN_ROW_HASH_INVALID')
    columns=body['columns']
    if any(not c['name'].replace('_','').isalnum() or c['type'] not in {'TEXT','INTEGER','REAL','BLOB','NUMERIC',''} for c in columns):raise ValueError('PLAN_SCHEMA_INVALID')
    path=output/'frozen.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE execution_plans ('+','.join('"'+c['name']+'" '+c['type'] for c in columns)+')')
        db.executemany('INSERT INTO execution_plans VALUES ('+','.join('?' for _ in columns)+')',
            [[r[c['name']] for c in columns] for r in body['rows']])
    counts={day:sum(r['expires_at'].startswith(day) for r in body['rows']) for day in ('2026-10-15','2026-10-16','2026-10-19')}
    result=dict(status='ORIGINAL_ROWS_COPIED_READONLY',actual_collected_at=body['actual_collected_at'],counts=counts,
        rows_sha256=body['rows_sha256'],source_json_sha256=hashlib.sha256(proc.stdout).hexdigest(),
        local_db_sha256=hashlib.file_digest(path.open('rb'),'sha256').hexdigest(),exit_code=0,production_writes=0)
    with (output/'EXPORT_RECEIPT.json').open('x') as f:json.dump(result,f,indent=2)
    print(json.dumps(result));return 0


if __name__=='__main__':raise SystemExit(main())
