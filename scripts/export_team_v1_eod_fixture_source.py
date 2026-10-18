"""0315 bounded readonly original v1 paper rows, never a Team account backfill."""
import argparse,base64,hashlib,json,subprocess,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'scripts')]
from run_team_a5_frozen_dryrun import save

REMOTE=r'''
import json,sqlite3,subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
assert Path.cwd()==Path('/www1/Agu/liangjian-funnel-workflow')
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head=='2b077f393321c1858f17546682191caeb8eaa41f'
s=Settings.from_env(root=Path.cwd());db=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
queries={
 'accounts':('SELECT * FROM virtual_accounts',()),
 'positions':('SELECT * FROM virtual_positions',()),
 'account_days':('SELECT * FROM account_trading_days',()),
 'marks':('SELECT * FROM virtual_marks',()),
 'fills':('SELECT * FROM virtual_fills WHERE bar_end<? ORDER BY bar_end,fill_id',('2026-10-17',)),
 'risk_events':("SELECT * FROM monitor_events WHERE minute_end>=? AND minute_end<? AND (action IN ('SELL_SIGNAL','REDUCE_SIGNAL') OR reason_code LIKE '%STOP%' OR reason_code LIKE '%HALT%') ORDER BY minute_end,event_id",('2026-10-14','2026-10-17')),
 'plan_rows':('SELECT plan_id,symbol,payload_json FROM execution_plans WHERE plan_id IN (SELECT plan_id FROM virtual_positions)',())}
tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
result={}
for key,(sql,args) in queries.items():
 if key=='marks' and 'virtual_marks' not in tables:
  result[key]=dict(status='TABLE_UNAVAILABLE');continue
 result[key]=[dict(r) for r in db.execute(sql,args)]
db.rollback();db.close()
print(json.dumps(dict(classification='ORIGINAL_V1_PAPER_FIXTURE_SOURCE_ONLY',head=head,
 actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),source_database=str(s.state_db_path),
 rows=result,production_writes=0,models=0,provider_calls=0),ensure_ascii=False))
'''

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    config=subprocess.check_output(['ssh','-G','aurum-vm'],text=True,stderr=subprocess.DEVNULL)
    if 'hostname 192.168.0.254' not in config.splitlines():raise ValueError('SSH_HOST_INVALID')
    known=subprocess.check_output(['ssh-keygen','-F','192.168.0.254'],text=True)
    pins={base64.b64encode(hashlib.sha256(base64.b64decode(r.split()[2])).digest()).decode().rstrip('=')
        for r in known.splitlines() if r and not r.startswith('#')}
    if 'LnPSC/kfZ7jQtzj55siqgWOkW0xy/6cGyoAwglS6yQ0' not in pins:raise ValueError('SSH_PIN_INVALID')
    command=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm',
        'cd /www1/Agu/liangjian-funnel-workflow && runuser -u www -- nice -n 10 .venv/bin/python -B -']
    output.mkdir(parents=True);started=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    with (output/'SOURCE.json').open('xb') as out,(output/'ssh.stderr.log').open('xb') as err:
        result=subprocess.run(command,input=REMOTE.encode(),stdout=out,stderr=err,timeout=35)
    save(output/'COMMAND.json',dict(command=command,actual_started_at=started,
        actual_finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),exit_code=result.returncode))
    if result.returncode:raise ValueError('READONLY_V1_EXPORT_FAILED')
    raw=(output/'SOURCE.json').read_bytes();body=json.loads(raw)
    save(output/'RECEIPT.json',dict(status='SEALED_FIXTURE_NOT_TEAM_ACCOUNT',sha256=hashlib.sha256(raw).hexdigest(),
        counts={k:len(v) if isinstance(v,list) else v for k,v in body['rows'].items()},production_writes=0))
    print(json.dumps(dict(status='SEALED_FIXTURE_NOT_TEAM_ACCOUNT',source_sha256=hashlib.sha256(raw).hexdigest(),
        counts={k:len(v) if isinstance(v,list) else v for k,v in body['rows'].items()})))
    return 0

if __name__=='__main__':sys.exit(main())
