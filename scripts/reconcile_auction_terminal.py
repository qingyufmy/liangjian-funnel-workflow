"""Repair a stale auction progress receipt from a confirmed parent termination.

Default read-only. Does not rerun research, replace plans, or modify raw data.
"""
import argparse
import json
import subprocess
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime.state import RuntimeStore


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day',type=date.fromisoformat,required=True)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    now=datetime.now(ZoneInfo('Asia/Shanghai'))
    if now.date()<args.day or (now.date()==args.day and now.strftime('%H:%M')<'15:35'):
        raise SystemExit('NON_TRADING_RELEASE_WINDOW_REQUIRED')
    s=Settings.from_env(root=Path.cwd())
    processes=subprocess.check_output(['ps','-eo','args'],text=True)
    if any('-m liangjian_funnel run-auction-refresh' in line for line in processes.splitlines()):
        raise SystemExit('AUCTION_PROCESS_ACTIVE')
    receipt_path=s.workflow_output_dir/'runs'/f'{args.day}-auction-refresh.json'
    receipt=json.loads(receipt_path.read_text())
    if receipt.get('status')!='RUNNING':
        print(json.dumps({'status':'NOOP','reason':'ALREADY_TERMINAL'}));return
    start=datetime.fromisoformat(receipt['started_at'])
    progress_path=s.workflow_output_dir/'auction_progress'/f'{args.day}-auction-refresh-{start:%H%M%S}.json'
    progress=json.loads(progress_path.read_text())
    term=[]
    for line in (s.workflow_output_dir/'node'/f'node-{args.day}.jsonl').open():
        row=json.loads(line)
        if (row.get('job')=='auction-refresh' and row.get('stream')=='node'
                and '任务结束 run-auction-refresh status=terminated' in row.get('message','')
                and datetime.fromisoformat(row['timestamp'].replace('Z','+00:00'))>=start):term.append(row)
    if len(term)!=1:raise SystemExit('PARENT_TERMINATION_NOT_UNIQUE')
    evidence={'parent_run_id':term[0]['runId'],'terminated_at':term[0]['timestamp'],
              'repair_at':now.isoformat(),'no_research_rerun':True,'execution_publication':'UNCHANGED'}
    print(json.dumps(evidence))
    if not args.apply:return
    store=RuntimeStore(s.state_db_path)
    lease=store.get_lease('scheduler:auction-refresh')
    if lease and (lease.get('owner')!='auction-refresh' or lease.get('dispatch_key')!=f'auction-refresh:{args.day}'):
        raise SystemExit('LEASE_IDENTITY_CHANGED')
    backup=s.workflow_output_dir/'recovery'/f'auction-terminal-{args.day}'
    if backup.exists():raise SystemExit('RECOVERY_BACKUP_ALREADY_EXISTS')
    atomic_write_json(backup/'before.json',{'receipt':receipt,'progress':progress,'lease':lease,'evidence':evidence})
    for path,payload in ((receipt_path,receipt),(progress_path,progress)):
        payload.update(status='BLOCKED',reason_code='AUCTION_REFRESH_PARENT_TIMEOUT',
                       finished_at=term[0]['timestamp'],reconciliation=evidence)
        if path==progress_path:payload.update(job_status='BLOCKED',phase='FAILED',eta_seconds=None)
        atomic_write_json(path,payload)
    if lease:store.release_lease('scheduler:auction-refresh','auction-refresh')
    atomic_write_json(backup/'result.json',evidence)


if __name__=='__main__':main()
