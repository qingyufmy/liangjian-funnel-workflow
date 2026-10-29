"""VM-external A4 watchdog. One invocation = one bounded read-only probe."""
from __future__ import annotations

import argparse
import json
import inspect
import subprocess
import re
import shlex
import hashlib
from datetime import datetime
from pathlib import Path

from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.lark import LarkNotifier
from liangjian_funnel.runtime.watchdog import TZ, WatchdogLedger, assess, completed_minutes_from_log

REMOTE_PROBE = '''
import json,sqlite3,urllib.request,time
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
root=Path.cwd();s=Settings.from_env(root=root);day=datetime.now(ZoneInfo('Asia/Shanghai')).date().isoformat()
db=sqlite3.connect(f'file:{s.state_db_path}?mode=ro',uri=True,timeout=3);db.row_factory=sqlite3.Row
db.execute('pragma query_only=ON');db.execute('pragma cache_size=-2048');db.execute('pragma mmap_size=0')
readonly_deadline=time.monotonic()+5;db.set_progress_handler(lambda:int(time.monotonic()>readonly_deadline),2000)
lane=s.research_primary_lane_id
minutes=[r[0] for r in db.execute('select distinct minute_end from monitor_events where lane_id=? and minute_end>=? and minute_end<?',(lane,day,day+'T15:01'))]
recent_minutes=[r[0] for r in db.execute('select distinct minute_end from monitor_events where lane_id=? and minute_end>=? and minute_end<? order by minute_end desc limit 3',(lane,day,day+'T15:01'))]
minute_health=[]
for stamp in recent_minutes:
 rows=[dict(r) for r in db.execute('select action,reason_code,payload_json from monitor_events where lane_id=? and minute_end=?',(lane,stamp))]
 scoped=[]
 for row in rows:
  try:payload=json.loads(row.get('payload_json') or '{}')
  except Exception:payload={}
  if payload.get('plan_id') or payload.get('symbol'):scoped.append(row)
 actions=Counter(str(row.get('action') or '') for row in scoped)
 reasons=Counter(str(row.get('reason_code') or '') for row in scoped if row.get('reason_code'))
 minute_health.append({'minute':stamp,'scope_count':len(scoped),'blocked_count':actions.get('DATA_BLOCK',0),
                       'all_scope_blocked':bool(scoped) and actions.get('DATA_BLOCK',0)==len(scoped),
                       'top_reason':reasons.most_common(1)[0][0] if reasons else None})
log=root/'outputs/node'/f'node-{day}.jsonl'
if log.exists():
 with log.open('rb') as source:
  source.seek(max(0,log.stat().st_size-4*1024*1024));tail=source.read(4*1024*1024).decode(errors='replace')
 minutes=sorted(set(minutes)|set(completed_minutes_from_log(tail)))
leases=[dict(r) for r in db.execute('select lease_name,state,last_dispatch_key from scheduler_leases')]
plan_rows=[dict(r) for r in db.execute(
 'select status,count(*) as count from execution_plans where substr(expires_at,1,10)=? '
 "and lane_id=? and status in ('ACTIVE_TODAY','PENDING_MORNING_REVIEW') group by status",
 (day,s.research_primary_lane_id))]
db.close()
plan_counts={row['status']:row['count'] for row in plan_rows}
clock=datetime.now(ZoneInfo('Asia/Shanghai'))
current_plan_count=(plan_counts.get('ACTIVE_TODAY',0)
 if (clock.hour,clock.minute)>=(9,31)
 else plan_counts.get('ACTIVE_TODAY',0)+plan_counts.get('PENDING_MORNING_REVIEW',0))
def run_receipt(name):
 try:
  row=json.loads((root/'outputs'/'runs'/name).read_text(encoding='utf-8'))
  return {'status':row.get('status'),'reason_code':row.get('reason_code')}
 except (OSError,ValueError,TypeError):return {}
premarket=run_receipt(day+'-a3-premarket.json')
auction=run_receipt(day+'-auction-refresh.json')
try:
 health=json.loads(urllib.request.urlopen('http://127.0.0.1:3210/api/health',timeout=3).read());healthy=health.get('status')=='ok'
except Exception:healthy=False
try:
 latest_doc=json.loads((root/'outputs/monitor/latest.json').read_text());latest=latest_doc.get('time')
except Exception:latest_doc={};latest=None
streak=0
for item in minute_health:
 if item['all_scope_blocked']:streak+=1
 else:break
obs=latest_doc.get('observability') or {};axes=obs.get('axes') or {}
decision_health={'all_scope_blocked_streak':streak,
 'latest_all_scope_blocked':bool(minute_health and minute_health[0]['all_scope_blocked']),
 'latest_reason':minute_health[0]['top_reason'] if minute_health else None,
 'observability_axes':axes,
 'reported_blocked_count':len(obs.get('blocked_scope') or []),
 'recent_minutes':minute_health}
print(json.dumps({'reachable':True,'app_healthy':healthy,'minutes':minutes,'leases':leases,
                  'last_completed_minute':latest,'decision_health':decision_health,
                  'plan_readiness':{'current_plan_count':current_plan_count,
                    'active_plan_count':plan_counts.get('ACTIVE_TODAY',0),
                    'pending_plan_count':plan_counts.get('PENDING_MORNING_REVIEW',0),
                    'premarket_status':premarket.get('status'),
                    'premarket_reason':premarket.get('reason_code'),
                    'auction_status':auction.get('status'),
                    'auction_reason':auction.get('reason_code')}}))
'''


def deployment_host():
    path=Path(__file__).resolve().parents[1]/'config/deployment_host.json'
    value=json.loads(path.read_text(encoding='utf-8'))
    root=value.get('production_root');alias=value.get('ssh_alias')
    if (value.get('schema_version')!='deployment-host/1' or not isinstance(root,str)
            or not root.startswith('/media/') or '..' in root.split('/')
            or not isinstance(alias,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}',alias)):
        raise ValueError('WATCHDOG_HOST_CONFIG_INVALID')
    return root,alias


class _WindowlessPinSubprocess:
    """Keep the audited pin checks, without consoles from a pythonw parent."""

    def __getattr__(self, name):
        return getattr(subprocess, name)

    def check_output(self, *args, **kwargs):
        kwargs['creationflags'] = (
            kwargs.get('creationflags', 0)
            | getattr(subprocess, 'CREATE_NO_WINDOW', 0)
        )
        return subprocess.check_output(*args, **kwargs)


def pinned_ssh_options():
    import importlib.util
    config=json.loads((Path(__file__).resolve().parents[1]/'config/deployment_host.json').read_text(encoding='utf8'))
    path=Path(config['ssh_pin_module'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=config['ssh_pin_module_sha256']:
        raise ValueError('WATCHDOG_PIN_MODULE_HASH_CONFLICT')
    spec=importlib.util.spec_from_file_location('watchdog_audited_pin',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    # Bind only the imported module; leave archived source and subprocess global.
    module.subprocess = _WindowlessPinSubprocess()
    return module.ssh_options()


def _probe_once() -> dict:
    phase='LOCAL_HOST_CONFIG'
    try:
        root,alias=deployment_host()
        phase='SSH_TRANSPORT_OR_REMOTE_RUNTIME'
        result = subprocess.run(
            ['ssh',*pinned_ssh_options(),'-o','ServerAliveInterval=5',
             '-o','ServerAliveCountMax=1',alias,
             'cd '+shlex.quote(root)+' && exec .venv/bin/python -'],
            input=(inspect.getsource(completed_minutes_from_log)+'\n'+REMOTE_PROBE).encode(),capture_output=True,timeout=20,check=True,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0),
        )
        phase='REMOTE_JSON_DECODE'
        value=json.loads(result.stdout)
        if not isinstance(value,dict) or value.get('reachable') is not True:
            raise ValueError('WATCHDOG_PROBE_SHAPE_INVALID')
        return value
    except Exception as exc:
        code=getattr(exc,'returncode',None);stderr=getattr(exc,'stderr',b'') or b''
        if isinstance(stderr,str):stderr=stderr.encode()
        if code==255:phase='SSH_TRANSPORT'
        elif code is not None:phase='REMOTE_RUNTIME'
        return dict(reachable=False,probe_reason='REMOTE_PROBE_FAILED',failure_stage=phase,
            exception_class=type(exc).__name__,exit_code=code,
            stderr_sha256=hashlib.sha256(stderr).hexdigest() if stderr else None)


def probe() -> dict:
    first=_probe_once()
    if first.get('reachable') and first.get('app_healthy'):
        return dict(first,probe_confirmation=dict(attempts=1,confirmed_failure=False))
    second=_probe_once()
    first_fact={k:first[k] for k in ('reachable','app_healthy','probe_reason','failure_stage','exception_class','exit_code','stderr_sha256') if k in first}
    return dict(second,probe_confirmation=dict(attempts=2,first=first_fact,
        confirmed_failure=not(second.get('reachable') and second.get('app_healthy'))))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir',type=Path,default=Path('state/a4_watchdog'))
    parser.add_argument('--webhook-file',type=Path,default=Path('state/a4_watchdog/lark_webhook.json'))
    parser.add_argument('--dry-run',action='store_true')
    parser.add_argument('--test-notification',action='store_true')
    args=parser.parse_args();now=datetime.now(TZ)
    try:
        trading_day=ExchangeTradingCalendar().is_trading_day(now.date())
        active=trading_day and 8*60+20<=now.hour*60+now.minute<=20*60+40
        snapshot=probe() if active else {}
        assessment=assess(now,snapshot,trading_day=trading_day)
    except Exception:
        assessment={'date':now.date().isoformat(),'checked_at':now.isoformat(),'reachable':False,
                    'problems':{'WATCHDOG_CALENDAR_UNAVAILABLE':'独立探针交易日历不可用，无法确认本日执行窗口。'},'gaps':[]}
    if args.dry_run:
        print(json.dumps(assessment,ensure_ascii=False));return
    settings=json.loads(args.webhook_file.read_text(encoding='utf-8'))
    notifier=LarkNotifier(settings['webhookUrl'],timeout_seconds=5)
    ledger=WatchdogLedger(args.state_dir/'watchdog.sqlite3')
    if args.test_notification:ledger.queue_test(now)
    result=ledger.process(assessment,notifier,now)
    atomic_write_json(args.state_dir/'latest.json',result)
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
