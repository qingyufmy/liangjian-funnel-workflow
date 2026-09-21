"""VM-external A4 watchdog. One invocation = one bounded read-only probe."""
from __future__ import annotations

import argparse
import json
import inspect
import subprocess
from datetime import datetime
from pathlib import Path

from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.lark import LarkNotifier
from liangjian_funnel.runtime.watchdog import TZ, WatchdogLedger, assess, completed_minutes_from_log

REMOTE_PROBE = '''
import json,sqlite3,urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
root=Path.cwd();s=Settings.from_env(root=root);day=datetime.now(ZoneInfo('Asia/Shanghai')).date().isoformat()
db=sqlite3.connect(f'file:{s.state_db_path}?mode=ro',uri=True,timeout=3);db.row_factory=sqlite3.Row
minutes=[r[0] for r in db.execute('select distinct minute_end from monitor_events where minute_end>=? and minute_end<?',(day,day+'T15:01'))]
recent_minutes=[r[0] for r in db.execute('select distinct minute_end from monitor_events where minute_end>=? and minute_end<? order by minute_end desc limit 3',(day,day+'T15:01'))]
minute_health=[]
for stamp in recent_minutes:
 rows=[dict(r) for r in db.execute('select action,reason_code,payload_json from monitor_events where minute_end=?',(stamp,))]
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
if log.exists():minutes=sorted(set(minutes)|set(completed_minutes_from_log(log.read_text(errors='replace'))))
leases=[dict(r) for r in db.execute('select lease_name,state,last_dispatch_key from scheduler_leases')]
db.close()
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
                  'last_completed_minute':latest,'decision_health':decision_health}))
'''


def probe() -> dict:
    try:
        result = subprocess.run(
            ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','ServerAliveInterval=5',
             '-o','ServerAliveCountMax=1','aurum-vm',
             'cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -'],
            input=(inspect.getsource(completed_minutes_from_log)+'\n'+REMOTE_PROBE).encode(),capture_output=True,timeout=20,check=True,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0),
        )
        return json.loads(result.stdout)
    except Exception:
        # Never persist stderr: SSH/environment diagnostics can contain secrets.
        return {'reachable':False,'probe_reason':'REMOTE_PROBE_FAILED'}


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
