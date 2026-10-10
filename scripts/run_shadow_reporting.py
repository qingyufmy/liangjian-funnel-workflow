"""Independent W3 reporting timer target; explicit sources, no production jobs."""
from __future__ import annotations
import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from liangjian_funnel.runtime.shadow_reporting import ReportingPaths,run_reporting_tick,run_weekly_reporting


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('state-db','shadow-db','approved-output-root','node-log-root','archive-root','report-root','bridge-outbox'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--shadow-jsonl',type=Path)
    parser.add_argument('--node-receipt',type=Path,help='Original Node recentJobRuns DTO; no default source')
    parser.add_argument('--lane',action='append',required=True)
    parser.add_argument('--as-of',help='Explicit fixture one-tick clock only; cannot combine with --watch')
    parser.add_argument('--watch',action='store_true',help='Independent single process until final/16:45 fallback')
    parser.add_argument('--week-session',action='append',help='Five explicit consecutive sessions; independent Friday weekly tick')
    parser.add_argument('--poll-seconds',type=float,default=5)
    args=parser.parse_args(argv)
    try:
        import math
        if (args.watch and (args.as_of or args.week_session) or not math.isfinite(args.poll_seconds) or not 1<=args.poll_seconds<=30):
            raise ValueError
        now=datetime.fromisoformat(args.as_of) if args.as_of else datetime.now(ZoneInfo('Asia/Shanghai'))
        cfg=ReportingPaths(state_db=args.state_db,shadow_db=args.shadow_db,shadow_jsonl=args.shadow_jsonl,
            lanes=tuple(args.lane),approved_output_root=args.approved_output_root,node_receipt_path=args.node_receipt,
            node_log_path=args.node_log_root/f'node-{now.date()}.jsonl',archive_root=args.archive_root,
            report_root=args.report_root,bridge_outbox=args.bridge_outbox)
        while True:
            result=(run_weekly_reporting(cfg,trading_days=args.week_session,observed_at=now) if args.week_session
                else run_reporting_tick(cfg,observed_at=now,
                    observation_clock=None if args.as_of else lambda:datetime.now(ZoneInfo('Asia/Shanghai'))))
            # Only narrow receipt, never print A5/body/model/account payloads.
            print(json.dumps({k:v for k,v in result.items() if k!='draft'},ensure_ascii=False),flush=True)
            if not args.watch or result['status'] not in ('WAIT_DRAFT','WAIT_A5'):break
            fence=now.replace(hour=16,minute=45,second=0,microsecond=0)
            time.sleep(min(args.poll_seconds,max(.01,(fence-datetime.now(ZoneInfo('Asia/Shanghai'))).total_seconds())))
            now=datetime.now(ZoneInfo('Asia/Shanghai'))
        return 0 if result['status'] in ('FORMAL_WRITTEN','ALREADY_WRITTEN','WEEK_WRITTEN','NON_TRADING_DAY','WAIT_DRAFT','WAIT_A5') else 2
    except KeyboardInterrupt:
        print(json.dumps({'status':'SHADOW_STOPPED','production_mutation':False}));return 130
    except Exception:
        print(json.dumps({'status':'SHADOW_SETUP_FAILED','production_mutation':False}));return 2


if __name__=='__main__':raise SystemExit(main())
