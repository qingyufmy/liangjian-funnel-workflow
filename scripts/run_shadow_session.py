"""Independent local shadow consumer; no scheduler, provider or production discovery."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import time
import sys

# Bind this explicit checkout, not an unrelated editable install in the venv.
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))

from liangjian_funnel.runtime.shadow_evidence import ShadowEvidenceLedger
from liangjian_funnel.runtime.shadow_session import ReadOnlyShadowSource, ShadowSession, validate_paths


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-db',type=Path,required=True)
    parser.add_argument('--minute-db',type=Path,required=True)
    parser.add_argument('--monitor-latest',type=Path,help='Explicit original atomic A4 completion file; no path discovery')
    parser.add_argument('--preopen-inputs-db',type=Path,help='Explicit independent preopen sidecar; read-only lookup only')
    parser.add_argument('--lane',action='append',required=True)
    parser.add_argument('--shadow-db',type=Path,required=True)
    parser.add_argument('--shadow-jsonl',type=Path,required=True)
    parser.add_argument('--receipt-jsonl',type=Path,required=True,help='Unique NEW independent receipt file')
    parser.add_argument('--poll-seconds',type=float,default=.5)
    parser.add_argument('--duration-seconds',type=float,default=60,help='Bounded standalone lifetime; no historical catch-up')
    parser.add_argument('--budget-seconds',type=float,default=5)
    parser.add_argument('--wait-seconds',type=float,default=5)
    parser.add_argument('--max-lateness-seconds',type=float,default=15)
    args = parser.parse_args(argv)
    try:
        import math
        if (not math.isfinite(args.duration_seconds) or not 0 < args.duration_seconds <= 86400
                or not math.isfinite(args.poll_seconds) or not .1 <= args.poll_seconds <= 5):
            raise ValueError('STANDALONE_DURATION_OR_POLL_INVALID')
        paths = validate_paths(args.state_db,args.minute_db,args.shadow_db,args.shadow_jsonl)
        receipt_path = args.receipt_jsonl.resolve()
        if receipt_path.exists() or receipt_path in paths:
            raise ValueError('UNIQUE_INDEPENDENT_RECEIPT_REQUIRED')
        if args.monitor_latest is not None:
            marker=args.monitor_latest.resolve()
            if any(marker==path or marker.exists() and path.exists() and os.path.samefile(marker,path)
                    for path in (*paths,receipt_path)):
                raise ValueError('COMPLETION_AND_OUTPUT_PATH_ALIAS_REFUSED')
        # Validate budgets before ledger construction can create an output DB.
        for value in (args.budget_seconds,args.wait_seconds,args.max_lateness_seconds):
            if not math.isfinite(value): raise ValueError('FINITE_BUDGET_REQUIRED')
        if not 0 < args.budget_seconds <= 5 or not 0 <= args.wait_seconds <= 5 or not 0 < args.max_lateness_seconds <= 30:
            raise ValueError('SHADOW_BUDGET_OR_WAIT_INVALID')
        now=datetime.now().astimezone()
        preopen_kwargs={}
        if args.preopen_inputs_db is not None:
            sidecar=args.preopen_inputs_db.resolve()
            if not sidecar.is_file() or any(sidecar==path or path.exists() and os.path.samefile(sidecar,path)
                    for path in (*paths,receipt_path)) or args.monitor_latest is not None and sidecar==args.monitor_latest.resolve():
                raise ValueError('EXPLICIT_DISTINCT_PREOPEN_INPUTS_SOURCE_REQUIRED')
            from liangjian_funnel.runtime.shadow_preopen_sidecar import PreopenInputsLedger
            preopen_kwargs['preopen_inputs_provider']=PreopenInputsLedger(sidecar).lookup
        source=ReadOnlyShadowSource(paths[0],paths[1],lanes=args.lane,monitor_latest=args.monitor_latest,
            checkout_root=Path(__file__).resolve().parents[1],**preopen_kwargs)
        ledger=ShadowEvidenceLedger(paths[2],paths[3])
        service=ShadowSession(source,ledger=ledger,started_at=now,budget_seconds=args.budget_seconds,
            wait_seconds=args.wait_seconds,max_lateness_seconds=args.max_lateness_seconds)
        # No PIT/bar network adapter is installed implicitly. Default receipts
        # are DATA_LIMITED and cannot claim simulated fills or live acceptance.
        with receipt_path.open('x',encoding='utf-8') as output:
            deadline=time.monotonic()+args.duration_seconds
            while time.monotonic()<deadline:
                receipt=service.poll(observed_at=datetime.now().astimezone())
                output.write(json.dumps(receipt,ensure_ascii=False,allow_nan=False)+'\n')
                output.flush()
                if receipt['status'] not in ('DUPLICATE','WAITING_COMPLETE_SCOPE','WAITING_PRODUCTION_COMPLETION'):
                    print(json.dumps({'status':receipt['status'],'minute':receipt.get('minute'),
                        'actual_execution_authorized':False},ensure_ascii=False))
                time.sleep(min(args.poll_seconds,max(0,deadline-time.monotonic())))
        return 0
    except KeyboardInterrupt:
        print(json.dumps({'status':'STOPPED','actual_execution_authorized':False})); return 130
    except Exception:
        print(json.dumps({'status':'DATA_LIMITED','error_code':'SHADOW_SESSION_SETUP_OR_RECEIPT_FAILED',
            'actual_execution_authorized':False})); return 2


if __name__=='__main__':
    raise SystemExit(main())
