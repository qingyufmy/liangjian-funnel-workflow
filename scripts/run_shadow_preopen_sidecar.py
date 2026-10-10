"""Explicit bounded local-cache preopen sidecar, never publish or acquire data."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from liangjian_funnel.runtime.shadow_preopen_sidecar import (
    ReadOnlyLocalFactCache,PreopenInputsLedger,generate_from_local_cache)


def main(argv=None, *, clock=lambda:datetime.now().astimezone()):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-db',type=Path,required=True)
    parser.add_argument('--fact-db',type=Path,required=True)
    parser.add_argument('--preopen-inputs-db',type=Path,required=True)
    parser.add_argument('--receipt-json',type=Path,required=True)
    parser.add_argument('--target-trade-date',type=date.fromisoformat,required=True)
    parser.add_argument('--lane',action='append',required=True)
    args=parser.parse_args(argv)
    try:
        paths=[p.resolve() for p in (args.state_db,args.fact_db,args.preopen_inputs_db,args.receipt_json)]
        if not all(p.is_file() for p in paths[:2]) or paths[3].exists():
            raise ValueError('EXPLICIT_SOURCE_AND_NEW_RECEIPT_REQUIRED')
        for index,path in enumerate(paths):
            if any(path==other or path.exists() and other.exists() and os.path.samefile(path,other) for other in paths[:index]):
                raise ValueError('DISTINCT_SOURCE_AND_SIDECAR_PATHS_REQUIRED')
        # No writable LocalFactCache constructor, Settings, provider or RuntimeStore.
        cache=ReadOnlyLocalFactCache(paths[1]); ledger=PreopenInputsLedger(paths[2])
        marks=','.join('?' for _ in args.lane)
        with closing(sqlite3.connect(paths[0].as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
            db.row_factory=sqlite3.Row; db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
            rows=[dict(r) for r in db.execute(f'SELECT * FROM execution_plans WHERE lane_id IN ({marks}) AND status IN ("ACTIVE_TODAY","PENDING_MORNING_REVIEW") AND expires_at LIKE ? ORDER BY plan_id LIMIT 1001',
                (*args.lane,args.target_trade_date.isoformat()+'%'))]
        if len(rows)>1000:
            raise ValueError('PREOPEN_SCOPE_BOUND_EXCEEDED')
        records=[]; skipped=[]
        for row in rows:
            plan=json.loads(row['payload_json']); facts=plan.get('strategy_facts')
            if isinstance(facts,dict) and 'shadow_inputs' in facts:
                skipped.append({'plan_id':row['plan_id'],'inputs_origin':'PLAN_FROZEN'}); continue
            try:
                record=generate_from_local_cache(row,cache=cache,target_trade_date=args.target_trade_date,
                    generated_at=clock(),clock=clock)
            except Exception:
                # Retain the per-plan failure, do not erase the requested scope
                # or substitute another cache/source/run.
                from liangjian_funnel.runtime.shadow_preopen_sidecar import generate_preopen_sidecar
                actual=clock()
                record=generate_preopen_sidecar(plan_row=row,rows=[],target_trade_date=args.target_trade_date,
                    generated_at=actual,source_as_of=actual,bar_cutoff=actual,expected_close_dates=[])
            written=ledger.record(record)
            records.append({'plan_id':row['plan_id'],'status':record['status'],
                'record_hash':record['record_hash'],'generated_at':record['generated_at'],
                'reason_codes':record['reason_codes'],'write_receipt':written})
        qualified=sum(r['status']=='AVAILABLE' and r['write_receipt']['ok'] for r in records)
        receipt={'schema_version':'shadow-preopen-run/1','target_trade_date':args.target_trade_date.isoformat(),
            'selected_plan_count':len(rows),'available_count':qualified,'records':records,'skipped':skipped,
            'source_plan_payload_sha256':{r['plan_id']:hashlib.sha256(r['payload_json'].encode()).hexdigest() for r in rows},
            'inputs_origin':'SHADOW_PREOPEN_SIDECAR','actual_execution_authorized':False,
            'status':'COMPLETE' if qualified and all(r['status']=='AVAILABLE' and r['write_receipt']['ok'] for r in records) else 'DATA_LIMITED'}
        paths[3].parent.mkdir(parents=True,exist_ok=True)
        with paths[3].open('x',encoding='utf-8') as output:
            json.dump(receipt,output,ensure_ascii=False,allow_nan=False,indent=2)
        print(json.dumps({'status':receipt['status'],'available_count':qualified,'actual_execution_authorized':False}))
        return 0 if receipt['status']=='COMPLETE' else 2
    except Exception:
        print(json.dumps({'status':'DATA_LIMITED','error_code':'PREOPEN_SIDECAR_FAILED','actual_execution_authorized':False})); return 2


if __name__=='__main__':
    raise SystemExit(main())
