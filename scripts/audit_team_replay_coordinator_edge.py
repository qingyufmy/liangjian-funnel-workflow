"""Selected frozen source -> actual minute coordinator, zero models/orders.

This is NOT the continuous six-day funded replay: selected minutes deliberately
remain labelled as a component slice; missing PM/A6 input grants no permission.
"""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.evaluation.team_v2_replay_adapter import FrozenReplayAdapter
from liangjian_funnel.evaluation.team_v2_replay_batches import read_sealed_source,chronological_batches
from liangjian_funnel.evaluation.team_v2_replay_materializer import FrozenMinuteMaterializer
from liangjian_funnel.runtime.broker.local_paper import create_broker
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.team_v2_control_minute import run_control_minute
from liangjian_funnel.runtime.team_v2_sources import stamp
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore


def pin(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-pack',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--decision-at',action='append',required=True)
    args=parser.parse_args();output=args.output.absolute()
    allowed=(ROOT/'artifacts/team-v2-20261016').resolve()
    if (output.resolve()!=output or output.exists() or not output.is_relative_to(allowed)
            or any(p.is_symlink() or getattr(p,'is_junction',lambda:False)() for p in (output,*output.parents))):
        raise ValueError('REPLAY_NEW_OWNED_OUTPUT_REQUIRED')
    requested=[stamp(at).isoformat() for at in args.decision_at]
    if len(set(requested))!=len(requested):raise ValueError('REPLAY_DUPLICATE_SLICE')
    started=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    paths=[Path(__file__),*(ROOT/'src/liangjian_funnel/evaluation'/name for name in (
        'team_v2_replay_adapter.py','team_v2_replay_batches.py','team_v2_replay_materializer.py')),
        ROOT/'src/liangjian_funnel/runtime/team_v2_control_minute.py']
    hashes={str(p.relative_to(ROOT)):pin(p) for p in paths}
    head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    receipt,records,plans=read_sealed_source(args.source_pack)
    batches=[row for row in chronological_batches(records,plans) if row['decision_at'] in requested]
    if {row['decision_at'] for row in batches}!=set(requested):raise ValueError('REPLAY_SLICE_NOT_SCHEDULED')
    output.mkdir(parents=True)
    clock=[stamp(batches[0]['trade_date']+'T08:33:00+08:00')]
    broker=create_broker('local_paper',database=output/'paper.db',paper_config=ROOT/'config/paper_broker.yaml',
        risk_config=ROOT/'config/risk_limits.yaml',clock=lambda:clock[0],calendar=ExchangeTradingCalendar(),run_id='source-edge-component')
    store=TeamMeetingStore(output/'team.db');rows=[];opened=set()
    with FrozenMinuteMaterializer(receipt['source_path'],expected_sha256=receipt['source_database_sha256'],
            plans=plans,settings=SimpleNamespace(root=ROOT)) as reader:
        adapter=FrozenReplayAdapter(reader,broker=broker,store=store,clock=lambda:clock[0],account_id='replay-paper')
        for batch in batches:
            day=batch['trade_date']
            if day not in opened:
                clock[0]=stamp(day+'T08:33:00+08:00');broker.open_risk_day(day);opened.add(day)
            clock[0]=stamp(batch['decision_at'])
            invocation=adapter(batch,broker,store)
            result=run_control_minute(session_kwargs=invocation['session_kwargs'],closed_bars=invocation['closed_bars'],
                deadline_monotonic=time.monotonic()+47,model_enabled=invocation['model_enabled'])
            reconciliation=broker.reconcile()
            if broker.get_orders(day) or result['trader'] is not None or reconciliation['status']!='MATCH':
                raise ValueError('REPLAY_COMPONENT_UNEXPECTED_MODEL_ORDER_OR_MISMATCH')
            rows.append(dict(decision_at=batch['decision_at'],typed_plans=len(invocation['session_kwargs']['all_plans']),
                original_frames=len(batch['source_frames']),symbols=len(invocation['closed_bars']),
                source_audit=invocation['source_audit'],result=result,reconciliation=reconciliation['status']))
        snapshot_reads=reader.snapshot_reads
    result=dict(actual_started_at=started,actual_completed_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=head,source_hashes=hashes,source_files_unchanged=all(pin(ROOT/p)==value for p,value in hashes.items()),
        status='SELECTED_SOURCE_TO_COORDINATOR_COMPONENT_EXECUTED',
        source_database_sha256=receipt['source_database_sha256'],source_archive_sha256=receipt['archive_sha256'],
        sampled_minutes=len(rows),snapshot_reads=snapshot_reads,minutes=rows,
        model_calls=0,orders=0,source_requests=0,production_writes=0,
        continuous_six_day_gate='NOT_RUN',funded_path='NOT_RUN_NO_PM_A6_AUTHORITY',returns='NOT_CLAIMED')
    with (output/'COORDINATOR_EDGE.json').open('x',encoding='utf-8',newline='\n') as stream:
        json.dump(result,stream,ensure_ascii=False,indent=2)
    print(json.dumps(dict(status=result['status'],sampled_minutes=len(rows),snapshot_reads=snapshot_reads,
        receipt_sha256=pin(output/'COORDINATOR_EDGE.json'),models=0,orders=0)))
    return 0


if __name__=='__main__':sys.exit(main())
