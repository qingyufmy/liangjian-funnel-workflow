"""Sealed six-day source -> continuous owned account, twice, with no model.

Missing historical Team meetings remain unavailable. This proves source wiring
and deterministic account repeatability, NOT funded/model CONTROL gates.
"""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.evaluation.team_v2_replay_adapter import FrozenReplayAdapter
from liangjian_funnel.evaluation.team_v2_replay_batches import read_sealed_source,chronological_batches
from liangjian_funnel.evaluation.team_v2_replay_materializer import FrozenMinuteMaterializer
from liangjian_funnel.evaluation.team_v2_replay_passes import run_two_passes,file_hash
from liangjian_funnel.evaluation.team_v2_replay_timeline import execute_timeline
from liangjian_funnel.runtime.broker.local_paper import create_broker
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.team_v2_sources import stamp
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-pack',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();started=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    receipt,records,plans=read_sealed_source(args.source_pack)
    batches=tuple(chronological_batches(records,plans))
    paths=[Path(__file__),*(ROOT/'src/liangjian_funnel/evaluation'/name for name in (
        'team_v2_replay_adapter.py','team_v2_replay_batches.py','team_v2_replay_materializer.py',
        'team_v2_replay_passes.py','team_v2_replay_timeline.py')),
        ROOT/'src/liangjian_funnel/runtime/team_v2_control_minute.py']
    hashes={str(p.relative_to(ROOT)):file_hash(p) for p in paths}
    head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip();passes=[]
    def run_pass(work,journal,client):
        ordinal=len(passes)+1;clock=[stamp(batches[0]['trade_date']+'T08:33:00+08:00')]
        broker=create_broker('local_paper',database=work/'paper.db',paper_config=ROOT/'config/paper_broker.yaml',
            risk_config=ROOT/'config/risk_limits.yaml',clock=lambda:clock[0],
            calendar=ExchangeTradingCalendar(),run_id='sealed-source-contiguous-no-authority')
        store=TeamMeetingStore(work/'team.db');counts=Counter();reasons=Counter()
        def set_clock(at):clock[0]=at
        def morning(day,actual,owned):
            return dict(status='HISTORICAL_PM_A6_UNAVAILABLE_NO_ALLOCATION',model_calls=0)
        def close(day,actual,owned):
            if actual.get_positions() or actual.get_orders(day):
                raise ValueError('SOURCE_AUDIT_UNEXPECTED_EXPOSURE_OR_ORDER')
            return dict(status='NO_EXPOSURE_NO_CLOSE_MARK_CREATED')
        with FrozenMinuteMaterializer(receipt['source_path'],expected_sha256=receipt['source_database_sha256'],
                plans=plans,settings=SimpleNamespace(root=ROOT)) as reader:
            adapter=FrozenReplayAdapter(reader,broker=broker,store=store,clock=lambda:clock[0],account_id='replay-paper')
            def minute(batch,actual,owned):
                invocation=adapter(batch,actual,owned);audit=invocation['source_audit']
                counts['typed_plan_minutes']+=len(invocation['session_kwargs']['all_plans'])
                counts['limited_plan_minutes']+=len(audit['limited'])
                counts['absent_original_plan_minutes']+=len(audit['absent_original_plan_ids'])
                for row in audit['limited']:reasons.update(row['reason_codes'])
                if batch['decision_at'][11:16] in {'09:31','11:30','15:00'}:
                    print(json.dumps(dict(pass_number=ordinal,decision_at=batch['decision_at'],
                        typed_plan_minutes=counts['typed_plan_minutes'])),flush=True)
                return invocation
            result=execute_timeline(batches,broker=broker,store=store,set_clock=set_clock,prepare_day=morning,
                prepare_minute=minute,consume_close=close,account_id='replay-paper')
            result.update(source_counts=dict(counts),source_limited_reasons=dict(reasons),snapshot_reads=reader.snapshot_reads)
        if result['legal_rate_denominator']!=0 or broker.reconcile()['status']!='MATCH':
            raise ValueError('SOURCE_AUDIT_UNEXPECTED_MODEL_OR_RECONCILIATION')
        with (work/'TIMELINE.json').open('x',encoding='utf-8',newline='\n') as stream:
            json.dump(result,stream,ensure_ascii=False,indent=2)
        passes.append(dict(pass_number=ordinal,timeline=result))
    repeat=run_two_passes(args.output,allowed_root=ROOT/'artifacts/team-v2-20261016',run_pass=run_pass)
    unchanged=all(file_hash(ROOT/p)==value for p,value in hashes.items())
    if not unchanged or not repeat['ledger_bytes_equal'] or not repeat['curve_bytes_equal']:
        raise ValueError('SOURCE_ACCOUNT_REPEATABILITY_OR_SOURCE_CHANGED')
    result=dict(actual_started_at=started,actual_finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=head,source_hashes=hashes,source_files_unchanged=unchanged,
        status='SEALED_SOURCE_CONTIGUOUS_NO_PERMISSION_TWO_PASS_IDENTICAL',
        source_database_sha256=receipt['source_database_sha256'],source_archive_sha256=receipt['archive_sha256'],
        passes=passes,repeatability=repeat,model_calls=0,source_requests=0,orders=0,production_writes=0,
        historical_pm_a6='UNAVAILABLE_NOT_MINTED',funded_path='NOT_RUN',model_legal_rate=None,
        control_release_gate='NOT_PASSED',returns='NOT_CLAIMED')
    target=args.output/'SOURCE_ACCOUNT_AUDIT.json'
    with target.open('x',encoding='utf-8',newline='\n') as stream:json.dump(result,stream,ensure_ascii=False,indent=2)
    print(json.dumps(dict(status=result['status'],receipt_sha256=file_hash(target),
        ledger_bytes_equal=True,curve_bytes_equal=True,models=0,orders=0)),flush=True)
    return 0


if __name__=='__main__':sys.exit(main())
