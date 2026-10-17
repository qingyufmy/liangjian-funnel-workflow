"""Read sealed source into all original minute contexts, without model/execution.

This is the source adapter audit, not another six-day model or strategy replay.
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

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.evaluation.team_v2_replay_batches import read_sealed_source,chronological_batches
from liangjian_funnel.evaluation.team_v2_replay_materializer import FrozenMinuteMaterializer
from run_team_a5_frozen_dryrun import save


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-pack',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();output=args.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('REPLAY_NEW_OWNED_OUTPUT_REQUIRED')
    output.mkdir(parents=True)
    started=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    receipt,records,plans=read_sealed_source(args.source_pack)
    days={};digest=hashlib.sha256()
    with FrozenMinuteMaterializer(receipt['source_path'],expected_sha256=receipt['source_database_sha256'],
            plans=plans,settings=SimpleNamespace(root=ROOT)) as reader, (output/'MINUTE_SOURCE_AUDIT.jsonl').open('x',encoding='utf-8',newline='\n') as stream:
        for batch in chronological_batches(records,plans):
            value=reader.materialize(batch)
            counts=Counter(dict(scheduled_minutes=1,model_ticks=int(batch['model_tick']),
                original_frames=len(batch['source_frames']),original_data_limited=len(batch['data_limited_source_frames']),
                source_projected_plan_minutes=len(value['projected']),
                source_eligible_entry_plan_minutes=len(value['entry_plan_ids']),
                original_exit_only_plan_minutes=len(batch['exit_only_source_frames']),
                source_or_projection_limited_plan_minutes=len(value['limited']),
                absent_original_plan_minutes=len(value['absent_original_plan_ids']),
                projected_symbol_minutes=len(value['closed_bars'])))
            row=days.setdefault(batch['trade_date'],dict(counts=Counter(),reasons=Counter()))
            row['counts'].update(counts)
            row['reasons'].update(code for item in value['limited'] for code in item['reason_codes'])
            item=dict(decision_at=batch['decision_at'],status=value['status'],source_digest=value['source_digest'],
                counts=dict(counts),limited=value['limited'],source_eligible_entry_plan_ids=value['entry_plan_ids'],
                quote_basis=value['quote_basis'],model_permission=value['model_permission'])
            line=json.dumps(item,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n'
            stream.write(line);digest.update(line.encode())
        snapshot_reads=reader.snapshot_reads
    result=dict(status='ORIGINAL_SIX_DAY_SOURCE_MATERIALIZED_NOT_EXECUTED',
        actual_started_at=started,actual_completed_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        archive_sha256=receipt['archive_sha256'],database_sha256=receipt['source_database_sha256'],
        minute_audit_sha256=digest.hexdigest(),snapshot_reads=snapshot_reads,days=days,
        model_calls=0,broker_constructors=0,orders=0,source_requests=0,production_writes=0,
        control_release_gate='NOT_RUN_PM_A6_A4_AND_CONTIGUOUS_PAPER_EXECUTION_REQUIRED',
        returns='NOT_CLAIMED_BAR_DERIVED_QUOTES',
        note='Missing windows and original terminal/expiry remain excluded from entries; no historical plan reactivation. Counts are plan-minutes, not unique plans or trades.')
    save(output/'MATERIALIZATION.json',result)
    print(json.dumps(result,ensure_ascii=True))
    return 0


if __name__=='__main__':
    sys.exit(main())
