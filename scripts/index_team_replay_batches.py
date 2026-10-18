"""Validate an existing replay source's chronological input batches only."""
import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.evaluation.team_v2_replay_batches import read_sealed_source,chronological_batches
from run_team_a5_frozen_dryrun import save


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-pack',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 output=a.output.resolve()
 if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_FILE_REQUIRED')
 start=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat();receipt,records,plans=read_sealed_source(a.source_pack)
 days={}
 for row in chronological_batches(records,plans):
  counts=days.setdefault(row['trade_date'],Counter());counts['scheduled_minutes']+=1;counts['closed_5m_regular_ticks']+=row['model_tick']
  counts[row['status']]+=1;counts['entry_source_plan_minutes']+=len(row['entry_source_frames']);counts['exit_only_source_plan_minutes']+=len(row['exit_only_source_frames'])
  counts['data_limited_source_plan_minutes']+=len(row['data_limited_source_frames']);counts['absent_original_plan_minutes']+=len(row['absent_original_plan_ids'])
  if row['model_tick']:counts['ticks_with_entry_source']+=bool(row['entry_source_frames'])
 result=dict(status='SOURCE_BATCH_INDEXED_NOT_EXECUTED',actual_started_at=start,actual_finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
  head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),source_archive_sha256=receipt['archive_sha256'],
  days=days,models=0,provider_calls=0,production_writes=0,control_gate='NOT_PASSED',
  remaining=['REAL_PM_AND_A6_OR_DECLARED_CONSERVATIVE_FALLBACK','REAL_TRADER_ALL_BATCHES','ACCOUNT_FILLS_SETTLEMENT_DOUBLEPASS','SPECIAL_1501_CLOSE_BAR'])
 save(output,result);print(json.dumps(result));return 0

if __name__=='__main__':sys.exit(main())
