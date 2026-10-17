"""Seal 0277 six-day original bindings locally. No model or execution calls."""
import argparse
from collections import Counter
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from liangjian_funnel.evaluation.team_v2_replay_source import frozen_frames, SIX_DAYS
from liangjian_funnel.runtime.team_v2_store import canonical


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--source-sha256', required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args(); output = a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('REPLAY_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    counts = Counter(); reasons = Counter(); days = {}; digest = hashlib.sha256()
    started = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    # xb plus deterministic gzip header; original frozen database remains RO.
    with (output/'frames.jsonl.gz').open('xb') as raw, gzip.GzipFile(fileobj=raw, mode='wb', mtime=0, filename='') as archive:
        for frame in frozen_frames(a.source, expected_sha256=a.source_sha256):
            line = (canonical(frame)+'\n').encode(); archive.write(line); digest.update(line)
            counts[frame['kind']]+=1
            if frame['kind']=='day':
                days[frame['trade_date']] = dict(status=frame['status'], observed_plans=len(frame['observed_plan_ids']),
                    original_events=frame['original_event_count'], source_bound_minutes=0, data_limited_minutes=0)
            elif frame['kind']=='plan_minute':
                key = 'source_bound_minutes' if frame['status']=='SOURCE_BOUND_RULES_PENDING' else 'data_limited_minutes'
                days[frame['trade_date']][key]+=1; reasons.update(frame['reason_codes'])
    result = dict(status='SOURCE_PREPARED_NOT_EXECUTED', actual_started_at=started,
        actual_completed_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        source_path=str(a.source.resolve()), source_database_sha256=a.source_sha256,
        days=days, required_six_days=list(SIX_DAYS), records=dict(counts), reason_distribution=dict(reasons),
        uncompressed_frames_sha256=digest.hexdigest(),
        archive_sha256=hashlib.file_digest((output/'frames.jsonl.gz').open('rb'),'sha256').hexdigest(),
        authority='0277_CORRECTNESS_SOURCE_ONLY_NOT_CONTROL_PASS_OR_RETURNS',
        model_calls=0, provider_calls=0, production_writes=0,
        remaining_gates=['REAL_TRADER_OUTPUT_VALIDITY','GUARDRAIL_OVERREACH_ZERO',
                         'TWO_RUN_ACCOUNT_CURVE_BYTES','REJECTION_DISTRIBUTION','TRADER_ENGINE_DISAGREEMENT'])
    with (output/'PREPARATION.json').open('x',encoding='utf-8') as f:
        f.write(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=True))


if __name__=='__main__':main()
