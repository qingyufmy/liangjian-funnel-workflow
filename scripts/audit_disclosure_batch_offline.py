"""Read local research projections only; no app/settings/cache/network clients.

This diagnoses prefilter route coverage, not original model replay. Missing raw
snapshots or A2 overlays must remain explicit and prohibit scope promotion.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.deterministic import screen_a2
from liangjian_funnel.pipeline.disclosure_scope import build_disclosure_prefilter, audit_disclosure_scope
from liangjian_funnel.pipeline.research.common import _sha256_json


DECISION_FIELDS = ('symbol', 'decision_id', 'status', 'local_eligibility_status',
    'local_eligible_for_review', 'reason_codes', 'a2_pool_channel', 'eligible_routes',
    'emotion_core_eligible', 'trend_core_eligible', 'rotation_reserve_eligible',
    'strong_trend_observation', 'strong_trend_observation_rank', 'theme_id',
    'rotation_direction_id', 'role')


def audit_projected_batch(snapshot, lane):
    stages = [s for s in lane.get('stages', ()) if s.get('stage') == 'A1']
    if len(stages) != 1:
        raise ValueError('UNIQUE_ORIGINAL_A1_STAGE_REQUIRED')
    stage = stages[0]
    if stage.get('snapshot_id') != snapshot.get('snapshot_id'):
        raise ValueError('ORIGINAL_A1_SNAPSHOT_MISMATCH')
    output = stage.get('output')
    if stage.get('status') != 'VALIDATED' or not isinstance(output, Mapping):
        raise ValueError('VALIDATED_ORIGINAL_A1_REQUIRED')
    if _sha256_json(output) != stage.get('output_hash'):
        raise ValueError('ORIGINAL_A1_OUTPUT_HASH_MISMATCH')
    data = snapshot['data']
    scope = sorted(set(data['g0_symbols']))
    a1_rows = output.get('active_research_pool', ())
    a1_symbols = [r['symbol'] for r in a1_rows]
    if len(a1_symbols) != len(set(a1_symbols)) or set(a1_symbols) - set(scope):
        raise ValueError('ORIGINAL_A1_SCOPE_MISMATCH')
    market_time = datetime.fromisoformat(str(data.get('MARKET_DATA_AS_OF') or snapshot['as_of']))
    research_time = datetime.fromisoformat(snapshot['as_of'])
    if market_time.tzinfo is None or research_time.tzinfo is None or market_time > research_time:
        raise ValueError('MARKET_TIME_INVALID')
    day = market_time.astimezone(ZoneInfo('Asia/Shanghai')).date()
    tier = data.get('TIER_STRUCTURE_SNAPSHOT', {})
    tier_rows = tier.get('by_symbol', {})
    tier_time = datetime.fromisoformat(str(tier.get('as_of') or market_time.isoformat()))
    events_complete = (tier.get('available') is True and tier.get('as_of') is not None
        and tier_time.tzinfo is not None and tier_time <= research_time
        and tier_time.astimezone(ZoneInfo('Asia/Shanghai')).date() == day
        and isinstance(tier_rows, Mapping) and all(
            isinstance(tier_rows.get(s), Mapping) and tier_rows[s].get('available') is True
            and tier_rows[s].get('availability_state') in ('OBSERVED_VALUE', 'OBSERVED_ABSENT')
            for s in scope))
    events = [s for s, r in tier_rows.items() if isinstance(r, Mapping)
              and (r.get('first_board_observed') is True or float(r.get('ladder_height') or 0) > 0)]
    prefilter = build_disclosure_prefilter(symbols=scope, trade_date=day,
        daily=data.get('RECENT_DAILY_BARS', {}),
        selected_board=data.get('SELECTED_BOARD_SNAPSHOT', {}),
        event_symbols=events, event_sources_complete=events_complete,
        hot_symbols=[r['symbol'] for r in data.get('EASTMONEY_HOT100_SNAPSHOT', {}).get('records', ())],
        discovery_symbols=[r['symbol'] for r in data.get('EARLY_DISCOVERY_SNAPSHOT', {}).get('records', ())
                           if r.get('review_budget_selected') is True])
    # Wide transport is an explicit diagnostic, not a guessed production
    # setting. Also inspect pre-rank eligibility; no model final A2 is input.
    parameters = {'minimum_identifiability_score': float(data.get('MIN_IDENTIFIABILITY_SCORE') or 60.0),
                  'rotation_theme_count': int(data.get('A2_ROTATION_THEME_COUNT') or 5),
                  'review_all_eligible': True, 'llm_top_n_per_theme': 5}
    gate = screen_a2(data, output, **parameters)
    coverage = audit_disclosure_scope(prefilter, gate.review_symbols, decisions=gate.decisions)
    limitations = ['ORIGINAL_RAW_FROZEN_SNAPSHOT_NOT_VERIFIED',
                   'ORIGINAL_A2_STAGE_OVERLAY_NOT_VERIFIED',
                   'TRANSPORT_SETTING_IS_DIAGNOSTIC_NOT_ORIGINAL_RUNTIME',
                   'PREFILTER_RECONSTRUCTED_NOT_ORIGINAL_PRE_ANNOUNCEMENT_RECEIPT']
    if not coverage['required_count']:
        limitations.append('EMPTY_QUANTITATIVE_DOMAIN')
    return {'schema_version': 'disclosure-batch-offline/1',
            'status': 'SCOPE_MISS' if coverage['missing_symbols'] else 'DATA_LIMITED',
            'mode': 'SHADOW', 'changes_query_scope': False, 'execution_authority': False,
            'snapshot_id': snapshot['snapshot_id'], 'declared_snapshot_hash': snapshot['snapshot_hash'],
            'as_of': snapshot['as_of'], 'market_trade_date': day.isoformat(),
            'a1_output_hash': stage['output_hash'], 'a1_count': len(a1_symbols),
            'diagnostic_parameters': parameters, 'limitations': limitations,
            'prefilter': prefilter, 'coverage': coverage,
            'quantitative_gate_hash': _sha256_json(list(gate.decisions)),
            'quantitative_decisions': [
                {**{k: r[k] for k in DECISION_FIELDS if k in r}, 'decision_hash': _sha256_json(r)}
                for r in gate.decisions]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--research', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.resolve() in (args.snapshot.resolve(), args.research.resolve()):
        raise ValueError('OUTPUT_CONFLICTS_WITH_INPUT')
    if args.output.exists():
        raise FileExistsError(args.output)
    evidence = []
    inputs = []
    for role, path in (('research_projection', args.snapshot), ('original_lane', args.research)):
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        with path.open(encoding='utf-8') as stream:
            inputs.append(json.load(stream))
        evidence.append({'role': role, 'path': str(path.resolve()), 'sha256': digest})
    value = audit_projected_batch(*inputs)
    value['input_files'] = evidence
    # Hash again before publishing: these inputs must remain immutable.
    for item in evidence:
        with Path(item['path']).open('rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != item['sha256']:
                raise ValueError('INPUT_CHANGED_DURING_AUDIT')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
    print(json.dumps({'status': value['status'], 'a1_count': value['a1_count'],
        'candidate_count': value['coverage']['candidate_count'],
        'required_count': value['coverage']['required_count'],
        'missing_count': len(value['coverage']['missing_symbols'])}, sort_keys=True))
    return 1 if value['status'] == 'SCOPE_MISS' else 2


if __name__ == '__main__':
    raise SystemExit(main())
