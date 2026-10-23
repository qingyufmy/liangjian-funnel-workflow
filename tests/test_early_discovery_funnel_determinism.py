"""Causal discovery/state regressions, not profit or entry recommendations."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import inspect
import json
from pathlib import Path

import pytest

from liangjian_funnel.pipeline import early_discovery as discovery
from liangjian_funnel.pipeline.deterministic import (
    _build_a2_local_market_factors, screen_a2,
)
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from test_early_discovery import bars, repair_bars, scan, TZ
from test_deterministic_pipeline_v2 import _snapshot, _trend_a2_factor_scores, _complete_a2_factor_scores
from test_rotation_theme import _metric_row


def test_cache_scan_excludes_later_fetch_and_revisions(tmp_path):
    cache = LocalFactCache(tmp_path / 'facts.sqlite3')
    rows = repair_bars()
    cutoff = datetime.fromisoformat(rows[-1]['date']) + timedelta(minutes=10)
    source = [dict(symbol='603186.SH', timestamp=row['date'], adjust='none',
                   fetched_at=cutoff, payload={**row, 'adjust_mode': 'none'}) for row in rows]
    cache.upsert_daily_bars(source)
    expected = discovery.scan_cached_universe(cache, ['603186.SH'], as_of=cutoff)
    assert expected['records'][0]['signals'] == ['REPAIR_WATCH']
    cache.upsert_daily_bars([{**source[-1], 'fetched_at': cutoff + timedelta(days=1),
        'payload': {**source[-1]['payload'], 'close': 1., 'low': .9, 'high': 1.1, 'open': 1.}}])
    assert discovery.scan_cached_universe(cache, ['603186.SH'], as_of=cutoff) == expected
    # A bar merely dated today is not known before its actual receive time.
    before_fetch = cutoff - timedelta(minutes=1)
    missing = discovery.scan_cached_universe(cache, ['603186.SH'], as_of=before_fetch)
    assert not missing['records'] and missing['data_gaps']


def _trend_fixture():
    snapshot = _snapshot(32)
    snapshot['SELECTED_BOARD_SNAPSHOT'] = dict(available=True, selected_primary_boards=[], by_symbol={})
    snapshot['A2_SCORE_WEIGHTS'] = {key: 1 for key in _complete_a2_factor_scores(90)}
    snapshot['CAPITAL_FLOW_SNAPSHOT'] = {'available':True,
        'by_symbol':{s:{'available':True,'capital_flow_score':90} for s in snapshot['g0_symbols']}}
    output = {'active_research_pool': [dict(symbol=s, primary_theme='T', industry_chain_node='N',
        business_exposure={'source_ref':'original', 'revenue_exposure_pct':65},
        business_exposure_facts=[{'source_ref':'original', 'revenue_exposure_pct':65}],
        a2_factor_scores=_trend_a2_factor_scores(90), data_quality_score=90)
        for s in snapshot['g0_symbols']]}
    return snapshot, output


def _event(symbol, as_of):
    event = scan(repair_bars())['records'][0]
    return {**event, 'symbol': symbol, 'as_of': as_of,
            'evidence': {**event['evidence'], 'setup_bar_end':'2026-08-27T15:00:00+08:00'}}


def test_discovery_does_not_change_routes_and_rank31_stays_explicitly_pending():
    snapshot, upstream = _trend_fixture()
    before = screen_a2(snapshot, upstream, minimum_identifiability_score=0, review_all_eligible=True)
    events = [_event(row['symbol'], snapshot['snapshot_manifest']['as_of']) for row in upstream['active_research_pool']]
    snapshot['EARLY_DISCOVERY_SNAPSHOT'] = {'records':events}
    gate = screen_a2(snapshot, upstream, minimum_identifiability_score=0, review_all_eligible=True)
    # Annotation only: local scores, statuses, work budget and model domain unchanged.
    assert gate.review_symbols == before.review_symbols and len(gate.review_symbols) == 30
    assert [(d['symbol'], d['status'], d['score']) for d in gate.decisions] == [
        (d['symbol'], d['status'], d['score']) for d in before.decisions]
    states = [d['early_discovery']['review_state'] for d in gate.decisions]
    assert states.count('SELECTED_PENDING_MODEL') == 30
    assert states.count('PENDING_REVIEW_BUDGET') == 2
    contexts = {d['symbol']: d['bottleneck_context'] for d in gate.decisions}
    snapshot['A2_BOTTLENECK_CONTEXT'] = contexts
    result = discovery.finalize_a2_discovery_review(snapshot, upstream, symbols=gate.review_symbols,
        status='VALIDATED', call_ids=['actual-journal-id'], input_hash='a'*64)
    assert result['review_state_counts'] == {'PENDING_REVIEW_BUDGET':2, 'REVIEW_VALIDATED':30}
    pending = [r for r in result['records'] if r['review_state'] == 'PENDING_REVIEW_BUDGET']
    assert all(not r['model_review_validated'] and 'model_stage_call_ids' not in r for r in pending)
    assert all(r['execution_permission'] == 'BLOCKED' for r in result['records'])


def test_unqualified_or_future_setup_is_not_promoted():
    cutoff = '2026-08-28T15:10:00+08:00'
    event = _event('A', cutoff)
    rows = [dict(symbol='A', status='LOCAL_MONITOR', sent_to_llm=False, reason_codes=['NEEDS_EVIDENCE'])]
    discovery.bind_a2_discovery_states(rows, {'records':[event]}, as_of=cutoff)
    assert rows[0]['early_discovery']['review_state'] == 'PENDING_A2_QUALIFICATION'
    future = deepcopy(event)
    future['evidence']['setup_bar_end'] = '2026-08-29T15:00:00+08:00'
    rows[0].update(status='REVIEW_CANDIDATE', sent_to_llm=True)
    discovery.bind_a2_discovery_states(rows, {'records':[future]}, as_of=cutoff)
    audit = rows[0]['early_discovery']
    assert audit['review_state'] == 'SOURCE_UNVERIFIED' and not audit['model_review_validated']


@pytest.mark.parametrize('status', ['VALIDATED', 'BLOCKED'])
def test_actual_team_stage_boundary_records_no_false_http_claim(tmp_path, status):
    from test_team_v2_a2_research import fixture
    mutate = None if status == 'VALIDATED' else lambda raw, n: raw['market_read'].update(opposing_refs=['A2:FOREIGN'])
    app, args, seen = fixture(tmp_path, mutate)
    data = deepcopy(dict(args['snapshot'].data))
    event = _event('000001.SZ', args['snapshot'].as_of.isoformat())
    event.update(review_state='SELECTED_PENDING_MODEL', model_scope_selected=True,
                 model_review_validated=False)
    data['EARLY_DISCOVERY_SNAPSHOT'] = {'records':[event]}
    data['A2_BOTTLENECK_CONTEXT']['000001.SZ']['early_discovery'] = event
    args['snapshot'] = replace(args['snapshot'], data=data)
    result = app._run_stage(**args)
    assert result.status == status, result.reason_codes
    audit = result.diagnostics['early_discovery']['records'][0]
    assert audit['review_state'] == ('REVIEW_VALIDATED' if status == 'VALIDATED' else 'REVIEW_BLOCKED')
    assert audit['model_review_validated'] is (status == 'VALIDATED')
    assert audit['model_stage_call_ids'] == result.diagnostics['team_call_ids']
    assert 'actual_http' not in audit and len(seen) == (1 if status == 'VALIDATED' else 3)
    if status == 'VALIDATED':
        assert result.output['early_discovery'] == result.diagnostics['early_discovery']


def test_equal_industry_share_has_stable_tie_not_membership_order():
    candidates = {'X':{'amount':1, 'change_ratio_pct':0},
                  'Y':{'amount':1, 'change_ratio_pct':3}, 'Z':{'amount':1, 'change_ratio_pct':-3}}
    memberships = {'X':[{'taxonomy_code':'A'}, {'taxonomy_code':'B'}],
                   'Y':[{'taxonomy_code':'A'}], 'Z':[{'taxonomy_code':'B'}]}
    a = _build_a2_local_market_factors({}, candidates=candidates, industry_membership=memberships)
    memberships['X'].reverse()
    b = _build_a2_local_market_factors({}, candidates=dict(reversed(list(candidates.items()))), industry_membership=memberships)
    assert a == b and a['X']['breadth']['score'] == 50
    # Reproduce the old exact first-encounter tie rule with identical facts.
    source = inspect.getsource(_build_a2_local_market_factors).replace(
        'min(codes, key=lambda code: (-sector_rows[code]["turnover_share"], code), default=None)',
        'max(codes, key=lambda code: sector_rows[code]["turnover_share"], default=None)')
    namespace = dict(_build_a2_local_market_factors.__globals__)
    exec(source, namespace)
    legacy = namespace['_build_a2_local_market_factors']
    old_b = legacy({}, candidates=candidates, industry_membership=memberships)
    memberships['X'].reverse()
    old_a = legacy({}, candidates=candidates, industry_membership=memberships)
    assert old_a['X']['breadth'] != old_b['X']['breadth']


def test_conflicting_duplicate_theme_ids_fail_closed_unique_ties_are_stable():
    from liangjian_funnel.data.rotation_theme import calculate_rotation_strength, RotationThemeDataError
    duplicate = [_metric_row('A', relative=9), _metric_row('A', relative=-9), _metric_row('B')]
    for rows in (duplicate, duplicate[::-1]):
        with pytest.raises(RotationThemeDataError, match='ROTATION_THEME_DUPLICATE_THEME_ID'):
            calculate_rotation_strength(rows)
    rows = [_metric_row('B'), _metric_row('A')]
    assert calculate_rotation_strength(rows) == calculate_rotation_strength(rows[::-1])


def test_synthetic_multiday_turn_tracking_and_failed_start_are_causal():
    prices = [10+i*.1 for i in range(65)] + [16.7,16.8,14.,18.5,18.6]
    before = scan(bars(prices[:-2]))
    turn, follow = scan(bars(prices[:-1])), scan(bars(prices))
    assert not before['records']
    first, continued = turn['records'][0], follow['records'][0]
    assert 'TREND_RESTART_WATCH' in first['signals']
    assert first['setup_event_id'] == continued['setup_event_id']
    assert continued['evidence']['observation_age_sessions'] == 1
    assert not scan(bars([*prices[:-1], 12.]))['records']
    assert first['execution_permission'] == continued['execution_permission'] == 'BLOCKED'


def test_real_oct19_23_original_replay_keeps_pit_and_later_shape_separate():
    # Optional immutable local evidence, never fetched or copied into fixtures.
    path = Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021/ROBOT_MULTIDAY_CACHE_REPLAY_20261023T194811.json')
    if not path.exists():
        pytest.skip('immutable postclose evidence is not present in this checkout')
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '31e1641421431a12b054843fc68444491534416e98b8e547fb0fe3e5e10830b6'
    original = json.loads(raw)
    outcomes = {}
    for report in original['reports']:
        view = report['views']['POINT_IN_TIME']
        cutoff = datetime.fromisoformat(report['decision_cutoff'])
        assert all(datetime.fromisoformat(r['fetched_at']) <= cutoff for r in view['records'])
        rows = [{**r['payload'], 'timestamp':r['timestamp'], 'adjust':r['adjust']} for r in view['records']]
        result = discovery.discover_early_setups({report['symbol']:rows}, as_of=cutoff, symbols=[report['symbol']])
        expected = view['result']
        assert [r['signals'] for r in result['records']] == [r['signals'] for r in expected['records']]
        assert result['data_gaps'] == expected['data_gaps']
        outcomes[(report['symbol'], report['trade_date'])] = result
    assert not outcomes[('603728.SH','2026-10-19')]['records']
    assert outcomes[('603728.SH','2026-10-21')]['records'][0]['signals'] == ['REPAIR_WATCH']
    assert outcomes[('603728.SH','2026-10-23')]['records'][0]['signals'] == ['MA520_CROSS_PREPARATION']
    assert outcomes[('301502.SZ','2026-10-19')]['data_gaps']
    assert all(not outcomes[(s, f'2026-10-{day}')]['records'] for s in ['603278.SH','601628.SH'] for day in range(19,24))
    assert path.read_bytes() == raw
