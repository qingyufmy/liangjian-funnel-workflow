"""Disclosure work-domain tests; not stock admission or execution tests."""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.disclosure_scope import (
    audit_disclosure_scope, build_disclosure_prefilter, event_scope,
)

DAY = date(2026, 10, 9)


def bars(up=True):
    values = list(range(10, 31)) if up else list(range(30, 9, -1))
    return [{"date_ms": int(datetime.combine(DAY-timedelta(days=20-i),
        datetime.min.time(), ZoneInfo("Asia/Shanghai")).timestamp()*1000),
        "close_price": value} for i, value in enumerate(values)]


def build(symbols, **kwargs):
    return build_disclosure_prefilter(symbols=symbols, trade_date=DAY,
        daily={s: bars(False) for s in symbols},
        selected_board={"available": True, "trade_date": DAY.isoformat(), "by_symbol": {}},
        event_symbols=[], event_sources_complete=True, **kwargs)


@pytest.mark.parametrize("source", ["hot_symbols", "discovery_symbols"])
def test_attention_or_discovery_is_collected_even_if_final_a2_rejects(source):
    value = build(["A"], **{source: ["A"]})
    assert value["candidate_symbols"] == ["A"]
    # A later A2/model rejection must not define the earlier query domain.
    assert audit_disclosure_scope(value, [])['status'] == 'COVERED'


@pytest.mark.parametrize("row", [
    {"selected_for_rotation": True},
    {"rotation_reserve_scope": "RESEARCH_ONLY_NO_AUTOMATIC_ENTRY"},
])
def test_primary_and_reserve_board_members_are_preserved(row):
    value = build_disclosure_prefilter(symbols=['A'], trade_date=DAY,
        daily={'A': bars(False)}, selected_board={'available': True, 'by_symbol': {'A': [row]}},
        event_symbols=[], event_sources_complete=True)
    assert value['candidate_symbols'] == ['A']


def test_strong_trend_outside_top5_is_not_lost():
    value = build_disclosure_prefilter(symbols=['A'], trade_date=DAY,
        daily={'A': bars()}, selected_board={'available': True, 'by_symbol': {}},
        event_symbols=[], event_sources_complete=True)
    assert value['candidate_symbols'] == ['A']
    assert value['records'][0]['trend_structure']['structure_confirmed'] is True


@pytest.mark.parametrize('gap', ['daily', 'board'])
def test_uncertainty_retains_candidate_instead_of_manufacturing_negative(gap):
    value = build_disclosure_prefilter(symbols=['A'], trade_date=DAY,
        daily={} if gap == 'daily' else {'A': bars(False)},
        selected_board={} if gap == 'board' else {'available': True, 'by_symbol': {}},
        event_symbols=[], event_sources_complete=gap != 'events')
    assert value['candidate_symbols'] == ['A']
    assert value['records'][0]['uncertainty_retained'] is True


def test_known_negative_no_channel_is_deferred_not_rejected_or_empty_success():
    value = build(['A'])
    assert value['candidate_symbols'] == []
    assert value['deferred_symbols'] == ['A']
    assert value['records'][0]['status'] == 'DEFERRED_DISCLOSURE_NOT_COLLECTED'
    assert value['execution_authority'] is False
    assert value['mode'] == 'SHADOW'


def test_event_anchor_is_retained_even_when_daily_trend_is_negative():
    value = build_disclosure_prefilter(symbols=['A'], trade_date=DAY,
        daily={'A': bars(False)}, selected_board={'available': True, 'by_symbol': {}},
        event_symbols=['A'], event_sources_complete=True)
    assert value['candidate_symbols'] == ['A']


def test_no_300_cap_and_input_order_does_not_change_hash():
    symbols = [str(i) for i in range(401)]
    first = build(symbols, hot_symbols=symbols)
    second = build(list(reversed(symbols)), hot_symbols=list(reversed(symbols)))
    assert len(first['candidate_symbols']) == 401
    assert first == second


def test_actual_a2_outside_domain_is_detected_not_silently_dropped():
    value = build(['A', 'B'], hot_symbols=['A'])
    audit = audit_disclosure_scope(value, ['A', 'B', 'C'])
    assert audit['status'] == 'SCOPE_MISS'
    assert audit['missing_symbols'] == ['B', 'C']
    assert value['candidate_symbols'] == ['A']


def test_corrupted_scope_cannot_pass_coverage_audit():
    value = build(['A'], hot_symbols=['A'])
    value['candidate_symbols'] = []
    with pytest.raises(ValueError, match='DISCLOSURE_PREFILTER_HASH_MISMATCH'):
        audit_disclosure_scope(value, [])


def test_source_and_daily_revisions_participate_in_hash():
    first = build(['A'], hot_symbols=['A'])
    revised = build_disclosure_prefilter(symbols=['A'], trade_date=DAY,
        daily={'A': bars()}, selected_board={'available': True, 'by_symbol': {}},
        event_symbols=[], event_sources_complete=True, hot_symbols=['A'])
    assert first['scope_hash'] != revised['scope_hash']


@pytest.mark.parametrize('rows,expected_complete', [([], True),
    ([{'thscode': '600000.SH'}], True), ([{'ticker': '600000'}], False)])
def test_event_shape_must_prove_absence_without_guessing_exchange(rows, expected_complete):
    from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
    result = HithinkFetchResult(endpoint='events', ok=True, complete=True, reason_code='OK',
        items=tuple(HithinkRow.model_validate(row) for row in rows),
        fetch_time=datetime(2026,10,9,15,10,tzinfo=ZoneInfo('Asia/Shanghai')),
        metadata={'market_trade_date': DAY.isoformat()})
    symbols, complete = event_scope({'LIMIT_UP_POOL': result, 'LIMIT_UP_LADDER': result}, DAY)
    assert complete is expected_complete
    assert symbols == (['600000.SH'] if rows and expected_complete else [])


def test_a2_gate_persists_real_subset_miss_without_changing_gate(tmp_path):
    import json
    from types import SimpleNamespace
    from liangjian_funnel.pipeline.research import ResearchPipeline
    from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
    gate = DeterministicGateResult(stage='A2_LOCAL_ROLE', decisions=(),
        review_symbols=('A', 'B'), monitor_symbols=(), rejected_symbols=())
    scope = build(['A', 'B'], hot_symbols=['A'])
    snapshot = SimpleNamespace(data={'DISCLOSURE_PREFILTER_SHADOW': scope}, as_of=None)
    pipeline = SimpleNamespace(output_dir=tmp_path, feature_store=None)
    ResearchPipeline._persist_gate(pipeline, 'run', 'lane', gate, snapshot)
    receipt = json.loads((tmp_path/'disclosure_scope_shadow/run/lane.json').read_text(encoding='utf-8'))
    assert receipt['status'] == 'SCOPE_MISS'
    assert receipt['missing_symbols'] == ['B']
    assert gate.review_symbols == ('A', 'B')


@pytest.mark.parametrize('board', [
    {'available': True, 'by_symbol': {}},
    {'available': True, 'trade_date': DAY.isoformat(), 'by_symbol': {'A': [None]}},
    {'available': True, 'trade_date': DAY.isoformat(), 'by_symbol': {'A': [{}]}},
])
def test_unproven_board_absence_never_defers_disclosure(board):
    value = build_disclosure_prefilter(symbols=['A'], trade_date=DAY,
        daily={'A': bars(False)}, selected_board=board,
        event_symbols=[], event_sources_complete=True)
    assert value['candidate_symbols'] == ['A']
    assert value['records'][0]['uncertainty_retained'] is True


def test_missing_event_date_cannot_prove_no_event_today():
    from liangjian_funnel.pipeline.data_source import HithinkFetchResult
    result = HithinkFetchResult(endpoint='events', ok=True, complete=True,
        reason_code='OK', items=(), fetch_time=datetime(2026,10,9,15,10,
        tzinfo=ZoneInfo('Asia/Shanghai')), metadata={})
    assert event_scope({'LIMIT_UP_POOL': result, 'LIMIT_UP_LADDER': result}, DAY) == ([], False)


def test_coverage_checks_local_eligible_before_transport_ranking():
    scope = build(['A', 'B'], hot_symbols=['A'])
    decisions = [{'symbol': 'A', 'status': 'REVIEW_CANDIDATE',
                  'local_eligible_for_review': True, 'a2_pool_channel': 'EMOTION'},
                 {'symbol': 'B', 'status': 'MONITOR',
                  'local_eligible_for_review': True, 'a2_pool_channel': 'TREND',
                  'rotation_reserve_eligible': True}]
    value = audit_disclosure_scope(scope, ['A'], decisions=decisions)
    assert value['status'] == 'SCOPE_MISS'
    assert value['missing_symbols'] == ['B']
    assert value['review_missing_symbols'] == []
    assert value['route_coverage']['RESERVE']['missing_symbols'] == ['B']


def test_even_rehashed_invalid_partition_is_not_coverage_evidence():
    from liangjian_funnel.pipeline.feature_store import content_hash
    scope = build(['A'], hot_symbols=['A'])
    scope['deferred_symbols'] = ['A']
    scope['scope_hash'] = content_hash({k:v for k,v in scope.items() if k != 'scope_hash'})
    with pytest.raises(ValueError, match='DISCLOSURE_PREFILTER_PARTITION_INVALID'):
        audit_disclosure_scope(scope, [])


def test_observation_clipped_after_rank_still_has_auditable_route():
    scope = build(['A'])
    decision = {'symbol': 'A', 'local_eligible_for_review': True,
                'strong_trend_observation': False, 'strong_trend_observation_rank': 31,
                'eligible_routes': ['MARKET_CORE'], 'status': 'LOCAL_MONITOR'}
    value = audit_disclosure_scope(scope, [], decisions=[decision])
    assert value['route_coverage']['STRONG_TREND_OBSERVATION']['missing_symbols'] == ['A']
    assert value['route_coverage']['ROUTE:MARKET_CORE']['missing_symbols'] == ['A']
