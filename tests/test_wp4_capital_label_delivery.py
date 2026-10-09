"""A-LABEL is observation transport, never a new capital factor policy."""
import copy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.a2_market import WINDOWS, _content_hash, build_capital_flow_snapshot
from liangjian_funnel.data.capital_source_policy import inspect_legacy_capital_weighting
from liangjian_funnel.pipeline.a2_features import build_a2_feature_snapshot
from liangjian_funnel.pipeline.research.common import _project_capital_flow
from liangjian_funnel.pipeline.deterministic import screen_a2
from test_wp4_capital_source_isolation import research

NOW = datetime(2026, 10, 9, 15, 0, tzinfo=ZoneInfo('Asia/Shanghai'))
SYMBOLS = ['600519.SH', '000001.SZ']


def raw(windows):
    return build_capital_flow_snapshot(
        frames={key: [{'代码': '600519', '主力净流入-净占比': 12},
                      {'代码': '000001', '主力净流入-净占比': -5}] for key in windows},
        as_of=NOW, ingested_at=NOW, expected_symbols=SYMBOLS,
    )


@pytest.mark.parametrize('windows,state', [
    (['today'], 'DEGRADED_RENORMALIZED'),
    (['today', '3d'], 'DEGRADED_RENORMALIZED'),
    ([key for key, _, _ in WINDOWS], 'FULL_WINDOWS'),
])
def test_label_contains_original_weights_and_does_not_accept_new_policy(windows, state):
    snapshot = raw(windows)
    before = copy.deepcopy(snapshot)
    audit = inspect_legacy_capital_weighting(snapshot)
    assert audit['label_only'] is True
    assert audit['policy_approved'] is False
    assert audit['original_weights'] == {key: weight for key, _, weight in WINDOWS}
    for symbol, row in audit['by_symbol'].items():
        assert row['normalization_state'] == state
        assert row['observed_windows'] == windows
        assert row['original_score'] == snapshot['by_symbol'][symbol]['capital_flow_score']
    assert snapshot == before


def test_model_projection_delivers_labels_without_rehashing_or_mutating_original():
    snapshot = raw(['today'])
    before = copy.deepcopy(snapshot)
    projected = _project_capital_flow(snapshot, {SYMBOLS[0]})
    assert set(projected['by_symbol']) == {SYMBOLS[0]}
    label = projected['by_symbol'][SYMBOLS[0]]['weighting_observation']
    assert label['normalization_state'] == 'DEGRADED_RENORMALIZED'
    assert label['original_score'] == snapshot['by_symbol'][SYMBOLS[0]]['capital_flow_score']
    assert projected['weighting_observation_input_hash'] == snapshot['content_hash']
    assert projected['content_hash'] == snapshot['content_hash']
    assert snapshot == before and snapshot['content_hash'] == _content_hash(snapshot)


def test_feature_factor_retains_labels_without_modifying_numeric_role_or_score():
    snapshot = raw(['today'])
    before = copy.deepcopy(snapshot)
    result = build_a2_feature_snapshot(
        candidates=[{'symbol': symbol, 'amount': 1000} for symbol in SYMBOLS],
        daily_bars={}, industry_membership=None, concept_membership=None,
        ladder_snapshot=None, dragon_tiger_snapshot=None, attention_snapshot=None,
        sector_cycle_snapshot=None, capital_flow_snapshot=snapshot, as_of=NOW,
    )
    for symbol in SYMBOLS:
        factor = result['by_symbol'][symbol]['factors']['capital_flow']
        assert factor['score'] == snapshot['by_symbol'][symbol]['capital_flow_score']
        assert factor['weighting_observation']['normalization_state'] == 'DEGRADED_RENORMALIZED'
        assert factor['weighting_observation']['original_score_preserved'] is True
    assert snapshot == before


@pytest.mark.parametrize('mode', ['EASTMONEY', 'LOCAL_REFERENCE'])
def test_formal_research_freezes_a_label_sidecar_preserving_routing(tmp_path, monkeypatch, mode):
    result, calls, _ = research(tmp_path, monkeypatch, mode, healthy=True)
    audit = result['CAPITAL_FLOW_WEIGHTING_AUDIT']
    factor = result['CAPITAL_FLOW_SNAPSHOT']
    assert audit['input_content_hash'] == factor['content_hash']
    assert audit['original_snapshot_mutated'] is False
    assert calls == {'tencent': 1, 'eastmoney': 0, 'board': 6 if mode == 'EASTMONEY' else 0}
    assert factor['available'] is (mode == 'EASTMONEY')
    expected = 'DEGRADED_RENORMALIZED' if mode == 'EASTMONEY' else 'DATA_LIMITED'
    assert {row['normalization_state'] for row in audit['by_symbol'].values()} == {expected}


def test_invalid_hash_cannot_receive_full_window_label():
    snapshot = raw([key for key, _, _ in WINDOWS])
    snapshot['content_hash'] = '0' * 64
    projected = _project_capital_flow(snapshot, set(SYMBOLS))
    assert projected['weighting_observation_status'] == 'DATA_LIMITED'
    assert all(row['weighting_observation']['normalization_state'] == 'DATA_LIMITED'
               for row in projected['by_symbol'].values())


@pytest.mark.parametrize('windows', [['today'], [key for key, _, _ in WINDOWS]])
def test_a_label_does_not_change_deterministic_candidate_routing_or_factor_scores(windows):
    capital = raw(windows)
    members = {'available': True, 'records': [
        {'thscode': symbol, 'memberships': [{'industry_thscode': '881001.TI', 'industry_name': '主线'}]}
        for symbol in SYMBOLS
    ]}
    features = build_a2_feature_snapshot(
        candidates=[{'symbol': symbol, 'amount': 1000} for symbol in SYMBOLS],
        daily_bars={symbol: [{'date_ms': int((NOW - timedelta(days=30-i)).timestamp()*1000),
                             'close_price': 10 * (1 + growth)**i} for i in range(31)]
                    for symbol, growth in zip(SYMBOLS, [.02, .01])},
        industry_membership=members, concept_membership=None,
        ladder_snapshot={'records': []}, dragon_tiger_snapshot={'records': []},
        attention_snapshot={'records': []}, sector_cycle_snapshot=None,
        capital_flow_snapshot=capital, as_of=NOW,
    )
    a1 = {'active_research_pool': [
        {'symbol': symbol, 'candidate_id': 'a1:'+symbol, 'primary_theme': 'theme-main',
         'industry_chain_node': 'node-main', 'structural_score': 85, 'data_quality_score': 90,
         'business_exposure': {'revenue_exposure_pct': 65, 'source_ref': 'fixture:business'},
         'business_exposure_facts': [{'revenue_exposure_pct': 65, 'evidence_ref': 'fixture:business'}]}
        for symbol in SYMBOLS
    ]}
    source = {'CAPITAL_FLOW_SNAPSHOT': capital, 'A2_FACTOR_SNAPSHOT': features,
              'TIER_STRUCTURE_SNAPSHOT': features, 'A2_THEME_METRICS': features['theme_metrics'],
              'g0_candidates': [{'symbol': symbol, 'amount': 1000, 'change_ratio_pct': 2} for symbol in SYMBOLS]}
    baseline = copy.deepcopy(source)
    for row in baseline['A2_FACTOR_SNAPSHOT']['by_symbol'].values():
        row['factors']['capital_flow'].pop('weighting_observation')
    # This is a metadata-consumption test, not a hash-verifier test. Both
    # snapshots retain the same binding to the full original frozen input.
    before = screen_a2(baseline, a1, minimum_identifiability_score=0, llm_top_n_per_theme=2)
    after = screen_a2(source, a1, minimum_identifiability_score=0, llm_top_n_per_theme=2)
    assert before.review_symbols and after.review_symbols == before.review_symbols
    assert after.monitor_symbols == before.monitor_symbols
    for old, new in zip(before.decisions, after.decisions, strict=True):
        for key in ['status', 'route', 'reason_codes', 'identifiability_score', 'sent_to_llm']:
            assert new.get(key) == old.get(key)
        for key, factor in new['a2_factor_scores'].items():
            assert factor['score'] == old['a2_factor_scores'][key]['score']
