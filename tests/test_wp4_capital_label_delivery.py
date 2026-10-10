"""A-LABEL is observation transport, never a new capital factor policy."""
import copy
import json
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.a2_market import WINDOWS, _content_hash, build_capital_flow_snapshot
from liangjian_funnel.data.capital_source_policy import inspect_legacy_capital_weighting
from liangjian_funnel.pipeline.a2_features import build_a2_feature_snapshot
from liangjian_funnel.pipeline.research.common import _project_capital_flow
from liangjian_funnel.pipeline.research import common
from liangjian_funnel.pipeline.research.common import FrozenInputSnapshot, _prompt_replacements
from liangjian_funnel.pipeline.prompts import PromptRepository
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


def test_model_projection_excludes_audit_label_and_preserves_original_bytes():
    snapshot = raw(['today'])
    before = copy.deepcopy(snapshot)
    projected = _project_capital_flow(snapshot, {SYMBOLS[0]})
    assert set(projected['by_symbol']) == {SYMBOLS[0]}
    assert 'weighting_observation' not in projected['by_symbol'][SYMBOLS[0]]
    assert 'weighting_observation_input_hash' not in projected
    assert 'weighting_observation_status' not in projected
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
    audit = inspect_legacy_capital_weighting(snapshot)
    assert audit['status'] == 'DATA_LIMITED'
    assert all(row['normalization_state'] == 'DATA_LIMITED'
               for row in audit['by_symbol'].values())


@pytest.mark.parametrize('windows', [['today'], [key for key, _, _ in WINDOWS]])
@pytest.mark.parametrize('scope', [None, set(), {SYMBOLS[0]}, set(SYMBOLS)])
@pytest.mark.parametrize('template', ['agent_2_theme_sentiment_transport_v2.txt', 'agent_2_theme_sentiment_v2.txt'])
def test_a_label_a2_rendered_prompt_is_byte_identical_to_deployed_projection(monkeypatch, windows, scope, template):
    """Golden projection copied from deployed adafe50, not a second score oracle."""
    capital = raw(windows)
    frozen_before = copy.deepcopy(capital)
    snapshot = FrozenInputSnapshot(snapshot_id='fixed-capital-prompt', as_of=NOW,
        data={'CAPITAL_FLOW_SNAPSHOT': capital,
              'snapshot_manifest': {'trade_date': '2026-10-09', 'frozen_input_hash': 'fixed'}})
    bundle = PromptRepository(Path(__file__).resolve().parents[1] / 'prompts').bundle()
    monkeypatch.setitem(common.STAGE_PROMPT_FILES, 'A2', template)
    upstream = {'active_research_pool': [{'symbol': symbol} for symbol in SYMBOLS]}
    actual = _prompt_replacements(bundle, 'A2', snapshot, upstream, projection_symbols=scope)
    rendered_actual = bundle.render_stage('A2', actual).encode('utf-8')

    def deployed_projection(value, symbols):
        if not isinstance(value, dict):
            return value
        projected = dict(value)
        by_symbol = value.get('by_symbol')
        if isinstance(by_symbol, dict) and symbols is not None:
            projected['by_symbol'] = common._filter_symbol_mapping(by_symbol, symbols)
            projected['prompt_symbol_count'] = len(projected['by_symbol'])
            projected['full_symbol_count'] = len(by_symbol)
        return projected

    assert common._project_capital_flow(capital, scope) == deployed_projection(capital, scope)
    monkeypatch.setattr(common, '_project_capital_flow', deployed_projection)
    expected = _prompt_replacements(bundle, 'A2', snapshot, upstream, projection_symbols=scope)
    assert rendered_actual == bundle.render_stage('A2', expected).encode('utf-8')
    assert json.dumps(actual, sort_keys=True, default=str) == json.dumps(expected, sort_keys=True, default=str)
    assert 'weighting_observation' not in rendered_actual.decode('utf-8')
    assert capital == frozen_before


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


@pytest.mark.parametrize('template', ['agent_2_theme_sentiment_transport_v2.txt', 'agent_2_theme_sentiment_v2.txt'])
def test_a_label_in_actual_review_factor_context_is_not_model_input(monkeypatch, template):
    """The factor-bearing A2 review path, not just the capital raw projection."""
    scope = set(SYMBOLS)
    capital = raw(['today'])
    contexts = {symbol: {'symbol': symbol, 'theme_id': 'theme-main',
        'deterministic_status': 'WATCH', 'a2_factor_scores': {'capital_flow': {
            'score': capital['by_symbol'][symbol]['capital_flow_score'],
            'weighting_observation': inspect_legacy_capital_weighting(capital)['by_symbol'][symbol],
        }}} for symbol in SYMBOLS}
    data = {'CAPITAL_FLOW_SNAPSHOT': capital, 'A2_BOTTLENECK_CONTEXT': contexts,
        'CAPITAL_FLOW_WEIGHTING_AUDIT': inspect_legacy_capital_weighting(capital),
        'snapshot_manifest': {'trade_date': '2026-10-09', 'frozen_input_hash': 'fixed'}}
    baseline = copy.deepcopy(data)
    baseline.pop('CAPITAL_FLOW_WEIGHTING_AUDIT')
    for row in baseline['A2_BOTTLENECK_CONTEXT'].values():
        row['a2_factor_scores']['capital_flow'].pop('weighting_observation')
    bundle = PromptRepository(Path(__file__).resolve().parents[1] / 'prompts').bundle()
    monkeypatch.setitem(common.STAGE_PROMPT_FILES, 'A2', template)
    upstream = {'active_research_pool': [{'symbol': symbol} for symbol in SYMBOLS]}
    def render(payload):
        snapshot = FrozenInputSnapshot(snapshot_id='fixed-a2-factor-prompt', as_of=NOW, data=payload)
        return bundle.render_stage('A2', _prompt_replacements(bundle, 'A2', snapshot,
            upstream, projection_symbols=scope)).encode('utf-8')
    assert render(data) == render(baseline)
    assert b'weighting_observation' not in render(data)
    assert 'weighting_observation' in contexts[SYMBOLS[0]]['a2_factor_scores']['capital_flow']
