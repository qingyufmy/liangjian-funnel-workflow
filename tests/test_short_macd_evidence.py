from types import SimpleNamespace

import pytest

from liangjian_funnel.pipeline.macd_evidence import macd_evidence
from liangjian_funnel.pipeline.factors import _daily_volume_evidence
from liangjian_funnel.pipeline.research import _a2_watch_row_research_eligible, _is_rotation_reserve
from liangjian_funnel.workflow import _compact_factor


def test_production_compaction_keeps_short_macd_and_volume_without_inventing_legacy_fields():
    daily = {'macd_short': {'available': True, 'parameters': [5, 10, 5], 'dif': 1, 'dea': 2},
             'volume_evidence': {'available': True, 'ratio_to_prior_five': .95}}
    compact = _compact_factor({'timeframes': {'daily': daily}})['timeframes']['daily']
    assert compact['macd_short'] == daily['macd_short']
    assert compact['volume_evidence'] == daily['volume_evidence']
    legacy = _compact_factor({'timeframes': {'daily': {'macd': {}}}})['timeframes']['daily']
    assert 'macd_short' not in legacy


def test_short_macd_matches_independent_ema_and_keeps_legacy_parameters():
    prices = [100 + i * .7 for i in range(60)] + [140, 138, 137, 136]
    short = macd_evidence(prices, parameters=(5, 10, 5))
    fast = slow = prices[0]
    signal = 0
    for price in prices[1:]:
        fast += (price - fast) / 3
        slow += (price - slow) * 2 / 11
        dif = fast - slow
        signal += (dif - signal) / 3
    assert short['dif'] == pytest.approx(dif, abs=1e-6)
    assert short['dea'] == pytest.approx(signal, abs=1e-6)
    assert short['dif'] < short['dea']
    assert short['parameters'] == [5, 10, 5]
    assert macd_evidence(prices)['parameters'] == [12, 26, 9]
    assert not macd_evidence(prices[:20], parameters=(5, 10, 5))['available']
    with pytest.raises(ValueError):
        macd_evidence(prices, parameters=(10, 5, 5))


def test_volume_rebound_is_not_five_day_expansion():
    bars = [SimpleNamespace(volume=x) for x in [27280204,29521311,26730062,21858225,19356458,23926368]]
    evidence = _daily_volume_evidence(bars)
    assert evidence['ratio_to_prior_five'] == pytest.approx(.959001416)
    assert evidence['ratio_to_previous'] > 1.2
    assert not evidence['baseline_includes_current']
    assert not _daily_volume_evidence(bars[:3])['available']


def test_strong_trend_requires_technical_review_not_automatic_entry():
    row = {'strong_trend_observation': True, 'top_rotation_theme': False,
           'research_observation_scope': 'REQUIRES_A3_A4_CONFIRMATION',
           'execution_permission': 'REQUIRES_A3_A4_CONFIRMATION'}
    assert _a2_watch_row_research_eligible(row)
    assert not _is_rotation_reserve(row)
    legacy = {**row, 'research_observation_scope': 'RESEARCH_ONLY_NO_AUTOMATIC_ENTRY'}
    assert _is_rotation_reserve(legacy)
