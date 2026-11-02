from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace

import pytest

from liangjian_funnel.pipeline.research.common import FrozenInputSnapshot, _capture_snapshot_recipe
from liangjian_funnel.pipeline.team_v2_research_time import research_times
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.publication_receipt import object_hash
from liangjian_funnel.runtime.same_day_recovery_time import recovery_time_context

CAL = ExchangeTradingCalendar()
def at(value): return datetime.fromisoformat(value + '+08:00')
NOW = at('2026-11-03T07:20:00')


def root():
    stamp = at('2026-11-03T07:10:00')
    data = dict(MARKET_DATA_AS_OF=stamp.isoformat(),
        snapshot_manifest=dict(as_of=stamp.isoformat(), frozen=True), g0_symbols=['600000.SH'],
        RECENT_DAILY_BARS={'600000.SH': [dict(date_ms=at('2026-11-02T00:00:00').timestamp()*1000)]})
    return FrozenInputSnapshot('snapshot-preopen', data=data, as_of=stamp)


def child(base):
    overlay = dict(NEWS_HEAT_SNAPSHOT={}, A2_STOCK_NEWS_SCOPE=[])
    recipe = dict(base_snapshot_hash=base.snapshot_hash, stage='A2', **overlay)
    digest = object_hash(recipe)
    result = FrozenInputSnapshot(base.snapshot_id + ':a2:' + digest[:12],
        data={**base.data, **overlay}, snapshot_hash=digest, as_of=NOW)
    return _capture_snapshot_recipe(base, result, recipe_kind='A2_STOCK_NEWS',
        hash_recipe=recipe, data_recipe=overlay)


def test_only_explicit_recovery_context_accepts_native_preopen_and_resets():
    base = root(); derived = child(base)
    with pytest.raises(ValueError, match='CLOSED_SOURCE_REQUIRED'):
        research_times(derived, CAL)
    with recovery_time_context(SimpleNamespace(snapshot=base), CAL, clock=lambda: NOW):
        assert research_times(base, CAL) == (base.as_of, at('2026-11-02T15:00:00'))
        assert research_times(derived, CAL) == (NOW, at('2026-11-02T15:00:00'))
        assert 'CLOSED_SESSION_CAPTURE' not in base.data
    with pytest.raises(ValueError, match='CLOSED_SOURCE_REQUIRED'):
        research_times(derived, CAL)


@pytest.mark.parametrize('case', ['live_daily', 'stale_daily', 'missing_daily', 'hash',
    'manifest', 'market', 'closed_proof', 'future', 'after_open', 'old_date'])
def test_invalid_root_cannot_enable_recovery(case):
    base = root(); data = deepcopy(dict(base.data)); stamp = base.as_of
    if case in ('live_daily', 'stale_daily'):
        value = '2026-11-03T00:00:00' if case == 'live_daily' else '2026-10-30T00:00:00'
        data['RECENT_DAILY_BARS']['600000.SH'][0]['date_ms'] = at(value).timestamp()*1000
    elif case == 'missing_daily': data['RECENT_DAILY_BARS'] = {}
    elif case == 'manifest': data['snapshot_manifest']['as_of'] = NOW.isoformat()
    elif case == 'market': data['MARKET_DATA_AS_OF'] = '2026-11-02T15:00:00+08:00'
    elif case == 'closed_proof': data['CLOSED_SESSION_CAPTURE'] = {}
    elif case == 'future': stamp = at('2026-11-03T07:21:00')
    elif case == 'after_open': stamp = at('2026-11-03T09:26:00')
    elif case == 'old_date': stamp = at('2026-11-02T07:10:00')
    bad = FrozenInputSnapshot(base.snapshot_id, data=data, as_of=stamp,
        snapshot_hash='f'*64 if case == 'hash' else '')
    with pytest.raises(ValueError):
        with recovery_time_context(SimpleNamespace(snapshot=bad), CAL, clock=lambda: NOW): pass


@pytest.mark.parametrize('case', ['parent', 'recipe', 'delta', 'terminal', 'timestamp', 'future'])
def test_derived_recipe_tampering_is_rejected(case):
    base = root(); derived = child(base); chain = deepcopy(derived.proof_chain)
    if case == 'parent': chain[0]['parent_snapshot_hash'] = 'f'*64
    elif case == 'recipe': chain[0]['hash_recipe']['stage'] = 'A3'
    elif case == 'delta': chain[0]['data_recipe']['A2_STOCK_NEWS_SCOPE'] = ['other']
    elif case == 'terminal': derived = replace(derived, data={**derived.data, 'other': True})
    elif case == 'timestamp': derived = replace(derived, as_of=at('2026-11-03T07:19:00'))
    elif case == 'future':
        chain[0]['as_of'] = at('2026-11-03T07:21:00').isoformat()
        derived = replace(derived, as_of=at('2026-11-03T07:21:00'))
    derived = replace(derived, proof_chain=chain)
    with recovery_time_context(SimpleNamespace(snapshot=base), CAL, clock=lambda: NOW):
        with pytest.raises(ValueError): research_times(derived, CAL)


def test_live_deadline_is_rechecked_for_each_stage():
    clock = [NOW]; base = root()
    with recovery_time_context(SimpleNamespace(snapshot=base), CAL, clock=lambda: clock[0]):
        clock[0] = at('2026-11-03T09:26:00')
        with pytest.raises(ValueError, match='WINDOW_CLOSED'): research_times(child(base), CAL)
