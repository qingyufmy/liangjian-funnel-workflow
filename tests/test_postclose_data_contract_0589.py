"""WP4 boundary contracts: frozen native bars, daily A1 and explicit A6.

All acquisition is a local callable. No Workflow/Store application is created,
and no provider, model or production endpoint is invoked.
"""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.indicator_history import prepare_pattern_history
from liangjian_funnel.runtime.regeneration_data_limits import (
    source_gap_detail, project_data_limited, a6_gap_requirements,
    gap_reviewed_ids, restrict_unreviewed,
)
from liangjian_funnel.workflow import WorkflowApplication, WorkflowError

TZ = ZoneInfo('Asia/Shanghai')
AT = datetime(2026, 10, 22, 16, tzinfo=TZ)
CAL = SimpleNamespace(previous_trading_day=lambda day: day - timedelta(days=1),
    is_trading_day=lambda day: day.weekday() < 5)
SYMBOL = '600201.SH'


def plan(pid='p1', symbol=SYMBOL):
    return dict(plan_id=pid, symbol=symbol, expires_at='2026-10-23T15:00:00+08:00')


def native_bars(symbol=SYMBOL):
    result = []
    for hour in (9, 13):
        start = AT.replace(hour=hour, minute=30 if hour == 9 else 0)
        for index in range(1, 25):
            result.append(MinuteBar(symbol=symbol, interval='5m',
                bar_end=start + timedelta(minutes=5 * index), open=10, high=11,
                low=9, close=10, volume=1000, amount=10000,
                source_id='MOOTDX:127.0.0.1:7709'))
    return result


def preparation(tmp_path, bars, *, plans=None, complete=True):
    calls = []
    store = MinuteBarStore(tmp_path)
    def fetch(*args, **kwargs):
        calls.append((args, kwargs))
        return dict(complete=complete, bars=bars)
    result = prepare_pattern_history(plans if plans is not None else [plan()], now=AT,
        calendar=CAL, minute_store=store, provider=SimpleNamespace(fetch_bars=fetch))
    return result, store, calls


@pytest.mark.parametrize('case', ['short', 'duplicate', 'wrong_symbol', 'wrong_interval',
    'wrong_day', 'unclosed', 'synthetic_quote'])
def test_no_non_native_or_incomplete_history_can_be_ready_or_written(tmp_path, case):
    bars = native_bars()
    complete = True
    if case == 'short': bars.pop()
    elif case == 'duplicate': bars[-1] = bars[0]
    elif case == 'wrong_symbol': bars[0] = bars[0].model_copy(update={'symbol': '600000.SH'})
    elif case == 'wrong_interval': bars[0] = bars[0].model_copy(update={'interval': '1m'})
    elif case == 'wrong_day': bars = [bar.model_copy(update={'bar_end': bar.bar_end - timedelta(days=1)}) for bar in bars]
    elif case == 'unclosed': complete = False
    elif case == 'synthetic_quote': bars[0] = bars[0].model_copy(update={'evidence_kind': 'SYNTHETIC_QUOTE'})
    result, store, calls = preparation(tmp_path, bars, complete=complete)
    assert result['status'] == 'DATA_LIMITED'
    assert store.load_latest(SYMBOL, '5m', limit=96) == ()
    assert len(calls) == 1


def test_zero_plan_domain_is_not_a_prepared_opening_prefix(tmp_path):
    result, _, calls = preparation(tmp_path, [], plans=[])
    assert result['status'] != 'READY'
    assert result['plans'] == [] and calls == []


def test_exact_native_history_uses_target_previous_session_and_one_request(tmp_path):
    bars = native_bars()
    result, store, calls = preparation(tmp_path, bars, plans=[plan('p1'), plan('p2')])
    assert result['status'] == 'READY' and len(calls) == 1
    assert calls[0] == ((SYMBOL, '5m', 48), {'as_of': AT.replace(hour=15)})
    assert all(row['source_trade_date'] == '2026-10-22' and row['target_trade_date'] == '2026-10-23'
        and row['available_5m_count'] == 48 for row in result['plans'])
    assert list(store.load_latest(SYMBOL, '5m', limit=96)) == bars
    original_rows = [bar.model_dump(mode='json') for bar in bars]
    expected_sha = hashlib.sha256(json.dumps(original_rows, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert all(row['native_rowset_sha256'] == expected_sha for row in result['plans'])
    assert all(row['source_ids'] == ['MOOTDX:127.0.0.1:7709'] for row in result['plans'])


def test_strict_history_conflict_rolls_back_without_overwriting_frozen_bar(tmp_path):
    store = MinuteBarStore(tmp_path)
    original = native_bars()[0]
    store.write([original])
    changed = native_bars(); changed[0] = original.model_copy(update={'close': 10.5})
    result = prepare_pattern_history([plan()], now=AT, calendar=CAL, minute_store=store,
        provider=SimpleNamespace(fetch_bars=lambda *a, **k: dict(complete=True, bars=changed)))
    assert result['status'] == 'DATA_LIMITED'
    assert store.load_latest(SYMBOL, '5m', limit=96) == (original,)


def test_cached_synthetic_history_cannot_skip_native_fetch_and_claim_ready(tmp_path):
    store = MinuteBarStore(tmp_path)
    bars = [bar.model_copy(update={'evidence_kind': 'SYNTHETIC_QUOTE',
        'source_id': 'TENCENT:qt.gtimg.cn:RISK_ONLY'}) for bar in native_bars()]
    store.write(bars)
    calls = []
    result = prepare_pattern_history([plan()], now=AT, calendar=CAL, minute_store=store,
        provider=SimpleNamespace(fetch_bars=lambda *a, **k: calls.append((a, k)) or dict(complete=False, bars=[])))
    assert result['status'] != 'READY'


def test_error_for_second_symbol_cannot_reuse_previous_symbols_availability():
    first = native_bars()
    def load(symbol, *a, **k):
        if symbol != SYMBOL: raise OSError('controlled unreadable cache')
        return first
    result = prepare_pattern_history([plan(), plan('p2', '600000.SH')], now=AT, calendar=CAL,
        minute_store=SimpleNamespace(load_latest=load),
        provider=SimpleNamespace(fetch_bars=lambda *a, **k: pytest.fail('unexpected fetch')))
    assert result['plans'][0]['status'] == 'READY'
    assert result['plans'][1]['status'] == 'DATA_LIMITED'
    assert result['plans'][1]['available_5m_count'] == 0


def daily_app(tmp_path, active):
    pointer = [active]
    calls = []
    def refresh(**kwargs):
        calls.append(kwargs)
        pointer[0] = SimpleNamespace(generation_id='new-sealed', as_of=kwargs['now'])
        return dict(status='READY')
    app = SimpleNamespace(a1_registry=SimpleNamespace(get_active_generation=lambda: pointer[0]),
        settings=SimpleNamespace(root=tmp_path), trading_calendar=CAL, run_a1_maintenance=refresh)
    return app, pointer, calls


def test_daily_refresh_calls_real_existing_incremental_and_does_not_relabel_old_object(tmp_path,monkeypatch):
    import liangjian_funnel.workflow as workflow
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return AT.astimezone(tz) if tz else AT.replace(tzinfo=None)
    monkeypatch.setattr(workflow,'datetime',Clock)
    old = SimpleNamespace(generation_id='old-sealed', as_of=AT - timedelta(days=1))
    app, pointer, calls = daily_app(tmp_path, old)
    WorkflowApplication._ensure_daily_a1(app, AT)
    assert len(calls) == 1 and calls[0] == dict(now=AT, mode='INCREMENTAL', daily_refresh=True,
        source_trade_date='2026-10-22')
    assert pointer[0].generation_id == 'new-sealed' and old.as_of == AT - timedelta(days=1)


@pytest.mark.parametrize('future', [AT + timedelta(minutes=1), AT + timedelta(days=1)])
def test_future_a1_generation_is_not_accepted_as_current_closed_source(tmp_path, future):
    app, _, calls = daily_app(tmp_path, SimpleNamespace(generation_id='future-sealed', as_of=future))
    try:
        result = WorkflowApplication._ensure_daily_a1(app, AT)
    except WorkflowError:
        return
    assert result.get('status') != 'CURRENT', 'future generation must not satisfy the current closed-day contract'
    assert calls


def test_daily_a1_pipeline_exception_emits_failure_alert_and_stops_downstream(tmp_path, monkeypatch):
    import liangjian_funnel.runtime.provider_health as health
    app, pointer, _ = daily_app(tmp_path, SimpleNamespace(generation_id='old', as_of=AT-timedelta(days=1)))
    def failed(**kwargs): raise RuntimeError('controlled data preparation failure')
    app.run_a1_maintenance = failed
    alerts = []
    monkeypatch.setattr(health, 'queue_internal', lambda *a, **kw: alerts.append(kw))
    with pytest.raises(Exception): WorkflowApplication._ensure_daily_a1(app, AT)
    assert len(alerts) == 1 and alerts[0]['kind'] == 'A1_DAILY_REFRESH_FAILED'
    assert pointer[0].generation_id == 'old'


@pytest.mark.parametrize('manifest', [{}, {'source_failures': []}, {'source_failures': None}])
def test_natural_publication_missing_source_proof_is_limited_not_default_eligible(manifest):
    data = dict(snapshot_manifest=manifest)
    before = deepcopy(data)
    detail = source_gap_detail(data, SYMBOL, strict=False)
    assert detail['a6_explicit_review_required'] is True and detail['maximum_risk_unit'] == 'PROBE'
    out = project_data_limited({'symbol': SYMBOL, 'risk_unit': 'STANDARD'}, detail)
    assert out['DATA_LIMITED'] is True and out['risk_unit'] == 'PROBE' and data == before


@pytest.mark.parametrize('stamp', [None, 'bad-time', '2026-10-21T16:00:00+08:00',
    '2026-10-23T16:00:00+08:00', '2026-10-22T14:59:00+08:00'])
def test_source_failure_proof_cannot_claim_other_or_unclosed_session_complete(stamp):
    data = dict(snapshot_manifest=dict(as_of=stamp, source_failures={}))
    detail = source_gap_detail(data, SYMBOL, strict=False, expected_source_date=AT.date())
    assert detail is not None and detail['status'] == 'DATA_LIMITED'
    assert detail['a6_explicit_review_required'] is True


def test_current_closed_failure_manifest_without_this_symbol_gap_is_not_falsely_limited():
    data = dict(snapshot_manifest=dict(as_of=AT.isoformat(), source_failures={'600000.SH': ['OTHER_GAP']}))
    assert source_gap_detail(data, SYMBOL, strict=False, expected_source_date=AT.date()) is None


def test_natural_team_publication_preserves_a1_proof_when_history_gap_is_added(tmp_path, monkeypatch):
    from test_w2_shadow_inputs_publication import publisher_fixture, AT as PUB_AT, SYMBOL as PUB_SYMBOL
    app, result, data, _ = publisher_fixture()
    # Geometry is deliberately stubbed: this tests the actual publication gap
    # projection, not A3 model judgment or full research/auction acceptance.
    audit = SimpleNamespace(stage='A3', thinking_variant='team_v2', output={}, audit_path=None)
    result = replace(result, lanes=(replace(result.lanes[0], stages=(audit,)),))
    app.settings = SimpleNamespace(root=tmp_path, workflow_output_dir=tmp_path,
        research_primary_lane_id='lane_1', publish_comparison_lanes=False)
    data['snapshot_manifest']['source_failures'] = {PUB_SYMBOL: ['A1:PDF_MISSING']}
    before = deepcopy(data)
    monkeypatch.setattr('liangjian_funnel.pipeline.team_v2_plan_geometry.project_stop_rr_geometry',
        lambda payload, **kwargs: dict(status='PUBLISHABLE', payload=payload, audit={'reason_code': 'OK'}))
    app.minute_store = MinuteBarStore(tmp_path / 'minute')
    app.market_data = SimpleNamespace(fetch_bars=lambda *a, **kw: dict(complete=False, bars=[]))
    alerts = []
    monkeypatch.setattr('liangjian_funnel.runtime.provider_health.queue_internal',
        lambda *a, **kw: alerts.append(kw))
    summary = WorkflowApplication._publish_plans(app, result, slot='close', now=PUB_AT, snapshot_data=data)
    payload = app.store.batch[0]['payload']
    detail = payload['source_data_limits']
    assert payload['DATA_LIMITED'] is True and payload['risk_unit'] == 'PROBE'
    assert 'A1:PDF_MISSING' in detail['reason_codes']
    assert detail['source_as_of'] == data['snapshot_manifest']['as_of']
    assert len(detail['source_failures_sha256']) == 64
    assert detail['history_readiness']['status'] == 'DATA_LIMITED'
    assert detail['a6_explicit_review_required'] is True
    assert summary['pattern_preparation']['status'] == 'DATA_LIMITED' and len(alerts) == 1
    assert data == before


def test_data_limited_actual_plan_fact_needs_exact_each_code_and_ref_before_any_eligibility():
    from liangjian_funnel.runtime.risk_engine import RiskEnvelope
    codes = ['A1:PDF_MISSING', 'PATTERN_HISTORY_48_INCOMPLETE']
    detail = dict(status='DATA_LIMITED', authority='0589_PLAN_HISTORY_GAPS', reason_codes=codes,
        a6_explicit_review_required=True, maximum_risk_unit='PROBE')
    e = RiskEnvelope('p1', SYMBOL, '2026-10-23', True, 'PROBE', Decimal(100), 1, 100,
        (), (), purpose='PLANNING', plan_risk_unit='PROBE', degradation_tags=('DATA_LIMITED',))
    catalog = {'items': [dict(id='PLAN:p1', category='PLAN', value=dict(plan_id='p1',
        publication_facts=dict(source_data_limits=detail)))]}
    requirements = a6_gap_requirements(catalog, (e,))
    for output in ({}, {'envelope_adjustments': []}, {'envelope_adjustments': [dict(plan_id='p1',
            reason_code='DATA_LIMITED_REVIEWED', supporting_refs=['PLAN:p1'], what_would_change_my_mind=[codes[0]])]}):
        reviewed = gap_reviewed_ids(output, requirements)
        blocked, = restrict_unreviewed((e,), reviewed)
        assert not blocked.eligible and blocked.max_shares_cap == 0 and blocked.risk_unit == 'NONE'
    explicit = dict(envelope_adjustments=[dict(plan_id='p1', reason_code='DATA_LIMITED_REVIEWED',
        supporting_refs=['PLAN:p1'], what_would_change_my_mind=[';'.join(codes)])])
    assert restrict_unreviewed((e,), gap_reviewed_ids(explicit, requirements)) == (e,)
