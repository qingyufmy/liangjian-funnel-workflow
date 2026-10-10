from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import threading

import pytest

from test_wp1_ablation_engine import fixture, NOW
from liangjian_funnel.runtime import strategies, shadow_variants as shadow
from liangjian_funnel.evaluation.ablation import engine


def item(inputs=True, profile="TREND_MA5"):
    plan, bars, context = fixture()
    plan["strategy_profile"] = profile
    plan["trend_entry_rule_version"] = "trend-ma5/2"
    if inputs:
        plan["strategy_facts"] = {"shadow_inputs": {
            "schema_version": "a4-shadow-inputs/1", "daily_ma5": 10.5, "atr14": 1,
            "previous_daily_closes": [10.3] * 4,
            "previous_daily_close_dates": ["2026-09-23", "2026-09-24", "2026-09-25", "2026-09-28"],
            "daily_as_of": "2026-09-29T15:00:00+08:00", "atr_source_hash": "a" * 64,
        }}
    baseline = strategies.evaluate_strategy(plan, bars, now=NOW, decision_time=NOW,
        market_context=context).model_dump(mode="json")
    return dict(plan_id="p", plan=plan, bars=bars, baseline=baseline, now=NOW,
        decision_time=NOW, market_context=context)


def fake_result(action="START_CONFIRMATION", status="OK"):
    return dict(action=action, state="CONFIRMING", reason_codes=["ZONE"],
        met_conditions=[], unmet_conditions=["A3_PULLBACK_ZONE"], veto_conditions=[],
        reference_price=10.5, ablation={"status": status, "effective_zone": {"low": 9, "high": 12}})


def test_fixed_mapping_matches_matrix_and_is_immutable():
    assert [v.variant_id for v in shadow.SHADOW_VARIANTS_V1] == [f"V{i}" for i in range(1, 7)]
    assert shadow.SHADOW_VARIANTS_V1[0].matrix_scenario == "SCAN:MA5_ATR:1"
    assert shadow.SHADOW_VARIANTS_V1[3].matrix_scenario == "SCAN:CROSS:MA5_ATR:1:TREND_CONFIRM_WITHIN_N:6"
    assert shadow.SHADOW_VARIANTS_V1[4].parameters()["pullback_length"] == 2
    with pytest.raises((AttributeError, TypeError)):
        shadow.SHADOW_VARIANTS_V1[0].variant_id = "OTHER"


def test_missing_inputs_does_not_use_old_geometry_or_fake_buy():
    called = []
    service = shadow.ShadowVariantEngine(evaluator=lambda *a, **k: called.append(1))
    data = item(False)
    result = service.evaluate_minute([data], minute=NOW)
    assert len(result["signals"]) == 6 and not called
    assert all(row["status"] == "DATA_LIMITED" and row["variant_action"] is None for row in result["signals"])
    assert result["minute_summary"]["first_trigger_count"] == 0


@pytest.mark.parametrize("bad", ["FUTURE", "HASH", "FOUR_DATES", "MA5_CONFLICT"])
def test_invalid_provenance_is_not_eligible(bad):
    data = item()
    values = data["plan"]["strategy_facts"]["shadow_inputs"]
    if bad == "FUTURE": values["daily_as_of"] = "2026-10-01T15:00:00+08:00"
    if bad == "HASH": values["atr_source_hash"] = "not-a-hash"
    if bad == "FOUR_DATES": values["previous_daily_close_dates"] = ["2026-09-28"] * 4
    if bad == "MA5_CONFLICT": values["daily_ma5"] = 12
    result = shadow.ShadowVariantEngine().evaluate_minute([data], minute=NOW)
    assert all(row["status"] == "DATA_LIMITED" for row in result["signals"])


def test_morning_observation_can_use_closed_prior_day_without_faking_source_time():
    data = item()
    values = data["plan"]["strategy_facts"]["shadow_inputs"]
    values["daily_as_of"] = NOW.replace(hour=9, minute=26).isoformat()
    values["last_closed_daily_bar_end"] = "2026-09-29T15:00:00+08:00"
    _, _, reason = shadow.prepare_shadow_plan(data["plan"], decision_time=NOW)
    assert reason is None


def test_same_day_unclosed_daily_input_is_never_accepted():
    data = item()
    values = data["plan"]["strategy_facts"]["shadow_inputs"]
    values["daily_as_of"] = NOW.replace(hour=9, minute=26).isoformat()
    values["previous_daily_close_dates"][-1] = NOW.date().isoformat()
    _, _, reason = shadow.prepare_shadow_plan(data["plan"], decision_time=NOW)
    assert reason == "PRECEDING_CLOSE_DATES_INVALID_OR_FUTURE"


@pytest.mark.parametrize("profile,count", [("TREND_MA5", 6), ("MA520_SWING", 3), ("LEADER_INTRADAY", 0), ("UNKNOWN", 0)])
def test_profile_applicability(profile, count):
    calls = []
    def evaluate(*args, **kwargs):
        calls.append(kwargs["variant"])
        return fake_result()
    result = shadow.ShadowVariantEngine(evaluator=evaluate).evaluate_minute([item(profile=profile)], minute=NOW)
    assert len(calls) == count
    assert result["minute_summary"]["evaluated_variant_count"] == count
    if profile == "MA520_SWING": assert all("research_sequence_model" not in row for row in calls)


def test_exception_and_malicious_evaluator_cannot_mutate_production_inputs():
    data = item()
    before = deepcopy(data)
    def broken(plan, bars, **kwargs):
        plan["symbol"] = "BAD"
        bars.clear()
        kwargs["baseline"]["action"] = "BUY_SIGNAL"
        raise ValueError("fault")
    result = shadow.ShadowVariantEngine(evaluator=broken).evaluate_minute([data], minute=NOW)
    assert data == before
    assert all(row["status"] == "ERROR" for row in result["signals"])
    assert result["minute_summary"]["first_trigger_count"] == 0


def test_real_engine_is_reused_and_production_globals_stay_unchanged():
    data = item()
    before = deepcopy(data)
    globals_before = dict(vars(strategies))
    rows = shadow.ShadowVariantEngine().evaluate_minute([data], minute=NOW)["signals"]
    normalized, _, reason = shadow.prepare_shadow_plan(data["plan"], decision_time=NOW)
    assert reason is None
    for spec, row in zip(shadow.SHADOW_VARIANTS_V1, rows):
        expected = engine._evaluate_with_baseline(normalized, data["bars"],
            now=NOW, decision_time=NOW, market_context=data["market_context"],
            baseline=data["baseline"], disabled=(), variant=spec.parameters())
        assert row["status"] == expected["ablation"]["status"]
        assert row["variant_action"] == expected["action"]
        assert row["baseline_action"] == data["baseline"]["action"]
    assert data == before
    assert all(vars(strategies)[key] is value for key, value in globals_before.items())


def test_complete_fixture_first_trigger_set_matches_actual_matrix_evaluator():
    from test_wp1_atomic_scans import phases
    plan, bars, context = phases([(10.4,10.3,10.2,200),(10.3,10.1,10,100),
        (10.1,10.2,10.05,120),(10.2,10.4,10.1,180),
        (10.4,10.35,10.15,150),(10.35,10.3,10.1,140)])
    plan['strategy_facts'] = item()['plan']['strategy_facts']
    baseline = strategies.evaluate_strategy(plan,bars,now=NOW,decision_time=NOW,
        market_context=context).model_dump(mode='json')
    data = dict(plan_id='golden-fixture',plan=plan,bars=bars,baseline=baseline,
        now=NOW,decision_time=NOW,market_context=context)
    prepared, _, limitation = shadow.prepare_shadow_plan(plan,decision_time=NOW)
    assert limitation is None
    expected = set()
    for spec in shadow.SHADOW_VARIANTS_V1:
        evaluation = engine._evaluate_with_baseline(prepared,bars,now=NOW,
            decision_time=NOW,market_context=context,baseline=baseline,
            disabled=(),variant=spec.parameters())
        assert evaluation['ablation']['status'] == 'OK'
        if evaluation['action'] in ('BUY_SIGNAL','ADD_SIGNAL'):
            expected.add((spec.variant_id,NOW.isoformat()))
    assert ('V4',NOW.isoformat()) in expected and ('V6',NOW.isoformat()) in expected
    service = shadow.ShadowVariantEngine()
    result = service.evaluate_minute([data],minute=NOW)
    actual = {(row['variant_id'],row['minute']) for row in result['signals']
        if row['event_kind'] == 'FIRST_TRIGGER'}
    assert actual == expected
    assert service.evaluate_minute([data],minute=NOW)['signals'] == []


def test_hard_deadline_drops_late_worker_and_never_expands_capacity():
    release = threading.Event()
    entered = threading.Event()
    def blocked(*args, **kwargs):
        entered.set()
        release.wait(1)
        return fake_result("BUY_SIGNAL")
    service = shadow.ShadowVariantEngine(evaluator=blocked)
    try:
        first = service.evaluate_minute([item()], minute=NOW, budget_seconds=.02)
        assert entered.is_set()
        assert first["signals"] == []
        assert first["minute_summary"]["status"] == "SHADOW_BUDGET_EXCEEDED"
        assert first["minute_summary"]["skipped_variant_count"] == 6
        second = service.evaluate_minute([item()], minute=NOW+timedelta(minutes=1), budget_seconds=.02)
        assert second["minute_summary"]["status"] == "SHADOW_WORKER_BUSY"
        assert second["signals"] == []
    finally:
        release.set()


def test_same_minute_concurrent_reentry_and_state_change_only():
    service = shadow.ShadowVariantEngine(evaluator=lambda *a, **k: fake_result("BUY_SIGNAL"))
    data = item()
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda _: service.evaluate_minute([data], minute=NOW), range(3)))
    assert sum(len(row["signals"]) for row in results) == 6
    assert all(row["event_kind"] == "FIRST_TRIGGER" for result in results for row in result["signals"])
    data.update(now=NOW+timedelta(minutes=1), decision_time=NOW+timedelta(minutes=1))
    result = service.evaluate_minute([data], minute=NOW+timedelta(minutes=1))
    assert result["signals"] == []
    service.evaluator = lambda *a, **k: fake_result()
    data.update(now=NOW+timedelta(minutes=2), decision_time=NOW+timedelta(minutes=2))
    result = service.evaluate_minute([data], minute=NOW+timedelta(minutes=2))
    assert all(row["event_kind"] == "STATE_CHANGE" for row in result["signals"])


def test_sink_failure_isolated_and_baseline_primary_not_unmet_projection():
    data = item()
    data["baseline"].update(reason_codes=["RAW_PRIMARY", "SECOND"], unmet_conditions=["DIFFERENT"])
    result = shadow.ShadowVariantEngine(evaluator=lambda *a, **k: fake_result()).evaluate_minute([data], minute=NOW)
    assert all(row["baseline_first_cause"] == "RAW_PRIMARY" for row in result["signals"])
    logs = []
    def failed(_): raise OSError("disk")
    receipts = shadow.emit_shadow_signals(result, failed, log_error=logs.append)
    assert len(logs) == len(receipts) == 6
    assert all(not row["ok"] for row in receipts)
    assert data["baseline"]["reason_codes"][0] == "RAW_PRIMARY"


def test_stale_minute_and_missing_baseline_do_not_create_evidence():
    data = item()
    data.pop("baseline")
    rows = shadow.ShadowVariantEngine().evaluate_minute([data], minute=NOW)["signals"]
    assert all(row["status"] == "DATA_LIMITED" for row in rows)
    assert all(row["baseline_action"] is None for row in rows)


def test_each_minute_keeps_actual_outer_baseline_separate_from_inner_and_events():
    data = item()
    data['actual_outer_baseline'] = {'action':'START_CONFIRMATION','state':'WARMUP',
        'reason':'A4_SESSION_WARMUP'}
    data['baseline_event_id'], data['baseline_event_sha256'] = 'original-event', 'a'*64
    data['baseline']['reason_codes'] = ['NO_CLOSED_15M']
    service = shadow.ShadowVariantEngine(evaluator=lambda *a,**k:fake_result())
    first = service.evaluate_minute([data],minute=NOW)
    outer = first['minute_summary']['baseline_records']
    assert len(outer) == 1 and outer[0]['first_cause'] == 'A4_SESSION_WARMUP'
    assert outer[0]['first_cause_source'] == 'OUTER_REASON'
    assert outer[0]['event_id'] == 'original-event' and outer[0]['event_sha256'] == 'a'*64
    assert all(row['baseline_first_cause'] == 'NO_CLOSED_15M' for row in first['signals'])
    data.update(now=NOW+timedelta(minutes=1),decision_time=NOW+timedelta(minutes=1))
    following = service.evaluate_minute([data],minute=NOW+timedelta(minutes=1))
    assert following['signals'] == []
    assert len(following['minute_summary']['baseline_records']) == 1
    assert following['minute_summary']['baseline_records'][0]['minute'] != outer[0]['minute']


def test_missing_outer_evidence_and_busy_do_not_turn_inner_into_production_proof():
    data = item()
    result = shadow.ShadowVariantEngine().evaluate_minute([data],minute=NOW)
    row = result['minute_summary']['baseline_records'][0]
    assert row['field_status']['outer_baseline'] == 'MISSING'
    assert row['action'] is None and row['event_id'] is None and row['event_sha256'] is None
    service = shadow.ShadowVariantEngine()
    service._active = type('Busy',(),{'is_alive':lambda self:True})()
    data['actual_outer_baseline'] = {'action':'START_CONFIRMATION','reason_codes':[]}
    result = service.evaluate_minute([data],minute=NOW)
    row = result['minute_summary']['baseline_records'][0]
    assert result['minute_summary']['status'] == 'SHADOW_WORKER_BUSY'
    assert row['action'] == 'START_CONFIRMATION' and row['first_cause'] is None
    assert row['field_status']['reason_codes'] == 'EMPTY'


def test_monotonic_regression_does_not_emit_late_trigger():
    values = iter([10, 9, 9])
    result = shadow.ShadowVariantEngine(clock=lambda: next(values), evaluator=lambda *a, **k: fake_result("BUY_SIGNAL")).evaluate_minute([item()], minute=NOW)
    assert result["minute_summary"]["status"] == "SHADOW_CLOCK_REGRESSED"
    assert result["signals"] == []


@pytest.mark.parametrize("budget", [0, -1, 5.1, float("nan"), True])
def test_invalid_budget_is_rejected(budget):
    with pytest.raises(ValueError):
        shadow.ShadowVariantEngine().evaluate_minute([], minute=NOW, budget_seconds=budget)
