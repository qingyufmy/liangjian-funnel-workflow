from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.evaluation.ablation.conditions import condition_catalog
from liangjian_funnel.evaluation.ablation.engine import evaluate_window
from liangjian_funnel.runtime import strategies


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 30, 10, 0, tzinfo=TZ)


def fixture():
    plan = {
        "symbol": "600001.SH", "strategy_profile": "TREND_MA5",
        "stock_behavior_type": "TREND", "trade_date": "2026-09-30",
        "trigger_zone": {"low": 10.0, "high": 12.0}, "invalidation_level": 8.0,
        "daily_indicators": {"ma5": 10.5, "atr14": 1.0},
    }
    context = {"live_market_state": {
        "status": "READY", "decision": "ALLOW", "as_of": NOW.isoformat(),
        "trade_date": "2026-09-30",
    }}
    bars = []
    for i in range(30):
        price = 10.5 + i * .01
        bars.append({
            "symbol": plan["symbol"], "bar_end": NOW - timedelta(minutes=29-i),
            "open": price-.2, "close": price, "low": price-.2,
            "high": price+.2, "volume": 100.0, "amount": price*100,
        })
    return plan, bars, context


def run(plan, bars, context, **kwargs):
    return evaluate_window(plan, bars, now=NOW, decision_time=NOW,
                           market_context=context, **kwargs)


def test_baseline_is_formal_evaluator_and_does_not_mutate_inputs():
    plan, bars, context = fixture()
    frozen = deepcopy((plan, bars, context))
    formal = strategies.evaluate_strategy(plan, bars, now=NOW, decision_time=NOW,
                                          market_context=context).model_dump(mode="json")
    baseline = run(plan, bars, context)
    assert {key: baseline[key] for key in formal} == formal
    assert baseline["counterfactual"] is False
    assert (plan, bars, context) == frozen


def test_single_and_double_condition_ablation_recalculate_real_strategy():
    plan, bars, context = fixture()
    plan["trigger_zone"] = {"low": 9.0, "high": 9.5}
    plan["volume_overheated"] = True
    assert run(plan, bars, context)["action"] != "BUY_SIGNAL"
    assert run(plan, bars, context, disabled=("A3_PULLBACK_ZONE",))["action"] != "BUY_SIGNAL"
    result = run(plan, bars, context, disabled=("A3_PULLBACK_ZONE", "TREND_VOLUME_NOT_OVERHEATED"))
    assert result["action"] == "BUY_SIGNAL"
    assert "A3_PULLBACK_ZONE" in result["met_conditions"]
    assert "TREND_VOLUME_NOT_OVERHEATED" in result["met_conditions"]
    assert result["counterfactual"] is True


@pytest.mark.parametrize("disabled", [("A3_PULLBACK_ZONE",), ("TREND_VOLUME_NOT_OVERHEATED",)])
def test_hard_stop_and_t1_still_apply(disabled):
    plan, bars, context = fixture()
    plan["position"] = {"total_qty": 100, "sellable_qty": 0, "avg_cost": 10.0}
    plan["invalidation_level"] = 11.0
    formal = strategies.evaluate_strategy(plan, bars, now=NOW, decision_time=NOW,
                                          market_context=context).model_dump(mode="json")
    result = run(plan, bars, context, disabled=disabled)
    assert result["action"] == "FORCED_RISK_EXIT"
    assert result["reason_codes"] == formal["reason_codes"]
    assert "BLOCKED_T1" in result["reason_codes"]


def test_no_future_bars_even_in_counterfactual():
    plan, bars, context = fixture()
    bars.append({**bars[-1], "bar_end": NOW + timedelta(minutes=1)})
    result = run(plan, bars, context, disabled=("A3_PULLBACK_ZONE",))
    assert result["action"] == "DATA_BLOCK"
    assert "FUTURE_BAR_DETECTED" in result["reason_codes"]


def test_locked_limit_and_missing_evidence_cannot_be_disabled():
    plan, bars, context = fixture()
    plan["locked_limit_up"] = True
    assert run(plan, bars, context, disabled=("A3_PULLBACK_ZONE",))["action"] != "BUY_SIGNAL"
    context.clear()
    result = run(plan, bars, context, disabled=("LIVE_MARKET_ENTRY_PERMISSION",))
    assert result["action"] == "DATA_BLOCK"
    assert result["ablation"]["status"] == "DATA_LIMITED"


def test_only_valid_ready_market_permission_is_bypassed():
    plan, bars, context = fixture()
    context["live_market_state"]["decision"] = "BLOCK_NEW_ENTRY"
    assert run(plan, bars, context)["action"] == "START_CONFIRMATION"
    assert run(plan, bars, context, disabled=("LIVE_MARKET_ENTRY_PERMISSION",))["action"] == "BUY_SIGNAL"
    context["live_market_state"]["status"] = "READY_DEGRADED"
    result = run(plan, bars, context, disabled=("LIVE_MARKET_ENTRY_PERMISSION",))
    assert result["action"] != "BUY_SIGNAL"
    assert result["ablation"]["status"] == "DATA_LIMITED"


def test_concurrent_variants_do_not_pollute_production_globals():
    plan, bars, context = fixture()
    plan["trigger_zone"] = {"low": 9.0, "high": 9.5}
    original_globals = dict(strategies.evaluate_strategy.__globals__)
    def evaluate(i):
        return run(plan, bars, context, disabled=("A3_PULLBACK_ZONE",) if i % 2 else ())
    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(evaluate, range(24)))
    assert [r["action"] == "BUY_SIGNAL" for r in results] == [bool(i % 2) for i in range(24)]
    assert strategies.evaluate_strategy.__globals__ == original_globals


def test_catalog_rejects_protective_and_unimplemented_leader_routes():
    catalog = condition_catalog("LEADER_INTRADAY")
    assert all({"id", "supported", "reason"} <= row.keys() for row in catalog)
    assert all(not row["supported"] for row in catalog if row["id"].startswith("LEADER_L"))
    plan, bars, context = fixture()
    result = run(plan, bars, context, disabled=("HARD_STOP",))
    assert result["ablation"]["status"] == "UNSUPPORTED"
    assert result["action"] == run(plan, bars, context)["action"]


def test_zone_scan_requires_frozen_atr_and_preserves_no_chase():
    plan, bars, context = fixture()
    plan["trigger_zone"] = {"low": 9, "high": 9.5}
    result = run(plan, bars, context, variant={"zone_model": "MA5_ATR", "atr_multiplier": .5})
    assert result["action"] == "BUY_SIGNAL"
    plan["no_chase"] = 10.6
    result = run(plan, bars, context, variant={"zone_model": "MA5_ATR", "atr_multiplier": .5})
    assert result["action"] != "BUY_SIGNAL"
    assert "A4_LIVE_NO_CHASE_EXCEEDED" in result["reason_codes"]
    del plan["daily_indicators"]["atr14"]
    assert run(plan, bars, context, variant={"zone_model": "MA5_ATR", "atr_multiplier": .5})["ablation"]["status"] == "DATA_LIMITED"


def sequence_fixture():
    plan, _, context = fixture()
    plan["trend_entry_rule_version"] = "trend-ma5/2"
    phases = [(10.4, 10.3, 10.2, 200), (10.3, 10.1, 10, 100),
              (10.1, 10.2, 10.05, 120), (10.2, 10.18, 10.06, 110),
              (10.18, 10.15, 10.07, 110), (10.2, 10.4, 10.1, 180)]
    bars = []
    for group, (opening, close, low, volume) in enumerate(phases):
        for minute in range(5):
            bars.append({"symbol": plan["symbol"],
                         "bar_end": NOW-timedelta(minutes=29-group*5-minute),
                         "open": opening, "close": close, "low": low,
                         "high": max(opening, close)+.1,
                         "volume": volume/5, "amount": close*volume/5})
    return plan, bars, context


def test_contiguous_scan_cannot_reuse_an_older_or_gapped_sequence():
    plan, bars, context = sequence_fixture()
    baseline = run(plan, bars, context)
    assert baseline["action"] != "BUY_SIGNAL"
    current = run(plan, bars, context, variant={"sequence_window": 4})
    assert current["action"] == baseline["action"]
    assert {key: current[key] for key in baseline if key not in {"counterfactual", "ablation"}} == {
        key: value for key, value in baseline.items() if key not in {"counterfactual", "ablation"}
    }
    for window in (6, 8):
        expanded = run(plan, bars, context, variant={"sequence_window": window})
        assert expanded["action"] == baseline["action"]
        assert expanded["ablation"]["sequence_scan_model"] == "CONTIGUOUS4_LATEST_CONFIRM"
        assert expanded["ablation"]["sequence_candidate_count"] == 1
        assert expanded["closed_5m_end"] == NOW.isoformat()
    historical = run(plan, bars, context, variant={
        "sequence_window": 6, "sequence_scan_model": "ORDERED_SUBSEQUENCE_LATEST_CONFIRM",
    })
    assert historical["action"] == "BUY_SIGNAL"
    assert historical["ablation"]["sequence_scan_model"] == "ORDERED_SUBSEQUENCE_LATEST_CONFIRM"
    assert historical["ablation"]["sequence_candidate_count"] == 10
    for bar in bars[-5:]:
        bar.update(open=10.4, close=10.3, high=10.5, low=10.1, amount=10.3*bar["volume"])
    assert run(plan, bars, context, variant={"sequence_window": 8})["action"] != "BUY_SIGNAL"


def test_atomic_sequence_removal_does_not_remove_other_sequence_gates():
    plan, bars, context = sequence_fixture()
    result = run(plan, bars, context, disabled=("TREND_PRIOR_5M_REVERSAL",))
    assert result["action"] == "BUY_SIGNAL"
    assert "TREND_PRIOR_5M_REVERSAL" in result["met_conditions"]
    assert "TREND_SUBSEQUENT_5M_CONFIRMATION" in result["met_conditions"]
    for bar in bars[-5:]:
        bar.update(volume=10, amount=10*bar["close"])
    assert run(plan, bars, context, disabled=("TREND_PRIOR_5M_REVERSAL",))["action"] != "BUY_SIGNAL"
    aggregate = run(plan, bars, context, disabled=("TREND_5M_REVERSAL_CONFIRMATION",))
    assert aggregate["ablation"]["status"] == "UNSUPPORTED"


def test_real_time_ma5_shift_needs_frozen_daily_closes_and_retains_geometry():
    plan, bars, context = fixture()
    plan["trigger_zone"] = {"low": 9, "high": 9.5}
    assert run(plan, bars, context, variant={"zone_model": "REALTIME_MA5_SHIFT"})["ablation"]["status"] == "DATA_LIMITED"
    plan["daily_indicators"]["previous_daily_closes"] = [10.8]*4
    result = run(plan, bars, context, variant={"zone_model": "REALTIME_MA5_SHIFT"})
    assert result["action"] == "BUY_SIGNAL"
    assert result["ablation"]["effective_zone"]["low"] == pytest.approx(10.548)
    plan["maximum_stop_distance_pct"] = .01
    assert run(plan, bars, context, variant={"zone_model": "REALTIME_MA5_SHIFT"})["action"] != "BUY_SIGNAL"


@pytest.mark.parametrize("profile", ["TREND_MA5", "MA520_SWING", "LEADER_INTRADAY"])
def test_supported_predicates_have_unique_ast_positions(profile):
    plan, bars, context = fixture()
    plan.update(strategy_profile=profile, no_chase=12)
    if profile == "MA520_SWING":
        plan["daily_indicators"].update(ma20=10, close=11)
        plan["strategy_facts"] = {"ma520_setup": {"second_wave_restart": True}}
    if profile == "LEADER_INTRADAY":
        plan["stock_behavior_type"] = "EMOTION"
        plan["leader_context"] = {"valid": True, "theme_stage": "IGNITION",
                                  "ladder_intact": True, "board_count": 2}
    for row in condition_catalog(profile):
        if row["supported"] and row["id"] not in {
            "TREND_PULLBACK_VOLUME_CONTRACTION", "TREND_PRIOR_5M_REVERSAL",
            "TREND_SUBSEQUENT_5M_CONFIRMATION", "TREND_VWAP_RECLAIMED",
        }:
            result = run(plan, bars, context, disabled=(row["id"],))
            assert result["ablation"]["status"] == "OK", (row["id"], result["ablation"])


def test_520_daily_removal_does_not_change_protective_daily_exit():
    plan, bars, context = fixture()
    plan.update(strategy_profile="MA520_SWING", daily_indicators={"ma5": 9, "ma20": 10, "close": 11},
                strategy_facts={"ma520_setup": {"second_wave_restart": True}})
    assert run(plan, bars, context)["action"] != "BUY_SIGNAL"
    assert run(plan, bars, context, disabled=("DAILY_MA5_ABOVE_MA20",))["action"] == "BUY_SIGNAL"
    plan["position"] = {"total_qty": 100, "sellable_qty": 100, "avg_cost": 9}
    result = run(plan, bars, context, disabled=("DAILY_MA5_ABOVE_MA20",))
    assert result["action"] == "SELL_SIGNAL"
    assert "MA520_DAILY_BREAKDOWN" in result["reason_codes"]


def test_missing_frozen_zone_and_sequence_evidence_fail_closed():
    plan, bars, context = fixture()
    del plan["trigger_zone"]
    assert run(plan, bars, context, disabled=("A3_PULLBACK_ZONE",))["ablation"]["status"] == "DATA_LIMITED"
    plan["trend_entry_rule_version"] = "trend-ma5/2"
    for bar in bars:
        bar["volume"] = bar["amount"] = 0
    result = run(plan, bars, context, disabled=("TREND_VWAP_RECLAIMED",))
    assert result["ablation"]["status"] == "DATA_LIMITED"


def test_ast_drift_is_rejected_without_global_mutation():
    from liangjian_funnel.evaluation.ablation.engine import _rewrite, _isolated_namespace
    namespace = _isolated_namespace()
    with pytest.raises(RuntimeError, match="PRODUCTION_PREDICATE_DRIFT"):
        _rewrite(strategies._evaluate_trend, namespace, [("123456789 == 10", "True")])
    assert namespace["_evaluate_trend"] is not strategies._evaluate_trend


def test_condition_trace_does_not_mark_short_circuited_zone_as_passed():
    plan, bars, context = fixture()
    plan["volume_overheated"] = True
    baseline = run(plan, bars, context)
    assert baseline["ablation"]["condition_results"]["A3_PULLBACK_ZONE"]["baseline"] == "NOT_REACHED"
    result = run(plan, bars, context, disabled=("TREND_VOLUME_NOT_OVERHEATED",))
    assert result["ablation"]["condition_results"]["TREND_VOLUME_NOT_OVERHEATED"] == {
        "baseline": "FAIL", "counterfactual": "PASS"
    }
    assert result["ablation"]["condition_results"]["A3_PULLBACK_ZONE"]["counterfactual"] == "PASS"


def test_market_permission_removal_preserves_caution_position_guidance():
    plan, bars, context = fixture()
    context["live_market_state"].update(decision="CAUTION", suggested_position_cap_pct=.03)
    baseline = run(plan, bars, context)
    result = run(plan, bars, context, disabled=("LIVE_MARKET_ENTRY_PERMISSION",))
    assert result["reason_codes"] == baseline["reason_codes"]
    assert result["suggested_position_cap_pct"] == baseline["suggested_position_cap_pct"]


@pytest.mark.parametrize("blocked", [False, True])
def test_already_passed_removals_equal_the_real_isolated_evaluator(blocked):
    from liangjian_funnel.evaluation.ablation.engine import _build_evaluator
    plan, bars, context = fixture()
    disabled = ("A3_PULLBACK_ZONE", "TREND_VOLUME_NOT_OVERHEATED")
    if blocked:
        plan["no_chase"] = 10.0
    result = run(plan, bars, context, disabled=disabled)
    direct = _build_evaluator(disabled, 4, "EXACT_LAST4")(
        plan, bars, now=NOW, decision_time=NOW, market_context=context).model_dump(mode="json")
    assert {key:result[key] for key in direct} == direct
    assert result["ablation"]["evaluation_mode"] == "FORMAL_BASELINE_EQUIVALENT_NO_FAILED_TARGET"
    assert result["ablation"]["predicate_substitutions"] == []
