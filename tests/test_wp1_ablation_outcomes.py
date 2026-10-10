"""Frozen counterexamples for research-only ablation outcomes."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.evaluation.ablation.outcomes import evaluate_outcome, opportunity_cost
from liangjian_funnel.evaluation.ablation.statistics import summarize

SH = ZoneInfo("Asia/Shanghai")


def clock(day="2026-09-08", minute="10:00"):
    return datetime.fromisoformat(f"{day}T{minute}:00").replace(tzinfo=SH)


def plan(**extra):
    return {"plan_id": "p1", "symbol": "600001.SH", "stop_level": 9.5,
            "upper_limit": 11, "lower_limit": 9,
            "valid_from": clock().isoformat(), "valid_until": clock(minute="14:00").isoformat(),
            "entry_reference_zone": [9.8, 10.2], **extra}


def bar(at=None, **extra):
    return {"symbol": "600001.SH", "interval": "1m", "bar_end": (at or clock(minute="10:01")).isoformat(),
            "open": 10.0, "high": 10.2, "low": 9.8, "close": 10.0,
            "volume": 1000, "amount": 10000, "source_id": "frozen-fixture",
            "upper_limit": 11, "lower_limit": 9, **extra}


def test_next_complete_bar_causal_and_shared_slippage_tick():
    result = evaluate_outcome(plan(), clock(), [bar(clock(), open=9), bar()])
    assert result["fill_status"] == "FILLED"
    assert result["fill_price"] == 10.01
    assert result["fill_bar_end"] == clock(minute="10:01").isoformat()
    assert result["return_kind"] == "RESEARCH_COUNTERFACTUAL"
    assert "account_return" not in result


def test_partial_next_minute_is_not_eligible_and_missing_next_bar_is_not_backfilled():
    result = evaluate_outcome(plan(), clock() + timedelta(seconds=15), [bar(), bar(clock(minute="10:02"))])
    assert result["fill_bar_end"] == clock(minute="10:02").isoformat()
    result = evaluate_outcome(plan(), clock(), [bar(clock(minute="10:02"))])
    assert result["fill_status"] == "INSUFFICIENT_EVIDENCE"
    assert result["fill_price"] is None


@pytest.mark.parametrize("changed,reason", [
    ({"open": 11, "high": 11, "low": 11, "close": 11}, "LIMIT_UP_LOCKED"),
    ({"high": 10.001}, "PRICE_OUTSIDE_BAR"),
    ({"volume": 0}, "BAR_NOT_EXECUTABLE"),
    ({"complete": False}, "NEXT_COMPLETE_MINUTE_MISSING"),
])
def test_non_executable_entry_does_not_fabricate_fill(changed, reason):
    result = evaluate_outcome(plan(), clock(), [bar(**changed)])
    assert result["fill_price"] is None
    assert reason in result["reason_codes"]
    assert result["stop_r"] is None


def test_unknown_limits_are_explicitly_insufficient():
    p = plan(upper_limit=None, lower_limit=None)
    result = evaluate_outcome(p, clock(), [bar(upper_limit=None, lower_limit=None)])
    assert result["fill_status"] == "INSUFFICIENT_EVIDENCE"
    assert "PRICE_LIMITS_UNKNOWN" in result["reason_codes"]


def test_same_day_hard_stop_is_risk_observation_and_t1_sell_uses_gap_open():
    following = clock("2026-09-09", "09:31")
    result = evaluate_outcome(plan(), clock(), [bar(), bar(clock(minute="10:02"), low=9.4),
        bar(following, open=9.2, high=9.4, low=9.0, close=9.3)])
    assert result["same_day_hard_exit_touched"] is True
    assert result["stop_exit_bar_end"] == following.isoformat()
    assert result["stop_exit_price"] == 9.19
    assert result["stop_r"] == pytest.approx((9.19-10.01)/(10.01-9.5))


def test_missing_legal_sell_bar_keeps_stop_return_null():
    result = evaluate_outcome(plan(), clock(), [bar(), bar(clock(minute="10:02"), low=9.4),
        bar(clock("2026-09-09", "09:32"), open=9.2, high=9.4, low=9, close=9.3)])
    assert result["same_day_hard_exit_touched"] is True
    assert result["stop_r"] is None
    assert "STOP_EXIT_MINUTE_MISSING" in result["reason_codes"]
    assert result["minute_gaps"]


def test_close_horizons_use_exchange_dates_not_observed_bar_counts():
    result = evaluate_outcome(plan(), clock(), [bar(),
        bar(clock("2026-09-09", "15:00"), close=10.1),
        bar(clock("2026-09-14", "15:00"), close=10.2)])
    assert result["t1_close_return"] == pytest.approx(10.1/10.01-1)
    assert result["forward_r_t1"] == pytest.approx((10.1-10.01)/(10.01-9.5))
    assert result["t3_close_return"] is None  # Friday missing, Monday is T+4
    assert result["t5_close_return"] is None
    assert result["mfe"] == pytest.approx(10.2/10.01-1)
    assert result["mae"] == pytest.approx(9.8/10.01-1)


def test_existing_label_returns_do_not_become_entry_based_returns():
    result = evaluate_outcome(plan(), clock(), [bar()], outcome_labels=[
        {"symbol": "600001.SH", "fwd_return_1d": .9, "fwd_return_5d": .8}])
    assert result["t1_close_return"] is None
    assert result["t5_close_return"] is None
    assert result["source_outcome_label_count"] == 1


def test_opportunity_cost_only_counts_plan_validity_high():
    result = opportunity_cost(plan(), [bar(clock(minute="09:59"), high=100),
        bar(high=10.5), bar(clock(minute="14:01"), high=200)])
    assert result["value"] == pytest.approx(10.5/10.2-1)
    assert opportunity_cost(plan(), []) ["value"] is None


def row(pid="p1", minute="10:00", **extra):
    return {"profile": "TREND_MA5", "scenario": "SINGLE:VWAP", "removed_conditions": ["VWAP"],
            "plan_id": pid, "minute": clock(minute=minute).isoformat(), "action": "BUY_SIGNAL",
            "baseline_action": "WAIT", "failed_conditions": ["VWAP"], "data_block": False,
            "outcome": {"fill_status": "FILLED", "return_kind": "RESEARCH_COUNTERFACTUAL", "stop_r": 1.0,
                        "t5_close_return": .05},
            "opportunity_cost": None, **extra}


def test_statistics_deduplicate_minutes_and_require_twenty_trade_samples():
    rows = [row(f"p{i}") for i in range(19)] + [row("p1", "10:01")]
    group = summarize(rows)["groups"][0]
    assert group["minute_trigger_count"] == 20
    assert group["triggered_plan_count"] == 19
    assert group["trade_sample_count"] == 19
    assert group["status"] == "INSUFFICIENT_EVIDENCE"
    assert group["win_rate"] is None
    group = summarize(rows + [row("p19")])["groups"][0]
    assert group["trade_sample_count"] == 20
    assert group["win_rate"] == 1.0
    assert "account_return" not in group


def test_kill_rate_denominator_and_data_blocks_are_not_relaxable():
    baseline = [row("p1", scenario="BASELINE", removed_conditions=[], action="WAIT"),
                row("p2", scenario="BASELINE", removed_conditions=[], action="WAIT", failed_conditions=["VWAP", "ZONE"]),
                row("p3", scenario="BASELINE", removed_conditions=[], action="DATA_BLOCK", data_block=True),
                row("p4", scenario="BASELINE", removed_conditions=[], action="WAIT", failed_conditions=["ZONE", "ZONE"])]
    rates = {r["condition"]: r for r in summarize(baseline)["condition_kill_rates"]}
    assert rates["VWAP"]["denominator"] == 3
    assert rates["VWAP"]["numerator"] == 1
    assert rates["ZONE"]["numerator"] == 1
    assert "DATA_BLOCK" not in rates


def test_first_trigger_missing_outcome_is_not_replaced_by_later_profitable_minute():
    result = summarize([row(outcome=None), row(minute="10:01")])["groups"][0]
    assert result["triggered_plan_count"] == 1
    assert result["trade_sample_count"] == 0
    assert result["win_rate"] is None


def test_stats_do_not_use_only_stop_losses_as_overall_win_rate():
    outcome = {"fill_status": "FILLED", "return_kind": "RESEARCH_COUNTERFACTUAL", "stop_r": -1.5}
    group = summarize([row(f"p{i}", outcome=outcome) for i in range(20)])["groups"][0]
    assert group["trade_sample_count"] == 20
    assert group["return_sample_count"] == 0
    assert group["win_rate"] is None
    assert group["return_distributions"]["stop_r"]["sample_count"] == 20


def test_synthetic_forward_quote_does_not_prove_exchange_close():
    result = evaluate_outcome(plan(), clock(), [bar(),
        bar(clock("2026-09-09", "15:00"), close=10.1, evidence_kind="SYNTHETIC_QUOTE")])
    assert result["t1_close_return"] is None


def test_adjusted_archive_is_not_silently_compared_to_raw_plan_stop():
    result = evaluate_outcome(plan(), clock(), [bar(adjust_mode="qfq")])
    assert result["fill_price"] is None
    assert "RAW_PRICE_EVIDENCE_REQUIRED" in result["reason_codes"]


def test_stop_exit_requires_own_day_price_limits():
    result = evaluate_outcome(plan(), clock(), [bar(low=9.4),
        bar(clock("2026-09-09", "09:31"), upper_limit=None, lower_limit=None)])
    assert result["same_day_hard_exit_touched"] is True
    assert result["stop_exit_price"] is None
    assert "PRICE_LIMITS_UNKNOWN" in result["reason_codes"]


def test_horizon_forward_r_does_not_claim_hold_after_stop_execution():
    result = evaluate_outcome(plan(), clock(), [bar(low=9.4),
        bar(clock("2026-09-09", "09:31")),
        bar(clock("2026-09-09", "15:00"), close=10.1)])
    assert result["stop_r"] < 0
    assert result["forward_r_t1"] > 0
    assert "NOT_STOP_STRATEGY_OR_ACCOUNT_PNL" in result["forward_r_basis"]


def test_condition_with_no_unique_failures_still_gets_zero_kill_rate():
    report = summarize([row(scenario="BASELINE", removed_conditions=[], action="WAIT", failed_conditions=[]), row()])
    rates = {item["condition"]: item for item in report["condition_kill_rates"]}
    assert rates["VWAP"]["numerator"] == 0
    assert rates["VWAP"]["denominator"] == 1


def test_partial_minute_does_not_prove_opportunity_high():
    report = opportunity_cost(plan(), [bar(high=99, complete=False)])
    assert report["value"] is None


def test_unsupported_baseline_fallback_does_not_count_as_scenario_trigger():
    report = summarize([row(ablation={"status": "UNSUPPORTED"}),
                        row("p2", ablation={"status": "DATA_LIMITED"})])
    group = report["groups"][0]
    assert group["minute_trigger_count"] == 0
    assert group["triggered_plan_count"] == 0
    assert group["trade_sample_count"] == 0
    assert group["unsupported_window_count"] == 1
    assert group["data_limited_window_count"] == 1


def test_invalid_baseline_window_is_not_kill_rate_denominator():
    report = summarize([row("p1", scenario="BASELINE", removed_conditions=[], action="WAIT"),
                        row("p2", scenario="BASELINE", removed_conditions=[], action="WAIT",
                            ablation={"status": "UNSUPPORTED"}),
                        row("p3", scenario="BASELINE", removed_conditions=[], action="WAIT",
                            ablation={"status": "DATA_LIMITED"})])
    rate = report["condition_kill_rates"][0]
    assert rate["denominator"] == 1
    assert rate["numerator"] == 1


def test_protection_failure_prevents_false_sole_technical_kill():
    report = summarize([row(scenario="BASELINE", removed_conditions=[], action="WAIT",
                            failed_conditions=["VWAP", "HARD_STOP", "LOCKED_LIMIT_UP", "RR_GATE"]),
                        row("p2", scenario="BASELINE", removed_conditions=[], action="WAIT",
                            failed_conditions=["VWAP"])])
    rate = next(item for item in report["condition_kill_rates"] if item["condition"] == "VWAP")
    assert rate["numerator"] == 1
    assert rate["denominator"] == 2


def test_data_failure_id_is_not_relaxed_or_dropped_from_sole_failure_test():
    report = summarize([row(scenario="BASELINE", removed_conditions=[], action="WAIT",
                            failed_conditions=["VWAP", "DATA_BLOCK"])])
    rates = {item["condition"]: item for item in report["condition_kill_rates"]}
    assert rates["VWAP"]["numerator"] == 0
    assert "DATA_BLOCK" not in rates
