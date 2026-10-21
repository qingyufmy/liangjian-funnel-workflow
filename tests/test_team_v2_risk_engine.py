from dataclasses import replace
from datetime import datetime
from decimal import Decimal as D
from pathlib import Path
import pytest
from liangjian_funnel.runtime.risk_engine import (
    RiskLimits, AccountRisk, PositionRisk, PlanRisk, build_risk_envelope,
    adjudicate_signal, conservative_envelope,
)

NOW = datetime.fromisoformat("2026-10-19T10:00:00+08:00")


def plan(**changes):
    return replace(PlanRisk("p1", "000001.SZ", "FINANCE", "ACTIVE_TODAY",
                           datetime.fromisoformat("2026-10-19T09:32:00+08:00"),
                           datetime.fromisoformat("2026-10-19T15:00:00+08:00"),
                           D("10"), D("9"), D("9.5"), D("10.2"), D("10.2"),
                           invalidation_price=D("9")), **changes)


def account(**changes):
    return replace(AccountRisk(D("1000000"), D("1000000"), D("1000000"), D("1000000")), **changes)


def envelope(p=None, a=None, **kwargs):
    return build_risk_envelope(p or plan(), a or account(), (), RiskLimits(), now=NOW,
                               regime="TREND_MAINLINE", **kwargs)


def test_authorized_limits_come_from_yaml_not_old_one_percent_defaults():
    limits = RiskLimits.from_yaml(Path(__file__).resolve().parents[1] / "config/risk_limits.yaml")
    assert limits.standard_risk_pct == D("0.03") and limits.max_risk_pct == D("0.05")
    assert limits.probe_multiplier == D("0.5") and limits.max_position_pct == D("0.20")
    assert limits.max_cluster_pct == D("0.40") and limits.max_cluster_signals == 2


def test_position_cap_binds_before_three_percent_risk_no_geometry_change():
    e = envelope()
    assert e.eligible and e.max_shares_cap == 20000
    assert "POSITION_CAP" in e.binding_limits
    assert e.risk_budget == D("30000")


@pytest.mark.parametrize("regime", ["BEAR_RISK", "RISK_OFF_RETREAT"])
def test_bear_regime_only_probe(regime):
    e = build_risk_envelope(plan(), account(), (), RiskLimits(), now=NOW, regime=regime)
    assert e.eligible and e.risk_unit == "PROBE" and e.risk_budget == D("15000")


@pytest.mark.parametrize("changes,code", [
    ({"status": "PENDING_MORNING_REVIEW"}, "ENVELOPE_INELIGIBLE"),
    ({"expires_at": datetime.fromisoformat("2026-10-18T15:00:00+08:00")}, "PLAN_EXPIRED"),
    ({"tradable": False}, "NOT_TRADABLE"), ({"data_ready": False}, "DATA_NOT_READY"),
    ({"price": D("11")}, "PRICE_OUTSIDE_PLAN_GEOMETRY"),
    ({"stop_price": D("11")}, "PRICE_OUTSIDE_PLAN_GEOMETRY"),
])
def test_invalid_plan_never_eligible(changes, code):
    e = envelope(plan(**changes))
    assert not e.eligible and code in e.blocked_reasons


def test_new_entry_count_and_cluster_count_are_hard_caps():
    assert "DAILY_ENTRY_CAP" in envelope(a=account(new_entries_today=3)).blocked_reasons
    assert "CLUSTER_SIGNAL_CAP" in envelope(cluster_signals_today=2).blocked_reasons


def test_losses_and_drawdowns_are_independent_of_model_hints():
    assert "DAILY_LOSS_HALT" in envelope(a=account(nav=D("980000"))).blocked_reasons
    assert "DRAWDOWN_HALT" in envelope(a=account(nav=D("850000"))).blocked_reasons
    # A prior drawdown but no daily loss halves the unit, not the limits.
    e = envelope(a=account(nav=D("920000"), day_open_nav=D("920000")))
    assert e.eligible and e.risk_budget == D("13800")


def test_zero_cash_and_full_cluster_never_get_a_lot():
    assert not envelope(a=account(cash=D("100000"))).eligible
    positions = (PositionRisk("old", "000002.SZ", "FINANCE", 40000, 40000, D("400000")),)
    e = build_risk_envelope(plan(), account(), positions, RiskLimits(), now=NOW, regime="TREND_MAINLINE")
    assert not e.eligible and "CLUSTER_CAP" in e.blocked_reasons


def test_high_conviction_cannot_overrule_a6_tightening():
    p = plan(stop_price=D("5"))
    assert envelope(p, high_conviction=True).risk_budget == D("50000")
    assert envelope(p, high_conviction=True, a6_tightened=True).risk_budget == D("30000")


def test_current_account_is_rechecked_not_only_old_envelope():
    e = envelope()
    decision = adjudicate_signal("s1", "BUY_SIGNAL", plan(), e, account(cash=D("100000")), (),
                                 RiskLimits(), now=NOW, regime="TREND_MAINLINE")
    assert decision.status == "REJECTED" and decision.shares == 0


def test_signal_plan_binding_and_expiry_are_hard():
    e = envelope()
    decision = adjudicate_signal("s1", "BUY_SIGNAL", plan(plan_id="p2"), e, account(), (),
                                 RiskLimits(), now=NOW, regime="TREND_MAINLINE")
    assert decision.status == "REJECTED" and "ENVELOPE_BINDING_MISMATCH" in decision.reason_codes


def test_t1_exit_cannot_sell_today_bought_shares():
    pos = (PositionRisk("p1", "000001.SZ", "FINANCE", 100, 0, D("1000")),)
    d = adjudicate_signal("s1", "SELL_SIGNAL", plan(), envelope(), account(), pos,
                           RiskLimits(), now=NOW, regime="ICE_POINT")
    assert d.status == "REJECTED" and "T1_LOCKED" in d.reason_codes


def test_a6_failure_tightens_once_then_halts_without_resuming_hard_halt():
    e = envelope()
    first = conservative_envelope(e, consecutive_failures=1)
    assert first.eligible and first.risk_unit == "PROBE" and first.max_batches == 1
    assert first.max_shares_cap <= e.max_shares_cap // 2
    second = conservative_envelope(first, consecutive_failures=2)
    assert not second.eligible and "A6_DEGRADED_HALT" in second.blocked_reasons


@pytest.mark.parametrize("bad", [True, "NaN", "Infinity", "-0.01"])
def test_malformed_limits_do_not_fall_back_to_permissive_defaults(bad):
    with pytest.raises(ValueError):
        RiskLimits(standard_risk_pct=bad)


def test_unproven_invalidation_and_truthy_string_flags_cannot_authorize_entry():
    assert not envelope(plan(invalidation_price=None)).eligible
    with pytest.raises(ValueError): plan(data_ready="false")
    with pytest.raises(ValueError): account(hard_halt="false")


def test_suspended_market_exit_remains_unexecutable_not_a_fake_approval():
    pos = (PositionRisk("p1", "000001.SZ", "FINANCE", 100, 100, D("1000")),)
    d = adjudicate_signal("s1", "SELL_SIGNAL", plan(tradable=False), envelope(), account(), pos,
                           RiskLimits(), now=NOW, regime="ICE_POINT")
    assert d.status == "REJECTED" and "NOT_TRADABLE" in d.reason_codes


def test_conservative_cap_that_rounds_to_zero_must_not_stay_eligible():
    e = replace(envelope(), max_shares_cap=100)
    assert not conservative_envelope(e, consecutive_failures=1).eligible


def test_add_requires_existing_profitable_position_and_remaining_batch():
    pos = (PositionRisk("p1", "000001.SZ", "FINANCE", 100, 100, D("1100"), avg_cost=D("11")),)
    e = build_risk_envelope(plan(), account(), pos, RiskLimits(), now=NOW, regime="TREND_MAINLINE", action="ADD_SIGNAL")
    assert not e.eligible and "LOSS_ADD_FORBIDDEN" in e.blocked_reasons


@pytest.mark.parametrize("field,value", [("high_conviction", "false"), ("a6_tightened", "false"),
                                        ("high_conviction", 1), ("a6_tightened", None)])
def test_model_flags_must_be_actual_booleans(field, value):
    with pytest.raises(ValueError, match="RISK_FLAG_INVALID"):
        envelope(**{field: value})


def test_0833_pending_plan_has_planning_capacity_but_is_never_an_execution_permission():
    from liangjian_funnel.runtime import risk_engine
    now = datetime.fromisoformat("2026-10-19T08:33:00+08:00")
    pending = plan(status="PENDING_MORNING_REVIEW", price=D("10.5"))
    before = pending
    e = risk_engine.build_planning_envelope(pending, account(), (), RiskLimits(), now=now, regime="TREND_MAINLINE")
    assert e.eligible and e.purpose == "PLANNING" and pending == before
    current = plan(price=D("10"))
    d = adjudicate_signal("s1", "BUY_SIGNAL", current, e, account(), (), RiskLimits(), now=NOW, regime="TREND_MAINLINE")
    assert d.status == "REJECTED" and "PLANNING_ENVELOPE_NOT_EXECUTABLE" in d.reason_codes


def test_fresh_quote_risk_amount_never_exceeds_a6_budget_even_when_share_cap_was_position_bound():
    old = plan(entry_high=D("12"), no_chase=D("12"))
    e = conservative_envelope(envelope(old), consecutive_failures=1)
    current = replace(old, price=D("11"))
    d = adjudicate_signal("s1", "BUY_SIGNAL", current, e, account(), (), RiskLimits(), now=NOW, regime="TREND_MAINLINE")
    assert d.shares > 0 and D(d.shares) * (current.price-current.stop_price) <= e.risk_budget
    assert "A6_RISK_BUDGET_CAP" in d.reason_codes


@pytest.mark.parametrize("changes", [{"status": "INVALIDATED"},
    {"data_ready": False}, {"tradable": False},
    {"expires_at": datetime.fromisoformat("2026-10-18T15:00:00+08:00")}])
def test_planning_capacity_does_not_remove_data_identity_or_calendar_gates(changes):
    from liangjian_funnel.runtime import risk_engine
    now=datetime.fromisoformat("2026-10-19T08:33:00+08:00")
    assert not risk_engine.build_planning_envelope(plan(**changes), account(), (), RiskLimits(), now=now,
                                                    regime="TREND_MAINLINE").eligible
