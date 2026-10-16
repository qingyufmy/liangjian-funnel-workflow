"""A6 cannot turn model syntax or KEEP into a larger engine envelope."""
from dataclasses import replace
from decimal import Decimal

import pytest

from liangjian_funnel.runtime.risk_engine import RiskEnvelope
from liangjian_funnel.runtime.team_v2_risk_meeting import apply_premarket_review, review_overnight


def envelope(plan="p1", **changes):
    base = RiskEnvelope(plan, "000001.SZ", "2026-10-19", True, "STANDARD", Decimal("30000"),
                        3, 2000, (), ("POSITION_CAP",))
    return replace(base, **changes)


def review(adjustment="KEEP", **changes):
    return {"session": "PREMARKET", "envelope_adjustments": [{"plan_id": "p1", "adjustment": adjustment}],
            "utilization_cap": {"cap_pct": 75}, **changes}


def apply(output, *, inputs=None, failures=0, validated=True):
    return apply_premarket_review(tuple(inputs or [envelope()]), output,
        engine_utilization_cap=Decimal("75"), validated=validated, previous_failures=failures)


@pytest.mark.parametrize("adjustment,unit,shares,batches", [
    ("KEEP", "STANDARD", 2000, 3), ("TIGHTEN_TO_PROBE", "PROBE", 1000, 1),
    ("REDUCE_BATCHES", "STANDARD", 2000, 1), ("SUSPEND", "NONE", 0, 1)])
def test_only_authorized_reductions_and_engine_binding_are_preserved(adjustment, unit, shares, batches):
    result = apply(review(adjustment)); e = result.envelopes[0]
    assert result.status == "READY" and e.plan_id == "p1" and e.trade_date == "2026-10-19"
    assert (e.risk_unit, e.max_shares_cap, e.max_batches) == (unit, shares, batches)
    assert e.risk_budget <= Decimal("30000") and e.binding_limits == ("POSITION_CAP",)


def test_keep_cannot_reopen_engine_blocked_or_enlarge_prior_probe():
    original = envelope(eligible=False, risk_unit="NONE", max_shares_cap=0, blocked_reasons=("DATA_NOT_READY",))
    assert apply(review(), inputs=[original]).envelopes[0] == original
    probe = envelope(risk_unit="PROBE", max_batches=1, max_shares_cap=900, a6_tightened=True)
    assert apply(review("TIGHTEN_TO_PROBE"), inputs=[probe]).envelopes[0].max_shares_cap == 900


@pytest.mark.parametrize("output", [None, [], {}, review("LOOSEN"),
    review(envelope_adjustments=[]), review(envelope_adjustments=[{"plan_id": "other", "adjustment": "KEEP"}]),
    review(envelope_adjustments=[{"plan_id": "p1", "adjustment": "KEEP"}] * 2),
    review(utilization_cap={"cap_pct": 76}), review(utilization_cap={"cap_pct": -1}),
    review(utilization_cap={"cap_pct": float("nan")}), review(utilization_cap={"cap_pct": True}),
    review(session="POST_CLOSE")])
def test_invalid_or_loosening_response_falls_back_once_and_halts_on_second(output):
    first = apply(output)
    assert first.status == "DEGRADED" and first.consecutive_failures == 1
    assert first.envelopes[0].risk_unit == "PROBE" and first.envelopes[0].max_batches == 1
    second = apply(output, failures=1)
    assert second.status == "HALT" and second.consecutive_failures == 2
    assert second.envelopes[0].eligible is False and second.envelopes[0].max_shares_cap == 0
    assert "A6_DEGRADED_HALT" in second.reason_codes


def test_invalid_generic_evidence_validation_is_not_ignored():
    assert apply(review(), validated=False).status == "DEGRADED"


def test_model_cannot_provide_amounts_or_change_batches_by_extra_fields():
    raw = review(); raw["envelope_adjustments"][0]["shares"] = 999999
    assert apply(raw).status == "DEGRADED"


def test_success_resets_failure_count_and_cap_may_only_tighten():
    result = apply(review(utilization_cap={"cap_pct": 60}), failures=1)
    assert result.status == "READY" and result.consecutive_failures == 0
    assert result.utilization_cap == Decimal("60")


def test_second_failure_halt_is_latched_for_the_day_even_if_a_later_model_reply_is_valid():
    result = apply(review(), failures=2)
    assert result.status == "HALT" and result.envelopes[0].eligible is False
    assert result.consecutive_failures >= 2 and "A6_DEGRADED_HALT" in result.reason_codes


def test_pm_overnight_review_has_exact_scope_and_never_reverses_a_more_conservative_stance():
    pm = {"000001.SZ": "EXIT_TOMORROW", "000002.SZ": "HOLD"}
    rows = [{"symbol": "000001.SZ", "pm_stance": "EXIT_TOMORROW", "review": "AGREE"},
            {"symbol": "000002.SZ", "pm_stance": "HOLD", "review": "OVERRIDE_TO_REDUCE"}]
    result = review_overnight(pm, rows)
    assert result == ({"symbol": "000001.SZ", "pm_stance": "EXIT_TOMORROW", "stance": "EXIT_TOMORROW", "overridden": False},
                      {"symbol": "000002.SZ", "pm_stance": "HOLD", "stance": "REDUCE_TOMORROW", "overridden": True})
    rows[0]["review"] = "OVERRIDE_TO_REDUCE"
    with pytest.raises(ValueError, match="OVERNIGHT_LOOSENING"): review_overnight(pm, rows)
    with pytest.raises(ValueError, match="OVERNIGHT_DOMAIN"): review_overnight(pm, rows[1:])
