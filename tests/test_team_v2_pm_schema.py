"""Frozen PM session domains: model allocations never enlarge the A6 envelope."""
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal

import pytest

from liangjian_funnel.pipeline.team_v2_pm_schema import validate_pm_premarket, validate_pm_post_close
from liangjian_funnel.runtime.risk_engine import RiskEnvelope

CATALOG = {"categories": ["ACCOUNT", "MARKET"], "items": [
    {"id": "A", "category": "ACCOUNT"}, {"id": "M", "category": "MARKET"}]}
REFS = {"supporting_refs": ["A"], "opposing_refs": ["M"]}
ENVELOPE = {"stage_id": "A3", "run_id": "unit-frozen"}


def env(plan="p1", **changes):
    return replace(RiskEnvelope(plan, "000001.SZ", "2026-10-19", True, "STANDARD",
        Decimal("30000"), 2, 2000, (), (), purpose="PLANNING"), **changes)


def response():
    return {"envelope": deepcopy(ENVELOPE), "session": "PM_PREMARKET",
        "deployment_target": {"target_pct": 60, "baseline_range": [50, 75], "basis": "frozen market", **REFS},
        "slot_plan": [{"slot": 1, "primary_plan_id": "p1", "backup_plan_id": "", "cluster": "bank",
                       "accept_order": 1, **REFS}],
        "plan_allocations": [{"plan_id": "p1", "intended_risk_unit": "STANDARD", "max_batches": 2,
                              "conviction": "NORMAL", "reason_code": "SHAPE", **REFS}],
        "shortfall_reason": "NONE", "handoff_notes": {"to_a4": ["watch"], "to_a6": ["risk"]},
        "what_would_change_my_mind": ["fresh market"]}


def check(output, **changes):
    args = dict(required_envelope=ENVELOPE, envelopes=(env(),), plan_clusters={"p1": "bank"},
        free_slots=2, baseline_range=(Decimal(50), Decimal(75)), utilization_cap=Decimal(75),
        high_conviction_ids=set())
    args.update(changes)
    return validate_pm_premarket(output, CATALOG, **args)


def test_valid_pm_and_zero_plan_meetings_leave_frozen_output_untouched():
    original = response(); saved = deepcopy(original)
    result = check(original)
    assert result.valid and len(result.judgments) == 3 and original == saved
    empty = response(); empty["slot_plan"] = []; empty["plan_allocations"] = []
    empty["shortfall_reason"] = "NO_ELIGIBLE_PLANS"
    assert check(empty, envelopes=(), plan_clusters={}).valid


@pytest.mark.parametrize("path,value", [
    (("deployment_target", "target_pct"), 76), (("deployment_target", "target_pct"), 49),
    (("deployment_target", "target_pct"), True), (("deployment_target", "target_pct"), float("nan")),
    (("deployment_target", "baseline_range"), [40, 75]), (("deployment_target", "shares"), 500),
    (("slot_plan", 0, "primary_plan_id"), "outside"), (("slot_plan", 0, "backup_plan_id"), "p1"),
    (("slot_plan", 0, "cluster"), "fake"), (("slot_plan", 0, "slot"), True),
    (("slot_plan", 0, "slot"), 3), (("slot_plan", 0, "accept_order"), 0),
    (("plan_allocations", 0, "max_batches"), 3), (("plan_allocations", 0, "max_batches"), True),
    (("plan_allocations", 0, "intended_risk_unit"), "MAX"),
    (("plan_allocations", 0, "conviction"), "HIGH_CONVICTION"),
    (("plan_allocations", 0, "shares"), 999), (("handoff_notes", "customer"), ["send"]),
    (("slot_plan", 0, "supporting_refs"), ["outside"]),
])
def test_declared_counterexamples_are_invalid_not_repaired(path, value):
    output = response(); target = output
    for key in path[:-1]: target = target[key]
    target[path[-1]] = value
    assert not check(output).valid


def test_allocations_cannot_reopen_or_upgrade_probe_or_prior_a6_tightening():
    assert not check(response(), envelopes=(env(eligible=False),)).valid
    assert not check(response(), envelopes=(env(risk_unit="PROBE"),)).valid
    high = response(); high["plan_allocations"][0]["conviction"] = "HIGH_CONVICTION"
    assert check(high, high_conviction_ids={"p1"}).valid
    assert not check(high, high_conviction_ids={"p1"}, envelopes=(env(a6_tightened=True),)).valid


def test_duplicate_reused_primary_backup_allocation_or_more_than_two_same_cluster_are_invalid():
    output = response(); output["slot_plan"].append(deepcopy(output["slot_plan"][0]))
    assert not check(output).valid
    output = response(); output["plan_allocations"] *= 2
    assert not check(output).valid
    output = response(); output["plan_allocations"] = []
    assert not check(output).valid
    output = response()
    for i in (2, 3):
        output["slot_plan"].append({**deepcopy(output["slot_plan"][0]), "slot": i,
            "primary_plan_id": "p"+str(i), "accept_order": i})
        output["plan_allocations"].append({**deepcopy(output["plan_allocations"][0]), "plan_id": "p"+str(i)})
    assert not check(output, envelopes=tuple(env("p"+str(i)) for i in (1, 2, 3)),
        plan_clusters={"p1": "bank", "p2": "bank", "p3": "bank"}, free_slots=3).valid


@pytest.mark.parametrize("value", [None, [], 3, "bad", {}, {"slot_plan": [False]}])
def test_malformed_json_returns_invalid_instead_of_throwing(value):
    assert not check(value).valid


def close_response():
    return {"envelope": ENVELOPE, "session": "PM_POST_CLOSE", "overnight_stances": [
        {"symbol": "000001.SZ", "stance": "HOLD", "reason_code": "STRUCTURE", **REFS,
         "what_would_change_my_mind": ["structure breaks"]}],
        "capacity_request": {"slots": 2, "clusters": {"bank": 1}, "note": "next session", **REFS},
        "handoff_notes": {"to_a6": ["check"], "to_a5": ["review"]},
        "what_would_change_my_mind": ["new facts"], "unresolved_questions": []}


def test_close_exact_positions_and_capacity_are_bounded_advisory_only():
    args = dict(required_envelope=ENVELOPE, position_symbols={"000001.SZ"},
                allowed_clusters={"bank"}, max_positions=6)
    assert validate_pm_post_close(close_response(), CATALOG, **args).valid
    for field, value in (("slots", 7), ("slots", True), ("clusters", {"outside": 1}),
                         ("clusters", {"bank": -1}), ("clusters", {"bank": 3})):
        output = close_response(); output["capacity_request"][field] = value
        assert not validate_pm_post_close(output, CATALOG, **args).valid
    output = close_response(); output["overnight_stances"] *= 2
    assert not validate_pm_post_close(output, CATALOG, **args).valid
    output = close_response(); output["overnight_stances"][0]["stance"] = "BUY"
    assert not validate_pm_post_close(output, CATALOG, **args).valid
