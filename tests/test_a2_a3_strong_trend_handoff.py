"""Independent TREND observations reach technical review, never auto-entry."""
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.research.a3 import (
    _a3_candidate_eligible,
    _build_a3_candidate_domain,
    _apply_a3_candidate_origin_policy,
)
from liangjian_funnel.pipeline.research.common import _is_rotation_reserve


def row(symbol="000538.SZ"):
    return dict(symbol=symbol, stock_behavior_type="TREND", top_rotation_theme=False,
        strong_trend_observation=True, independent_strategy_review=True,
        research_observation_scope="REQUIRES_A3_A4_CONFIRMATION",
        research_route_qualifications={"TREND_MA5": {"eligible": True}, "MA520_SWING": {"eligible": True}},
        route_permission=["TREND_MA5", "MA520_SWING"], data_sufficiency_state="SUFFICIENT",
        eligible_routes=["MARKET_CORE"], a2_route="MARKET_CORE")


def test_full_handoff_reaches_domain_and_preserves_watch_provenance():
    watch = row()
    original = deepcopy(watch)
    domain, origins = _build_a3_candidate_domain({"focus_pool": [], "watch_only_pool": [watch]})
    assert origins == {watch["symbol"]: "WATCH_ONLY"}
    assert domain["focus_pool"][0]["candidate_origin"] == "WATCH_ONLY"
    assert "eligibility" not in domain["focus_pool"][0]
    assert "execution_permission" not in domain["focus_pool"][0]
    assert not _is_rotation_reserve(watch)
    assert watch == original


@pytest.mark.parametrize("field,value", [
    ("strong_trend_observation", False), ("independent_strategy_review", False),
    ("research_observation_scope", "UNKNOWN"), ("stock_behavior_type", "UNRESOLVED"),
    ("research_route_qualifications", {}), ("route_permission", []),
    ("route_permission", ["LEADER_INTRADAY"]),
    ("research_route_qualifications", {"TREND_MA5": {"eligible": False}}),
])
def test_partial_or_wrong_handoff_does_not_open_domain(field, value):
    watch = row()
    watch[field] = value
    assert not _a3_candidate_eligible(watch, origin="WATCH_ONLY")


@pytest.mark.parametrize("changes", [
    {"data_sufficiency_state": "INSUFFICIENT"}, {"tradable": False},
    {"hard_reason_codes": ["A3_HARD_RISK_EVENT"]}, {"a2_route": "NO_ROUTE"},
    {"eligible_routes": ["INVALID"]},
])
def test_independent_handoff_never_bypasses_later_vetoes(changes):
    watch = row()
    watch.update(changes)
    assert not _a3_candidate_eligible(watch, origin="WATCH_ONLY")


def test_old_reserve_contract_remains_distinct():
    watch = row()
    watch.update(research_observation_scope="RESEARCH_ONLY_NO_AUTOMATIC_ENTRY",
                 independent_strategy_review=False)
    assert _is_rotation_reserve(watch)
    assert _a3_candidate_eligible(watch, origin="WATCH_ONLY")


def test_ordinary_non_top_five_trend_remains_rejected():
    watch = row()
    for key in ("strong_trend_observation", "independent_strategy_review",
                "research_observation_scope", "research_route_qualifications"):
        watch.pop(key)
    assert not _a3_candidate_eligible(watch, origin="WATCH_ONLY")


def test_watch_origin_still_caps_technically_qualified_candidate_to_probe():
    symbol = "000538.SZ"
    output, _ = _apply_a3_candidate_origin_policy(
        {"core_watch_pool": [{"symbol": symbol, "risk_unit": "FULL", "strategy_profile": "TREND_MA5"}],
         "secondary_watch_pool": []},
        {"A3_CANDIDATE_ORIGIN": {symbol: "WATCH_ONLY"}, "A2_BOTTLENECK_CONTEXT": {symbol: row(symbol)}})
    assert output["core_watch_pool"][0]["risk_unit"] == "PROBE"
    assert output["core_watch_pool"][0]["candidate_origin"] == "WATCH_ONLY"
