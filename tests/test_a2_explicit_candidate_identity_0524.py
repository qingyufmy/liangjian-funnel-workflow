"""A2 posterior identity is explicit; narrative references remain narrative."""
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
from liangjian_funnel.pipeline.research import (
    _a2_bottleneck_reasons,
    _a2_relative_top5_market_core_exception,
    _canonicalize_a2_bottleneck_scorecards,
    _canonicalize_a2_complete_partition,
    _enrich_a2_decision_facts,
    _merge_a2_outputs,
)


OWN = "600333.SH"
OTHER = "000001.SZ"


def row(symbol=OWN, **extra):
    return {"symbol": symbol, "thesis": f"Compare against {OTHER}",
            "source_refs": [f"peer:{OTHER}"], **extra}


def context(**extra):
    return {"preferred_route": "MARKET_CORE", "eligible_routes": ["MARKET_CORE"],
            "deterministic_status": "REVIEW_CANDIDATE", "top_rotation_theme": True,
            "deterministic_market_role": "TREND_LEADER", **extra}


def test_merge_does_not_drop_other_rows_due_to_narrative_mentions():
    output = {"focus_pool": [row()], "watch_only_pool": [row(OTHER, thesis=OWN)],
              "rejected_candidates": [row("600999.SH", thesis=OWN)]}
    original = deepcopy(output)
    merged = _merge_a2_outputs([output])
    assert merged["focus_pool"] == output["focus_pool"]
    assert merged["watch_only_pool"] == output["watch_only_pool"]
    assert merged["rejected_candidates"] == output["rejected_candidates"]
    assert output == original


def test_merge_still_removes_true_explicit_cross_pool_duplicate():
    merged = _merge_a2_outputs([{"focus_pool": [row()], "watch_only_pool": [row()],
                                 "rejected_candidates": [row()]}])
    assert len(merged["focus_pool"]) == 1
    assert merged["watch_only_pool"] == []
    assert merged["rejected_candidates"] == []


def test_complete_partition_does_not_turn_peer_reference_into_membership():
    gate = DeterministicGateResult("A2_LOCAL_ROLE", (
        {"symbol": OWN, "status": "REVIEW_CANDIDATE", "note": OTHER},
        {"symbol": OTHER, "status": "LOCAL_MONITOR", "note": OWN},
    ), (OWN,), (OTHER,), ())
    result, changes = _canonicalize_a2_complete_partition({"focus_pool": [row()]}, gate)
    assert result["focus_pool"] == [row()]
    assert [r["symbol"] for r in result["outside_rotation_pool"]] == [OTHER]
    assert changes == 1


def test_scorecard_route_and_facts_use_own_context_not_peer_context():
    data = {"A2_BOTTLENECK_CONTEXT": {
        OWN: context(), OTHER: context(preferred_route="SUPPLY_CHAIN_ALPHA",
                                      eligible_routes=["SUPPLY_CHAIN_ALPHA"],
                                      deterministic_market_role="EMOTION_LEADER")}}
    output = {"focus_pool": [row()]}
    routed, _ = _canonicalize_a2_bottleneck_scorecards(output, data)
    assert routed["focus_pool"][0]["a2_route"] == "MARKET_CORE"
    enriched, _ = _enrich_a2_decision_facts(output, data)
    assert enriched["focus_pool"][0]["market_role"] == "TREND_LEADER"
    assert enriched["focus_pool"][0]["source_refs"] == output["focus_pool"][0]["source_refs"]


def test_top5_exception_and_bottleneck_read_only_own_context():
    data = {"A2_BOTTLENECK_CONTEXT": {OWN: context(), OTHER: context(top_rotation_theme=False)}}
    own = row(bottleneck_status="NOT_REQUIRED_FOR_MARKET_CORE")
    assert _a2_relative_top5_market_core_exception(own, data)
    assert _a2_bottleneck_reasons(own, data) == []
    data["A2_BOTTLENECK_CONTEXT"].pop(OWN)
    assert not _a2_relative_top5_market_core_exception(own, data)
    assert "A2_BOTTLENECK_CONTEXT_MISSING" in _a2_bottleneck_reasons(own, data)


@pytest.mark.parametrize("identity", [None, "", "invalid", f"{OWN} {OTHER}", [OWN], {"symbol": OWN}])
def test_bad_or_ambiguous_explicit_identity_is_not_repaired_from_narrative(identity):
    bad = row(identity)
    data = {"A2_BOTTLENECK_CONTEXT": {OTHER: context()}}
    assert not _a2_relative_top5_market_core_exception(bad, data)
    assert "A2_BOTTLENECK_CONTEXT_MISSING" in _a2_bottleneck_reasons(bad, data)
    enriched, changes = _enrich_a2_decision_facts({"focus_pool": [bad]}, data)
    assert enriched["focus_pool"] == [bad]
    assert changes == 0


def test_equal_score_merge_order_uses_explicit_identity_not_prose():
    first = row("600999.SH", thesis=OTHER)
    second = row("600111.SH", thesis="600888.SH")
    merged = _merge_a2_outputs([{"focus_pool": [first, second]}])
    assert [item["symbol"] for item in merged["focus_pool"]] == ["600111.SH", "600999.SH"]
