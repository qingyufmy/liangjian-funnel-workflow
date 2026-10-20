"""Original production failures remain invalid; only the visible domain changes."""
import copy
import hashlib
import json
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.research import (
    _a2_rotation_retry_feedback,
    _project_a2_bottleneck_context,
    _semantic_retry_instruction,
    _validate_a2_rotation_focus_coverage,
)

FIXTURES = Path(__file__).parent / "fixtures" / "a2_review_domain_20261013"
PINS = {
    "attempt-1-84843ab379487d29.json": "cd0fdea7a42919a2e630fb90a9a0aa437e04fd571a992976a67acd90d3032a34",
    "attempt-2-70a7c5afde581828.json": "a7688cda05be7e9d66baeaa5ec965e3e2576d47adf4117139212d1dc62a1be8b",
}

def frozen(name):
    raw = (FIXTURES / name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == PINS[name]
    record = json.loads(raw)
    # These three booleans are exact rename/copies in the production projection.
    # This is a validator counterexample, not a reconstructed full market snapshot.
    contexts = {}
    for symbol, row in record["review_context"].items():
        if symbol.startswith("_"):
            continue
        context = copy.deepcopy(row)
        context["trend_core_eligible"] = row["trend_eligible"]
        context["emotion_core_eligible"] = row["emotion_eligible"]
        context["theme_id"] = row["a1_theme_id"]
        contexts[symbol] = context
    return record, {"A2_BOTTLENECK_CONTEXT": contexts}, set(contexts)

@pytest.mark.parametrize("name", list(PINS))
def test_original_responses_remain_strictly_rejected_without_mutation(name):
    record, snapshot, symbols = frozen(name)
    original = copy.deepcopy(record)
    assert "_rotation_review_scope" not in record["review_context"]
    assert len(record["model_output"]["rotation_reviews"]) == 11
    # Keep the historical diagnostics verbatim. P0-8a intentionally removes
    # their label-only ALREADY_FOCUSED tag: the exact qualified member domain
    # is empty, so no direction can be covered by these FOCUS symbols.
    assert sum("ALREADY_FOCUSED" in reason for reason in record["validation_reasons"]) == 4
    expected = {
        f"A2_ROTATION_REVIEW_INVALID:{row['rotation_direction_id']}:UNKNOWN_DIRECTION"
        for row in record["post_policy_output"]["rotation_reviews"]
    }
    reasons = _validate_a2_rotation_focus_coverage(record["post_policy_output"], snapshot, symbols)
    assert set(reasons) == expected
    assert len(reasons) == 11
    assert all(reason.endswith(":UNKNOWN_DIRECTION") for reason in reasons)
    assert record == original

def test_empty_domain_is_explicit_despite_strong_trend_observations():
    record, snapshot, symbols = frozen(next(iter(PINS)))
    assert any(row.get("strong_trend_observation") for row in snapshot["A2_BOTTLENECK_CONTEXT"].values())
    projected = _project_a2_bottleneck_context(snapshot["A2_BOTTLENECK_CONTEXT"], symbols)
    assert projected["_rotation_review_scope"] == {}
    assert projected["_rotation_review_contract"] == {"direction_count": 0, "allowed_direction_ids": []}
    assert {key for key in projected if not key.startswith("_")} == symbols

@pytest.mark.parametrize("name", list(PINS))
def test_retry_empty_domain_has_no_member_coverage_and_rejects_all_foreign_reviews(name):
    record, snapshot, symbols = frozen(name)
    feedback = _a2_rotation_retry_feedback(record["post_policy_output"], snapshot, symbols)
    assert feedback["allowed_direction_count"] == 0
    assert feedback["allowed_rotation_direction_ids"] == []
    assert feedback["required_no_focus_if_focus_unchanged"] == {}
    # The feedback uses the same exact member-domain contract as validation;
    # historical board labels must not invent coverage when its domain is {}.
    assert feedback["already_focused_directions"] == {}
    assert set(feedback["validation_errors"]) == {
        f"A2_ROTATION_REVIEW_INVALID:{row['rotation_direction_id']}:UNKNOWN_DIRECTION"
        for row in record["post_policy_output"]["rotation_reviews"]
    }
    assert len(feedback["validation_errors"]) == 11

def test_semantic_retry_restates_zero_catalog_and_forbids_using_candidate_ids():
    text = _semantic_retry_instruction("A2", ("A2_ROTATION_REVIEW_INVALID:INDUSTRY:X:UNKNOWN_DIRECTION",))
    assert "direction_count=0" in text
    assert "rotation_reviews=[]" in text
    assert "allowed_direction_ids" in text

def domain():
    base = {"deterministic_status": "REVIEW_CANDIDATE", "trend_core_eligible": True,
            "top_rotation_theme": True, "selected_board": {"selected_for_rotation": True}}
    rows = {"600001.SH": dict(base, rotation_direction_id="BOARD:A"),
            "600002.SH": dict(base, rotation_direction_id="BOARD:B")}
    return {"A2_BOTTLENECK_CONTEXT": rows}, set(rows)

def review(direction, symbol):
    return {"rotation_direction_id": direction, "decision": "NO_FOCUS", "reviewed_symbols": [symbol],
            "reason_codes": ["FROZEN_EVIDENCE_CONFLICT"], "explanation": "冻结主营证据与该方向不一致，保留观察而不聚焦。"}

def test_nonempty_domain_requires_complete_focus_or_valid_no_focus():
    snapshot, symbols = domain()
    output = {"focus_pool": [{"symbol": "600001.SH"}], "rotation_reviews": [review("BOARD:B", "600002.SH")]}
    assert _validate_a2_rotation_focus_coverage(output, snapshot, symbols) == []
    output["rotation_reviews"] = []
    assert _validate_a2_rotation_focus_coverage(output, snapshot, symbols) == ["A2_ROTATION_FOCUS_COVERAGE_MISSING:BOARD:B"]

@pytest.mark.parametrize("case", ["fake", "duplicate", "wrong_representative", "focused"])
def test_nonempty_domain_never_accepts_invalid_reviews(case):
    snapshot, symbols = domain()
    output = {"focus_pool": [{"symbol": "600001.SH"}], "rotation_reviews": [review("BOARD:B", "600002.SH")]}
    if case == "fake": output["rotation_reviews"][0]["rotation_direction_id"] = "BOARD:FAKE"
    if case == "duplicate": output["rotation_reviews"] *= 2
    if case == "wrong_representative": output["rotation_reviews"][0]["reviewed_symbols"] = ["600001.SH"]
    if case == "focused": output["rotation_reviews"] = [review("BOARD:A", "600001.SH")]
    assert _validate_a2_rotation_focus_coverage(output, snapshot, symbols)

def test_no_qualified_rotation_directions_does_not_veto_independent_research_focus():
    record, snapshot, symbols = frozen(next(iter(PINS)))
    # Explicit hypothetical compliant review, not repair of the production record.
    hypothesis = {"focus_pool": copy.deepcopy(record["post_policy_output"]["focus_pool"]), "rotation_reviews": []}
    assert _validate_a2_rotation_focus_coverage(hypothesis, snapshot, symbols) == []
    assert len(hypothesis["focus_pool"]) == 7
