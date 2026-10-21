"""A3 explicit identity regression; no Workflow/Store/provider/production access.

The compressed fixture contains the three exact captured raw replies and a
field-only projection of their original frozen contexts (not reconstructed
prices). DERIVED_TRACE below exercises identity postprocessors, not a full
research entry-point replay or a new model response.
"""
import base64
from copy import deepcopy
import hashlib
import json
import zlib

import pytest

from liangjian_funnel.pipeline.research import common as c

_FROZEN = json.loads(zlib.decompress(base64.b64decode(
    "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"
)))
BATCHES = _FROZEN["batches"]
CONTEXTS = {s: row for batch in BATCHES for s, row in batch["contexts"].items()}


def _raw(index):
    batch = BATCHES[index]
    assert hashlib.sha256(batch["raw"].encode()).hexdigest() == batch["raw_sha256"]
    return json.loads(batch["raw"])


def _snapshot():
    return {
        "DETERMINISTIC_RESEARCH_V2_ENABLED": True,
        "A3_DETERMINISTIC_CONTEXT": deepcopy(CONTEXTS),
        "MAX_STOP_DISTANCE": CONTEXTS["603656.SH"]["maximum_stop_distance_pct"],
        "MIN_REWARD_RISK": CONTEXTS["603656.SH"]["minimum_reward_risk"],
    }


def _threshold(raw):
    return c._apply_stage_threshold_policy(raw, stage="A3", snapshot_data=_snapshot())[0]


def test_original_fixture_source_binding_and_offending_raw_are_unmodified():
    assert _FROZEN["source_sha256"] == "680a68c079c37bf603a76ecd69f83013f903e146cd524684e94d244ba63d64d1"
    row = _raw(2)["core_watch_pool"][3]
    assert row["symbol"] == "603656.SH"
    assert row["what_would_change_my_mind"][0] == "机器人主题龙头002164.SZ断板"
    assert c._first_symbol(row) == "002164.SZ"
    assert c._a3_candidate_identity(row) == "603656.SH"


@pytest.mark.parametrize("index", range(3))
def test_exact_original_batch_membership_red_green(index):
    raw = _raw(index)
    frozen = deepcopy(raw)
    domain = set(BATCHES[index]["domain"])
    legacy_outside = c._candidate_pool_symbols(raw) - domain
    assert legacy_outside == ({"002164.SZ"} if index == 2 else set())
    assert c._candidate_pool_symbols(raw, stage="A3") - domain == set()
    assert raw == frozen


@pytest.mark.parametrize("index", range(3))
def test_derived_original_threshold_identity_and_first_two_unchanged(index, monkeypatch):
    raw = _raw(index)
    frozen = deepcopy(raw)
    fixed = _threshold(raw)
    with monkeypatch.context() as patch:
        # Exact former whole-row identity read; every other function unchanged.
        patch.setattr(c, "_a3_candidate_identity", c._first_symbol)
        former = _threshold(raw)
    assert raw == frozen
    if index < 2:
        assert fixed == former
    else:
        old = next(r for r in former["core_watch_pool"] if r["symbol"] == "603656.SH")
        new = next(r for r in fixed["core_watch_pool"] if r["symbol"] == "603656.SH")
        assert (old["strategy_profile"], old["stock_behavior_type"]) == ("LEADER_INTRADAY", "EMOTION")
        assert (new["strategy_profile"], new["stock_behavior_type"]) == ("TREND_MA5", "TREND")
        assert new["what_would_change_my_mind"] == raw["core_watch_pool"][3]["what_would_change_my_mind"]
        # No price evidence is fabricated. Empty levels deliberately leave
        # PRICE_LEVELS_UNAVAILABLE; only the two identity mismatches go away.
        snapshot = {**_snapshot(), "PRICE_LEVELS": {}}
        old_reasons = c._validate_a3_provenance(former, snapshot)
        new_reasons = c._validate_a3_provenance(fixed, snapshot)
        identity_errors = {"A3_STRATEGY_PROFILE_PROVENANCE_MISMATCH", "A3_BEHAVIOR_TYPE_PROVENANCE_MISMATCH"}
        assert identity_errors <= set(old_reasons)
        assert not identity_errors.intersection(new_reasons)
        assert "A3_PRICE_LEVELS_UNAVAILABLE" in new_reasons


@pytest.mark.parametrize("index", range(3))
def test_original_stage_outcome_symbols_exclude_narrative_security(index):
    raw = _raw(index)
    assert c._approved_symbols(raw, "A3") == {r["symbol"] for r in raw["core_watch_pool"]}
    assert c._approved_symbols(raw, "A3") <= set(BATCHES[index]["domain"])


@pytest.mark.parametrize("identity", [None, "", "not-a-security", "603656.SH 002164.SZ", ["603656.SH"], {"symbol": "603656.SH"}])
def test_invalid_explicit_identity_never_recovers_from_prose(identity):
    row = {"symbol": identity, "reason": "603656.SH qualifies", "supporting_refs": ["TECH:603656.SH"]}
    assert c._a3_candidate_identity(row) == ""
    output = {"core_watch_pool": [row], "secondary_watch_pool": [], "rejected_candidates": [],
        "envelope": {"stage_id": "A3", "model_name": "frozen", "status": "OK",
            "input_snapshot_ids": ["frozen"], "config_version": "frozen",
            "prompt_version": "frozen", "market_regime": "frozen"}}
    reasons = c._validate_output(output, stage="A3", model="frozen", snapshot_id="frozen", upstream_symbols={"603656.SH"})
    assert "CANDIDATE_LINEAGE_UNPROVABLE" in reasons
    assert "APPROVED_POOL_SYMBOL_INVALID" in reasons


def test_missing_mapping_identity_does_not_use_prose_but_true_legacy_scalar_is_kept():
    assert c._a3_candidate_identity({"reason": "603656.SH"}) == ""
    assert c._a3_candidate_identity("603656.SH") == c._first_symbol("603656.SH")
    assert c._candidate_pool_symbols({"secondary_watch_pool": ["603656.SH"]}, stage="A3") == {"603656.SH"}


def test_true_foreign_explicit_candidate_is_still_rejected():
    raw = _raw(2)
    raw["core_watch_pool"][3]["symbol"] = "002164.SZ"
    outside = c._candidate_pool_symbols(raw, stage="A3") - set(BATCHES[2]["domain"])
    assert outside == {"002164.SZ"}
    raw["envelope"] = {"stage_id": "A3", "model_name": "frozen", "status": "OK",
        "input_snapshot_ids": ["frozen"], "config_version": "frozen",
        "prompt_version": "frozen", "market_regime": "frozen"}
    assert "POOL_OUTSIDE_UPSTREAM" in c._validate_output(
        raw, stage="A3", model="frozen", snapshot_id="frozen", upstream_symbols=set(BATCHES[2]["domain"]))


def test_candidate_origin_uses_explicit_security_not_mentioned_leader():
    raw = {"core_watch_pool": [_raw(2)["core_watch_pool"][3]], "secondary_watch_pool": []}
    snapshot = {"A3_CANDIDATE_ORIGIN": {"603656.SH": "WATCH_ONLY", "002164.SZ": "FOCUS"},
        "A2_BOTTLENECK_CONTEXT": {"002164.SZ": {"rotation_reserve_eligible": True}}}
    fixed, _ = c._apply_a3_candidate_origin_policy(raw, snapshot)
    row = fixed["core_watch_pool"][0]
    assert row["candidate_origin"] == "WATCH_ONLY"
    assert row["risk_unit"] == "PROBE"
    assert "rotation_reserve_eligible" not in row
    assert fixed["secondary_watch_pool"] == []


def test_a3_batch_projection_only_filters_explicit_identity_and_a2_unchanged():
    raw = {"focus_pool": [{"symbol": "603656.SH", "reason": "002164.SZ"},
        {"symbol": "002164.SZ", "reason": "603656.SH"}]}
    fixed = c._project_upstream_output(raw, {"603656.SH"}, stage="A3")
    assert [r["symbol"] for r in fixed["focus_pool"]] == ["603656.SH"]
    legacy = c._project_upstream_output(raw, {"603656.SH"})
    assert {r["symbol"] for r in legacy["focus_pool"]} == {"603656.SH", "002164.SZ"}


@pytest.mark.parametrize("stage,key", [("A1", "active_research_pool")])
def test_a1_and_default_recursive_scan_compatibility(stage, key):
    raw = {key: [{"symbol": "603656.SH", "reason": "002164.SZ"}]}
    assert c._candidate_pool_symbols(raw, stage=stage) == {"603656.SH", "002164.SZ"}
    assert c._candidate_pool_symbols(raw) == {"603656.SH", "002164.SZ"}
    assert c._approved_symbols(raw, stage) == {"603656.SH", "002164.SZ"}


def test_a2_membership_outcome_projection_and_normalization_use_explicit_identity():
    row = {"symbol": "603656.SH", "reason": "002164.SZ"}
    raw = {"focus_pool": [row, {"symbol": "002164.SZ", "reason": "603656.SH"}]}
    assert c._candidate_pool_symbols({"focus_pool": [row]}, stage="A2") == {"603656.SH"}
    assert c._approved_symbols({"focus_pool": [row]}, "A2") == {"603656.SH"}
    projected = c._project_upstream_output(raw, {"603656.SH"}, stage="A2")
    assert [r["symbol"] for r in projected["focus_pool"]] == ["603656.SH"]
    assert c._normalize_pool_symbol(row) == row


@pytest.mark.parametrize("identity", [None, "", "bad", "603656.SH 002164.SZ", "leader 603656.SH"])
def test_a2_invalid_explicit_identity_does_not_recover_or_publish(identity):
    row = {"symbol": identity, "reason": "603656.SH"}
    assert c._normalize_pool_symbol(row) == row
    assert c._candidate_pool_symbols({"focus_pool": [row]}, stage="A2") == set()
    raw = {"focus_pool": [row], "watch_only_pool": [], "rejected_candidates": [],
        "envelope": {"stage_id": "A2", "model_name": "frozen", "status": "OK",
            "input_snapshot_ids": ["frozen"], "config_version": "frozen",
            "prompt_version": "frozen", "market_regime": "frozen"}}
    assert "CANDIDATE_LINEAGE_UNPROVABLE" in c._validate_output(
        raw, stage="A2", model="frozen", snapshot_id="frozen", upstream_symbols={"603656.SH"})


@pytest.mark.parametrize("scalar,expected", [("603656.SH", "603656.SH"),
    (" SHSE.603656 ", "603656.SH"), ("SZSE.002164", "002164.SZ")])
def test_genuine_legacy_scalar_identity_remains_canonical(scalar, expected):
    assert c._normalize_pool_symbol(scalar) == expected
    assert c._a3_candidate_identity(scalar) == expected


@pytest.mark.parametrize("value", ["leader 603656.SH", "603656.SH and 002164.SZ",
    {"reason": "603656.SH"}, {"symbol": "bad", "reason": "603656.SH"}])
def test_nonidentity_legacy_text_or_mapping_is_never_normalized_into_security(value):
    assert c._a3_candidate_identity(value) == ""
    assert c._normalize_pool_symbol(value) == value


@pytest.mark.parametrize("validator,codes", [
    ("_a3_origin_only_veto_reasons", ["WATCH_ONLY"]),
    ("_a3_prior_market_only_veto_reasons", ["PRIOR_DAY_NO_NEW_ENTRY"]),
    ("_a3_semantic_price_reasons", ["A3_REWARD_RISK_BELOW_MINIMUM"]),
])
def test_secondary_veto_checks_never_pick_context_from_narrative(validator, codes):
    # Controlled contract negative: explicit candidate is not QUALIFIED, the
    # mentioned leader is QUALIFIED. Its context must not cause a false veto.
    raw = {"secondary_watch_pool": [{"symbol": "603656.SH", "reason": "002164.SZ",
        "review_status": "VETO", "reason_codes": codes, "risk_unit": "NO_ENTRY"}]}
    snapshot = {
        "A3_CANDIDATE_ORIGIN": {"603656.SH": "FOCUS", "002164.SZ": "WATCH_ONLY"},
        "A3_DETERMINISTIC_CONTEXT": {
            "603656.SH": {"eligibility": "DATA_GAP"},
            "002164.SZ": {"eligibility": "QUALIFIED", "strategy_profile": "TREND_MA5", "route_permission": "ALLOW_A4"}},
        "PRICE_LEVELS": {"002164.SZ": {"available": True, "reward_risk": 3.0}},
    }
    assert getattr(c, validator)(raw, snapshot) == []

