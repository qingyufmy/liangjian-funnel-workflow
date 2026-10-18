"""Approved A2 v3 schema: reject bad outputs, never repair them into a pass."""
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.team_v2_a2_schema import validate_a2_response


CATALOG = {"categories": ["MARKET", "NEWS"], "items": [
    {"id": "M", "category": "MARKET"}, {"id": "N", "category": "NEWS"}]}
REFS = {"supporting_refs": ["M"], "opposing_refs": ["N"]}
ENVELOPE = {"run_id": "frozen-run", "stage_id": "A2"}


def packet():
    return {"envelope": deepcopy(ENVELOPE), "reason_codes": [],
            "market_read": {"regime": "ROTATION", "dominant_themes": [],
                            "emotion_cycle": "CONFIRMATION", **REFS},
            "theme_reviews": [{"theme_id": theme, "stage": "CONFIRMATION",
                "new_entry_policy": "ALLOW", "penalty_codes": [], "support_codes": [],
                "risk_codes": [], "what_would_change_my_mind": ["flow reverses"], **REFS} for theme in ('T','U')],
            "focus_decisions": [{"symbol": "000001.SZ", "support_codes": [], "risk_codes": [], **REFS}],
            "rotation_reviews": [], "reject_decisions": [], "model_nominations": [],
            "handoff_notes": {"to_a3": ["watch next confirmation"]},
            "what_would_change_my_mind": ["new official evidence"], "unresolved_questions": []}


def validate(output, **overrides):
    context = dict(required_envelope=ENVELOPE, candidate_ids={"000001.SZ", "000002.SZ"},
                   nomination_scope={"000003.SZ"}, symbol_themes={"000001.SZ": "T", "000002.SZ": "U"},
                   allowed_theme_ids={"T", "U"}, direction_symbols={"D": {"000001.SZ", "000002.SZ"}})
    return validate_a2_response(output, CATALOG, **{**context, **overrides})


def test_authored_a2_response_valid_and_unchanged():
    raw = packet(); before = deepcopy(raw)
    assert validate(raw).valid and raw == before


def test_0283_no_focus_still_requires_every_frozen_theme_once():
    raw=packet();raw['focus_decisions']=[]
    raw['rotation_reviews']=[dict(rotation_direction_id='D',decision='NO_FOCUS',
        reviewed_symbols=['000001.SZ'],reason_codes=[],explanation='frozen weak structure',**REFS)]
    assert validate(raw).valid
    raw['theme_reviews']=raw['theme_reviews'][:1]
    assert not validate(raw).valid
    raw['theme_reviews'].append(deepcopy(raw['theme_reviews'][0]))
    assert not validate(raw).valid


@pytest.mark.parametrize("field", list(packet()))
def test_missing_any_authored_root_is_invalid(field):
    raw = packet(); del raw[field]
    assert not validate(raw).valid


@pytest.mark.parametrize("mutation", [
    lambda r: r.update(envelope={**ENVELOPE, "run_id": "another-run"}),
    lambda r: r.update(score_breakdown={}),
    lambda r: r["theme_reviews"][0].update(theme_score=99),
    lambda r: r["theme_reviews"][0].update(stage="UNKNOWN"),
    lambda r: r["theme_reviews"][0].update(new_entry_policy="NO_NEW_ENTRY"),
    lambda r: r["theme_reviews"][0].update(theme_id="OUTSIDE"),
    lambda r: r["theme_reviews"][0].update(what_would_change_my_mind=[]),
    lambda r: r["focus_decisions"][0].update(symbol="OUTSIDE"),
    lambda r: r["reject_decisions"].append({"symbol": "000001.SZ", "risk_codes": ["HARD"], **REFS}),
    lambda r: r["focus_decisions"].append(deepcopy(r["focus_decisions"][0])),
    lambda r: r.update(theme_reviews=[]),
    lambda r: r["rotation_reviews"].append({"rotation_direction_id": "D", "decision": "NO_FOCUS",
        "reviewed_symbols": ["000002.SZ"], "reason_codes": [], "explanation": "specific weakness", **REFS}),
    lambda r: r.update(model_nominations=[{"symbol": "000001.SZ", **REFS}]),
    lambda r: r.update(reason_codes=["A"] * 5),
])
def test_invalid_outputs_are_not_silently_fixed(mutation):
    raw = packet(); mutation(raw); before = deepcopy(raw)
    assert not validate(raw).valid and raw == before


def test_empty_rotation_domain_requires_empty_review_not_a_summary_placeholder():
    assert validate(packet(), direction_symbols={}).valid
    raw = packet(); raw["rotation_reviews"] = [{"rotation_direction_id": "SUMMARY", "decision": "NO_FOCUS",
        "reviewed_symbols": [], "reason_codes": [], "explanation": "summary", **REFS}]
    assert not validate(raw, direction_symbols={}).valid


def test_direction_without_focus_requires_exact_review_and_in_domain_symbols():
    raw = packet(); raw["rotation_reviews"] = [{"rotation_direction_id": "E", "decision": "NO_FOCUS",
        "reviewed_symbols": ["000002.SZ"], "reason_codes": [], "explanation": "frozen weak breadth", **REFS}]
    directions = {"D": {"000001.SZ"}, "E": {"000002.SZ"}}
    assert validate(raw, direction_symbols=directions).valid
    raw["rotation_reviews"][0]["reviewed_symbols"] = ["000001.SZ"]
    assert not validate(raw, direction_symbols=directions).valid


def test_nomination_is_disjoint_research_only_and_cannot_create_theme_review():
    raw = packet(); raw["model_nominations"] = [{"symbol": "000003.SZ", "theme_id": "V",
        "nomination_thesis": "fresh catalyst", "quant_filter_reason": "closed quant filter",
        "why_i_disagree": "frozen flow", "falsifiers": ["flow reverses"], **REFS}]
    assert validate(raw).valid
    assert not validate(raw, nomination_scope={"000001.SZ", "000003.SZ"},
                        candidate_ids={"000001.SZ", "000002.SZ", "000003.SZ"}).valid


@pytest.mark.parametrize("stage,policy", [("IGNITION", "PROBE_ONLY"), ("CONFIRMATION", "ALLOW"),
    ("ACCELERATION", "ALLOW"), ("CLIMAX", "WATCH_ONLY"), ("DIVERGENCE", "WATCH_ONLY"),
    ("RETREAT", "NO_NEW_ENTRY"), ("REPAIR", "ALLOW"), ("FADE", "NO_NEW_ENTRY")])
def test_authored_stage_policy_map(stage, policy):
    raw = packet(); raw["theme_reviews"][0].update(stage=stage, new_entry_policy=policy)
    assert validate(raw).valid


@pytest.mark.parametrize("raw", [None, [], {"focus_decisions": [None]}, {"theme_reviews": "wrong"}])
def test_malformed_model_json_returns_invalid_not_exception(raw):
    assert not validate(raw).valid


@pytest.mark.parametrize("value", [None, [], {}, True, 42])
@pytest.mark.parametrize("pool,key", [("model_nominations", "symbol"), ("focus_decisions", "symbol"),
    ("theme_reviews", "stage"), ("theme_reviews", "theme_id"), ("market_read", "emotion_cycle")])
def test_wrong_typed_scalar_is_rejected_without_validator_exception(pool, key, value):
    raw = packet()
    if pool == "model_nominations":
        raw[pool] = [{"symbol": "000003.SZ", "theme_id": "T", "nomination_thesis": "catalyst",
            "quant_filter_reason": "quant", "why_i_disagree": "flow", "falsifiers": ["reversal"], **REFS}]
    row = raw[pool] if pool == "market_read" else raw[pool][0]
    row[key] = value
    assert not validate(raw).valid
