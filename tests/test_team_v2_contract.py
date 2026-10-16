from copy import deepcopy
import pytest
from liangjian_funnel.pipeline.team_v2_contract import validate_team_output, blind_plan_facts
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository


CATALOG = {"categories": ["MARKET", "NEWS"], "items": [
    {"id": "M1", "category": "MARKET", "summary": "closed market fact"},
    {"id": "N1", "category": "NEWS", "summary": "published risk fact"},
]}


def vote(**changes):
    return {"symbol": "000001.SZ", "supporting_refs": ["M1"], "opposing_refs": ["N1"], **changes}


def response(**changes):
    return {"session": "PLAN_REVIEW", "core_watch_pool": [vote(review_status="PASS")],
            "secondary_watch_pool": [], "rejected_candidates": [],
            "handoff_notes": {"to_a4": ["check disclosure"]},
            "what_would_change_my_mind": ["updated disclosure contradicts thesis"], **changes}


def test_valid_output_is_not_mutated_and_keeps_opposing_search_evidence():
    raw = response(); before = deepcopy(raw)
    result = validate_team_output("A3", raw, CATALOG, session="PLAN_REVIEW", expected_ids={"000001.SZ"})
    assert result.valid and result.sentinel_count == 0 and raw == before


@pytest.mark.parametrize("field,value", [
    ("handoff_notes", {}), ("handoff_notes", {"to_a4": []}),
    ("handoff_notes", {"to_a4": [""]}), ("handoff_notes", {"to_a4": ["a"] * 4}),
    ("what_would_change_my_mind", []), ("what_would_change_my_mind", [""]),
    ("what_would_change_my_mind", ["a"] * 3),
])
def test_root_obligations_cannot_be_satisfied_by_nested_notes(field, value):
    assert not validate_team_output("A3", response(**{field: value}), CATALOG).valid


@pytest.mark.parametrize("changes", [
    {"supporting_refs": []}, {"opposing_refs": []}, {"opposing_refs": ["OUTSIDE"]},
    {"supporting_refs": ["M1"] * 4}, {"opposing_refs": ["N1", "N1"]},
    {"opposing_refs": ["NO_OPPOSING_EVIDENCE_FOUND"]},
    {"opposing_refs": ["NO_OPPOSING_EVIDENCE_FOUND", "N1"], "opposing_search_scope": ["MARKET", "NEWS"]},
    {"opposing_refs": ["NO_OPPOSING_EVIDENCE_FOUND"], "opposing_search_scope": ["NEWS", "NEWS"]},
    {"opposing_refs": ["NO_OPPOSING_EVIDENCE_FOUND"], "opposing_search_scope": ["NEWS", "UNKNOWN"]},
])
def test_invalid_references_fail_without_repair(changes):
    raw = response(core_watch_pool=[vote(**changes)])
    assert not validate_team_output("A3", raw, CATALOG).valid


def test_sentinel_is_only_legal_with_two_actual_catalog_categories():
    raw = response(core_watch_pool=[vote(opposing_refs=["NO_OPPOSING_EVIDENCE_FOUND"],
                                       opposing_search_scope=["NEWS", "MARKET"])])
    result = validate_team_output("A3", raw, CATALOG)
    assert result.valid and result.sentinel_count == 1
    assert result.judgments[0]["opposing_search_scope"] == ["NEWS", "MARKET"]


def test_duplicate_and_missing_candidate_never_default_to_pass():
    assert not validate_team_output("A3", response(core_watch_pool=[vote(), vote()]), CATALOG,
                                   expected_ids={"000001.SZ"}).valid
    assert not validate_team_output("A3", response(), CATALOG, expected_ids={"000001.SZ", "000002.SZ"}).valid


def test_empty_approved_pools_are_legal_with_complete_root_notes():
    assert validate_team_output("A3", response(core_watch_pool=[]), CATALOG, expected_ids=set()).valid


def test_a5_support_alias_does_not_relax_opposing_requirement():
    good = {"handoff_notes": {"to_a1": ["check method"]}, "what_would_change_my_mind": ["more samples"],
            "a2_review": {"evidence_ids": ["M1"], "opposing_refs": ["N1"]}}
    assert validate_team_output("A5", good, CATALOG).valid
    del good["a2_review"]["opposing_refs"]
    assert not validate_team_output("A5", good, CATALOG).valid


def test_a2_nomination_never_enters_candidate_scope():
    raw = response(core_watch_pool=[]); raw.pop("session")
    raw.update(focus_decisions=[], reject_decisions=[], theme_reviews=[], rotation_reviews=[],
               model_nominations=[vote(nomination_thesis="verify sector", falsifiers=["flow reverses"])])
    assert not validate_team_output("A2", raw, CATALOG, nomination_scope=set()).valid
    assert validate_team_output("A2", raw, CATALOG, nomination_scope={"000001.SZ"}).valid


def test_blind_projection_removes_decisions_recursively_without_rewriting_facts():
    raw = {"symbol": "000001.SZ", "eligibility": "QUALIFIED", "technical": {"close": 12,
           "met_conditions": ["PRESSURE"], "review_status": "PASS"}, "route": "TREND_MA5"}
    before = deepcopy(raw); blind = blind_plan_facts(raw)
    assert blind == {"symbol": "000001.SZ", "technical": {"close": 12}, "route": "TREND_MA5"}
    assert raw == before


def test_real_prompt_package_shared_is_used_and_values_are_rendered_once():
    from pathlib import Path
    repo = TeamPromptRepository(Path(__file__).resolve().parents[1] / "prompts/team_v2")
    doc = repo.document("A2")
    values = {key: {"status": "INSUFFICIENT"} for key in doc.placeholders}
    values["EVIDENCE_CATALOG"] = CATALOG
    request = repo.prepare("A2", values)
    assert request.shared_version == "shared-system/3.0.0"
    assert request.role_version == "agent-2/3.0.0"
    assert "Legacy TOP5 wording" not in request.system
    assert "{{" not in request.system
    assert request.system.count('"id":"M1"') == 1


def test_missing_prompt_input_fails_before_any_model_call():
    from pathlib import Path
    repo = TeamPromptRepository(Path(__file__).resolve().parents[1] / "prompts/team_v2")
    with pytest.raises(ValueError, match="UNRESOLVED_PLACEHOLDER"):
        repo.prepare("A2", {})


def test_a1_revision_instruction_is_not_a_callable_complete_prompt():
    from pathlib import Path
    repo = TeamPromptRepository(Path(__file__).resolve().parents[1] / "prompts/team_v2")
    with pytest.raises(ValueError, match="A1_COMPOSITION_REQUIRED"):
        repo.prepare("A1", {})


def test_handoff_can_omit_an_unneeded_recipient_but_not_all_recipients():
    assert validate_team_output("A3", response(handoff_notes={"to_a4": ["wait"], "to_a6": []}), CATALOG).valid


def test_raw_model_cannot_spoof_internal_judgment_path_to_escape_domain_checks():
    raw = response(core_watch_pool=[vote(path="not_a_candidate")])
    assert not validate_team_output("A3", raw, CATALOG, expected_ids={"000002.SZ"}).valid


@pytest.mark.parametrize("catalog", [
    {"categories": ["MARKET"], "items": [{"id": "M1", "category": []}]},
    {"categories": [["MARKET"]], "items": []},
])
def test_malformed_catalog_returns_invalid_not_a_validator_exception(catalog):
    assert not validate_team_output("A3", response(), catalog).valid


def test_plan_review_routes_only_its_own_session_and_blinds_both_inputs():
    from pathlib import Path
    repo = TeamPromptRepository(Path(__file__).resolve().parents[1] / "prompts/team_v2")
    values={k: [] for k in ('A3_CANDIDATE_POOL','A3_FACT_SHEET','MARKET_REGIME_SNAPSHOT',
                           'SECTOR_PERMISSIONS','A2_HANDOFF','RISK_CAPACITY','PRIOR_OUTCOME_FEEDBACK')}
    values['A3_FACT_SHEET']={'eligibility':'SECRET_QUALIFIED','price':12}
    values['A3_CANDIDATE_POOL']=[{'symbol':'000001.SZ','met_conditions':['SECRET_MET']}]
    values['EVIDENCE_CATALOG']=CATALOG
    before=deepcopy(values)
    request=repo.prepare('A3',values,session='PLAN_REVIEW')
    assert 'SECRET_QUALIFIED' not in request.system and 'SECRET_MET' not in request.system
    assert '═══════ 会话 PM_PREMARKET' not in request.system
    assert '═══════ 会话 PM_POST_CLOSE' not in request.system
    assert values==before
    with pytest.raises(TypeError):request.source_hashes['x']='bad'
