"""Real existing client + HTTP mock: no model network, secrets or production DB."""
from datetime import datetime
from dataclasses import replace
from pathlib import Path
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.pipeline.team_v2_a2_schema import validate_a2_response
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.pipeline.team_v2_model import call_team_model
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository, _ENVELOPE_KEYS
from liangjian_funnel.settings import Settings

MODEL = "deepseek-v4-pro-0813"
CATALOG = {"categories": ["MARKET", "NEWS"], "items": [
    {"id": "M", "category": "MARKET"}, {"id": "N", "category": "NEWS"}]}
NOW = datetime.fromisoformat("2026-10-16T18:40:00+08:00")


def setup(tmp_path, *, bad_output=False, failure=False, bad_regime=False):
    envelope = {key: "FROZEN" for key in _ENVELOPE_KEYS}
    envelope.update(stage_id="A2", model_name=MODEL, run_id="local-team-test",
                    source_trade_date="2026-10-16", execution_trade_date="2026-10-19",
                    market_regime="ROTATION_NO_MAINLINE")
    repo = TeamPromptRepository(Path(__file__).resolve().parents[1] / "prompts/team_v2")
    values = {key: [] for key in repo.document("A2").placeholders}; values["EVIDENCE_CATALOG"] = CATALOG
    request = repo.prepare_messages("A2", values, required_envelope=envelope, session="CLOSE")
    output = {"envelope": envelope, "reason_codes": [], "market_read": {"regime": envelope["market_regime"],
        "dominant_themes": [], "emotion_cycle": "CONFIRMATION", "supporting_refs": ["M"], "opposing_refs": ["N"]},
        "theme_reviews": [], "focus_decisions": [], "rotation_reviews": [], "reject_decisions": [],
        "model_nominations": [], "handoff_notes": {"to_a3": ["no focused theme"]},
        "what_would_change_my_mind": ["fresh fact"], "unresolved_questions": []}
    if bad_output: output["market_read"]["supporting_refs"] = ["OUTSIDE"]
    if bad_regime: output["market_read"]["regime"] = "TREND_MAINLINE"
    seen = []
    def handler(http_request):
        seen.append(json.loads(http_request.read()))
        if failure: raise httpx.ConnectError("TEST_ONLY_SENSITIVE_ERROR_DO_NOT_STORE", request=http_request)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(output)}}]}, request=http_request)
    settings = Settings.from_env({"LIANGJIAN_MODEL_API_KEY": "UNIT_TEST_ONLY_NOT_A_CREDENTIAL"}, root=tmp_path).model_copy(
        update={"model_base_url": "https://legacy-gateway.example.test/v1"})
    client = OpenAICompatibleModelClient(settings, transport=httpx.MockTransport(handler), max_attempts=1)
    journal = ModelJudgmentJournal(tmp_path / "research.sqlite3")
    def validator(result):
        return validate_a2_response(result, CATALOG, required_envelope=envelope, candidate_ids=set(),
            nomination_scope=set(), symbol_themes={}, allowed_theme_ids=set(), direction_symbols={})
    kwargs = dict(call_id="local:A2:attempt1", run_id="local-team-test", model=MODEL, session="CLOSE",
        source_trade_date="2026-10-16", execution_trade_date="2026-10-19", catalog=CATALOG,
        timeout_seconds=30, max_output_tokens=4096, validator=validator, clock=lambda: NOW, allow_model_call=True)
    return request, client, journal, kwargs, seen


def test_actual_client_and_authored_a2_schema_capture_without_changing_model_or_retry_policy(tmp_path):
    request, client, journal, kwargs, seen = setup(tmp_path)
    result = call_team_model(request, client, journal, **kwargs)
    assert result.validation.valid and len(seen) == 1
    assert seen[0]["model"] == MODEL and seen[0]["enable_thinking"] is False
    assert len(seen[0]["messages"]) == 2
    assert journal.summary()[0]["logical_calls"] == 1
    with pytest.raises(ValueError, match="ALREADY_RECORDED"):
        call_team_model(request, client, journal, **kwargs)
    assert len(seen) == 1


def test_invalid_original_json_is_preserved_not_repaired_or_retried(tmp_path):
    request, client, journal, kwargs, seen = setup(tmp_path, bad_output=True)
    result = call_team_model(request, client, journal, **kwargs)
    assert not result.validation.valid and len(seen) == 1
    with sqlite3.connect(journal.path) as db:
        valid, output = db.execute("SELECT validated,output_json FROM model_calls").fetchone()
        assert valid == 0 and json.loads(output)["market_read"]["supporting_refs"] == ["OUTSIDE"]


def test_market_regime_mismatch_stays_invalid_and_original_is_not_repaired(tmp_path):
    request,client,journal,kwargs,seen=setup(tmp_path,bad_regime=True)
    result=call_team_model(request,client,journal,**kwargs)
    assert result.validation.issues==('A2_MARKET_REGIME_BINDING_MISMATCH',) and len(seen)==1
    with sqlite3.connect(journal.path) as db:
        valid,text=db.execute('SELECT validated,output_json FROM model_calls').fetchone()
    raw=json.loads(text)
    assert valid==0 and raw['envelope']['market_regime']=='ROTATION_NO_MAINLINE'
    assert raw['market_read']['regime']=='TREND_MAINLINE'


def test_transport_failure_leaves_durable_claim_and_only_stable_code(tmp_path):
    request, client, journal, kwargs, seen = setup(tmp_path, failure=True)
    with pytest.raises(ValueError, match="^TEAM_MODEL_CALL_FAILED$"):
        call_team_model(request, client, journal, **kwargs)
    with pytest.raises(ValueError, match="OUTCOME_UNKNOWN_NO_RETRY"):
        call_team_model(request, client, journal, **kwargs)
    with sqlite3.connect(journal.path) as db:
        assert db.execute("SELECT state,failure_code FROM model_call_claims").fetchone() == ("FAILED", "TEAM_MODEL_CALL_FAILED")
    assert len(seen) == 1 and b"SENSITIVE_ERROR" not in journal.path.read_bytes()


def test_schema_implementation_exception_preserves_raw_response_as_invalid(tmp_path):
    request, client, journal, kwargs, seen = setup(tmp_path)
    def broken(_): raise RuntimeError("TEST_ONLY_PRIVATE_EXCEPTION")
    kwargs["validator"] = broken
    result = call_team_model(request, client, journal, **kwargs)
    assert not result.validation.valid and result.validation.issues == ("TEAM_VALIDATOR_FAILED",)
    with sqlite3.connect(journal.path) as db:
        assert db.execute("SELECT output_json FROM model_calls").fetchone()[0]
    assert b"PRIVATE_EXCEPTION" not in journal.path.read_bytes() and len(seen) == 1


@pytest.mark.parametrize("change,code", [
    ({"run_id": "wrong"}, "ENVELOPE_MISMATCH"),
    ({"session": "WRONG"}, "ENVELOPE_MISMATCH"),
    ({"catalog": {"categories": [], "items": []}}, "CATALOG_BINDING_MISMATCH"),
    ({"timeout_seconds": float("inf")}, "BUDGET_INVALID"),
    ({"timeout_seconds": True}, "BUDGET_INVALID"),
    ({"max_output_tokens": False}, "BUDGET_INVALID"),
    ({"allow_model_call": False}, "NOT_AUTHORIZED"),
    ({"clock": lambda: datetime(2026, 10, 16)}, "CLOCK_INVALID"),
])
def test_invalid_call_binding_never_reaches_client(tmp_path, change, code):
    request, client, journal, kwargs, seen = setup(tmp_path); kwargs.update(change)
    with pytest.raises(ValueError, match=code): call_team_model(request, client, journal, **kwargs)
    assert not seen


def test_inflight_claim_blocks_duplicate_before_network(tmp_path):
    request, client, journal, kwargs, seen = setup(tmp_path)
    journal.claim(kwargs["call_id"], "another-request")
    with pytest.raises(ValueError, match="BINDING_CONFLICT"): call_team_model(request, client, journal, **kwargs)
    assert not seen


def test_forged_prompt_digest_fails_before_provider_or_claim(tmp_path):
    request, client, journal, kwargs, seen = setup(tmp_path)
    forged = replace(request, prompt=replace(request.prompt, sha256='0' * 64))
    with pytest.raises(ValueError, match='MESSAGE_BINDING_MISMATCH'):
        call_team_model(forged, client, journal, **kwargs)
    assert not seen
