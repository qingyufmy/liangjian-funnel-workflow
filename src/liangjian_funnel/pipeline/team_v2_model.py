"""Team transport through the existing client, with durable research receipts.

No model defaults, strategy/eligibility changes, retries, broker or customer I/O.
Live callers must provide their role/session validator and frozen metadata.
"""
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
from typing import Callable

from .model_client import ModelCallResult
from .team_v2_contract import TeamValidation
from .team_v2_judgments import ModelJudgmentJournal
from .team_v2_prompts import TeamMessageRequest


@dataclass(frozen=True)
class TeamCallOutcome:
    response: ModelCallResult
    validation: TeamValidation
    receipt: dict


def _hash(value):
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(text.encode()).hexdigest()


def team_call_binding(request, *, run_id, model, session, timeout_seconds, max_output_tokens):
    return _hash({'run_id':run_id,'session':session,'role':request.prompt.role,
        'input_hash':request.input_hash,'model':model,'timeout_seconds':timeout_seconds,
        'max_output_tokens':max_output_tokens,'catalog_hash':request.evidence_catalog_hash})


def call_team_model(request: TeamMessageRequest, client, journal: ModelJudgmentJournal, *,
                    call_id: str, run_id: str, model: str, session: str,
                    source_trade_date: str, execution_trade_date: str, catalog: dict,
                    timeout_seconds: float, max_output_tokens: int,
                    validator: Callable[[dict], TeamValidation], clock: Callable[[], datetime],
                    allow_model_call: bool = False) -> TeamCallOutcome:
    if not allow_model_call:
        raise ValueError("TEAM_MODEL_CALL_NOT_AUTHORIZED")
    messages = [dict(message) for message in request.messages]
    if (_hash(messages) != request.input_hash or len(messages) != 2
            or messages[0].get('role') != 'system' or messages[1].get('role') != 'user'
            or hashlib.sha256(messages[0]['content'].encode()).hexdigest() != request.prompt.sha256):
        raise ValueError("TEAM_MESSAGE_BINDING_MISMATCH")
    runtime = json.loads(messages[1]["content"].removeprefix("RUNTIME_INPUT\n"))
    envelope = runtime["required_envelope"]
    if (envelope["model_name"] != model or envelope["run_id"] != run_id
            or envelope["source_trade_date"] != source_trade_date
            or envelope["execution_trade_date"] != execution_trade_date
            or envelope["stage_id"] != request.prompt.role or runtime["session"] != session):
        raise ValueError("TEAM_CALL_ENVELOPE_MISMATCH")
    if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or timeout_seconds <= 0
            or type(max_output_tokens) is not int or max_output_tokens <= 0):
        raise ValueError("TEAM_CALL_BUDGET_INVALID")
    if request.prompt.role == "A4" and timeout_seconds > 30:
        raise ValueError("TEAM_CALL_BUDGET_INVALID")
    if request.prompt.role == "A6" and session == "INTRADAY_EXCEPTION" and timeout_seconds > 20:
        raise ValueError("TEAM_CALL_BUDGET_INVALID")
    catalog_hash = _hash(catalog)
    if catalog_hash != request.evidence_catalog_hash or catalog_hash != runtime["catalog_sha256"]:
        raise ValueError("TEAM_CATALOG_BINDING_MISMATCH")
    for day in (source_trade_date, execution_trade_date):
        if date.fromisoformat(day).isoformat() != day:raise ValueError("TEAM_CALL_DATE_INVALID")
    now=clock()
    if not isinstance(now,datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("TEAM_CALL_CLOCK_INVALID")
    binding = team_call_binding(request,run_id=run_id,model=model,session=session,
        timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens)
    journal.claim(call_id, binding)
    try:
        response = client.complete(model, messages, prompt_hash=request.prompt.sha256,
            input_hash=request.input_hash, stage=request.prompt.role,
            timeout_seconds=timeout_seconds, max_output_tokens=max_output_tokens)
    except Exception:
        journal.mark_failed(call_id, "TEAM_MODEL_CALL_FAILED")
        raise ValueError("TEAM_MODEL_CALL_FAILED") from None
    if (not isinstance(response, ModelCallResult) or response.model != model
            or response.prompt_hash != request.prompt.sha256 or response.input_hash != request.input_hash):
        journal.mark_failed(call_id, "TEAM_MODEL_RESULT_INVALID")
        raise ValueError("TEAM_MODEL_RESULT_INVALID")
    try:
        validation = validator(response.output)
        if not isinstance(validation, TeamValidation) or validation.valid != (not validation.issues):
            raise ValueError("TEAM_VALIDATOR_FAILED")
    except Exception:
        # The schema implementation can fail too. Preserve original model JSON,
        # but never turn a validator exception into a valid or executable vote.
        validation = TeamValidation(False, ("TEAM_VALIDATOR_FAILED",), (), 0)
    receipt = journal.capture(call_id=call_id, run_id=run_id, role=request.prompt.role, session=session,
        source_trade_date=source_trade_date, execution_trade_date=execution_trade_date, captured_at=clock(),
        catalog_hash=catalog_hash, response=response, validation=validation,catalog=catalog)
    return TeamCallOutcome(response, validation, receipt)
