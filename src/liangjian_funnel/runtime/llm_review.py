"""Strict, identity-bound contract for the veto-only A4 model review.

The model is an advisory risk reviewer after deterministic eligibility.  It
cannot create candidates or execution parameters.  This module deliberately
keeps transport metadata outside the untrusted model JSON and validates the
candidate set, evidence references and deadline again at the consumer edge.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


REQUEST_SCHEMA = "a4-llm-review-request/2.0.0"
RESPONSE_SCHEMA = "a4-llm-review/2.0.0"
_REASON = re.compile(r"^[A-Z][A-Z0-9_\-]{0,79}$")
_RESPONSE_KEYS = {"schema_version", "decision_id", "minute_snapshot_id", "signals"}
_SIGNAL_KEYS = {"plan_id", "llm_veto", "reason_code", "evidence_refs"}


class LLMReviewError(RuntimeError):
    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class FrozenReviewRequest:
    schema_version: str
    decision_id: str
    lane_id: str
    minute_snapshot_id: str
    minute_end: str
    eligible_plan_ids: tuple[str, ...]
    input_hash: str
    model_identity: str
    prompt_version: str
    evidence_refs_by_plan: Mapping[str, tuple[str, ...]]
    response_deadline_monotonic: float
    frozen_monotonic: float

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "decision_id": self.decision_id,
            "lane_id": self.lane_id,
            "minute_snapshot_id": self.minute_snapshot_id,
            "minute_end": self.minute_end,
            "eligible_plan_ids": list(self.eligible_plan_ids),
            "input_hash": self.input_hash,
            "model_identity": self.model_identity,
            "prompt_version": self.prompt_version,
            "evidence_catalog": {
                plan_id: list(self.evidence_refs_by_plan[plan_id])
                for plan_id in self.eligible_plan_ids
            },
            "response_deadline_monotonic": self.response_deadline_monotonic,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FrozenReviewRequest":
        try:
            plan_ids = tuple(str(item) for item in value["eligible_plan_ids"])
            catalog = value["evidence_catalog"]
            return cls(
                schema_version=str(value["schema_version"]),
                decision_id=str(value["decision_id"]),
                lane_id=str(value["lane_id"]),
                minute_snapshot_id=str(value["minute_snapshot_id"]),
                minute_end=str(value["minute_end"]),
                eligible_plan_ids=plan_ids,
                input_hash=str(value["input_hash"]),
                model_identity=str(value["model_identity"]),
                prompt_version=str(value["prompt_version"]),
                evidence_refs_by_plan={
                    plan_id: tuple(str(item) for item in catalog[plan_id])
                    for plan_id in plan_ids
                },
                response_deadline_monotonic=float(value["response_deadline_monotonic"]),
                frozen_monotonic=float(value.get("frozen_monotonic", 0.0)),
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise LLMReviewError("LLM_REVIEW_REQUEST_INVALID") from exc


@dataclass(frozen=True, slots=True)
class ReviewTransportAudit:
    model: str
    prompt_version: str
    prompt_hash: str | None
    input_hash: str | None
    latency_ms: int
    attempts: int
    thinking_variant: str
    reasoning_tokens: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost: float | None = None
    model_response_id: str | None = None
    output_hash: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReviewCallbackResult:
    raw_response: Mapping[str, Any]
    audit: ReviewTransportAudit
    received_monotonic: float | None = None


@dataclass(frozen=True, slots=True)
class StrictReviewDecision:
    plan_id: str
    llm_veto: bool
    reason_code: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ValidatedReviewBatch:
    request: FrozenReviewRequest
    decisions: tuple[StrictReviewDecision, ...]
    audit: ReviewTransportAudit | None

    def by_plan(self) -> dict[str, StrictReviewDecision]:
        return {item.plan_id: item for item in self.decisions}


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise LLMReviewError("LLM_REVIEW_INPUT_NOT_CANONICAL") from exc


def freeze_llm_review_request(
    *,
    lane_id: str,
    minute_snapshot_id: str,
    minute_end: datetime,
    eligible_plans: Sequence[Mapping[str, Any]],
    response_deadline_monotonic: float,
    frozen_monotonic: float,
    model_identity: str,
    prompt_version: str,
    frozen_input: Any | None = None,
) -> FrozenReviewRequest:
    if not lane_id or not minute_snapshot_id or not eligible_plans or not model_identity or not prompt_version:
        raise LLMReviewError("LLM_REVIEW_REQUEST_INVALID")
    if not math.isfinite(response_deadline_monotonic) or response_deadline_monotonic <= frozen_monotonic:
        raise LLMReviewError("LLM_REVIEW_DEADLINE_INVALID")
    normalized: list[dict[str, Any]] = []
    plan_ids: list[str] = []
    catalog: dict[str, tuple[str, ...]] = {}
    for item in eligible_plans:
        plan_id = str(item.get("plan_id") or "").strip()
        symbol = str(item.get("symbol") or "").strip()
        if not plan_id or plan_id in plan_ids or item.get("eligible") is not True:
            raise LLMReviewError("LLM_REVIEW_ELIGIBLE_SET_INVALID")
        plan_ids.append(plan_id)
        normalized.append(dict(item))
        catalog[plan_id] = (
            f"PLAN:{plan_id}",
            f"TRIGGER:{plan_id}",
            f"SNAPSHOT:{minute_snapshot_id}",
            *(
                (f"MARKET:{symbol}",)
                if symbol
                and isinstance(item.get("market_context"), Mapping)
                and item.get("market_context")
                else ()
            ),
        )
    input_hash = hashlib.sha256(_canonical(
        frozen_input if frozen_input is not None else normalized
    )).hexdigest()
    identity = {
        "schema_version": REQUEST_SCHEMA,
        "lane_id": lane_id,
        "minute_snapshot_id": minute_snapshot_id,
        "minute_end": minute_end.isoformat(),
        "eligible_plan_ids": plan_ids,
        "input_hash": input_hash,
        "model_identity": model_identity,
        "prompt_version": prompt_version,
    }
    decision_id = "llm-review:" + hashlib.sha256(_canonical(identity)).hexdigest()[:32]
    return FrozenReviewRequest(
        schema_version=REQUEST_SCHEMA,
        decision_id=decision_id,
        lane_id=lane_id,
        minute_snapshot_id=minute_snapshot_id,
        minute_end=minute_end.isoformat(),
        eligible_plan_ids=tuple(plan_ids),
        input_hash=input_hash,
        model_identity=model_identity,
        prompt_version=prompt_version,
        evidence_refs_by_plan=catalog,
        response_deadline_monotonic=float(response_deadline_monotonic),
        frozen_monotonic=float(frozen_monotonic),
    )


def validate_llm_review_response(
    request: FrozenReviewRequest,
    response: Mapping[str, Any] | ReviewCallbackResult,
    *,
    received_monotonic: float | None = None,
) -> ValidatedReviewBatch:
    callback = response if isinstance(response, ReviewCallbackResult) else None
    raw = callback.raw_response if callback is not None else response
    audit = callback.audit if callback is not None else None
    observed = received_monotonic if received_monotonic is not None else (
        callback.received_monotonic if callback is not None else None
    )
    if observed is None or not math.isfinite(float(observed)):
        raise LLMReviewError("LLM_RESPONSE_TIME_UNVERIFIED")
    if float(observed) > request.response_deadline_monotonic:
        raise LLMReviewError("LLM_RESPONSE_DEADLINE_EXCEEDED")
    if not isinstance(raw, Mapping):
        raise LLMReviewError("LLM_RESPONSE_SCHEMA_INVALID")
    if set(raw) != _RESPONSE_KEYS:
        raise LLMReviewError("LLM_OUTPUT_PERMISSION_ESCALATION" if set(raw) - _RESPONSE_KEYS else "LLM_RESPONSE_SCHEMA_INVALID")
    if raw.get("schema_version") != RESPONSE_SCHEMA:
        raise LLMReviewError("LLM_RESPONSE_SCHEMA_INVALID")
    if raw.get("decision_id") != request.decision_id:
        raise LLMReviewError("LLM_DECISION_ID_MISMATCH")
    if raw.get("minute_snapshot_id") != request.minute_snapshot_id:
        raise LLMReviewError("LLM_SNAPSHOT_ID_MISMATCH")
    signals = raw.get("signals")
    if not isinstance(signals, list):
        raise LLMReviewError("LLM_SIGNALS_TYPE_INVALID")

    expected = set(request.eligible_plan_ids)
    seen: set[str] = set()
    decisions: dict[str, StrictReviewDecision] = {}
    for record in signals:
        if not isinstance(record, Mapping):
            raise LLMReviewError("LLM_SIGNAL_SCHEMA_INVALID")
        if set(record) != _SIGNAL_KEYS:
            raise LLMReviewError(
                "LLM_OUTPUT_PERMISSION_ESCALATION"
                if set(record) - _SIGNAL_KEYS else "LLM_SIGNAL_SCHEMA_INVALID"
            )
        plan_id = record.get("plan_id")
        if not isinstance(plan_id, str) or not plan_id:
            raise LLMReviewError("LLM_PLAN_ID_INVALID")
        if plan_id in seen:
            raise LLMReviewError("LLM_DUPLICATE_PLAN_ID")
        if plan_id not in expected:
            raise LLMReviewError("LLM_UNKNOWN_PLAN_ID")
        seen.add(plan_id)
        veto = record.get("llm_veto")
        if type(veto) is not bool:
            raise LLMReviewError("LLM_VETO_TYPE_INVALID")
        reason = record.get("reason_code")
        if not isinstance(reason, str) or not _REASON.fullmatch(reason):
            raise LLMReviewError("LLM_REASON_CODE_INVALID")
        if (not veto and reason != "PASS") or (veto and reason == "PASS"):
            raise LLMReviewError("LLM_REASON_CODE_CONFLICT")
        refs = record.get("evidence_refs")
        if not isinstance(refs, list) or len(refs) > 3 or any(not isinstance(item, str) for item in refs):
            raise LLMReviewError("LLM_EVIDENCE_REFS_INVALID")
        if len(set(refs)) != len(refs):
            raise LLMReviewError("LLM_EVIDENCE_REFS_INVALID")
        if veto and not refs:
            raise LLMReviewError("LLM_EVIDENCE_REQUIRED")
        allowed = set(request.evidence_refs_by_plan[plan_id])
        if any(item not in allowed for item in refs):
            raise LLMReviewError("LLM_EVIDENCE_REF_INVALID")
        decisions[plan_id] = StrictReviewDecision(plan_id, veto, reason, tuple(refs))
    if seen != expected:
        raise LLMReviewError("LLM_CANDIDATE_SET_MISMATCH")
    if audit is not None:
        if audit.input_hash != request.input_hash:
            raise LLMReviewError("LLM_INPUT_IDENTITY_MISMATCH")
        if audit.model != request.model_identity or audit.prompt_version != request.prompt_version:
            raise LLMReviewError("LLM_MODEL_OR_PROMPT_IDENTITY_MISMATCH")
        if audit.latency_ms < 0 or audit.attempts < 1:
            raise LLMReviewError("LLM_TRANSPORT_AUDIT_INVALID")
        for value in (audit.input_tokens, audit.output_tokens, audit.reasoning_tokens):
            if value is not None and (type(value) is not int or value < 0):
                raise LLMReviewError("LLM_TRANSPORT_AUDIT_INVALID")
        if audit.cost is not None and (not math.isfinite(audit.cost) or audit.cost < 0):
            raise LLMReviewError("LLM_TRANSPORT_AUDIT_INVALID")
    return ValidatedReviewBatch(
        request=request,
        decisions=tuple(decisions[plan_id] for plan_id in request.eligible_plan_ids),
        audit=audit,
    )


def isolate_untrusted_text(*, evidence_id: str, source_identity: str, text: str) -> dict[str, Any]:
    encoded = str(text).encode("utf-8", errors="replace")
    return {
        "evidence_id": str(evidence_id),
        "source_identity": str(source_identity),
        "trust": "UNTRUSTED_DATA",
        "permissions": [],
        "content_sha256": hashlib.sha256(encoded).hexdigest(),
        "content": str(text)[:20_000],
    }


def assert_shadow_review_isolation(
    *,
    official_store: str | Path,
    shadow_store: str | Path,
    official_account_id: str,
    shadow_account_id: str,
    official_output_dir: str | Path,
    shadow_output_dir: str | Path,
) -> None:
    if Path(official_store).resolve() == Path(shadow_store).resolve():
        raise LLMReviewError("SHADOW_STORE_NOT_ISOLATED")
    if official_account_id == shadow_account_id:
        raise LLMReviewError("SHADOW_ACCOUNT_NOT_ISOLATED")
    if Path(official_output_dir).resolve() == Path(shadow_output_dir).resolve():
        raise LLMReviewError("SHADOW_OUTPUT_NOT_ISOLATED")


__all__ = [
    "FrozenReviewRequest",
    "LLMReviewError",
    "RESPONSE_SCHEMA",
    "ReviewCallbackResult",
    "ReviewTransportAudit",
    "StrictReviewDecision",
    "ValidatedReviewBatch",
    "assert_shadow_review_isolation",
    "freeze_llm_review_request",
    "isolate_untrusted_text",
    "validate_llm_review_response",
]
