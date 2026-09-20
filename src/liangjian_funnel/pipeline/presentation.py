"""Canonical read-only presentation semantics for A1-A4 research facts.

The projection never grants trading permission.  It is attached to persisted
stage rows so API, Markdown and notification consumers can render the same
meaning without inventing scores or collapsing unknown data into zero/safe.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any


SCHEMA_VERSION = "research-presentation/1.0.0"
_A1_POOLS = {
    "approved": "RESEARCH_ADMITTED_NOT_QUALITY_CERTIFICATE",
    "watch": "RESEARCH_MONITOR_ONLY",
    "rejected": "RESEARCH_NOT_ADMITTED",
}
_GAP_TOKENS = ("MISSING", "GAP", "UNAVAILABLE", "UNVERIFIED", "INSUFFICIENT", "NOT_PUBLISHED")
_NEGATIVE_TOKENS = ("NEGATIVE", "DECLINE", "LOSS", "WEAK", "BEARISH", "VETO", "REJECT")


def _sequence(value: Any) -> list[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return list(value)


def _strings(value: Any) -> list[str]:
    return list(dict.fromkeys(str(item).strip() for item in _sequence(value) if str(item).strip()))


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _source_identities(row: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key in (
        "source_refs", "supporting_source_refs", "base_source_refs",
        "theme_source_refs", "node_source_refs", "evidence_refs",
    ):
        raw = row.get(key)
        for item in _sequence(raw):
            if isinstance(item, Mapping):
                identity = _first(item, "source_id", "source", "fact_id", "evidence_id", "content_hash")
                if identity:
                    values.append(str(identity))
            elif str(item).strip():
                values.append(str(item).strip())
    return list(dict.fromkeys(values))[:100]


def _reason_codes(row: Mapping[str, Any]) -> list[str]:
    values = _strings(row.get("reason_codes"))
    single = _first(row, "reason_code", "reasonCode")
    if single and str(single) not in values:
        values.append(str(single))
    return values


def _explicit_total_score(row: Mapping[str, Any], stage: str) -> float | None:
    if stage == "A1":
        return _number(_first(row, "structural_score", "structuralScore", "composite_score", "total_score"))
    if stage == "A2":
        return _number(_first(row, "stock_composite_score", "stock_total_score", "individual_total_score"))
    return _number(_first(row, "technical_score", "technicalScore"))


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _a3_target(row: Mapping[str, Any]) -> dict[str, Any]:
    facts = _mapping(row.get("strategy_facts"))
    observations = _mapping(facts.get("observation_targets"))
    basis = _first(row, "target_basis", "pressure_basis") or observations.get("target_basis")
    resistance = _number(_first(row, "first_resistance", "pressure_reduce_price"))
    r2 = _number(observations.get("r2"))
    if str(basis or "").upper() == "R_MULTIPLE_NO_RESISTANCE_REQUIRED" or (
        row.get("price_discovery") is True and resistance is None and r2 is not None
    ):
        return {
            "price": r2,
            "kind": "FIXED_R_OBSERVATION",
            "basis": "R_MULTIPLE_NO_RESISTANCE_REQUIRED",
            "claim": "OBSERVATION_NOT_MARKET_PROOF",
        }
    if resistance is not None:
        return {
            "price": resistance,
            "kind": "OBSERVED_RESISTANCE",
            "basis": str(basis or "FIRST_RESISTANCE"),
            "claim": "MARKET_STRUCTURE_REFERENCE",
        }
    return {"price": None, "kind": "UNAVAILABLE", "basis": None, "claim": "NO_TARGET_EVIDENCE"}


def _a3_projection(row: Mapping[str, Any], pool: str, now: datetime | None) -> dict[str, Any]:
    eligibility = str(_first(row, "deterministic_eligibility", "eligibility") or "UNKNOWN").upper()
    permission = str(_first(row, "execution_permission", "route_permission") or "").upper()
    deferred = _strings(_first(row, "a4_deferred_conditions", "confirmation_conditions") or [])
    if permission in {"BLOCKED", "NO_NEW_ENTRY", "RESEARCH_ONLY"}:
        entry = "BLOCKED"
    elif eligibility == "DATA_GAP":
        entry = "DATA_GAP"
    elif eligibility == "QUALIFIED" and pool == "approved":
        entry = "PENDING_A4"
    else:
        entry = "NOT_ELIGIBLE"
    confirmation = "REQUIRED" if entry == "PENDING_A4" or deferred else "NOT_APPLICABLE"
    expiry_raw = _first(row, "plan_expiry", "expires_at", "expiresAt")
    valid_from_raw = _first(row, "valid_from", "validFrom")
    expiry = _parse_time(expiry_raw)
    valid_from = _parse_time(valid_from_raw)
    if not _first(row, "plan_id", "planId"):
        validity = "NOT_PUBLISHED"
    elif now is None or expiry is None:
        validity = "TIME_UNVERIFIED"
    elif now > expiry:
        validity = "EXPIRED"
    elif valid_from is not None and now < valid_from:
        validity = "NOT_YET_VALID"
    else:
        validity = "VALID_WINDOW"
    return {
        "daily_setup_state": eligibility,
        "a4_confirmation_state": confirmation,
        "current_entry_eligibility": entry,
        "plan_validity_state": validity,
        "valid_from": valid_from_raw,
        "expires_at": expiry_raw,
        "deferred_conditions": deferred,
        "execution_permission": permission or None,
        "target": _a3_target(row),
    }


def project_research_presentation(
    row: Mapping[str, Any], *, stage: str, pool: str, now: datetime | None = None,
) -> dict[str, Any]:
    canonical_stage = str(stage).upper()
    canonical_pool = str(pool).lower()
    reasons = _reason_codes(row)
    score = _explicit_total_score(row, canonical_stage)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "stage": canonical_stage,
        "pool": canonical_pool,
        "display_score": score,
        "score_meaning": "EXPLICIT_STAGE_SCORE" if score is not None else "NO_INDIVIDUAL_TOTAL_SCORE",
        "source_identities": _source_identities(row),
        "legacy_adapter": False,
    }
    if canonical_stage == "A1":
        result["a1"] = {
            "admission_path": _first(row, "admission_path", "selection_route", "candidate_origin", "route"),
            "research_level": _first(row, "research_level", "research_depth", "evidence_level") or "UNSPECIFIED",
            "permission_meaning": _A1_POOLS.get(canonical_pool, "RESEARCH_STATE_UNKNOWN"),
            "critical_gaps": [code for code in reasons if any(token in code.upper() for token in _GAP_TOKENS)],
            "known_negative_facts": list(dict.fromkeys([
                *_strings(_first(row, "risk_reasons", "risk_flags", "known_negatives") or []),
                *[code for code in reasons if any(token in code.upper() for token in _NEGATIVE_TOKENS)],
            ])),
            "evidence_as_of": _first(row, "evidence_as_of", "as_of", "observation_date", "publish_time"),
        }
    elif canonical_stage == "A2":
        relative = _first(row, "relative_strength_score", "relativeStrengthScore", "relative_strength", "relativeStrength")
        result["a2"] = {
            "theme_strength": _number(_first(row, "theme_score", "themeScore")),
            "stock_relative_strength": _number(relative) if not isinstance(relative, Mapping) else dict(relative),
            "market_role": _first(row, "market_role", "marketRole", "leader_role", "leaderRole"),
            "research_path": _first(row, "a2_route", "a2Route", "selection_route", "selectionRoute", "route"),
            "individual_total_score": score,
        }
    elif canonical_stage == "A3":
        result["a3"] = _a3_projection(row, canonical_pool, now)
    return result


def attach_research_presentations(
    output: Mapping[str, Any], *, stage: str, now: datetime | None = None,
) -> dict[str, Any]:
    pools = {
        "A1": {"active_research_pool": "approved", "monitor_pool": "watch", "rejected_candidates": "rejected"},
        "A2": {"focus_pool": "approved", "watch_only_pool": "watch", "rejected_candidates": "rejected"},
        "A3": {"core_watch_pool": "approved", "secondary_watch_pool": "watch", "rejected_candidates": "rejected"},
    }.get(str(stage).upper(), {})
    projected = dict(output)
    for key, pool in pools.items():
        rows = output.get(key)
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
            continue
        projected[key] = [
            {
                **dict(row),
                "presentation": project_research_presentation(row, stage=stage, pool=pool, now=now),
            }
            if isinstance(row, Mapping) else row
            for row in rows
        ]
    return projected


def project_unified_status(
    *, job_status: str, data_state: str, opportunity_state: str,
    actionability_state: str, critical_data: bool, coverage: float | None,
) -> dict[str, Any]:
    job = str(job_status).upper()
    data = str(data_state).upper()
    opportunity = str(opportunity_state).upper()
    actionability = str(actionability_state).upper()
    if job in {"FAILED", "TIMED_OUT", "INTERRUPTED", "CANCELLED"}:
        overall = "JOB_NOT_COMPLETED"
    elif critical_data and data in {"INSUFFICIENT", "MISSING", "BLOCKED", "UNKNOWN"}:
        overall = "BLOCKED_DATA"
    elif data in {"PARTIAL", "DEGRADED"}:
        overall = "SUCCEEDED_DEGRADED_NO_OPPORTUNITY" if opportunity == "ABSENT" else "SUCCEEDED_DEGRADED"
    elif opportunity == "ABSENT":
        overall = "SUCCEEDED_NO_OPPORTUNITY"
    elif actionability == "ACTIONABLE":
        overall = "ACTIONABLE"
    else:
        overall = "SUCCEEDED"
    return {
        "job_status": job,
        "data_state": data,
        "opportunity_state": opportunity,
        "actionability_state": actionability,
        "critical_data": bool(critical_data),
        "coverage": coverage if isinstance(coverage, (int, float)) and not isinstance(coverage, bool) else None,
        "overall_state": overall,
    }


def project_position_presentation(
    *, data_health: Mapping[str, Any], sellable_qty: int | float | None,
    exit_signal_pending: bool,
) -> dict[str, Any]:
    status = str(data_health.get("status") or "UNKNOWN").upper()
    trusted = data_health.get("current_price_trusted") is True
    if status == "READY" and trusted:
        verification = "VERIFIED"
    elif status == "HARD_STOP_ONLY" and trusted:
        verification = "CURRENT_QUOTE_TRUSTED_HISTORY_INSUFFICIENT"
    elif status in {"STALE", "QUOTE_STALE"}:
        verification = "QUOTE_STALE"
    else:
        verification = "QUOTE_UNVERIFIED"
    sellable = _number(sellable_qty)
    return {
        "verification_state": verification,
        "sell_state": (
            "SELLABILITY_UNKNOWN" if sellable is None
            else "T1_NOT_SELLABLE" if sellable <= 0
            else "SELLABLE"
        ),
        "exit_state": "EXIT_TRIGGERED_NOT_FILLED" if exit_signal_pending else "NO_PENDING_EXIT",
    }


__all__ = [
    "SCHEMA_VERSION",
    "attach_research_presentations",
    "project_position_presentation",
    "project_research_presentation",
    "project_unified_status",
]
