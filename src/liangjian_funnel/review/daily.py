"""Twice-daily, read-only review of the A2 -> A3 -> A4 decision chain."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from ..pipeline.model_client import ModelCallResult, ModelClientError, OpenAICompatibleModelClient
from ..pipeline.prompts import PromptRepository
from ..reporting import atomic_write_json, atomic_write_text
from ..runtime.state import RuntimeStore
from .signal_audit import build_signal_stock_reviews
from .context import A5ReviewError, render_a5_prompt
from .plan_scope import carryover_evidence, select_review_plans
from .fact_guard import normalize_quality, reconcile_report, business_metrics, verification_totals


SHANGHAI = ZoneInfo("Asia/Shanghai")
_AUDIT_SAFE = re.compile(r"^[A-Za-z0-9_.-]{1,180}$")
_A5_PROMPT = "agent_5_daily_reviewer_v1.txt"


class A5ReviewKind(StrEnum):
    MIDDAY = "MIDDAY"
    POST_CLOSE = "POST_CLOSE"


class A5LayerReview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    verdict: str = Field(pattern=r"^(HEALTHY|NEEDS_ATTENTION|DATA_LIMITED|NOT_APPLICABLE)$")
    summary: str = Field(min_length=1, max_length=600)
    strengths: list[str] = Field(default_factory=list, max_length=5)
    defects: list[str] = Field(default_factory=list, max_length=5)
    evidence_ids: list[str] = Field(default_factory=list, max_length=20)
    data_limitations: list[str] = Field(default_factory=list, max_length=8)


class A5SignalReview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=16)
    name: str = Field(default="", max_length=80)
    strategy_profile: str = Field(default="UNKNOWN", max_length=48)
    lifecycle_status: str = Field(default="UNKNOWN", max_length=48)
    assessment: str = Field(min_length=1, max_length=400)
    evidence_ids: list[str] = Field(default_factory=list, max_length=12)
    attribution: str = Field(
        default="UNCLASSIFIED",
        pattern=r"^(GOOD_EXECUTION|SELECTION_ERROR|PLAN_ERROR|CONFIRM_ERROR|DATA_ERROR|MARKET_REVERSAL|DATA_LIMITED|NOT_AN_ERROR|UNCLASSIFIED)$",
    )


class A5CounterexampleReview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=16)
    name: str = Field(default="", max_length=80)
    theme: str = Field(default="", max_length=120)
    observed_performance: str = Field(min_length=1, max_length=300)
    funnel_drop_stage: str = Field(pattern=r"^(A1|A2|A3|A4|UNRESOLVED)$")
    assessment: str = Field(min_length=1, max_length=600)
    evidence_ids: list[str] = Field(min_length=1, max_length=12)
    is_confirmed_defect: bool = False


class A5Defect(BaseModel):
    model_config = ConfigDict(extra="forbid")

    layer: str = Field(pattern=r"^(A2|A3|A4|ORCHESTRATOR)$")
    severity: str = Field(pattern=r"^(HIGH|MEDIUM|LOW)$")
    problem: str = Field(min_length=1, max_length=500)
    evidence_ids: list[str] = Field(default_factory=list, max_length=20)
    confidence: str = Field(pattern=r"^(HIGH|MEDIUM|LOW)$")
    blocked_by_data: bool = False


class A5Proposal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    proposal_id: str = Field(min_length=1, max_length=80)
    type: str = Field(pattern=r"^(ENGINEERING_FIX|DATA_FIX|SHADOW_TEST)$")
    target: str = Field(pattern=r"^(A2|A3|A4|ORCHESTRATOR)$")
    hypothesis: str = Field(min_length=1, max_length=500)
    evidence_ids: list[str] = Field(default_factory=list, max_length=20)
    proposed_change: str = Field(min_length=1, max_length=800)
    validation_method: str = Field(min_length=1, max_length=800)
    success_criteria: str = Field(min_length=1, max_length=500)
    falsification_criteria: str = Field(min_length=1, max_length=500)
    min_shadow_days: int = Field(ge=0, le=120)
    risk: str = Field(min_length=1, max_length=500)
    automatic_production_change: bool = False

    @field_validator("min_shadow_days")
    @classmethod
    def shadow_test_needs_observations(cls, value, info):
        if info.data.get("type") == "SHADOW_TEST" and value < 1:
            raise ValueError("Shadow strategy tests require observation days")
        return value

    @field_validator("automatic_production_change")
    @classmethod
    def production_change_forbidden(cls, value: bool) -> bool:
        if value:
            raise ValueError("A5 may not change production automatically")
        return value


class A5UnresolvedQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1, max_length=500)
    reason: str = Field(pattern=r"^(INSUFFICIENT_SAMPLE|MISSING_DATA|CONFOUNDED|REGIME_NOT_OBSERVED)$")
    resolution: str = Field(min_length=1, max_length=500)


class A5ReviewReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(pattern=r"^a5-daily-review/1\.0\.0$")
    review_kind: A5ReviewKind
    trade_date: date
    overall_verdict: str = Field(pattern=r"^(HEALTHY|NEEDS_ATTENTION|DATA_LIMITED|INCIDENT)$")
    executive_summary: str = Field(min_length=1, max_length=1200)
    sample_sufficient_for_strategy_change: bool = False
    a2_review: A5LayerReview
    a3_review: A5LayerReview
    a4_review: A5LayerReview
    signal_reviews: list[A5SignalReview] = Field(default_factory=list, max_length=80)
    # Populated by server facts after model validation, never model authority.
    signal_stock_reviews: list[dict[str, Any]] = Field(default_factory=list)
    fact_reconciliation: list[str] = Field(default_factory=list)
    # The full-market top percentile plus stage-stratified supplements must fit
    # without forcing the model or server reconciliation to drop identities.
    missed_opportunity_reviews: list[A5CounterexampleReview] = Field(default_factory=list, max_length=80)
    core_defects: list[A5Defect] = Field(default_factory=list, max_length=8)
    improvement_proposals: list[A5Proposal] = Field(default_factory=list, max_length=3)
    data_collection_tasks: list[str] = Field(default_factory=list, max_length=8)
    unresolved_questions: list[A5UnresolvedQuestion] = Field(min_length=1, max_length=10)

    @model_validator(mode="after")
    def daily_report_never_authorizes_strategy_change(self) -> "A5ReviewReport":
        if self.sample_sufficient_for_strategy_change:
            raise ValueError("one daily review cannot authorize a production strategy change")
        return self


def _canonicalize_report_output(value: Any, *, allowed_evidence: set[str] | None = None) -> Any:
    """Collapse known detailed drop reasons into the report's layer contract.

    Independent evidence deliberately keeps granular reason codes such as
    ``A2_NOT_FOCUSED``.  The A5 report field represents only the owning funnel
    layer.  Models may copy the evidence code verbatim, so normalize recognized
    layer-prefixed codes while leaving every unrelated value for strict schema
    validation to reject.
    """

    if not isinstance(value, Mapping):
        return value
    payload = dict(value)
    # A projected job incident uses the SHA prefix of its archived raw rows.
    # The model occasionally copies that exact digest under the raw JOB type.
    # Correct only this unambiguous type typo; every other unknown citation
    # still fails the evidence guard. Never alter the archived model response.
    if allowed_evidence is not None:
        def normalize_refs(row: Any) -> Any:
            if not isinstance(row, Mapping) or not isinstance(row.get("evidence_ids"), list):
                return row
            result = dict(row)
            refs = []
            for ref in row["evidence_ids"]:
                match = re.fullmatch(r"ENGINEERING:JOB:([a-f0-9]{16})", ref) if isinstance(ref, str) else None
                incident = f"ENGINEERING:JOB_INCIDENT:{match.group(1)}" if match else None
                refs.append(incident if ref not in allowed_evidence and incident in allowed_evidence else ref)
            result["evidence_ids"] = refs
            return result

        for key in ("a2_review", "a3_review", "a4_review"):
            payload[key] = normalize_refs(payload.get(key))
        for key in ("signal_reviews", "missed_opportunity_reviews", "core_defects", "improvement_proposals"):
            if isinstance(payload.get(key), list):
                payload[key] = [normalize_refs(row) for row in payload[key]]
    tasks = payload.get("data_collection_tasks")
    if isinstance(tasks, list):
        priorities = {"HIGH": "高", "MEDIUM": "中", "LOW": "低"}
        normalized_tasks = []
        for task in tasks:
            # Only normalize the observed, losslessly representable shape.
            # Unknown keys and invalid values still fail strict validation.
            if (isinstance(task, Mapping) and set(task) <= {"task", "target", "priority", "reason", "evidence_ids"}
                    and isinstance(task.get("task"), str) and task["task"].strip()
                    and ("target" not in task or isinstance(task["target"], str) and task["target"] in {"A1", "A2", "A3", "A4", "A5", "ORCHESTRATOR"})
                    and ("reason" not in task or isinstance(task["reason"], str) and task["reason"].strip())
                    and ("evidence_ids" not in task or isinstance(task["evidence_ids"], list)
                         and allowed_evidence is not None
                         and all(isinstance(ref, str) and ref in allowed_evidence for ref in task["evidence_ids"]))
                    and ("priority" not in task or isinstance(task["priority"], str) and task["priority"] in priorities)):
                target = task.get("target", "")
                if target == "ORCHESTRATOR":
                    target = "任务编排"
                tags = [target] if target else []
                if "priority" in task:
                    tags.append("优先级：" + priorities[task["priority"]])
                text = ("【" + "；".join(tags) + "】" if tags else "") + task["task"]
                if "reason" in task:
                    text += "；说明：" + task["reason"]
                if task.get("evidence_ids"):
                    text += "；依据：" + "、".join(task["evidence_ids"])
                normalized_tasks.append(text)
            else:
                normalized_tasks.append(task)
        payload["data_collection_tasks"] = normalized_tasks
    rows = payload.get("missed_opportunity_reviews")
    if not isinstance(rows, list):
        return payload
    normalized: list[Any] = []
    for item in rows:
        if not isinstance(item, Mapping):
            normalized.append(item)
            continue
        row = dict(item)
        stage = str(row.get("funnel_drop_stage") or "").strip().upper()
        if stage not in {"A1", "A2", "A3", "A4", "UNRESOLVED"}:
            for layer in ("A1", "A2", "A3", "A4"):
                if stage.startswith(f"{layer}_"):
                    row["funnel_drop_stage"] = layer
                    break
        normalized.append(row)
    payload["missed_opportunity_reviews"] = normalized
    return payload


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _rows(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else []


def _canonical_hash(value: Mapping[str, Any]) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _within_cutoff(value: Any, trade_date: date, cutoff: datetime) -> bool:
    try:
        stamp = datetime.fromisoformat(str(value))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            stamp = stamp.replace(tzinfo=SHANGHAI)
        stamp = stamp.astimezone(SHANGHAI)
    except (TypeError, ValueError):
        return False
    return stamp.date() == trade_date and stamp <= cutoff


def _plan_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_mapping(row.get("payload_json"))
    reasons = payload.get("selection_reasons") if isinstance(payload.get("selection_reasons"), list) else []
    return {
        "evidence_id": f"A3:PLAN:{row.get('plan_id')}",
        "plan_id": row.get("plan_id"),
        "source_run_id": payload.get("source_run_id"),
        "symbol": row.get("symbol"),
        "name": payload.get("name"),
        "status": row.get("status"),
        "stock_behavior_type": payload.get("stock_behavior_type"),
        "strategy_profile": payload.get("strategy_profile"),
        "plan_priority": payload.get("plan_priority"),
        "setup_type": payload.get("setup_type"),
        "daily_macd": payload.get("daily_macd") or {},
        "trigger_low": payload.get("trigger_low"),
        "trigger_high": payload.get("trigger_high"),
        "stop_level": payload.get("stop_level"),
        "entry_reference_zone": payload.get("entry_reference_zone"),
        "first_resistance": payload.get("first_resistance"),
        "a4_deferred_conditions": payload.get("a4_deferred_conditions") or [],
        "minimum_reward_risk": _json_mapping(payload.get("deterministic_price_evidence")).get(
            "minimum_reward_risk", payload.get("minimum_reward_risk")),
        "maximum_stop_distance_pct": _json_mapping(payload.get("deterministic_price_evidence")).get(
            "maximum_stop_distance_pct", payload.get("maximum_stop_distance_pct")),
        "no_chase_price": payload.get("no_chase_price", payload.get("max_chase_price")),
        "selection_reasons": [str(item)[:500] for item in reasons[:8]],
        "valid_from": row.get("valid_from"),
        "expires_at": row.get("expires_at"),
        "updated_at": row.get("updated_at"),
    }


def _audit_output(output_dir: Path, run_id: str, lane_id: str) -> tuple[dict[str, Any], list[str]]:
    if not _AUDIT_SAFE.fullmatch(run_id) or not _AUDIT_SAFE.fullmatch(lane_id):
        return {}, ["A2_AUDIT_ID_INVALID"]
    path = output_dir / "research" / f"research_{run_id}_{lane_id}.json"
    try:
        if not path.is_file():
            return {}, ["A2_AUDIT_NOT_FOUND"]
        if path.stat().st_size > 64 * 1024 * 1024:
            from .audit_reader import read_audit_stage
            stages = [read_audit_stage(path, name) for name in ("A1", "A2", "A3")]
            return {"stages": [stage for stage in stages if stage]}, []
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}, ["A2_AUDIT_UNREADABLE"]
    return (dict(value), []) if isinstance(value, Mapping) else ({}, ["A2_AUDIT_INVALID"])


def _a2_projection(audit: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    from .selection_evidence import a2_gate_evidence
    stages = _rows(audit.get("stages"))
    stage = next((item for item in stages if str(item.get("stage")).upper() == "A2"), None)
    if stage is None:
        return {"status": "NOT_AVAILABLE", "candidates": [], "themes": []}, ["A2_STAGE_NOT_FOUND"]
    output = _json_mapping(stage.get("output"))
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for pool_name, values in (
        ("FOCUS", output.get("focus_pool")),
        ("WATCH", output.get("watch_only_pool")),
        ("REJECTED", output.get("rejected_candidates")),
        ("OUTSIDE_ROTATION", output.get("outside_rotation_pool")),
        ("CROWDED", output.get("crowded_pool")),
        ("LOW_IDENTITY", output.get("low_identity_pool")),
    ):
        for item in _rows(values):
            symbol = str(item.get("symbol") or "")
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            reasons = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
            candidates.append({
                "evidence_id": f"A2:{pool_name}:{symbol}",
                "pool": pool_name,
                "symbol": symbol,
                "name": item.get("name") or item.get("company_name"),
                "theme_id": item.get("theme_id"),
                "theme_name": item.get("theme_name"),
                "market_role": item.get("market_role"),
                "score": item.get("identifiability_score", item.get("score")),
                "selection_reasons": item.get("selection_reasons") or reasons,
                "reason_codes": reasons,
                "quant_status": item.get("local_eligibility_status") or item.get("local_screen_status") or item.get("status"),
                "behavior_type": item.get("stock_behavior_type") or item.get("behavior_type"),
                "llm_reviewed": item.get("sent_to_llm", pool_name in {"FOCUS", "WATCH", "REJECTED"}),
                "risk_reasons": item.get("risk_reasons") if isinstance(item.get("risk_reasons"), list) else [],
                "quant_gate_evidence": a2_gate_evidence(item),
                **({"research_observation_scope": item.get("research_observation_scope"),
                    "execution_permission": item.get("execution_permission")}
                   if item.get("strong_trend_observation") else {}),
                **({"research_route_qualifications": item.get("research_route_qualifications")}
                   if item.get("independent_strategy_review") else {}),
                **({"rotation_reserve_scope": item.get("rotation_reserve_scope"),
                    "rotation_reserve_boards": item.get("rotation_reserve_boards", [])}
                   if item.get("rotation_reserve_eligible") else {}),
            })
    themes = []
    for item in _rows(output.get("active_themes"))[:30]:
        theme_id = str(item.get("theme_id") or item.get("theme_name") or "UNKNOWN")
        themes.append({
            "evidence_id": f"A2:THEME:{theme_id}",
            "theme_id": item.get("theme_id"),
            "theme_name": item.get("theme_name") or item.get("display_name"),
            "theme_score": item.get("theme_score"),
            "stage": item.get("stage"),
            "weekly_state": item.get("weekly_momentum_state"),
            "new_entry_policy": item.get("new_entry_policy"),
            "chase_risk_level": item.get("chase_risk_level"),
            "score_breakdown": item.get("score_breakdown") if isinstance(item.get("score_breakdown"), Mapping) else {},
        })
    counts = {name: sum(item["pool"] == name for item in candidates) for name in (
        "FOCUS", "WATCH", "REJECTED", "OUTSIDE_ROTATION", "CROWDED", "LOW_IDENTITY",
    )}
    summary = _json_mapping(output.get("local_screen_summary"))
    evaluated_count = summary.get("evaluated_count")
    lineage_complete = evaluated_count is None or evaluated_count == len(candidates)
    return {
        "status": stage.get("status"),
        "reason_codes": stage.get("reason_codes") if isinstance(stage.get("reason_codes"), list) else [],
        "counts": counts,
        "quant_evaluated_count": evaluated_count,
        "llm_reviewed_count": summary.get("sent_to_llm_count"),
        "lineage_complete": lineage_complete,
        "themes": themes,
        "candidates": candidates,
    }, (["A2_LINEAGE_COUNT_MISMATCH"] if not lineage_complete else []) if output else ["A2_OUTPUT_MISSING"]


def _a3_candidates(audit: Mapping[str, Any]) -> list[dict[str, Any]]:
    stage = next((s for s in _rows(audit.get("stages")) if s.get("stage") == "A3"), {})
    output = _json_mapping(stage.get("output"))
    result: dict[str, dict[str, Any]] = {}
    for pool in ("core_watch_pool", "secondary_watch_pool", "core_targets", "secondary_watchlist", "rejected_candidates"):
        for row in _rows(output.get(pool)):
            symbol = str(row.get("symbol") or "")
            if symbol:
                result[symbol] = {"evidence_id": f"A3:CANDIDATE:{symbol}", "symbol": symbol,
                    "pool": pool, "eligibility": row.get("deterministic_eligibility") or row.get("eligibility"),
                    "strategy_profile": row.get("strategy_profile") or row.get("deterministic_strategy_profile"),
                    "decision_as_of": row.get("decision_as_of"),
                    "unmet_conditions": row.get("deterministic_unmet_conditions") or row.get("unmet_conditions") or [],
                    "technical_evidence": row.get("deterministic_technical_evidence") or {},
                    "reference_price": row.get("reference_price"),
                    "reference_price_as_of": row.get("reference_price_as_of"),
                    "stock_behavior_type": row.get("stock_behavior_type"),
                    "research_state": row.get("research_state"),
                    "execution_permission": row.get("execution_permission"),
                    "strategy_checks": {profile: {key: check.get(key) for key in
                        ("eligibility", "unmet_conditions", "veto_conditions", "reason_codes")}
                        for profile, check in _json_mapping(row.get("strategy_checks")).items() if isinstance(check, Mapping)},
                    "a4_deferred_conditions": row.get("a4_deferred_conditions") or [],
                    "reason_codes": row.get("deterministic_reason_codes") or row.get("reason_codes") or [],
                    "veto_conditions": row.get("deterministic_veto_conditions") or row.get("veto_conditions") or []}
    return list(result.values())


def _compact_a4_observations(events: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep every consequential event and aggregate repeated observations.

    The immutable fact snapshot remains the event ledger.  The model receives
    every effective event verbatim, while non-effective rows are grouped by
    stock and primary reason with exact counts, time bounds and a deterministic
    hash of the original evidence identities.  This is a projection only: it
    never rewrites the archived A4 decisions.
    """

    groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    selected: list[dict[str, Any]] = []
    for event in events:
        if event.get("effective"):
            selected.append(dict(event))
        else:
            key = (
                str(event.get("symbol") or "UNKNOWN"),
                str(event.get("reason_code") or "UNKNOWN"),
            )
            groups.setdefault(key, []).append(event)
    summaries: list[dict[str, Any]] = []
    for key, rows in sorted(groups.items()):
        ordered = sorted(rows, key=lambda row: (str(row.get("minute_end")), str(row.get("event_id"))))
        action_counts: dict[str, int] = {}
        strategy_reason_counts: dict[str, int] = {}
        plan_ids: set[str] = set()
        evidence_index: list[dict[str, Any]] = []
        for row in ordered:
            action = str(row.get("action") or "UNKNOWN")
            action_counts[action] = action_counts.get(action, 0) + 1
            if row.get("plan_id"):
                plan_ids.add(str(row["plan_id"]))
            for reason in set(row.get("strategy_reason_codes") or []):
                code = str(reason)
                strategy_reason_counts[code] = strategy_reason_counts.get(code, 0) + 1
            evidence_index.append({
                "evidence_id": row.get("evidence_id"),
                "event_id": row.get("event_id"),
                "minute_end": row.get("minute_end"),
                "plan_id": row.get("plan_id"),
                "action": row.get("action"),
                "reason_code": row.get("reason_code"),
            })
        summaries.append({
            "evidence_id": "A4:OBSERVATION_GROUP:" + _canonical_hash(evidence_index)[:16],
            "symbol": key[0],
            "reason_code": key[1],
            "category": "ANOMALY" if "DATA_BLOCK" in action_counts else "WAITING",
            "observation_count": len(ordered),
            "first_at": ordered[0].get("minute_end"),
            "last_at": ordered[-1].get("minute_end"),
            "action_counts": action_counts,
            "plan_ids": sorted(plan_ids),
            "strategy_reason_counts": strategy_reason_counts,
            "raw_evidence_sha256": _canonical_hash(evidence_index),
            "archive_locator": "a4.events",
        })
    return sorted(selected, key=lambda row: (str(row.get("minute_end")), str(row.get("event_id")))), summaries


def _count_values(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(field) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _dictionary_encode(value: Any, dictionary: list[str], index: dict[str, int]) -> Any:
    if isinstance(value, str):
        if value not in index:
            index[value] = len(dictionary)
            dictionary.append(value)
        return index[value]
    if isinstance(value, list):
        return [_dictionary_encode(item, dictionary, index) for item in value]
    return value


def _compact_a2_candidates_for_model(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Keep FOCUS/WATCH identities and summarize the unchanged A1 remainder."""

    detailed = [dict(row) for row in rows if str(row.get("pool") or "").upper() in {"FOCUS", "WATCH"}]
    outside = [dict(row) for row in rows if str(row.get("pool") or "").upper() not in {"FOCUS", "WATCH"}]
    detailed_columns = (
        "symbol", "name", "pool", "theme_id", "theme_name", "market_role",
        "behavior_type", "score", "quant_status", "llm_reviewed",
        "selection_reasons", "risk_reasons", "reason_codes",
    )
    detailed_rows = [
        [row.get(column) for column in detailed_columns]
        for row in sorted(detailed, key=lambda item: str(item.get("symbol") or ""))
    ]
    detailed_dictionary: list[str] = []
    detailed_index: dict[str, int] = {}
    encoded_columns = set(range(2, len(detailed_columns))) - {7, 9}
    for row in detailed_rows:
        for offset in encoded_columns:
            row[offset] = _dictionary_encode(row[offset], detailed_dictionary, detailed_index)
    outside_index = [
        {
            "evidence_id": row.get("evidence_id"),
            "symbol": row.get("symbol"),
            "pool": row.get("pool"),
            "theme_id": row.get("theme_id"),
            "quant_status": row.get("quant_status"),
            "selection_reasons": row.get("selection_reasons") or [],
            "risk_reasons": row.get("risk_reasons") or [],
        }
        for row in sorted(outside, key=lambda item: str(item.get("symbol") or ""))
    ]
    reason_counts: dict[str, int] = {}
    theme_counts: dict[str, int] = {}
    for row in outside:
        theme = str(row.get("theme_id") or "UNMAPPED")
        theme_counts[theme] = theme_counts.get(theme, 0) + 1
        reasons = row.get("selection_reasons") or row.get("reason_codes") or ["UNSPECIFIED"]
        for reason in set(str(value) for value in reasons):
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    return {
        "encoding": "a5-a2-candidate-projection/1",
        "decoding": (
            "FOCUS/WATCH groups retain every detailed stock. OUTSIDE_ROTATION and other non-promoted A1 rows "
            "are represented by exact counts plus a hash of their immutable archive index; selected counterexamples "
            "remain detailed in independent_verification.counterexamples."
        ),
        "total_count": len(rows),
        "detailed_count": len(detailed),
        "detailed_candidates": {
            "encoding": "a5-a2-candidate-table/1",
            "columns": list(detailed_columns),
            "rows": detailed_rows,
            "string_dictionary": detailed_dictionary,
            "dictionary_encoded_columns": [detailed_columns[index] for index in sorted(encoded_columns)],
            "evidence_id_rule": "A2:{pool}:{symbol}",
            "raw_evidence_sha256": _canonical_hash(detailed),
            "archive_locator": "a2.candidates",
        },
        "archived_remainder": {
            "count": len(outside),
            "pool_counts": _count_values(outside, "pool"),
            "quant_status_counts": _count_values(outside, "quant_status"),
            "theme_counts": dict(sorted(theme_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
            "raw_evidence_sha256": _canonical_hash(outside_index),
            "archive_locator": "a2.candidates",
        },
    }


def _compact_a3_candidates_for_model(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Encode every candidate once without repeating field names or timestamps."""

    columns = (
        "symbol", "pool", "eligibility", "strategy_profile", "research_state",
        "execution_permission", "stock_behavior_type", "reference_price",
        "reason_codes", "unmet_conditions", "veto_conditions", "a4_deferred_conditions",
        "daily_state", "distribution", "overextended", "one_price_locked", "theme_stage",
    )
    records: list[list[Any]] = []
    for row in sorted(rows, key=lambda item: str(item.get("symbol") or "")):
        technical = _json_mapping(row.get("technical_evidence"))
        values = {
            **dict(row),
            "daily_state": technical.get("daily_state"),
            "distribution": technical.get("distribution"),
            "overextended": technical.get("overextended"),
            "one_price_locked": technical.get("one_price_locked"),
            "theme_stage": technical.get("theme_stage"),
        }
        records.append([values.get(column) for column in columns])
    dictionary: list[str] = []
    dictionary_index: dict[str, int] = {}
    encoded_columns = set(range(1, len(columns))) - {7, 14, 15, 16}
    for record in records:
        for offset in encoded_columns:
            record[offset] = _dictionary_encode(record[offset], dictionary, dictionary_index)
    return {
        "encoding": "a5-a3-candidate-table/1",
        "row_count": len(rows),
        "columns": list(columns),
        "rows": records,
        "string_dictionary": dictionary,
        "dictionary_encoded_columns": [columns[index] for index in sorted(encoded_columns)],
        "evidence_id_rule": "A3:CANDIDATE:{symbol}",
        "decision_as_of": sorted({str(row.get("decision_as_of") or "") for row in rows}),
        "reference_price_as_of": sorted({str(row.get("reference_price_as_of") or "") for row in rows}),
        "raw_evidence_sha256": _canonical_hash(rows),
        "archive_locator": "a3.candidates",
    }


def _compact_a3_plans_for_model(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Keep every executable price/risk fact while deduplicating plan prefixes."""

    source_ids = sorted({str(row.get("source_run_id") or "") for row in rows})
    shared_source = source_ids[0] if len(source_ids) == 1 else ""
    common_fields = (
        "expires_at", "maximum_stop_distance_pct", "minimum_reward_risk",
        "status", "updated_at",
    )
    common_values = {
        field: sorted({json.dumps(row.get(field), ensure_ascii=False, sort_keys=True, default=str) for row in rows})
        for field in common_fields
    }
    common = {
        field: json.loads(values[0])
        for field, values in common_values.items()
        if len(values) == 1
    }
    columns = (
        "plan_id", "symbol", "name", "strategy_profile", "setup_type",
        "plan_priority", "stock_behavior_type", "valid_from", "trigger_low",
        "trigger_high", "stop_level", "no_chase_price", "first_resistance",
        "daily_macd", "selection_reasons", "a4_deferred_conditions",
    )
    records: list[list[Any]] = []
    for row in sorted(rows, key=lambda item: str(item.get("symbol") or "")):
        values = dict(row)
        plan_id = str(row.get("plan_id") or "")
        if shared_source and plan_id.startswith(shared_source + ":"):
            values["plan_id"] = plan_id[len(shared_source) + 1 :]
        records.append([values.get(column) for column in columns])
    dictionary: list[str] = []
    dictionary_index: dict[str, int] = {}
    encoded_columns = {3, 4, 5, 6, 7, 14, 15}
    for record in records:
        for offset in encoded_columns:
            record[offset] = _dictionary_encode(record[offset], dictionary, dictionary_index)
    return {
        "encoding": "a5-a3-plan-table/1",
        "row_count": len(rows),
        "source_run_id": shared_source or source_ids,
        "plan_id_rule": (
            "source_run_id + ':' + stored plan_id; evidence_id is 'A3:PLAN:' + full plan_id"
            if shared_source else "stored plan_id is complete; evidence_id is 'A3:PLAN:' + plan_id"
        ),
        "common": common,
        "columns": list(columns),
        "rows": records,
        "string_dictionary": dictionary,
        "dictionary_encoded_columns": [columns[index] for index in sorted(encoded_columns)],
        "raw_evidence_sha256": _canonical_hash(rows),
        "archive_locator": "a3.plans",
    }


def _compact_a4_observation_groups_for_model(groups: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Table-encode exact waiting/anomaly groups and deduplicate long plan ids."""

    plan_index: dict[str, list[str]] = {}
    columns = (
        "evidence_id", "symbol", "reason_code", "category", "observation_count",
        "first_at", "last_at", "action",
        "raw_evidence_sha256",
    )
    rows: list[list[Any]] = []
    strategy_reason_totals: dict[str, int] = {}
    for group in groups:
        symbol = str(group.get("symbol") or "UNKNOWN")
        plan_index.setdefault(symbol, [])
        plan_index[symbol].extend(str(value) for value in group.get("plan_ids") or [])
        action_counts = _json_mapping(group.get("action_counts"))
        action = next(iter(action_counts)) if len(action_counts) == 1 else action_counts
        compact = dict(group)
        compact["first_at"] = str(group.get("first_at") or "")[11:19]
        compact["last_at"] = str(group.get("last_at") or "")[11:19]
        compact["action"] = action
        rows.append([compact.get(column) for column in columns])
        for reason, count in _json_mapping(group.get("strategy_reason_counts")).items():
            strategy_reason_totals[str(reason)] = strategy_reason_totals.get(str(reason), 0) + int(count or 0)
    dictionary: list[str] = []
    dictionary_index: dict[str, int] = {}
    encoded_columns = {"symbol", "reason_code", "category", "action"}
    for row in rows:
        for offset, column in enumerate(columns):
            if column in encoded_columns:
                row[offset] = _dictionary_encode(row[offset], dictionary, dictionary_index)
    return {
        "encoding": "a5-a4-observation-group-table/1",
        "grouping_key": "symbol+reason_code",
        "group_count": len(groups),
        "observation_count": sum(int(group.get("observation_count") or 0) for group in groups),
        "columns": list(columns),
        "rows": rows,
        "string_dictionary": dictionary,
        "dictionary_encoded_columns": sorted(encoded_columns),
        "strategy_reason_totals": dict(sorted(strategy_reason_totals.items())),
        "time_encoding": "HH:MM:SS on the top-level trade_date",
        "action_encoding": "A string means every observation in the group has that action; otherwise exact counts are provided.",
        "symbol_plan_index": {
            symbol: sorted(set(values)) for symbol, values in sorted(plan_index.items())
        },
        "archive_locator": "a4.events",
    }


def _group_records(
    rows: Sequence[Mapping[str, Any]],
    *,
    identity_fields: set[str],
    evidence_prefix: str | None = None,
) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        common = {key: value for key, value in row.items() if key not in identity_fields}
        key = json.dumps(common, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
        group = groups.setdefault(key, {"common": common, "records": []})
        identity = {field: row.get(field) for field in identity_fields if field in row}
        if evidence_prefix and row.get("evidence_id") == evidence_prefix + str(row.get("symbol") or ""):
            identity.pop("evidence_id", None)
            group["derive_evidence_id"] = True
        group["records"].append(identity)
    return {
        "encoding": "a5-grouped-records/1",
        "row_count": len(rows),
        "groups": list(groups.values()),
    }


def _compact_plan_scope_for_model(value: Mapping[str, Any]) -> dict[str, Any]:
    result = {key: value[key] for key in ("evidence_id", "session_plan_count", "retired_before_session_count") if key in value}
    retired = _rows(value.get("retired_before_session"))
    groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in retired:
        key = (str(row.get("reason_code") or "UNKNOWN"), str(row.get("source_run_id") or "UNKNOWN"))
        groups.setdefault(key, []).append(row)
    result["retired_groups"] = [
        {
            "reason_code": key[0],
            "source_run_id": key[1],
            "count": len(rows),
            "symbols": sorted(str(row.get("symbol") or "") for row in rows),
            "first_retired_at": min((str(row.get("retired_at") or "") for row in rows), default=""),
            "last_retired_at": max((str(row.get("retired_at") or "") for row in rows), default=""),
            "raw_evidence_sha256": _canonical_hash(rows),
        }
        for key, rows in sorted(groups.items())
    ]
    result["archive_locator"] = "a3.plan_scope.retired_before_session"
    return result


def _compact_operational_evidence_for_model(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    job_rows = [dict(row) for row in rows if str(row.get("kind") or "").startswith("JOB_")]
    other_rows = [dict(row) for row in rows if not str(row.get("kind") or "").startswith("JOB_")]
    groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = {}
    for row in job_rows:
        key = (str(row.get("kind") or "UNKNOWN"), str(row.get("job") or "UNKNOWN"), str(row.get("reason") or "UNKNOWN"))
        groups.setdefault(key, []).append(row)
    incidents: list[dict[str, Any]] = []
    for job in sorted({str(row.get("job") or "UNKNOWN") for row in job_rows}):
        ordered = sorted(
            (row for row in job_rows if str(row.get("job") or "UNKNOWN") == job),
            key=lambda row: str(row.get("time") or ""),
        )
        current: list[Mapping[str, Any]] = []
        previous_at: datetime | None = None
        for row in ordered:
            try:
                stamp = datetime.fromisoformat(str(row.get("time") or "").replace("Z", "+00:00"))
            except ValueError:
                stamp = None
            if current and stamp is not None and previous_at is not None and (stamp - previous_at).total_seconds() > 2400:
                incidents.append(_operational_job_incident(job, current))
                current = []
            current.append(row)
            if stamp is not None:
                previous_at = stamp
        if current:
            incidents.append(_operational_job_incident(job, current))
    return {
        "schema_version": "a5-operational-projection/1",
        "original_count": len(rows),
        "job_incident_count": len(incidents),
        "job_incidents": incidents,
        "job_failure_groups": [
            {
                "evidence_id": "ENGINEERING:JOB_GROUP:" + _canonical_hash(items)[:16],
                "kind": key[0], "job": key[1], "reason": key[2],
                "count": len(items),
                "first_at": min(str(item.get("time") or "") for item in items),
                "last_at": max(str(item.get("time") or "") for item in items),
                "raw_evidence_sha256": _canonical_hash(items),
            }
            for key, items in sorted(groups.items())
        ],
        "other_events": other_rows,
        "projected_count": len(job_rows) + len(other_rows),
        "archive_locator": "operational_evidence",
    }


def _operational_job_incident(job: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    kinds: dict[str, int] = {}
    reasons: dict[str, int] = {}
    for row in rows:
        kind = str(row.get("kind") or "UNKNOWN")
        reason = str(row.get("reason") or "UNKNOWN")
        kinds[kind] = kinds.get(kind, 0) + 1
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "evidence_id": "ENGINEERING:JOB_INCIDENT:" + _canonical_hash(rows)[:16],
        "job": job,
        "record_count": len(rows),
        "kind_counts": kinds,
        "reason_counts": reasons,
        "first_at": min(str(item.get("time") or "") for item in rows),
        "last_at": max(str(item.get("time") or "") for item in rows),
        "raw_evidence_ids": [str(item.get("evidence_id") or "") for item in rows],
        "raw_evidence_sha256": _canonical_hash(rows),
        "interpretation": "One bounded job incident containing multiple retry/termination records; record_count is not an independent task count.",
    }


def _compact_counterexamples_for_model(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Retain every counterexample while removing duplicated A3 matrices."""

    result: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        candidate = _json_mapping(row.get("a3_candidate"))
        if candidate:
            row["a3_candidate"] = _compact_a3_candidate_for_model(candidate)
        audit = dict(_json_mapping(row.get("selection_audit")))
        if audit:
            audit_candidate = _json_mapping(audit.get("a3"))
            if candidate and audit_candidate == candidate:
                audit.pop("a3", None)
                audit["a3_reference"] = "a3_candidate"
            row["selection_audit"] = audit
        row["raw_evidence_sha256"] = _canonical_hash(source)
        row["archive_locator"] = (
            f"independent_verification.counterexamples[{len(result)}]"
        )
        result.append(row)
    return result


def _model_fact_projection(facts: Mapping[str, Any]) -> dict[str, Any]:
    projected = dict(facts)
    projected["data_quality"] = normalize_quality(_json_mapping(facts.get("data_quality")))
    projected["operational_evidence"] = _compact_operational_evidence_for_model(
        _rows(facts.get("operational_evidence"))
    )
    # Persisted prose is not independently validated history. Keep identity
    # references, not old numbers/claims that contaminate a new session.
    projected["review_history"] = [{key: row[key] for key in (
        "evidence_id", "review_id", "trade_date", "review_kind", "proposal_ids") if key in row}
        for row in _rows(facts.get("review_history"))]
    post_close = _json_mapping(facts.get("post_close_archive"))
    if post_close:
        projected["post_close_archive"] = _compact_post_close_archive_for_model(post_close)
    # These are exact duplicates of the authoritative A3/root evidence.
    projected["a2"] = dict(_json_mapping(facts.get("a2")))
    projected["a2"].pop("technical_candidates", None)
    projected["a2"]["candidates"] = [
        {key: value for key, value in row.items()
         if key != "quant_gate_evidence" and not (key == "reason_codes" and value == row.get("selection_reasons"))}
        for row in _rows(projected["a2"].get("candidates"))
    ]
    projected["a2"]["reason_encoding"] = "When reason_codes is omitted, it equals selection_reasons exactly; no reasons are truncated."
    projected["a2"]["gate_evidence_scope"] = "Full per-stock quant_gate_evidence remains in the fact archive; selected counterexamples carry it in selection_audit. No candidate identity or disposition is removed."
    projected["a2"]["candidates"] = _compact_a2_candidates_for_model(
        projected["a2"]["candidates"]
    )
    projected["a3"] = dict(_json_mapping(facts.get("a3")))
    compact_candidates = [
        _compact_a3_candidate_for_model(row)
        for row in _rows(projected["a3"].get("candidates"))
    ]
    projected["a3"]["candidates"] = _compact_a3_candidates_for_model(compact_candidates)
    projected["a3"]["plans"] = _compact_a3_plans_for_model(
        _rows(projected["a3"].get("plans"))
    )
    projected["a3"]["plan_scope"] = _compact_plan_scope_for_model(
        _json_mapping(projected["a3"].get("plan_scope"))
    )
    projected["a3"]["candidate_evidence_scope"] = (
        "Every candidate identity, disposition, selected strategy, reason, unmet/veto/deferred condition and "
        "compact risk state is present. Full per-condition matrices, moving-average vectors, path booleans and "
        "per-route checks remain in the immutable fact archive. Published plans and selected counterexamples "
        "retain their detailed evidence."
    )
    independent = dict(_json_mapping(facts.get("independent_verification")))
    independent.pop("signal_market", None)  # minute paths stay in the fact archive
    archives = independent.pop("market_data_evidence_archives", None)
    if isinstance(archives, Mapping):
        # File names, hashes and byte counts belong to the immutable evidence
        # index, not the analyst context. Preserve every failure and coverage
        # count; all business findings below remain untouched.
        summaries = {}
        for source, records in archives.items():
            records = _json_mapping(records)
            failures = [{"symbol": symbol, "status": row.get("status", "NOT_ARCHIVED") if isinstance(row, Mapping) else "INVALID_ARCHIVE_REFERENCE"}
                        for symbol, row in records.items()
                        if not (isinstance(row, Mapping) and row.get("sha256") and row.get("relative_path"))]
            summaries[source] = {"requested_count": len(records),
                                 "archived_count": len(records) - len(failures), "failures": failures}
        independent["market_evidence_archive_summary"] = {
            "scope": "FILE_INDEX_IN_FULL_FACT_ARCHIVE_NOT_MODEL_CONTEXT", "sources": summaries}
    if "independent_verification" in facts:
        projected["independent_verification"] = independent
    if independent:
        independent["counterexamples"] = _compact_counterexamples_for_model(
            _rows(independent.get("counterexamples"))
        )
        independent["a2"] = dict(_json_mapping(independent.get("a2")))
        missing_cross_section = independent["a2"].get("market_cross_section_missing_symbols")
        if isinstance(missing_cross_section, list):
            independent["a2"]["market_cross_section_missing_symbols"] = {
                "count": len(missing_cross_section),
                "sha256": _canonical_hash(missing_cross_section),
                "archive_locator": "independent_verification.a2.market_cross_section_missing_symbols",
            }
        recovery = _rows(independent["a2"].get("market_cross_section_recovery"))
        if recovery:
            # The server already ranks the full cross-section and emits all
            # selected counterexamples below. Do not ask the model to rank
            # 812 raw quotes again. Keep the complete coverage index and all
            # failures; original per-stock quotes remain in frozen facts.
            times = sorted(str(r["verification_fetched_at"]) for r in recovery if r.get("verification_fetched_at"))
            business_rows = [
                {k: v for k, v in row.items() if k not in {"input_digest", "verification_fetched_at"}}
                for row in recovery]
            recovery_groups: dict[str, dict[str, Any]] = {}
            for row in business_rows:
                common = {k: row.get(k) for k in ("source_ids", "return_basis", "price_at", "evidence_scope")}
                key = json.dumps(common, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                group = recovery_groups.setdefault(key, {"basis": common, "symbols": [], "priced_count": 0, "failures": []})
                group["symbols"].append(row.get("symbol"))
                if row.get("return") is not None:
                    group["priced_count"] += 1
                else:
                    group["failures"].append(row)
            independent["a2"]["market_cross_section_recovery"] = {
                "projection": "FULL_COVERAGE_INDEX_SERVER_RANKED_QUOTES_IN_FROZEN_ARCHIVE",
                "row_count": len(recovery),
                "interpretation": "Every recovered symbol is listed. All unavailable quote rows remain. Full per-stock prices and returns are in frozen facts; server-selected counterexamples retain detailed returns. No model re-ranking or claim that this index contains every raw quote.",
                "groups": list(recovery_groups.values())}
            independent["a2"]["market_recovery_provenance"] = {
                "row_count": len(recovery), "individual_provenance_in_full_fact_archive": True,
                "first_fetched_at": times[0] if times else None,
                "last_fetched_at": times[-1] if times else None,
                "scope": "POST_HOC_VERIFICATION_NOT_ORIGINAL_DECISION_INPUT"}
        original_independent = _json_mapping(facts.get("independent_verification"))
        if _rows(_json_mapping(original_independent.get("a2")).get("counterexamples")) == _rows(
            original_independent.get("counterexamples")
        ):
            independent["a2"].pop("counterexamples", None)
        if _rows(_json_mapping(original_independent.get("a2")).get("top_performance_ledger")) == _rows(
            original_independent.get("top_performance_ledger")
        ):
            independent["a2"].pop("top_performance_ledger", None)
        independent["a3"] = _compact_a3_verification_for_model(
            _json_mapping(independent.get("a3"))
        )
        independent["a4"] = _compact_a4_verification_for_model(
            _json_mapping(independent.get("a4"))
        )
        projected["independent_verification"] = independent
    a4 = dict(_json_mapping(facts.get("a4")))
    events, groups = _compact_a4_observations(_rows(a4.get("events")))
    original_count = len(a4.get("events") or [])
    a4.update(events=[_compact_a4_event_for_model(row) for row in events],
              observation_groups=_compact_a4_observation_groups_for_model(groups),
              model_projection={"original_event_count": original_count, "representative_event_count": len(events),
                                "aggregated_event_count": sum(int(row.get("observation_count") or 0) for row in groups),
                                "aggregate_group_count": len(groups),
                                "all_effective_events_retained": True, "full_evidence_archived": True,
                                "grouping_key": "symbol+reason_code",
                                "empty_values_and_derivable_event_ids_archived": True})
    projected["a4"] = a4
    return projected


def _compact_a3_candidate_for_model(row: Mapping[str, Any]) -> dict[str, Any]:
    """Keep every A3 disposition while moving repeated raw matrices to the archive.

    A5 reasons about server-owned outcomes, exceptions and executable plans.  It
    does not need 140 copies of the same successful condition schema.  The
    immutable facts file remains the audit authority for every omitted matrix
    cell, so this projection never changes an A3 decision.
    """

    result = dict(row)
    result.pop("strategy_checks", None)
    technical = _json_mapping(result.get("technical_evidence"))
    if technical:
        result["technical_evidence"] = {
            key: technical[key]
            for key in (
                "daily_close",
                "daily_state",
                "distribution",
                "overextended",
                "one_price_locked",
                "theme_stage",
            )
            if key in technical
        }
    return result


def _compact_a4_event_for_model(row: Mapping[str, Any]) -> dict[str, Any]:
    """Remove transport-only empty cells while retaining every event fact."""

    result = {
        key: value
        for key, value in row.items()
        if value not in (None, "", [], {})
    }
    event_id = str(result.get("event_id") or "")
    if event_id and result.get("evidence_id") == f"A4:EVENT:{event_id}":
        result.pop("event_id", None)
    if result.get("recorded_effective") == result.get("effective"):
        result.pop("recorded_effective", None)
    return result


def _compact_post_close_archive_for_model(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep finalization coverage and failures, not repeated stable hashes."""

    result = {key: value[key] for key in (
        "collected_at", "creates_signals", "status", "reason_code"
    ) if key in value}
    records: list[dict[str, Any]] = []
    for row in _rows(value.get("records")):
        attempts = _rows(row.get("attempts"))
        item = {key: row[key] for key in (
            "symbol", "interval", "status", "provider_official_final"
        ) if key in row}
        item["attempt_count"] = len(attempts)
        reason_counts: dict[str, int] = {}
        for attempt in attempts:
            reason = str(attempt.get("reason") or "UNKNOWN")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        item["attempt_reason_counts"] = reason_counts
        exceptions = [
            dict(attempt)
            for attempt in attempts
            if str(attempt.get("reason") or "") != "OK"
        ]
        if exceptions:
            item["exception_attempts"] = exceptions
        records.append(item)
    result["records"] = records
    result["projection_scope"] = (
        "Every symbol/interval finalization status and attempt reason count is present. Non-OK attempts are exact; "
        "repeated stable hashes/timestamps and the receipt path remain in the immutable fact archive."
    )
    return result


def _verification_field_totals(plans: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    totals: dict[str, dict[str, Any]] = {}
    for plan in plans:
        for side in ("cross_source_field_checks", "archived_tdx_field_checks"):
            checks = plan.get(side)
            if not isinstance(checks, Mapping):
                continue
            for name, raw in checks.items():
                if not isinstance(raw, Mapping):
                    continue
                key = f"{side}:{name}"
                total = totals.setdefault(
                    key,
                    {
                        "statuses": {},
                        "compared_count": 0,
                        "mismatch_count": 0,
                        "not_comparable_count": 0,
                    },
                )
                status = str(raw.get("status") or "UNKNOWN")
                total["statuses"][status] = total["statuses"].get(status, 0) + 1
                for field in ("compared_count", "mismatch_count", "not_comparable_count"):
                    total[field] += int(raw.get(field) or 0)
    return totals


def _field_check_exceptions(value: Any) -> dict[str, Any]:
    checks = value if isinstance(value, Mapping) else {}
    return {
        str(name): dict(raw)
        for name, raw in checks.items()
        if isinstance(raw, Mapping)
        and (
            int(raw.get("mismatch_count") or 0) > 0
            or bool(raw.get("mismatch_samples"))
            or bool(raw.get("difference_patterns"))
        )
    }


def _compact_a4_verification_for_model(value: Mapping[str, Any]) -> dict[str, Any]:
    """Aggregate repeated cross-source checks without hiding exceptions."""

    result = dict(value)
    plans = _rows(result.get("plans"))
    coverage_totals = verification_totals({
        "metrics": {"a3_plan_count": len(plans)},
        "independent_verification": {"a4": {"plans": plans}},
    })
    coverage_totals.pop("fields", None)
    result["field_totals"] = _verification_field_totals(plans)
    formula_totals: dict[str, dict[str, int]] = {}
    raw_formula_verification = {"verified": 0, "not_verified": 0}
    for plan in plans:
        audit = _json_mapping(plan.get("indicator_formula_audit"))
        raw_verified = audit.get("raw_source_independently_verified") is True
        raw_formula_verification["verified" if raw_verified else "not_verified"] += 1
        for indicator, statuses in _json_mapping(audit.get("counts")).items():
            if not isinstance(statuses, Mapping):
                continue
            total = formula_totals.setdefault(str(indicator), {})
            for status, count in statuses.items():
                total[str(status)] = total.get(str(status), 0) + int(count or 0)
    result["indicator_formula_totals"] = formula_totals
    result["indicator_raw_source_verification"] = raw_formula_verification
    exceptions: list[dict[str, Any]] = []
    plan_index: list[dict[str, Any]] = []
    status_totals: dict[str, dict[str, int]] = {}
    keep = (
        "evidence_id",
        "plan_id",
        "symbol",
        "decision_scope",
        "decision_window_end",
        "decision_snapshot",
        "archived_bar_count",
        "tencent_bar_count",
        "tdx_bar_count",
        "expected_observation_minutes",
        "recorded_observation_minutes",
        "missing_observation_count",
        "missing_observation_samples",
        "observation_coverage",
        "orchestration_omission_count",
        "orchestration_omissions",
        "cross_source_overlap_count",
        "cross_source_status",
        "cross_source_max_close_difference",
        "archived_tdx_overlap_count",
        "archived_tdx_status",
        "archived_tdx_max_close_difference",
        "tencent_reason_code",
        "tdx_reason_code",
        "tdx_stop_touched",
        "tdx_trigger_zone_seen",
        "discrepancy_class",
        "effective_actions",
    )
    for plan in plans:
        item = {key: plan[key] for key in keep if key in plan}
        plan_index.append({
            "evidence_id": plan.get("evidence_id"),
            "plan_id": plan.get("plan_id"),
            "symbol": plan.get("symbol"),
        })
        for field in (
            "cross_source_status", "archived_tdx_status", "discrepancy_class",
            "tencent_reason_code", "tdx_reason_code",
        ):
            status = str(plan.get(field) or "UNKNOWN")
            counts = status_totals.setdefault(field, {})
            counts[status] = counts.get(status, 0) + 1
        cross_exceptions = _field_check_exceptions(plan.get("cross_source_field_checks"))
        tdx_exceptions = _field_check_exceptions(plan.get("archived_tdx_field_checks"))
        if cross_exceptions:
            item["cross_source_field_exceptions"] = cross_exceptions
        if tdx_exceptions:
            item["archived_tdx_field_exceptions"] = tdx_exceptions
        formula_audit = _json_mapping(plan.get("indicator_formula_audit"))
        formula_issues = formula_audit.get("issue_samples")
        if formula_issues:
            item["indicator_formula_issue_samples"] = formula_issues
        is_exception = bool(
            cross_exceptions
            or tdx_exceptions
            or formula_issues
            or int(plan.get("missing_observation_count") or 0) > 0
            or int(plan.get("orchestration_omission_count") or 0) > 0
            or plan.get("effective_actions")
            or str(plan.get("discrepancy_class") or "").upper()
               not in {
                   "", "NONE", "MATCH", "NO_DISCREPANCY", "NOT_APPLICABLE",
                   "NO_COMPARABLE_MISMATCH",
               }
        )
        if is_exception:
            exceptions.append(item)
    exception_columns = (
        "evidence_id", "symbol", "expected_observation_minutes",
        "recorded_observation_minutes", "missing_observation_count",
        "missing_observation_samples", "orchestration_omission_count",
        "orchestration_omissions", "cross_source_status",
        "archived_tdx_status", "tencent_reason_code", "tdx_reason_code",
        "discrepancy_class", "effective_actions", "raw_evidence_sha256",
    )
    exception_rows: list[list[Any]] = []
    for item in exceptions:
        compact = dict(item)
        compact["raw_evidence_sha256"] = _canonical_hash(item)
        exception_rows.append([compact.get(column) for column in exception_columns])
    result["plans"] = {
        "encoding": "a5-a4-verification-projection/1",
        "plan_count": len(plans),
        # Preserve both the row count and the incident grouping.  A global
        # missing minute affecting many plans is one outage window, not many
        # independent faults.
        "coverage_totals": coverage_totals,
        "status_totals": status_totals,
        "plan_index_sha256": _canonical_hash(plan_index),
        "plan_symbols": sorted(str(row.get("symbol") or "") for row in plans),
        "exception_count": len(exceptions),
        "exceptions": {
            "encoding": "a5-a4-verification-exception-table/1",
            "columns": list(exception_columns),
            "rows": exception_rows,
        },
        "archive_locator": "independent_verification.a4.plans",
    }
    result["projection_scope"] = (
        "All plan identities, coverage, source statuses, aggregated formula counts and every mismatch/not-comparable "
        "or formula issue sample "
        "are present. Repeated per-field zero rows and file digests remain in the immutable fact archive; "
        "field_totals is the exact server-side aggregation across all plans."
    )
    return result


def _compact_a3_verification_for_model(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep plan-level verification outcomes and expand only exceptions."""

    result = dict(value)
    compact: list[dict[str, Any]] = []
    plan_index: list[dict[str, Any]] = []
    status_totals: dict[str, dict[str, int]] = {}
    keep = (
        "evidence_id",
        "plan_id",
        "symbol",
        "strategy_profile",
        "daily_reference_date",
        "daily_bar_count",
        "formula_status",
        "price_levels_valid",
        "route_contract_match",
        "cross_source_price_status",
        "cross_source_close_relative_difference",
        "cross_source_period_policy",
        "tdx_previous_close",
        "tdx_reason_code",
    )
    for plan in _rows(result.get("plans")):
        item = {key: plan[key] for key in keep if key in plan}
        plan_index.append({
            "evidence_id": plan.get("evidence_id"),
            "plan_id": plan.get("plan_id"),
            "symbol": plan.get("symbol"),
        })
        for field in (
            "formula_status", "price_levels_valid", "route_contract_match",
            "cross_source_price_status", "tdx_reason_code",
        ):
            status = str(plan.get(field) if plan.get(field) is not None else "UNKNOWN")
            counts = status_totals.setdefault(field, {})
            counts[status] = counts.get(status, 0) + 1
        macd = _json_mapping(plan.get("daily_macd_verification"))
        if macd:
            item["daily_macd_verification"] = {
                key: macd[key]
                for key in ("bar_count", "formula_status", "input_hash_status")
                if key in macd
            }
        is_formula_exception = (
            str(plan.get("formula_status") or "") not in {"", "MATCH"}
            or str(macd.get("formula_status") or "") not in {"", "MATCH"}
            or str(macd.get("input_hash_status") or "") not in {"", "MATCH"}
            or plan.get("price_levels_valid") is not True
            or plan.get("route_contract_match") is not True
        )
        if is_formula_exception:
            for key in (
                "local_daily_close",
                "recomputed_ma",
                "declared_ma_relative_errors",
            ):
                if key in plan:
                    item[key] = plan[key]
            if macd:
                item["daily_macd_verification"] = dict(macd)
        if is_formula_exception:
            compact.append(item)
    result["plans"] = {
        "encoding": "a5-a3-verification-projection/1",
        "plan_count": len(plan_index),
        "status_totals": status_totals,
        "plan_index_sha256": _canonical_hash(plan_index),
        "plan_symbols": sorted(str(row.get("symbol") or "") for row in plan_index),
        "exception_count": len(compact),
        "exceptions": compact,
        "archive_locator": "independent_verification.a3.plans",
    }
    result["projection_scope"] = (
        "Every plan identity and formula/price/route/source outcome is present. Exact recomputed vectors and hashes "
        "are expanded for exceptions; repeated matching numeric vectors remain in the immutable fact archive."
    )
    return result


def _a1_market_universe(audit: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the full traceable A1 universe for post-close counterexamples."""

    stages = _rows(audit.get("stages"))
    stage = next((item for item in stages if str(item.get("stage")).upper() == "A1"), None)
    output = _json_mapping(stage.get("output")) if stage is not None else {}
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for pool_name, values in (
        ("A1_ACTIVE", output.get("active_research_pool")),
        ("A1_MONITOR", output.get("monitor_pool")),
        ("A1_REJECTED", output.get("rejected_candidates")),
    ):
        for item in _rows(values):
            symbol = str(item.get("symbol") or "")
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            selection = _json_mapping(item.get("a1_selection_evidence"))
            fundamental = _json_mapping(item.get("fundamental_support"))
            half_year = _json_mapping(fundamental.get("latest_half_year"))
            disclosed = _json_mapping(item.get("disclosed_business_match"))
            result.append({
                "symbol": symbol,
                "name": item.get("name") or item.get("company_name"),
                "theme_id": item.get("theme_id") or item.get("primary_theme"),
                "theme_name": item.get("theme_name") if item.get("theme_id") else item.get("primary_theme_name"),
                "pool": pool_name,
                "selection_reasons": item.get("core_thesis") if isinstance(item.get("core_thesis"), list) else [],
                "risk_reasons": item.get("bear_case") if isinstance(item.get("bear_case"), list) else [],
                "a1_gate_evidence": {
                    "autonomous_status": item.get("autonomous_status"),
                    "downstream_trade_eligible": item.get("downstream_trade_eligible"),
                    "selection_performed": selection.get("selection_performed"),
                    "reason_codes": list(selection.get("reason_codes") or []),
                    "data_gaps": list(selection.get("data_gaps") or []),
                    "financial_quality_score": item.get("financial_quality_score"),
                    "data_quality_score": item.get("data_quality_score"),
                    "evidence_confidence": item.get("evidence_confidence"),
                    "monthly_direction_id": item.get("monthly_direction_id"),
                    "missing_factors": list(item.get("missing_factors") or []),
                    "fundamental_support": {
                        "supported": fundamental.get("supported"),
                        "score": fundamental.get("score"),
                        "minimum_score": fundamental.get("minimum_score"),
                        "coverage_ratio": fundamental.get("coverage_ratio"),
                        "latest_half_year": {key: half_year.get(key) for key in (
                            "fiscal_year", "fiscal_period", "operating_income_yoy_pct",
                            "parent_holder_net_profit_yoy_pct", "reason_code", "supported",
                        )},
                    },
                    "disclosed_business_match": {key: disclosed.get(key) for key in (
                        "raw_disclosure_available", "structured_exposure_available",
                        "structured_match_confirmed", "match_basis",
                    )},
                },
            })
    return result


def build_a5_fact_snapshot(
    store: RuntimeStore,
    output_dir: Path,
    *,
    trade_date: date,
    cutoff_at: datetime,
    review_kind: A5ReviewKind,
    lane_id: str,
    independent_verifier: Any | None = None,
) -> dict[str, Any]:
    """Freeze all persisted facts A5 is allowed to interpret."""

    if cutoff_at.tzinfo is None or cutoff_at.utcoffset() is None:
        raise ValueError("A5 cutoff must be timezone-aware")
    cutoff = cutoff_at.astimezone(SHANGHAI)
    if cutoff.date() != trade_date:
        raise ValueError("A5 cutoff must belong to trade date")

    session_start = cutoff.replace(hour=9, minute=0, second=0, microsecond=0)
    raw_event_rows = store.list_monitor_events(
        lane_id=lane_id, effective_only=False, from_time=session_start, to_time=cutoff,
    )
    observed_plan_ids = {str(_json_mapping(row.get("payload_json")).get("plan_id") or "") for row in raw_event_rows}
    raw_plans = store.list_execution_plans(lane_id=lane_id)
    # Prior entries remain auditable after T+1 exits, including a position
    # closed today. They must not inflate today's A3 publication count.
    carryover_lifecycles = carryover_evidence(store.list_a4_signal_lifecycles(
        lane_id=lane_id, status=("OPEN", "EXIT_PENDING", "PARTIALLY_CLOSED", "CLOSED"), limit=10000,
    ), cutoff)
    selected_plan_rows, carryover_plan_rows, retired_plans = select_review_plans(
        raw_plans, cutoff=cutoff, observed_ids=observed_plan_ids,
        carryover_ids={str(row.get("plan_id") or "") for row in carryover_lifecycles},
    )
    plans = [_plan_projection(row) for row in selected_plan_rows]

    source_latest: dict[str, str] = {}
    for item in plans:
        source = str(item.get("source_run_id") or "")
        if source:
            source_latest[source] = max(source_latest.get(source, ""), str(item.get("updated_at") or ""))
    source_run_ids = sorted(source_latest, key=lambda item: (source_latest[item], item), reverse=True)
    if not source_run_ids:
        # A zero-plan day is precisely when A5 must still be able to distinguish
        # an empty A2 focus pool from an over-selective A3 gate.  In that case
        # there is no plan payload to carry source_run_id, so bind to the latest
        # persisted close run for the primary lane at or before this session.
        source_run_ids = [
            str(row.get("run_id"))
            for row in store.list_workflow_runs(limit=200)
            if str(row.get("lane_id") or "") == lane_id
            and str(row.get("slot") or "").lower() == "close"
            and str(row.get("trade_date") or "") < trade_date.isoformat()
            and str(row.get("run_id") or "")
        ][:1]
    missing: list[str] = []
    a2 = {"status": "NOT_AVAILABLE", "counts": {}, "themes": [], "candidates": []}
    market_universe: list[dict[str, Any]] = []
    if source_run_ids:
        audit, reasons = _audit_output(Path(output_dir), source_run_ids[0], lane_id)
        missing.extend(reasons)
        a2, reasons = _a2_projection(audit)
        a2["technical_candidates"] = _a3_candidates(audit)
        missing.extend(reasons)
        market_universe = _a1_market_universe(audit)
    else:
        missing.append("A3_SOURCE_RUN_NOT_FOUND")

    events = []
    action_counts: dict[str, int] = {}
    effective_action_counts: dict[str, int] = {}
    effective_event_count = 0
    warmed_macd_plans: set[str] = set()
    for row in raw_event_rows:
        action = str(row.get("action") or "UNKNOWN")
        action_counts[action] = action_counts.get(action, 0) + 1
        # Older producers marked DATA_BLOCK effective for alert delivery.
        # Preserve that raw flag but classify it as a data observation, not
        # an effective business signal. Never rewrite the original ledger.
        recorded_effective = bool(row.get("effective"))
        effective = recorded_effective and action != "DATA_BLOCK"
        effective_event_count += int(effective)
        if effective:
            effective_action_counts[action] = effective_action_counts.get(action, 0) + 1
        payload = _json_mapping(row.get("payload_json"))
        indicators = _json_mapping(_json_mapping(payload.get("strategy")).get("indicator_observations"))
        if _json_mapping(indicators.get("m15_macd")).get("warmup_complete") is True:
            warmed_macd_plans.add(str(payload.get("plan_id")))
        # Keep complete counts but only send consequential rows to A5.  A
        # per-plan NO_ACTION heartbeat can number in the thousands and carries
        # no additional causal evidence after aggregation.
        if not effective and action == "NO_ACTION":
            continue
        payload = _json_mapping(row.get("payload_json"))
        symbol = str(payload.get("symbol") or "")
        strategy = _json_mapping(payload.get("strategy"))
        events.append({
            "evidence_id": f"A4:EVENT:{row.get('event_id')}",
            "event_id": row.get("event_id"),
            "minute_end": row.get("minute_end"),
            "plan_id": payload.get("plan_id"),
            "symbol": symbol,
            "action": row.get("action"),
            "effective": effective,
            "recorded_effective": recorded_effective,
            "reason_code": row.get("reason_code"),
            "diagnostic_code": payload.get("diagnostic_code"),
            "strategy_profile": _json_mapping(payload.get("strategy")).get("strategy_profile"),
            "strategy_reason_codes": _json_mapping(payload.get("strategy")).get("reason_codes") or [],
            "unmet_conditions": _json_mapping(payload.get("strategy")).get("unmet_conditions") or [],
            "entry_geometry": {key: strategy[key] for key in (
                "live_entry_price", "live_stop_level", "live_target_price", "live_no_chase_price",
                "live_reward_risk", "live_stop_distance_pct", "minimum_reward_risk",
                "reward_risk_entry_ceiling", "entry_ceiling_scope",
                "maximum_stop_distance_pct", "closed_5m_end", "closed_15m_end") if key in strategy},
        })

    lifecycles = []
    raw_lifecycles = store.list_a4_signal_lifecycles(lane_id=lane_id, trade_date=trade_date.isoformat(), limit=1000)
    for row in raw_lifecycles:
        signal_time = row.get("signal_time")
        if signal_time and not _within_cutoff(signal_time, trade_date, cutoff):
            continue
        lifecycles.append({
            "evidence_id": f"A4:LIFECYCLE:{row.get('lifecycle_id')}",
            "lifecycle_id": row.get("lifecycle_id"),
            "plan_id": row.get("plan_id"),
            "source_run_id": row.get("source_run_id"),
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "stock_behavior_type": row.get("stock_behavior_type"),
            "strategy_profile": row.get("strategy_profile"),
            "status": row.get("status"),
            "signal_time": row.get("signal_time"),
            "signal_price": row.get("signal_price"),
            "entry_time": row.get("entry_time"),
            "entry_price": row.get("entry_price"),
            "entry_qty": row.get("entry_qty"),
            "exit_qty": row.get("exit_qty"),
            "remaining_qty": row.get("remaining_qty"),
            "exit_signal_time": row.get("exit_signal_time"),
            "exit_signal_price": row.get("exit_signal_price"),
            "exit_signal_qty": row.get("exit_signal_qty"),
            "exit_metadata": _json_mapping(row.get("exit_metadata_json")),
            "exit_time": row.get("exit_time"),
            "exit_price": row.get("exit_price"),
            "exit_reason": row.get("exit_reason"),
            "net_return": row.get("net_return"),
            "realized_pnl": row.get("realized_pnl"),
            "mfe": row.get("mfe"),
            "mae": row.get("mae"),
            "holding_minutes": row.get("holding_minutes"),
            "last_bar_end": row.get("last_bar_end"),
        })

    lifecycle_counts: dict[str, int] = {}
    for item in lifecycles:
        key = str(item.get("status") or "UNKNOWN")
        lifecycle_counts[key] = lifecycle_counts.get(key, 0) + 1
    finished_returns = [float(item["net_return"]) for item in lifecycles
                        if item.get("status") == "CLOSED" and item.get("remaining_qty") == 0 and item.get("net_return") is not None]
    metrics = {
        "a2_focus_count": int(a2.get("counts", {}).get("FOCUS", 0)),
        "a2_watch_count": int(a2.get("counts", {}).get("WATCH", 0)),
        "a2_theme_count": len(a2.get("themes", [])),
        "a3_plan_count": len(plans),
        "a3_strategy_counts": _count_by(plans, "strategy_profile"),
        "a3_daily_macd_complete_count": sum(all(_json_mapping(item.get("daily_macd")).get(key) is not None
                                                for key in ("dif", "dea", "hist")) for item in plans),
        "a4_m15_macd_warmed_plan_count": len(warmed_macd_plans),
        "a4_m15_macd_applicable_plan_count": sum(item.get("strategy_profile") == "MA520_SWING" for item in plans),
        "a4_m15_macd_not_warmed_plan_ids": [str(item.get("plan_id")) for item in plans
            if item.get("strategy_profile") == "MA520_SWING" and str(item.get("plan_id")) not in warmed_macd_plans],
        "indicator_verification_scope": "SEE_INDEPENDENT_VERIFICATION_FIELD_CHECKS_AND_FROZEN_INDICATOR_FORMULA_AUDIT; FORMULA_MATCH_IS_NOT_RAW_SOURCE_VERIFICATION",
        "a4_monitor_observation_count": sum(action_counts.values()),
        "a4_effective_event_count": effective_event_count,
        "a4_effective_action_counts": effective_action_counts,
        "a4_trade_signal_count": sum(effective_action_counts.get(action, 0) for action in (
            "BUY_SIGNAL", "ADD_SIGNAL", "SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT")),
        "a4_plan_invalidation_count": effective_action_counts.get("PLAN_INVALIDATED", 0),
        "a4_action_counts": action_counts,
        "a4_lifecycle_count": len(lifecycles),
        "a4_lifecycle_counts": lifecycle_counts,
        "a4_completed_return_count": len(finished_returns),
        "a4_mean_net_return": (sum(finished_returns) / len(finished_returns)) if finished_returns else None,
        "a4_positive_return_count": sum(value > 0 for value in finished_returns),
    }
    snapshot = {
        "schema_version": "a5-review-facts/1.0.0",
        "trade_date": trade_date.isoformat(),
        "review_kind": review_kind.value,
        "cutoff_at": cutoff.isoformat(),
        "lane_id": lane_id,
        "source_run_ids": source_run_ids,
        "metrics": metrics,
        "a2": a2,
        "a3": {"plans": plans, "candidates": a2.get("technical_candidates", []),
               "plan_scope": {"evidence_id": "A3:PLAN_SCOPE", "session_plan_count": len(plans),
                              "retired_before_session_count": len(retired_plans),
                              "retired_before_session": retired_plans}},
        "a4": {"events": events, "lifecycles": lifecycles, "observation_groups": _compact_a4_observations(events)[1],
               "carryover_plans": [_plan_projection(row) for row in carryover_plan_rows],
               "carryover_lifecycles": carryover_lifecycles},
        "review_history": _review_history(
            store,
            trade_date=trade_date,
            review_kind=review_kind,
            cutoff_at=cutoff,
        ),
        "data_quality": {
            "status": "READY" if not missing else "DEGRADED",
            "missing_components": list(dict.fromkeys(missing)),
            "note": "缺失项只限制结论强度，不得自动解释为策略无效。",
        },
    }
    if independent_verifier is not None:
        try:
            independent = independent_verifier.verify(
                a2=a2,
                market_universe=market_universe,
                plan_rows=selected_plan_rows,
                event_rows=raw_event_rows,
                cutoff_at=cutoff,
            )
        except Exception:
            independent = {
                "schema_version": "a5-independent-verification/1.0.0",
                "status": "UNAVAILABLE",
                "reason_code": "A5_INDEPENDENT_VERIFICATION_FAILED",
                "counterexamples": [],
            }
        snapshot["independent_verification"] = independent
        snapshot["metrics"]["a5_counterexample_count"] = len(
            independent.get("counterexamples", [])
            if isinstance(independent.get("counterexamples"), list)
            else []
        )
        coverage_ledger = _rows(independent.get("top_performance_ledger"))
        snapshot["metrics"]["a5_top_performance_ledger_count"] = len(coverage_ledger)
        snapshot["metrics"]["a5_top_performance_captured_count"] = sum(
            str(row.get("coverage_status") or "") == "CAPTURED_EFFECTIVE_A4"
            for row in coverage_ledger
        )
        snapshot["metrics"]["a5_stage_supplement_count"] = int(
            _json_mapping(independent.get("a2")).get("stage_supplement_count") or 0
        )
        snapshot["metrics"]["a5_independent_verification_status"] = str(
            independent.get("status") or "UNAVAILABLE"
        )
        snapshot["metrics"]["a5_counterexample_drop_stage_counts"] = _count_by(
            _rows(independent.get("counterexamples")), "drop_stage")
        independent_status = str(independent.get("status") or "UNAVAILABLE").upper()
        if independent_status != "READY":
            reason = (
                "A5_INDEPENDENT_VERIFICATION_DEGRADED"
                if independent_status == "DEGRADED"
                else "A5_INDEPENDENT_VERIFICATION_UNAVAILABLE"
            )
            snapshot["data_quality"]["status"] = "DEGRADED"
            snapshot["data_quality"].setdefault("limitation_reasons", []).append(reason)
    signal_market = _json_mapping(_json_mapping(snapshot.get("independent_verification")).get("signal_market"))
    snapshot["signal_stock_reviews"] = build_signal_stock_reviews(
        raw_event_rows, selected_plan_rows, store.list_fills(), signal_market, cutoff, lifecycles=raw_lifecycles)
    from .engineering import operational_evidence
    snapshot["operational_evidence"] = operational_evidence(store, Path(output_dir), cutoff=cutoff)
    snapshot["input_hash"] = _canonical_hash(snapshot)
    return snapshot


def _count_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        result[value] = result.get(value, 0) + 1
    return result


def _review_history(
    store: RuntimeStore,
    *,
    trade_date: date,
    review_kind: A5ReviewKind,
    cutoff_at: datetime,
) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for row in store.list_a5_reviews(limit=30):
        if str(row.get("trade_date") or "") == trade_date.isoformat() and str(row.get("review_kind") or "") == review_kind.value:
            continue
        try:
            created_at = datetime.fromisoformat(str(row.get("created_at") or ""))
            if created_at.tzinfo is None or created_at.utcoffset() is None:
                created_at = created_at.replace(tzinfo=SHANGHAI)
            created_at = created_at.astimezone(SHANGHAI)
        except (TypeError, ValueError):
            # A history row without a trustworthy creation time cannot be
            # admitted into a point-in-time review.
            continue
        if created_at > cutoff_at:
            continue
        report = _json_mapping(row.get("report_json"))
        if not report:
            continue
        review_id = str(row.get("review_id") or "")
        defects = _rows(report.get("core_defects"))
        missed = _rows(report.get("missed_opportunity_reviews"))
        proposals = _rows(report.get("improvement_proposals"))
        history.append({
            "evidence_id": f"A5H:{review_id}",
            "review_id": review_id,
            "trade_date": row.get("trade_date"),
            "review_kind": row.get("review_kind"),
            "overall_verdict": report.get("overall_verdict"),
            "confirmed_defect_count": sum(bool(item.get("is_confirmed_defect")) for item in missed),
            "counterexample_count": len(missed),
            "defects": [str(item.get("problem") or "")[:300] for item in defects[:8]],
            "proposal_ids": [str(item.get("proposal_id") or "") for item in proposals[:3]],
        })
        if len(history) >= 20:
            break
    return history


def _evidence_ids(snapshot: Mapping[str, Any]) -> set[str]:
    values: set[str] = {"METRICS:DAILY", "DATA_QUALITY:DAILY"}
    a2 = _json_mapping(snapshot.get("a2"))
    a3 = _json_mapping(snapshot.get("a3"))
    a4 = _json_mapping(snapshot.get("a4"))
    for group in (a2.get("themes"), a2.get("candidates"), a3.get("plans"), a3.get("candidates"), a4.get("events"), a4.get("lifecycles"), a4.get("observation_groups")):
        for row in _rows(group):
            if row.get("evidence_id"):
                values.add(str(row["evidence_id"]))
    def collect(value: Any) -> None:
        if isinstance(value, Mapping):
            evidence_id = value.get("evidence_id")
            if evidence_id:
                values.add(str(evidence_id))
            for nested in value.values():
                collect(nested)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for nested in value:
                collect(nested)

    collect(snapshot.get("independent_verification"))
    collect(snapshot.get("review_history"))
    collect(snapshot.get("signal_stock_reviews"))
    collect(snapshot.get("operational_evidence"))
    collect(a3.get("plan_scope"))
    collect(a4.get("carryover_plans"))
    collect(a4.get("carryover_lifecycles"))
    return values


def _projection_evidence_ids(projection: Mapping[str, Any]) -> set[str]:
    """Return only evidence identities actually exposed to the model.

    Model projections may introduce deterministic aggregate identities that do
    not exist as rows in the immutable fact archive.  Table encodings retain
    those identities in a ``columns``/``rows`` envelope, so the regular nested
    mapping collector is not enough.  Restricting the allow-list to identities
    present in this exact projection keeps strict citation validation while
    allowing a report to cite a traceable aggregate.
    """

    values: set[str] = set()

    def collect(value: Any) -> None:
        if isinstance(value, Mapping):
            evidence_id = value.get("evidence_id")
            if isinstance(evidence_id, str) and evidence_id:
                values.add(evidence_id)
            columns = value.get("columns")
            rows = value.get("rows")
            if (isinstance(columns, Sequence) and not isinstance(columns, (str, bytes))
                    and isinstance(rows, Sequence) and not isinstance(rows, (str, bytes))
                    and "evidence_id" in columns):
                index = list(columns).index("evidence_id")
                for row in rows:
                    if (isinstance(row, Sequence) and not isinstance(row, (str, bytes))
                            and len(row) > index and isinstance(row[index], str) and row[index]):
                        values.add(row[index])
            for nested in value.values():
                collect(nested)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for nested in value:
                collect(nested)

    collect(projection)
    return values


def _validate_evidence(
    report: A5ReviewReport,
    snapshot: Mapping[str, Any],
    *,
    check_stage: bool = True,
    allowed_projection_evidence: set[str] | None = None,
) -> None:
    allowed = _evidence_ids(snapshot) | (allowed_projection_evidence or set())
    referenced: list[str] = []
    for layer in (report.a2_review, report.a3_review, report.a4_review):
        referenced.extend(layer.evidence_ids)
    for item in report.signal_reviews:
        referenced.extend(item.evidence_ids)
    for item in report.missed_opportunity_reviews:
        referenced.extend(item.evidence_ids)
    for item in report.core_defects:
        referenced.extend(item.evidence_ids)
    for item in report.improvement_proposals:
        referenced.extend(item.evidence_ids)
    if any(item not in allowed for item in referenced):
        raise A5ReviewError("A5_OUTPUT_EVIDENCE_INVALID")
    facts = {str(row.get("symbol")): row for row in _rows(_json_mapping(snapshot.get("independent_verification")).get("counterexamples"))}
    for item in report.missed_opportunity_reviews:
        fact = facts.get(item.symbol)
        if fact is None:
            raise A5ReviewError("A5_COUNTEREXAMPLE_NOT_IN_FACTS")
        expected = str(fact.get("drop_stage") or "UNRESOLVED").split("_")[0]
        if expected not in {"A1", "A2", "A3", "A4"}:
            expected = "UNRESOLVED"
        if check_stage and item.funnel_drop_stage != expected:
            raise A5ReviewError("A5_COUNTEREXAMPLE_STAGE_CONFLICT")
        if str(fact.get("evidence_id")) not in item.evidence_ids:
            raise A5ReviewError("A5_COUNTEREXAMPLE_REFERENCE_MISMATCH")


def _markdown(report: A5ReviewReport, snapshot: Mapping[str, Any]) -> str:
    kind = "盘中复盘" if report.review_kind is A5ReviewKind.MIDDAY else "盘后复盘"
    metrics = business_metrics(snapshot)
    lines = [
        f"# A5 {kind}｜{report.trade_date.isoformat()}", "",
        "> 内部模拟复盘，不构成投资建议；A5 不修改生产策略，不产生或执行交易信号。", "",
        f"- 结论：**{report.overall_verdict}**",
        f"- 事实截止：`{snapshot.get('cutoff_at')}`",
        f"- A2 聚焦/观察：`{metrics.get('a2_focus_count', 0)}/{metrics.get('a2_watch_count', 0)}`",
        f"- A3 计划：`{metrics.get('a3_plan_count', 0)}`",
        f"- A4 业务状态事件/生命周期：`{metrics.get('a4_effective_event_count', 0)}/{metrics.get('a4_lifecycle_count', 0)}`；交易信号：{metrics.get('a4_trade_signal_count', '未单独统计')}；计划失效：{metrics.get('a4_plan_invalidation_count', '未单独统计')}",
        f"- 强势股覆盖账本/需复核反例：`{metrics.get('a5_top_performance_ledger_count', 0)}/{metrics.get('a5_counterexample_count', 0)}`；其中已产生有效盘中信号：{metrics.get('a5_top_performance_captured_count', 0)}",
        f"- 反例落层计数（代码统计）：`{json.dumps(metrics.get('a5_counterexample_drop_stage_counts', {}), ensure_ascii=False)}`",
        "- 核验边界：价格字段一致不代表开高低、成交量或全部技术指标一致；次日可卖也不代表必定成交。",
        "", "## 总结", "", report.executive_summary, "",
    ]
    if report.fact_reconciliation:
        lines.extend(["## 事实审校", "", *[f"- {note}" for note in report.fact_reconciliation], ""])
    for title, layer in (("A2 选股与题材", report.a2_review), ("A3 日线计划", report.a3_review), ("A4 日内择时", report.a4_review)):
        lines.extend([f"## {title}", "", f"**{layer.verdict}**｜{layer.summary}", ""])
        if layer.strengths:
            lines.extend(["优点：", *[f"- {item}" for item in layer.strengths], ""])
        if layer.defects:
            lines.extend(["缺陷：", *[f"- {item}" for item in layer.defects], ""])
        if layer.data_limitations:
            lines.extend(["数据限制：", *[f"- {item}" for item in layer.data_limitations], ""])
    if report.signal_reviews:
        lines.extend(["## 信号逐项评价", "", "| 股票 | 策略 | 状态 | 归因 | 评价 |", "|---|---|---|---|---|"])
        for item in report.signal_reviews:
            lines.append(f"| {item.name} {item.symbol} | {item.strategy_profile} | {item.lifecycle_status} | {item.attribution} | {item.assessment} |")
        lines.append("")
    if report.signal_stock_reviews:
        lines.extend(["## 信号股票：当日表现与入场审计", "",
                      "价格表现不等于已实现收益；仅使用复盘截止前的行情和成交。", ""])
        for item in report.signal_stock_reviews:
            lines.extend([f"### {item.get('name')}（{item.get('symbol')}）", "",
                          f"- 当日表现：{item.get('performance_summary')}",
                          f"- 入场审计：{item.get('entry_audit_summary')}",
                          f"- 证据编号：{item.get('evidence_id')}", ""])
    if report.missed_opportunity_reviews:
        lines.extend(["## 反向拷问：当日强势但未被捕获", "", "| 股票 | 主题 | 表现 | 漏斗位置 | 判断 |", "|---|---|---|---|---|"])
        for item in report.missed_opportunity_reviews:
            confirmed = "已确认缺陷" if item.is_confirmed_defect else "待影子验证"
            lines.append(
                f"| {item.name} {item.symbol} | {item.theme or '未映射'} | {item.observed_performance} | "
                f"{item.funnel_drop_stage} | {item.assessment}（{confirmed}） |"
            )
        lines.append("")
    lines.extend(["## 核心缺陷", ""])
    lines.extend([f"- **{item.layer}/{item.severity}** {item.problem}" for item in report.core_defects] or ["- 本次未发现可由当前样本确认的核心缺陷。"])
    lines.extend(["", "## 改进提案（仅建议/影子验证）", ""])
    for item in report.improvement_proposals:
        lines.extend([
            f"### {item.proposal_id}｜{item.target}", "",
            f"- 假设：{item.hypothesis}", f"- 建议：{item.proposed_change}",
            f"- 验证：{item.validation_method}", f"- 成功标准：{item.success_criteria}",
            f"- 证伪标准：{item.falsification_criteria}",
            (f"- 最少影子观察：{item.min_shadow_days} 个交易日" if item.type == "SHADOW_TEST" else "- 验收方式：确定性回归及数据核对，不设统一影子观察天数"), "",
        ])
    lines.extend(["## 尚不能下结论", ""])
    lines.extend([f"- {item.question}（{item.reason}）：{item.resolution}" for item in report.unresolved_questions])
    return "\n".join(lines).rstrip() + "\n"


def _enforce_verified_findings(report: A5ReviewReport, facts: Mapping[str, Any]) -> None:
    """Model prose cannot clear failed deterministic verification."""
    findings = []
    # Older model/server reports described retry records as independent
    # research-task failures and plan×minute gaps as independent plan
    # incidents. Replace those derived statements with the canonical grouped
    # facts below instead of duplicating them during frozen revalidation.
    report.core_defects = [item for item in report.core_defects if not (
        ("研究任务失败或超时" in item.problem and item.layer == "A2")
        or ("计划在应观察窗口内缺少决策记录" in item.problem)
        or ("实际决策窗口缺少" in item.problem and "判断记录" in item.problem)
    )]
    def references(rows):
        # Report citations are a bounded index, not the source archive. Keep
        # the complete event set in frozen facts and the total in the finding.
        ids = list(dict.fromkeys(str(row["evidence_id"]) for row in rows))
        return ids if len(ids) <= 20 else ids[:10] + ids[-10:]
    operations = _rows(facts.get("operational_evidence"))
    failed_research = [row for row in operations if row.get("kind") in {"JOB_TERMINATED", "JOB_FAILED"}
                       and row.get("job") in {"auction-refresh", "close", "morning"}]
    if failed_research:
        incidents: list[list[Mapping[str, Any]]] = []
        for job in sorted({str(row.get("job") or "UNKNOWN") for row in failed_research}):
            ordered = sorted((row for row in failed_research if str(row.get("job") or "UNKNOWN") == job),
                             key=lambda row: str(row.get("time") or ""))
            current: list[Mapping[str, Any]] = []
            previous: datetime | None = None
            for row in ordered:
                try:
                    stamp = datetime.fromisoformat(str(row.get("time") or "").replace("Z", "+00:00"))
                except ValueError:
                    stamp = None
                if current and stamp is not None and previous is not None and (stamp - previous).total_seconds() > 2400:
                    incidents.append(current)
                    current = []
                current.append(row)
                if stamp is not None:
                    previous = stamp
            if current:
                incidents.append(current)
        labels = {"auction-refresh": "竞价刷新", "close": "收盘研究", "morning": "盘前研究"}
        affected_jobs = "、".join(sorted({labels.get(str(row.get('job')), str(row.get('job'))) for row in failed_research}))
        findings.append(A5Defect(layer="ORCHESTRATOR", severity="MEDIUM", confidence="HIGH", blocked_by_data=False,
            problem=f"{affected_jobs}发生{len(incidents)}个失败周期，共包含{len(failed_research)}条失败或超时记录；记录条数不是独立任务数，需按故障周期核对恢复和输出血缘。",
            evidence_ids=references(failed_research)))
    position_alerts = [row for row in operations if row.get("kind") == "POSITION_DATA_HEALTH_EVENT" and row.get("state") == "BLOCKED"]
    if position_alerts:
        findings.append(A5Defect(layer="A4", severity="HIGH", confidence="HIGH", blocked_by_data=True,
            problem=f"存在{len(position_alerts)}次持仓风险数据受限通知；须核对当时可用风险输入与恢复，不能从无成交推断无风险，也不能据此断言漏卖。",
            evidence_ids=references(position_alerts)))
    alerts = [row for row in operations if row.get("kind") == "SOURCE_HEALTH_EVENT" and row.get("state") == "BLOCKED"]
    if alerts:
        findings.append(A5Defect(layer="A4", severity="MEDIUM", confidence="HIGH", blocked_by_data=True,
            problem=f"当日记录{len(alerts)}次行情阻断告警。告警可能为误报，须逐窗对照执行数据；观察记录齐全不代表告警和执行口径一致。",
            evidence_ids=references(alerts)))
    verification = _json_mapping(facts.get("independent_verification"))
    a4 = _json_mapping(verification.get("a4"))
    metrics = _json_mapping(facts.get("metrics"))
    applicable = metrics.get("a4_m15_macd_applicable_plan_count", _json_mapping(metrics.get("a3_strategy_counts")).get("MA520_SWING", 0))
    if applicable and metrics.get("a4_monitor_observation_count", 0) and metrics.get("a4_m15_macd_warmed_plan_count", 0) < applicable:
        findings.append(A5Defect(layer="A4", severity="MEDIUM", confidence="HIGH", blocked_by_data=True,
            problem=f"{applicable}个适用520计划中，仅{metrics.get('a4_m15_macd_warmed_plan_count', 0)}个记录15分钟MACD预热完成；不能以均线通过代替指标完整性验收。",
            evidence_ids=["METRICS:DAILY"]))
    bad_prices = [row for row in _rows(a4.get("plans"))
                  if row.get("cross_source_status") == "MISMATCH" or row.get("archived_tdx_status") == "MISMATCH"]
    if bad_prices:
        findings.append(A5Defect(layer="A4", severity="MEDIUM", confidence="HIGH", blocked_by_data=True,
            problem=f"{len(bad_prices)}个计划存在异源价格超容差差异；行情覆盖完整不等于数值一致。",
            evidence_ids=[str(row["evidence_id"]) for row in bad_prices[:20]]))
    totals = verification_totals(facts)
    gaps = [row for row in _rows(a4.get("plans")) if float(row.get("observation_coverage", 1)) < 1]
    if gaps:
        incidents = int(totals.get("missing_observation_incident_count") or 0)
        records = int(totals.get("missing_observation_count") or 0)
        findings.append(A5Defect(layer="A4", severity="HIGH", confidence="HIGH",
            problem=f"应观察窗口发生{incidents}个缺失事件，影响{len(gaps)}个计划、共{records}条股票×分钟记录；需要按事件时间和调度租约核对。",
            evidence_ids=[str(row["evidence_id"]) for row in gaps[:20]]))
    omissions = [
        omission
        for plan in _rows(a4.get("plans"))
        for omission in _rows(plan.get("orchestration_omissions"))
    ]
    if omissions:
        findings.append(A5Defect(layer="ORCHESTRATOR", severity="MEDIUM", confidence="HIGH", blocked_by_data=True,
            problem=f"发现{len(omissions)}条策略候选动作未形成同分钟有效动作记录；候选动作、执行数据状态和最终发布动作必须分开保存，数据受限窗口不得补记为有效交易信号。",
            evidence_ids=[str(row.get("evidence_id") or "A5V:A4:SUMMARY") for row in _rows(a4.get("plans")) if row.get("orchestration_omission_count")][:20]))
    if _json_mapping(facts.get("a2")).get("lineage_complete") is False:
        findings.append(A5Defect(layer="A2", severity="HIGH", confidence="HIGH", blocked_by_data=True,
            problem="A2量化评价数量与全池去向不闭合，不能认定漏选归因完整。", evidence_ids=["DATA_QUALITY:DAILY"]))
    if findings:
        deduplicated: list[A5Defect] = []
        seen_findings: set[tuple[str, str]] = set()
        for item in [*findings, *report.core_defects]:
            key = (item.layer, item.problem)
            if key in seen_findings:
                continue
            seen_findings.add(key)
            deduplicated.append(item)
        report.core_defects = deduplicated[:8]
        report.overall_verdict = "NEEDS_ATTENTION" if report.overall_verdict != "INCIDENT" else "INCIDENT"
        for layer in {item.layer for item in findings}:
            review = getattr(report, f"{layer.lower()}_review", None)
            if review is not None:
                review.verdict = "NEEDS_ATTENTION"
                review.summary = "；".join(item.problem for item in findings if item.layer == layer)[:600]
        report.executive_summary = ("确定性核验仍有待处理问题：" + "；".join(item.problem for item in findings) + " 模型分析：" + report.executive_summary)[:1200]
    # A2 labels cannot override actual A3 membership or A4 plan lineage.
    missed = {str(row.get("symbol")): str(row.get("drop_stage") or "")
              for row in _rows(verification.get("counterexamples"))}
    for row in report.missed_opportunity_reviews:
        actual = missed.get(row.symbol, "")
        if actual.startswith(("A1_", "A2_", "A3_", "A4_")):
            if row.funnel_drop_stage != actual[:2]:
                row.assessment = (
                    f"冻结晋级证据确认落层为{actual[:2]}（{actual}）。模型原始落层归因与证据冲突，"
                    "该项原归因不采纳；不能仅凭上涨确认策略缺陷，需按真实阶段继续核对。"
                )
                row.is_confirmed_defect = False
            row.funnel_drop_stage = actual[:2]
        source = next((item for item in _rows(verification.get("counterexamples")) if item.get("symbol") == row.symbol), {})
        explanation = _json_mapping(source.get("selection_audit")).get("explanation")
        if explanation:
            row.assessment = (str(explanation)[:350] + "。以上为原时点筛选依据；当日涨幅不能证明应入选。"
                              + row.assessment)[:600]
    if _json_mapping(verification.get("a3")).get("not_verified_fields"):
        names = {"MACD":"MACD", "KDJ":"KDJ", "VOLUME":"成交量"}
        missing = _json_mapping(verification.get("a3"))["not_verified_fields"]
        note = "独立复算仍未验证" + "、".join(names.get(str(k),str(k)) for k in missing) + "，不能据此宣称三套策略全部验收。"
        report.a3_review.data_limitations = [note, *report.a3_review.data_limitations][:8]
    for proposal in report.improvement_proposals:
        if proposal.type in {"ENGINEERING_FIX", "DATA_FIX"}:
            proposal.min_shadow_days = 0


class A5DailyReviewService:
    def __init__(
        self,
        *,
        store: RuntimeStore,
        prompts: PromptRepository,
        model_client: OpenAICompatibleModelClient,
        output_dir: Path,
        lane_id: str,
        model: str,
        independent_verifier: Any | None = None,
        notification_publisher: Any | None = None,
    ):
        self.store = store
        self.prompts = prompts
        self.model_client = model_client
        self.output_dir = Path(output_dir)
        self.lane_id = lane_id
        self.model = model
        self.independent_verifier = independent_verifier
        self.notification_publisher = notification_publisher
        self._failure_context: dict[str, Any] = {}

    def run(self, *, review_kind: A5ReviewKind, now: datetime,
            frozen_facts: Mapping[str, Any] | None = None, close_archive: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self._failure_context = {
            "input_hash": _json_mapping(frozen_facts).get("input_hash"),
        }
        try:
            result = self._run(
                review_kind=review_kind,
                now=now,
                frozen_facts=frozen_facts,
                close_archive=close_archive,
            )
        except Exception as exc:
            reason = str(getattr(exc, "reason_code", None) or type(exc).__name__)
            diagnostics = {
                **self._failure_context,
                **dict(getattr(exc, "diagnostics", {}) or {}),
            }
            publish = getattr(self.notification_publisher, "publish_a5_task_failure", None)
            if callable(publish):
                try:
                    publish(review_kind.value, reason_code=reason, diagnostics=diagnostics, now=now)
                except Exception:
                    pass
            raise
        publish_recovery = getattr(self.notification_publisher, "publish_a5_task_recovery", None)
        if callable(publish_recovery):
            try:
                publish_recovery(
                    review_kind.value,
                    input_hash=str(result.get("input_hash") or self._failure_context.get("input_hash") or "UNAVAILABLE"),
                    now=now,
                )
            except Exception:
                pass
        return result

    def _run(self, *, review_kind: A5ReviewKind, now: datetime,
             frozen_facts: Mapping[str, Any] | None = None, close_archive: Mapping[str, Any] | None = None) -> dict[str, Any]:
        current = now.astimezone(SHANGHAI)
        cutoff_clock = (11, 30) if review_kind is A5ReviewKind.MIDDAY else (15, 0)
        cutoff = current.replace(hour=cutoff_clock[0], minute=cutoff_clock[1], second=0, microsecond=0)
        if current < cutoff:
            raise A5ReviewError("A5_REVIEW_BEFORE_CUTOFF")
        if frozen_facts is None:
            facts = build_a5_fact_snapshot(
                self.store, self.output_dir, trade_date=current.date(), cutoff_at=cutoff,
                review_kind=review_kind, lane_id=self.lane_id,
                independent_verifier=self.independent_verifier,
            )
            if close_archive is not None:
                facts["post_close_archive"] = dict(close_archive)
        else:
            # Retry today's failed report without re-fetching later prices or
            # replacing the original observation/decision evidence.
            import copy
            facts = copy.deepcopy(dict(frozen_facts))
            if (facts.get("trade_date") != current.date().isoformat()
                    or facts.get("review_kind") != review_kind.value
                    or facts.get("cutoff_at") != cutoff.isoformat()
                    or facts.get("input_hash") != _canonical_hash({k: v for k, v in facts.items() if k != "input_hash"})):
                raise A5ReviewError("A5_FROZEN_FACT_IDENTITY_OR_HASH_MISMATCH")
        # Identical market facts must not reuse prose produced by an older
        # prompt/verification contract after a release.
        facts["review_contract"] = {
            "version": "a5-full-lineage-entry-audit/12",
            "prompt_sha256": self.prompts.document(_A5_PROMPT).sha256,
            "model": self.model,
        }
        facts["input_hash"] = _canonical_hash({key: value for key, value in facts.items() if key != "input_hash"})
        self._failure_context["input_hash"] = facts["input_hash"]
        signal_delivery = getattr(self.notification_publisher, "publish_signal_day_review", None)
        signal_notifications = []
        if callable(signal_delivery):
            try:
                signal_notifications = list(signal_delivery(facts, now=current))
            except Exception:
                # The publisher owns its delivery ledger. A notification
                # failure must not prevent research or trigger model retries.
                signal_notifications = [{"status": "FAILED", "reason_code": "SIGNAL_DAY_NOTIFICATION_FAILED"}]
        existing = self.store.list_a5_reviews(
            trade_date=current.date().isoformat(), review_kind=review_kind.value, limit=20,
        )
        same = next((row for row in existing if row.get("input_hash") == facts["input_hash"]), None)
        if same is not None:
            return self._public_row(
                same,
                created=False,
                notifications=signal_notifications + self._publish_notification(same, now=current),
            )

        target_dir = self.output_dir / "a5" / current.date().isoformat()
        artifact_stem = f"{review_kind.value.lower().replace('_', '-')}-{str(facts['input_hash'])[:12]}"
        # Preserve failed requests' facts as well as successful reviews.
        facts_path = target_dir / f"{artifact_stem}-facts.json"
        if facts_path.exists():
            if json.loads(facts_path.read_text(encoding="utf-8")) != facts:
                raise A5ReviewError("A5_ARCHIVED_FACT_CONFLICT")
        else:
            atomic_write_json(facts_path, facts)
        projection = _model_fact_projection(facts)
        allowed_projection_evidence = _projection_evidence_ids(projection)
        try:
            prompt, context_diagnostics = render_a5_prompt(self.prompts, _A5_PROMPT, projection)
        except A5ReviewError as exc:
            self._failure_context.update(exc.diagnostics)
            atomic_write_json(target_dir / f"{artifact_stem}-context.json", exc.diagnostics)
            raise
        self._failure_context.update(context_diagnostics)
        atomic_write_json(target_dir / f"{artifact_stem}-context.json", context_diagnostics)
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        try:
            if frozen_facts is not None and getattr(self.model_client, "revalidate_facts", False):
                result = self.model_client.revalidate_frozen(
                    facts=facts, model=self.model, template_hash=self.prompts.document(_A5_PROMPT).sha256,
                    rendered_prompt_hash=prompt_hash)
                atomic_write_json(target_dir / f"{artifact_stem}-archive-revalidation.json",
                                  self.model_client.archive_validation_provenance)
            else:
                # Keep the exact request bytes for future same-request replay.
                request_path = target_dir / f"{artifact_stem}-prompt-{prompt_hash[:12]}.txt"
                if not request_path.exists():
                    atomic_write_text(request_path, prompt)
                result: ModelCallResult = self.model_client.complete(
                    self.model,
                    [{"role": "system", "content": prompt}],
                    prompt_hash=prompt_hash,
                    input_hash=str(facts["input_hash"]),
                    stage="A5",
                    timeout_seconds=600,
                    max_output_tokens=32_768,
                )
        except ModelClientError as exc:
            # Keep request failure evidence without provider bodies, keys or
            # hidden reasoning; no report or notification is created.
            atomic_write_json(target_dir / f"{artifact_stem}-request-failure.json", {
                "status": "FAILED", "reason_code": exc.reason_code,
                "http_status": exc.status_code, "attempts": exc.attempts,
                "model": self.model, "input_hash": facts["input_hash"],
                "prompt_hash": prompt_hash, "failed_at": datetime.now(SHANGHAI).isoformat(),
            })
            raise
        prompt_hash = result.prompt_hash or prompt_hash
        # Preserve complete model responses even if schema/evidence validation
        # later rejects them. Never send this unvalidated artifact to Lark.
        raw_path = target_dir / f"{artifact_stem}-model-{result.output_hash[:12]}.json"
        raw_payload = {
            "model": self.model, "output_hash": result.output_hash,
            "thinking_variant": result.thinking_variant, "output": result.output,
            "prompt_hash": prompt_hash,
            "validation_status": "RAW_NOT_APPROVED", "input_hash": facts["input_hash"],
        }
        if raw_path.exists():
            if json.loads(raw_path.read_text(encoding="utf-8")) != raw_payload:
                raise A5ReviewError("A5_ARCHIVED_RESPONSE_CONFLICT")
        else:
            atomic_write_json(raw_path, raw_payload)
        try:
            report = A5ReviewReport.model_validate(_canonicalize_report_output(
                result.output, allowed_evidence=_evidence_ids(facts) | allowed_projection_evidence))
        except ValidationError as exc:
            raise A5ReviewError("A5_OUTPUT_SCHEMA_INVALID") from exc
        if report.review_kind is not review_kind or report.trade_date != current.date():
            raise A5ReviewError("A5_OUTPUT_IDENTITY_MISMATCH")
        _validate_evidence(
            report,
            facts,
            check_stage=False,
            allowed_projection_evidence=allowed_projection_evidence,
        )
        report.signal_stock_reviews = list(facts.get("signal_stock_reviews") or [])
        try:
            _enforce_verified_findings(report, facts)
            report.fact_reconciliation = reconcile_report(report, facts)
            report = A5ReviewReport.model_validate(report.model_dump())
        except ValidationError as exc:
            diagnostics = {"phase": "SERVER_FACT_RECONCILIATION", "errors": [
                {"field": ".".join(str(part) for part in e["loc"]), "type": e["type"]}
                for e in exc.errors(include_input=False, include_url=False)]}
            atomic_write_json(target_dir / f"{artifact_stem}-validation-failure.json", diagnostics)
            raise A5ReviewError("A5_SERVER_FACT_SCHEMA_INVALID", diagnostics=diagnostics) from exc
        archived_source = getattr(self.model_client, "archived_response_source", None)
        if archived_source:
            report.fact_reconciliation.append(
                f"本版复验已归档模型响应（{archived_source}），未重新调用模型；沿用原提示词及冻结输入，执行当前结构与事实校验。")
            if getattr(self.model_client, "revalidate_facts", False):
                report.fact_reconciliation.append(
                    "本次为冻结事实及原响应复验，不是原请求字节重放；原始与重建提示词哈希已分别留档，未宣称二者完全一致。")
        report = A5ReviewReport.model_validate(report.model_dump())
        _validate_evidence(
            report,
            facts,
            allowed_projection_evidence=allowed_projection_evidence,
        )

        target_dir = self.output_dir / "a5" / current.date().isoformat()
        artifact_stem = f"{review_kind.value.lower().replace('_', '-')}-{str(facts['input_hash'])[:12]}"
        # A late fact backfill may legitimately create another review revision for
        # the same session.  Keep every revision immutable instead of overwriting
        # the Markdown referenced by an earlier ledger row.
        target = target_dir / f"{artifact_stem}.md"
        json_target = target.with_suffix(".json")
        report_payload = report.model_dump(mode="json")
        atomic_write_json(json_target, {"facts": facts, "report": report_payload})
        atomic_write_text(target, _markdown(report, facts))
        status = "DEGRADED" if _json_mapping(facts.get("data_quality")).get("status") == "DEGRADED" else "COMPLETED"
        row, created = self.store.record_a5_review(
            trade_date=current.date().isoformat(), review_kind=review_kind.value,
            cutoff_at=cutoff, status=status, model=self.model,
            source_run_ids=facts.get("source_run_ids", []), input_hash=str(facts["input_hash"]),
            prompt_hash=prompt_hash, output_hash=result.output_hash,
            latency_ms=result.latency_ms, attempts=result.attempts,
            thinking_variant=result.thinking_variant, fact_snapshot=facts,
            report=report_payload, markdown_path=str(target),
        )
        return self._public_row(
            row,
            created=created,
            notifications=signal_notifications + self._publish_notification(row, now=current),
        )

    def _publish_notification(
        self,
        row: Mapping[str, Any],
        *,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Notify only after the immutable review row exists.

        Lark is an observability side effect.  A delivery failure must not
        invalidate the persisted review or trigger another model call.
        """

        if self.notification_publisher is None:
            return []
        try:
            return list(self.notification_publisher.publish_a5_review(row, now=now))
        except Exception:
            return [{"status": "FAILED", "reason_code": "LARK_NOTIFICATION_FAILED"}]

    @staticmethod
    def _public_row(
        row: Mapping[str, Any],
        *,
        created: bool,
        notifications: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        return {
            "status": row.get("status"), "created": created,
            "review_id": row.get("review_id"), "trade_date": row.get("trade_date"),
            "review_kind": row.get("review_kind"), "cutoff_at": row.get("cutoff_at"),
            "model": row.get("model"), "markdown_path": row.get("markdown_path"),
            "input_hash": row.get("input_hash"),
            "notifications": [dict(item) for item in notifications],
        }
