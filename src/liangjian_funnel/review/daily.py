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

from ..pipeline.model_client import ModelCallResult, OpenAICompatibleModelClient
from ..pipeline.prompts import PromptRepository
from ..reporting import atomic_write_json, atomic_write_text
from ..runtime.state import RuntimeStore


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
    min_shadow_days: int = Field(ge=1, le=120)
    risk: str = Field(min_length=1, max_length=500)
    automatic_production_change: bool = False

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
    missed_opportunity_reviews: list[A5CounterexampleReview] = Field(default_factory=list, max_length=20)
    core_defects: list[A5Defect] = Field(default_factory=list, max_length=8)
    improvement_proposals: list[A5Proposal] = Field(default_factory=list, max_length=3)
    data_collection_tasks: list[str] = Field(default_factory=list, max_length=8)
    unresolved_questions: list[A5UnresolvedQuestion] = Field(min_length=1, max_length=10)

    @model_validator(mode="after")
    def daily_report_never_authorizes_strategy_change(self) -> "A5ReviewReport":
        if self.sample_sufficient_for_strategy_change:
            raise ValueError("one daily review cannot authorize a production strategy change")
        return self


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
        "trigger_low": payload.get("trigger_low"),
        "trigger_high": payload.get("trigger_high"),
        "stop_level": payload.get("stop_level"),
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
            return {}, ["A2_AUDIT_OVERSIZE"]
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}, ["A2_AUDIT_UNREADABLE"]
    return (dict(value), []) if isinstance(value, Mapping) else ({}, ["A2_AUDIT_INVALID"])


def _a2_projection(audit: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    stages = _rows(audit.get("stages"))
    stage = next((item for item in stages if str(item.get("stage")).upper() == "A2"), None)
    if stage is None:
        return {"status": "NOT_AVAILABLE", "candidates": [], "themes": []}, ["A2_STAGE_NOT_FOUND"]
    output = _json_mapping(stage.get("output"))
    candidates: list[dict[str, Any]] = []
    for pool_name, values in (
        ("FOCUS", output.get("focus_pool")),
        ("WATCH", output.get("watch_only_pool")),
        ("REJECTED", output.get("rejected_candidates")),
    ):
        for item in _rows(values)[:300]:
            symbol = str(item.get("symbol") or "")
            candidates.append({
                "evidence_id": f"A2:{pool_name}:{symbol}",
                "pool": pool_name,
                "symbol": symbol,
                "name": item.get("name") or item.get("company_name"),
                "theme_id": item.get("theme_id"),
                "theme_name": item.get("theme_name"),
                "market_role": item.get("market_role"),
                "score": item.get("identifiability_score", item.get("score")),
                "selection_reasons": item.get("selection_reasons") if isinstance(item.get("selection_reasons"), list) else [],
                "risk_reasons": item.get("risk_reasons") if isinstance(item.get("risk_reasons"), list) else [],
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
    counts = {name: sum(item["pool"] == name for item in candidates) for name in ("FOCUS", "WATCH", "REJECTED")}
    return {
        "status": stage.get("status"),
        "reason_codes": stage.get("reason_codes") if isinstance(stage.get("reason_codes"), list) else [],
        "counts": counts,
        "themes": themes,
        "candidates": candidates,
    }, [] if output else ["A2_OUTPUT_MISSING"]


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
            result.append({
                "symbol": symbol,
                "name": item.get("name") or item.get("company_name"),
                "theme_id": item.get("primary_theme") or item.get("theme_id"),
                "theme_name": item.get("primary_theme_name") or item.get("theme_name"),
                "pool": pool_name,
                "selection_reasons": item.get("core_thesis") if isinstance(item.get("core_thesis"), list) else [],
                "risk_reasons": item.get("bear_case") if isinstance(item.get("bear_case"), list) else [],
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

    raw_plans = store.list_execution_plans(lane_id=lane_id)
    plans = []
    selected_plan_rows: list[dict[str, Any]] = []
    for row in raw_plans:
        valid_from = row.get("valid_from")
        expires_at = row.get("expires_at")
        payload = _json_mapping(row.get("payload_json"))
        target_day = str(payload.get("target_trade_date") or "")
        in_session = target_day == trade_date.isoformat()
        if not in_session:
            try:
                start = datetime.fromisoformat(str(valid_from)).date() if valid_from else None
                end = datetime.fromisoformat(str(expires_at)).date() if expires_at else None
                in_session = bool((start is None or start <= trade_date) and (end is None or trade_date <= end))
            except ValueError:
                in_session = False
        if in_session:
            plans.append(_plan_projection(row))
            selected_plan_rows.append(dict(row))

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
        missing.extend(reasons)
        market_universe = _a1_market_universe(audit)
    else:
        missing.append("A3_SOURCE_RUN_NOT_FOUND")

    events = []
    action_counts: dict[str, int] = {}
    effective_event_count = 0
    session_start = cutoff.replace(hour=9, minute=0, second=0, microsecond=0)
    raw_event_rows = store.list_monitor_events(
        lane_id=lane_id, effective_only=False, from_time=session_start, to_time=cutoff,
    )
    for row in raw_event_rows:
        action = str(row.get("action") or "UNKNOWN")
        action_counts[action] = action_counts.get(action, 0) + 1
        effective = bool(row.get("effective"))
        effective_event_count += int(effective)
        # Keep complete counts but only send consequential rows to A5.  A
        # per-plan NO_ACTION heartbeat can number in the thousands and carries
        # no additional causal evidence after aggregation.
        if not effective and action == "NO_ACTION":
            continue
        payload = _json_mapping(row.get("payload_json"))
        symbol = str(payload.get("symbol") or "")
        events.append({
            "evidence_id": f"A4:EVENT:{row.get('event_id')}",
            "event_id": row.get("event_id"),
            "minute_end": row.get("minute_end"),
            "plan_id": payload.get("plan_id"),
            "symbol": symbol,
            "action": row.get("action"),
            "effective": effective,
            "reason_code": row.get("reason_code"),
            "diagnostic_code": payload.get("diagnostic_code"),
            "strategy_profile": _json_mapping(payload.get("strategy")).get("strategy_profile"),
        })

    lifecycles = []
    for row in store.list_a4_signal_lifecycles(lane_id=lane_id, trade_date=trade_date.isoformat(), limit=1000):
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
    finished_returns = [float(item["net_return"]) for item in lifecycles if item.get("net_return") is not None]
    metrics = {
        "a2_focus_count": int(a2.get("counts", {}).get("FOCUS", 0)),
        "a2_watch_count": int(a2.get("counts", {}).get("WATCH", 0)),
        "a2_theme_count": len(a2.get("themes", [])),
        "a3_plan_count": len(plans),
        "a3_strategy_counts": _count_by(plans, "strategy_profile"),
        "a4_monitor_observation_count": sum(action_counts.values()),
        "a4_effective_event_count": effective_event_count,
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
        "a3": {"plans": plans},
        "a4": {"events": events, "lifecycles": lifecycles},
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
        snapshot["metrics"]["a5_independent_verification_status"] = str(
            independent.get("status") or "UNAVAILABLE"
        )
        independent_status = str(independent.get("status") or "UNAVAILABLE").upper()
        if independent_status != "READY":
            reason = (
                "A5_INDEPENDENT_VERIFICATION_DEGRADED"
                if independent_status == "DEGRADED"
                else "A5_INDEPENDENT_VERIFICATION_UNAVAILABLE"
            )
            snapshot["data_quality"]["status"] = "DEGRADED"
            snapshot["data_quality"]["missing_components"].append(reason)
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
    for group in (a2.get("themes"), a2.get("candidates"), a3.get("plans"), a4.get("events"), a4.get("lifecycles")):
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
    return values


def _validate_evidence(report: A5ReviewReport, snapshot: Mapping[str, Any]) -> None:
    allowed = _evidence_ids(snapshot)
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
        raise ValueError("A5_OUTPUT_EVIDENCE_INVALID")


def _markdown(report: A5ReviewReport, snapshot: Mapping[str, Any]) -> str:
    kind = "盘中复盘" if report.review_kind is A5ReviewKind.MIDDAY else "盘后复盘"
    metrics = _json_mapping(snapshot.get("metrics"))
    lines = [
        f"# A5 {kind}｜{report.trade_date.isoformat()}", "",
        "> 内部模拟复盘，不构成投资建议；A5 不修改生产策略，不产生或执行交易信号。", "",
        f"- 结论：**{report.overall_verdict}**",
        f"- 事实截止：`{snapshot.get('cutoff_at')}`",
        f"- A2 聚焦/观察：`{metrics.get('a2_focus_count', 0)}/{metrics.get('a2_watch_count', 0)}`",
        f"- A3 计划：`{metrics.get('a3_plan_count', 0)}`",
        f"- A4 有效事件/生命周期：`{metrics.get('a4_effective_event_count', 0)}/{metrics.get('a4_lifecycle_count', 0)}`",
        "", "## 总结", "", report.executive_summary, "",
    ]
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
            f"- 证伪标准：{item.falsification_criteria}", f"- 最少影子观察：{item.min_shadow_days} 个交易日", "",
        ])
    lines.extend(["## 尚不能下结论", ""])
    lines.extend([f"- {item.question}（{item.reason}）：{item.resolution}" for item in report.unresolved_questions])
    return "\n".join(lines).rstrip() + "\n"


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

    def run(self, *, review_kind: A5ReviewKind, now: datetime) -> dict[str, Any]:
        current = now.astimezone(SHANGHAI)
        cutoff_clock = (11, 30) if review_kind is A5ReviewKind.MIDDAY else (15, 0)
        cutoff = current.replace(hour=cutoff_clock[0], minute=cutoff_clock[1], second=0, microsecond=0)
        if current < cutoff:
            raise ValueError("A5_REVIEW_BEFORE_CUTOFF")
        facts = build_a5_fact_snapshot(
            self.store, self.output_dir, trade_date=current.date(), cutoff_at=cutoff,
            review_kind=review_kind, lane_id=self.lane_id,
            independent_verifier=self.independent_verifier,
        )
        existing = self.store.list_a5_reviews(
            trade_date=current.date().isoformat(), review_kind=review_kind.value, limit=20,
        )
        same = next((row for row in existing if row.get("input_hash") == facts["input_hash"]), None)
        if same is not None:
            return self._public_row(
                same,
                created=False,
                notifications=self._publish_notification(same, now=current),
            )

        prompt = self.prompts.render(_A5_PROMPT, {"A5_FACT_SNAPSHOT": facts})
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        result: ModelCallResult = self.model_client.complete(
            self.model,
            [{"role": "system", "content": prompt}],
            prompt_hash=prompt_hash,
            input_hash=str(facts["input_hash"]),
            stage="A5",
            timeout_seconds=300,
        )
        try:
            report = A5ReviewReport.model_validate(result.output)
        except ValidationError as exc:
            raise ValueError("A5_OUTPUT_SCHEMA_INVALID") from exc
        if report.review_kind is not review_kind or report.trade_date != current.date():
            raise ValueError("A5_OUTPUT_IDENTITY_MISMATCH")
        _validate_evidence(report, facts)

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
            notifications=self._publish_notification(row, now=current),
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
