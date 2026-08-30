"""Deterministic weekly strategy overlay for A1 and A2.

The monthly strategy defines the structural research domain.  This module
adds the missing weekly bridge between that slow-moving view and the daily A2
market-selection layer.  It never turns commentary, index targets or named
stock recommendations into facts.  Every state is derived from frozen
point-in-time metrics already present in the workflow.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
SCHEMA_VERSION = "weekly-strategy-context/1.0.0"


def build_weekly_strategy_context(
    snapshot: Mapping[str, Any],
    *,
    as_of: datetime | str,
    monthly_rotations: Sequence[Mapping[str, Any]] = (),
    policy_documents: Sequence[Mapping[str, Any]] = (),
    macro_asset_quadrant: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a bounded, evidence-only weekly overlay without stock picking."""

    cutoff = _aware(as_of)
    week_start = cutoff - timedelta(days=7)
    weekly_documents = [
        _bounded_policy_document(item)
        for item in policy_documents
        if isinstance(item, Mapping)
        and (published := _parse_time(item.get("publish_time") or item.get("event_time"))) is not None
        and week_start <= published <= cutoff
    ][:24]
    rotations = [
        _weekly_rotation_row(item, rank)
        for rank, item in enumerate(monthly_rotations, start=1)
        if isinstance(item, Mapping)
    ][:40]
    rotations = [item for item in rotations if item]

    quadrant = dict(macro_asset_quadrant or {})
    cross_market = snapshot.get("CROSS_MARKET_LEAD_SNAPSHOT")
    cross_market = _bounded_mapping(cross_market) if isinstance(cross_market, Mapping) else {}
    global_macro = snapshot.get("GLOBAL_MACRO_SNAPSHOT")
    global_macro = _bounded_mapping(global_macro) if isinstance(global_macro, Mapping) else {}
    ready_pillars = {
        "asset_regime": quadrant.get("status") in {"READY", "DEGRADED"},
        "weekly_policy_impulse": bool(weekly_documents),
        "weekly_industry_rotation": bool(rotations),
        "cross_market_confirmation": cross_market.get("available") is True,
    }
    ready_count = sum(ready_pillars.values())
    # A weekly overlay is enrichment, never an upstream hard gate.  One
    # observed pillar is still useful and must remain explicitly DEGRADED
    # instead of being mislabeled as a negative market conclusion.
    status = "READY" if ready_count >= 3 else "DEGRADED" if ready_count >= 1 else "BLOCKED"

    return {
        "schema_version": SCHEMA_VERSION,
        "as_of": cutoff.isoformat(),
        "status": status,
        "research_horizon": {
            "cadence": "WEEKLY",
            "policy_impulse_lookback_days": 7,
            "rotation_windows_trading_days": [5, 10, 20],
            "event_anticipation_window_trading_days": [20, 60],
        },
        "pillar_availability": ready_pillars,
        "asset_regime": quadrant,
        "global_macro_confirmation": global_macro,
        "weekly_policy_impulse": {
            "window_start": week_start.isoformat(),
            "window_end": cutoff.isoformat(),
            "document_count": len(weekly_documents),
            "official_documents": weekly_documents,
        },
        "industry_rotation": rotations,
        "cross_market_confirmation": cross_market,
        "decision_rules": {
            "monthly_direction_is_prior_not_weekly_answer": True,
            "weekly_change_must_compare_5d_10d_20d": True,
            "expected_event_requires_date_and_lead_lag_evidence": True,
            "orders_and_shipments_are_separate_realization_stages": True,
            "spot_asset_and_equity_proxy_must_not_be_conflated": True,
            "supply_demand_and_macro_pricing_must_not_be_conflated": True,
            "cross_market_mapping_requires_domestic_confirmation": True,
            "single_week_move_cannot_prove_structural_cycle": True,
        },
        "prohibited_claims": [
            "SUBJECTIVE_INDEX_TARGET_AS_DETERMINISTIC_SIGNAL",
            "SEASONAL_RHYTHM_WITHOUT_BACKTEST_AS_FACT",
            "NAMED_STOCK_RECOMMENDATION_AS_PRIMARY_EVIDENCE",
            "VALUATION_FORECAST_WITHOUT_AS_OF_AND_SOURCE",
            "ORDER_DEMAND_AS_REALIZED_REVENUE",
            "COMMODITY_PRICE_MOVE_AS_EQUITY_PROXY_RETURN",
        ],
    }


def weekly_rotation_state(value: Mapping[str, Any]) -> str:
    """Classify short/medium-term rotation without inventing missing values."""

    r5 = _number(value.get("return_5d"))
    r10 = _number(value.get("return_10d"))
    r20 = _number(value.get("return_20d"))
    if r5 is None or r20 is None:
        return "UNKNOWN"
    if r20 <= 0 < r5:
        return "EARLY_REVERSAL"
    if r20 > 0 and r5 <= 0:
        return "COOLING"
    if r20 <= 0 and r5 <= 0:
        return "WEAK"
    if r10 is not None and r5 / 5.0 > r10 / 10.0 > r20 / 20.0 > 0:
        return "ACCELERATING"
    if r5 > 0 and r20 > 0:
        return "PERSISTENT"
    return "MIXED"


def _weekly_rotation_row(value: Mapping[str, Any], rank: int) -> dict[str, Any]:
    code = str(value.get("industry_thscode") or "").strip().upper()
    name = str(value.get("industry_name") or "").strip()
    if not code and not name:
        return {}
    refs = _source_refs(value)
    return {
        "rank": rank,
        "industry_thscode": code or None,
        "industry_name": name or None,
        "weekly_state": weekly_rotation_state(value),
        "return_5d": _number(value.get("return_5d")),
        "return_10d": _number(value.get("return_10d")),
        "return_20d": _number(value.get("return_20d")),
        "relative_strength_percentile_20d": _number(value.get("relative_strength_percentile_20d")),
        "top10_appearance_count": _number(value.get("top10_appearance_count")),
        "turnover_persistence_ratio": _number(value.get("turnover_persistence_ratio")),
        "source_refs": refs,
        "evidence_state": "OBSERVED" if refs else "SOURCE_REF_MISSING",
    }


def _bounded_policy_document(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value.get(key)
        for key in (
            "fact_id", "title", "issuing_body", "official_category",
            "publish_time", "event_time", "source_url", "formal_document",
            "financial_transmission_evidence",
        )
        if key in value
    }


def _bounded_mapping(value: Mapping[str, Any], limit: int = 36) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in list(value.items())[:limit]:
        if isinstance(item, list):
            result[str(key)] = item[:limit]
        elif isinstance(item, Mapping):
            result[str(key)] = dict(list(item.items())[:limit])
        else:
            result[str(key)] = item
    return result


def _source_refs(value: Mapping[str, Any]) -> list[str]:
    refs: list[str] = []
    for key in ("source_ref", "source_url", "fact_id"):
        raw = value.get(key)
        if isinstance(raw, str) and raw.strip():
            refs.append(raw.strip())
    raw_refs = value.get("source_refs")
    if isinstance(raw_refs, Sequence) and not isinstance(raw_refs, (str, bytes, bytearray)):
        refs.extend(str(item).strip() for item in raw_refs if str(item).strip())
    return list(dict.fromkeys(refs))


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace("%", "").replace(",", ""))
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(SHANGHAI)


def _aware(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else _parse_time(value)
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("weekly strategy as_of must be timezone-aware")
    return parsed.astimezone(SHANGHAI)


__all__ = ["SCHEMA_VERSION", "build_weekly_strategy_context", "weekly_rotation_state"]
