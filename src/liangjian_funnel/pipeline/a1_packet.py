"""Compact, complete and auditable input projection for A1 discovery.

The persisted snapshot remains full fidelity.  This module creates the
*model view* only: every configured industry and every server-owned monthly
decision is present, while long raw macro/industry history is converted into
deterministic sufficient statistics.  No stock universe or historical fact is
deleted by this projection.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from .a1_contract import (
    A1_CONTRACT_VERSION,
    A1_MONTHLY_DECISION_COUNT,
    canonical_json,
    canonicalize_monthly_decisions,
    stable_digest,
)


A1_RESEARCH_PACKET_SCHEMA_VERSION = "a1-research-packet/1.0.0"
A1_PACKET_TOKEN_BUDGET = 100_000
_MACRO_WINDOWS = (1, 3, 6, 12)
_INDUSTRY_METRIC_KEYS = (
    "return_5d",
    "return_10d",
    "return_20d",
    "return_60d",
    "relative_strength_percentile_20d",
    "top10_appearance_count",
    "top10_appearance_rate",
    "recent_turnover",
    "turnover_persistence_ratio",
)
_SOURCE_KEYS = ("source_ref", "source_refs", "source_url", "fact_id", "content_hash")


class A1PacketSizeError(ValueError):
    """Raised only when a caller explicitly asks for a budget assertion."""

    def __init__(self, diagnostics: Mapping[str, Any]):
        self.reason_code = "A1_PACKET_TOO_LARGE"
        self.diagnostics = dict(diagnostics)
        super().__init__(self.reason_code)


def build_a1_research_packet(
    snapshot: Mapping[str, Any] | Any,
    *,
    as_of: datetime | str | None = None,
    snapshot_id: str | None = None,
    snapshot_hash: str | None = None,
    monthly_strategy_context: Mapping[str, Any] | None = None,
    prior_theme_registry: Mapping[str, Any] | None = None,
    policy_document_limit: int = 60,
    max_estimated_tokens: int | None = None,
    raise_on_budget: bool = False,
) -> dict[str, Any]:
    """Build a stable A1 model packet from a frozen full-fidelity snapshot.

    ``snapshot`` may be a mapping or a ``FrozenInputSnapshot``-like object.
    The function does not mutate it.  If no pre-built monthly context is
    supplied, the existing deterministic monthly strategy builder is imported
    lazily and used to produce the canonical twenty decisions.
    """

    data, object_id, object_hash, object_as_of = _snapshot_parts(snapshot)
    effective_id = str(snapshot_id or object_id or data.get("snapshot_id") or "UNKNOWN")
    effective_hash = str(snapshot_hash or object_hash or data.get("snapshot_hash") or stable_digest(data))
    effective_as_of = as_of if as_of is not None else object_as_of or data.get("as_of")
    context = monthly_strategy_context
    if not isinstance(context, Mapping):
        context = _build_monthly_context(data, effective_as_of, prior_theme_registry, policy_document_limit)
    context = dict(context or {})

    decisions, decision_coverage = canonicalize_monthly_decisions(
        context.get("monthly_industry_decisions")
        if isinstance(context.get("monthly_industry_decisions"), list)
        else (),
        expected_count=_safe_int(
            context.get("monthly_rotation_coverage", {}).get("requested_top_n")
            if isinstance(context.get("monthly_rotation_coverage"), Mapping)
            else A1_MONTHLY_DECISION_COUNT,
            A1_MONTHLY_DECISION_COUNT,
        ),
    )
    policy_dossiers = _project_policy_documents(context, policy_document_limit)
    macro_features = _project_macro_features(data.get("MACRO_ECONOMIC_DATA"))
    industry_features = _project_industry_features(data, context)
    cross_market = _project_bounded(data.get("CROSS_MARKET_LEAD_SNAPSHOT"), 32)
    if not cross_market:
        cross_market = _project_bounded(context.get("cross_market_leads"), 32)
    broker = _project_bounded(data.get("BROKER_RESEARCH_CONSENSUS"), 24)
    if not broker:
        broker = _project_bounded(context.get("broker_research_consensus"), 24)
    prior = _project_prior_registry(
        prior_theme_registry
        if isinstance(prior_theme_registry, Mapping)
        else context.get("prior_theme_registry")
    )
    packet: dict[str, Any] = {
        "schema_version": A1_RESEARCH_PACKET_SCHEMA_VERSION,
        "contract_version": A1_CONTRACT_VERSION,
        "snapshot_id": effective_id,
        "snapshot_hash": effective_hash,
        "as_of": _text(effective_as_of),
        "quality_summary": _quality_summary(data, context),
        "macro_asset_quadrant": _project_bounded(
            context.get("macro_asset_quadrant") or data.get("ASSET_ROTATION_SNAPSHOT"),
            24,
        ),
        "macro_features": macro_features,
        "policy_dossiers": policy_dossiers,
        "cross_market_leads": cross_market,
        "broker_research_consensus": broker,
        "industry_features": industry_features,
        "canonical_monthly_decisions": decisions,
        "prior_theme_registry": prior,
        "source_index": {},
        "coverage": {
            "macro_feature_count": len(macro_features),
            "policy_document_count": len(policy_dossiers),
            "cross_market_lead_count": _container_count(cross_market),
            "industry_count": len(industry_features),
            "canonical_decision_count": len(decisions),
            "canonical_decision_requested": decision_coverage.get("requested_top_n", A1_MONTHLY_DECISION_COUNT),
            "canonical_decision_status": decision_coverage.get("status", "INCOMPLETE"),
            "full_snapshot_retained": True,
            "raw_history_projected_out": True,
        },
    }
    packet["source_index"] = _build_source_index(packet)
    body_hash = stable_digest(packet)
    packet["packet_hash"] = body_hash
    limit = A1_PACKET_TOKEN_BUDGET if max_estimated_tokens is None else max(1, int(max_estimated_tokens))
    # Compute the published diagnostics after adding the budget metadata so a
    # consumer comparing ``packet_diagnostics(packet)`` with the embedded
    # value observes the same section sizes.  The budget is a tiny part of the
    # packet, so iterating to a fixed point avoids a self-referential one-token
    # drift when the number of digits in the estimate changes.
    estimate = 0
    diagnostics = packet_diagnostics(packet)
    for _ in range(4):
        packet["coverage"]["budget"] = {
            "estimated_input_tokens": estimate,
            "input_token_limit": limit,
            "within_budget": estimate <= limit,
        }
        diagnostics = packet_diagnostics(packet)
        next_estimate = diagnostics["estimated_input_tokens"]
        if next_estimate == estimate:
            break
        estimate = next_estimate
    packet["coverage"]["budget"] = {
        "estimated_input_tokens": diagnostics["estimated_input_tokens"],
        "input_token_limit": limit,
        "within_budget": diagnostics["estimated_input_tokens"] <= limit,
    }
    diagnostics = packet_diagnostics(packet)
    packet["diagnostics"] = diagnostics
    if raise_on_budget and diagnostics["estimated_input_tokens"] > limit:
        raise A1PacketSizeError({**diagnostics, "input_token_limit": limit})
    return packet


def assert_packet_budget(packet: Mapping[str, Any], max_estimated_tokens: int = A1_PACKET_TOKEN_BUDGET) -> dict[str, Any]:
    """Return diagnostics or fail closed with ``A1_PACKET_TOO_LARGE``."""

    diagnostics = packet_diagnostics(packet)
    limit = max(1, int(max_estimated_tokens))
    if diagnostics["estimated_input_tokens"] > limit:
        raise A1PacketSizeError({**diagnostics, "input_token_limit": limit})
    return {**diagnostics, "input_token_limit": limit, "within_budget": True}


def packet_diagnostics(packet: Mapping[str, Any]) -> dict[str, Any]:
    """Expose section sizes without exposing source content in logs."""

    sections = {
        str(key): len(canonical_json(value))
        for key, value in packet.items()
        if str(key) not in {"packet_hash", "diagnostics"}
    }
    total_chars = len(canonical_json({key: value for key, value in packet.items() if key != "diagnostics"}))
    estimated = _estimate_tokens(canonical_json({key: value for key, value in packet.items() if key != "diagnostics"}))
    largest = sorted(sections.items(), key=lambda item: (-item[1], item[0]))[:12]
    return {
        "packet_chars": total_chars,
        "estimated_input_tokens": estimated,
        "section_chars": sections,
        "largest_sections": [{"section": key, "chars": chars} for key, chars in largest],
        "raw_history_keys_excluded": ["MACRO_ECONOMIC_DATA.series", "INDUSTRY_ACTIVITY_DATA.items"],
        "full_snapshot_retained": True,
    }


def project_macro_features(value: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """Public wrapper for deterministic macro sufficient statistics."""

    return _project_macro_features(value)


def project_industry_features(
    snapshot: Mapping[str, Any],
    context: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Public wrapper; all catalog industries are retained."""

    return _project_industry_features(snapshot, context or {})


def _project_macro_features(value: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    raw_rows = value.get("series")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if isinstance(raw_rows, list):
        for index, raw in enumerate(raw_rows):
            if not isinstance(raw, Mapping):
                continue
            name = _metric_name(raw, index)
            item = _macro_observation(raw, index)
            if item is not None:
                grouped[name].append(item)
    latest = value.get("latest")
    if isinstance(latest, Mapping):
        for raw_name, raw in latest.items():
            name = str(raw_name).strip().upper()
            if not name or name in grouped or not isinstance(raw, Mapping):
                continue
            item = _macro_observation(raw, 0)
            if item is not None:
                grouped[name].append(item)
    values = value.get("values")
    if isinstance(values, Mapping):
        for raw_name, raw_value in values.items():
            name = str(raw_name).strip().upper()
            if not name or name in grouped or isinstance(raw_value, (Mapping, list, tuple)):
                continue
            number = _number(raw_value)
            if number is not None:
                grouped[name].append({"value": number, "observation_date": None, "publish_time": None, "source_refs": []})
    quality = value.get("quality") if isinstance(value.get("quality"), Mapping) else {}
    result: list[dict[str, Any]] = []
    for name in sorted(grouped):
        rows = sorted(grouped[name], key=lambda item: (_date_key(item.get("observation_date")), str(item.get("publish_time") or "")))
        latest_row = rows[-1]
        values_only = [_number(item.get("value")) for item in rows]
        values_only = [item for item in values_only if item is not None]
        changes = {
            f"change_{window}m": (
                values_only[-1] - values_only[-1 - window]
                if len(values_only) > window
                else None
            )
            for window in _MACRO_WINDOWS
        }
        percent_changes = {
            f"change_{window}m_pct": _pct_change(values_only[-1], values_only[-1 - window])
            if len(values_only) > window else None
            for window in _MACRO_WINDOWS
        }
        feature = {
            "series_name": name,
            "latest_observation": latest_row,
            "observation_count": len(rows),
            "changes": {**changes, **percent_changes},
            "trend": _trend(values_only),
            "percentile": _percentile(values_only),
            "source_refs": sorted({ref for row in rows for ref in row.get("source_refs", ())}),
            "quality": _project_quality(quality.get(name) if isinstance(quality, Mapping) else None, value),
        }
        result.append(feature)
    return result


def _project_industry_features(snapshot: Mapping[str, Any], context: Mapping[str, Any]) -> list[dict[str, Any]]:
    rotations = context.get("monthly_industry_rotation")
    if not isinstance(rotations, list):
        cycle = snapshot.get("SECTOR_CYCLE_SNAPSHOT")
        history = cycle.get("history_metrics") if isinstance(cycle, Mapping) else None
        rotations = history.get("monthly_rotation_candidates") if isinstance(history, Mapping) else []
    rotation_by_key: dict[str, dict[str, Any]] = {}
    for rank, raw in enumerate(rotations or (), start=1):
        if not isinstance(raw, Mapping):
            continue
        key = _industry_key(raw)
        if key and key not in rotation_by_key:
            rotation_by_key[key] = {**_industry_metrics(raw), "rank": rank}

    catalog = snapshot.get("THS_INDUSTRY_CATALOG")
    catalog_rows = catalog.get("records") if isinstance(catalog, Mapping) else None
    merged: dict[str, dict[str, Any]] = {}
    if isinstance(catalog_rows, list):
        for raw in catalog_rows:
            if not isinstance(raw, Mapping):
                continue
            code = _industry_code(raw)
            name = _industry_name(raw)
            key = code or name
            if not key:
                continue
            merged[key] = {
                "industry_thscode": code or None,
                "industry_name": name or None,
                "rank": None,
                "metrics": {},
                "activity": {},
                "profit": {},
                "source_refs": _source_refs(raw),
                "quality": "CATALOG",
            }
    # Rotations are part of the authoritative sector-cycle view.  Add any
    # code not found in the catalog rather than silently dropping it.
    for key, raw in rotation_by_key.items():
        row = merged.setdefault(key, {
            "industry_thscode": raw.get("industry_thscode") or key,
            "industry_name": raw.get("industry_name"),
            "rank": None,
            "metrics": {},
            "activity": {},
            "profit": {},
            "source_refs": [],
            "quality": "ROTATION_ONLY",
        })
        row["rank"] = raw.get("rank")
        row["metrics"].update({key: raw.get(key) for key in _INDUSTRY_METRIC_KEYS if key in raw})
        row["source_refs"] = sorted(set(row.get("source_refs", ())) | set(_source_refs(raw)))

    activity = snapshot.get("INDUSTRY_ACTIVITY_DATA")
    _merge_industry_observations(merged, activity, "activity")
    profit = snapshot.get("INDUSTRY_PROFIT_DATA")
    _merge_industry_observations(merged, profit, "profit")
    result = []
    for key, row in sorted(merged.items(), key=lambda item: (_safe_int(item[1].get("rank"), 10_000), str(item[0]))):
        row["source_refs"] = sorted(set(row.get("source_refs", ())))
        row["data_quality"] = _industry_quality(row, activity, profit)
        result.append(row)
    return result


def _merge_industry_observations(target: dict[str, dict[str, Any]], value: Any, field: str) -> None:
    if not isinstance(value, Mapping):
        return
    raw_rows = value.get("items") or value.get("records") or value.get("data")
    if not isinstance(raw_rows, list):
        return
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            continue
        key = _industry_key(raw)
        if key:
            grouped[key].append(raw)
    for key, rows in grouped.items():
        target_key = key if key in target else _find_industry_by_name_or_code(target, key, rows)
        if not target_key:
            target_key = key
            target[target_key] = {
                "industry_thscode": _industry_code(rows[-1]),
                "industry_name": _industry_name(rows[-1]) or key,
                "rank": None,
                "metrics": {},
                "activity": {},
                "profit": {},
                "source_refs": [],
                "quality": "DATA_ONLY",
            }
        ordered = sorted(rows, key=lambda item: (_date_key(item.get("observation_date") or item.get("date") or item.get("period")), str(item.get("publish_time") or "")))
        latest = ordered[-1]
        numbers = []
        for raw in ordered:
            number = _first_number(raw, ("value", "yoy", "growth", "profit", "revenue", "amount"))
            if number is not None:
                numbers.append(number)
        target[target_key][field] = {
            "latest": _bounded_scalar_mapping(latest),
            "observation_count": len(ordered),
            "trend": _trend(numbers),
            "change_3m": numbers[-1] - numbers[-4] if len(numbers) >= 4 else None,
            "change_12m": numbers[-1] - numbers[-13] if len(numbers) >= 13 else None,
        }
        target[target_key]["source_refs"] = sorted(set(target[target_key].get("source_refs", ())) | set(_source_refs(value)) | {ref for raw in rows for ref in _source_refs(raw)})


def _project_policy_documents(context: Mapping[str, Any], limit: int) -> list[dict[str, Any]]:
    window = context.get("policy_window") if isinstance(context.get("policy_window"), Mapping) else {}
    documents = window.get("official_documents")
    if not isinstance(documents, list):
        documents = context.get("official_documents") if isinstance(context.get("official_documents"), list) else []
    selected: list[dict[str, Any]] = []
    cap = max(1, min(int(limit), 120))
    for raw in documents[:cap]:
        if not isinstance(raw, Mapping) or raw.get("prompt_injection_suspected") is True:
            continue
        row = {
            key: raw.get(key)
            for key in (
                "fact_id", "title", "summary", "issuing_body", "document_number",
                "official_category", "publish_time", "event_time", "source_url",
                "formal_document", "financial_transmission_evidence",
            )
            if key in raw
        }
        if isinstance(row.get("summary"), str):
            row["summary"] = row["summary"][:1_200]
        row["source_refs"] = _source_refs(raw)
        selected.append(row)
    return selected


def _project_prior_registry(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {"available": False, "themes": [], "nodes": []}
    themes = value.get("themes") if isinstance(value.get("themes"), list) else []
    nodes = value.get("nodes") if isinstance(value.get("nodes"), list) else []
    return {
        "available": bool(themes),
        "as_of": value.get("as_of"),
        "version_hash": value.get("version_hash"),
        "themes": [_bounded_scalar_mapping(item) for item in themes[:24] if isinstance(item, Mapping)],
        "nodes": [_bounded_scalar_mapping(item) for item in nodes[:100] if isinstance(item, Mapping)],
    }


def _quality_summary(data: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "monthly_context_status": context.get("status"),
        "missing_pillars": list(context.get("missing_pillars", ())) if isinstance(context.get("missing_pillars"), list) else [],
        "g0_symbol_count": _container_count(data.get("g0_symbols") or data.get("g0") or data.get("g0_candidates")),
    }
    for name in ("MACRO_ECONOMIC_DATA", "INDUSTRY_ACTIVITY_DATA", "INDUSTRY_PROFIT_DATA", "MACRO_POLICY_FEED"):
        value = data.get(name)
        if isinstance(value, Mapping):
            result[name] = {
                "available": value.get("available"),
                "reason_code": value.get("reason_code"),
                "quality_tier": value.get("quality_tier") or value.get("quality", {}).get("tier") if isinstance(value.get("quality"), Mapping) else value.get("quality_tier"),
                "pit_verified": value.get("pit_verified"),
                "publish_time_available": value.get("publish_time_available"),
            }
    return result


def _build_source_index(packet: Mapping[str, Any]) -> dict[str, Any]:
    refs: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key) in _SOURCE_KEYS:
                    if isinstance(item, str) and item.strip():
                        refs.add(item.strip())
                    elif isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
                        refs.update(str(ref).strip() for ref in item if str(ref).strip())
                elif str(key) not in {"source_index", "packet_hash", "diagnostics"}:
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(packet)
    return {
        ref: {"source_ref": ref, "in_packet": True}
        for ref in sorted(refs)
    }


def _snapshot_parts(snapshot: Mapping[str, Any] | Any) -> tuple[dict[str, Any], Any, Any, Any]:
    if isinstance(snapshot, Mapping):
        nested = snapshot.get("data")
        data = dict(nested) if isinstance(nested, Mapping) else dict(snapshot)
        return (
            data,
            snapshot.get("snapshot_id") or data.get("snapshot_id"),
            snapshot.get("snapshot_hash") or data.get("snapshot_hash"),
            snapshot.get("as_of") or data.get("as_of"),
        )
    raw_data = getattr(snapshot, "data", {})
    data = dict(raw_data) if isinstance(raw_data, Mapping) else {}
    return data, getattr(snapshot, "snapshot_id", None), getattr(snapshot, "snapshot_hash", None), getattr(snapshot, "as_of", None)


def _build_monthly_context(
    data: Mapping[str, Any],
    as_of: datetime | str | None,
    prior_registry: Mapping[str, Any] | None,
    policy_document_limit: int,
) -> Mapping[str, Any]:
    if as_of is None:
        as_of = data.get("as_of")
    try:
        from .monthly_strategy import build_monthly_strategy_context

        return build_monthly_strategy_context(
            data,
            as_of=as_of,
            prior_registry=prior_registry,
            policy_document_limit=policy_document_limit,
        )
    except (TypeError, ValueError, KeyError):
        return {}


def _macro_observation(raw: Mapping[str, Any], index: int) -> dict[str, Any] | None:
    value = _first_number(raw, ("value", "val", "amount", "yoy", "growth"))
    if value is None:
        return None
    refs = _source_refs(raw)
    return {
        "value": value,
        "observation_date": _text(raw.get("observation_date") or raw.get("date") or raw.get("period") or raw.get("time")) or None,
        "publish_time": _text(raw.get("publish_time") or raw.get("published_at")) or None,
        "source_refs": refs,
        "row_index": index,
    }


def _metric_name(raw: Mapping[str, Any], index: int) -> str:
    return str(raw.get("id") or raw.get("series") or raw.get("series_id") or raw.get("name") or "SERIES_%04d" % index).strip().upper()


def _industry_metrics(raw: Mapping[str, Any]) -> dict[str, Any]:
    result = {key: raw.get(key) for key in _INDUSTRY_METRIC_KEYS if key in raw}
    result["industry_thscode"] = _industry_code(raw) or None
    result["industry_name"] = _industry_name(raw) or None
    result["source_refs"] = _source_refs(raw)
    return result


def _industry_code(raw: Mapping[str, Any]) -> str | None:
    value = raw.get("industry_thscode") or raw.get("thscode") or raw.get("industry_code") or raw.get("code")
    text = str(value or "").strip().upper()
    return text or None


def _industry_name(raw: Mapping[str, Any]) -> str | None:
    value = raw.get("industry_name") or raw.get("name") or raw.get("industry") or raw.get("sector")
    text = str(value or "").strip()
    return text or None


def _industry_key(raw: Mapping[str, Any]) -> str:
    return _industry_code(raw) or _industry_name(raw) or ""


def _find_industry_by_name_or_code(target: Mapping[str, Mapping[str, Any]], key: str, rows: Sequence[Mapping[str, Any]]) -> str | None:
    code = _industry_code(rows[-1]) if rows else None
    name = _industry_name(rows[-1]) if rows else None
    for target_key, row in target.items():
        if code and row.get("industry_thscode") == code:
            return target_key
        if name and row.get("industry_name") == name:
            return target_key
    return None


def _industry_quality(row: Mapping[str, Any], activity: Any, profit: Any) -> str:
    if row.get("rank") is not None and (row.get("activity") or row.get("profit")):
        return "ROTATION_AND_OPERATING_DATA"
    if row.get("rank") is not None:
        return "ROTATION_ONLY"
    if row.get("activity") or row.get("profit"):
        return "OPERATING_DATA_ONLY"
    return "CATALOG_ONLY"


def _project_quality(raw: Any, parent: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return {
            "tier": raw.get("tier") or raw.get("quality_tier") or parent.get("quality_tier"),
            "pit_verified": raw.get("pit_verified", parent.get("pit_verified")),
            "publish_time_available": raw.get("publish_time_available", parent.get("publish_time_available")),
            "reason_code": raw.get("reason_code") or parent.get("reason_code"),
        }
    return {
        "tier": parent.get("quality_tier"),
        "pit_verified": parent.get("pit_verified"),
        "publish_time_available": parent.get("publish_time_available"),
        "reason_code": parent.get("reason_code"),
    }


def _bounded_scalar_mapping(raw: Any, max_string: int = 1_200) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    result: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            result[str(key)] = value if not isinstance(value, str) else value[:max_string]
    return result


def _project_bounded(value: Any, limit: int) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _project_bounded(item, limit)
            for key, item in list(value.items())[: max(1, int(limit))]
            if str(key) not in {"series", "items", "records", "by_symbol"}
        }
    if isinstance(value, list):
        return [_project_bounded(item, limit) for item in value[: max(1, int(limit))]]
    if isinstance(value, str):
        return value[:1_200]
    return value


def _source_refs(value: Any) -> list[str]:
    refs: list[str] = []
    if isinstance(value, Mapping):
        for key in _SOURCE_KEYS:
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                refs.append(item.strip())
            elif isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
                refs.extend(str(ref).strip() for ref in item if str(ref).strip())
    return list(dict.fromkeys(refs))


def _first_number(value: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    for key in keys:
        number = _number(value.get(key))
        if number is not None:
            return number
    return None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _pct_change(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return (current - previous) / abs(previous)


def _trend(values: Sequence[float]) -> str:
    if len(values) < 2:
        return "INSUFFICIENT"
    delta = values[-1] - values[0]
    if abs(delta) < max(1e-9, abs(values[0]) * 0.01):
        return "FLAT"
    return "RISING" if delta > 0 else "FALLING"


def _percentile(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    latest = values[-1]
    below = sum(value <= latest for value in ordered)
    return round((below - 1) / max(1, len(ordered) - 1), 6)


def _date_key(value: Any) -> str:
    return str(value or "")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _container_count(value: Any) -> int:
    return len(value) if isinstance(value, (Mapping, Sequence)) and not isinstance(value, (str, bytes, bytearray)) else 0


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _estimate_tokens(text: str) -> int:
    # Keep the estimate conservative for mixed Chinese/ASCII JSON while being
    # deterministic and inexpensive.  It is a gate, not provider billing.
    ascii_chars = sum(ord(char) < 128 for char in text)
    non_ascii_chars = len(text) - ascii_chars
    return max(1, math.ceil(ascii_chars / 4 + non_ascii_chars / 1.5) + 8)


__all__ = [
    "A1_PACKET_TOKEN_BUDGET",
    "A1_RESEARCH_PACKET_SCHEMA_VERSION",
    "A1PacketSizeError",
    "assert_packet_budget",
    "build_a1_research_packet",
    "packet_diagnostics",
    "project_industry_features",
    "project_macro_features",
]
