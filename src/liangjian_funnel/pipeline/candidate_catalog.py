"""Canonical stock metadata projection for A1/A2/A3 result rows.

Model output is not authoritative for a security's name or taxonomy.  This
module joins immutable snapshot metadata by symbol and records mapping gaps
explicitly so the API never depends on a model repeating catalog fields.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any


CATALOG_SCHEMA = "candidate-catalog/1.0.0"
_POOL_KEYS = (
    "active_research_pool",
    "monitor_pool",
    "focus_pool",
    "watch_only_pool",
    "core_watch_pool",
    "secondary_watch_pool",
    "rejected_candidates",
)


def enrich_candidate_metadata(
    output: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a result copy with deterministic name/taxonomy metadata."""

    result = dict(output)
    catalog = build_candidate_catalog(snapshot)
    observed = 0
    named = 0
    mapped = 0
    missing_symbols: list[str] = []
    for key in _POOL_KEYS:
        raw = result.get(key)
        if not isinstance(raw, list):
            continue
        enriched: list[Any] = []
        for item in raw:
            if not isinstance(item, Mapping):
                enriched.append(item)
                continue
            row = dict(item)
            symbol = _symbol(row)
            if not symbol:
                enriched.append(row)
                continue
            observed += 1
            metadata = catalog.get(symbol)
            if metadata is None:
                missing_symbols.append(symbol)
                reasons = list(row.get("reason_codes") or ())
                if "CANDIDATE_CATALOG_MISSING" not in reasons:
                    reasons.append("CANDIDATE_CATALOG_MISSING")
                row["reason_codes"] = reasons
                enriched.append(row)
                continue
            row["symbol"] = symbol
            if not row.get("company_name") and not row.get("name"):
                row["company_name"] = metadata.get("company_name")
            if row.get("company_name") or row.get("name"):
                named += 1
            if not row.get("ths_industries"):
                row["ths_industries"] = list(metadata.get("ths_industries") or ())
            if not row.get("ths_concepts"):
                row["ths_concepts"] = list(metadata.get("ths_concepts") or ())
            if row.get("ths_industries") or row.get("ths_concepts"):
                mapped += 1
            else:
                reasons = list(row.get("reason_codes") or ())
                if "CANDIDATE_TAXONOMY_MAPPING_GAP" not in reasons:
                    reasons.append("CANDIDATE_TAXONOMY_MAPPING_GAP")
                row["reason_codes"] = reasons
            enriched.append(row)
        result[key] = enriched
    result["candidate_metadata_coverage"] = {
        "schema_version": CATALOG_SCHEMA,
        "row_count": observed,
        "name_count": named,
        "name_coverage": round(named / observed, 6) if observed else 1.0,
        "taxonomy_count": mapped,
        "taxonomy_coverage": round(mapped / observed, 6) if observed else 1.0,
        "catalog_missing_count": len(set(missing_symbols)),
        "catalog_missing_symbols": sorted(set(missing_symbols))[:100],
    }
    return result


def build_candidate_catalog(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    for key in ("g0_candidates", "universe_candidates", "trade_candidates"):
        values = snapshot.get(key)
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
            continue
        for raw in values:
            if not isinstance(raw, Mapping) or not (symbol := _symbol(raw)):
                continue
            current = candidates.setdefault(symbol, {"symbol": symbol})
            name = raw.get("name") or raw.get("company_name") or raw.get("sec_name")
            if name and not current.get("company_name"):
                current["company_name"] = str(name).strip()

    industries = _membership(snapshot.get("THS_INDUSTRY_MEMBERSHIP"), kind="INDUSTRY")
    concepts = _membership(snapshot.get("THS_CONCEPT_MEMBERSHIP"), kind="CONCEPT")
    for symbol in set(candidates).union(industries).union(concepts):
        current = candidates.setdefault(symbol, {"symbol": symbol})
        current["ths_industries"] = list(industries.get(symbol, ()))
        current["ths_concepts"] = list(concepts.get(symbol, ()))
    return candidates


def _membership(value: Any, *, kind: str) -> dict[str, tuple[dict[str, str], ...]]:
    records = value.get("records") if isinstance(value, Mapping) else None
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
        return {}
    result: dict[str, list[dict[str, str]]] = defaultdict(list)
    for raw in records:
        if not isinstance(raw, Mapping) or not (symbol := _symbol(raw)):
            continue
        memberships = raw.get("memberships")
        if not isinstance(memberships, Sequence) or isinstance(memberships, (str, bytes, bytearray)):
            continue
        for item in memberships:
            if not isinstance(item, Mapping):
                continue
            if kind == "INDUSTRY":
                code = item.get("industry_thscode") or item.get("taxonomy_code")
                name = item.get("industry_name") or item.get("taxonomy_name")
                projected = {"industry_thscode": str(code or ""), "industry_name": str(name or "")}
            else:
                code = item.get("concept_thscode") or item.get("taxonomy_code")
                name = item.get("concept_name") or item.get("taxonomy_name")
                projected = {"concept_thscode": str(code or ""), "concept_name": str(name or "")}
            if code and projected not in result[symbol]:
                result[symbol].append(projected)
    return {symbol: tuple(rows) for symbol, rows in result.items()}


def _symbol(value: Mapping[str, Any] | Any) -> str:
    raw = (
        value.get("symbol") or value.get("thscode") or value.get("ts_code") or value.get("code")
        if isinstance(value, Mapping)
        else value
    )
    text = str(raw or "").strip().upper()
    if len(text) == 6 and text.isdigit():
        suffix = "SH" if text.startswith(("5", "6", "9")) else "BJ" if text.startswith(("4", "8")) else "SZ"
        return f"{text}.{suffix}"
    return text if len(text) == 9 and text[6] == "." else ""


__all__ = ["CATALOG_SCHEMA", "build_candidate_catalog", "enrich_candidate_metadata"]
