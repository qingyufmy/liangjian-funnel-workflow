"""Generic, versioned THS industry/concept membership collector."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from ..facts.contracts import canonical_json_bytes
from ..pipeline.data_source import HithinkFetchResult, HithinkRow
from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")
_ENDPOINT = "/api/a-share-index/constituents/ths-stock-list"
_CACHE_SCHEMA = "liangjian-ths-taxonomy-cache/2.0.0"
Taxonomy = Literal["industry", "concept"]


def collect_ths_taxonomy_membership(
    client: Any,
    catalog: HithinkFetchResult,
    symbols: Sequence[str],
    *,
    taxonomy: Taxonomy,
    cache_dir: Path,
    as_of: datetime,
    max_attempts: int = 3,
    retry_wait_seconds: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> HithinkFetchResult:
    """Crawl one THS taxonomy and project its complete graph to ``symbols``.

    Partial crawls are never cached or reported as complete.  Explicit
    ``UNMAPPED`` rows preserve denominator integrity for coverage checks.
    """

    if taxonomy not in {"industry", "concept"}:
        raise ValueError("taxonomy must be industry or concept")
    cutoff = _aware(as_of)
    wanted = tuple(sorted({_symbol(item) for item in symbols if _symbol(item)}))
    if not catalog.ok or not catalog.complete:
        return _failure(
            f"THS_{taxonomy.upper()}_CATALOG_UNAVAILABLE",
            cutoff,
            taxonomy,
            {"catalog_reason_code": catalog.reason_code},
        )
    catalog_rows = _catalog_rows(catalog)
    if not catalog_rows:
        return _failure(f"THS_{taxonomy.upper()}_CATALOG_EMPTY", cutoff, taxonomy)
    catalog_hash = hashlib.sha256(canonical_json_bytes(catalog_rows)).hexdigest()
    target_dir = Path(cache_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    cache_path = target_dir / f"ths-{taxonomy}-{cutoff.date().isoformat()}.json"
    cached = _load_cache(
        cache_path,
        taxonomy=taxonomy,
        catalog_hash=catalog_hash,
        trade_date=cutoff.date().isoformat(),
    )
    fetched_at = max(cutoff, catalog.fetch_time)
    cache_hit = cached is not None
    if cached is not None:
        edges = list(cached["memberships"])
        parsed = _parse_time(cached.get("fetched_at"))
        if parsed is not None:
            fetched_at = parsed
    else:
        edges: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        for index in catalog_rows:
            result: HithinkFetchResult | None = None
            for attempt in range(1, max_attempts + 1):
                result = client.ths_index_constituents(index["taxonomy_code"])
                fetched_at = max(fetched_at, result.fetch_time)
                if result.ok and result.complete:
                    break
                if result.reason_code not in {"RATE_LIMITED", "REQUEST_FAILED"} or attempt >= max_attempts:
                    break
                sleep(retry_wait_seconds * attempt)
            assert result is not None
            if not result.ok or not result.complete:
                failures.append({"taxonomy_code": index["taxonomy_code"], "reason_code": result.reason_code})
                continue
            for member in result.items:
                raw = member.model_dump(mode="json")
                symbol = _symbol(raw.get("thscode"))
                if not symbol:
                    continue
                edges.append({
                    "taxonomy_code": index["taxonomy_code"],
                    "taxonomy_name": index["taxonomy_name"],
                    "member_thscode": symbol,
                    "member_ticker": str(raw.get("ticker") or symbol.split(".", 1)[0]),
                    "member_name": str(raw.get("name") or ""),
                })
        edges = _dedupe_edges(edges)
        if failures:
            projected, coverage = _project(edges, wanted, taxonomy)
            return HithinkFetchResult(
                endpoint=_ENDPOINT,
                ok=False,
                complete=False,
                reason_code=f"THS_{taxonomy.upper()}_MEMBERSHIP_PARTIAL",
                items=tuple(HithinkRow.model_validate(row) for row in projected),
                pages=len(catalog_rows),
                total=len(projected),
                fetch_time=fetched_at,
                metadata={
                    "taxonomy": taxonomy.upper(),
                    "catalog_hash": catalog_hash,
                    "catalog_count": len(catalog_rows),
                    "failed_count": len(failures),
                    "failures": failures[:20],
                    "selected_symbol_count": len(wanted),
                    "membership_coverage": coverage,
                    "cache_hit": False,
                },
            )
        atomic_write_json(cache_path, {
            "schema_version": _CACHE_SCHEMA,
            "taxonomy": taxonomy,
            "trade_date": cutoff.date().isoformat(),
            "fetched_at": fetched_at.isoformat(),
            "catalog_hash": catalog_hash,
            "catalog_count": len(catalog_rows),
            "complete": True,
            "memberships_sha256": hashlib.sha256(canonical_json_bytes(edges)).hexdigest(),
            "memberships": edges,
        })

    projected, coverage = _project(edges, wanted, taxonomy)
    return HithinkFetchResult(
        endpoint=_ENDPOINT,
        ok=True,
        complete=True,
        reason_code="OK",
        items=tuple(HithinkRow.model_validate(row) for row in projected),
        pages=len(catalog_rows),
        total=len(projected),
        fetch_time=fetched_at,
        metadata={
            "timestamp": int(fetched_at.timestamp() * 1000),
            "taxonomy": taxonomy.upper(),
            "catalog_hash": catalog_hash,
            "catalog_count": len(catalog_rows),
            "full_membership_edge_count": len(edges),
            "selected_symbol_count": len(wanted),
            "membership_coverage": coverage,
            "cache_hit": cache_hit,
            "cache_trade_date": cutoff.date().isoformat(),
        },
    )


def _catalog_rows(catalog: HithinkFetchResult) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in catalog.items:
        raw = item.model_dump(mode="json")
        code = _symbol(raw.get("thscode"))
        name = str(raw.get("name") or "").strip()
        if code.endswith(".TI") and name:
            rows.append({"taxonomy_code": code, "taxonomy_name": name})
    return sorted(rows, key=lambda item: (item["taxonomy_code"], item["taxonomy_name"]))


def _project(
    edges: Sequence[Mapping[str, Any]],
    wanted: Sequence[str],
    taxonomy: Taxonomy,
) -> tuple[list[dict[str, Any]], float]:
    reverse: dict[str, list[dict[str, str]]] = {symbol: [] for symbol in wanted}
    names: dict[str, str] = {}
    for edge in edges:
        symbol = _symbol(edge.get("member_thscode"))
        if symbol not in reverse:
            continue
        names.setdefault(symbol, str(edge.get("member_name") or ""))
        reverse[symbol].append({
            "taxonomy_code": str(edge.get("taxonomy_code") or ""),
            "taxonomy_name": str(edge.get("taxonomy_name") or ""),
        })
    code_key = "industry_thscode" if taxonomy == "industry" else "concept_thscode"
    name_key = "industry_name" if taxonomy == "industry" else "concept_name"
    projected: list[dict[str, Any]] = []
    mapped = 0
    for symbol in wanted:
        memberships = sorted(
            {(item["taxonomy_code"], item["taxonomy_name"]) for item in reverse[symbol]},
            key=lambda item: item,
        )
        if memberships:
            mapped += 1
        projected.append({
            "thscode": symbol,
            "ticker": symbol.split(".", 1)[0],
            "name": names.get(symbol, ""),
            "taxonomy": taxonomy.upper(),
            "mapping_status": "MAPPED" if memberships else "UNMAPPED",
            "memberships": [
                {
                    "taxonomy_code": code,
                    "taxonomy_name": name,
                    code_key: code,
                    name_key: name,
                }
                for code, name in memberships
            ],
        })
    return projected, mapped / len(wanted) if wanted else 1.0


def _dedupe_edges(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        key = (str(row.get("taxonomy_code") or ""), str(row.get("member_thscode") or ""))
        if all(key):
            unique[key] = row
    return [unique[key] for key in sorted(unique)]


def _load_cache(
    path: Path,
    *,
    taxonomy: Taxonomy,
    catalog_hash: str,
    trade_date: str,
) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(value, dict):
        return None
    if (
        value.get("schema_version") != _CACHE_SCHEMA
        or value.get("taxonomy") != taxonomy
        or value.get("catalog_hash") != catalog_hash
        or value.get("trade_date") != trade_date
        or value.get("complete") is not True
    ):
        return None
    memberships = value.get("memberships")
    if not isinstance(memberships, list) or any(not isinstance(item, dict) for item in memberships):
        return None
    if value.get("memberships_sha256") != hashlib.sha256(canonical_json_bytes(memberships)).hexdigest():
        return None
    return value


def _failure(
    reason_code: str,
    fetched_at: datetime,
    taxonomy: Taxonomy,
    metadata: Mapping[str, Any] | None = None,
) -> HithinkFetchResult:
    return HithinkFetchResult(
        endpoint=_ENDPOINT,
        ok=False,
        complete=False,
        reason_code=reason_code,
        fetch_time=fetched_at,
        metadata={"taxonomy": taxonomy.upper(), **dict(metadata or {})},
    )


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(SHANGHAI)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("THS taxonomy cutoff must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if "." in text and len(text) <= 32 else ""


__all__ = ["collect_ths_taxonomy_membership"]
