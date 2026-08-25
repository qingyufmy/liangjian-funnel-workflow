"""Daily HiThink industry-membership snapshot for the selected research pool.

HiThink exposes an industry catalog and one current-constituents endpoint per
industry, but no reverse ``stock -> industries`` endpoint.  This module builds
that reverse relation once per Shanghai trade date, persists the complete raw
membership graph, and projects only the requested symbols into the immutable
fact layer.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..facts.contracts import canonical_json_bytes
from ..pipeline.data_source import HithinkFetchResult, HithinkRow
from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")
_ENDPOINT = "/api/a-share-index/constituents/ths-stock-list"
_CACHE_SCHEMA = "liangjian-ths-industry-cache/1.0.0"


def collect_ths_industry_membership(
    client: Any,
    catalog: HithinkFetchResult,
    symbols: Sequence[str],
    *,
    cache_dir: Path,
    as_of: datetime,
    max_attempts: int = 3,
    retry_wait_seconds: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> HithinkFetchResult:
    """Return current THS memberships for ``symbols`` with a daily full cache.

    A partial industry crawl is never written as a reusable cache and never
    reported as successful.  Unknown selected symbols remain explicit rows
    with ``mapping_status=UNMAPPED`` so coverage cannot be confused with an
    empty, successful response.
    """

    cutoff = _aware(as_of)
    wanted = tuple(sorted({_symbol(item) for item in symbols if _symbol(item)}))
    fetched_at = cutoff
    if not catalog.ok or not catalog.complete:
        return _failure(
            "THS_INDUSTRY_CATALOG_UNAVAILABLE",
            fetched_at=max(fetched_at, catalog.fetch_time),
            metadata={"catalog_reason_code": catalog.reason_code, "taxonomy": "THS"},
        )

    catalog_rows = _catalog_rows(catalog)
    if not catalog_rows:
        return _failure("THS_INDUSTRY_CATALOG_EMPTY", fetched_at=max(fetched_at, catalog.fetch_time))
    catalog_hash = hashlib.sha256(canonical_json_bytes(catalog_rows)).hexdigest()
    cache_path = Path(cache_dir) / f"ths-industry-{cutoff.date().isoformat()}.json"
    cached = _load_cache(cache_path, catalog_hash=catalog_hash, trade_date=cutoff.date().isoformat())
    cache_hit = cached is not None
    if cached is not None:
        membership_rows = list(cached["memberships"])
        fetched_at = _parse_time(cached.get("fetched_at")) or max(fetched_at, catalog.fetch_time)
    else:
        membership_rows: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        latest = max(fetched_at, catalog.fetch_time)
        for industry in catalog_rows:
            result: HithinkFetchResult | None = None
            for attempt in range(1, max_attempts + 1):
                result = client.ths_index_constituents(industry["industry_thscode"])
                latest = max(latest, result.fetch_time)
                if result.ok and result.complete:
                    break
                if result.reason_code not in {"RATE_LIMITED", "REQUEST_FAILED"} or attempt >= max_attempts:
                    break
                sleep(retry_wait_seconds * attempt)
            assert result is not None
            if not result.ok or not result.complete:
                failures.append({
                    "industry_thscode": industry["industry_thscode"],
                    "reason_code": result.reason_code,
                })
                continue
            for member in result.items:
                raw = member.model_dump(mode="json")
                member_symbol = _symbol(raw.get("thscode"))
                if not member_symbol:
                    continue
                membership_rows.append({
                    "industry_thscode": industry["industry_thscode"],
                    "industry_name": industry["industry_name"],
                    "member_thscode": member_symbol,
                    "member_ticker": str(raw.get("ticker") or member_symbol.split(".", 1)[0]),
                    "member_name": str(raw.get("name") or ""),
                })
        fetched_at = latest
        membership_rows = _dedupe_memberships(membership_rows)
        if failures:
            projected, coverage = _project(membership_rows, wanted)
            return HithinkFetchResult(
                endpoint=_ENDPOINT,
                ok=False,
                complete=False,
                reason_code="THS_INDUSTRY_MEMBERSHIP_PARTIAL",
                items=tuple(HithinkRow.model_validate(row) for row in projected),
                pages=len(catalog_rows),
                total=len(projected),
                fetch_time=fetched_at,
                metadata={
                    "taxonomy": "THS",
                    "catalog_hash": catalog_hash,
                    "industry_count": len(catalog_rows),
                    "failed_industry_count": len(failures),
                    "failed_industries": failures[:20],
                    "selected_symbol_count": len(wanted),
                    "membership_coverage": coverage,
                    "cache_hit": False,
                },
            )
        atomic_write_json(cache_path, {
            "schema_version": _CACHE_SCHEMA,
            "trade_date": cutoff.date().isoformat(),
            "fetched_at": fetched_at.isoformat(),
            "catalog_hash": catalog_hash,
            "industry_count": len(catalog_rows),
            "complete": True,
            "memberships_sha256": hashlib.sha256(canonical_json_bytes(membership_rows)).hexdigest(),
            "memberships": membership_rows,
        })

    projected, coverage = _project(membership_rows, wanted)
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
            "taxonomy": "THS",
            "catalog_hash": catalog_hash,
            "industry_count": len(catalog_rows),
            "full_membership_edge_count": len(membership_rows),
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
        if code and code.endswith(".TI") and name:
            rows.append({"industry_thscode": code, "industry_name": name})
    return sorted(rows, key=lambda item: (item["industry_thscode"], item["industry_name"]))


def _project(rows: Sequence[dict[str, Any]], wanted: Sequence[str]) -> tuple[list[dict[str, Any]], float]:
    reverse: dict[str, list[dict[str, str]]] = {symbol: [] for symbol in wanted}
    names: dict[str, str] = {}
    for row in rows:
        symbol = _symbol(row.get("member_thscode"))
        if symbol not in reverse:
            continue
        names.setdefault(symbol, str(row.get("member_name") or ""))
        reverse[symbol].append({
            "industry_thscode": str(row["industry_thscode"]),
            "industry_name": str(row["industry_name"]),
        })
    projected = []
    mapped = 0
    for symbol in wanted:
        memberships = sorted(
            { (item["industry_thscode"], item["industry_name"]) for item in reverse[symbol] },
            key=lambda item: (item[0], item[1]),
        )
        if memberships:
            mapped += 1
        projected.append({
            "thscode": symbol,
            "ticker": symbol.split(".", 1)[0],
            "name": names.get(symbol, ""),
            "taxonomy": "THS",
            "mapping_status": "MAPPED" if memberships else "UNMAPPED",
            "memberships": [
                {"industry_thscode": code, "industry_name": name}
                for code, name in memberships
            ],
        })
    coverage = mapped / len(wanted) if wanted else 1.0
    return projected, coverage


def _dedupe_memberships(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("industry_thscode") or ""), str(row.get("member_thscode") or ""))
        if all(key):
            unique[key] = dict(row)
    return [unique[key] for key in sorted(unique)]


def _load_cache(path: Path, *, catalog_hash: str, trade_date: str) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(raw, dict):
        return None
    if raw.get("schema_version") != _CACHE_SCHEMA or raw.get("complete") is not True:
        return None
    if raw.get("catalog_hash") != catalog_hash or raw.get("trade_date") != trade_date:
        return None
    memberships = raw.get("memberships")
    if not isinstance(memberships, list) or any(not isinstance(item, dict) for item in memberships):
        return None
    expected = hashlib.sha256(canonical_json_bytes(memberships)).hexdigest()
    if raw.get("memberships_sha256") != expected:
        return None
    return raw


def _failure(reason_code: str, *, fetched_at: datetime, metadata: dict[str, Any] | None = None) -> HithinkFetchResult:
    return HithinkFetchResult(
        endpoint=_ENDPOINT,
        ok=False,
        complete=False,
        reason_code=reason_code,
        fetch_time=fetched_at,
        metadata=metadata or {"taxonomy": "THS"},
    )


def _symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if "." in text and len(text) <= 32 else ""


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
        raise ValueError("THS industry cutoff must be timezone-aware")
    return value.astimezone(SHANGHAI)


__all__ = ["collect_ths_industry_membership"]
