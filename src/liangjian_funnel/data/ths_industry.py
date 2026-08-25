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
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..facts.contracts import canonical_json_bytes
from ..pipeline.data_source import HithinkFetchResult, HithinkRow
from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")
_ENDPOINT = "/api/a-share-index/constituents/ths-stock-list"
_HISTORY_ENDPOINT = "/api/a-share-index/prices/historical"
_CACHE_SCHEMA = "liangjian-ths-industry-cache/1.0.0"
_HISTORY_CACHE_SCHEMA = "liangjian-ths-industry-history-cache/1.0.0"


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


def select_industry_diversified_symbols(
    records: Sequence[Any],
    membership: HithinkFetchResult,
    *,
    limit: int,
    top_n_per_node: int,
    node_count_target: Sequence[int],
) -> tuple[tuple[str, ...], dict[str, Any]]:
    """Build deterministic A1 input by industry node, never global turnover.

    The most specific THS industry membership (884*) is used as the node and
    its 881* membership as the broad parent.  Specific nodes are chosen
    round-robin across broad parents, ranked by eligible-company coverage, so
    A1 cannot collapse into the day's highest-turnover themes.  Turnover is
    used only to rank companies *inside* an already selected node.
    """

    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("industry-diversified limit must be positive")
    if not isinstance(top_n_per_node, int) or isinstance(top_n_per_node, bool) or top_n_per_node < 1:
        raise ValueError("top_n_per_node must be positive")
    targets = tuple(int(value) for value in node_count_target)
    if len(targets) != 2 or targets[0] < 1 or targets[0] > targets[1]:
        raise ValueError("node_count_target must contain an increasing min/max pair")
    if not membership.ok or not membership.complete:
        raise ValueError("industry membership must be complete")

    record_by_symbol = {
        str(getattr(record, "symbol", "")): record
        for record in records
        if str(getattr(record, "symbol", ""))
    }
    node_by_symbol: dict[str, tuple[str, str, str, str]] = {}
    for raw in membership.items:
        row = raw.model_dump(mode="python")
        symbol = _symbol(row.get("thscode"))
        raw_memberships = row.get("memberships")
        if symbol not in record_by_symbol or not isinstance(raw_memberships, list):
            continue
        available = [
            item
            for item in raw_memberships
            if isinstance(item, Mapping)
            and _symbol(item.get("industry_thscode"))
            and str(item.get("industry_name") or "").strip()
        ]
        if not available:
            continue
        specific_items = sorted(
            (item for item in available if str(item.get("industry_thscode") or "").startswith("884")),
            key=lambda item: str(item.get("industry_thscode") or ""),
        )
        broad_items = sorted(
            (item for item in available if str(item.get("industry_thscode") or "").startswith("881")),
            key=lambda item: str(item.get("industry_thscode") or ""),
        )
        # THS currently gives at most one 884* and one 881* membership per
        # stock.  Keep the deterministic ordering explicit if that changes.
        specific = specific_items[0] if specific_items else available[0]
        broad = broad_items[0] if broad_items else specific
        node_by_symbol[symbol] = (
            str(specific["industry_thscode"]),
            str(specific["industry_name"]),
            str(broad["industry_thscode"]),
            str(broad["industry_name"]),
        )

    grouped: dict[str, list[Any]] = {}
    node_names: dict[str, str] = {}
    parent_votes: dict[str, Counter[tuple[str, str]]] = {}
    for symbol, node in node_by_symbol.items():
        code, name, parent_code, parent_name = node
        grouped.setdefault(code, []).append(record_by_symbol[symbol])
        node_names[code] = name
        parent_votes.setdefault(code, Counter())[(parent_code, parent_name)] += 1
    for values in grouped.values():
        values.sort(key=lambda item: (-float(getattr(item, "amount", None) or 0.0), str(item.symbol)))

    parent_by_node = {
        code: sorted(votes.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))[0][0]
        for code, votes in parent_votes.items()
    }
    nodes_by_parent: dict[tuple[str, str], list[str]] = {}
    for code, parent in parent_by_node.items():
        nodes_by_parent.setdefault(parent, []).append(code)
    for nodes in nodes_by_parent.values():
        nodes.sort(key=lambda code: (-len(grouped[code]), code))
    parent_members: dict[tuple[str, str], set[str]] = {}
    for symbol, (code, _name, _parent_code, _parent_name) in node_by_symbol.items():
        parent_members.setdefault(parent_by_node[code], set()).add(symbol)
    ordered_parents = sorted(
        nodes_by_parent,
        key=lambda parent: (-len(parent_members[parent]), parent[0], parent[1]),
    )

    minimum_nodes, maximum_nodes = targets
    desired_nodes = min(maximum_nodes, limit, len(grouped))
    selected_nodes: list[str] = []
    child_rank = 0
    while len(selected_nodes) < desired_nodes:
        added = False
        for parent in ordered_parents:
            children = nodes_by_parent[parent]
            if child_rank >= len(children):
                continue
            selected_nodes.append(children[child_rank])
            added = True
            if len(selected_nodes) == desired_nodes:
                break
        if not added:
            break
        child_rank += 1
    # Small explicit capability runs may request fewer stocks than the formal
    # 40-node production minimum. They still receive one node per stock and
    # are labelled by their actual coverage; production limits must satisfy
    # the configured minimum in full.
    if len(selected_nodes) < min(minimum_nodes, limit):
        raise ValueError("industry membership cannot satisfy minimum node coverage")

    selected: list[str] = []
    for rank in range(top_n_per_node):
        for code in selected_nodes:
            candidates = grouped[code]
            if rank < len(candidates):
                selected.append(str(candidates[rank].symbol))
                if len(selected) == limit:
                    break
        if len(selected) == limit:
            break

    metadata = {
        "strategy": "THS_PARENT_BALANCED_SPECIFIC_NODE_ROUND_ROBIN_TOP_N",
        "requested_limit": limit,
        "selected_count": len(selected),
        "node_count": len(selected_nodes),
        "parent_industry_count": len({parent_by_node[code] for code in selected_nodes}),
        "mapped_symbol_count": len(node_by_symbol),
        "top_n_per_node": top_n_per_node,
        "nodes": [
            {
                "industry_thscode": code,
                "industry_name": node_names[code],
                "parent_industry_thscode": parent_by_node[code][0],
                "parent_industry_name": parent_by_node[code][1],
                "available_members": len(grouped[code]),
                "selected_members": sum(node_by_symbol.get(symbol, (None,))[0] == code for symbol in selected),
            }
            for code in selected_nodes
        ],
    }
    return tuple(selected), metadata


def collect_ths_industry_history(
    client: Any,
    catalog: HithinkFetchResult,
    *,
    cache_dir: Path,
    as_of: datetime,
    lookback_days: int = 15,
) -> HithinkFetchResult:
    """Collect point-in-time 881* industry bars used to prove market regime.

    Market breadth alone cannot establish a persistent main line.  This daily
    cache provides the cross-sector history required to measure Top-3 overlap
    and turnover persistence without calling current winners a main line.
    """

    cutoff = _aware(as_of)
    if lookback_days < 7 or lookback_days > 60:
        raise ValueError("industry history lookback must be between 7 and 60 days")
    if not catalog.ok or not catalog.complete:
        return _history_failure(
            "THS_INDUSTRY_CATALOG_UNAVAILABLE",
            cutoff,
            {"catalog_reason_code": catalog.reason_code},
        )
    catalog_rows = [
        row for row in _catalog_rows(catalog)
        if row["industry_thscode"].startswith("881")
    ]
    if not catalog_rows:
        return _history_failure("THS_BROAD_INDUSTRY_CATALOG_EMPTY", cutoff)
    catalog_hash = hashlib.sha256(canonical_json_bytes(catalog_rows)).hexdigest()
    cache_path = Path(cache_dir) / f"ths-industry-history-{cutoff.date().isoformat()}.json"
    cached = _load_history_cache(
        cache_path,
        catalog_hash=catalog_hash,
        trade_date=cutoff.date().isoformat(),
        lookback_days=lookback_days,
    )
    if cached is not None:
        fetched_at = _parse_time(cached.get("fetched_at")) or cutoff
        rows = cached["industries"]
        return HithinkFetchResult(
            endpoint=_HISTORY_ENDPOINT,
            ok=True,
            complete=True,
            reason_code="OK",
            items=tuple(HithinkRow.model_validate(row) for row in rows),
            pages=len(rows),
            total=len(rows),
            fetch_time=fetched_at,
            metadata={
                "taxonomy": "THS",
                "cache_hit": True,
                "trade_date": cutoff.date().isoformat(),
                "lookback_days": lookback_days,
                "industry_count": len(rows),
            },
        )

    start = int((cutoff - timedelta(days=lookback_days)).timestamp() * 1000)
    end = int(cutoff.timestamp() * 1000)
    rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    fetched_at = cutoff
    for industry in catalog_rows:
        result = client.index_history_1d(industry["industry_thscode"], start=start, end=end)
        fetched_at = max(fetched_at, result.fetch_time)
        if not result.ok or not result.complete or len(result.items) < 5:
            failures.append({
                "industry_thscode": industry["industry_thscode"],
                "reason_code": result.reason_code if result.items else "INSUFFICIENT_BARS",
            })
            continue
        bars = sorted(
            (item.model_dump(mode="json") for item in result.items),
            key=lambda item: (int(item.get("date_ms") or 0), str(item)),
        )
        rows.append({**industry, "bars": bars})
    if failures:
        return _history_failure(
            "THS_INDUSTRY_HISTORY_PARTIAL",
            fetched_at,
            {
                "taxonomy": "THS",
                "industry_count": len(catalog_rows),
                "completed_industry_count": len(rows),
                "failed_industry_count": len(failures),
                "failed_industries": failures[:20],
            },
            rows=rows,
        )
    payload = {
        "schema_version": _HISTORY_CACHE_SCHEMA,
        "trade_date": cutoff.date().isoformat(),
        "fetched_at": fetched_at.isoformat(),
        "catalog_hash": catalog_hash,
        "lookback_days": lookback_days,
        "complete": True,
        "industries_sha256": hashlib.sha256(canonical_json_bytes(rows)).hexdigest(),
        "industries": rows,
    }
    atomic_write_json(cache_path, payload)
    return HithinkFetchResult(
        endpoint=_HISTORY_ENDPOINT,
        ok=True,
        complete=True,
        reason_code="OK",
        items=tuple(HithinkRow.model_validate(row) for row in rows),
        pages=len(rows),
        total=len(rows),
        fetch_time=fetched_at,
        metadata={
            "taxonomy": "THS",
            "cache_hit": False,
            "trade_date": cutoff.date().isoformat(),
            "lookback_days": lookback_days,
            "industry_count": len(rows),
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


def _load_history_cache(
    path: Path,
    *,
    catalog_hash: str,
    trade_date: str,
    lookback_days: int,
) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(raw, dict):
        return None
    if (
        raw.get("schema_version") != _HISTORY_CACHE_SCHEMA
        or raw.get("complete") is not True
        or raw.get("catalog_hash") != catalog_hash
        or raw.get("trade_date") != trade_date
        or raw.get("lookback_days") != lookback_days
    ):
        return None
    rows = raw.get("industries")
    if not isinstance(rows, list) or any(not isinstance(item, dict) for item in rows):
        return None
    if raw.get("industries_sha256") != hashlib.sha256(canonical_json_bytes(rows)).hexdigest():
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


def _history_failure(
    reason_code: str,
    fetched_at: datetime,
    metadata: dict[str, Any] | None = None,
    *,
    rows: Sequence[dict[str, Any]] = (),
) -> HithinkFetchResult:
    return HithinkFetchResult(
        endpoint=_HISTORY_ENDPOINT,
        ok=False,
        complete=False,
        reason_code=reason_code,
        items=tuple(HithinkRow.model_validate(row) for row in rows),
        pages=len(rows),
        total=len(rows),
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


__all__ = [
    "collect_ths_industry_history",
    "collect_ths_industry_membership",
    "select_industry_diversified_symbols",
]
