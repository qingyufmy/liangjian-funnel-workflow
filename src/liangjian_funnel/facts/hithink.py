"""Normalize HiThink endpoint outcomes into immutable fact snapshots."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx

from ..pipeline.data_source import HithinkFetchResult, HithinkRow
from .contracts import (
    FactSnapshotManifest,
    RealtimeFactEnvelope,
    SourceHealth,
    SourceHealthStatus,
    SourceTier,
    canonical_json_bytes,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
_AUCTION_SYMBOL_BATCH_SIZE = 100
_EASTMONEY_LIMIT_DOWN_URL = "https://push2ex.eastmoney.com/getTopicDTPool"
_EASTMONEY_PUBLIC_UT = "7eea3edcaed734bea9cbfc24409ed989"


def normalize_hithink_results(
    results: Mapping[str, HithinkFetchResult],
    *,
    base_url: str,
    as_of: datetime,
    snapshot_id: str | None = None,
    ingest_time: datetime | None = None,
) -> FactSnapshotManifest:
    """Build one deterministic manifest without treating failed data as empty."""

    effective_as_of = _aware(as_of)
    ingested = _aware(ingest_time or datetime.now(SHANGHAI))
    facts: list[RealtimeFactEnvelope] = []
    health: list[SourceHealth] = []
    checksums: dict[str, str] = {}
    coverage: dict[str, float] = {}
    for fact_type, result in sorted(results.items()):
        source_id = _source_id(result.endpoint)
        payload = {
            "endpoint": result.endpoint,
            "metadata": result.metadata,
            "record_count": len(result.items),
            "records": [row.model_dump(mode="json") for row in result.items],
        }
        available = bool(result.ok and result.complete)
        content_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest() if available else None
        event_time = _event_time(result.metadata.get("timestamp"), result.fetch_time)
        fact_digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "source_id": source_id,
                    "fact_type": fact_type,
                    "event_time": event_time,
                    "content_hash": content_hash,
                    "reason_code": result.reason_code,
                }
            )
        ).hexdigest()
        facts.append(
            RealtimeFactEnvelope(
                fact_id=f"sha256:{fact_digest}",
                source_id=source_id,
                source_tier=SourceTier.T2,
                fact_type=fact_type,
                event_time=event_time,
                fetch_time=result.fetch_time,
                ingest_time=max(ingested, result.fetch_time),
                available=available,
                reason_code=result.reason_code,
                source_url=urljoin(f"{base_url.rstrip('/')}/", result.endpoint.lstrip("/")),
                content_hash=content_hash,
                payload=payload,
            )
        )
        health.append(
            SourceHealth(
                source_id=source_id,
                status=SourceHealthStatus.HEALTHY if available else SourceHealthStatus.UNAVAILABLE,
                checked_at=max(ingested, result.fetch_time),
                last_success_time=result.fetch_time if available else None,
                reason_code=result.reason_code,
                coverage=1.0 if available else 0.0,
                http_status=result.http_status,
                available=available,
                details={
                    "complete": result.complete,
                    "pages": result.pages,
                    "record_count": len(result.items),
                },
            )
        )
        if content_hash is not None:
            checksums[fact_type] = content_hash
        coverage[fact_type] = 1.0 if available else 0.0

    manifest_id = snapshot_id
    # Realtime endpoint timestamps can be generated a few seconds after the
    # caller starts collection.  The frozen fact cutoff is the latest event
    # actually included, never the earlier request-start timestamp.
    if facts:
        effective_as_of = max(effective_as_of, *(fact.event_time for fact in facts))
    if manifest_id is None:
        identity = hashlib.sha256(
            canonical_json_bytes(
                {
                    "as_of": effective_as_of,
                    "facts": facts,
                    "health": health,
                }
            )
        ).hexdigest()
        manifest_id = f"hithink-{identity[:24]}"
    return FactSnapshotManifest(
        snapshot_id=manifest_id,
        as_of=effective_as_of,
        facts=tuple(facts),
        source_health=tuple(health),
        source_checksums=checksums,
        coverage_by_fact_type=coverage,
    )


def manifest_projection(manifest: FactSnapshotManifest) -> dict[str, Any]:
    """Return the hash-bound prompt projection for a frozen input snapshot."""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for fact in manifest.facts:
        payload = dict(fact.payload)
        if payload.get("prompt_injection_suspected") is True:
            # Keep the immutable fact manifest verbatim for audit, but never
            # forward suspicious provider-controlled prose to a model.  The
            # same projection is shared by CNINFO announcements and official
            # policy search results, hence the deliberately small allow-list
            # of untrusted text fields below.
            for field in ("announcement_title", "title", "summary"):
                if field in payload:
                    payload[field] = "[UNTRUSTED_TEXT_BLOCKED]"
            if "pdf_evidence_snippets" in payload:
                payload["pdf_evidence_snippets"] = []
                payload["pdf_evidence_text_blocked"] = True
        grouped.setdefault(fact.fact_type, []).append({
            "fact_id": fact.fact_id,
            "fact_type": fact.fact_type,
            "symbol": fact.symbol,
            "available": fact.available,
            "reason_code": fact.reason_code,
            "event_time": fact.event_time.isoformat(),
            "publish_time": fact.publish_time.isoformat() if fact.publish_time is not None else None,
            "fetch_time": fact.fetch_time.isoformat(),
            "source_id": fact.source_id,
            "source_url": fact.source_url,
            "content_hash": fact.content_hash,
            **payload,
        })
    facts: dict[str, Any] = {}
    for fact_type, records in grouped.items():
        if len(records) == 1:
            facts[fact_type] = records[0]
        else:
            aggregate = {
                "available": all(record.get("available") is True for record in records),
                "reason_code": "OK" if all(record.get("available") is True for record in records) else "PARTIAL_SOURCE_FAILURE",
                "record_count": len(records),
            }
            # ``fact_groups`` is the authoritative multi-record collection.
            # Repeating thousands of disclosure rows below ``facts`` doubled
            # both snapshot bytes and serialization memory.  Small groups keep
            # the legacy convenience field; large groups retain only a summary.
            if len(records) <= 256:
                aggregate["records"] = records
            facts[fact_type] = aggregate
    return {
        "schema_version": manifest.schema_version,
        "snapshot_id": manifest.snapshot_id,
        "as_of": manifest.as_of.isoformat(),
        "manifest_hash": manifest.manifest_hash,
        "facts_sha256": manifest.facts_sha256,
        "coverage_by_fact_type": manifest.coverage_by_fact_type,
        "facts": facts,
        "fact_groups": grouped,
        "source_health": [item.model_dump(mode="json") for item in manifest.source_health],
    }


def collect_market_results(
    client: Any,
    symbols: Sequence[str],
    *,
    market_trade_date: date | None = None,
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> dict[str, HithinkFetchResult]:
    """Fetch the Phase-1 market facts; each endpoint retains its own status."""

    pool_kwargs: dict[str, Any] = {}
    dragon_tiger_kwargs: dict[str, Any] = {}
    if market_trade_date is not None:
        # The pool endpoints default to the wall-clock date.  Before the
        # market has closed that date is either empty or only partially
        # formed, so A2 must explicitly bind to the latest closed session.
        pool_kwargs["date_ms"] = int(
            datetime(
                market_trade_date.year,
                market_trade_date.month,
                market_trade_date.day,
                tzinfo=SHANGHAI,
            ).timestamp()
            * 1000
        )
        dragon_tiger_kwargs["date"] = market_trade_date.isoformat()

    calls = (
        ("THS_INDUSTRY_CATALOG", lambda: client.ths_index_catalog(tag="industry")),
        ("THS_CONCEPT_CATALOG", lambda: client.ths_index_catalog(tag="cn_concept")),
        ("LIMIT_UP_POOL", lambda: client.limit_up_pool(**pool_kwargs)),
        ("LIMIT_DOWN_POOL", lambda: client.limit_down_pool(**pool_kwargs)),
        ("LIMIT_BREAK_POOL", lambda: client.limit_break_pool(**pool_kwargs)),
        ("LIMIT_UP_LADDER", lambda: client.limit_up_ladder()),
        ("DRAGON_TIGER_LIST", lambda: client.dragon_tiger_list(**dragon_tiger_kwargs)),
        ("HOT_STOCK_LIST", lambda: client.hot_stock_list(period="hour")),
    )
    results = {}
    for index, (fact_type, fetch) in enumerate(calls):
        if progress_callback is not None:
            progress_callback(f"MARKET_FACT_{fact_type}", index, len(calls))
        results[fact_type] = fetch()
        if progress_callback is not None:
            progress_callback(f"MARKET_FACT_{fact_type}", index + 1, len(calls))
    if market_trade_date is not None:
        # The pool and dragon-tiger endpoints are explicitly queried for the
        # requested completed session.  Their response envelope timestamp is
        # nevertheless the HTTP generation time, which may be a weekend or a
        # later trading day.  It is provenance, not the market event time.
        # Bind those dated facts to the requested close while retaining the
        # provider timestamp and real fetch_time for audit.
        for fact_type in (
            "LIMIT_UP_POOL",
            "LIMIT_DOWN_POOL",
            "LIMIT_BREAK_POOL",
            "DRAGON_TIGER_LIST",
        ):
            results[fact_type] = _bind_closed_session_event_time(
                results[fact_type],
                market_trade_date=market_trade_date,
            )

        results["LIMIT_UP_LADDER"] = project_closed_ladder(
            results["LIMIT_UP_LADDER"], market_trade_date=market_trade_date,
        )
    if symbols:
        normalized_symbols = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
        batches = tuple(
            normalized_symbols[index : index + _AUCTION_SYMBOL_BATCH_SIZE]
            for index in range(0, len(normalized_symbols), _AUCTION_SYMBOL_BATCH_SIZE)
        )
        results["AUCTION_FINAL"] = _merge_auction_batches(
            tuple(client.auction_snapshot(batch, stage="final") for batch in batches),
            requested_symbols=normalized_symbols,
        )
    return results


def recover_required_market_results(
    client: Any,
    initial: Mapping[str, HithinkFetchResult],
    *,
    market_trade_date: date,
    required: Sequence[str] = (
        "LIMIT_UP_POOL", "LIMIT_DOWN_POOL", "LIMIT_BREAK_POOL", "LIMIT_UP_LADDER",
    ),
    max_retries: int = 2,
    pause: Callable[[float], None] = time.sleep,
    fallback_fetcher: Callable[[date], HithinkFetchResult] | None = None,
) -> tuple[dict[str, HithinkFetchResult], list[dict[str, Any]]]:
    """Bounded source recovery; never convert a failed fact to an empty fact.

    Successful first observations stay frozen. Only failed required endpoints
    are re-fetched; persistent failures still block downstream research.
    """

    results = dict(initial)
    attempts: list[dict[str, Any]] = []
    date_ms = int(datetime(
        market_trade_date.year, market_trade_date.month, market_trade_date.day,
        tzinfo=SHANGHAI,
    ).timestamp() * 1000)
    fetches = {
        "LIMIT_UP_POOL": lambda: _bind_closed_session_event_time(
            client.limit_up_pool(date_ms=date_ms), market_trade_date=market_trade_date,
        ),
        "LIMIT_DOWN_POOL": lambda: _bind_closed_session_event_time(
            client.limit_down_pool(date_ms=date_ms), market_trade_date=market_trade_date,
        ),
        "LIMIT_BREAK_POOL": lambda: _bind_closed_session_event_time(
            client.limit_break_pool(date_ms=date_ms), market_trade_date=market_trade_date,
        ),
        "LIMIT_UP_LADDER": lambda: project_closed_ladder(
            client.limit_up_ladder(), market_trade_date=market_trade_date,
        ),
    }
    for retry_number in range(1, min(2, max(0, int(max_retries))) + 1):
        failed = [
            name for name in required
            if name not in results or not results[name].ok or not results[name].complete
        ]
        if not failed:
            break
        pause(float(retry_number))
        for name in failed:
            result = fetches[name]()
            results[name] = result
            attempts.append({
                "retry_number": retry_number,
                "fact_type": name,
                "ok": bool(result.ok and result.complete),
                "reason_code": result.reason_code,
                "business_code": result.business_code,
                "market_trade_date": market_trade_date.isoformat(),
            })
    failed_down = results.get("LIMIT_DOWN_POOL")
    if ("LIMIT_DOWN_POOL" in required and
            (failed_down is None or not failed_down.ok or not failed_down.complete)):
        alternative = (fallback_fetcher or fetch_eastmoney_limit_down_pool)(market_trade_date)
        attempts.append({
            "retry_number": "EASTMONEY_FALLBACK",
            "fact_type": "LIMIT_DOWN_POOL",
            "ok": bool(alternative.ok and alternative.complete),
            "reason_code": alternative.reason_code,
            "business_code": alternative.business_code,
            "market_trade_date": market_trade_date.isoformat(),
            "source_endpoint": alternative.endpoint,
        })
        if alternative.ok and alternative.complete:
            results["LIMIT_DOWN_POOL"] = alternative.model_copy(update={
                "metadata": {
                    **alternative.metadata,
                    "primary_failure_reason": failed_down.reason_code if failed_down else "SOURCE_NOT_CONFIGURED",
                    "primary_business_code": failed_down.business_code if failed_down else None,
                },
            })
    return results, attempts


def fetch_eastmoney_limit_down_pool(
    market_trade_date: date,
    *,
    http_client: httpx.Client | None = None,
) -> HithinkFetchResult:
    """Date/total-checked free-source fallback for one failed limit-down pool.

    This is a bounded data read, not a strategy bypass. A malformed, stale or
    partial page stays unavailable. The normalized rows contain only count
    identities and numeric fields needed for the market-emotion calculation.
    """

    fetched_at = datetime.now(SHANGHAI)
    expected_date = market_trade_date.strftime("%Y%m%d")

    def failure(reason: str, status: int | None = None) -> HithinkFetchResult:
        return HithinkFetchResult(
            endpoint=_EASTMONEY_LIMIT_DOWN_URL, ok=False, complete=False,
            reason_code=reason, fetch_time=fetched_at, http_status=status,
            metadata={"expected_market_trade_date": market_trade_date.isoformat()},
        )

    owns_client = http_client is None
    client = http_client or httpx.Client(timeout=7.0, trust_env=False)
    try:
        response = client.get(_EASTMONEY_LIMIT_DOWN_URL, params={
            "ut": _EASTMONEY_PUBLIC_UT, "dpt": "wz.ztzt", "Pageindex": "0",
            "pagesize": "10000", "sort": "fund:asc", "date": expected_date,
        })
        if response.status_code != 200:
            return failure("EASTMONEY_HTTP_ERROR", response.status_code)
        payload = response.json()
    except (httpx.HTTPError, ValueError):
        return failure("EASTMONEY_FETCH_FAILED")
    finally:
        if owns_client:
            client.close()

    if not isinstance(payload, Mapping) or payload.get("rc") != 0:
        return failure("EASTMONEY_BUSINESS_ERROR", 200)
    data = payload.get("data")
    if not isinstance(data, Mapping) or str(data.get("qdate")) != expected_date:
        return failure("EASTMONEY_TRADE_DATE_MISMATCH", 200)
    pool, total = data.get("pool"), data.get("tc")
    if (not isinstance(pool, list) or not isinstance(total, int) or
            isinstance(total, bool) or not 0 <= total <= 10000 or total != len(pool)):
        return failure("EASTMONEY_POOL_INCOMPLETE", 200)
    rows: list[HithinkRow] = []
    seen: set[str] = set()
    for item in pool:
        if not isinstance(item, Mapping):
            return failure("EASTMONEY_ROW_INVALID", 200)
        code, market = str(item.get("c") or ""), item.get("m")
        price, change, streak = item.get("p"), item.get("zdp"), item.get("days")
        if (len(code) != 6 or not code.isdigit() or
                not isinstance(market, int) or isinstance(market, bool) or market not in (0, 1, 2) or
                not isinstance(price, (int, float)) or isinstance(price, bool) or price <= 0 or
                not isinstance(change, (int, float)) or isinstance(change, bool) or change >= 0 or
                not isinstance(streak, int) or isinstance(streak, bool) or streak < 1):
            return failure("EASTMONEY_ROW_INVALID", 200)
        symbol = f"{code}.{'SZ' if market == 0 else 'SH' if market == 1 else 'BJ'}"
        if symbol in seen:
            return failure("EASTMONEY_DUPLICATE_SYMBOL", 200)
        seen.add(symbol)
        rows.append(HithinkRow.model_validate({
            "thscode": symbol,
            "ticker": code,
            "last_price": price / 1000,
            "change_ratio_pct": change,
            "limit_down_streak": streak,
        }))
    result = HithinkFetchResult(
        endpoint=_EASTMONEY_LIMIT_DOWN_URL, ok=True, complete=True,
        reason_code="OK", items=tuple(rows), pages=1, total=total,
        fetch_time=fetched_at, http_status=200, business_code=0,
        metadata={
            "provider": "EASTMONEY", "provider_qdate": expected_date,
            "provider_total": total, "provider_rc": 0,
        },
    )
    return _bind_closed_session_event_time(result, market_trade_date=market_trade_date)


def project_closed_ladder(result: HithinkFetchResult, *, market_trade_date: date) -> HithinkFetchResult:
    """Select the dated closed prefix, never relabel today's partial ladder."""
    if not result.ok or not result.complete:
        return result
    target = market_trade_date.isoformat()
    window = result.metadata.get("window")
    dates = window.get("date_list") if isinstance(window, Mapping) else None
    metadata = {**dict(result.metadata), "expected_market_trade_date": target,
                "observed_latest_market_trade_date": dates[0] if isinstance(dates, list) and dates else None}

    def failure(reason):
        return result.model_copy(update={"ok": False, "complete": False,
                                         "reason_code": reason, "metadata": metadata})

    if not isinstance(dates, list) or target not in dates:
        return failure("MARKET_TRADE_DATE_MISMATCH")
    try:
        if any(date.fromisoformat(d).isoformat() != d for d in dates):
            return failure("LADDER_WINDOW_INVALID")
    except (TypeError, ValueError):
        return failure("LADDER_WINDOW_INVALID")
    raw = [row.model_dump(mode="json") for row in result.items]
    row_dates = [row.get("date") for row in raw]
    if (len(set(dates)) != len(dates) or len(row_dates) != len(dates)
            or any(not isinstance(d, str) for d in row_dates)
            or len(set(row_dates)) != len(row_dates) or set(row_dates) != set(dates)):
        return failure("LADDER_WINDOW_ROWS_MISMATCH")
    kept = sorted((row for row in raw if row["date"] <= target), key=lambda row: row["date"], reverse=True)
    for row in kept:
        if not isinstance(row.get("boards"), Mapping) or any(
            not isinstance(group, list) or any(not isinstance(stock, dict) for stock in group)
            for group in row["boards"].values()
        ):
            return failure("LADDER_BOARDS_INVALID")
        # The provider revises next-day annotations even on historical rows.
        # Outcomes attached to the cutoff day's stocks are not closed facts.
        if row["date"] == target:
            for group in row["boards"].values():
                for stock in group:
                    stock.pop("seal_nextday", None)
                    stock.pop("sign_level", None)
    metadata.update({"window": {"date_list": [row["date"] for row in kept], "length": len(kept)},
                     "projection": "CLOSED_DATE_PREFIX", "dropped_newer_day_count": len(raw) - len(kept),
                     "cutoff_day_forward_annotations_removed": True})
    projected = result.model_copy(update={"items": tuple(HithinkRow.model_validate(row) for row in kept),
                                          "total": len(kept), "metadata": metadata})
    return _bind_closed_session_event_time(projected, market_trade_date=market_trade_date)


def _bind_closed_session_event_time(
    result: HithinkFetchResult,
    *,
    market_trade_date: date,
) -> HithinkFetchResult:
    event_time = datetime(
        market_trade_date.year,
        market_trade_date.month,
        market_trade_date.day,
        15,
        0,
        tzinfo=SHANGHAI,
    )
    metadata = dict(result.metadata)
    provider_response_timestamp = metadata.get("timestamp")
    metadata.update(
        {
            "timestamp": event_time.isoformat(),
            "market_trade_date": market_trade_date.isoformat(),
            "event_time_basis": "REQUESTED_CLOSED_MARKET_SESSION",
            "provider_response_timestamp": provider_response_timestamp,
        }
    )
    return result.model_copy(update={"metadata": metadata})


def _merge_auction_batches(
    results: Sequence[HithinkFetchResult],
    *,
    requested_symbols: Sequence[str],
) -> HithinkFetchResult:
    """Merge bounded auction requests without turning a failed batch into empty data."""

    if not results:
        raise ValueError("auction batches must not be empty")
    first = results[0]
    items: list[Any] = []
    seen: set[str] = set()
    for result in results:
        for row in result.items:
            raw = row.model_dump(mode="json")
            identity = str(
                raw.get("thscode")
                or raw.get("symbol")
                or raw.get("code")
                or hashlib.sha256(canonical_json_bytes(raw)).hexdigest()
            )
            if identity in seen:
                continue
            seen.add(identity)
            items.append(row)
    requested = set(requested_symbols)
    returned = {
        str(row.model_dump(mode="json").get("thscode") or "").strip().upper()
        for row in items
    }
    missing_symbols = sorted(symbol for symbol in requested if symbol not in returned)
    failed = [
        {
            "batch_index": index,
            "ok": result.ok,
            "complete": result.complete,
            "reason_code": result.reason_code,
            "record_count": len(result.items),
        }
        for index, result in enumerate(results)
        if not result.ok or not result.complete
    ]
    all_complete = not failed and not missing_symbols
    metadata = {
        "batch_size": _AUCTION_SYMBOL_BATCH_SIZE,
        "batch_count": len(results),
        "successful_batch_count": len(results) - len(failed),
        "requested_symbol_count": len(requested),
        "returned_symbol_count": len(returned),
        "missing_symbol_count": len(missing_symbols),
        "missing_symbols": missing_symbols[:100],
        "record_count": len(items),
        "failed_batches": failed,
    }
    return HithinkFetchResult(
        endpoint=first.endpoint,
        ok=all_complete,
        complete=all_complete,
        reason_code=(
            "OK"
            if all_complete
            else "AUCTION_SYMBOL_COVERAGE_INCOMPLETE"
            if missing_symbols and not failed
            else "AUCTION_BATCH_PARTIAL_FAILURE"
        ),
        items=tuple(items),
        pages=sum(result.pages for result in results),
        total=len(items),
        fetch_time=max(result.fetch_time for result in results),
        http_status=next((result.http_status for result in results if result.http_status is not None), None),
        business_code=next((result.business_code for result in results if result.business_code is not None), None),
        metadata=metadata,
    )


def _source_id(endpoint: str) -> str:
    if endpoint.startswith("https://push2ex.eastmoney.com/"):
        return "eastmoney.limit_down_pool"
    tail = endpoint.strip("/").replace("/", ".").replace("-", "_")
    return f"hithink.{tail}"[-128:]


def _event_time(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = float(value) / 1000 if abs(float(value)) >= 10_000_000_000 else float(value)
        try:
            return datetime.fromtimestamp(seconds, tz=SHANGHAI)
        except (OSError, OverflowError, ValueError):
            pass
    if isinstance(value, str) and value.strip():
        try:
            return _aware(datetime.fromisoformat(value.strip().replace("Z", "+00:00")))
        except ValueError:
            pass
    return _aware(fallback)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("fact snapshot timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


__all__ = ["collect_market_results", "manifest_projection", "normalize_hithink_results"]
