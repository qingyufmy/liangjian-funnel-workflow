"""Incremental HiThink synchronization backed by :mod:`local_fact_cache`."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .data_source import HithinkClient, HithinkFetchResult
from .local_fact_cache import LocalFactCache


SHANGHAI = ZoneInfo("Asia/Shanghai")
FINANCIAL_DATASETS = ("INCOME", "INDICATORS", "BALANCE", "CASH_FLOW")
CORE_FINANCIAL_DATASETS = ("INCOME", "BALANCE", "CASH_FLOW")


@dataclass(frozen=True, slots=True)
class SyncResult:
    daily: dict[str, list[dict[str, Any]]]
    fundamental: dict[str, Any]
    failures: dict[str, list[str]]
    processed: int
    total: int
    cache_hits: int
    cache_misses: int
    daily_updates: int = 0
    financial_refreshes: int = 0
    deferred_financial_refreshes: int = 0
    # Symbols for which at least one provider response was successfully
    # persisted during this call.  Cache-only hits and failed responses are
    # deliberately excluded so downstream feature maintenance cannot rebuild
    # an unchanged or incomplete entity.
    updated_symbols: tuple[str, ...] = ()


ProgressCallback = Callable[[Mapping[str, Any]], None]
FundamentalProjector = Callable[[list[dict[str, Any]]], Any]


class HithinkIncrementalSynchronizer:
    """Synchronize bounded symbol facts and return compact model projections.

    Every successful provider response is committed before the next symbol is
    requested.  A killed bootstrap therefore resumes from the durable cache
    instead of starting the entire market again.
    """

    def __init__(
        self,
        cache: LocalFactCache,
        *,
        fundamental_refresh_hours: int = 24,
        fundamental_refresh_symbols_per_run: int = 100,
        daily_refresh_hours: int = 4,
        progress_every: int = 25,
        batch_size: int = 50,
    ) -> None:
        self.cache = cache
        self.fundamental_refresh = timedelta(hours=max(1, int(fundamental_refresh_hours)))
        self.fundamental_refresh_symbols_per_run = max(
            0, int(fundamental_refresh_symbols_per_run)
        )
        self.daily_refresh = timedelta(hours=max(1, int(daily_refresh_hours)))
        self.progress_every = max(1, int(progress_every))
        self.batch_size = max(1, int(batch_size))

    def sync(
        self,
        client: HithinkClient,
        symbols: Sequence[str],
        *,
        as_of: datetime,
        lookback_days: int = 800,
        compact_daily_bars: int = 30,
        fundamental_projector: FundamentalProjector | None = None,
        progress: ProgressCallback | None = None,
    ) -> SyncResult:
        current = _aware(as_of)
        ordered = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols))
        failures: dict[str, list[str]] = {}
        daily: dict[str, list[dict[str, Any]]] = {}
        fundamental: dict[str, Any] = {}
        updated_symbols: list[str] = []
        hits = 0
        misses = 0
        daily_updates = 0
        financial_refreshes = 0
        start = current - timedelta(days=max(1, int(lookback_days)))
        closed_daily_end = _closed_daily_end(current)
        required_latest_daily = (
            current.replace(hour=0, minute=0, second=0, microsecond=0)
            if current.time().replace(tzinfo=None) >= datetime_time(15, 0)
            else None
        )
        financial_states = {
            (str(state.get("endpoint") or ""), str(state.get("symbol") or "")): state
            for state in self.cache.list_sync_state()
            if str(state.get("endpoint") or "").startswith("HITHINK_FINANCIAL_")
        }
        financial_refresh_symbols, deferred_financial_symbols = (
            self._select_financial_refresh_symbols(
                ordered,
                states=financial_states,
                current=current,
            )
        )

        for index, symbol in enumerate(ordered, start=1):
            symbol_hit = True
            symbol_updated = False
            daily_ready = self._daily_ready(
                symbol,
                start=start,
                closed_daily_end=closed_daily_end,
                required_latest=required_latest_daily,
            )
            if not daily_ready:
                symbol_hit = False
                latest = self.cache.latest_daily_bar(symbol, adjust="none")
                request_start = start
                if latest is not None:
                    request_start = max(
                        start,
                        _aware(datetime.fromisoformat(str(latest["timestamp"]))) - timedelta(days=7),
                    )
                result = client.history_1d(
                    symbol,
                    start=int(request_start.timestamp() * 1000),
                    end=int(closed_daily_end.timestamp() * 1000),
                    adjust="none",
                    limit=1000,
                    max_pages=1,
                )
                closed_items = tuple(
                    row
                    for row in result.items
                    if _row_time(row.model_dump(mode="python")) < closed_daily_end
                )
                if result.ok and result.complete and closed_items:
                    self.cache.upsert_daily_bars(
                        (
                            {
                                "symbol": symbol,
                                "timestamp": _row_time(row.model_dump(mode="python")),
                                "adjust": "none",
                                "fetched_at": result.fetch_time,
                                "payload": row.model_dump(mode="json"),
                            }
                            for row in closed_items
                        ),
                        batch_size=self.batch_size,
                    )
                    self.cache.update_sync_state(
                        "HITHINK_DAILY_1D",
                        symbol,
                        last_success=result.fetch_time,
                        cursor={"through": _latest_row_time(closed_items)},
                        status="READY",
                        reason=None,
                    )
                    symbol_updated = True
                    daily_updates += 1
                else:
                    reason = result.reason_code if not result.ok else "NO_CLOSED_DAILY_BARS"
                    failures.setdefault(symbol, []).append(f"DAILY:{reason}")
                    self.cache.update_sync_state(
                        "HITHINK_DAILY_1D", symbol, status="FAILED", reason=reason
                    )

            rows = self.cache.query_daily_bars(
                symbol,
                adjust="none",
                start=start,
                end=closed_daily_end,
                limit=compact_daily_bars,
                descending=True,
            )
            if rows:
                daily[symbol] = [dict(item["payload"]) for item in reversed(rows)]
            else:
                failures.setdefault(symbol, []).append("DAILY:CACHE_EMPTY")

            financial_rows: list[dict[str, Any]] = []
            symbol_financial_refreshed = False
            for dataset in FINANCIAL_DATASETS:
                endpoint = f"HITHINK_FINANCIAL_{dataset}"
                state = financial_states.get((endpoint, symbol))
                due = self._financial_due(state, current)
                missing_core = dataset in CORE_FINANCIAL_DATASETS and state is None
                should_refresh = missing_core or (
                    symbol in financial_refresh_symbols and due
                )
                if should_refresh:
                    symbol_hit = False
                    result = _fetch_financial(client, dataset, symbol)
                    if result.ok and result.complete and result.items:
                        cache_rows = []
                        for row in result.items:
                            payload = row.model_dump(mode="json")
                            cache_rows.append(
                                {
                                    "symbol": symbol,
                                    "dataset": dataset,
                                    "report_period": _report_period(payload, result.fetch_time),
                                    # When the provider row has no publication
                                    # timestamp, observation time is the only
                                    # defensible point-in-time boundary.
                                    "published_at": _published_at(payload, result.fetch_time),
                                    "fetched_at": result.fetch_time,
                                    "payload": payload,
                                }
                            )
                        self.cache.upsert_financial_facts(
                            cache_rows,
                            batch_size=self.batch_size,
                        )
                        self.cache.update_sync_state(
                            endpoint,
                            symbol,
                            last_success=result.fetch_time,
                            cursor={"rows": len(cache_rows)},
                            status="READY",
                            reason=None,
                        )
                        symbol_updated = True
                        symbol_financial_refreshed = True
                    else:
                        reason = result.reason_code if result.items or not result.ok else "EMPTY_DATA"
                        failures.setdefault(symbol, []).append(f"{dataset}:{reason}")
                        self.cache.update_sync_state(
                            endpoint, symbol, status="FAILED", reason=reason
                        )

                cached = self.cache.query_financial_facts(symbol, dataset=dataset)
                if not cached:
                    failures.setdefault(symbol, []).append(f"{dataset}:CACHE_EMPTY")
                    continue
                financial_rows.extend(
                    {"_dataset": dataset, **dict(item["payload"])} for item in cached
                )
            if symbol_financial_refreshed:
                financial_refreshes += 1
            # Indicators are an optional enrichment dataset.  The three
            # statements remain a usable fundamental projection when the
            # provider has no indicators for a symbol; the original
            # INDICATORS failure is still retained in ``failures`` above.
            if financial_rows and all(
                any(row.get("_dataset") == dataset for row in financial_rows)
                for dataset in CORE_FINANCIAL_DATASETS
            ):
                # The complete revision history is already durable in SQLite.
                # Formal full-market runs provide a bounded projector here so
                # only the model-facing summary survives this iteration.  The
                # default preserves the historical public API for callers that
                # explicitly need every cached row.
                fundamental[symbol] = (
                    fundamental_projector(financial_rows)
                    if fundamental_projector is not None
                    else financial_rows
                )

            if symbol_hit:
                hits += 1
            else:
                misses += 1
            if symbol_updated:
                updated_symbols.append(symbol)
            if progress is not None and (index == len(ordered) or index % self.progress_every == 0):
                progress(
                    {
                        "processed": index,
                        "total": len(ordered),
                        "cache_hits": hits,
                        "cache_misses": misses,
                        "failures": len(failures),
                        "current_symbol": symbol,
                        "daily_updates": daily_updates,
                        "financial_refreshes": financial_refreshes,
                        "deferred_financial_refreshes": len(deferred_financial_symbols),
                    }
                )

        return SyncResult(
            daily=daily,
            fundamental=fundamental,
            failures=failures,
            processed=len(ordered),
            total=len(ordered),
            cache_hits=hits,
            cache_misses=misses,
            daily_updates=daily_updates,
            financial_refreshes=financial_refreshes,
            deferred_financial_refreshes=len(deferred_financial_symbols),
            updated_symbols=tuple(updated_symbols),
        )

    def _daily_ready(
        self,
        symbol: str,
        *,
        start: datetime,
        closed_daily_end: datetime,
        required_latest: datetime | None,
    ) -> bool:
        state = self.cache.get_sync_state("HITHINK_DAILY_1D", symbol)
        if not state or state.get("status") != "READY":
            return False
        if not state.get("last_success"):
            return False
        rows = self.cache.query_daily_bars(
            symbol,
            adjust="none",
            start=start,
            end=closed_daily_end,
            limit=30,
            descending=True,
        )
        if len(rows) < 30:
            return False
        latest = datetime.fromisoformat(str(rows[0]["timestamp"]))
        if required_latest is not None:
            return latest >= required_latest
        return latest >= closed_daily_end - timedelta(days=7)

    def _financial_due(self, state: Mapping[str, Any] | None, current: datetime) -> bool:
        """Return whether one dataset is due without retrying fresh failures.

        ``updated_at`` is the retry watermark for failed endpoints.  Otherwise
        an optional provider gap would consume the refresh budget on every
        research run and could starve older valid symbols indefinitely.
        """

        if not state:
            return True
        watermark = state.get("last_success")
        if state.get("status") != "READY":
            watermark = state.get("updated_at") or watermark
        if not watermark:
            return True
        observed = datetime.fromisoformat(str(watermark))
        return observed < current.astimezone(observed.tzinfo) - self.fundamental_refresh

    def _select_financial_refresh_symbols(
        self,
        symbols: Sequence[str],
        *,
        states: Mapping[tuple[str, str], Mapping[str, Any]],
        current: datetime,
    ) -> tuple[frozenset[str], frozenset[str]]:
        """Select the oldest due symbols; never cap the research universe.

        Missing core statements bypass the network budget because they cannot
        be reconstructed from stale cache.  Ordinary stale rows are rotated in
        oldest-first order and every non-selected symbol remains available to
        research from its durable cached statements.
        """

        endpoints = tuple(f"HITHINK_FINANCIAL_{name}" for name in FINANCIAL_DATASETS)
        core_endpoints = frozenset(
            f"HITHINK_FINANCIAL_{name}" for name in CORE_FINANCIAL_DATASETS
        )
        mandatory: set[str] = set()
        due: list[tuple[datetime, str]] = []
        far_past = datetime.min.replace(tzinfo=SHANGHAI)
        for symbol in symbols:
            symbol_states = [states.get((endpoint, symbol)) for endpoint in endpoints]
            if any(
                states.get((endpoint, symbol)) is None
                for endpoint in core_endpoints
            ):
                mandatory.add(symbol)
                continue
            due_states = [state for state in symbol_states if self._financial_due(state, current)]
            if not due_states:
                continue
            watermarks: list[datetime] = []
            for state in due_states:
                if not state:
                    watermarks.append(far_past)
                    continue
                raw = state.get("last_success") or state.get("updated_at")
                watermarks.append(datetime.fromisoformat(str(raw)) if raw else far_past)
            due.append((min(watermarks), symbol))
        due.sort(key=lambda item: (item[0], item[1]))
        budget = self.fundamental_refresh_symbols_per_run
        selected_due = {symbol for _watermark, symbol in due[:budget]}
        selected = frozenset(mandatory | selected_due)
        deferred = frozenset(symbol for _watermark, symbol in due[budget:])
        return selected, deferred


def _fetch_financial(client: HithinkClient, dataset: str, symbol: str) -> HithinkFetchResult:
    if dataset == "INCOME":
        return client.income_statements(symbol, limit=20, max_pages=1)
    if dataset == "INDICATORS":
        return client.financial_indicators(symbol, limit=100, max_pages=1)
    if dataset == "BALANCE":
        return client.balance_sheets(symbol, limit=20)
    if dataset == "CASH_FLOW":
        return client.cash_flow_statements(symbol, limit=20)
    raise ValueError("unsupported financial dataset")


def _row_time(row: Mapping[str, Any]) -> datetime:
    for key in ("date_ms", "timestamp", "time", "date", "bar_end"):
        value = row.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            return datetime.fromtimestamp(number / 1000 if abs(number) >= 1e11 else number, tz=SHANGHAI)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)
    raise ValueError("daily row timestamp missing")


def _latest_row_time(rows: Sequence[Any]) -> str | None:
    values = [_row_time(row.model_dump(mode="python")) for row in rows]
    return max(values).isoformat() if values else None


def _closed_daily_end(value: datetime) -> datetime:
    """Exclusive cutoff containing only fully closed A-share daily bars."""

    current = _aware(value)
    day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    if current.time().replace(tzinfo=None) >= datetime_time(15, 0):
        return day_start + timedelta(days=1)
    return day_start


def _report_period(row: Mapping[str, Any], fallback: datetime) -> str:
    for key in ("report_period", "report_date_ms", "period_end_ms", "end_date", "report"):
        value = row.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return datetime.fromtimestamp(float(value) / 1000, tz=SHANGHAI).date().isoformat()
        return str(value)[:40]
    index_id = str(row.get("index_id") or "")
    return f"{fallback.year}-INDICATORS-{index_id}"[:80]


def _published_at(row: Mapping[str, Any], fallback: datetime) -> datetime:
    for key in ("published_at", "publish_time", "announcement_time", "update_time"):
        value = row.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            return datetime.fromtimestamp(number / 1000 if abs(number) >= 1e11 else number, tz=SHANGHAI)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)
    return _aware(fallback)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    return value.astimezone(SHANGHAI)


__all__ = [
    "CORE_FINANCIAL_DATASETS",
    "FINANCIAL_DATASETS",
    "FundamentalProjector",
    "HithinkIncrementalSynchronizer",
    "SyncResult",
]
