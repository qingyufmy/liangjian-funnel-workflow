"""Incremental HiThink synchronization backed by :mod:`local_fact_cache`."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, time as datetime_time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .data_source import HithinkClient, HithinkFetchResult
from .local_fact_cache import LocalFactCache
from .feature_store import content_hash
from .early_discovery import discover_early_setups, merge_discovery_parts
from ..runtime.calendar import ExchangeTradingCalendar


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
    early_discovery: dict[str, Any] = field(default_factory=dict)
    daily_requests: dict[str, dict[str, Any]] = field(default_factory=dict)


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
        trading_calendar: ExchangeTradingCalendar | None = None,
    ) -> None:
        self.cache = cache
        self.fundamental_refresh = timedelta(hours=max(1, int(fundamental_refresh_hours)))
        self.fundamental_refresh_symbols_per_run = max(
            0, int(fundamental_refresh_symbols_per_run)
        )
        self.daily_refresh = timedelta(hours=max(1, int(daily_refresh_hours)))
        self.progress_every = max(1, int(progress_every))
        self.batch_size = max(1, int(batch_size))
        self.trading_calendar = trading_calendar or ExchangeTradingCalendar()

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
        collect_early_discovery: bool = False,
        include_financial: bool = True,
        daily_reset_reasons: Mapping[str, str] | None = None,
        daily_batch_callback: Callable[[Mapping[str, Any], Mapping[str, Any]], None] | None = None,
    ) -> SyncResult:
        current = _aware(as_of)
        ordered = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols))
        failures: dict[str, list[str]] = {}
        daily: dict[str, list[dict[str, Any]]] = {}
        fundamental: dict[str, Any] = {}
        updated_symbols: list[str] = []
        stale_daily: list[str] = []
        discovery_parts: dict[str, dict[str, Any]] = {}
        daily_requests: dict[str, dict[str, Any]] = {}
        reset_blocked: set[str] = set()
        reset_reasons = dict(daily_reset_reasons or {})
        if any(reason not in {'ADJUSTMENT_FACTOR_CHANGED', 'HISTORICAL_REVISION_CONFIRMED'}
               for reason in reset_reasons.values()):
            raise ValueError('UNVERIFIED_DAILY_RESET_REASON')
        hits = 0
        misses = 0
        daily_updates = 0
        financial_refreshes = 0
        start = current - timedelta(days=max(1, int(lookback_days)))
        batch_symbols: list[str] = []
        required_latest_daily, closed_daily_end = _closed_daily_window(
            current,
            self.trading_calendar,
        )
        financial_states = {
            (str(state.get("endpoint") or ""), str(state.get("symbol") or "")): state
            for state in (self.cache.list_sync_state() if include_financial else ())
            if str(state.get("endpoint") or "").startswith("HITHINK_FINANCIAL_")
        }
        financial_refresh_symbols, deferred_financial_symbols = (
            self._select_financial_refresh_symbols(
                ordered,
                states=financial_states,
                current=current,
            )
        ) if include_financial else (set(), set())

        for index, symbol in enumerate(ordered, start=1):
            symbol_hit = True
            symbol_updated = False
            state = self.cache.get_sync_state('HITHINK_DAILY_1D', symbol)
            cursor = state.get('cursor') if state else None
            pending_reset = cursor.get('pending_reset_reason') if isinstance(cursor, Mapping) else None
            history_coverage = cursor.get('history_coverage') if isinstance(cursor, Mapping) else None
            if pending_reset:
                if pending_reset not in {'ADJUSTMENT_FACTOR_CHANGED', 'HISTORICAL_REVISION_CONFIRMED'}:
                    raise ValueError('UNVERIFIED_DAILY_RESET_REASON')
                reset_reasons.setdefault(symbol, pending_reset)
            # One bounded cache read serves the readiness check, incremental
            # cursor and discovery. A future bar is never the request cursor.
            rows = self.cache.query_daily_bars(
                symbol, adjust='none', start=start, end=closed_daily_end,
                limit=None if collect_early_discovery else max(30, compact_daily_bars),
                descending=True,
            )
            daily_ready = symbol not in reset_reasons and self._daily_ready(
                symbol,
                start=start,
                closed_daily_end=closed_daily_end,
                required_latest=required_latest_daily,
                cached_rows=rows,
                cached_state=state,
            )
            daily_requests[symbol] = {'mode': 'CACHE_HIT', 'reason_code': 'LATEST_CLOSED_DAY_READY',
                                      'adjust': 'none', 'factor_detection': 'NOT_PROVIDED_BY_ENDPOINT'}
            if not daily_ready:
                symbol_hit = False
                latest = rows[0] if rows else None
                reason = reset_reasons.get(symbol)
                request_start_ms = int(start.timestamp() * 1000)
                mode = 'FULL_REFRESH'
                if reason is None:
                    if len(rows) < 30 and not _complete_short_history(state, start, rows):
                        reason = 'HISTORY_SHORT_BOOTSTRAP'
                    elif latest is not None and (required_latest_daily is None or
                          _aware(datetime.fromisoformat(str(latest['timestamp']))) < required_latest_daily):
                        # Recheck actual cached sessions, not three calendar
                        # days. Publication metadata is not an OHLCV revision.
                        request_start_ms = max(request_start_ms,
                            int(_aware(datetime.fromisoformat(str(rows[2]['timestamp']))).timestamp()*1000))
                        mode, reason = 'INCREMENTAL', 'LAST_THREE_CLOSED_BARS_OVERLAP'
                    else:
                        # A failed/missing source receipt is not repaired by
                        # marking its cached rows ready without revalidation.
                        reason = 'SOURCE_RECEIPT_REVALIDATION'
                daily_requests[symbol] = {'mode': mode, 'reason_code': reason, 'adjust': 'none',
                    'start_ms': request_start_ms, 'end_ms': int(closed_daily_end.timestamp()*1000),
                    'factor_detection': 'EXPLICIT_RESET_EVIDENCE' if symbol in reset_reasons else 'NOT_PROVIDED_BY_ENDPOINT'}
                result = client.history_1d(
                    symbol,
                    start=request_start_ms,
                    end=int(closed_daily_end.timestamp() * 1000),
                    adjust="none",
                    limit=1000,
                    max_pages=1,
                )
                closed_items = tuple(
                    row
                    for row in result.items
                    if request_start_ms <= int(_row_time(row.model_dump(mode="python")).timestamp()*1000)
                    < int(closed_daily_end.timestamp()*1000)
                )
                overlap_complete = True
                if mode == 'INCREMENTAL' and not (result.ok and result.complete):
                    overlap_complete = False
                    reset_blocked.add(symbol)
                if mode == 'INCREMENTAL' and result.ok and result.complete:
                    returned = {int(_row_time(row.model_dump(mode='python')).timestamp()*1000):
                                row.model_dump(mode='python') for row in closed_items}
                    overlap = {int(_aware(datetime.fromisoformat(str(row['timestamp']))).timestamp()*1000):
                               row['payload'] for row in rows[:3]}
                    overlap_complete = overlap.keys() <= returned.keys() and len(returned) == len(closed_items)
                    revised = sorted(stamp for stamp, payload in overlap.items()
                        if stamp in returned and _daily_value_hash(payload) != _daily_value_hash(returned[stamp]))
                    daily_requests[symbol].update(overlap_expected_rows=len(overlap),
                        overlap_complete=overlap_complete, revised_timestamps_ms=revised,
                        overlap_received_at=result.fetch_time.isoformat(),
                        overlap_source_hash=content_hash([r.model_dump(mode='json') for r in closed_items]),
                        revision_evidence=[{'timestamp_ms': stamp,
                            'cached_hash': _daily_value_hash(overlap[stamp]),
                            'source_hash': _daily_value_hash(returned[stamp])} for stamp in revised])
                    if not overlap_complete:
                        reset_blocked.add(symbol)
                    elif revised:
                        # Persist before the network request. An interrupted or
                        # partial rebuild must remain blocked on the next run.
                        reset_reasons[symbol] = 'HISTORICAL_REVISION_CONFIRMED'
                        self.cache.update_sync_state('HITHINK_DAILY_1D', symbol,
                            status='FAILED', reason='HISTORICAL_REVISION_CONFIRMED',
                            cursor={'pending_reset_reason': reset_reasons[symbol],
                                    'request': daily_requests[symbol]})
                        request_start_ms = int(start.timestamp()*1000)
                        daily_requests[symbol].update(mode='FULL_REFRESH',
                            reason_code=reset_reasons[symbol], start_ms=request_start_ms)
                        result = client.history_1d(symbol, start=request_start_ms,
                            end=int(closed_daily_end.timestamp()*1000), adjust='none', limit=1000, max_pages=1)
                        closed_items = tuple(row for row in result.items
                            if request_start_ms <= int(_row_time(row.model_dump(mode='python')).timestamp()*1000)
                            < int(closed_daily_end.timestamp()*1000))
                reset_complete = True
                if symbol in reset_reasons:
                    reset_history = self.cache.query_daily_bars(
                        symbol, adjust='none', start=start, end=closed_daily_end, limit=None,
                    )
                    required_stamps = {int(_aware(datetime.fromisoformat(str(row['timestamp']))).timestamp()*1000)
                                       for row in reset_history}
                    returned_stamps = {int(_row_time(row.model_dump(mode='python')).timestamp()*1000)
                                       for row in closed_items}
                    reset_complete = required_stamps <= returned_stamps
                    if not reset_complete:
                        reset_blocked.add(symbol)
                daily_requests[symbol].update({'returned_closed_rows': len(closed_items),
                    'source_ok': result.ok, 'source_complete': result.complete,
                    'source_reason_code': result.reason_code,
                    'received_at': result.fetch_time.isoformat()})
                if result.ok and result.complete and closed_items and reset_complete and overlap_complete:
                    if daily_requests[symbol]['mode'] == 'FULL_REFRESH':
                        coverage = {'start_ms':request_start_ms,
                            'end_ms':int(closed_daily_end.timestamp()*1000), 'adjust':'none',
                            'source_ok':True,'source_complete':True,
                            'received_at':result.fetch_time.isoformat(),
                            'row_count':len(closed_items),
                            'source_hash':content_hash([r.model_dump(mode='json') for r in closed_items])}
                        coverage['coverage_hash'] = content_hash(coverage)
                        history_coverage = coverage
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
                        cursor={"through": _latest_row_time(closed_items),
                                'request': daily_requests[symbol],
                                'history_coverage':history_coverage},
                        status="READY",
                        reason=None,
                    )
                    symbol_updated = True
                    daily_updates += 1
                else:
                    reason = ('INCREMENTAL_OVERLAP_INCOMPLETE' if not overlap_complete else
                              'FULL_REFRESH_HISTORY_INCOMPLETE' if not reset_complete else
                              result.reason_code if not result.ok else "NO_CLOSED_DAILY_BARS")
                    failures.setdefault(symbol, []).append(f"DAILY:{reason}")
                    self.cache.update_sync_state(
                        "HITHINK_DAILY_1D", symbol, status="FAILED", reason=reason,
                        **({'cursor': {'pending_reset_reason': reset_reasons[symbol],
                                       'request': daily_requests[symbol]}}
                           if symbol in reset_reasons else {}),
                    )

                rows = self.cache.query_daily_bars(
                    symbol, adjust='none', start=start, end=closed_daily_end,
                    limit=None if collect_early_discovery else max(30, compact_daily_bars), descending=True,
                )
            if collect_early_discovery:
                discovery_parts[symbol] = discover_early_setups({symbol: [
                    {**row["payload"], "timestamp": row["timestamp"], "adjust": row["adjust"]}
                    for row in reversed(rows)]}, as_of=current, symbols=[symbol])
            if rows:
                daily[symbol] = [dict(item["payload"]) for item in reversed(rows[:compact_daily_bars])]
                if required_latest_daily is not None and _row_time(daily[symbol][-1]) < required_latest_daily:
                    failures.setdefault(symbol, []).append("DAILY:LATEST_CLOSED_DAY_MISSING")
                    self.cache.update_sync_state(
                        "HITHINK_DAILY_1D", symbol, status="FAILED", reason="LATEST_CLOSED_DAY_MISSING"
                    )
                    if symbol not in reset_blocked and symbol not in reset_reasons:
                        stale_daily.append(symbol)
            else:
                failures.setdefault(symbol, []).append("DAILY:CACHE_EMPTY")

            if daily_batch_callback is not None:
                batch_symbols.append(symbol)
                if len(batch_symbols) >= self.batch_size or index == len(ordered):
                    daily_batch_callback(
                        {s: daily.get(s, []) for s in batch_symbols},
                        {s: list(failures[s]) for s in batch_symbols if s in failures},
                    )
                    batch_symbols.clear()

            financial_rows: list[dict[str, Any]] = []
            symbol_financial_refreshed = False
            for dataset in FINANCIAL_DATASETS if include_financial else ():
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

        # A provider can acknowledge the request while its latest daily bar
        # is still being published. After the full pass, retry at most ten
        # symbols once; never turn a successful HTTP response into freshness.
        for symbol in stale_daily[:10]:
            result = client.history_1d(
                symbol, start=int(required_latest_daily.timestamp() * 1000),
                end=int(closed_daily_end.timestamp() * 1000), adjust="none", limit=1000, max_pages=1,
            )
            closed_items = tuple(
                row for row in result.items
                if required_latest_daily
                <= _row_time(row.model_dump(mode="python")) < closed_daily_end
            )
            if not (result.ok and result.complete and closed_items
                    and max(_row_time(row.model_dump(mode="python")) for row in closed_items) >= required_latest_daily):
                continue
            self.cache.upsert_daily_bars(({
                "symbol": symbol, "timestamp": _row_time(row.model_dump(mode="python")),
                "adjust": "none", "fetched_at": result.fetch_time,
                "payload": row.model_dump(mode="json"),
            } for row in closed_items), batch_size=self.batch_size)
            self.cache.update_sync_state(
                "HITHINK_DAILY_1D", symbol, last_success=result.fetch_time,
                cursor={"through": _latest_row_time(closed_items), 'request': daily_requests[symbol],
                        'latest_day_retry': True}, status="READY", reason=None,
            )
            rows = self.cache.query_daily_bars(
                symbol, adjust="none", start=start, end=closed_daily_end,
                limit=None if collect_early_discovery else compact_daily_bars, descending=True,
            )
            if collect_early_discovery:
                discovery_parts[symbol] = discover_early_setups({symbol: [
                    {**row["payload"], "timestamp": row["timestamp"], "adjust": row["adjust"]}
                    for row in reversed(rows)]}, as_of=current, symbols=[symbol])
            daily[symbol] = [dict(item["payload"]) for item in reversed(rows[:compact_daily_bars])]
            remaining = [reason for reason in failures.get(symbol, ()) if not reason.startswith("DAILY:")]
            if remaining:
                failures[symbol] = remaining
            else:
                failures.pop(symbol, None)
            if symbol not in updated_symbols:
                updated_symbols.append(symbol)
            if daily_batch_callback is not None:
                daily_batch_callback({symbol: daily[symbol]},
                                     {symbol: list(failures[symbol])} if symbol in failures else {})

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
            early_discovery=merge_discovery_parts(list(discovery_parts.values()), as_of=current) if collect_early_discovery else {},
            daily_requests=daily_requests,
        )

    def _daily_ready(
        self,
        symbol: str,
        *,
        start: datetime,
        closed_daily_end: datetime,
        required_latest: datetime | None,
        cached_rows: Sequence[Mapping[str, Any]] | None = None,
        cached_state: Mapping[str, Any] | None = None,
    ) -> bool:
        state = cached_state if cached_state is not None else self.cache.get_sync_state("HITHINK_DAILY_1D", symbol)
        if not state or state.get("status") != "READY":
            return False
        if not state.get("last_success"):
            return False
        rows = cached_rows if cached_rows is not None else self.cache.query_daily_bars(
            symbol,
            adjust="none",
            start=start,
            end=closed_daily_end,
            limit=30,
            descending=True,
        )
        if len(rows) < 30 and not _complete_short_history(state, start, rows):
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


def _daily_value_hash(row: Mapping[str, Any]) -> str:
    """Price/volume identity, independent of fetch metadata and int/float form."""
    fields = ('open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover')
    return content_hash({key: float(value) if isinstance(value, (int, float))
                         and not isinstance(value, bool) else value
                         for key in fields for value in (row.get(key),)})


def _latest_row_time(rows: Sequence[Any]) -> str | None:
    values = [_row_time(row.model_dump(mode="python")) for row in rows]
    return max(values).isoformat() if values else None


def _closed_daily_window(
    value: datetime,
    calendar: ExchangeTradingCalendar,
) -> tuple[datetime, datetime]:
    """Return latest closed session and its exclusive daily-bar cutoff.

    Wall-clock dates are not exchange sessions.  On weekends, exchange
    holidays and a trading-day morning, the latest fully closed daily bar is
    the previous session.  After 15:00 on a trading day it is the current
    session.  The calendar is deliberately fail-closed; callers must not
    silently fall back to weekday arithmetic when the session table is
    unavailable.
    """

    current = _aware(value)
    current_date = current.date()
    current_session_closed = (
        calendar.is_trading_day(current_date)
        and current.time().replace(tzinfo=None) >= datetime_time(15, 0)
    )
    latest_session = (
        current_date
        if current_session_closed
        else calendar.previous_trading_day(current_date)
    )
    required_latest = datetime.combine(
        latest_session,
        datetime_time.min,
        tzinfo=SHANGHAI,
    )
    return required_latest, required_latest + timedelta(days=1)


def _report_period(row: Mapping[str, Any], fallback: datetime) -> str:
    # Report publication and the fiscal period are different dates. Never
    # use report_date_ms as a period identifier (amendments would look like
    # new fiscal periods and historical statements could outrank current ones).
    for key in ("report_period", "period_end_ms", "end_date", "report"):
        value = row.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return datetime.fromtimestamp(float(value) / 1000, tz=SHANGHAI).date().isoformat()
        return str(value)[:40]
    index_id = str(row.get("index_id") or "")
    return f"{fallback.year}-INDICATORS-{index_id}"[:80]


def _published_at(row: Mapping[str, Any], fallback: datetime) -> datetime:
    for key in ("published_at", "publish_time", "announcement_time", "report_date_ms", "update_time"):
        value = row.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            return datetime.fromtimestamp(number / 1000 if abs(number) >= 1e11 else number, tz=SHANGHAI)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)
    return _aware(fallback)


def _complete_short_history(state: Mapping | None, start: datetime,
                            rows: Sequence[Mapping]) -> bool:
    """Transport completeness is not technical eligibility or suspension proof.

    Only a successful full-range source receipt may avoid daily bootstrap.
    Legacy cursors or fewer than three overlap bars cannot establish this.
    """
    if not state or state.get('status') != 'READY' or len(rows) < 3:
        return False
    cursor = state.get('cursor')
    coverage = cursor.get('history_coverage') if isinstance(cursor, Mapping) else None
    if not isinstance(coverage, Mapping):
        return False
    if content_hash({k:v for k,v in coverage.items() if k != 'coverage_hash'}) != coverage.get('coverage_hash'):
        return False
    if (coverage.get('adjust') != 'none' or coverage.get('source_ok') is not True
            or coverage.get('source_complete') is not True
            or not isinstance(coverage.get('source_hash'), str)
            or len(coverage['source_hash']) != 64):
        return False
    try:
        return (int(coverage['start_ms']) <= int(start.timestamp()*1000)
            and int(coverage['end_ms']) > int(coverage['start_ms'])
            and int(coverage['row_count']) >= 3
            and _aware(datetime.fromisoformat(coverage['received_at']))
                <= _aware(datetime.fromisoformat(state['last_success'])))
    except (ValueError, TypeError, KeyError):
        return False


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
