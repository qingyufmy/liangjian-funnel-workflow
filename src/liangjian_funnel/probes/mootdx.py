from __future__ import annotations

import time
from datetime import datetime, time as datetime_time
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from ..contracts import CapabilityCheck, CapabilityReport, CapabilityStatus
from ..data.cache import CacheConflictError, MinuteBarStore
from ..data.mootdx import FetchResult, MootdxAdapter, detect_missing_bars
from ..data.quality import CrossCheckStatus, compare_prices
from ..redaction import safe_error
from ..settings import Settings


class MootdxProbe:
    def __init__(
        self,
        settings: Settings,
        *,
        adapter: MootdxAdapter | Any | None = None,
        store: MinuteBarStore | Any | None = None,
        hithink_transport: httpx.BaseTransport | None = None,
    ):
        self.settings = settings
        self.adapter = adapter or MootdxAdapter(
            nodes=settings.mootdx_servers,
            page_size=settings.mootdx_page_size,
            max_pages=settings.mootdx_max_pages,
            timeout_seconds=settings.mootdx_timeout_seconds,
        )
        self.hithink_transport = hithink_transport
        self.store = store

    def run(self, *, now: datetime | None = None) -> CapabilityReport:
        current = now or datetime.now(ZoneInfo(self.settings.timezone))
        checks: list[CapabilityCheck] = []

        one_minute, latency_1m = self._fetch("1m", 20)
        checks.append(_fetch_check("historical_1m", one_minute, latency_1m))

        five_minute, latency_5m = self._fetch("5m", self.settings.mootdx_history_5m_required_bars)
        checks.append(_fetch_check("history_5m_12240", five_minute, latency_5m))

        pool_ok = len(self.settings.mootdx_servers) >= 2 and bool(
            (one_minute and one_minute.complete) or (five_minute and five_minute.complete)
        )
        selected = (one_minute.server if one_minute and one_minute.complete else None) or (
            five_minute.server if five_minute and five_minute.complete else None
        )
        attempts = max(len(one_minute.attempts) if one_minute else 0, len(five_minute.attempts) if five_minute else 0)
        checks.append(
            CapabilityCheck(
                name="node_pool",
                status=CapabilityStatus.PASS if pool_ok else CapabilityStatus.BLOCKED,
                reason_code=None if pool_ok else "NODE_POOL_UNAVAILABLE",
                evidence={"configured_nodes": len(self.settings.mootdx_servers), "attempts": attempts, "selected_server": selected},
            )
        )

        checks.append(self._cache_check(one_minute, five_minute))

        raw_ok = bool(one_minute and five_minute and one_minute.complete and five_minute.complete) and all(
            bar.adjust_mode in {"none", "raw"} for result in (one_minute, five_minute) for bar in result.bars
        )
        checks.append(
            CapabilityCheck(
                name="raw_price_mode",
                status=CapabilityStatus.PASS if raw_ok else CapabilityStatus.BLOCKED,
                reason_code=None if raw_ok else "RAW_MODE_UNPROVEN",
            )
        )

        gap_count = None
        gaps_ok = False
        if one_minute and one_minute.complete:
            latest_day = one_minute.bars[-1].bar_end.date()
            day_bars = tuple(bar for bar in one_minute.bars if bar.bar_end.date() == latest_day)
            gaps = detect_missing_bars(day_bars, "1m", as_of=current)
            gap_count = len(gaps)
            gaps_ok = not gaps
        checks.append(
            CapabilityCheck(
                name="session_gap_detection",
                status=CapabilityStatus.PASS if gaps_ok else CapabilityStatus.BLOCKED,
                reason_code=None if gaps_ok else "MINUTE_BAR_GAP",
                evidence={"missing_closed_bars": gap_count},
            )
        )

        bj_result = self.adapter.fetch_bars("920002.BJ", "1m", 1)
        bj_ok = not bj_result.complete and bj_result.reason_code == "UNSUPPORTED_EXCHANGE"
        checks.append(
            CapabilityCheck(
                name="beijing_fail_closed",
                status=CapabilityStatus.PASS if bj_ok else CapabilityStatus.BLOCKED,
                reason_code=None if bj_ok else "BEIJING_NOT_BLOCKED",
            )
        )

        checks.append(self._cross_source_check(one_minute, current))
        overall = CapabilityStatus.PASS if all(check.status is CapabilityStatus.PASS for check in checks) else CapabilityStatus.BLOCKED
        return CapabilityReport(provider="MOOTDX", generated_at=current, overall_status=overall, checks=tuple(checks))

    def _cache_check(self, one_minute: FetchResult | None, five_minute: FetchResult | None) -> CapabilityCheck:
        if not one_minute or not five_minute or not one_minute.complete or not five_minute.complete:
            return CapabilityCheck(name="minute_cache_write", status=CapabilityStatus.BLOCKED, reason_code="VALIDATED_BARS_UNAVAILABLE")
        try:
            store = self.store or MinuteBarStore(self.settings.minute_cache_dir)
            result = store.write((*one_minute.bars, *five_minute.bars))
            return CapabilityCheck(
                name="minute_cache_write",
                status=CapabilityStatus.PASS,
                evidence={"inserted": result.inserted, "unchanged": result.unchanged},
            )
        except CacheConflictError:
            return CapabilityCheck(name="minute_cache_write", status=CapabilityStatus.BLOCKED, reason_code="MINUTE_CACHE_CONFLICT")
        except (OSError, ValueError):
            return CapabilityCheck(name="minute_cache_write", status=CapabilityStatus.BLOCKED, reason_code="MINUTE_CACHE_FAILED")

    def _fetch(self, interval: str, required_bars: int) -> tuple[FetchResult | None, int]:
        started = time.perf_counter()
        try:
            result = self.adapter.fetch_bars("600519.SH", interval, required_bars)
            return result, int((time.perf_counter() - started) * 1000)
        except Exception:
            return None, int((time.perf_counter() - started) * 1000)

    def _cross_source_check(self, minute: FetchResult | None, now: datetime) -> CapabilityCheck:
        if not minute or not minute.complete or not minute.bars:
            return CapabilityCheck(name="cross_source_latest_price", status=CapabilityStatus.BLOCKED, reason_code="MOOTDX_MINUTE_UNAVAILABLE")
        if self.settings.hithink_api_key is None:
            return CapabilityCheck(name="cross_source_latest_price", status=CapabilityStatus.BLOCKED, reason_code="HITHINK_API_KEY_MISSING")
        started = time.perf_counter()
        try:
            with httpx.Client(
                base_url=self.settings.hithink_base_url,
                timeout=self.settings.timeout_seconds,
                transport=self.hithink_transport,
                trust_env=False,
                headers={"X-api-key": self.settings.hithink_api_key.get_secret_value(), "Accept": "application/json"},
            ) as client:
                response = client.get("/api/a-share/prices/snapshot", params={"thscodes": "600519.SH"})
            latency = int((time.perf_counter() - started) * 1000)
            envelope = response.json()
            if response.status_code >= 400 or not isinstance(envelope, dict) or envelope.get("code") not in (0, "0"):
                return CapabilityCheck(name="cross_source_latest_price", status=CapabilityStatus.BLOCKED, http_status=response.status_code, latency_ms=latency, reason_code="HITHINK_SNAPSHOT_FAILED")
            data = envelope.get("data")
            items = data.get("item") if isinstance(data, dict) else None
            if not isinstance(items, list) or not items or not isinstance(items[0], dict):
                return CapabilityCheck(name="cross_source_latest_price", status=CapabilityStatus.BLOCKED, http_status=response.status_code, latency_ms=latency, reason_code="HITHINK_SNAPSHOT_INVALID")
            timestamp = data.get("timestamp")
            if not isinstance(timestamp, (int, float)):
                return CapabilityCheck(name="cross_source_latest_price", status=CapabilityStatus.BLOCKED, http_status=response.status_code, latency_ms=latency, reason_code="HITHINK_TIMESTAMP_INVALID")
            snapshot_time = datetime.fromtimestamp(float(timestamp) / 1000, tz=ZoneInfo(self.settings.timezone))
            latest = minute.bars[-1]
            comparison_mode, maximum_lag, time_reason = _comparison_window(now, snapshot_time, latest.bar_end)
            if time_reason:
                return CapabilityCheck(
                    name="cross_source_latest_price",
                    status=CapabilityStatus.BLOCKED,
                    http_status=response.status_code,
                    latency_ms=latency,
                    reason_code=time_reason,
                    evidence={"comparison_mode": comparison_mode},
                )
            comparison = compare_prices(
                hithink_price=items[0].get("last_price"),
                mootdx_price=latest.close,
                hithink_time=snapshot_time,
                mootdx_time=latest.bar_end,
                maximum_timestamp_lag_seconds=maximum_lag,
            )
            return CapabilityCheck(
                name="cross_source_latest_price",
                status=CapabilityStatus.PASS if comparison.status is CrossCheckStatus.PASS else CapabilityStatus.BLOCKED,
                http_status=response.status_code,
                latency_ms=latency,
                reason_code=comparison.reason_code,
                evidence={
                    "difference_pct": str(comparison.difference_pct) if comparison.difference_pct is not None else None,
                    "timestamp_lag_seconds": comparison.timestamp_lag_seconds,
                    "comparison_mode": comparison_mode,
                },
            )
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            return CapabilityCheck(
                name="cross_source_latest_price",
                status=CapabilityStatus.BLOCKED,
                latency_ms=int((time.perf_counter() - started) * 1000),
                reason_code="CROSS_SOURCE_CHECK_FAILED",
                evidence={"error": safe_error(exc)},
            )


def _fetch_check(name: str, result: FetchResult | None, latency_ms: int) -> CapabilityCheck:
    if result is None:
        return CapabilityCheck(name=name, status=CapabilityStatus.BLOCKED, latency_ms=latency_ms, reason_code="ADAPTER_FAILED")
    return CapabilityCheck(
        name=name,
        status=CapabilityStatus.PASS if result.complete else CapabilityStatus.BLOCKED,
        latency_ms=latency_ms,
        reason_code=None if result.complete else result.reason_code,
        evidence={
            "requested_bars": result.requested_bars,
            "returned_bars": result.returned_bars,
            "server": result.server,
            "attempts": len(result.attempts),
            "pages": result.attempts[-1].pages if result.attempts else 0,
            "first_bar_end": result.bars[0].bar_end.isoformat() if result.bars else None,
            "last_bar_end": result.bars[-1].bar_end.isoformat() if result.bars else None,
        },
    )


def _comparison_window(now: datetime, snapshot_time: datetime, bar_end: datetime) -> tuple[str, float, str | None]:
    local_now = now.astimezone(bar_end.tzinfo)
    current = local_now.timetz().replace(tzinfo=None)
    in_session = datetime_time(9, 30) <= current <= datetime_time(11, 30) or datetime_time(13, 0) <= current <= datetime_time(15, 0)
    if in_session:
        if snapshot_time.date() != bar_end.date():
            return "CONTINUOUS_SESSION", 90.0, "TRADING_DATE_MISMATCH"
        return "CONTINUOUS_SESSION", 90.0, None

    if snapshot_time < bar_end:
        return "CLOSED_SESSION", 0.0, "HITHINK_TIMESTAMP_BEFORE_BAR"
    if datetime_time(11, 30) < current < datetime_time(13, 0) and bar_end.date() == local_now.date():
        expected = datetime_time(11, 30)
        mode = "MIDDAY_BREAK"
    else:
        expected = datetime_time(15, 0)
        mode = "AFTER_CLOSE_OR_NONTRADING"
    if bar_end.timetz().replace(tzinfo=None) != expected:
        return mode, 0.0, "LATEST_CLOSED_BAR_INVALID"
    return mode, 4 * 24 * 60 * 60.0, None
