from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

import httpx

from ..contracts import CapabilityCheck, CapabilityReport, CapabilityStatus
from ..redaction import safe_error
from ..settings import Settings


class HithinkProbe:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.settings = settings
        self.transport = transport
        self.sleep = sleep

    def run(self, *, now: datetime | None = None) -> CapabilityReport:
        current = now or datetime.now(ZoneInfo(self.settings.timezone))
        if self.settings.hithink_api_key is None:
            return CapabilityReport(
                provider="HITHINK",
                generated_at=current,
                overall_status=CapabilityStatus.BLOCKED,
                checks=(CapabilityCheck(name="credentials", status=CapabilityStatus.BLOCKED, reason_code="HITHINK_API_KEY_MISSING"),),
            )
        key = self.settings.hithink_api_key.get_secret_value()
        with httpx.Client(
            base_url=self.settings.hithink_base_url,
            timeout=self.settings.timeout_seconds,
            transport=self.transport,
            trust_env=False,
            headers={"X-api-key": key, "Accept": "application/json"},
        ) as client:
            checks = tuple(self._run_checks(client, current))
        overall = _overall(
            checks,
            critical={"ticker_catalog", "realtime_snapshot", "raw_price_mode", "adjustment_events"},
        )
        return CapabilityReport(provider="HITHINK", generated_at=current, overall_status=overall, checks=checks)

    def _run_checks(self, client: httpx.Client, now: datetime) -> list[CapabilityCheck]:
        day = now.date()
        start_ms = int(datetime.combine(day - timedelta(days=7), datetime.min.time(), tzinfo=now.tzinfo).timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)
        symbol = "600519.SH"
        specs: list[tuple[str, str, dict[str, Any], Callable[[dict[str, Any]], bool] | None]] = [
            ("trading_calendar", "/api/a-share/calendar/trading-days", {}, _has_items),
            ("ticker_catalog", "/api/meta/tickers/list", {"exchange": "SH,SZ,BJ", "asset_type": "a-share", "limit": 10, "offset": 0}, _has_items),
            ("realtime_snapshot", "/api/a-share/prices/snapshot", {"thscodes": symbol}, _has_items),
            ("historical_1d", "/api/a-share/prices/historical", {"thscode": symbol, "interval": "1d", "start": start_ms, "end": end_ms, "adjust": "forward"}, _has_items),
            ("raw_price_mode", "/api/a-share/prices/historical", {"thscode": symbol, "interval": "1d", "start": start_ms, "end": end_ms, "adjust": "none"}, _proves_raw_mode),
            (
                "adjustment_events",
                "/api/a-share/corporate-actions/adjustment-factors",
                {"thscode": symbol},
                _proves_adjustment_events,
            ),
            ("auction_final", "/api/a-share/auction/snapshot", {"thscodes": symbol, "stage": "final"}, _has_items),
            ("financial_statements", "/api/a-share/financials/income-statements", {"thscode": symbol, "period": "quarterly", "limit": 2}, _has_items),
        ]
        checks: list[CapabilityCheck] = []
        for index, (name, path, params, validator) in enumerate(specs):
            if index:
                self.sleep(self.settings.hithink_min_request_interval_seconds)
            checks.append(self._request(client, name, path, params, validator))
        return checks

    def _request(
        self,
        client: httpx.Client,
        name: str,
        path: str,
        params: dict[str, Any],
        validator: Callable[[dict[str, Any]], bool] | None,
    ) -> CapabilityCheck:
        started = time.perf_counter()
        try:
            response = client.get(path, params=params)
            latency = int((time.perf_counter() - started) * 1000)
            if response.status_code == 429:
                return CapabilityCheck(name=name, status=CapabilityStatus.BLOCKED, latency_ms=latency, http_status=429, reason_code="RATE_LIMITED")
            if response.status_code >= 400:
                status = CapabilityStatus.UNVERIFIED if response.status_code in {400, 404, 405, 422} else CapabilityStatus.FAIL
                return CapabilityCheck(name=name, status=status, latency_ms=latency, http_status=response.status_code, reason_code="HTTP_ERROR")
            envelope = response.json()
            if not isinstance(envelope, dict):
                return CapabilityCheck(name=name, status=CapabilityStatus.FAIL, latency_ms=latency, http_status=response.status_code, reason_code="INVALID_ENVELOPE")
            if "code" not in envelope:
                return CapabilityCheck(name=name, status=CapabilityStatus.FAIL, latency_ms=latency, http_status=response.status_code, reason_code="INVALID_ENVELOPE")
            business_code = envelope.get("code")
            if business_code not in (0, "0"):
                return CapabilityCheck(
                    name=name, status=CapabilityStatus.UNVERIFIED, latency_ms=latency, http_status=response.status_code,
                    reason_code="BUSINESS_ERROR", evidence={"business_code": business_code},
                )
            data = envelope.get("data")
            if not isinstance(data, dict):
                return CapabilityCheck(name=name, status=CapabilityStatus.FAIL, latency_ms=latency, http_status=response.status_code, reason_code="INVALID_ENVELOPE")
            item = data.get("item")
            evidence = {
                "top_level_data_keys": sorted(str(key) for key in data.keys())[:30],
                "item_count": len(item) if isinstance(item, list) else None,
                "timestamp_present": data.get("timestamp") is not None,
                "rate_limit_headers": _safe_rate_headers(response.headers),
            }
            passed = validator(data) if validator else True
            return CapabilityCheck(
                name=name,
                status=CapabilityStatus.PASS if passed else CapabilityStatus.UNVERIFIED,
                latency_ms=latency,
                http_status=response.status_code,
                evidence=evidence,
                reason_code=None if passed else "EMPTY_OR_UNPROVEN",
            )
        except (httpx.HTTPError, ValueError) as exc:
            return CapabilityCheck(
                name=name,
                status=CapabilityStatus.FAIL,
                latency_ms=int((time.perf_counter() - started) * 1000),
                reason_code="REQUEST_FAILED",
                evidence={"error": safe_error(exc)},
            )


def _has_items(data: dict[str, Any]) -> bool:
    return isinstance(data.get("item"), list) and bool(data["item"])


def _proves_raw_mode(data: dict[str, Any]) -> bool:
    declared = str(data.get("adjust") or data.get("adjust_mode") or data.get("price_mode") or "").lower()
    return _has_items(data) and declared in {"none", "raw", "unadjusted", "no_adjust"}


def _proves_adjustment_events(data: dict[str, Any]) -> bool:
    rows = data.get("item")
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        return False
    keys = {str(key).lower() for key in rows[0]}
    return {"ex_date_ms", "dividend_per_share", "per_share_bonus"}.issubset(keys)


def _safe_rate_headers(headers: httpx.Headers) -> dict[str, str]:
    allowed = {"retry-after", "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"}
    return {key.lower(): value for key, value in headers.items() if key.lower() in allowed}


def _overall(checks: tuple[CapabilityCheck, ...], *, critical: set[str]) -> CapabilityStatus:
    by_name = {check.name: check for check in checks}
    if any(by_name[name].status is not CapabilityStatus.PASS for name in critical if name in by_name):
        return CapabilityStatus.BLOCKED
    if all(check.status is CapabilityStatus.PASS for check in checks):
        return CapabilityStatus.PASS
    return CapabilityStatus.PARTIAL
