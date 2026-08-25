from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from liangjian_funnel.contracts import CapabilityStatus
from liangjian_funnel.probes.hithink import HithinkProbe
from liangjian_funnel.settings import Settings


NOW = datetime(2026, 8, 24, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


def _settings(tmp_path: Path, key: str | None = "probe-secret") -> Settings:
    env = {"HITHINK_FINANCE_API_KEY": key or ""}
    return Settings.from_env(env, root=tmp_path)


def test_missing_key_blocks_without_network(tmp_path: Path):
    report = HithinkProbe(_settings(tmp_path, None), sleep=lambda _: None).run(now=NOW)
    assert report.overall_status is CapabilityStatus.BLOCKED
    assert report.checks[0].reason_code == "HITHINK_API_KEY_MISSING"


def test_partial_capability_and_rate_limit_fail_closed_without_secret_leak(tmp_path: Path):
    secret = "probe-secret"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-api-key"] == secret
        if request.url.path.endswith("prices/snapshot"):
            return httpx.Response(429, headers={"Retry-After": "1"}, request=request)
        if request.url.path.endswith("adjustment-factors"):
            return httpx.Response(404, request=request)
        return httpx.Response(
            200,
            json={"code": 0, "message": "ok", "data": {"item": [{"ok": True}], "timestamp": 1}},
            request=request,
        )

    report = HithinkProbe(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None).run(now=NOW)
    assert report.overall_status is CapabilityStatus.BLOCKED
    checks = {item.name: item for item in report.checks}
    assert checks["realtime_snapshot"].reason_code == "RATE_LIMITED"
    assert checks["adjustment_events"].status is CapabilityStatus.UNVERIFIED
    assert secret not in report.model_dump_json()
    assert all("X-api-key" not in str(item.evidence) for item in report.checks)


def test_invalid_envelope_fails(tmp_path: Path):
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"unexpected": True}, request=request))
    report = HithinkProbe(_settings(tmp_path), transport=transport, sleep=lambda _: None).run(now=NOW)
    assert all(item.reason_code == "INVALID_ENVELOPE" for item in report.checks)


def test_items_alone_do_not_falsely_prove_raw_or_adjustment_events(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"code": 0, "data": {"item": [{"date_ms": 1}, {"date_ms": 86_400_001}]}},
            request=request,
        )

    report = HithinkProbe(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None).run(now=NOW)
    checks = {item.name: item for item in report.checks}
    assert checks["raw_price_mode"].status is CapabilityStatus.UNVERIFIED
    assert checks["adjustment_events"].status is CapabilityStatus.UNVERIFIED


def test_business_error_is_reported_before_data_shape(tmp_path: Path):
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"code": 1002, "data": None}, request=request)
    )
    report = HithinkProbe(_settings(tmp_path), transport=transport, sleep=lambda _: None).run(now=NOW)
    assert all(item.status is CapabilityStatus.UNVERIFIED for item in report.checks)
    assert all(item.reason_code == "BUSINESS_ERROR" for item in report.checks)
