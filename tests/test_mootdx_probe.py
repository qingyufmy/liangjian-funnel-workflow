from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from liangjian_funnel.contracts import CapabilityStatus
from liangjian_funnel.data.mootdx import FetchResult, MinuteBar, NodeAttempt
from liangjian_funnel.probes.mootdx import MootdxProbe
from liangjian_funnel.settings import Settings


ZONE = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 24, 18, 0, tzinfo=ZONE)


def _bar(at: datetime, interval: str) -> MinuteBar:
    return MinuteBar(
        symbol="600519.SH",
        interval=interval,
        bar_end=at,
        open=100,
        high=100,
        low=100,
        close=100,
        volume=100,
        amount=10000,
        source_id="MOOTDX:127.0.0.1:7709",
        adjust_mode="none",
    )


class FakeAdapter:
    def fetch_bars(self, symbol: str, interval: str, required_bars: int) -> FetchResult:
        if symbol.endswith(".BJ"):
            return FetchResult(
                symbol=symbol,
                interval=interval,
                requested_bars=required_bars,
                returned_bars=0,
                reason_code="UNSUPPORTED_EXCHANGE",
                complete=False,
            )
        if interval == "1m":
            start = datetime(2026, 8, 24, 14, 41, tzinfo=ZONE)
            bars = tuple(_bar(start + timedelta(minutes=index), "1m") for index in range(required_bars))
        else:
            end = datetime(2026, 8, 24, 15, 0, tzinfo=ZONE)
            bars = tuple(_bar(end - timedelta(minutes=5 * (required_bars - index - 1)), "5m") for index in range(required_bars))
        return FetchResult(
            symbol="600519.SH",
            interval=interval,
            requested_bars=required_bars,
            returned_bars=len(bars),
            bars=bars,
            server="127.0.0.1:7709",
            attempts=(NodeAttempt(server="127.0.0.1:7709", pages=1, returned_bars=len(bars), reason_code="OK"),),
            reason_code="OK",
            complete=True,
        )


def _settings(tmp_path: Path) -> Settings:
    return Settings.from_env(
        {
            "HITHINK_FINANCE_API_KEY": "test-secret",
            "LIANGJIAN_MODEL_API_KEY": "test-secret",
            "MOOTDX_SERVERS": "127.0.0.1:7709,127.0.0.2:7709",
            "MOOTDX_HISTORY_5M_REQUIRED_BARS": "255",
        },
        root=tmp_path,
    )


def test_composite_minute_probe_passes_without_leaking_key(tmp_path: Path):
    timestamp = int(datetime(2026, 8, 24, 15, 0, tzinfo=ZONE).timestamp() * 1000)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-api-key"] == "test-secret"
        return httpx.Response(
            200,
            json={"code": 0, "data": {"timestamp": timestamp, "item": [{"last_price": 100}]}},
            request=request,
        )

    report = MootdxProbe(
        _settings(tmp_path),
        adapter=FakeAdapter(),
        hithink_transport=httpx.MockTransport(handler),
    ).run(now=NOW)
    assert report.overall_status is CapabilityStatus.PASS
    assert all(check.status is CapabilityStatus.PASS for check in report.checks)
    assert "test-secret" not in report.model_dump_json()


def test_cross_source_timestamp_mismatch_blocks(tmp_path: Path):
    stale_timestamp = int(datetime(2026, 8, 24, 14, 0, tzinfo=ZONE).timestamp() * 1000)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json={"code": 0, "data": {"timestamp": stale_timestamp, "item": [{"last_price": 100}]}},
            request=request,
        )
    )
    report = MootdxProbe(_settings(tmp_path), adapter=FakeAdapter(), hithink_transport=transport).run(now=NOW)
    checks = {check.name: check for check in report.checks}
    assert report.overall_status is CapabilityStatus.BLOCKED
    assert checks["cross_source_latest_price"].reason_code == "HITHINK_TIMESTAMP_BEFORE_BAR"


def test_after_close_snapshot_assembly_time_is_allowed(tmp_path: Path):
    after_close_timestamp = int(datetime(2026, 8, 24, 18, 0, tzinfo=ZONE).timestamp() * 1000)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json={"code": 0, "data": {"timestamp": after_close_timestamp, "item": [{"last_price": 100}]}},
            request=request,
        )
    )
    report = MootdxProbe(_settings(tmp_path), adapter=FakeAdapter(), hithink_transport=transport).run(now=NOW)
    checks = {check.name: check for check in report.checks}
    assert checks["cross_source_latest_price"].status is CapabilityStatus.PASS
    assert checks["cross_source_latest_price"].evidence["comparison_mode"] == "AFTER_CLOSE_OR_NONTRADING"
