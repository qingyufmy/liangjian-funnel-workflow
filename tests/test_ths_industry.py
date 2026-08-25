from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.data.ths_industry import collect_ths_industry_membership
from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 25, 9, 25, tzinfo=TZ)


def _result(endpoint: str, rows: list[dict], *, ok: bool = True, reason: str = "OK") -> HithinkFetchResult:
    return HithinkFetchResult(
        endpoint=endpoint,
        ok=ok,
        complete=ok,
        reason_code=reason,
        items=tuple(HithinkRow.model_validate(row) for row in rows),
        pages=1,
        total=len(rows),
        fetch_time=NOW,
        metadata={"timestamp": int(NOW.timestamp() * 1000)},
    )


class FakeClient:
    def __init__(self, responses: dict[str, HithinkFetchResult]):
        self.responses = responses
        self.calls: list[str] = []

    def ths_index_constituents(self, thscode: str) -> HithinkFetchResult:
        self.calls.append(thscode)
        return self.responses[thscode]


def test_membership_builds_reverse_map_and_reuses_complete_daily_cache(tmp_path) -> None:
    catalog = _result("/catalog", [
        {"thscode": "881101.TI", "name": "食品饮料"},
        {"thscode": "884001.TI", "name": "白酒"},
    ])
    first_client = FakeClient({
        "881101.TI": _result("/members", [
            {"thscode": "600519.SH", "ticker": "600519", "name": "贵州茅台"},
            {"thscode": "000858.SZ", "ticker": "000858", "name": "五粮液"},
        ]),
        "884001.TI": _result("/members", [
            {"thscode": "600519.SH", "ticker": "600519", "name": "贵州茅台"},
        ]),
    })

    first = collect_ths_industry_membership(
        first_client,
        catalog,
        ["600519.SH", "000001.SZ"],
        cache_dir=tmp_path,
        as_of=NOW,
        sleep=lambda _: None,
    )

    assert first.ok and first.complete
    assert first_client.calls == ["881101.TI", "884001.TI"]
    rows = {row.model_dump()["thscode"]: row.model_dump() for row in first.items}
    assert rows["600519.SH"]["mapping_status"] == "MAPPED"
    assert [item["industry_thscode"] for item in rows["600519.SH"]["memberships"]] == [
        "881101.TI", "884001.TI"
    ]
    assert rows["000001.SZ"]["mapping_status"] == "UNMAPPED"
    assert first.metadata["membership_coverage"] == 0.5

    second_client = FakeClient({})
    second = collect_ths_industry_membership(
        second_client,
        catalog,
        ["600519.SH"],
        cache_dir=tmp_path,
        as_of=NOW,
        sleep=lambda _: None,
    )
    assert second.ok and second.metadata["cache_hit"] is True
    assert second_client.calls == []


def test_partial_crawl_is_not_cached_or_reported_as_success(tmp_path) -> None:
    catalog = _result("/catalog", [
        {"thscode": "881101.TI", "name": "行业A"},
        {"thscode": "881102.TI", "name": "行业B"},
    ])
    client = FakeClient({
        "881101.TI": _result("/members", [{"thscode": "600519.SH", "ticker": "600519"}]),
        "881102.TI": _result("/members", [], ok=False, reason="HTTP_ERROR"),
    })

    result = collect_ths_industry_membership(
        client,
        catalog,
        ["600519.SH"],
        cache_dir=tmp_path,
        as_of=NOW,
        sleep=lambda _: None,
    )

    assert result.ok is False
    assert result.complete is False
    assert result.reason_code == "THS_INDUSTRY_MEMBERSHIP_PARTIAL"
    assert not list(tmp_path.glob("ths-industry-*.json"))


def test_rate_limit_is_retried_without_reusing_partial_data(tmp_path) -> None:
    catalog = _result("/catalog", [{"thscode": "881101.TI", "name": "行业A"}])
    limited = _result("/members", [], ok=False, reason="RATE_LIMITED")
    success = _result("/members", [{"thscode": "600519.SH", "ticker": "600519"}])

    class RetryClient:
        calls = 0

        def ths_index_constituents(self, _thscode: str) -> HithinkFetchResult:
            self.calls += 1
            return limited if self.calls == 1 else success

    client = RetryClient()
    waits: list[float] = []
    result = collect_ths_industry_membership(
        client,
        catalog,
        ["600519.SH"],
        cache_dir=tmp_path,
        as_of=NOW,
        sleep=waits.append,
    )

    assert result.ok is True
    assert client.calls == 2
    assert waits == [2.0]
