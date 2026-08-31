from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.ths_industry import (
    collect_ths_industry_history,
    collect_ths_industry_membership,
    select_industry_diversified_symbols,
)
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


def test_industry_history_collects_broad_indices_and_reuses_daily_cache(tmp_path) -> None:
    catalog = _result("/catalog", [
        {"thscode": "881101.TI", "name": "宽口径A"},
        {"thscode": "881102.TI", "name": "宽口径B"},
        {"thscode": "884001.TI", "name": "细分A"},
    ])
    bars = [
        {
            "date_ms": index * 86_400_000,
            "open_price": 10 + index,
            "high_price": 11 + index,
            "low_price": 9 + index,
            "close_price": 10.5 + index,
            "volume": 100,
            "turnover": 1000 + index,
        }
        for index in range(1, 6)
    ]

    class HistoryClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def index_history_1d(self, thscode: str, *, start: int, end: int) -> HithinkFetchResult:
            assert end > start
            self.calls.append(thscode)
            return _result("/history", bars).model_copy(
                update={"fetch_time": NOW + timedelta(minutes=10)}
            )

    first_client = HistoryClient()
    first = collect_ths_industry_history(
        first_client,
        catalog,
        cache_dir=tmp_path,
        as_of=NOW,
    )

    assert first.ok and first.complete
    assert first_client.calls == ["881101.TI", "881102.TI"]
    assert len(first.items) == 2
    assert len(first.items[0].model_dump()["bars"]) == 5
    assert first.fetch_time == NOW + timedelta(minutes=10)
    assert first.metadata["timestamp"] == NOW.isoformat()

    second_client = HistoryClient()
    second = collect_ths_industry_history(
        second_client,
        catalog,
        cache_dir=tmp_path,
        as_of=NOW,
    )
    assert second.ok and second.metadata["cache_hit"] is True
    assert second.fetch_time == NOW + timedelta(minutes=10)
    assert second.metadata["timestamp"] == NOW.isoformat()
    assert second_client.calls == []


def test_a1_preselect_is_node_diversified_with_strict_top_n() -> None:
    rows = []
    records = []
    for node_index in range(45):
        node_code = f"{884000 + node_index}.TI"
        for member_index in range(4):
            symbol = f"{600000 + node_index * 4 + member_index:06d}.SH"
            rows.append({
                "thscode": symbol,
                "mapping_status": "MAPPED",
                "memberships": [
                    {
                        "industry_thscode": f"{881100 + node_index:06d}.TI",
                        "industry_name": f"宽口径行业{node_index}",
                    },
                    {"industry_thscode": node_code, "industry_name": f"细分行业{node_index}"},
                ],
            })
            records.append(SimpleNamespace(symbol=symbol, amount=float(10_000 - node_index * 10 - member_index)))
    membership = _result("/memberships", rows)

    selected, metadata = select_industry_diversified_symbols(
        records,
        membership,
        limit=120,
        top_n_per_node=8,
        node_count_target=[40, 80],
    )

    assert len(selected) == 120
    assert metadata["node_count"] == 45
    assert metadata["parent_industry_count"] == 45
    assert metadata["strategy"] == "THS_PARENT_BALANCED_SPECIFIC_NODE_ROUND_ROBIN_TOP_N"
    assert max(item["selected_members"] for item in metadata["nodes"]) == 3
    assert min(item["selected_members"] for item in metadata["nodes"]) == 2


def test_a1_full_coverage_mode_preserves_every_trade_candidate() -> None:
    rows = []
    records = []
    for node_index in range(3):
        node_code = f"884900{node_index}.TI"
        for member_index in range(4):
            symbol = f"{600900 + node_index * 4 + member_index:06d}.SH"
            rows.append({
                "thscode": symbol,
                "mapping_status": "MAPPED",
                "memberships": [
                    {
                        "industry_thscode": f"881900{node_index}.TI",
                        "industry_name": f"宽口径行业{node_index}",
                    },
                    {"industry_thscode": node_code, "industry_name": f"细分行业{node_index}"},
                ],
            })
            records.append(SimpleNamespace(symbol=symbol, amount=float(100 - member_index)))
    # An explicit unmapped row must also remain in the full formal universe.
    rows.append({
        "thscode": "601000.SZ",
        "mapping_status": "UNMAPPED",
        "memberships": [],
    })
    records.append(SimpleNamespace(symbol="601000.SZ", amount=1.0))
    membership = _result("/memberships", rows)

    selected, metadata = select_industry_diversified_symbols(
        records,
        membership,
        limit=len(records),
        top_n_per_node=1,
        node_count_target=[1, 1],
    )

    assert metadata["full_coverage"] is True
    assert metadata["strategy"] == "THS_PARENT_BALANCED_FULL_COVERAGE_ROUND_ROBIN"
    assert metadata["top_n_applied"] is False
    assert set(selected) == {record.symbol for record in records}
    assert len(selected) == len(records)


def test_a1_node_choice_is_parent_balanced_not_turnover_concentrated() -> None:
    rows = []
    records = []
    # Ten very hot child nodes share one parent; twenty quiet nodes each have
    # a distinct parent. A1 must cover the parents before taking a second hot
    # child from the same market theme.
    for node_index in range(30):
        node_code = f"{884400 + node_index}.TI"
        parent_index = 0 if node_index < 10 else node_index - 9
        parent_code = f"{881400 + parent_index}.TI"
        for member_index in range(2):
            symbol = f"{300000 + node_index * 2 + member_index:06d}.SZ"
            rows.append({
                "thscode": symbol,
                "mapping_status": "MAPPED",
                "memberships": [
                    {"industry_thscode": parent_code, "industry_name": f"宽口径{parent_index}"},
                    {"industry_thscode": node_code, "industry_name": f"细分{node_index}"},
                ],
            })
            turnover = 1_000_000.0 if node_index < 10 else 1.0
            records.append(SimpleNamespace(symbol=symbol, amount=turnover - member_index))
    membership = _result("/memberships", rows)

    selected, metadata = select_industry_diversified_symbols(
        records,
        membership,
        limit=20,
        top_n_per_node=8,
        node_count_target=[10, 20],
    )

    assert len(selected) == 20
    assert metadata["node_count"] == 20
    assert metadata["parent_industry_count"] == 20
    assert sum(symbol < "300020.SZ" for symbol in selected) == 1
