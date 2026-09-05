from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.a2_market import (
    MALFORMED,
    OBSERVED_EMPTY,
    SOURCE_UNAVAILABLE,
    STALE,
    build_board_capital_flow_snapshot,
    build_capital_flow_snapshot,
    collect_eastmoney_capital_flow,
    collect_eastmoney_board_flow,
    collect_ths_market_fact,
    inspect_board_capital_flow_snapshot,
    inspect_trade_date_fact,
    load_trade_date_fact,
    write_trade_date_fact,
)


TZ = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 8, 28, 15, 10, tzinfo=TZ)


def test_trade_date_cache_replays_historical_ths_fact_without_callback(tmp_path: Path) -> None:
    calls: list[int] = []

    def fetch() -> dict:
        calls.append(1)
        return {"records": [{"date": "2026-08-28", "boards": {"首板": [{"symbol": "600001.SH"}]}}]}

    first = collect_ths_market_fact(
        cache_dir=tmp_path,
        dataset="LIMIT_UP_LADDER",
        as_of=AS_OF,
        now=AS_OF,
        fetch=fetch,
    )
    replay = collect_ths_market_fact(
        cache_dir=tmp_path,
        dataset="LIMIT_UP_LADDER",
        as_of=AS_OF,
        now=AS_OF + timedelta(days=1),
        fetch=lambda: (_ for _ in ()).throw(AssertionError("historical replay must not fetch")),
    )
    assert calls == [1]
    assert replay == first
    assert load_trade_date_fact(tmp_path, "LIMIT_UP_LADDER", "2026-08-28") == first


def test_historical_missing_stale_and_malformed_cache_states_are_distinct(tmp_path: Path) -> None:
    missing = collect_ths_market_fact(
        cache_dir=tmp_path,
        dataset="THS_CONCEPT_MEMBERSHIP",
        as_of=AS_OF - timedelta(days=1),
        now=AS_OF,
        fetch=lambda: (_ for _ in ()).throw(AssertionError("historical source must not fetch")),
    )
    assert missing["availability_state"] == SOURCE_UNAVAILABLE

    write_trade_date_fact(
        tmp_path,
        "THS_CONCEPT_MEMBERSHIP",
        "2026-08-27",
        {"records": [{"thscode": "600001.SH"}]},
        source_id="HITHINK",
        source_kind="THS",
        as_of=AS_OF - timedelta(days=1),
        ingested_at=AS_OF - timedelta(days=timedelta(days=2).days),
    )
    stale = collect_ths_market_fact(
        cache_dir=tmp_path,
        dataset="THS_CONCEPT_MEMBERSHIP",
        as_of=AS_OF - timedelta(days=1),
        now=AS_OF,
        max_age_seconds=3600,
        fetch=lambda: (_ for _ in ()).throw(AssertionError("stale historical source must not fetch")),
    )
    assert stale["availability_state"] == STALE

    malformed_path = tmp_path / "THS_CONCEPT_MEMBERSHIP-2026-08-26.json"
    malformed_path.write_text("{not-json", encoding="utf-8")
    malformed = collect_ths_market_fact(
        cache_dir=tmp_path,
        dataset="THS_CONCEPT_MEMBERSHIP",
        as_of=AS_OF - timedelta(days=2),
        now=AS_OF,
        fetch=lambda: (_ for _ in ()).throw(AssertionError("malformed historical source must not fetch")),
    )
    assert malformed["availability_state"] == MALFORMED
    inspected = inspect_trade_date_fact(tmp_path, "THS_CONCEPT_MEMBERSHIP", "2026-08-26")
    assert inspected["availability_state"] == MALFORMED


def test_observed_empty_is_not_source_unavailable() -> None:
    empty = build_board_capital_flow_snapshot(
        [],
        as_of=AS_OF,
        board_type="concept",
        period="today",
    )
    assert empty["availability_state"] == OBSERVED_EMPTY
    assert empty["available"] is True


def test_capital_flow_distinguishes_empty_unavailable_and_malformed_windows() -> None:
    empty = build_capital_flow_snapshot(
        {"today": [], "3d": [], "5d": [], "10d": []},
        as_of=AS_OF,
        expected_symbols=["600001.SH"],
    )
    assert empty["availability_state"] == OBSERVED_EMPTY
    assert empty["coverage_by_window"]["today"]["availability_state"] == OBSERVED_EMPTY

    unavailable = build_capital_flow_snapshot(
        {},
        as_of=AS_OF,
        expected_symbols=["600001.SH"],
    )
    assert unavailable["availability_state"] == SOURCE_UNAVAILABLE

    malformed = build_capital_flow_snapshot(
        {"today": "not-a-record-list"},
        as_of=AS_OF,
        expected_symbols=["600001.SH"],
    )
    assert malformed["availability_state"] == MALFORMED
    assert malformed["coverage_by_window"]["today"]["availability_state"] == MALFORMED


def test_historical_stale_capital_flow_cache_is_not_refetched(tmp_path: Path) -> None:
    row = {
        "symbol": "600001.SH",
        "net_inflow_ratio": 12,
        "net_inflow_amount": 100,
    }

    def fetch(_indicator: str):
        return [row]

    collect_eastmoney_capital_flow(
        as_of=AS_OF,
        now=AS_OF,
        expected_symbols=["600001.SH"],
        cache_dir=tmp_path,
        fetch_rank=fetch,
    )
    stale = collect_eastmoney_capital_flow(
        as_of=AS_OF,
        now=AS_OF + timedelta(days=1),
        expected_symbols=["600001.SH"],
        cache_dir=tmp_path,
        cache_max_age_seconds=60,
        fetch_rank=lambda _indicator: (_ for _ in ()).throw(AssertionError("historical cache must not refetch")),
    )
    assert stale["availability_state"] == STALE


def test_current_board_flow_is_cached_and_historical_read_is_exact_date(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    def fetch(board_type: str, period: str) -> dict:
        calls.append((board_type, period))
        return {"rows": [{"code": "880001.TI", "name": "粮食", "main_net": 10}]}

    current = collect_eastmoney_board_flow(
        as_of=AS_OF,
        now=AS_OF,
        board_type="industry",
        period="today",
        cache_dir=tmp_path,
        fetch_board=fetch,
    )
    replay = collect_eastmoney_board_flow(
        as_of=AS_OF,
        now=AS_OF + timedelta(days=1),
        board_type="industry",
        period="today",
        cache_dir=tmp_path,
        fetch_board=lambda *_: (_ for _ in ()).throw(AssertionError("cache should be used")),
    )
    assert calls == [("industry", "today")]
    assert replay == current


def test_historical_board_flow_recovers_only_from_exact_provider_date(tmp_path: Path) -> None:
    provider_timestamp = int(AS_OF.timestamp())

    recovered = collect_eastmoney_board_flow(
        as_of=AS_OF,
        now=AS_OF + timedelta(days=1),
        board_type="industry",
        period="today",
        cache_dir=tmp_path,
        fetch_board=lambda *_: {
            "rows": [{
                "code": "BK0001",
                "name": "基础化工",
                "main_net": 10,
                "provider_timestamp": provider_timestamp,
            }]
        },
        allow_historical_recovery=True,
    )

    assert recovered["available"] is True
    assert recovered["historical_recovery"] is True
    assert recovered["provider_trade_date_verified"] is True


def test_historical_board_flow_rejects_provider_date_mismatch(tmp_path: Path) -> None:
    recovered = collect_eastmoney_board_flow(
        as_of=AS_OF - timedelta(days=1),
        now=AS_OF,
        board_type="industry",
        period="today",
        cache_dir=tmp_path,
        fetch_board=lambda *_: {
            "rows": [{
                "code": "BK0001",
                "name": "基础化工",
                "main_net": 10,
                "provider_timestamp": int(AS_OF.timestamp()),
            }]
        },
        allow_historical_recovery=True,
    )

    assert recovered["available"] is False
    assert recovered["reason_code"] == "HISTORICAL_BOARD_FLOW_PROVIDER_DATE_MISMATCH"


def test_board_flow_cache_rejects_mislabeled_provider_date(tmp_path: Path) -> None:
    previous_day = AS_OF - timedelta(days=1)
    collected = collect_eastmoney_board_flow(
        as_of=AS_OF,
        now=AS_OF,
        board_type="concept",
        period="today",
        cache_dir=tmp_path,
        fetch_board=lambda *_: {
            "rows": [{
                "code": "BK0001",
                "name": "算力",
                "main_net": 10,
                "provider_timestamp": int(previous_day.timestamp()),
            }]
        },
    )

    assert collected["provider_trade_date_verified"] is False
    inspected = inspect_board_capital_flow_snapshot(
        tmp_path,
        "concept",
        "today",
        AS_OF.date().isoformat(),
    )
    assert inspected["available"] is False
    assert inspected["reason_code"] == "BOARD_FLOW_CACHE_PROVIDER_DATE_MISMATCH"
