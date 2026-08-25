from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from liangjian_funnel.data.mootdx import (
    BarGap,
    FREQUENCY_BY_INTERVAL,
    MinuteBar,
    MootdxAdapter,
    MootdxNode,
    detect_missing_bars,
    map_symbol,
    normalize_bars,
)


TZ = ZoneInfo("Asia/Shanghai")


def row(at: datetime, *, close: float = 10.2, volume: float = 100, amount: float = 1000) -> dict:
    return {
        "datetime": at,
        "open": 10.0,
        "high": max(10.5, close),
        "low": min(9.9, close),
        "close": close,
        "volume": volume,
        "amount": amount,
    }


def make_bars(start: datetime, count: int, *, step: int = 1) -> list[dict]:
    return [row(start.replace(minute=start.minute + index * step)) for index in range(count)]


class FakeClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls: list[tuple[str, int, int, int]] = []
        self.closed = False

    def bars(self, symbol, frequency, start, offset):
        self.calls.append((symbol, frequency, start, offset))
        page = start // offset
        if callable(self.pages):
            return self.pages(page)
        return self.pages[page] if page < len(self.pages) else []

    def close(self):
        self.closed = True


def test_frequency_mapping_and_symbol_exchange_fail_closed():
    assert FREQUENCY_BY_INTERVAL == {"1m": 8, "5m": 0}
    assert map_symbol("600519").canonical == "600519.SH"
    assert map_symbol("SZ.000001").canonical == "000001.SZ"
    assert map_symbol("300750.XSHE").canonical == "300750.SZ"
    with pytest.raises(Exception) as exc:
        map_symbol("830799")
    assert getattr(exc.value, "reason_code", None) == "UNSUPPORTED_EXCHANGE"
    with pytest.raises(Exception) as exc:
        map_symbol("600519.SZ")
    assert getattr(exc.value, "reason_code", None) == "SYMBOL_EXCHANGE_MISMATCH"


def test_first_node_failure_rotates_to_next_node_without_network():
    good = FakeClient([[row(datetime(2026, 8, 24, 9, 30, tzinfo=TZ))]])
    calls = []

    def factory(node):
        calls.append(node.server)
        if len(calls) == 1:
            raise TimeoutError("must not be exposed")
        return good

    adapter = MootdxAdapter(
        nodes=[("node-a", 7709), ("node-b", 7709)],
        client_factory=factory,
    )
    result = adapter.fetch_bars("600519", "1m", 1)
    assert result.complete is True
    assert result.reason_code == "OK"
    assert result.server == "node-b:7709"
    assert [attempt.reason_code for attempt in result.attempts] == ["NODE_REQUEST_FAILED", "OK"]
    assert calls == ["node-a:7709", "node-b:7709"]
    assert good.closed is True


def test_empty_table_fails_closed_and_attempts_next_node():
    empty = FakeClient([[]])
    good = FakeClient([[row(datetime(2026, 8, 24, 9, 30, tzinfo=TZ))]])
    clients = iter([empty, good])
    result = MootdxAdapter(
        nodes=[("node-a", 7709), ("node-b", 7709)],
        client_factory=lambda _node: next(clients),
    ).fetch_bars("600519", "1m", 1)
    assert result.complete is True
    assert [attempt.reason_code for attempt in result.attempts] == ["EMPTY_DATA", "OK"]
    assert empty.closed and good.closed


@pytest.mark.parametrize(
    "bad_row",
    [
        {**row(datetime(2026, 8, 24, 9, 30, tzinfo=TZ)), "close": float("nan")},
        {**row(datetime(2026, 8, 24, 9, 30, tzinfo=TZ)), "low": 11.0},
        {**row(datetime(2026, 8, 24, 9, 30, tzinfo=TZ)), "volume": -1},
    ],
)
def test_illegal_fields_are_rejected(bad_row):
    with pytest.raises(Exception):
        normalize_bars([bad_row], symbol="600519", interval="1m", source_id="MOOTDX:node-a:7709")


def test_minute_bar_is_frozen_and_requires_aware_shanghai_time():
    bar = MinuteBar(
        symbol="600519",
        interval="5m",
        bar_end=datetime(2026, 8, 24, 9, 35, tzinfo=TZ),
        open=10,
        high=11,
        low=9,
        close=10.5,
        volume=1,
        amount=10,
        source_id="MOOTDX:node-a:7709",
        adjust_mode="raw",
    )
    assert bar.symbol == "600519.SH"
    assert bar.bar_end.tzinfo is not None
    with pytest.raises(ValidationError):
        MinuteBar(
            symbol="600519",
            interval="1m",
            bar_end=datetime(2026, 8, 24, 9, 30),
            open=10,
            high=11,
            low=9,
            close=10.5,
            volume=1,
            amount=10,
            source_id="MOOTDX:node-a:7709",
        )


def test_pagination_deduplicates_and_returns_latest_requested_bars():
    start = datetime(2026, 8, 24, 9, 30, tzinfo=TZ)
    page1 = make_bars(start, 3)
    page2 = [page1[-1], *make_bars(start.replace(minute=33), 3)]
    page2 = list(reversed(page2))  # a monotonically descending page is valid
    client = FakeClient([page1, page2])
    result = MootdxAdapter(nodes=[("node-a", 7709)], client_factory=lambda _node: client).fetch_bars(
        "600519", "1m", 5
    )
    assert result.complete is True
    assert result.returned_bars == 5
    assert [bar.bar_end.minute for bar in result.bars] == [31, 32, 33, 34, 35]
    assert [call[2:] for call in client.calls] == [(0, 800), (800, 800)]


def test_pagination_reports_insufficient_history_instead_of_succeeding():
    start = datetime(2026, 8, 24, 9, 30, tzinfo=TZ)
    client = FakeClient([make_bars(start, 2)])
    result = MootdxAdapter(nodes=[("node-a", 7709)], client_factory=lambda _node: client).fetch_bars(
        "600519", "5m", 5
    )
    assert result.complete is False
    assert result.reason_code == "INSUFFICIENT_BARS"
    assert result.returned_bars == 2
    assert result.attempts[0].reason_code == "INSUFFICIENT_BARS"


def test_fetch_bars_excludes_forming_5m_bar_and_keeps_requested_count():
    page = list(
        reversed(
            [
                row(datetime(2026, 8, 24, 9, 35, tzinfo=TZ)),
                row(datetime(2026, 8, 24, 9, 40, tzinfo=TZ)),
                row(datetime(2026, 8, 24, 9, 45, tzinfo=TZ)),
                row(datetime(2026, 8, 24, 9, 50, tzinfo=TZ)),
            ]
        )
    )
    adapter = MootdxAdapter(
        nodes=[("node-a", 7709)],
        client_factory=lambda _node: FakeClient([page]),
        page_size=4,
        max_pages=1,
    )

    before_close = adapter.fetch_bars(
        "600519",
        "5m",
        2,
        as_of=datetime(2026, 8, 24, 9, 49, tzinfo=TZ),
    )
    assert before_close.complete is True
    assert before_close.returned_bars == 2
    assert [bar.bar_end.minute for bar in before_close.bars] == [40, 45]

    after_close = adapter.fetch_bars(
        "600519",
        "5m",
        2,
        as_of=datetime(2026, 8, 24, 9, 50, tzinfo=TZ),
    )
    assert after_close.complete is True
    assert [bar.bar_end.minute for bar in after_close.bars] == [45, 50]


def test_fetch_bars_rejects_naive_as_of_as_structured_failure():
    adapter = MootdxAdapter(
        nodes=[("node-a", 7709)],
        client_factory=lambda _node: pytest.fail("invalid as_of must not contact a node"),
    )
    result = adapter.fetch_bars("600519", "5m", 1, as_of=datetime(2026, 8, 24, 9, 49))
    assert result.complete is False
    assert result.reason_code == "INVALID_AS_OF"
    assert result.attempts == ()


def test_fetch_bars_reports_insufficient_when_capacity_is_consumed_by_forming_bar():
    page = [
        row(datetime(2026, 8, 24, 9, 50, tzinfo=TZ)),
        row(datetime(2026, 8, 24, 9, 45, tzinfo=TZ)),
    ]
    adapter = MootdxAdapter(
        nodes=[("node-a", 7709)],
        client_factory=lambda _node: FakeClient([page]),
        page_size=2,
        max_pages=1,
    )
    result = adapter.fetch_bars(
        "600519",
        "5m",
        2,
        as_of=datetime(2026, 8, 24, 9, 49, tzinfo=TZ),
    )
    assert result.complete is False
    assert result.reason_code == "INSUFFICIENT_BARS"
    assert result.returned_bars == 1
    assert result.bars[0].bar_end == datetime(2026, 8, 24, 9, 45, tzinfo=TZ)


def test_arbitrary_page_order_and_duplicate_timestamps_fail_closed():
    start = datetime(2026, 8, 24, 9, 30, tzinfo=TZ)
    unordered = [row(start.replace(minute=32)), row(start), row(start.replace(minute=31))]
    duplicate = [row(start), row(start)]
    for bad_page in (unordered, duplicate):
        result = MootdxAdapter(nodes=[("node-a", 7709)], client_factory=lambda _node, p=bad_page: FakeClient([p])).fetch_bars(
            "600519", "1m", 2
        )
        assert result.complete is False
        assert result.reason_code in {"UNORDERED_BAR_DATA", "DUPLICATE_BAR_TIME"}


def _bar(minute: int, *, day: int = 24) -> MinuteBar:
    return MinuteBar(
        symbol="600519",
        interval="1m",
        bar_end=datetime(2026, 8, day, minute // 60, minute % 60, tzinfo=TZ),
        open=10,
        high=11,
        low=9,
        close=10.5,
        volume=1,
        amount=10,
        source_id="MOOTDX:node-a:7709",
    )


def test_gap_detection_does_not_count_midday_break():
    bars = [_bar(11 * 60 + 30), _bar(13 * 60 + 1), _bar(13 * 60 + 2)]
    assert detect_missing_bars(bars, "1m") == ()


def test_gap_detection_reports_missing_closed_bar_and_honors_as_of():
    bars = [_bar(9 * 60 + 31), _bar(9 * 60 + 33)]
    gaps = detect_missing_bars(bars, "1m")
    assert len(gaps) == 1
    assert isinstance(gaps[0], BarGap)
    assert gaps[0].expected_end == datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    # A timestamp after the last observed bar is not checked without a
    # second observed boundary; an in-progress as_of cannot add a false gap.
    assert detect_missing_bars(bars, "1m", as_of=datetime(2026, 8, 24, 9, 33, 30, tzinfo=TZ)) == gaps


def test_gap_detection_5m_uses_session_boundaries():
    bars = [
        MinuteBar(
            symbol="600519",
            interval="5m",
            bar_end=datetime(2026, 8, 24, 9, 35, tzinfo=TZ),
            open=10,
            high=11,
            low=9,
            close=10.5,
            volume=1,
            amount=10,
            source_id="MOOTDX:node-a:7709",
        ),
        MinuteBar(
            symbol="600519",
            interval="5m",
            bar_end=datetime(2026, 8, 24, 9, 45, tzinfo=TZ),
            open=10,
            high=11,
            low=9,
            close=10.5,
            volume=1,
            amount=10,
            source_id="MOOTDX:node-a:7709",
        ),
    ]
    gaps = detect_missing_bars(bars, "5m")
    assert [gap.expected_end.minute for gap in gaps] == [40]

    # A 5m bar ends at 11:30 before lunch and at 13:05 after lunch.  The
    # exchange break itself is not a missing 5m bar.
    lunch_bars = [bar.model_copy(update={"bar_end": datetime(2026, 8, 24, 11, 30, tzinfo=TZ)}) for bar in bars[:1]]
    lunch_bars.append(bars[0].model_copy(update={"bar_end": datetime(2026, 8, 24, 13, 5, tzinfo=TZ)}))
    assert detect_missing_bars(lunch_bars, "5m") == ()
