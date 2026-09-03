from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.runtime.live_market import (
    classify_full_market,
    classify_index_fallback,
)


NOW = datetime(2026, 9, 3, 10, 5, tzinfo=ZoneInfo("Asia/Shanghai"))


def _rows(up: int, down: int, *, up_change: float = 1.0, down_change: float = -1.0):
    return ([{"price_change_ratio_pct": up_change}] * up) + (
        [{"price_change_ratio_pct": down_change}] * down
    )


def test_full_market_allows_normal_breadth() -> None:
    state = classify_full_market(_rows(700, 300), as_of=NOW, expected_count=1000)
    assert state["status"] == "READY"
    assert state["entry_permission"] == "ALLOW"
    assert state["trade_date"] == "2026-09-03"
    assert state["breadth"] == 0.7


def test_full_market_weak_rotation_is_caution_not_block() -> None:
    state = classify_full_market(_rows(400, 600), as_of=NOW, expected_count=1000)
    assert state["status"] == "READY"
    assert state["entry_permission"] == "CAUTION"
    assert state["suggested_position_cap_pct"] == 0.5


def test_full_market_only_blocks_confirmed_systemic_selloff() -> None:
    state = classify_full_market(
        _rows(100, 900, up_change=0.2, down_change=-2.5),
        as_of=NOW,
        expected_count=1000,
    )
    assert state["status"] == "READY"
    assert state["entry_permission"] == "BLOCK_NEW_ENTRY"
    assert state["reason_code"] == "A4_LIVE_SYSTEMIC_SELL_OFF"


def test_full_market_rejects_partial_coverage() -> None:
    state = classify_full_market(_rows(400, 100), as_of=NOW, expected_count=1000)
    assert state["status"] == "DATA_BLOCKED"
    assert state["entry_permission"] == "UNKNOWN"


def test_index_fallback_retains_degraded_quality_and_permission() -> None:
    quotes = [
        {"price": 101, "previous_close": 100},
        {"price": 100.5, "previous_close": 100},
        {"price": 99.8, "previous_close": 100},
    ]
    state = classify_index_fallback(quotes, as_of=NOW)
    assert state["status"] == "READY_DEGRADED"
    assert state["entry_permission"] == "ALLOW"
    assert state["breadth"] is None


def test_index_fallback_cannot_claim_readiness_without_three_indices() -> None:
    state = classify_index_fallback(
        [{"price": 101, "previous_close": 100}],
        as_of=NOW,
    )
    assert state["status"] == "DATA_BLOCKED"
    assert state["reason_code"] == "A4_LIVE_MARKET_SOURCE_UNAVAILABLE"
