from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from liangjian_funnel.data.quality import CrossCheckStatus, compare_prices


ZONE = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 24, 10, 0, tzinfo=ZONE)


def test_matching_prices_pass():
    result = compare_prices(
        hithink_price="100",
        mootdx_price="100.4",
        hithink_time=NOW,
        mootdx_time=NOW - timedelta(seconds=30),
    )
    assert result.status is CrossCheckStatus.PASS
    assert result.difference_pct == Decimal("0.400000")


def test_price_divergence_blocks():
    result = compare_prices(
        hithink_price="100",
        mootdx_price="101",
        hithink_time=NOW,
        mootdx_time=NOW,
    )
    assert result.status is CrossCheckStatus.BLOCKED
    assert result.reason_code == "PRICE_DIVERGENCE"


def test_stale_or_invalid_inputs_block_before_comparison():
    stale = compare_prices(
        hithink_price="100",
        mootdx_price="100",
        hithink_time=NOW,
        mootdx_time=NOW - timedelta(seconds=91),
    )
    invalid = compare_prices(
        hithink_price="NaN",
        mootdx_price="100",
        hithink_time=NOW,
        mootdx_time=NOW,
    )
    assert stale.reason_code == "TIMESTAMP_MISMATCH"
    assert invalid.reason_code == "INVALID_PRICE"
