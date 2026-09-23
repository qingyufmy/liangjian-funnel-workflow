from datetime import datetime
from zoneinfo import ZoneInfo
import pytest
from liangjian_funnel.data.sina_minute_shadow import normalize_sina_5m, fetch_sina_5m_shadow, compare_five_minute_bars

NOW = datetime(2026, 9, 23, 13, 5, tzinfo=ZoneInfo("Asia/Shanghai"))

def row(day="2026-09-23 13:05:00", **changes):
    return {"day": day, "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "1000", **changes}

def test_shares_amount_provenance_and_closed_only():
    bars = normalize_sina_5m([row(), row("2026-09-23 13:10:00")], symbol="688111.SH", as_of=NOW)
    assert len(bars) == 1 and bars[0].volume == 1000
    assert bars[0].amount_kind == "ohlc_estimate"
    assert bars[0].volume_unit == "shares"

@pytest.mark.parametrize("bad", [[], [row("2026-09-22 13:05:00")], [row("2026-09-23 12:05:00")],
    [row("2026-09-23 13:01:00")], [row(close="nan")], [row(volume="-1")], [row(), row(close="10.6")]])
def test_bad_data_is_not_promoted(bad):
    with pytest.raises(ValueError):
        normalize_sina_5m(bad, symbol="600519.SH", as_of=NOW)

def test_lunch_end_and_duplicate_identity():
    bars = normalize_sina_5m([row("2026-09-23 11:30:00"), row(), row()], symbol="600519.SH", as_of=NOW)
    assert len(bars) == 2

def test_fetch_is_explicit_shadow_and_never_claims_one_minute():
    result = fetch_sina_5m_shadow("600519.SH", fetch=lambda *_: [row()], clock=lambda: NOW)
    assert result["raw_rows"] == [row()]
    assert result["shadow_only"] and not result["execution_authority"] and not result["one_minute_supported"]

def test_difference_does_not_become_confirmation():
    a = normalize_sina_5m([row()], symbol="600519.SH", as_of=NOW)
    b = normalize_sina_5m([row(close="10.6")], symbol="600519.SH", as_of=NOW)
    report = compare_five_minute_bars(a, b)
    assert report["different_bar_count"] == 1 and not report["independent_confirmation"]
    assert not report["execution_authority"]


def test_same_provider_and_wrong_stock_cannot_confirm_independently():
    a = normalize_sina_5m([row()], symbol="600519.SH", as_of=NOW)
    assert not compare_five_minute_bars(a, a)["independent_confirmation"]
    b = [a[0].model_copy(update={"source_id": "TENCENT:ifzq.gtimg.cn"})]
    assert compare_five_minute_bars(a, b)["independent_confirmation"]
    with pytest.raises(ValueError, match="CROSS_SYMBOL"):
        compare_five_minute_bars(a, [b[0].model_copy(update={"symbol": "000001.SZ"})])
