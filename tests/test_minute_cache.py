from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.cache import CacheConflictError, MinuteBarStore
from liangjian_funnel.data.mootdx import MinuteBar


ZONE = ZoneInfo("Asia/Shanghai")


def _bar(minute: int, *, close: float = 10.0) -> MinuteBar:
    return MinuteBar(
        symbol="600519.SH",
        interval="1m",
        bar_end=datetime(2026, 8, 24, 9, minute, tzinfo=ZONE),
        open=10,
        high=max(10, close),
        low=min(10, close),
        close=close,
        volume=100,
        amount=1000,
        source_id="MOOTDX:node-a:7709",
        adjust_mode="none",
    )


def test_cache_is_idempotent_and_reads_in_ascending_order(tmp_path: Path):
    store = MinuteBarStore(tmp_path)
    bars = (_bar(31), _bar(32))
    assert store.write(bars).inserted == 2
    replay = store.write(bars)
    assert replay.inserted == 0
    assert replay.unchanged == 2
    loaded = store.load_latest("600519.SH", "1m", limit=2)
    assert [bar.bar_end.minute for bar in loaded] == [31, 32]


def test_same_market_bar_from_a_different_source_is_safe_idempotent(tmp_path: Path):
    store = MinuteBarStore(tmp_path)
    store.write((_bar(31),))
    replay = _bar(31).model_copy(update={"source_id": "MOOTDX:node-b:7710"})
    result = store.write((replay,))
    assert result.inserted == 0
    assert result.unchanged == 1


def test_conflicting_observation_rolls_back_batch(tmp_path: Path):
    store = MinuteBarStore(tmp_path)
    store.write((_bar(31),))
    with pytest.raises(CacheConflictError, match="MINUTE_CACHE_CONFLICT"):
        store.write((_bar(32), _bar(31, close=10.1)))
    assert [bar.bar_end.minute for bar in store.load_latest("600519", "1m", limit=10)] == [31]


def test_conflict_exposes_safe_key_and_differing_fields_only(tmp_path: Path):
    store = MinuteBarStore(tmp_path)
    store.write((_bar(31),))
    with pytest.raises(CacheConflictError) as caught:
        store.write((_bar(31, close=10.1),))
    error = caught.value
    assert error.reason_code == "MINUTE_CACHE_CONFLICT"
    assert error.symbol == "600519.SH"
    assert error.interval == "1m"
    assert error.bar_end == "2026-08-24T09:31:00+08:00"
    assert error.differing_fields == ("high", "close")
    assert error.diagnostics == {
        "reason_code": "MINUTE_CACHE_CONFLICT",
        "symbol": "600519.SH",
        "interval": "1m",
        "bar_end": "2026-08-24T09:31:00+08:00",
        "differing_fields": ("high", "close"),
    }
    assert "10.1" not in str(error)
    assert "1000" not in str(error)


def test_invalid_load_contract_is_rejected(tmp_path: Path):
    store = MinuteBarStore(tmp_path)
    with pytest.raises(ValueError, match="interval"):
        store.load_latest("600519", "15m", limit=1)
    with pytest.raises(ValueError, match="positive"):
        store.load_latest("600519", "1m", limit=0)
