from datetime import datetime, timedelta
import json
import sqlite3
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import MinuteBar, MootdxError, normalize_bars

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 7, 11, 30, tzinfo=TZ)


def bar(close=10, source="TENCENT:ifzq.gtimg.cn"):
    return MinuteBar(symbol="000001.SZ", interval="1m", bar_end=NOW,
        open=10, high=max(10, close), low=min(10, close), close=close,
        volume=10000, amount=100000, source_id=source, volume_unit="shares",
        amount_kind="ohlc_estimate", normalizer_version="tencent-equity-minute-v2")


def test_versions_preserve_first_archive_but_next_decision_uses_revision(tmp_path):
    store = MinuteBarStore(tmp_path)
    old, new = bar(), bar(10.2)
    store.write_live([old], as_of=NOW, snapshot_id="first")
    store.write_live([new], as_of=NOW, snapshot_id="first")
    assert store.load_decision_snapshot("first", old.symbol, "1m", as_of=NOW) == (old,)
    later = NOW + timedelta(minutes=91)
    store.write_live([new], as_of=later, snapshot_id="next")
    assert store.load_decision_snapshot("next", old.symbol, "1m", as_of=later) == (new,)
    assert store.load_latest(old.symbol, "1m", limit=1)[0].close == 10
    assert store.latest_decision_snapshot(old.symbol, "1m", as_of=NOW)["bars"] == (old,)
    assert store.latest_decision_snapshot(old.symbol, "1m", as_of=later)["bars"] == (new,)
    assert store.latest_decision_snapshot(old.symbol, "1m", as_of=NOW+timedelta(days=1)) == {}
    with sqlite3.connect(store.path) as db:
        rows = db.execute("SELECT payload_json, observed_at FROM minute_bar_versions").fetchall()
        assert {json.loads(r[0])["close"] for r in rows} == {10, 10.2}
        # Historical as_of cannot backdate the actual fetch/recording time.
        assert all(datetime.fromisoformat(r[1]) > NOW for r in rows)


def test_snapshot_refuses_missing_wrong_cutoff_and_corrupt_payload(tmp_path):
    store = MinuteBarStore(tmp_path)
    with pytest.raises(ValueError, match="MISSING"):
        store.load_decision_snapshot("x", "000001", "1m", as_of=NOW)
    store.write_live([bar()], as_of=NOW, snapshot_id="x")
    with pytest.raises(ValueError, match="TIME_MISMATCH"):
        store.load_decision_snapshot("x", "000001", "1m", as_of=NOW + timedelta(minutes=1))
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE minute_decision_snapshots SET payload_sha256='invalid'")
    with pytest.raises(ValueError, match="HASH_MISMATCH"):
        store.load_decision_snapshot("x", "000001", "1m", as_of=NOW)


def test_snapshot_filters_future_and_preserves_independent_sources(tmp_path):
    store = MinuteBarStore(tmp_path)
    future = bar().model_copy(update={"bar_end": NOW + timedelta(minutes=1)})
    store.write_live([bar(), future], as_of=NOW, snapshot_id="a")
    other = bar(source="MOOTDX:node:7709")
    store.write_live([other], as_of=NOW, snapshot_id="b")
    assert store.load_decision_snapshot("a", "000001", "1m", as_of=NOW) == (bar(),)
    assert store.load_decision_snapshot("b", "000001", "1m", as_of=NOW) == (other,)
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT COUNT(*) FROM minute_bar_versions").fetchone()[0] == 2


@pytest.mark.parametrize("interval", ["1m", "5m"])
def test_tdx_lunch_alias_and_ambiguous_duplicates(interval):
    row = {"datetime": "2026-09-07 13:00", "open": 10, "high": 10,
           "low": 10, "close": 10, "vol": 10000, "amount": 100000}
    kwargs = dict(symbol="000001", interval=interval, source_id="MOOTDX:node:7709")
    result = normalize_bars([row], **kwargs)[0]
    assert result.bar_end == NOW
    assert result.provider_bar_end == NOW.replace(hour=13, minute=0)
    assert result.volume == 10000 and result.volume_unit == "shares"
    assert result.amount_kind == "reported"
    with pytest.raises(MootdxError, match="DUPLICATE_BAR_TIME"):
        normalize_bars([{**row, "datetime": "2026-09-07 11:30"}, row], **kwargs)


def test_explicit_revision_retains_both_full_payloads(tmp_path):
    store = MinuteBarStore(tmp_path)
    store.write([bar()])
    store.write([bar(10.2)], allow_revisions_for=NOW.date(), observed_at=NOW)
    with sqlite3.connect(store.path) as db:
        prices = {json.loads(r[0])["close"] for r in db.execute("SELECT payload_json FROM minute_bar_versions")}
    assert prices == {10, 10.2}
