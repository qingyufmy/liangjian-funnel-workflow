from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
from liangjian_funnel.runtime.monitor import MonitorBatchResult
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus
from liangjian_funnel.workflow import WorkflowApplication


@pytest.mark.parametrize("broken_snapshot,incomplete", [(False, False), (True, False), (False, True)])
def test_monitor_consumes_persisted_revision_or_blocks(monkeypatch, tmp_path, broken_snapshot, incomplete):
    now = datetime(2026, 9, 7, 9, 32, tzinfo=ZoneInfo("Asia/Shanghai"))
    symbol = "000001.SZ"
    original = MinuteBar(symbol=symbol, interval="1m", bar_end=now-timedelta(minutes=1),
        open=10, high=10.2, low=9.9, close=10, volume=100, amount=1000, source_id="TEST")
    revised = original.model_copy(update={"close": 10.2})
    current = revised.model_copy(update={"bar_end": now})
    store = RuntimeStore(tmp_path/"state.sqlite3")
    store.create_execution_plan("p", "lane_1", symbol, status=PlanStatus.ACTIVE_TODAY,
        valid_from=now-timedelta(minutes=2), expires_at=now.replace(hour=15, minute=0), payload={})
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(workflow_output_dir=tmp_path/"outputs", research_primary_lane_id="lane_1")
    app.store = store
    app.minute_store = MinuteBarStore(tmp_path/"minute")
    app.minute_store.write([original])
    app.brokers = {"lane_1": object()}
    app.market_data = object()
    app.lark_publisher = None
    app._ensure_trading_day = lambda _: None
    app._expire_missed_a4_entries = lambda _: None
    app.activate_latest_a3_for_monitor = lambda **_: {}
    settlements = []
    app._settle_prior_signals = lambda *args: settlements.append(args) or []
    app._record_a4_outcomes = lambda *args: {}
    app._a4_callback = lambda *args: None
    app._fetch_live_bars = lambda *args: FetchResult(symbol=symbol, interval="1m",
        requested_bars=2, returned_bars=2, bars=(revised, current),
        reason_code="CLOSE_BAR_FINALIZATION_UNCONFIRMED" if incomplete else "OK", complete=not incomplete)
    monkeypatch.setattr("liangjian_funnel.workflow.load_or_refresh_live_market_state", lambda *args, **kwargs: {})
    calls = []
    class Engine:
        def __init__(self, *args, **kwargs): pass
        def process_minute(self, lane_id, bars, **kwargs):
            calls.append(kwargs)
            return MonitorBatchResult(lane_id=lane_id, minute_snapshot_id=kwargs["minute_snapshot_id"])
    monkeypatch.setattr("liangjian_funnel.workflow.MonitorEngine", Engine)
    if broken_snapshot:
        def fail(*args, **kwargs): raise ValueError("broken snapshot")
        app.minute_store.load_decision_snapshot = fail
    result = app.monitor_once(now=now)
    assert len(calls) == 1
    assert calls[0]["data_ok"] is not broken_snapshot
    if incomplete:
        assert calls[0]["data_errors"][symbol] == "CLOSE_BAR_FINALIZATION_UNCONFIRMED"
    if not broken_snapshot and not incomplete:
        assert calls[0]["bar_histories"][symbol][0].close == 10.2
        # The old archive remains untouched even though the new decision uses revision.
        assert app.minute_store.load_latest(symbol, "1m", limit=2)[0].close == 10
    else:
        assert settlements == []
        if broken_snapshot:
            assert result["minute_cache"]["errors"][0]["reason_code"] == "MINUTE_DECISION_SNAPSHOT_UNAVAILABLE"
