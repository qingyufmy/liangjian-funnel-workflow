from __future__ import annotations

import time
from datetime import datetime, timedelta
from threading import Lock, RLock
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationAction, SimulationConfig
from liangjian_funnel.runtime.state import MonitorAction, PlanStatus, RuntimeStore
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot as ResearchSnapshot,
    LaneResult,
    ResearchRunResult,
)
from liangjian_funnel.pipeline.research import _approved_symbols, _runtime_input
from liangjian_funnel.workflow import (
    WorkflowApplication,
    _canonical_symbol,
    _compact_factor,
    _intraday_market_context,
    _latest_required_5m_end,
    _minute_cache_ready,
    _plan_expiry,
    _progress_stdout,
    _tightens,
)


TZ = ZoneInfo("Asia/Shanghai")


def _bar(end: datetime) -> MinuteBar:
    return MinuteBar(
        symbol="600519.SH",
        interval="1m",
        bar_end=end,
        open=10,
        high=10.2,
        low=9.9,
        close=10.1,
        volume=1000,
        amount=10_000,
        source_id="mootdx:test",
        adjust_mode="none",
    )


def test_minute_history_cache_is_reused_only_at_the_expected_closed_bar():
    as_of = datetime(2026, 8, 25, 22, 0, tzinfo=TZ)
    bars = (
        _bar(datetime(2026, 8, 25, 14, 55, tzinfo=TZ)).model_copy(update={"interval": "5m"}),
        _bar(datetime(2026, 8, 25, 15, 0, tzinfo=TZ)).model_copy(update={"interval": "5m"}),
    )
    assert _latest_required_5m_end(as_of) == datetime(2026, 8, 25, 15, 0, tzinfo=TZ)
    assert _minute_cache_ready(bars, required_bars=2, as_of=as_of)
    stale = bars[:-1]
    assert not _minute_cache_ready(stale, required_bars=1, as_of=as_of)


def test_monitor_confirmation_survives_new_process_instance(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    start = datetime(2026, 8, 24, 10, 0, tzinfo=TZ)
    store.create_execution_plan(
        "plan-1",
        "lane_1",
        "600519.SH",
        status=PlanStatus.DRAFT_CLOSE,
        expires_at=start + timedelta(hours=1),
        payload={"trigger_low": 10, "trigger_high": 11, "stop_level": 9, "confirmation_bars": 2},
    )
    store.set_plan_pending_morning_review("plan-1")
    store.activate_plan("plan-1", valid_from=start)
    first_calls = []
    first = MonitorEngine(store, llm_veto=lambda value: first_calls.append(value) or {"vetoes": {"plan-1": False}})
    one = first.process_minute("lane_1", [_bar(start)], minute_snapshot_id="m1", now=start)
    assert any(event.action == "START_CONFIRMATION" for event in one.events)
    assert len(first_calls) == 1

    second_calls = []
    second = MonitorEngine(store, llm_veto=lambda value: second_calls.append(value) or {"vetoes": {"plan-1": False}})
    two = second.process_minute(
        "lane_1",
        [_bar(start + timedelta(minutes=1))],
        minute_snapshot_id="m2",
        now=start + timedelta(minutes=1),
    )
    assert any(event.action == "BUY_SIGNAL" for event in two.events)
    assert len(second_calls) == 1


def test_workflow_plan_helpers_are_fail_closed():
    assert _canonical_symbol("SHSE.600519") == "600519.SH"
    parent = {"payload_json": '{"trigger_low":10,"trigger_high":11,"risk_unit":"STANDARD"}'}
    assert _tightens(parent, {"trigger_low": 10.1, "trigger_high": 10.9, "risk_unit": "PROBE"})
    assert not _tightens(parent, {"trigger_low": 9.9, "trigger_high": 11.1, "risk_unit": "STANDARD"})
    expiry = _plan_expiry("not-a-time", datetime(2026, 8, 28, 15, 10, tzinfo=TZ), "close")
    assert expiry.weekday() == 0
    # A model-proposed Monday morning expiry is floored by the server to the
    # next trading day close, so an Aug-28 close plan remains usable for Aug-31.
    assert _plan_expiry(
        "2026-08-31T09:15:00+08:00",
        datetime(2026, 8, 28, 15, 10, tzinfo=TZ),
        "close",
    ) == datetime(2026, 8, 31, 15, 0, tzinfo=TZ)
    compact = _compact_factor({"symbol": "600519.SH", "timeframes": {"5m": {"bars": [1, 2], "latest": {"close": 10}, "moving_averages": {"ma5": 9}, "ready": True}}})
    assert "bars" not in compact["timeframes"]["5m"]


def test_plan_publication_blocks_symbols_without_trade_permission(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    current = datetime(2026, 8, 26, 15, 10, tzinfo=TZ)
    output = {
        "core_watch_pool": [
            {
                "plan_id": "blocked-plan",
                "symbol": "600519.SH",
                "risk_unit": "STANDARD",
                "trigger_zone": {"low": 10, "high": 11},
                "invalidation_level": 9,
            },
            {
                "plan_id": "allowed-plan",
                "symbol": "000001.SZ",
                "risk_unit": "STANDARD",
                "trigger_zone": {"low": 10, "high": 11},
                "invalidation_level": 9,
            },
        ]
    }
    result = ResearchRunResult(
        run_id="run-full-market",
        generated_at=current,
        snapshot_id="snapshot-full-market",
        snapshot_hash="a" * 64,
        status="READY",
        lanes=(LaneResult("lane_1", "model", "READY", (), output),),
        audit_paths=(),
        markdown_path=None,
    )
    app = SimpleNamespace(store=store)

    publication = WorkflowApplication._publish_plans(
        app,
        result,
        "close",
        current,
        snapshot_data={
            "TRADABILITY_FLAGS": {
                "600519.SH": {"tradable": False, "exclusion_reasons": ["ST_RISK"]},
                "000001.SZ": {"tradable": True, "exclusion_reasons": []},
            }
        },
    )

    assert len(publication["created"]) == 1
    assert store.list_execution_plans(lane_id="lane_1")[0]["symbol"] == "000001.SZ"
    assert {
        (item.get("symbol"), item["reason"])
        for item in publication["blocked"]
    } == {("600519.SH", "PLAN_SYMBOL_NOT_TRADABLE")}


def test_plan_publication_allows_only_complete_secondary_probe(tmp_path):
    store = RuntimeStore(tmp_path / "runtime-secondary.sqlite3")
    current = datetime(2026, 8, 28, 15, 10, tzinfo=TZ)
    secondary_probe = {
        "plan_id": "watch-probe",
        "symbol": "002957.SZ",
        "candidate_origin": "WATCH_ONLY",
        "risk_unit": "PROBE",
        "setup_type": "BREAKOUT_RETEST",
        "trigger_zone": {"low": 10.0, "high": 10.2},
        "invalidation_level": 9.8,
        "stop_distance_pct": 0.02,
        "first_resistance": 11.0,
        "reward_risk": 2.5,
        "technical_score": 80,
        "score_breakdown": {},
        "confirmation_conditions": ["5m_close_above_trigger"],
        "scenarios": {"normal_open_plan": {}, "weak_open_plan": {}, "high_gap_no_chase_plan": {}, "invalidation_plan": {}},
        "plan_expiry": "2026-08-31T15:00:00+08:00",
    }
    secondary_no_entry = {
        "symbol": "000001.SZ",
        "candidate_origin": "WATCH_ONLY",
        "risk_unit": "NO_ENTRY",
        "reason_codes": ["A3_TECHNICAL_SCORE_BELOW_MINIMUM"],
    }
    result = ResearchRunResult(
        run_id="run-secondary-probe",
        generated_at=current,
        snapshot_id="snapshot-secondary",
        snapshot_hash="b" * 64,
        status="READY",
        lanes=(LaneResult(
            "lane_1",
            "model",
            "READY",
            (),
            {"core_watch_pool": [], "secondary_watch_pool": [secondary_probe, secondary_no_entry]},
        ),),
        audit_paths=(),
        markdown_path=None,
    )
    app = SimpleNamespace(store=store)
    publication = WorkflowApplication._publish_plans(
        app,
        result,
        "close",
        current,
        snapshot_data={
            "TRADABILITY_FLAGS": {
                "002957.SZ": {"tradable": True},
                "000001.SZ": {"tradable": True},
            },
            "PRICE_LEVELS": {
                "002957.SZ": {
                    "available": True,
                    "trigger_zone": {"low": 10.0, "high": 10.2},
                    "invalidation": 9.8,
                    "stop_distance_pct": 0.02,
                    "first_resistance": 11.0,
                    "reward_risk": 2.5,
                }
            },
            "MIN_REWARD_RISK": 2.0,
            "MAX_STOP_DISTANCE": 0.06,
            "MIN_TECHNICAL_SCORE": 70,
        },
    )
    assert len(publication["created"]) == 1
    assert store.list_execution_plans(lane_id="lane_1")[0]["symbol"] == "002957.SZ"
    assert ("000001.SZ", "SECONDARY_NOT_PROBE") in {
        (item.get("symbol"), item["reason"]) for item in publication["blocked"]
    }


def test_secondary_probe_uses_stage_canonical_hash_when_base_snapshot_has_no_price_levels(tmp_path):
    store = RuntimeStore(tmp_path / "runtime-secondary-stage-overlay.sqlite3")
    current = datetime(2026, 8, 30, 12, 0, tzinfo=TZ)
    price_contract = {
        "trigger_zone": {"low": 10.0, "high": 10.2},
        "invalidation_level": 9.8,
        "stop_distance_pct": 0.02,
        "first_resistance": 11.0,
        "reward_risk": 2.5,
    }
    from liangjian_funnel.workflow import _hash_json

    secondary_probe = {
        "plan_id": "stage-overlay-probe",
        "symbol": "002957.SZ",
        "candidate_origin": "WATCH_ONLY",
        "risk_unit": "PROBE",
        "setup_type": "BREAKOUT_RETEST",
        **price_contract,
        "technical_score": 80,
        "score_breakdown": {},
        "confirmation_conditions": ["5m_close_above_trigger"],
        "scenarios": {
            "normal_open_plan": {},
            "weak_open_plan": {},
            "high_gap_no_chase_plan": {},
            "invalidation_plan": {},
        },
        "plan_expiry": "2026-08-31T15:00:00+08:00",
        "server_price_levels_hash": _hash_json(price_contract),
    }
    result = ResearchRunResult(
        run_id="run-stage-overlay-probe",
        generated_at=current,
        snapshot_id="snapshot-stage-overlay",
        snapshot_hash="d" * 64,
        status="READY",
        lanes=(LaneResult(
            "lane_1", "model", "READY", (),
            {"core_watch_pool": [], "secondary_watch_pool": [secondary_probe]},
        ),),
        audit_paths=(),
        markdown_path=None,
    )
    publication = WorkflowApplication._publish_plans(
        SimpleNamespace(store=store),
        result,
        "close",
        current,
        snapshot_data={
            "TRADABILITY_FLAGS": {"002957.SZ": {"tradable": True}},
            "PRICE_LEVELS": {},
            "MIN_REWARD_RISK": 2.0,
            "MAX_STOP_DISTANCE": 0.06,
            "MIN_TECHNICAL_SCORE": 70,
        },
    )
    assert len(publication["created"]) == 1
    assert store.list_execution_plans(lane_id="lane_1")[0]["symbol"] == "002957.SZ"


def test_a3_minute_cache_writes_are_serialized_but_fetches_overlap(monkeypatch):
    import liangjian_funnel.workflow as workflow_module

    as_of = datetime(2026, 8, 28, 15, 10, tzinfo=TZ)
    write_state = {"active": 0, "overlaps": 0}
    state_lock = Lock()
    fetch_state = {"active": 0, "max": 0}
    fetch_lock = Lock()

    class MinuteStore:
        def load_latest(self, *_args, **_kwargs):
            return []

        def write(self, _bars):
            with state_lock:
                if write_state["active"]:
                    write_state["overlaps"] += 1
                write_state["active"] += 1
            time.sleep(0.02)
            with state_lock:
                write_state["active"] -= 1

    class FactCache:
        def query_daily_bars(self, *_args, **_kwargs):
            return []

    class Mootdx:
        def fetch_bars(self, symbol, *_args, **_kwargs):
            with fetch_lock:
                fetch_state["active"] += 1
                fetch_state["max"] = max(fetch_state["max"], fetch_state["active"])
            time.sleep(0.02)
            with fetch_lock:
                fetch_state["active"] -= 1
            return SimpleNamespace(complete=True, bars=[])

    class Factor:
        def __init__(self, symbol):
            self.symbol = symbol

        def compute(self, **_kwargs):
            return SimpleNamespace(
                model_dump=lambda mode="json": {
                    "symbol": self.symbol,
                    "ready": True,
                    "timeframes": {},
                }
            )

    monkeypatch.setattr(workflow_module, "FactorEngine", Factor)
    monkeypatch.setattr(
        workflow_module,
        "build_technical_aggregates",
        lambda _factor: {"KLINE_PATTERNS": {}, "PRICE_LEVELS": {"available": True}},
    )
    app = SimpleNamespace(
        settings=SimpleNamespace(mootdx_history_5m_required_bars=1),
        fact_cache=FactCache(),
        minute_store=MinuteStore(),
        mootdx=Mootdx(),
        _stage_technical_cache={},
        _stage_technical_lock=RLock(),
    )
    snapshot = ResearchSnapshot(
        snapshot_id="snapshot-minute-lock",
        snapshot_hash="c" * 64,
        data={},
        as_of=as_of,
    )
    enriched = WorkflowApplication._stage_snapshot_enricher(
        app,
        stage="A3",
        lane_id="lane_1",
        model="model",
        upstream_symbols=frozenset({"002957.SZ", "300661.SZ"}),
        snapshot=snapshot,
    )
    assert set(enriched["A3_TECHNICAL_READY"]) == {"002957.SZ", "300661.SZ"}
    assert write_state["overlaps"] == 0
    assert fetch_state["max"] >= 2


def test_workflow_progress_is_emitted_for_node_log_stream(capsys):
    _progress_stdout(
        {
            "run_id": "data-sync",
            "status": "RUNNING",
            "phase": "DATA_SYNC",
            "data": {"processed": 10, "total": 5562, "failures": 1},
        }
    )

    output = capsys.readouterr().out
    assert '"event":"WORKFLOW_PROGRESS"' in output
    assert '"processed":10' in output
    assert '"total":5562' in output


def test_historical_trading_day_validation_does_not_regress_accounts():
    calls = []
    app = SimpleNamespace(
        trading_calendar=SimpleNamespace(is_trading_day=lambda _day: True),
        brokers={"lane_1": SimpleNamespace(start_trading_day=lambda day: calls.append(day))},
    )
    historical = datetime(2026, 8, 25, 15, 10, tzinfo=TZ)
    WorkflowApplication._ensure_trading_day(app, historical, synchronize_accounts=False)
    assert calls == []
    WorkflowApplication._ensure_trading_day(app, historical)
    assert calls == [historical.date()]


def test_research_runtime_injects_exact_model_and_only_approved_pool_flows_downstream():
    snapshot = ResearchSnapshot(
        snapshot_id="snap-1",
        snapshot_hash="a" * 64,
        data={"g0_symbols": ["600519.SH"], "MARKET_REGIME_SNAPSHOT": {"regime": "ROTATION_NO_MAINLINE"}},
    )
    runtime = _runtime_input(snapshot, "lane_1", "deepseek-v4-pro-0813", "A1", None, {"600519.SH"})
    assert runtime["required_envelope"]["model_name"] == "deepseek-v4-pro-0813"
    output = {
        "active_research_pool": [],
        "monitor_pool": [{"symbol": "SHSE.600519"}],
    }
    assert _approved_symbols(output, "A1") == set()


def test_sell_and_reduce_monitor_events_reach_next_bar_simulation(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(
        store,
        account_id="paper:lane_1",
        model="model-a",
        config=SimulationConfig(initial_cash=100_000),
    )
    start = datetime(2026, 8, 24, 10, 0, tzinfo=TZ)
    bought = broker.apply(
        SimulationAction(
            account_id="paper:lane_1",
            signal_id="buy",
            symbol="600519.SH",
            action="BUY",
            signal_bar_end=start,
            entry_reference=10,
            stop_level=9,
            requested_qty=400,
            plan_id="plan-1",
        ),
        _bar(start + timedelta(minutes=1)),
    )
    assert bought.reason_code == "FILLED"
    broker.start_trading_day((start + timedelta(days=1)).date())
    store.create_execution_plan(
        "plan-1",
        "lane_1",
        "600519.SH",
        status=PlanStatus.ACTIVE_TODAY,
        payload={"trigger_low": 10, "trigger_high": 11, "stop_level": 9},
    )
    reduce_time = start + timedelta(days=1)
    store.record_monitor_event(
        event_key="effective:reduce",
        lane_id="lane_1",
        minute_end=reduce_time,
        action=MonitorAction.REDUCE_SIGNAL,
        effective=True,
        payload={"plan_id": "plan-1", "symbol": "600519.SH"},
    )
    app = SimpleNamespace(store=store, brokers={"lane_1": broker})
    reduced = WorkflowApplication._settle_prior_signals(
        app, "lane_1", "600519.SH", _bar(reduce_time + timedelta(minutes=1))
    )
    assert any(item["action"] == "REDUCE" and item["status"] == "FILLED" for item in reduced)
    remaining = store.get_position("paper:lane_1", "600519.SH")["total_qty"]
    assert 0 < remaining < bought.qty

    sell_time = reduce_time + timedelta(minutes=2)
    store.record_monitor_event(
        event_key="effective:sell",
        lane_id="lane_1",
        minute_end=sell_time,
        action=MonitorAction.SELL_SIGNAL,
        effective=True,
        payload={"plan_id": "plan-1", "symbol": "600519.SH"},
    )
    sold = WorkflowApplication._settle_prior_signals(
        app, "lane_1", "600519.SH", _bar(sell_time + timedelta(minutes=1))
    )
    assert any(item["action"] == "SELL" and item["status"] == "FILLED" for item in sold)
    assert store.get_position("paper:lane_1", "600519.SH") is None


def test_morning_review_activates_pending_plans_without_research_models(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    current = datetime(2026, 8, 24, 9, 26, tzinfo=TZ)
    store.create_execution_plan(
        "pending-1",
        "lane_1",
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=current.replace(hour=15, minute=0),
        payload={"trigger_low": 10, "trigger_high": 11, "stop_level": 9},
    )
    bars = (_bar(current - timedelta(minutes=1)), _bar(current))

    class Market:
        def fetch_bars(self, symbol, interval, required_bars, *, as_of):
            assert (symbol, interval, required_bars, as_of) == ("600519.SH", "1m", 2, current)
            return FetchResult(
                symbol=symbol,
                interval=interval,
                requested_bars=2,
                returned_bars=2,
                bars=bars,
                reason_code="OK",
                complete=True,
            )

    app = SimpleNamespace(
        store=store,
        brokers={"lane_1": object()},
        mootdx=Market(),
        minute_store=SimpleNamespace(write=lambda _bars: None),
        settings=SimpleNamespace(workflow_output_dir=tmp_path / "outputs"),
        _ensure_trading_day=lambda _current: None,
    )
    result = WorkflowApplication.review_pending_morning(app, now=current)
    assert result["status"] == "READY"
    assert result["atomic"] is True
    assert result["activated"] == ["pending-1"]
    assert store.get_execution_plan("pending-1")["status"] == PlanStatus.ACTIVE_TODAY.value
    assert store.get_execution_plan("pending-1")["valid_from"].endswith("09:32:00+08:00")


def test_a4_context_contains_independent_closed_1m_5m_15m_ma_and_vwap():
    current = datetime(2026, 8, 24, 10, 0, tzinfo=TZ)
    one = tuple(_bar(current - timedelta(minutes=20 - index)) for index in range(21))
    five = tuple(
        MinuteBar(
            symbol="600519.SH",
            interval="5m",
            bar_end=current.replace(hour=9, minute=30) + timedelta(minutes=5 * (index + 1)),
            open=10,
            high=10.2,
            low=9.9,
            close=10.1,
            volume=1000,
            amount=10_000,
            source_id="mootdx:test",
            adjust_mode="none",
        )
        for index in range(6)
    )
    context = _intraday_market_context("600519.SH", one, five, current=current)
    assert context["realtime_quote"]["bar_end"] == current.isoformat()
    assert len(context["closed_bars"]["1m"]) == 21
    assert len(context["closed_bars"]["5m"]) == 6
    assert len(context["closed_bars"]["15m"]) == 2
    assert context["moving_averages"]["1m"]["ma20"] is not None
    assert context["moving_averages"]["5m"]["vwap"] == 10
