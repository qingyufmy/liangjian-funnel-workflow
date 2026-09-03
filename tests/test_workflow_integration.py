from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from threading import RLock
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationAction, SimulationConfig
from liangjian_funnel.runtime.state import A4SignalStatus, MonitorAction, PlanStatus, RuntimeStore
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot as ResearchSnapshot,
    LaneResult,
    ResearchRunResult,
)
from liangjian_funnel.pipeline.research import _approved_symbols, _runtime_input
from liangjian_funnel.workflow import (
    WorkflowApplication,
    _a4_prompt_plan,
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
    assert len(first_calls) == 0

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


def test_monitor_archives_invalidated_plan_without_returning_it_to_a4(tmp_path):
    current = datetime(2026, 9, 3, 10, 0, tzinfo=TZ)
    store = RuntimeStore(tmp_path / "archive-only.sqlite3")
    store.create_execution_plan(
        "invalidated-plan",
        "lane_1",
        "600176.SH",
        status=PlanStatus.INVALIDATED,
        expires_at=datetime(2026, 9, 3, 15, 0, tzinfo=TZ),
        payload={"name": "中国巨石"},
    )

    class Provider:
        def __init__(self):
            self.calls: list[tuple[str, str, int]] = []

        def fetch_bars(self, symbol, interval, required_bars, *, as_of):
            self.calls.append((symbol, interval, required_bars))
            step = 1 if interval == "1m" else 5
            bars = tuple(
                MinuteBar(
                    symbol=symbol,
                    interval=interval,
                    bar_end=as_of - timedelta(minutes=step * (required_bars - index - 1)),
                    open=10,
                    high=10.2,
                    low=9.9,
                    close=10.1,
                    volume=1_000,
                    amount=10_000,
                    source_id="TENCENT:test",
                    adjust_mode="none",
                )
                for index in range(required_bars)
            )
            return FetchResult(
                symbol=symbol,
                interval=interval,
                requested_bars=required_bars,
                returned_bars=len(bars),
                bars=bars,
                reason_code="OK",
                complete=True,
            )

    provider = Provider()
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(
        fact_store_dir=tmp_path / "facts",
        workflow_output_dir=tmp_path / "outputs",
        research_primary_lane_id="lane_1",
    )
    app.store = store
    app.minute_store = MinuteBarStore(tmp_path / "minute")
    app.market_data = SimpleNamespace(fallback=provider)
    app.brokers = {"lane_1": object()}
    app.lark_publisher = None
    app._ensure_trading_day = lambda _current: None
    app._expire_missed_a4_entries = lambda _current: None
    app.activate_latest_a3_for_monitor = lambda *, now: {
        "status": "NOT_APPLICABLE",
        "reason_code": "A3_SCOPE_ACTIVATION_WINDOW_CLOSED",
        "as_of": now.isoformat(),
        "activated": [],
        "invalidated": [],
    }

    result = app.monitor_once(now=current)

    assert result["archive_only_symbols"] == ["600176.SH"]
    assert {call[:2] for call in provider.calls} == {
        ("600176.SH", "1m"),
        ("600176.SH", "5m"),
    }
    archived = app.minute_store.load_latest("600176.SH", "1m", limit=240)
    assert archived
    assert archived[-1].bar_end == current
    # The terminal plan remains terminal and never appears in an A4 decision
    # event, signal lifecycle, or position.
    assert store.get_execution_plan("invalidated-plan")["status"] == PlanStatus.INVALIDATED.value
    assert store.get_position("paper:lane_1", "600176.SH") is None
    for event in store.list_monitor_events(lane_id="lane_1"):
        payload = json.loads(str(event.get("payload_json") or "{}"))
        assert payload.get("symbol") != "600176.SH"


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
    assert _plan_expiry(
        "2026-10-01T09:15:00+08:00",
        datetime(2026, 9, 30, 15, 10, tzinfo=TZ),
        "close",
        minimum_trade_date=date(2026, 10, 8),
    ) == datetime(2026, 10, 8, 15, 0, tzinfo=TZ)
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
                "strategy_profile": "TREND_MA5",
                "eligibility": "QUALIFIED",
                "review_status": "PASS",
                "trigger_zone": {"low": 10, "high": 11},
                "invalidation_level": 9,
            },
            {
                "plan_id": "allowed-plan",
                "symbol": "000001.SZ",
                "risk_unit": "STANDARD",
                "strategy_profile": "MA520_SWING",
                "eligibility": "QUALIFIED",
                "review_status": "PASS",
                "trigger_zone": {"low": 10, "high": 11},
                "invalidation_level": 9,
            },
            {
                "plan_id": "behavior-conflict-plan",
                "symbol": "600000.SH",
                "risk_unit": "STANDARD",
                "strategy_profile": "TREND_MA5",
                "strategy_version": "a3-a4-three-strategy/1.3.0",
                "stock_behavior_type": "EMOTION",
                "route_permission": "ALLOW_A4",
                "eligibility": "QUALIFIED",
                "review_status": "PASS",
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
                "600000.SH": {"tradable": True, "exclusion_reasons": []},
            }
        },
    )

    assert len(publication["created"]) == 1
    assert store.list_execution_plans(lane_id="lane_1")[0]["symbol"] == "000001.SZ"
    assert {
        (item.get("symbol"), item["reason"])
        for item in publication["blocked"]
    } == {
        ("600519.SH", "PLAN_SYMBOL_NOT_TRADABLE"),
        ("600000.SH", "A3_STRATEGY_CONTRACT_NOT_EXECUTABLE"),
    }


def test_plan_publication_allows_qualified_a2_watch_origin_as_probe(tmp_path):
    store = RuntimeStore(tmp_path / "runtime-watch-core.sqlite3")
    source_close = datetime(2026, 8, 31, 23, 40, tzinfo=TZ)
    output = {
        "core_watch_pool": [
            {
                "plan_id": "qualified-watch-probe",
                "symbol": "000713.SZ",
                "candidate_origin": "WATCH_ONLY",
                "risk_unit": "PROBE",
                "strategy_profile": "TREND_MA5",
                "eligibility": "QUALIFIED",
                "review_status": "PASS",
                "trigger_zone": {"low": 6.418, "high": 6.4822},
                "invalidation_level": 6.4,
                "reward_risk": 4.23,
                "stop_distance_pct": 0.013,
            }
        ],
        "secondary_watch_pool": [],
    }
    result = ResearchRunResult(
        run_id="run-qualified-watch-core",
        generated_at=source_close,
        snapshot_id="snapshot-qualified-watch-core",
        snapshot_hash="e" * 64,
        status="READY",
        lanes=(LaneResult("lane_1", "model", "READY", (), output),),
        audit_paths=(),
        markdown_path=None,
    )

    publication = WorkflowApplication._publish_plans(
        SimpleNamespace(store=store),
        result,
        "close",
        source_close,
        snapshot_data={
            "A3_CANDIDATE_ORIGIN": {"000713.SZ": "WATCH_ONLY"},
            "TRADABILITY_FLAGS": {"000713.SZ": {"tradable": True}},
        },
    )

    assert len(publication["created"]) == 1
    assert publication["blocked"] == []
    plan = store.list_execution_plans(lane_id="lane_1")[0]
    assert plan["symbol"] == "000713.SZ"
    assert plan["status"] == PlanStatus.PENDING_MORNING_REVIEW.value
    assert datetime.fromisoformat(str(plan["expires_at"])) == datetime(2026, 9, 1, 15, 0, tzinfo=TZ)


def test_same_day_recovery_publishes_new_a3_scope_as_pending_without_parent(tmp_path):
    store = RuntimeStore(tmp_path / "runtime-same-day-recovery.sqlite3")
    frozen_at = datetime(2026, 9, 3, 0, 51, tzinfo=TZ)
    store.create_execution_plan(
        "stale-pending",
        "lane_1",
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=datetime(2026, 9, 3, 15, 0, tzinfo=TZ),
        payload={"source_run_id": "old-run"},
    )
    output = {
        "core_watch_pool": [
            {
                "plan_id": "recovered-plan",
                "symbol": "002837.SZ",
                "risk_unit": "PROBE",
                "strategy_profile": "TREND_MA5",
                "strategy_version": "a3-a4-three-strategy/1.3.0",
                "stock_behavior_type": "TREND",
                "route_permission": "ALLOW_A4",
                "eligibility": "QUALIFIED",
                "review_status": "PASS",
                "trigger_zone": {"low": 47.0, "high": 48.0},
                "invalidation_level": 45.5,
                "plan_expiry": "2026-09-03T15:00:00+08:00",
            }
        ],
        "secondary_watch_pool": [],
    }
    result = ResearchRunResult(
        run_id="same-day-recovery-2026-09-03",
        generated_at=frozen_at,
        snapshot_id="snapshot-recovery",
        snapshot_hash="f" * 64,
        status="READY",
        lanes=(LaneResult("lane_1", "model", "READY", (), output),),
        audit_paths=(),
        markdown_path=None,
    )

    publication = WorkflowApplication._publish_plans(
        SimpleNamespace(store=store),
        result,
        "morning",
        frozen_at,
        snapshot_data={"TRADABILITY_FLAGS": {"002837.SZ": {"tradable": True}}},
        minimum_trade_date=frozen_at.date(),
        same_day_recovery=True,
    )

    assert len(publication["created"]) == 1
    assert publication["activated"] == []
    assert publication["blocked"] == []
    assert publication["publication_mode"] == "SAME_DAY_RECOVERY_PENDING"
    recovered = store.get_execution_plan(publication["created"][0])
    assert recovered is not None
    assert recovered["status"] == PlanStatus.PENDING_MORNING_REVIEW.value
    assert store.get_execution_plan("stale-pending")["status"] == PlanStatus.INVALIDATED.value


def test_close_publication_replaces_pending_scope_even_when_new_a3_is_empty(tmp_path):
    store = RuntimeStore(tmp_path / "runtime-replacement.sqlite3")
    current = datetime(2026, 9, 1, 10, 12, tzinfo=TZ)
    expiry = datetime(2026, 9, 2, 15, 0, tzinfo=TZ)
    store.create_execution_plan(
        "stale-pending",
        "lane_1",
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=expiry,
        payload={"source_run_id": "old"},
    )
    store.create_execution_plan(
        "active-today",
        "lane_1",
        "000001.SZ",
        status=PlanStatus.ACTIVE_TODAY,
        expires_at=expiry,
        payload={"source_run_id": "old"},
    )
    result = ResearchRunResult(
        run_id="new-close-empty",
        generated_at=current,
        snapshot_id="snapshot-empty",
        snapshot_hash="a" * 64,
        status="READY",
        lanes=(
            LaneResult(
                "lane_1",
                "model",
                "READY",
                (),
                {"core_watch_pool": [], "secondary_watch_pool": []},
            ),
        ),
        audit_paths=(),
        markdown_path=None,
    )

    publication = WorkflowApplication._publish_plans(
        SimpleNamespace(store=store),
        result,
        "close",
        current,
        snapshot_data={},
    )

    assert publication["created"] == []
    assert store.get_execution_plan("stale-pending")["status"] == PlanStatus.INVALIDATED.value
    assert store.get_execution_plan("active-today")["status"] == PlanStatus.ACTIVE_TODAY.value


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
    assert publication["created"] == []
    assert store.list_execution_plans(lane_id="lane_1") == ()
    assert ("000001.SZ", "A3_SECONDARY_NON_EXECUTABLE") in {
        (item.get("symbol"), item["reason"]) for item in publication["blocked"]
    }
    assert ("002957.SZ", "A3_SECONDARY_NON_EXECUTABLE") in {
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
    assert publication["created"] == []
    assert store.list_execution_plans(lane_id="lane_1") == ()


def test_a3_uses_minute_cache_only_as_optional_observation(monkeypatch):
    import liangjian_funnel.workflow as workflow_module

    as_of = datetime(2026, 8, 28, 15, 10, tzinfo=TZ)
    calls = {"writes": 0, "fetches": 0}

    class MinuteStore:
        def load_latest(self, *_args, **_kwargs):
            return []

        def write(self, _bars):
            calls["writes"] += 1

    class FactCache:
        def query_daily_bars(self, *_args, **_kwargs):
            return []

    class Mootdx:
        def fetch_bars(self, *_args, **_kwargs):
            calls["fetches"] += 1
            raise AssertionError("A3 must not fetch minute history")

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
        lambda _factor, **_kwargs: {"KLINE_PATTERNS": {}, "PRICE_LEVELS": {"available": True}},
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
    assert calls == {"writes": 0, "fetches": 0}


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


def test_simulation_does_not_retry_a_signal_after_its_only_next_bar(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    at = datetime(2026, 8, 24, 10, 0, tzinfo=TZ)
    store.create_execution_plan(
        "plan-late",
        "lane_1",
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=at.replace(hour=15),
        payload={"trigger_low": 10, "trigger_high": 11, "stop_level": 9},
    )
    store.activate_plan("plan-late", valid_from=at)
    store.record_monitor_event(
        event_key="effective:late",
        lane_id="lane_1",
        minute_end=at,
        action=MonitorAction.BUY_SIGNAL,
        effective=True,
        payload={"plan_id": "plan-late", "symbol": "600519.SH"},
    )
    broker = PaperBroker(store, account_id="paper:lane_1", model="TEST_ONLY")
    app = SimpleNamespace(store=store, brokers={"lane_1": broker})
    assert WorkflowApplication._settle_prior_signals(
        app,
        "lane_1",
        "600519.SH",
        _bar(at + timedelta(minutes=2)),
    ) == []
    assert store.list_fills("paper:lane_1") == ()


def test_a4_signal_lifecycle_survives_t1_and_closes_on_next_session(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(
        store,
        account_id="paper:lane_1",
        model="deepseek-v4-pro-0813",
        config=SimulationConfig(initial_cash=100_000),
    )
    signal_time = datetime(2026, 9, 3, 10, 0, tzinfo=TZ)
    store.create_execution_plan(
        "plan-lifecycle",
        "lane_1",
        "600519.SH",
        status=PlanStatus.ACTIVE_TODAY,
        expires_at=signal_time.replace(hour=15),
        payload={
            "source_run_id": "run-20260903",
            "name": "贵州茅台",
            "stock_behavior_type": "TREND",
            "strategy_profile": "TREND_MA5",
            "trigger_low": 10,
            "trigger_high": 11,
            "stop_level": 9,
        },
    )
    store.record_monitor_event(
        event_key="effective:entry-lifecycle",
        lane_id="lane_1",
        minute_end=signal_time,
        action=MonitorAction.BUY_SIGNAL,
        reason_code="DETERMINISTIC_TRIGGER_PASS",
        effective=True,
        sync_a4_lifecycle=True,
        payload={"plan_id": "plan-lifecycle", "symbol": "600519.SH"},
    )
    app = SimpleNamespace(store=store, brokers={"lane_1": broker})

    entry = WorkflowApplication._settle_prior_signals(
        app,
        "lane_1",
        "600519.SH",
        _bar(signal_time + timedelta(minutes=1)),
    )
    assert entry[0]["status"] == "FILLED"
    lifecycle = store.get_a4_signal_lifecycle("effective:entry-lifecycle")
    assert lifecycle["status"] == A4SignalStatus.OPEN.value
    assert lifecycle["remaining_qty"] > 0
    store.observe_a4_lifecycle(
        "paper:lane_1",
        "600519.SH",
        signal_time + timedelta(minutes=2),
        10.4,
        9.8,
        10.2,
    )

    exit_time = signal_time + timedelta(minutes=3)
    store.record_monitor_event(
        event_key="effective:exit-lifecycle",
        lane_id="lane_1",
        minute_end=exit_time,
        action=MonitorAction.FORCED_RISK_EXIT,
        reason_code="HARD_STOP",
        effective=True,
        sync_a4_lifecycle=True,
        payload={"plan_id": "plan-lifecycle", "symbol": "600519.SH"},
    )
    assert store.get_a4_signal_lifecycle("effective:entry-lifecycle")["status"] == A4SignalStatus.EXIT_PENDING.value
    # Same-day A-share quantity is not sellable. The exit intent remains
    # durable instead of disappearing or being converted into invalidation.
    assert WorkflowApplication._settle_prior_signals(
        app,
        "lane_1",
        "600519.SH",
        _bar(exit_time + timedelta(minutes=1)),
    ) == []

    next_day = datetime(2026, 9, 4, 9, 31, tzinfo=TZ)
    broker.start_trading_day(next_day.date())
    closed = WorkflowApplication._settle_prior_signals(
        app,
        "lane_1",
        "600519.SH",
        _bar(next_day),
    )
    assert any(item["action"] == "FORCED_RISK_EXIT" and item["status"] == "FILLED" for item in closed)
    lifecycle = store.get_a4_signal_lifecycle("effective:entry-lifecycle")
    assert lifecycle["status"] == A4SignalStatus.CLOSED.value
    assert lifecycle["exit_reason"] == "HARD_STOP"
    assert lifecycle["remaining_qty"] == 0
    assert lifecycle["mfe"] is not None
    assert lifecycle["mae"] is not None
    assert lifecycle["net_return"] is not None


def test_missed_next_bar_entry_is_retained_as_unfilled_sample(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    signal_time = datetime(2026, 9, 3, 10, 0, tzinfo=TZ)
    store.create_execution_plan(
        "plan-unfilled",
        "lane_1",
        "600519.SH",
        status=PlanStatus.ACTIVE_TODAY,
        payload={
            "source_run_id": "run-20260903",
            "name": "贵州茅台",
            "stock_behavior_type": "TREND",
            "strategy_profile": "TREND_MA5",
            "trigger_low": 10,
            "trigger_high": 11,
            "stop_level": 9,
        },
    )
    store.record_monitor_event(
        event_key="effective:unfilled",
        lane_id="lane_1",
        minute_end=signal_time,
        action=MonitorAction.BUY_SIGNAL,
        effective=True,
        sync_a4_lifecycle=True,
        payload={"plan_id": "plan-unfilled", "symbol": "600519.SH"},
    )
    app = SimpleNamespace(store=store)

    expired = WorkflowApplication._expire_missed_a4_entries(
        app,
        signal_time + timedelta(minutes=2),
    )

    assert expired == 1
    lifecycle = store.get_a4_signal_lifecycle("effective:unfilled")
    assert lifecycle["status"] == A4SignalStatus.UNFILLED.value
    assert lifecycle["exit_reason"] == "ENTRY_NEXT_BAR_MISSED"


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
    class Market:
        def fetch_quote(self, symbol, *, as_of):
            from liangjian_funnel.data.tencent_minute import MarketQuote, QuoteResult

            assert (symbol, as_of) == ("600519.SH", current)
            return QuoteResult(
                symbol=symbol,
                reason_code="OK",
                complete=True,
                quote=MarketQuote(
                    symbol=symbol,
                    name="贵州茅台",
                    quote_time=current,
                    price=10.5,
                    open=10.5,
                    previous_close=10.4,
                    volume=1_000,
                    amount=10_500,
                ),
            )

    app = SimpleNamespace(
        store=store,
        brokers={"lane_1": object()},
        market_data=Market(),
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


def test_a4_prompt_plan_excludes_full_research_lineage():
    projected = _a4_prompt_plan(
        {
            "plan_id": "plan-1",
            "lane_id": "lane_1",
            "symbol": "600519.SH",
            "status": "ACTIVE_TODAY",
            "valid_from": "2026-08-31T09:32:00+08:00",
            "expires_at": "2026-08-31T15:00:00+08:00",
            "payload_json": __import__("json").dumps(
                {
                    "name": "贵州茅台",
                    "trigger_low": 10,
                    "trigger_high": 11,
                    "stop_level": 9,
                    "confirmation_bars": 2,
                    "full_research_evidence": "x" * 100_000,
                }
            ),
        }
    )
    assert projected["plan_id"] == "plan-1"
    assert projected["trigger_low"] == 10
    assert "full_research_evidence" not in projected
    assert len(str(projected)) < 2_000
