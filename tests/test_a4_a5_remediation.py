from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import json
import sqlite3

import pytest

from liangjian_funnel.data.mootdx import MinuteBar, FetchResult
from liangjian_funnel.pipeline.macd_evidence import macd_evidence
from liangjian_funnel.review.daily import _a3_candidates, _model_fact_projection
from liangjian_funnel.review.verification import counterexample_drop_stage
from liangjian_funnel.runtime.execution_eligibility import project_exit_eligibility
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationAction
from liangjian_funnel.runtime.state import RuntimeStore, MonitorAction, PlanStatus, StateTransitionError
from liangjian_funnel.runtime.stock_trading_rules import stock_trading_rules
from liangjian_funnel.runtime.strategies import _historical_fifteen, _Bar, _macd_observation
from liangjian_funnel.workflow import WorkflowApplication

TZ = ZoneInfo("Asia/Shanghai")


def at(day, hour=10, minute=0):
    return datetime(2026, 9, day, hour, minute, tzinfo=TZ)


def bar(time):
    return MinuteBar(symbol="600519.SH", interval="1m", bar_end=time,
                     open=10, high=11, low=9, close=10, volume=1000,
                     amount=10000, source_id="TEST")


def buy(broker, key, time, kind="BUY", qty=100):
    return broker.apply(SimulationAction(
        account_id="paper:lane_1", signal_id=key, symbol="600519.SH",
        action=kind, signal_bar_end=time, entry_reference=10,
        stop_level=9, plan_id="plan", requested_qty=qty,
    ), bar(time + timedelta(minutes=1)).model_copy(update={"close": 10.5} if kind == "ADD" else {}))


@pytest.mark.parametrize("position, expected", [
    ({"total_qty": 100, "sellable_qty": 0}, "EXIT_PENDING"),
    ({"total_qty": 200, "sellable_qty": 100}, "EXIT_READY"),
    ({"total_qty": 100}, "EXIT_PENDING"),
    ({"total_qty": 100, "sellable_qty": 200}, "EXIT_PENDING"),
])
def test_exit_is_technical_not_a_fill(position, expected):
    result = project_exit_eligibility({"action": "SELL_SIGNAL"}, position)
    assert result["state"] == expected
    assert result["technical_action"] == "SELL_SIGNAL"
    assert "fill" not in result


def test_restart_does_not_unlock_today_and_friday_unlocks_monday(tmp_path):
    store = RuntimeStore(tmp_path / "state.db")
    broker = PaperBroker(store, account_id="paper:lane_1", model="test")
    assert buy(broker, "buy", at(4)).status.value == "FILLED"
    restarted = PaperBroker(RuntimeStore(tmp_path / "state.db"), account_id="paper:lane_1", model="test")
    restarted.start_trading_day(at(4).date())
    assert store.get_position("paper:lane_1", "600519.SH")["sellable_qty"] == 0
    with pytest.raises(StateTransitionError):
        restarted.start_trading_day(at(5).date())
    restarted.start_trading_day(at(7).date())
    assert store.get_position("paper:lane_1", "600519.SH")["sellable_qty"] == 100
    with pytest.raises(StateTransitionError):
        restarted.start_trading_day(at(4).date())


def test_mixed_t1_position_sells_old_shares_and_keeps_residual_exit(tmp_path):
    store = RuntimeStore(tmp_path / "state.db")
    broker = PaperBroker(store, account_id="paper:lane_1", model="test")
    store.create_execution_plan("plan", "lane_1", "600519.SH", status=PlanStatus.ACTIVE_TODAY,
                                payload={"trigger_low": 10, "stop_level": 9})
    assert buy(broker, "old", at(3)).status.value == "FILLED"
    added = buy(broker, "new", at(4), kind="ADD")
    assert added.status.value == "FILLED", added.reason_code
    position = store.get_position("paper:lane_1", "600519.SH")
    assert (position["total_qty"], position["sellable_qty"]) == (200, 100)
    for key, action, time in (("sell", MonitorAction.SELL_SIGNAL, at(4, 10, 2)),
                              ("reduce", MonitorAction.REDUCE_SIGNAL, at(4, 10, 3))):
        store.record_monitor_event(event_key=key, lane_id="lane_1", minute_end=time,
                                   action=action, effective=True,
                                   payload={"plan_id": "plan", "symbol": "600519.SH"})
    app = SimpleNamespace(store=store, brokers={"lane_1": broker})
    settled = WorkflowApplication._settle_prior_signals(app, "lane_1", "600519.SH", bar(at(4, 10, 4)))
    assert [(item["action"], item["qty"]) for item in settled] == [("SELL", 100)]
    assert store.get_position("paper:lane_1", "600519.SH")["total_qty"] == 100
    broker.start_trading_day(at(7).date())
    settled = WorkflowApplication._settle_prior_signals(app, "lane_1", "600519.SH", bar(at(7, 9, 31)))
    assert [(item["action"], item["qty"]) for item in settled] == [("SELL", 100)]
    assert store.get_position("paper:lane_1", "600519.SH") is None
    assert WorkflowApplication._settle_prior_signals(app, "lane_1", "600519.SH", bar(at(7, 9, 32))) == []


def test_first_exit_is_not_overwritten_by_reduction(tmp_path):
    store = RuntimeStore(tmp_path / "state.db")
    plan = store.create_execution_plan("plan", "lane_1", "600519.SH", status=PlanStatus.ACTIVE_TODAY)
    store.record_a4_entry_signal({"event_key": "entry", "event_id": "entry", "lane_id": "lane_1",
                                 "minute_end": at(8), "action": "BUY_SIGNAL",
                                 "payload": {"plan_id": "plan", "symbol": "600519.SH"}}, plan)
    store.apply_a4_fill("entry", {"fill_id": "buy", "action": "BUY", "qty": 100,
                                 "price": 10, "fee": 1, "bar_end": at(8, 10, 1)})
    for key, action, minute in (("sell", MonitorAction.SELL_SIGNAL, 2), ("reduce", MonitorAction.REDUCE_SIGNAL, 3)):
        store.record_monitor_event(event_key=key, lane_id="lane_1", minute_end=at(8, 10, minute),
                                   action=action, effective=True, sync_a4_lifecycle=True, reason_code=key,
                                   payload={"plan_id": "plan", "symbol": "600519.SH", "strategy": {"reference_price": 9.8}})
    row = store.get_a4_signal_lifecycle("entry")
    metadata = json.loads(row["exit_metadata_json"])
    assert row["exit_reason"] == "sell"
    assert row["exit_signal_price"] == 9.8
    assert row["exit_signal_qty"] == 100
    assert metadata["first_exit"]["event_key"] == "sell"
    assert metadata["highest_priority_exit"]["event_key"] == "sell"
    assert metadata["latest_observation"]["event_key"] == "reduce"


def test_legacy_projection_repair_is_dry_run_then_audited_and_idempotent(tmp_path):
    store = RuntimeStore(tmp_path / "state.db")
    broker = PaperBroker(store, account_id="paper:lane_1", model="test")
    plan = store.create_execution_plan("plan", "lane_1", "600519.SH", status=PlanStatus.ACTIVE_TODAY)
    row, _ = store.record_a4_entry_signal({"event_key": "entry", "event_id": "entry", "lane_id": "lane_1",
        "minute_end": at(8), "action": "BUY_SIGNAL", "payload": {"plan_id": "plan", "symbol": "600519.SH"}}, plan)
    fill = buy(broker, "entry", at(8)).fill
    store.apply_a4_fill("entry", fill)
    for key, action, minute in (("sell", MonitorAction.SELL_SIGNAL, 2), ("reduce", MonitorAction.REDUCE_SIGNAL, 3)):
        store.record_monitor_event(event_key=key, lane_id="lane_1", minute_end=at(8, 10, minute),
            action=action, effective=True, sync_a4_lifecycle=True, reason_code=key,
            payload={"plan_id": "plan", "symbol": "600519.SH", "strategy": {"reference_price": 9.8}})
    with sqlite3.connect(store.path) as connection:
        connection.execute("UPDATE a4_signal_lifecycles SET exit_reason='reduce',exit_signal_qty=0,exit_signal_price=NULL,exit_metadata_json='{}'")
    raw_events = store.list_monitor_events(lane_id="lane_1")
    raw_fills = store.list_fills()
    preview = store.rebuild_a4_exit_projection(row["lifecycle_id"])
    assert preview["changed"] and not preview["applied"]
    assert store.get_a4_lifecycle("entry")["exit_reason"] == "reduce"
    repaired = store.rebuild_a4_exit_projection(row["lifecycle_id"], apply=True)
    assert repaired["after"]["exit_reason"] == "sell"
    assert repaired["after"]["exit_signal_qty"] == 100
    assert store.rebuild_a4_exit_projection(row["lifecycle_id"], apply=True)["changed"] is False
    assert store.list_monitor_events(lane_id="lane_1") == raw_events
    assert store.list_fills() == raw_fills


@pytest.mark.parametrize("symbol,pool,plans,candidates,expected", [
    ("002286.SZ", "WATCH", [], {"002286.SZ": {}}, "A3_NOT_PLANNED"),
    ("300647.SZ", "WATCH", ["300647.SZ"], {}, "A4_NO_EFFECTIVE_SIGNAL"),
    ("301189.SZ", "WATCH", [], {}, "UNRESOLVED_A3_LINEAGE_MISSING"),
    ("688001.SH", "REJECTED", [], {}, "A2_REJECTED"),
])
def test_a5_uses_real_lineage(symbol, pool, plans, candidates, expected):
    assert counterexample_drop_stage(symbol, pool, plans, candidates) == expected


def test_a3_all_real_pool_names_are_projected():
    output = {name: [{"symbol": symbol}] for name, symbol in (
        ("core_watch_pool", "002286.SZ"), ("secondary_watch_pool", "300647.SZ"),
        ("rejected_candidates", "301189.SZ"))}
    rows = _a3_candidates({"stages": [{"stage": "A3", "output": output}]})
    assert {row["symbol"] for row in rows} == {"002286.SZ", "300647.SZ", "301189.SZ"}


def test_macd_requires_history_and_is_reproducible():
    assert macd_evidence([10] * 34)["available"] is False
    ready = macd_evidence([10] * 35)
    assert ready["available"] and ready["hist"] == 0
    assert ready == macd_evidence([10] * 35)
    assert macd_evidence([10] * 34 + [float("nan")])["available"] is False


def test_board_lots_odd_balance_and_tick():
    main = stock_trading_rules("600519.SH")
    star = stock_trading_rules("688001.SH")
    assert main.floor_buy(137) == 100
    assert star.floor_buy(199) == 0 and star.floor_buy(201) == 201
    assert main.sell_quantity(37, 37) == 37
    assert main.sell_quantity(37, 137) == 0
    assert main.adverse_tick(30.124094, buy=True) == 30.13
    assert main.adverse_tick(30.124094, buy=False) == 30.12
    with pytest.raises(ValueError):
        stock_trading_rules("920001.BJ")


@pytest.mark.parametrize("secondary_volume,complete", [(1000, True), (0, False)])
def test_close_zero_volume_requires_independent_finalized_window(secondary_volume, complete):
    current = at(8, 15, 0)
    base = bar(current)
    first = FetchResult(symbol=base.symbol, interval="1m", requested_bars=1,
                        returned_bars=1, bars=(base.model_copy(update={"volume": 0}),),
                        complete=True, reason_code="OK")
    second = first.model_copy(update={"bars": (base.model_copy(update={"volume": secondary_volume}),)})
    app = SimpleNamespace(market_data=SimpleNamespace(
        fallback=SimpleNamespace(fetch_bars=lambda *a, **k: first),
        primary=SimpleNamespace(fetch_bars=lambda *a, **k: second)))
    result = WorkflowApplication._fetch_live_bars(app, base.symbol, "1m", 1, current)
    assert result.complete is complete
    assert first.bars[0].volume == 0
    if not complete:
        assert result.reason_code == "CLOSE_BAR_FINALIZATION_UNCONFIRMED"


def test_historical_macd_uses_contiguous_prior_sessions_only():
    history = []
    for day in (3, 4, 7, 8, 9):
        for hour, minute in ((9, 35), (13, 5)):
            for index in range(24):
                item = bar(at(day, hour, minute) + timedelta(minutes=5 * index))
                history.append(item.model_copy(update={"interval": "5m"}).model_dump(mode="json"))
    current = [_Bar(symbol="600519.SH", end=at(8, 9, 45), open=10, high=11, low=9, close=10, volume=1000, amount=10000)]
    prior = _historical_fifteen({"market_context": {"historical_5m": history}}, current)
    assert len(prior) == 48 and prior[-1].end == at(7, 15, 0)
    assert _macd_observation([*prior, *current])["warmup_complete"]
    missing_close = [item for item in history if item["bar_end"] != at(7, 15, 0).isoformat()]
    assert _historical_fifteen({"market_context": {"historical_5m": missing_close}}, current) == []


def test_outcome_reason_correction_preserves_returns_and_event(tmp_path):
    store = RuntimeStore(tmp_path / "state.db")
    event, _ = store.record_monitor_event(event_key="entry", lane_id="lane_1", minute_end=at(8),
        action=MonitorAction.BUY_SIGNAL, reason_code="TECHNICAL_PASS", effective=True,
        payload={"symbol": "600519.SH", "strategy": {"reason_codes": ["RR_OK"]}})
    label = store.record_outcome_label({"run_id": "run", "lane_id": "lane_1", "trade_date": "2026-09-08",
        "stage": "A4", "symbol": "600519.SH", "decision": "PASSED", "reason_codes": [],
        "selection_basis": "A4_EXECUTION_SIGNAL", "snapshot_id": "snap", "config_hash": "cfg",
        "metadata": {"entry_event_key": "entry"}})
    before = store.get_outcome_label(label["label_id"])
    assert store.repair_a4_outcome_reasons(label["label_id"])["applied"] is False
    assert store.get_outcome_label(label["label_id"]) == before
    result = store.repair_a4_outcome_reasons(label["label_id"], apply=True)
    assert result["after"] == ["TECHNICAL_PASS", "RR_OK"]
    after = store.get_outcome_label(label["label_id"])
    for key, value in before.items():
        if key not in {"reason_codes", "metadata_json"}:
            assert after[key] == value
    assert store.repair_a4_outcome_reasons(label["label_id"], apply=True)["changed"] is False
    assert store.list_monitor_events(lane_id="lane_1")[0] == event


def test_a5_compaction_keeps_complete_counts_and_effective_events():
    events = [{"evidence_id": f"E:{index}", "event_id": str(index), "minute_end": f"{index:06}",
               "plan_id": f"P{index % 3}", "symbol": str(index % 3), "effective": False,
               "action": "START_CONFIRMATION", "reason_code": "WAIT" if index % 2 else "RR_LOW",
               "strategy_reason_codes": ["RR_LOW"] if index % 2 else ["WAIT"],
               "unmet_conditions": ["X"] if index != 333 else []} for index in range(5000)]
    effective = {**events[0], "evidence_id": "E:BUY", "effective": True, "action": "BUY_SIGNAL"}
    events.append(effective)
    facts = {"a4": {"events": events}, "metrics": {"a4_monitor_observation_count": 5001}}
    original = json.dumps(facts, sort_keys=True)
    projected = _model_fact_projection(facts)
    groups = projected["a4"]["observation_groups"]
    assert sum(row["observation_count"] for row in groups) == 5000
    assert sum(sum(row["primary_reason_counts"].values()) for row in groups) == 5000
    assert effective in projected["a4"]["events"]
    assert events[333] in projected["a4"]["events"]
    assert len(projected["a4"]["events"]) <= 10
    assert projected["metrics"] == facts["metrics"]
    assert json.dumps(facts, sort_keys=True) == original
