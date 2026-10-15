"""V2 only delays 15m confirmation; independent 5m and exits stay intact."""
from copy import deepcopy
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime import strategies as s

TZ = ZoneInfo("Asia/Shanghai")
END = datetime(2026, 10, 15, 10, 0, tzinfo=TZ)
POLICY = {"STRUCTURE_FAIL_CONFIRM_BARS_15M": 2}


def bar(end=END, *, close=9.5, high=10.1):
    return s._Bar("600001.SH", end, close, high, 9.3, close, 100, 950)


def decision(fifteen, *, five=None, policy=POLICY, profile=s.StrategyProfile.TREND_MA5):
    five = five or [bar(END-timedelta(minutes=5), close=10.2, high=10.3), bar()]
    plan = {"market_context": {"runtime_policy": dict(policy)}} if policy is not None else {}
    return s._pre_entry_risk_decision(profile, plan, five[-1], five, fifteen, 10.)


def test_one_closed_15m_waits_but_two_consecutive_breaks_invalidate():
    assert decision([bar()])["state"] == "CONFIRMING"
    result = decision([bar(END-timedelta(minutes=15)), bar()])
    assert result["state"] == "PLAN_INVALIDATED"
    assert result["met_conditions"] == ["TREND_PRE_ENTRY_CLOSED_BREAKDOWN", "TREND_15M_BREAKDOWN_CONFIRMED"]


def test_legacy_frozen_input_defaults_to_one_bar():
    assert decision([bar()], policy=None)["state"] == "PLAN_INVALIDATED"
    assert decision([bar()], policy={"STRUCTURE_FAIL_CONFIRM_BARS_15M": 1})["state"] == "PLAN_INVALIDATED"


@pytest.mark.parametrize("ends", [
    [END-timedelta(minutes=30), END],
    [END, END],
    [END, END-timedelta(minutes=15)],
    [datetime(2026,10,15,11,30,tzinfo=TZ), datetime(2026,10,15,13,15,tzinfo=TZ)],
])
def test_missing_duplicate_reversed_or_lunch_bars_do_not_confirm(ends):
    bars = [bar(end) for end in ends]
    five = [bar(ends[-1]-timedelta(minutes=5), close=10.2, high=10.3), bar(ends[-1])]
    assert decision(bars, five=five)["state"] != "PLAN_INVALIDATED"


def test_recovery_between_two_breaks_resets_confirmation():
    assert decision([bar(END-timedelta(minutes=30)), bar(END-timedelta(minutes=15), close=10.2, high=10.3), bar()])["state"] == "CONFIRMING"


def test_independent_5m_failed_reclaim_is_immediate():
    five = [bar(END-timedelta(minutes=5), high=9.8), bar(high=9.8)]
    result = decision([bar()], five=five)
    assert result["state"] == "PLAN_INVALIDATED"
    assert "TREND_5M_FAILED_RECLAIM" in result["met_conditions"]
    assert "TREND_15M_BREAKDOWN_CONFIRMED" not in result["met_conditions"]


@pytest.mark.parametrize("profile", [s.StrategyProfile.MA520_SWING,s.StrategyProfile.LEADER_INTRADAY])
def test_other_strategy_pre_entry_routes_unchanged(profile):
    assert decision([bar()], profile=profile) == decision([bar()], policy=None, profile=profile)


@pytest.mark.parametrize("value", [0,3,"2",True,None])
def test_malformed_policy_fails_closed_not_silent_weakening(value):
    result = decision([bar()], policy={"STRUCTURE_FAIL_CONFIRM_BARS_15M":value})
    assert result["action"] == "DATA_BLOCK"
    assert result["reason_codes"] == ["A4_RUNTIME_POLICY_INVALID"]


def minute_bars():
    start = datetime(2026,10,15,9,31,tzinfo=TZ)
    return [{"symbol":"600001.SH","interval":"1m","bar_end":start+timedelta(minutes=i),
             "open":9.5,"high":10.1,"low":9.3,"close":9.5,"volume":100.,"amount":950.,"closed":True}
            for i in range(30)]


def plan():
    return {"symbol":"600001.SH","strategy_profile":"TREND_MA5","stock_behavior_type":"TREND",
            "stop_level":10.,"trigger_zone":{"low":10.,"high":11.},
            "daily_indicators":{"ma5":11.,"ma10":10.8,"ma20":10.5,"close":11.2}}


def test_frozen_effective_policy_is_exact_and_plan_is_not_mutated():
    original = plan(); saved=deepcopy(original)
    context={"runtime_policy":dict(POLICY)}; context_saved=deepcopy(context)
    result=s.evaluate_strategy(original,minute_bars(),now=END,market_context=context)
    assert result.runtime_policy == POLICY
    assert original == saved and context == context_saved
    legacy=s.evaluate_strategy(original,minute_bars(),now=END)
    assert "runtime_policy" not in legacy.model_dump()


@pytest.mark.parametrize("sellable", [0,100])
def test_position_hard_stop_and_t1_semantics_unchanged(sellable):
    position={"total_qty":100,"sellable_qty":sellable,"avg_cost":10.5}
    base=s.evaluate_strategy(plan(),minute_bars(),now=END,position=position).model_dump()
    opted=s.evaluate_strategy(plan(),minute_bars(),now=END,position=position,market_context={"runtime_policy":POLICY}).model_dump()
    opted.pop("runtime_policy",None)
    assert opted == base
    assert base["action"] == "FORCED_RISK_EXIT"


def test_aggregation_uses_only_complete_closed_buckets():
    # Missing minute makes the first bucket unavailable; do not use a partial bar.
    bars=minute_bars()[1:]
    result=s.evaluate_strategy(plan(),bars,now=END,market_context={"runtime_policy":POLICY})
    assert result.state != "PLAN_INVALIDATED"


@pytest.mark.parametrize("runtime_policy,expected", [(None,"PLAN_INVALIDATED"),(POLICY,"START_CONFIRMATION")])
def test_monitor_freezes_policy_and_does_not_modify_original_plan(tmp_path,runtime_policy,expected):
    from liangjian_funnel.data.mootdx import MinuteBar
    from liangjian_funnel.runtime.monitor import MonitorEngine
    from liangjian_funnel.runtime.state import PlanStatus,RuntimeStore
    store=RuntimeStore(tmp_path/'isolated.sqlite3')
    store.create_execution_plan('p','lane-a','600001.SH',status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=END.replace(hour=15),payload=plan())
    store.activate_plan('p',valid_from=END.replace(hour=9,minute=31))
    before=store.get_execution_plan('p')['payload_json']
    rows=minute_bars()[15:]
    rows=[MinuteBar(**{key:value for key,value in row.items() if key!='closed'},source_id='ISOLATED') for row in rows]
    contexts={'600001.SH':{'frozen_marker':'unchanged'}}; original=deepcopy(contexts)
    engine=MonitorEngine(store,runtime_policy=runtime_policy)
    result=engine.process_minute('lane-a',{'600001.SH':rows[-1]},minute_snapshot_id='isolated-minute',
        now=END,bar_histories={'600001.SH':rows},market_contexts=contexts)
    assert result.events[0].action == expected
    persisted=json.loads(store.list_monitor_events(lane_id='lane-a')[-1]['payload_json'])['strategy']
    if runtime_policy is None:
        assert 'runtime_policy' not in persisted
    else:
        assert persisted['runtime_policy'] == POLICY
        replay=s.evaluate_strategy(plan(),rows,now=END,market_context={'runtime_policy':persisted['runtime_policy']})
        assert replay.state == persisted['state'] and replay.action == persisted['action']
    assert store.get_execution_plan('p')['payload_json'] == before
    assert contexts == original


@pytest.mark.parametrize('case',['future','forming'])
def test_opt_in_does_not_accept_unclosed_or_future_minute(case):
    bars=minute_bars()
    if case=='future':
        bars[-1]['bar_end']=END+timedelta(minutes=1)
    else:
        bars[-1]['closed']=False
    result=s.evaluate_strategy(plan(),bars,now=END,market_context={'runtime_policy':POLICY})
    assert result.action=='DATA_BLOCK'


def test_live_workflow_supplies_same_policy_to_model_and_monitor_without_plan_rewrite(monkeypatch, tmp_path):
    """Exercise the real orchestration; no provider, model or production store."""
    from types import SimpleNamespace
    from liangjian_funnel.data.cache import MinuteBarStore
    from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
    from liangjian_funnel.runtime.monitor import MonitorBatchResult
    from liangjian_funnel.runtime.state import PlanStatus, RuntimeStore
    from liangjian_funnel.workflow import WorkflowApplication

    current = END.replace(hour=9, minute=32)
    symbol = '600001.SH'
    source = MinuteBar(symbol=symbol, interval='1m', bar_end=current-timedelta(minutes=1),
        open=10., high=10.2, low=9.9, close=10., volume=100., amount=1000., source_id='ISOLATED')
    store = RuntimeStore(tmp_path/'isolated-state.sqlite3')
    store.create_execution_plan('p', 'lane_1', symbol, status=PlanStatus.ACTIVE_TODAY,
        valid_from=current-timedelta(minutes=2), expires_at=current.replace(hour=15, minute=0), payload=plan())
    original_payload = store.get_execution_plan('p')['payload_json']
    # This isolated fixture intentionally installs all I/O boundaries before
    # invoking monitor_once; production construction is never bypassed.
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(workflow_output_dir=tmp_path/'outputs', research_primary_lane_id='lane_1')
    app.store, app.minute_store = store, MinuteBarStore(tmp_path/'minute')
    app.brokers, app.market_data, app.lark_publisher = {'lane_1':object()}, object(), None
    app._ensure_trading_day = lambda *_: None
    app._expire_missed_a4_entries = lambda *_: None
    app.activate_latest_a3_for_monitor = lambda **_: {}
    app._settle_prior_signals = lambda *_: []
    app._record_a4_outcomes = lambda *_: {}
    app._fetch_live_bars = lambda *_, **__: FetchResult(symbol=symbol, interval='1m', requested_bars=1,
        returned_bars=1, bars=(source,), reason_code='OK', complete=True)
    app._fetch_live_quote = lambda *_, **__: None
    monkeypatch.setattr('liangjian_funnel.workflow.load_or_refresh_live_market_state', lambda *_, **__: {})
    model_contexts, engine_calls = [], []

    def callback(_lane, _plans, contexts, *_args, **_kwargs):
        model_contexts.append(deepcopy(contexts))
        return None

    class Engine:
        def __init__(self, *_args, **kwargs):
            engine_calls.append(deepcopy(kwargs.get('runtime_policy')))

        def process_minute(self, lane, _bars, **kwargs):
            assert kwargs['market_contexts'][symbol]['runtime_policy'] == POLICY
            return MonitorBatchResult(lane_id=lane, minute_snapshot_id=kwargs['minute_snapshot_id'])

    app._a4_callback = callback
    monkeypatch.setattr('liangjian_funnel.workflow.MonitorEngine', Engine)
    app.monitor_once(now=current)
    assert engine_calls == [POLICY]
    assert model_contexts[0][symbol]['runtime_policy'] == POLICY
    assert store.get_execution_plan('p')['payload_json'] == original_payload
