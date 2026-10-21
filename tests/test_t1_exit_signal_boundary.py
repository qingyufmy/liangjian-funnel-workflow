from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.execution_eligibility import sellable_qty


AT = datetime(2026, 10, 21, 14, 51, tzinfo=ZoneInfo("Asia/Shanghai"))


class DatedPositions:
    def __init__(self, lots, total, sellable):
        self.lots = lots
        self.position = {"total_qty": total, "sellable_qty": sellable}

    def get_position(self, account, symbol):
        assert (account, symbol) == ("paper:lane_1", "002270.SZ")
        return self.position

    def list_position_lots(self, account, symbol):
        assert (account, symbol) == ("paper:lane_1", "002270.SZ")
        return self.lots


def lot(qty, acquired="2026-10-21", released="2026-10-22"):
    return {"remaining_qty": qty, "acquired_trade_date": acquired, "sellable_from": released}


@pytest.mark.parametrize("declared", [0, 10500])
def test_real_002270_five_same_day_fills_never_sellable(declared):
    store = DatedPositions([lot(x) for x in (3700, 2100, 2400, 1800, 500)], 10500, declared)
    assert sellable_qty(store, "paper:lane_1", "002270.SZ", AT) == 0


def test_yesterday_and_mixed_lots_only_release_previous_day():
    store = DatedPositions([lot(300), lot(700, "2026-10-20", "2026-10-21")], 1000, 1000)
    assert sellable_qty(store, "paper:lane_1", "002270.SZ", AT) == 700


@pytest.mark.parametrize("lots", [[], [lot(10499)], [lot(10500, "bad")]])
def test_missing_or_conflicting_lot_lineage_is_unknown(lots):
    store = DatedPositions(lots, 10500, 10500)
    assert sellable_qty(store, "paper:lane_1", "002270.SZ", AT) is None


class EventStore(DatedPositions):
    def __init__(self, lots, total, sellable):
        super().__init__(lots, total, sellable)
        self.events = {}

    def record_monitor_event(self, **values):
        key = values['event_key']
        if key in self.events:
            return self.events[key], False
        self.events[key] = values
        return values, True


@pytest.mark.parametrize('action', ['REDUCE_SIGNAL', 'SELL_SIGNAL', 'FORCED_RISK_EXIT'])
def test_same_day_exit_becomes_single_non_executable_risk_task(action):
    from liangjian_funnel.runtime.monitor import MonitorEngine

    store = EventStore([lot(x) for x in (3700, 2100, 2400, 1800, 500)], 10500, 0)
    plan = {'plan_id': 'real-002270', 'symbol': '002270.SZ'}
    for delta in range(5):
        result = MonitorEngine(store)._emit_effective('lane_1', plan,
            AT + timedelta(minutes=delta), 'original-1450-closed5m', action,
            'TREND_HIGH_VOLUME_UPPER_SHADOW',
            strategy_result={'state': 'EXIT_PENDING', 'execution_eligibility': {
                'total_qty': 10500, 'sellable_qty': 0, 'reason': 'BLOCKED_T1'}})
        assert result.action == 'NO_ACTION'
        assert result.reason_code == 'T1_RISK_PENDING'
        assert result.effective is False
    assert len(store.events) == 1
    row = next(iter(store.events.values()))
    assert row['effective'] is False
    assert row['payload']['risk_pending']['not_an_executable_order'] is True
    assert row['payload']['risk_pending']['trigger_action'] == action


def test_yesterday_lot_still_allows_a_fresh_exit():
    from liangjian_funnel.runtime.monitor import MonitorEngine

    store = EventStore([lot(700, '2026-10-20', '2026-10-21')], 700, 700)
    result = MonitorEngine(store)._emit_effective('lane_1',
        {'plan_id': 'yesterday-held', 'symbol': '002270.SZ'}, AT, 'fresh-quote',
        'REDUCE_SIGNAL', 'TREND_HIGH_VOLUME_UPPER_SHADOW')
    assert result.action == 'REDUCE_SIGNAL' and result.effective


def test_next_open_reassesses_fresh_price_not_the_old_action():
    import json
    from types import SimpleNamespace
    from liangjian_funnel.runtime.monitor import MonitorEngine, MonitorEvent

    store = EventStore([], 0, 0)
    pending = {'event_id': 'real-1451-pending', 'minute_end': AT.isoformat(),
        'reason_code': 'T1_RISK_PENDING', 'payload_json': json.dumps({
            'symbol': '002270.SZ', 'plan_id': 'real-002270', 'risk_pending': {
                'account_id': 'paper:lane_1', 'trigger_action': 'REDUCE_SIGNAL'}})}
    minute = AT.replace(day=22, hour=9, minute=31)
    observation = minute - timedelta(minutes=1)
    fresh = SimpleNamespace(interval='1m', bar_end=observation, close=18.95)
    outcome = MonitorEvent(lane_id='lane_1', plan_id='real-002270', symbol='002270.SZ',
        minute_end=minute, action='NO_ACTION', reason_code='FRESH_RISK_CONDITION_CLEAR')
    MonitorEngine(store)._record_t1_open_reassessment('lane_1', minute, 'new-opening',
        [pending], [outcome], {'002270.SZ': fresh}, observation)
    row = next(iter(store.events.values()))
    assert row['reason_code'] == 'T1_OPEN_REASSESSMENT' and not row['effective']
    assert row['payload']['new_price'] == 18.95
    assert row['payload']['old_action_replayed'] is False
    assert row['payload']['status'] == 'FRESH_RULES_EVALUATED'


def test_missing_opening_evidence_does_not_consume_later_fresh_reassessment():
    import json
    from types import SimpleNamespace
    from liangjian_funnel.runtime.monitor import MonitorEngine, MonitorEvent

    store = EventStore([], 0, 0)
    pending = {'event_id': 'pending', 'minute_end': AT.isoformat(),
        'reason_code': 'T1_RISK_PENDING', 'payload_json': json.dumps({
            'symbol': '002270.SZ', 'risk_pending': {'account_id': 'paper:lane_1'}})}
    minute = AT.replace(day=22, hour=9, minute=30)
    engine = MonitorEngine(store)
    blocked = MonitorEvent(lane_id='lane_1', symbol='002270.SZ', minute_end=minute,
        action='DATA_BLOCK', reason_code='BAR_MISSING')
    engine._record_t1_open_reassessment('lane_1', minute, 'missing', [pending], [blocked], {}, minute)
    next_minute = minute + timedelta(minutes=1)
    clear = blocked.model_copy(update={'minute_end': next_minute, 'action': 'NO_ACTION'})
    fresh = SimpleNamespace(interval='1m', bar_end=minute, close=18.8)
    engine._record_t1_open_reassessment('lane_1', next_minute, 'fresh', [pending],
        [clear], {'002270.SZ': fresh}, minute)
    assert {x['payload']['status'] for x in store.events.values()} == {
        'DATA_LIMITED_RETRY_FRESH_RULES', 'FRESH_RULES_EVALUATED'}
    assert all(x['action'] == 'NO_ACTION' and not x['effective'] for x in store.events.values())


def test_pre_fix_yesterday_exit_is_never_automatically_settled():
    import json
    from types import SimpleNamespace
    from liangjian_funnel.workflow import WorkflowApplication

    old = {'action': 'REDUCE_SIGNAL', 'event_key': 'old-1451-reduce',
        'minute_end': AT.isoformat(), 'payload_json': json.dumps({
            'plan_id': 'real-002270', 'symbol': '002270.SZ'})}
    store = SimpleNamespace(list_monitor_events=lambda **_: [old],
        get_execution_plan=lambda _: None)
    broker = SimpleNamespace(apply=lambda *_: pytest.fail('stale exit reached broker'))
    app = SimpleNamespace(store=store, brokers={'lane_1': broker})
    bar = SimpleNamespace(symbol='002270.SZ', interval='1m',
        bar_end=AT.replace(day=22, hour=9, minute=31))
    assert WorkflowApplication._settle_prior_signals(app, 'lane_1', '002270.SZ', bar) == []


def test_fresh_full_exit_is_not_deduplicated_against_yesterdays_exit():
    from liangjian_funnel.runtime.monitor import MonitorEngine

    store = EventStore([lot(700, '2026-10-20', '2026-10-21')], 700, 700)
    plan = {'plan_id': 'held-through-two-days', 'symbol': '002270.SZ'}
    for minute in (AT, AT + timedelta(days=1)):
        event = MonitorEngine(store)._emit_effective('lane_1', plan, minute,
            'fresh', 'SELL_SIGNAL', 'FRESH_STOP_TRIGGER')
        assert event.effective
    assert len(store.events) == 2
