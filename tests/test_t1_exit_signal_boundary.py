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
        assert result.reason_code == 'T1_LOCKED'
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


def test_yesterday_dated_lot_releases_even_before_position_cache_day_roll():
    from liangjian_funnel.runtime.execution_eligibility import sellable_qty
    store=EventStore([lot(700,'2026-10-20','2026-10-21')],700,0)
    assert sellable_qty(store,'paper:lane_1','002270.SZ',AT)==700
    # A wrongly optimistic cache cannot make today's acquired lot sellable.
    store=EventStore([lot(700,'2026-10-21','2026-10-22')],700,700)
    assert sellable_qty(store,'paper:lane_1','002270.SZ',AT)==0


@pytest.mark.parametrize('cache',['missing',None,'expired',False])
def test_missing_or_invalid_display_cache_never_overrides_dated_lots(cache):
    store=EventStore([lot(700,'2026-10-20','2026-10-21')],700,0)
    if cache=='missing':store.position.pop('sellable_qty')
    else:store.position['sellable_qty']=cache
    assert sellable_qty(store,'paper:lane_1','002270.SZ',AT)==700
    store.lots=[lot(700)]
    assert sellable_qty(store,'paper:lane_1','002270.SZ',AT)==0


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


def test_actual_1451_payload_through_exit_guard_and_customer_notification(tmp_path):
    import hashlib
    import json
    from pathlib import Path
    from types import SimpleNamespace
    from liangjian_funnel.runtime.monitor import MonitorEngine
    from liangjian_funnel.runtime.state import RuntimeStore
    from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
    from test_workflow_lark_notifications import FakeNotifier

    original=json.loads((Path(__file__).parent/'fixtures/t1_002270_20261021_1451.json').read_text(encoding='utf-8'))
    event=original['event'];raw=event['payload_json'];body=json.loads(raw)
    assert hashlib.sha256(raw.encode()).hexdigest()=='956bb09c531e29f5f884264e926a2033efb5a95f789fd61340684feb6b3197cf'
    assert event['effective']==1 and event['action']=='REDUCE_SIGNAL'  # frozen old red
    source=EventStore(original['lots'],original['position']['total_qty'],0)
    engine=MonitorEngine(source)
    last_bar=body['strategy']['execution_data']['receipts']['1m']['last_bar']
    for delta in range(5):
        result=engine._emit_effective(event['lane_id'],body,AT+timedelta(minutes=delta),
            body['minute_snapshot_id'],event['action'],event['reason_code'],
            strategy_result=body['strategy'],observed_bar=SimpleNamespace(
                close=last_bar['close'],low=last_bar['low'],bar_end=datetime.fromisoformat(last_bar['bar_end'])))
        assert not result.effective and result.action=='NO_ACTION' and result.reason_code=='T1_LOCKED'
    assert len(source.events)==1
    store=RuntimeStore(tmp_path/'isolated-customer.sqlite3')
    for row in source.events.values():store.record_monitor_event(**row)
    publisher=WorkflowLarkPublisher(store,'https://open.larksuite.com/open-apis/bot/v2/hook/test-token')
    notifier=FakeNotifier();publisher.notifier=notifier
    rows=store.list_monitor_events(lane_id='lane_1')
    publisher.publish_a4_events(rows,plans={},now=AT)
    publisher.publish_a4_events(rows,plans={},now=AT+timedelta(minutes=4))
    assert sum(bool(x['effective']) and x['action']=='REDUCE_SIGNAL' for x in rows)==0
    deliveries=store.list_notification_deliveries()
    assert len(deliveries)==1 and deliveries[0]['kind']=='A4_RISK_PENDING'
    assert len(notifier.calls)==1 and '减仓触发' not in str(notifier.calls)
    pending=json.loads(rows[0]['payload_json'])['risk_pending']
    assert pending['trigger_price']==18.71 and pending['trigger_bar_end']=='2026-10-21T14:50:00+08:00'


def test_team_uses_same_dated_lot_function_and_one_pending_then_fresh_next_open(tmp_path,monkeypatch):
    import json
    from decimal import Decimal as D
    from types import SimpleNamespace
    from liangjian_paper_broker.ledger import LocalPaperBroker
    from liangjian_paper_broker.matching import MinuteBar,PaperRules
    from liangjian_funnel.runtime.team_v2_control_minute import _hard_exit
    from test_team_v2_paper_ledger import stamp,bar

    clock=[stamp(day='21',tm='13:10:00')]
    broker=LocalPaperBroker(tmp_path/'isolated-team.db',initial_capital=D('1000000'),clock=lambda:clock[0])
    broker.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',
        client_order_id='owned',plan_id='p1',signal_id='owned',no_chase_price='10.2',tradable=True)
    clock[0]=stamp(day='21',tm='13:12:00')
    broker.on_bar(bar(day='21',tm='13:11:00'),PaperRules('2026-10-21'),observed_at=clock[0])
    plan=SimpleNamespace(plan_id='p1',symbol='000001.SZ',stop_price=D('9'),invalidation_price=None,no_chase=D('10.2'))
    for minute in ('14:50:00','14:51:00'):
        clock[0]=stamp(day='21',tm=minute)
        result=_hard_exit(broker,plan=plan,bar=bar(day='21',tm=minute),at=clock[0],account_id='team')
        assert not result['dispatched'] and result['judgment']['reason_codes']==['T1_LOCKED']
    with broker._tx(readonly=True) as c:
        pending=c.execute("SELECT payload_json FROM exec_ledger WHERE kind='T1_RISK_PENDING'").fetchall()
    assert len(pending)==1 and json.loads(pending[0][0])['not_an_executable_order']
    assert not [x for x in broker.get_orders('2026-10-21') if x['side']=='SELL']
    clock[0]=stamp(day='22',tm='09:30:00')
    limited=broker.record_t1_open_reassessment(at=clock[0],closed_bars={},decisions=[])
    assert limited[0]['status']=='DATA_LIMITED_RETRY_FRESH_RULES'
    clock[0]=stamp(day='22',tm='09:31:00');fresh=bar(day='22',tm='09:30:00',low=D('8.8'),high=D('9.1'),close=D('8.9'),vwap=D('8.95'))
    sale=_hard_exit(broker,plan=plan,bar=fresh,at=clock[0],account_id='team')
    assert sale['dispatched'] and sale['judgment']['shares']==1000
    for _ in range(2):
        records=broker.record_t1_open_reassessment(at=clock[0],closed_bars={fresh.symbol:fresh},decisions=[sale])
        assert records[0]['first_attempt_by_0935'] and not records[0]['old_action_replayed']
    with broker._tx(readonly=True) as c:
        assert c.execute("SELECT count(*) FROM exec_ledger WHERE kind='T1_OPEN_REASSESSMENT'").fetchone()[0]==2
    assert broker.reconcile()['status']=='MATCH'
