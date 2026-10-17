from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path
import time

from liangjian_funnel.runtime.broker.local_paper import create_broker
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.risk_engine import PlanRisk
from liangjian_funnel.runtime.team_v2_record_protection import protect_owned_minute,has_owned_exposure
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from test_team_v2_paper_ledger import bar,stamp

ROOT=Path(__file__).resolve().parents[1]


def test_0283_held_record_cancels_buy_before_match_keeps_sells_and_hard_stop_without_model(tmp_path):
    clock=[stamp(day='16',tm='08:33:00')]
    broker=create_broker('local_paper',database=tmp_path/'paper.db',paper_config=ROOT/'config/paper_broker.yaml',
        risk_config=ROOT/'config/risk_limits.yaml',clock=lambda:clock[0],calendar=ExchangeTradingCalendar(),run_id='record-test')
    def order(identity,side,qty,plan_id='held'):
        return broker.place_order(client_order_id=identity,plan_id=plan_id,signal_id=identity,
            symbol='000001.SZ',side=side,qty=qty,order_type='MARKET',limit_price=None,
            no_chase_price=D('12'),tradable=True)
    broker.open_risk_day('2026-10-16');clock[0]=stamp(day='16',tm='10:00:00');order('original-buy','BUY',1000)
    clock[0]=stamp(day='16',tm='10:02:00');broker.on_bar(bar(day='16'))
    clock[0]=stamp(day='16',tm='15:01:00');broker.on_bar(bar(day='16',tm='15:00:00'))
    clock[0]=stamp(day='16',tm='15:30:00');assert broker.settle('2026-10-16')['status']=='COMPLETE'
    clock[0]=stamp(tm='08:33:00');broker.open_risk_day('2026-10-19');clock[0]=stamp(tm='10:01:00')
    order('pending-buy','BUY',100,plan_id='pending')
    order('protective-sell','SELL',100)
    store=TeamMeetingStore(tmp_path/'team.db')
    plan=PlanRisk('held','000001.SZ','FINANCE','EXPIRED',stamp(day='16',tm='09:26:00'),
        stamp(day='16',tm='15:00:00'),D('10'),D('9.95'),D('10'),D('11'),D('12'),
        invalidation_price=D('9.95'))
    args=dict(broker=broker,store=store,clock=lambda:clock[0],account_id='local-paper',plans={'held':plan},
        bars={'000001.SZ':bar(tm='10:00:00')},deadline_monotonic=time.monotonic()+47,reason_code='TEST_RECORD')
    first=protect_owned_minute(**args)
    assert first['status']=='RECORD_FALLBACK_ACTIVE' and first['model_calls']==first['new_buy_orders']==0
    assert len(first['cancelled_buys'])==1 and len(first['protection']['hard_exits'])==1
    orders=broker.get_orders('2026-10-19')
    assert next(o for o in orders if o['client_order_id']=='pending-buy')['status']=='CANCELLED'
    assert next(o for o in orders if o['client_order_id']=='protective-sell')['status']=='OPEN'
    assert broker.get_fills('2026-10-19')==()  # do not use a pre-order bar to fill either sell
    assert protect_owned_minute(**args)['started_at']==first['started_at']
    clock[0]=stamp(tm='10:03:00')
    second=protect_owned_minute(**(args|{'bars':{'000001.SZ':bar(tm='10:02:00')}}))
    assert broker.get_positions()==() and broker.reconcile()['status']=='MATCH'
    assert all(f['side']=='SELL' for f in broker.get_fills('2026-10-19'))
    assert second['started_at']==first['started_at'] and not second['cancelled_buys']


def test_record_exposure_probe_does_not_construct_missing_database(tmp_path):
    from types import SimpleNamespace
    assert not has_owned_exposure(SimpleNamespace(root=tmp_path))
    assert list(tmp_path.iterdir())==[]
