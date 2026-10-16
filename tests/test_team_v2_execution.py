from dataclasses import replace
from datetime import timedelta
from decimal import Decimal as D
from types import SimpleNamespace
import pytest

from test_team_v2_paper_ledger import broker, bar, drive, stamp
from test_team_v2_risk_engine import plan, envelope
from liangjian_paper_broker.matching import PaperRules
from liangjian_funnel.runtime.risk_engine import RiskLimits
from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal


def fixture(tmp_path):
    b=broker(tmp_path/'paper.db')
    # Isolated fixture premarket basis, not a copied production account.
    b.clock=lambda:stamp(tm='08:33:00')
    b.open_risk_day('2026-10-19')
    b.clock=lambda:stamp()
    calendar=SimpleNamespace(is_trading_day=lambda day:day.isoformat()=='2026-10-19')
    args=dict(broker=b,signal_id='A4:d1:p1',action='BUY_SIGNAL',plan=plan(),envelope=envelope(),
        limits=RiskLimits(),all_plans={'p1':plan()},now=stamp(),closed_5m_end=stamp(),
        mark_as_of=stamp(),regime='TREND_MAINLINE',live_market_ready=True,snapshot_ready=True,
        mode='control',calendar=calendar,input_sha256='a'*64)
    return b,args


def test_real_guard_risk_admit_and_next_minute_fill(tmp_path):
    b,args=fixture(tmp_path)
    result=admit_trader_signal(**args)
    assert result['dispatched'] and result['judgment']['shares']==20000
    fills=drive(b,bar(volume_shares=300000),PaperRules('2026-10-19'))
    assert fills[0]['qty']==20000 and b.get_positions()[0]['sellable_qty']==0
    assert b.reconcile()['status']=='MATCH'
    assert admit_trader_signal(**args)==result


def test_pending_buy_consumes_daily_cluster_and_position_limits_before_any_fill(tmp_path):
    b,args=fixture(tmp_path)
    assert admit_trader_signal(**args)['dispatched']
    p2=replace(plan(),plan_id='p2',symbol='000002.SZ')
    e2=replace(envelope(),plan_id='p2',symbol='000002.SZ')
    p3=replace(plan(),plan_id='p3',symbol='000003.SZ')
    e3=replace(envelope(),plan_id='p3',symbol='000003.SZ')
    plans={'p1':plan(),'p2':p2,'p3':p3}
    second=admit_trader_signal(**(args|dict(signal_id='s2',plan=p2,envelope=e2,all_plans=plans)))
    assert second['dispatched']
    third=admit_trader_signal(**(args|dict(signal_id='s3',plan=p3,envelope=e3,all_plans=plans)))
    assert not third['dispatched'] and 'CLUSTER_SIGNAL_CAP' in third['judgment']['reason_codes']
    assert b.get_fills('2026-10-19')==()


def test_existing_pending_buy_cannot_double_allocate_same_stock(tmp_path):
    b,args=fixture(tmp_path)
    admit_trader_signal(**args)
    other=admit_trader_signal(**(args|dict(signal_id='second-tick')))
    assert not other['dispatched'] and 'TRADER_BATCH_CAP' in other['judgment']['reason_codes']


def test_missing_basis_is_not_silently_initial_capital(tmp_path):
    b,args=fixture(tmp_path)
    empty=broker(tmp_path/'without-basis.db')
    result=admit_trader_signal(**(args|dict(broker=empty)))
    assert not result['dispatched'] and 'PAPER_RISK_BASIS_MISSING' in result['judgment']['reason_codes']


def test_current_held_mark_is_required_and_daily_loss_halt_persists(tmp_path):
    b,args=fixture(tmp_path)
    admit_trader_signal(**args)
    drive(b,bar(volume_shares=300000),PaperRules('2026-10-19'))
    p2=replace(plan(),plan_id='p2',symbol='000002.SZ')
    changed=args|dict(signal_id='s2',plan=p2,envelope=replace(envelope(),plan_id='p2',symbol='000002.SZ'),
        all_plans={'p1':plan(),'p2':p2},now=stamp(tm='10:05:00'),closed_5m_end=stamp(tm='10:05:00'),mark_as_of=stamp(tm='10:05:00'))
    b.clock=lambda:stamp(tm='10:05:00')
    stale=admit_trader_signal(**changed)
    assert not stale['dispatched'] and 'PAPER_POSITION_MARK_STALE' in stale['judgment']['reason_codes']
    drive(b,bar(tm='10:05:00',low=D('8.7'),high=D('8.9'),vwap=D('8.8'),close=D('8.8')),PaperRules('2026-10-19'))
    halted=admit_trader_signal(**(changed|dict(signal_id='s3')))
    assert not halted['dispatched'] and 'DAILY_LOSS_HALT' in halted['judgment']['reason_codes']
    drive(b,bar(tm='10:10:00'),PaperRules('2026-10-19'))
    restarted=broker(b.path);restarted.clock=lambda:stamp(tm='10:10:00')
    rebound=admit_trader_signal(**(changed|dict(broker=restarted,signal_id='s4',now=stamp(tm='10:10:00'),
        closed_5m_end=stamp(tm='10:10:00'),mark_as_of=stamp(tm='10:10:00'))))
    assert not rebound['dispatched'] and 'DAILY_LOSS_HALT' in rebound['judgment']['reason_codes']


def test_record_mode_does_not_enter_admission_trading_counters(tmp_path):
    b,args=fixture(tmp_path)
    row=admit_trader_signal(**(args|dict(mode='record')))
    assert not row['dispatched'] and b.get_orders('2026-10-19')==()


def test_basis_cannot_be_opened_late_or_reset_after_orders(tmp_path):
    b=broker(tmp_path/'paper.db')
    with pytest.raises(ValueError,match='RISK_DAY_OPEN_TIME'):
        b.open_risk_day('2026-10-19')
    b,args=fixture(tmp_path/'initialized')
    original=b.open_risk_day('2026-10-19')
    admit_trader_signal(**args)
    drive(b,bar(volume_shares=300000),PaperRules('2026-10-19'))
    assert broker(b.path).open_risk_day('2026-10-19')==original


def test_t1_applies_to_model_exit_but_next_day_expired_plan_can_sell(tmp_path):
    b,args=fixture(tmp_path)
    admit_trader_signal(**args)
    drive(b,bar(volume_shares=300000),PaperRules('2026-10-19'))
    same_day=admit_trader_signal(**(args|dict(signal_id='exit1',action='SELL_SIGNAL')))
    assert not same_day['dispatched'] and 'T1_NOT_SELLABLE' in same_day['judgment']['reason_codes']
    tomorrow=stamp('20')
    restarted=broker(b.path,'20')
    result=admit_trader_signal(**(args|dict(broker=restarted,signal_id='exit2',action='SELL_SIGNAL',
        now=tomorrow,closed_5m_end=tomorrow,mark_as_of=tomorrow,
        envelope=replace(envelope(),trade_date='2026-10-20'),
        calendar=SimpleNamespace(is_trading_day=lambda day:day.isoformat()=='2026-10-20'))))
    assert result['dispatched'] and result['judgment']['shares']==20000


def test_drawdown_halt_survives_rebound_and_new_risk_day(tmp_path):
    b,args=fixture(tmp_path)
    admit_trader_signal(**args)
    drive(b,bar(volume_shares=300000),PaperRules('2026-10-19'))
    b.clock=lambda:stamp(tm='10:05:00')
    drive(b,bar(tm='10:05:00',low=D('1'),high=D('1.1'),vwap=D('1'),close=D('1')),PaperRules('2026-10-19'))
    p2=replace(plan(),plan_id='p2',symbol='000002.SZ')
    later=args|dict(signal_id='d1',plan=p2,envelope=replace(envelope(),plan_id='p2',symbol='000002.SZ'),
        all_plans={'p1':plan(),'p2':p2},now=stamp(tm='10:05:00'),closed_5m_end=stamp(tm='10:05:00'),mark_as_of=stamp(tm='10:05:00'))
    result=admit_trader_signal(**later)
    assert not result['dispatched'] and 'DRAWDOWN_HALT' in result['judgment']['reason_codes']
    drive(b,bar(tm='10:10:00'),PaperRules('2026-10-19'))
    b.clock=lambda:stamp(tm='10:10:00')
    result=admit_trader_signal(**(later|dict(signal_id='d2',now=stamp(tm='10:10:00'),
        closed_5m_end=stamp(tm='10:10:00'),mark_as_of=stamp(tm='10:10:00'))))
    assert not result['dispatched'] and 'DRAWDOWN_HALT' in result['judgment']['reason_codes']
    drive(b,bar(tm='15:00:00'),PaperRules('2026-10-19'))
    b.clock=lambda:stamp(tm='15:30:00')
    assert b.settle('2026-10-19')['status']=='COMPLETE'
    restarted=broker(b.path,'20');restarted.clock=lambda:stamp('20','08:33:00')
    restarted.open_risk_day('2026-10-20')
    restarted.clock=lambda:stamp('20')
    drive(restarted,bar('20',tm='10:00:00'),PaperRules('2026-10-20'))
    result=admit_trader_signal(**(later|dict(broker=restarted,signal_id='d3',now=stamp('20'),
        closed_5m_end=stamp('20'),mark_as_of=stamp('20'))))
    assert not result['dispatched'] and 'DRAWDOWN_HALT' in result['judgment']['reason_codes']
