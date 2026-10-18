from dataclasses import replace
from datetime import datetime
from decimal import Decimal as D
import pytest
from liangjian_paper_broker.ledger import LocalPaperBroker
from liangjian_paper_broker.matching import PaperRules, MinuteBar


def stamp(day='19',tm='10:00:00'):return datetime.fromisoformat(f'2026-10-{day}T{tm}+08:00')


def broker(path,day='19'):
    return LocalPaperBroker(path,initial_capital=D('1000000'),clock=lambda:stamp(day))


def place(b,client='c1',**kw):
    values=dict(symbol='000001.SZ',side='BUY',qty=1000,order_type='LIMIT',limit_price='10',
                client_order_id=client,plan_id='p1',signal_id='s1',no_chase_price='10.2',tradable=True)
    values.update(kw);return b.place_order(**values)


def bar(day='19',tm='10:01:00',**kw):
    return replace(MinuteBar('000001.SZ',stamp(day,tm),D('9.9'),D('10.1'),D('10'),100000,
                            closed=True,tradability_known=True,close=D('10')),**kw)


def drive(b,minute_bar,rules):
    # Synthetic replay arrival, explicitly bound rather than read from wall-clock.
    return b.on_bar(minute_bar,rules,observed_at=minute_bar.minute_end)


def test_orders_reserve_cash_and_fills_are_idempotent_across_restart(tmp_path):
    path=tmp_path/'paper.db';b=broker(path);oid=place(b)
    assert D(b.get_account()['frozen'])==D('10005')
    assert place(b)==oid
    assert len(drive(b,bar(),PaperRules('2026-10-19')))==1
    before=b.snapshot_bytes();b=broker(path)
    assert drive(b,bar(),PaperRules('2026-10-19'))==()
    assert b.snapshot_bytes()==before
    assert D(b.get_account()['cash'])==D('989995')
    assert b.get_positions()[0]['qty']==1000 and b.get_positions()[0]['sellable_qty']==0
    assert b.reconcile()['status']=='MATCH'


def test_same_client_id_different_order_and_conflicting_bar_are_rejected(tmp_path):
    b=broker(tmp_path/'paper.db');place(b)
    with pytest.raises(ValueError,match='IDEMPOTENCY_CONFLICT'):place(b,qty=2000)
    drive(b,bar(),PaperRules('2026-10-19'))
    with pytest.raises(ValueError,match='BAR_CONFLICT'):
        drive(b,bar(volume_shares=200000),PaperRules('2026-10-19'))


def test_total_participation_is_shared_across_orders_and_partial_cash_remains_frozen(tmp_path):
    b=broker(tmp_path/'paper.db');place(b,'c1');place(b,'c2',plan_id='p2',signal_id='s2')
    fills=drive(b,bar(volume_shares=2500),PaperRules('2026-10-19'))
    assert sum(f['qty'] for f in fills)==200
    assert D(b.get_account()['frozen'])>D('18000')
    assert b.reconcile()['status']=='MATCH'


def test_buy_reservations_cannot_overspend_account_even_before_fills(tmp_path):
    b=broker(tmp_path/'paper.db');place(b,qty=90000)
    with pytest.raises(ValueError,match='INSUFFICIENT_CASH'):place(b,'c2',qty=20000)
    assert len(b.get_orders('2026-10-19'))==1


def test_t1_and_selling_reservations_are_authoritative(tmp_path):
    path=tmp_path/'paper.db';b=broker(path);place(b);drive(b,bar(),PaperRules('2026-10-19'))
    with pytest.raises(ValueError,match='T1_NOT_SELLABLE'):
        place(b,'sell1',side='SELL')
    b=broker(path,'20')
    assert b.get_positions()[0]['sellable_qty']==1000
    sell=place(b,'sell2',side='SELL',qty=600)
    with pytest.raises(ValueError,match='T1_NOT_SELLABLE'):
        place(b,'sell3',side='SELL',qty=500)
    b.cancel_order(sell)
    place(b,'sell4',side='SELL',qty=1000)
    fills=drive(b,bar('20'),PaperRules('2026-10-20'))
    assert sum(f['qty'] for f in fills)==1000
    assert b.get_positions()==() and b.reconcile()['status']=='MATCH'
    assert D(b.get_account()['pnl_realized'])==D('-15.00')


def test_suspend_reject_has_no_cash_or_fill_side_effect(tmp_path):
    b=broker(tmp_path/'paper.db')
    with pytest.raises(ValueError,match='NOT_TRADABLE'):place(b,tradable=False)
    assert b.get_orders('2026-10-19')==() and D(b.get_account()['frozen'])==0


def test_replay_two_independent_databases_has_identical_curve_and_ledger(tmp_path):
    snapshots=[]
    for name in ('a','b'):
        b=broker(tmp_path/(name+'.db'));place(b)
        drive(b,bar(volume_shares=2000),PaperRules('2026-10-19'))
        drive(b,bar(tm='10:02:00',volume_shares=4000),PaperRules('2026-10-19'))
        snapshots.append(b.snapshot_bytes())
    assert snapshots[0]==snapshots[1]


def test_earlier_bar_never_fills_or_rewrites_accepted_later_bar(tmp_path):
    b=broker(tmp_path/'paper.db');place(b)
    drive(b,bar(tm='10:03:00'),PaperRules('2026-10-19'))
    before=b.snapshot_bytes()
    with pytest.raises(ValueError,match='BAR_OUT_OF_ORDER'):
        drive(b,bar(tm='10:02:00'),PaperRules('2026-10-19'))
    assert b.snapshot_bytes()==before


def test_unclosed_bar_does_not_poison_later_closed_bar_identity(tmp_path):
    b=broker(tmp_path/'paper.db');place(b);before=b.snapshot_bytes()
    with pytest.raises(ValueError,match='BAR_NOT_READY'):
        drive(b,bar(closed=False),PaperRules('2026-10-19'))
    assert b.snapshot_bytes()==before
    assert drive(b,bar(),PaperRules('2026-10-19'))


def test_fill_priority_follows_acceptance_not_hashed_order_id(tmp_path):
    b=broker(tmp_path/'paper.db');first=place(b,'first');place(b,'second',plan_id='p2',signal_id='s2')
    assert drive(b,bar(volume_shares=1000),PaperRules('2026-10-19'))[0]['order_id']==first


def test_settlement_cancels_open_orders_and_is_immutable(tmp_path):
    path=tmp_path/'paper.db';b=broker(path);place(b)
    drive(b,bar(volume_shares=2000),PaperRules('2026-10-19'))
    drive(b,bar(tm='15:00:00',low=D('10.5'),high=D('11'),vwap=D('11'),close=D('11')),PaperRules('2026-10-19'))
    b=LocalPaperBroker(path,initial_capital=D('1000000'),clock=lambda:stamp(tm='15:30:00'))
    report=b.settle('2026-10-19')
    assert report['status']=='COMPLETE' and report['reconcile']['status']=='MATCH'
    assert D(report['account']['frozen'])==0
    assert b.settle('2026-10-19')==report
    assert b.get_orders('2026-10-19')[0]['status']=='CANCELLED'


def test_other_database_is_rejected_before_schema_mutation(tmp_path):
    import sqlite3
    path=tmp_path/'foreign.db'
    with sqlite3.connect(path) as c:c.execute('CREATE TABLE foreign_data(id INTEGER)')
    before=path.read_bytes()
    with pytest.raises(ValueError,match='DATABASE_SCOPE_INVALID'):broker(path)
    assert path.read_bytes()==before


def test_reservation_fee_policy_cannot_silently_change_during_matching(tmp_path):
    b=broker(tmp_path/'paper.db');place(b);before=b.snapshot_bytes()
    with pytest.raises(ValueError,match='RULE_POLICY_CONFLICT'):
        drive(b,bar(),replace(PaperRules('2026-10-19'),commission_rate=D('0.1')))
    assert b.snapshot_bytes()==before


def test_future_bar_does_not_write_a_mark_or_fill(tmp_path):
    b=broker(tmp_path/'paper.db');place(b);before=b.snapshot_bytes()
    with pytest.raises(ValueError,match='FUTURE_BAR'):
        b.on_bar(bar(),PaperRules('2026-10-19'),observed_at=stamp())
    assert b.snapshot_bytes()==before


def test_stale_intraday_mark_is_not_a_complete_closing_nav(tmp_path):
    path=tmp_path/'paper.db';b=broker(path);place(b);drive(b,bar(),PaperRules('2026-10-19'))
    b=LocalPaperBroker(path,initial_capital=D('1000000'),clock=lambda:stamp(tm='15:30:00'))
    report=b.settle('2026-10-19')
    assert report['status']=='DATA_LIMITED' and report['account']['nav'] is None
