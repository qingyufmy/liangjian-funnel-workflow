"""0253: cancel unfilled BUY only, original session/order idempotency."""
from dataclasses import replace
from decimal import Decimal as D
import sqlite3

import pytest

from test_team_v2_paper_ledger import stamp,bar
from liangjian_paper_broker.ledger import LocalPaperBroker
from liangjian_paper_broker.matching import PaperRules


def broker(tmp_path):
    return LocalPaperBroker(tmp_path/'paper.db',initial_capital=D(1000000),clock=stamp)


def order(b,*,symbol='000001.SZ',plan='p1',side='BUY',key='first',qty=1000):
    return b.place_order(symbol,side,qty,order_type='LIMIT',limit_price=D('10'),client_order_id=key,
        plan_id=plan,signal_id=key,no_chase_price=D('10.2'),tradable=True)


def test_scope_only_current_pending_buy_and_exact_retry_binding(tmp_path):
    b=broker(tmp_path);one=order(b);two=order(b,plan='p2',symbol='000002.SZ',key='other')
    result=b.cancel_buy_reservations('a6:test',plan_ids={'p1'})
    assert result==[dict(order_id=one,status='CANCELLED',cancelled_remaining_qty=1000)]
    assert b.cancel_buy_reservations('a6:test',plan_ids={'p1'})==result
    assert {r['order_id']:r['status'] for r in b.get_orders(stamp().date().isoformat())}[two]=='OPEN'
    with sqlite3.connect(b.path) as db:
        assert db.execute('SELECT count(*) FROM order_cancellations').fetchone()[0]==1
    with pytest.raises(ValueError,match='CANCELLATION_BINDING_CONFLICT'):
        b.cancel_buy_reservations('a6:test',plan_ids={'p1','p2'})
    assert b.reconcile()['status']=='MATCH'
    later=order(b,key='later')
    assert b.cancel_buy_reservations('a6:test',plan_ids={'p1'})==result
    assert {r['order_id']:r['status'] for r in b.get_orders(stamp().date().isoformat())}[later]=='OPEN'


def test_partial_fill_cancel_remaining_protective_sell_and_old_fill_unchanged(tmp_path):
    b=broker(tmp_path);one=order(b)
    # Production matcher produces a partial fill from real lot/participation
    # rules. A separate prior-day lot allows a protective sell reservation.
    b.on_bar(replace(bar(),volume_shares=1000),PaperRules(stamp().date().isoformat()),observed_at=stamp(tm='10:02:00'))
    fills=b.get_fills(stamp().date().isoformat());assert fills and fills[0]['qty']<1000
    with b._tx() as db:
        db.execute("UPDATE paper_lots SET open_date='2026-10-16'")
    sell=order(b,side='SELL',key='protect',qty=fills[0]['qty'])
    result=b.cancel_buy_reservations('a6:partial',plan_ids={'p1'})
    assert result[0]['cancelled_remaining_qty']==1000-fills[0]['qty']
    states={r['order_id']:r['status'] for r in b.get_orders(stamp().date().isoformat())}
    assert states[one]=='CANCELLED' and states[sell]=='OPEN'
    assert b.get_fills(stamp().date().isoformat())==fills


def test_mid_cancellation_fault_is_atomic(tmp_path):
    b=broker(tmp_path);one=order(b);order(b,key='second')
    # Install the owned schema first via an empty legitimate session.
    b.cancel_buy_reservations('empty',plan_ids=set())
    with sqlite3.connect(b.path) as db:
        db.execute("CREATE TRIGGER fail BEFORE INSERT ON order_cancellations BEGIN SELECT RAISE(ABORT,'FAULT'); END")
    with pytest.raises(sqlite3.IntegrityError):b.cancel_buy_reservations('a6:fault',plan_ids={'p1'})
    assert all(r['status']=='OPEN' for r in b.get_orders(stamp().date().isoformat()))
    with sqlite3.connect(b.path) as db:db.execute('DROP TRIGGER fail')
    assert len(b.cancel_buy_reservations('a6:fault',plan_ids={'p1'}))==2


def test_actual_post_trader_halt_cancels_buy_without_touching_position(tmp_path):
    from test_team_v2_exception_coordinator import hook_fixture,run
    args,seen,raw,clock,risk_calls,callback=hook_fixture(tmp_path,action='HALT_NEW_ENTRY')
    first=run(args,bar(tm='10:00:00'),exception_handler=callback)
    assert first['risk_exception']['review']['controls']
    assert first['risk_exception']['cancellations']
    assert args['broker'].get_orders(args['trade_date'])[0]['status']=='CANCELLED'
    again=run(args,bar(tm='10:00:00'),exception_handler=callback)
    assert again['risk_exception']==first['risk_exception'] and len(risk_calls)==1
    assert args['broker'].get_positions()==()
