"""Same H1 invariant functions as legacy broker, independent new ledger."""
from datetime import datetime
from decimal import Decimal
import sqlite3
import pytest
from h1_common_broker_contract import assert_atomic_failure,assert_replay_idempotent,assert_terminal_protected
from liangjian_paper_broker.ledger import LocalPaperBroker
from liangjian_paper_broker.matching import MinuteBar,PaperRules


@pytest.mark.parametrize('scenario',['ATOMIC_FAULT','IDEMPOTENT_RETRY','TERMINAL_PROTECTION'])
def test_local_paper_shared_invariants(tmp_path,scenario):
    now=datetime.fromisoformat('2026-10-16T11:16:17+08:00')
    database=tmp_path/'independent-paper.sqlite3'
    broker=LocalPaperBroker(database,initial_capital=Decimal('1000000'),clock=lambda:now,run_id='H1_CONTRACT_LOCAL')
    oid=broker.place_order('600621.SH','BUY',100,order_type='LIMIT',limit_price='14.85',
        no_chase_price='15.67',client_order_id='h1-contract',plan_id='p1',signal_id='h1-entry',tradable=True)
    bar=MinuteBar(symbol='600621.SH',minute_end=now.replace(minute=18,second=0),
        low='14.83',high='14.86',vwap='14.84',volume_shares=186100,close='14.85',closed=True,tradability_known=True)
    def attempt():return broker.on_bar(bar,PaperRules('2026-10-16'),observed_at=now.replace(minute=19,second=0))
    def snapshot():return {'cash':broker.get_account()['cash'],'fills':broker.get_fills('2026-10-16'),
        'positions':broker.get_positions(),'terminal':broker.get_orders('2026-10-16')}
    if scenario=='ATOMIC_FAULT':
        with sqlite3.connect(database) as c:
            c.execute("CREATE TRIGGER shared_fault AFTER INSERT ON paper_fills BEGIN SELECT RAISE(ABORT,'H1_SHARED_FAULT'); END")
        assert_atomic_failure(attempt,snapshot,allowed_errors={'H1_SHARED_FAULT'})
    elif scenario=='IDEMPOTENT_RETRY':assert_replay_idempotent(attempt,snapshot)
    else:
        broker.cancel_order(oid)
        assert_terminal_protected(attempt,snapshot)
