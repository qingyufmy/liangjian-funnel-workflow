from dataclasses import replace
from decimal import Decimal as D
from datetime import timedelta
from contextlib import closing
import sqlite3

import pytest

from test_team_v2_paper_matching import match,order,bar
from test_team_v2_paper_ledger import broker,place,drive,stamp,bar as ledger_bar
from test_team_v2_minute_projection import source,project
from liangjian_paper_broker.ledger import LocalPaperBroker
from liangjian_paper_broker.matching import PaperRules

EST='OHLC_ESTIMATED_PAPER_FILL'


@pytest.mark.parametrize('kind',['ohlc_estimate','legacy_unspecified'])
def test_estimated_projection_keeps_vwap_absent_and_rule_permission_unproven(kind):
    result=project((source(amount_kind=kind,amount=0),))
    assert result['paper_bar'].fill_basis==EST and result['paper_bar'].vwap is None
    assert not result['paper_bar'].tradability_known


def test_estimated_market_fills_are_side_specific_and_not_vwap():
    b=bar(vwap=None,fill_basis=EST,close=D('10.1'))
    buy=match(o=order(order_type='MARKET',limit_price=None),b=b)
    sell=match(o=order(side='SELL',sellable_qty=1000,order_type='MARKET',limit_price=None),b=b)
    assert buy.price==D('10.15') and sell.price==D('9.95')
    assert buy.fill_basis==sell.fill_basis==EST


def test_estimate_never_grants_better_limit_price_or_volume_or_rule_permission():
    b=bar(vwap=None,fill_basis=EST,close=D('10.1'))
    assert match(b=b).reason_code=='ESTIMATED_PRICE_OUTSIDE_LIMIT'
    assert match(b=replace(b,tradability_known=False)).qty==0
    assert match(b=replace(b,volume_shares=0)).qty==0
    assert match(o=order(order_type='MARKET',limit_price=None),b=replace(b,limit_up=True)).qty==0
    assert match(o=order(order_type='MARKET',limit_price=None),b=replace(b,volume_shares=2500)).qty==200
    assert project((source(amount_kind='ohlc_estimate',volume=0),))['paper_bar'] is None


def test_estimated_bar_cannot_supply_a_fake_vwap_or_omit_close():
    with pytest.raises(ValueError):bar(fill_basis=EST)
    with pytest.raises(ValueError):bar(fill_basis=EST,vwap=None,close=None)
    with pytest.raises(ValueError):bar(fill_basis='UNKNOWN')


def test_one_account_persists_both_bases_and_separates_settlement_cohorts(tmp_path):
    path=tmp_path/'paper.db';b=broker(path)
    place(b,'strict',plan_id='strict');drive(b,ledger_bar(),PaperRules('2026-10-19'))
    place(b,'estimated',plan_id='estimated',limit_price='10.2')
    fills=drive(b,ledger_bar(tm='10:02:00',vwap=None,fill_basis=EST),PaperRules('2026-10-19'))
    assert fills[0]['fill_basis']==EST and D(fills[0]['price'])==D('10.05')
    assert {f['fill_basis'] for f in b.get_fills('2026-10-19')}=={EST,'REPORTED_VWAP'}
    b=broker(path,'20')
    place(b,'sell-strict',plan_id='strict',side='SELL',order_type='MARKET',limit_price=None)
    place(b,'sell-estimated',plan_id='estimated',side='SELL',order_type='MARKET',limit_price=None)
    drive(b,ledger_bar('20',vwap=None,fill_basis=EST),PaperRules('2026-10-20'))
    b=LocalPaperBroker(path,initial_capital=D('1000000'),clock=lambda:stamp('20','15:30:00'))
    report=b.settle('2026-10-20');groups=report['fill_basis_report']
    assert report['reconcile']['status']=='MATCH'
    assert groups['daily_by_fill_basis'][EST]['fill_count']==2
    assert groups['performance_cohorts']['REPORTED_VWAP']['closed_qty']==0
    assert groups['performance_cohorts']['MIXED_BASIS']['closed_qty']==1000
    assert groups['performance_cohorts'][EST]['closed_qty']==1000
    assert sum(D(v['realized_pnl']) for v in groups['performance_cohorts'].values())==D(report['account']['pnl_realized'])
    assert b.settle('2026-10-20')==report


def test_two_basis_replay_snapshot_and_settlement_are_reproducible(tmp_path):
    outputs=[]
    for name in ('a','b'):
        b=broker(tmp_path/(name+'.db'));place(b,'a',plan_id='a')
        drive(b,ledger_bar(),PaperRules('2026-10-19'))
        place(b,'b',plan_id='b',limit_price='10.2')
        drive(b,ledger_bar(tm='10:02:00',vwap=None,fill_basis=EST),PaperRules('2026-10-19'))
        drive(b,ledger_bar(tm='15:00:00'),PaperRules('2026-10-19'))
        b=LocalPaperBroker(b.path,initial_capital=D('1000000'),clock=lambda:stamp('19','15:30:00'))
        b.settle('2026-10-19');outputs.append(b.snapshot_bytes())
    assert outputs[0]==outputs[1]


def test_upgrade_does_not_relabel_existing_unknown_fills_as_reported(tmp_path):
    path=tmp_path/'paper.db';b=broker(path);place(b)
    drive(b,ledger_bar(),PaperRules('2026-10-19'))
    # Isolated old-schema fixture. Never touches an existing production ledger.
    with closing(sqlite3.connect(path)) as db,db:
        db.execute('ALTER TABLE paper_fills DROP COLUMN fill_basis')
    b=broker(path)
    assert b.get_fills('2026-10-19')[0]['fill_basis']=='LEGACY_UNCLASSIFIED'
    drive(b,ledger_bar(tm='15:00:00'),PaperRules('2026-10-19'))
    b=LocalPaperBroker(path,initial_capital=D('1000000'),clock=lambda:stamp('19','15:30:00'))
    groups=b.settle('2026-10-19')['fill_basis_report']
    assert groups['daily_by_fill_basis']['REPORTED_VWAP']['fill_count']==0
    assert groups['open_by_entry_basis']['LEGACY_UNCLASSIFIED']['qty']==1000


def test_partial_mixed_basis_exit_attributes_costs_and_fees_without_pnl_loss(tmp_path):
    path=tmp_path/'paper.db';b=broker(path)
    place(b,'reported',plan_id='p1');drive(b,ledger_bar(),PaperRules('2026-10-19'))
    place(b,'estimated',plan_id='p1',limit_price='10.2')
    drive(b,ledger_bar(tm='10:02:00',vwap=None,fill_basis=EST),PaperRules('2026-10-19'))
    b=broker(path,'20');place(b,'partial-sell',plan_id='p1',side='SELL',qty=1500,order_type='MARKET',limit_price=None)
    drive(b,ledger_bar('20'),PaperRules('2026-10-20'))
    drive(b,ledger_bar('20',tm='15:00:00'),PaperRules('2026-10-20'))
    b=LocalPaperBroker(path,initial_capital=D('1000000'),clock=lambda:stamp('20','15:30:00'))
    report=b.settle('2026-10-20');groups=report['fill_basis_report']
    assert sum(v['closed_qty'] for v in groups['performance_cohorts'].values())==1500
    assert sum(D(v['realized_pnl']) for v in groups['performance_cohorts'].values())==D(report['account']['pnl_realized'])
    assert sum(D(v['unrealized_pnl']) for v in groups['open_by_entry_basis'].values())==D(report['account']['pnl_unrealized'])


def test_actual_control_handler_estimated_source_to_later_closed_minute_fill(tmp_path,monkeypatch):
    from test_team_v2_monitor_dispatch import prepared,advance
    from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
    from liangjian_funnel.data.mootdx import FetchResult
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    def estimated(symbol,interval,count,cutoff,**kwargs):
        rows=tuple(source(bar_end=source().bar_end+timedelta(minutes=i),amount_kind='ohlc_estimate',
            **(dict(open=9.9,low=9.8,high=10,close=9.9) if i>=5 else {})) for i in range(count))
        return FetchResult(symbol=symbol,interval='1m',requested_bars=count,returned_bars=count,
            bars=rows,complete=True,reason_code='OK')
    app._fetch_live_bars=estimated
    first=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert first['result']['trader']['admissions'][0]['dispatched']
    advance(clock,9,38)
    later=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert later['result']['fills'][0]['fill_basis']==EST
    assert D(later['result']['fills'][0]['price'])==D('9.95') and len(seen)==1
