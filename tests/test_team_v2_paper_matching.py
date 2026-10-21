from dataclasses import replace
from datetime import datetime
from decimal import Decimal as D
import pytest
from liangjian_paper_broker.matching import PaperRules, PaperOrder, MinuteBar, match_order


def stamp(value): return datetime.fromisoformat('2026-10-19T'+value+'+08:00')
RULES=PaperRules(trade_date='2026-10-19')


def order(**kw):
    return replace(PaperOrder('o1','p1','s1','000001.SZ','BUY','LIMIT',1000,1000,
                            stamp('10:00:00'),limit_price=D('10'),no_chase_price=D('10.2')),**kw)


def bar(**kw):
    return replace(MinuteBar('000001.SZ',stamp('10:01:00'),D('9.8'),D('10.2'),D('10'),100000,
                             closed=True,tradability_known=True),**kw)


def match(o=None,b=None,**kw):
    return match_order(o or order(),b or bar(),RULES,spendable_cash=D('100000'),**kw)


def test_next_closed_minute_uses_vwap_not_signal_close():
    result=match(b=bar(vwap=D('9.9')))
    assert result.status=='FILLED' and result.qty==1000 and result.price==D('9.9')
    assert result.fee==D('5.00')


@pytest.mark.parametrize('change,code',[
    ({'minute_end':stamp('10:00:00')},'NEXT_MINUTE_NOT_YET_AVAILABLE'),
    ({'closed':False},'BAR_NOT_CLOSED'),({'tradability_known':False},'TRADABILITY_UNPROVEN'),
    ({'halted':True},'SUSPENDED'),({'limit_up':True},'LIMIT_UP_LOCKED'),
    ({'low':D('10.1')},'LIMIT_NOT_TOUCHED'),
])
def test_unexecutable_buy_cannot_get_a_fake_fill(change,code):
    kwargs=dict(change)
    if kwargs.get('low')==D('10.1'):kwargs['vwap']=D('10.1')
    result=match(b=bar(**kwargs))
    assert result.qty==0 and result.reason_code==code


def test_buy_never_exceeds_limit_or_no_chase():
    assert match(b=bar(vwap=D('10.1'))).price==D('10')
    result=match(o=order(order_type='MARKET',limit_price=None,no_chase_price=D('9.9')))
    assert result.qty==0 and result.reason_code=='PRICE_ABOVE_NO_CHASE'


def test_market_buy_requires_explicit_plan_price_cap():
    assert match(o=order(order_type='MARKET',limit_price=None,no_chase_price=None)).reason_code=='BUY_PRICE_CAP_REQUIRED'


def test_volume_cap_is_shared_input_not_a_fresh_allowance_for_each_order():
    result=match(b=bar(volume_shares=2500))
    assert result.status=='PARTIAL' and result.qty==200
    remaining=match(b=bar(volume_shares=2500),available_volume_shares=50)
    assert remaining.qty==0 and remaining.reason_code=='PARTICIPATION_LOT_UNAVAILABLE'


def test_t1_zero_sellable_and_limit_down_do_not_execute():
    sell=order(side='SELL',sellable_qty=0)
    assert match(o=sell).reason_code=='T1_LOCKED'
    sell=replace(sell,sellable_qty=1000)
    assert match(o=sell,b=bar(limit_down=True)).reason_code=='LIMIT_DOWN_LOCKED'


def test_sell_limit_uses_max_limit_vwap_and_sell_fees():
    result=match(o=order(side='SELL',sellable_qty=1000,limit_price=D('10.1')),
                 b=bar(vwap=D('10')))
    assert result.price==D('10.1') and result.qty==1000
    assert result.fee==D('10.05') # min commission5 + sell stamp5.05, SZ no transfer


def test_odd_lot_sell_is_permitted_but_odd_lot_buy_is_not():
    assert match(o=order(side='SELL',qty=37,remaining_qty=37,sellable_qty=37)).qty==37
    assert match(o=order(qty=37,remaining_qty=37)).reason_code=='BUY_ROUND_LOT_REQUIRED'


def test_shanghai_transfer_fee_and_cumulative_min_commission():
    sh=order(symbol='600000.SH')
    first=match(o=sh,b=bar(symbol='600000.SH',volume_shares=2000))
    assert first.qty==200 and first.fee==D('5.02')
    partial=replace(sh,remaining_qty=800,filled_notional=D('2000'),fees_paid=first.fee)
    second=match(o=partial,b=bar(symbol='600000.SH',minute_end=stamp('10:02:00'),volume_shares=2000))
    assert second.qty==200 and second.fee==D('0.02')


def test_fee_is_not_silently_ignored_when_cash_is_barely_sufficient():
    r=match_order(order(),bar(),RULES,spendable_cash=D('10000'))
    assert r.qty==900 and r.qty*r.price+r.fee<=D('10000')


def test_no_fill_outside_sessions_or_on_wrong_rules_date():
    assert match(b=bar(minute_end=stamp('12:00:00'))).reason_code=='OUTSIDE_TRADING_SESSION'
    later=replace(RULES,trade_date='2026-10-20')
    assert match_order(order(),bar(),later,spendable_cash=D('100000')).reason_code=='RULE_SNAPSHOT_DATE_MISMATCH'


def test_matching_same_immutable_inputs_is_byte_reproducible():
    assert match().canonical_bytes()==match().canonical_bytes()


@pytest.mark.parametrize('bad',[-1,True,D('NaN')])
def test_invalid_volume_is_rejected_at_the_input_boundary(bad):
    with pytest.raises(ValueError):bar(volume_shares=bad)
