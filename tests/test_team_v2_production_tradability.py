from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

from liangjian_funnel.runtime.team_v2_tradability import production_monitor_tradability
from test_team_v2_paper_ledger import bar


def proof(**updates):
    minute=bar(tm='10:00:00');now=minute.minute_end+timedelta(minutes=1)
    args=dict(symbol=minute.symbol,now=now,bar=minute,
        quote=SimpleNamespace(symbol=minute.symbol,quote_time=now,price=10),
        plan=dict(symbol=minute.symbol),minute_bars=())
    args.update(updates)
    return production_monitor_tradability(None,**args)


def test_production_contract_does_not_need_strict_research_artifacts():
    result=proof()
    assert result['status']=='KNOWN' and result['entry_tradable']
    assert result['shadow_pit_required'] is False


def test_existing_explicit_upper_or_lock_prohibits_entry():
    minute=bar(tm='10:00:00')
    for extra in ({'upper_limit':10,'lower_limit':8},{'locked_limit_up':True}):
        result=proof(plan=dict(symbol=minute.symbol,**extra))
        assert result['status']=='KNOWN' and result['limit_up'] and not result['entry_tradable']


def test_no_fresh_quote_still_allows_closed_bar_for_held_protection():
    result=proof(quote=None)
    assert result['status']=='KNOWN' and not result['entry_tradable'] and not result['halted']


def test_one_price_or_no_volume_not_executable_and_stale_identity_fails():
    minute=bar(tm='10:00:00')
    assert not proof(bar=replace(minute,low=minute.high,close=minute.high,vwap=minute.high))['entry_tradable']
    result=proof(bar=replace(minute,volume_shares=0))
    assert result['halted'] and not result['entry_tradable']
    assert proof(symbol='600001.SH')['status']=='DATA_LIMITED'
    assert proof(now=minute.minute_end+timedelta(minutes=3))['status']=='DATA_LIMITED'


def test_conflicting_original_limit_aliases_not_silently_accepted():
    minute=bar(tm='10:00:00')
    assert proof(plan=dict(symbol=minute.symbol,upper_limit=11,limit_up=12))['reason_code']=='CONTROL_EXPLICIT_LIMIT_CONFLICT'


def test_later_quote_limit_does_not_relabel_closed_bar_for_existing_order_match():
    minute=bar(tm='10:00:00');now=minute.minute_end+timedelta(minutes=1)
    result=proof(plan=dict(symbol=minute.symbol,upper_limit=11,lower_limit=9),
        quote=SimpleNamespace(symbol=minute.symbol,quote_time=now,price=11))
    assert result['status']=='KNOWN' and not result['entry_tradable'] and not result['limit_up']
