from copy import deepcopy
from datetime import datetime,timedelta

import pytest

from liangjian_funnel.runtime.entry_confirmation import DEFAULT_CONFIG,evaluate_pattern,load_entry_confirmation

CFG={**DEFAULT_CONFIG,'mode':'pattern_v1'}
LEGACY={**DEFAULT_CONFIG,'mode':'legacy'}
DAY=datetime.fromisoformat('2026-10-20T09:30:00+08:00')


def bar(end,close=100,volume=100,**changes):
    row=dict(bar_end=end.isoformat(),interval='5m',open=close-.05,close=close,
        high=close+.1,low=close-.1,volume=volume,closed=True)
    row.update(changes);return row


def history():
    start=DAY.replace(day=19,hour=13,minute=20)
    return [bar(start+timedelta(minutes=5*(i+1))) for i in range(20)]


def daily():
    return dict(daily_closes=[dict(close=100,as_of=(DAY-timedelta(days=20-i)).replace(hour=15).isoformat()) for i in range(20)],
        previous_day_high=dict(value=100,as_of=DAY.replace(day=19,hour=15).isoformat()),
        daily_ma5=dict(value=100.1,as_of=DAY.replace(day=19,hour=15).isoformat()))


def plan(profile='TREND_MA5',low=100.2,high=100.8,stop=99,target=105):
    return dict(strategy_profile=profile,reference_price=100,trigger_zone=dict(low=low,high=high),
        stop_level=stop,first_resistance=target,minimum_reward_risk=2.5,no_chase=102)


def run(p,rows,*,env=None,facts=None,vwap=100,**kwargs):
    return evaluate_pattern(p,rows,env or [],as_of=datetime.fromisoformat(rows[-1]['bar_end']),
        config=CFG,daily_facts=daily() if facts is None else facts,vwap=vwap,**kwargs)


def a_rows():
    rows=history()
    for i,(close,volume) in enumerate(((100.5,150),(101,150),(102,150),(101,50),(100.4,50),(100.5,40))):
        rows.append(bar(DAY+timedelta(minutes=5*(i+1)),close,volume))
    rows[-1].update(open=100.5,low=100.3)
    return rows


def environment(rows,close=100.5):
    end=datetime.fromisoformat(rows[-1]['bar_end']);end=end.replace(minute=end.minute-end.minute%15)
    return [dict(bar(end,close),interval='15m')]


def intraday_minutes():
    """Synthetic same-day, complete exchange buckets including lunch gap."""
    rows=a_rows()
    endpoints=[DAY+timedelta(minutes=5*(i+1)) for i in range(24)]
    endpoints.extend([DAY.replace(hour=13,minute=5),DAY.replace(hour=13,minute=10)])
    result=[]
    for source,end in zip(rows,endpoints):
        for offset in range(4,-1,-1):
            result.append(dict(symbol='600001.SH',interval='1m',bar_end=end-timedelta(minutes=offset),
                open=source['open'],high=source['high'],low=source['low'],close=source['close'],
                volume=source['volume']/5,amount=source['volume']/5*source['close'],closed=True))
    return result


def integrated_plan():
    return {**plan(), 'symbol':'600001.SH','stock_behavior_type':'TREND','trade_date':'2026-10-20'}


def server_context(at,**extra):
    return dict(live_market_state=dict(status='READY',decision='ALLOW',as_of=at.isoformat(),trade_date='2026-10-20'),**extra)


def test_shared_entry_confirmation_legacy_has_exact_result_equality_and_ignores_plan_mode():
    from liangjian_funnel.runtime.strategies import evaluate_strategy
    rows=intraday_minutes();at=rows[-1]['bar_end'];p=integrated_plan();ctx=server_context(at)
    original=evaluate_strategy(p,rows,now=at,market_context=ctx).model_dump(mode='json')
    explicit=evaluate_strategy(p,rows,now=at,market_context={**ctx,'entry_confirmation_config':LEGACY}).model_dump(mode='json')
    spoof={**p,'entry_confirmation_config':CFG,'market_context':{'entry_confirmation_config':CFG}}
    ignored=evaluate_strategy(spoof,rows,now=at,market_context=ctx).model_dump(mode='json')
    assert original==explicit==ignored
    assert 'entry_confirmation' not in original


def test_shared_a_pattern_accepts_doji_without_old_pressure_easing_gate():
    from liangjian_funnel.runtime.strategies import evaluate_strategy
    rows=intraday_minutes();at=rows[-1]['bar_end'];p=integrated_plan()
    result=evaluate_strategy(p,rows,now=at,market_context=server_context(at,entry_confirmation_config=CFG)).model_dump()
    assert result['action']=='BUY_SIGNAL',result
    assert result['entry_confirmation']['pattern']=='A_TREND_PULLBACK'
    assert 'TREND_15M_PRESSURE_EASING' not in result['met_conditions']+result['unmet_conditions']
    assert result['live_entry_price']==rows[-1]['close']
    assert 'A_LOWER_WICK' in result['entry_confirmation']['conditions']


def test_shared_pattern_uses_prior_closed_cache_for_recent20_not_today20():
    from liangjian_funnel.runtime.strategies import evaluate_strategy
    five=a_rows()[20:];minutes=[]
    for row in five:
        end=datetime.fromisoformat(row['bar_end'])
        for offset in range(4,-1,-1):
            minutes.append(dict(symbol='600001.SH',interval='1m',bar_end=end-timedelta(minutes=offset),
                open=row['open'],high=row['high'],low=row['low'],close=row['close'],volume=row['volume']/5,
                amount=row['volume']/5*row['close'],closed=True))
    at=minutes[-1]['bar_end'];p=integrated_plan()
    ctx=server_context(at,entry_confirmation_config=CFG)
    cold=evaluate_strategy(p,minutes,now=at,market_context=ctx).model_dump()
    assert cold['entry_confirmation']['status']=='CONFIRM_WARMUP'
    seed=[{**row,'symbol':'600001.SH'} for row in history()]
    warm=evaluate_strategy(p,minutes,now=at,market_context={**ctx,'historical_5m':seed}).model_dump()
    assert warm['entry_confirmation']['status']!='CONFIRM_WARMUP'
    assert warm['closed_5m_count']==6
    assert warm['entry_confirmation']['pattern']=='A_TREND_PULLBACK'
    # A persisted plan cannot provide its own cache seed/capability.
    spoof={**p,'market_context':{'historical_5m':seed}}
    ignored=evaluate_strategy(spoof,minutes,now=at,market_context=ctx).model_dump()
    assert ignored['entry_confirmation']['status']=='CONFIRM_WARMUP'


def test_historical_seed_cannot_fill_current_gap_or_cross_wrong_session_boundary():
    rows=a_rows()[20:];p=integrated_plan()
    at=datetime.fromisoformat(rows[-1]['bar_end'])
    for seed in (history()[:-1], [{**row,'symbol':'OTHER'} for row in history()],
            [*history()[:-1],rows[0]]):
        result=evaluate_pattern(p,rows,environment(rows),as_of=at,config=CFG,historical_five=seed)
        assert not result['confirmed']
        assert result['status']=='DATA_LIMITED'
    result=evaluate_pattern(p,rows[:2]+rows[3:],environment(rows),as_of=at,config=CFG,historical_five=history())
    assert result['reason_codes']==['CONFIRM_CLOSED_BAR_GAP']


@pytest.mark.parametrize('current,seed',[((),history()),(history(),())])
def test_prior_session_bars_only_cannot_confirm_today(current,seed):
    result=evaluate_pattern(integrated_plan(),current,[],as_of=DAY.replace(hour=10),config=CFG,historical_five=seed)
    assert result['status']=='DATA_LIMITED' and result['confirmed'] is False
    assert result['reason_codes']==['CONFIRM_CURRENT_SESSION_5M_MISSING']


def test_shared_b_cannot_emit_entry_without_actual_execution_binding(monkeypatch):
    import liangjian_funnel.runtime.entry_confirmation as module
    from liangjian_funnel.runtime.strategies import evaluate_strategy
    monkeypatch.setattr(module,'evaluate_pattern',lambda *a,**k:dict(mode='pattern_v1',status='CONFIRMED',
        confirmed=True,pattern='B_BREAKOUT_RETEST',reason_codes=[],conditions={},force_probe=True,tightened_stop=100))
    rows=intraday_minutes();at=rows[-1]['bar_end']
    result=evaluate_strategy(integrated_plan(),rows,now=at,market_context=server_context(at,entry_confirmation_config=CFG)).model_dump()
    assert result['action']=='DATA_BLOCK'
    assert 'ENTRY_PATTERN_B_EXECUTION_UNBOUND' in result['reason_codes']
    assert result['entry_confirmation']['execution_binding']=='UNBOUND_NO_ENTRY'


def test_runtime_overlay_legacy_is_empty_invalid_blocks_new_entry_only(tmp_path):
    from liangjian_funnel.runtime.entry_confirmation import runtime_confirmation_overlay
    from liangjian_funnel.runtime.strategies import evaluate_strategy
    assert runtime_confirmation_overlay(tmp_path)=={}
    (tmp_path/'config').mkdir();(tmp_path/'config/entry_confirmation.yaml').write_text('bad: [',encoding='utf-8')
    overlay=runtime_confirmation_overlay(tmp_path)
    rows=intraday_minutes();at=rows[-1]['bar_end'];p=integrated_plan();ctx=server_context(at,**overlay)
    result=evaluate_strategy(p,rows,now=at,market_context=ctx).model_dump()
    assert result['action']=='DATA_BLOCK'
    assert list(result['reason_codes'])==['ENTRY_CONFIRMATION_CONFIG_INVALID']
    held=evaluate_strategy({**p,'stop_level':101},rows,now=at,position={'quantity':100},market_context=ctx).model_dump()
    assert held['action']=='FORCED_RISK_EXIT'
    assert 'entry_confirmation' not in held


@pytest.mark.parametrize('volume',[20,40,60])
def test_a_positive_flat_confirm_is_allowed_and_all_conditions_audited(volume):
    rows=a_rows();rows[-1]['volume']=volume
    out=run(plan(),rows,env=environment(rows))
    assert out['confirmed'] and out['pattern']=='A_TREND_PULLBACK',out
    assert rows[-1]['close']==rows[-1]['open']
    assert out['conditions']['A_CONTRACTION']['value']['pullback_mean']==50
    assert out['conditions']['A_15M_SUPPORT']['source_as_of']


@pytest.mark.parametrize('case',['no_contraction','support_break','15m_break'])
def test_a_three_independent_rejections(case):
    rows=a_rows();env=environment(rows)
    if case=='no_contraction':rows[-2]['volume']=rows[-3]['volume']=150
    elif case=='support_break':rows[-2]['low']=98
    else:env=environment(rows,99)
    out=run(plan(),rows,env=env)
    assert not out['confirmed']
    expected={'no_contraction':'A_CONTRACTION','support_break':'A_PULLBACK','15m_break':'A_15M_SUPPORT'}[case]
    assert out['conditions'][expected]['status']=='UNMET'


def b_rows():
    rows=history()
    for i,(close,volume) in enumerate(((101,200),(100.8,50),(100.5,40),(100.7,120))):
        rows.append(bar(DAY+timedelta(minutes=5*(i+1)),close,volume))
    rows[-2]['low']=100.3
    rows[-1].update(open=100.6,high=100.8,low=100.5)
    return rows


@pytest.mark.parametrize('profile,volume',[('TREND_MA5',120),('MA520_SWING',140),('TREND_MA5',160)])
def test_b_positive_second_path_stop_only_up_probe_and_exact_key_sources(profile,volume):
    rows=b_rows();rows[-1]['volume']=volume;p=plan(profile,high=100.2)
    original=deepcopy(p);out=run(p,rows)
    assert out['confirmed'] and out['pattern']=='B_BREAKOUT_RETEST',out
    assert out['force_probe'] and out['tightened_stop']>=p['stop_level']
    assert out['current_entry_close']>p['trigger_zone']['high']
    assert out['metrics']['key_level']==100.2 and out['metrics']['breakout_count']==1
    assert len(out['metrics']['key_evidence']['daily_sources'])==20 and p==original


@pytest.mark.parametrize('case',['weak_breakout','loud_retest','lower_stop'])
def test_b_three_independent_rejections(case):
    rows=b_rows();p=plan(high=100.2)
    if case=='weak_breakout':rows[-4]['volume']=100
    elif case=='loud_retest':rows[-2]['volume']=rows[-3]['volume']=200
    else:p['stop_level']=100.4
    out=run(p,rows)
    assert not out['confirmed'],out
    expected={'weak_breakout':'B_EFFECTIVE_BREAKOUT','loud_retest':'B_RETEST_VOLUME','lower_stop':'B_STOP_ONLY_RAISED'}[case]
    assert out['conditions'][expected]['status']=='UNMET'


def test_b_breach_invalidates_and_two_attempt_limit_does_not_reset_on_third_breakout():
    rows=history()
    for i,(close,volume) in enumerate(((101,200),(99.5,100),(101.2,250),(99.4,100),(101.4,300))):
        rows.append(bar(DAY+timedelta(minutes=5*(i+1)),close,volume))
    out=run(plan(high=100.2),rows)
    assert not out['confirmed'] and out['metrics']['breakout_count']==2
    assert out['metrics']['active_breakout'] is None and len(out['metrics']['invalidated_at'])==2


def test_b_no_chase_and_missing_daily_sources_are_not_bypassed():
    p=plan(high=100.2);p['no_chase']=100
    assert not run(p,b_rows())['confirmed']
    out=run(plan(high=100.2),b_rows(),facts={})
    assert out['status']=='DATA_LIMITED' and out['conditions']['B_KEY_LEVEL']['status']=='UNPROVEN'


def c_rows():
    rows=history()
    for i,(close,volume) in enumerate(((100,100),(99.5,100),(99,100),(99.6,200))):
        rows.append(bar(DAY+timedelta(minutes=5*(i+1)),close,volume))
    rows[-1].update(open=99,high=99.7,low=98.95)
    return rows


@pytest.mark.parametrize('volume',[180,200,250])
def test_c_three_volume_rebound_positives(volume):
    rows=c_rows();rows[-1]['volume']=volume
    out=run(plan('MA520_SWING',low=99,high=100,stop=98),rows,vwap=100)
    assert out['confirmed'] and out['pattern']=='C_VOLUME_REBOUND',out
    assert out['conditions']['C_VOLUME_PRICE_SYNCHRONIZED']['status']=='MET'


def test_trend_downleg_may_use_c_repair_instead_of_a():
    out=run(plan('TREND_MA5',low=99,high=100,stop=98),c_rows(),vwap=100)
    assert out['confirmed'] and out['pattern']=='C_VOLUME_REBOUND'


@pytest.mark.parametrize('case',['weak_volume','long_upper_wick','small_decline'])
def test_c_three_independent_rejections(case):
    rows=c_rows()
    if case=='weak_volume':rows[-1]['volume']=100
    elif case=='long_upper_wick':rows[-1]['high']=101.5
    else:
        rows[-3]=bar(datetime.fromisoformat(rows[-3]['bar_end']),99.99)
        rows[-2]=bar(datetime.fromisoformat(rows[-2]['bar_end']),99.98)
        rows[-1]=bar(datetime.fromisoformat(rows[-1]['bar_end']),100,200,open=99.98,high=100.01,low=99.97)
    assert not run(plan('MA520_SWING',low=99,high=100,stop=98),rows)['confirmed']


def test_c_volume_then_repair_in_adjacent_bar_is_valid_without_same_bar_volume():
    rows=history()
    for i,(close,volume) in enumerate(((100,100),(99.5,100),(98.8,100),(99.2,200),(99.5,80))):
        rows.append(bar(DAY+timedelta(minutes=5*(i+1)),close,volume))
    rows[-2].update(open=98.8,high=99.21,low=98.79)
    rows[-1].update(open=99.2,high=99.51,low=99.19)
    out=run(plan('MA520_SWING',low=99,high=100,stop=98),rows)
    assert out['confirmed'],out


def test_legacy_delegates_without_reading_new_inputs_or_changing_anything():
    class Unreadable:
        def __iter__(self):raise AssertionError('legacy read new bars')
    out=evaluate_pattern(None,Unreadable(),Unreadable(),as_of=None,config=LEGACY)
    assert out==dict(mode='legacy',status='LEGACY_DELEGATED',confirmed=None)


@pytest.mark.parametrize('case',['warmup','unclosed','future','zero','duplicate','gap'])
def test_unknown_closed_bar_boundaries_never_create_a_confirmation(case):
    rows=a_rows();as_of=datetime.fromisoformat(rows[-1]['bar_end'])
    if case=='warmup':rows=rows[-19:]
    elif case=='unclosed':rows[-1]['closed']=False
    elif case=='future':as_of-=timedelta(minutes=1)
    elif case=='zero':rows[-1]['volume']=0
    elif case=='duplicate':rows.append(deepcopy(rows[-1]))
    elif case=='gap':rows.pop(-3)
    out=evaluate_pattern(plan(),rows,[],as_of=as_of,config=CFG)
    assert not out['confirmed']
    if case=='warmup':assert out['status']=='CONFIRM_WARMUP'
    if case=='zero':assert out['status']=='NO_TRADES'


def test_actual_loader_defaults_to_legacy_and_validates_exact_contract(tmp_path):
    assert load_entry_confirmation(tmp_path)==LEGACY
    target=tmp_path/'config';target.mkdir()
    (target/'entry_confirmation.yaml').write_text('schema_version: entry-confirmation/1\nentry_confirmation: {}\n')
    with pytest.raises(ValueError,match='CONFIG_INVALID'):load_entry_confirmation(tmp_path)
