from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.evaluation.b3_variants import (build_evaluator, rebase_plan,
    VariantEvidenceError)
from liangjian_funnel.runtime import strategies

TZ=ZoneInfo('Asia/Shanghai')
NOW=datetime(2026,10,15,10,16,tzinfo=TZ)

def bar(end,*,close=9.5,low=9.4,high=9.8,open=9.6):
    return strategies._Bar('600001.SH',end,open,high,low,close,100,950)

def evidence(**changes):
    return dict(auction_price=10.,auction_at='2026-10-15T09:25:00+08:00',
        auction_received_at='2026-10-15T09:26:00+08:00',auction_status='ORIGINAL_TIMESTAMPED',
        atr14=1.,atr_as_of='2026-10-14T16:00:00+08:00',
        atr_last_closed_day='2026-10-14',**changes)

def test_v0_is_exact_production_function():
    assert build_evaluator('V0') is strategies.evaluate_strategy

def test_v2_does_not_permanently_invalidate_on_first_15m_even_5m_failed():
    helper=build_evaluator('V2').__globals__['_pre_entry_risk_decision']
    five=[bar(NOW-timedelta(minutes=11)),bar(NOW-timedelta(minutes=6)),bar(NOW-timedelta(minutes=1))]
    fifteen=[bar(NOW-timedelta(minutes=16))]
    original=strategies._pre_entry_risk_decision(strategies.StrategyProfile.TREND_MA5,{},five[-1],five,fifteen,10.)
    assert original['state']=='PLAN_INVALIDATED'
    revised=helper(strategies.StrategyProfile.TREND_MA5,{},five[-1],five,fifteen,10.)
    assert revised['state']!='PLAN_INVALIDATED'
    assert revised['reason_codes']==['PRE_ENTRY_RISK_LEVEL_TOUCHED']

def test_v2_requires_both_15m_closes_below_stop_and_preserves_two_bar_failure():
    helper=build_evaluator('V2').__globals__['_pre_entry_risk_decision']
    five=[bar(NOW-timedelta(minutes=11)),bar(NOW-timedelta(minutes=6)),bar(NOW-timedelta(minutes=1))]
    fifteen=[bar(NOW-timedelta(minutes=16)),bar(NOW-timedelta(minutes=1))]
    assert helper(strategies.StrategyProfile.TREND_MA5,{},five[-1],five,fifteen,10.)['state']=='PLAN_INVALIDATED'
    fifteen[0]=bar(fifteen[0].end,close=10.2,high=10.3)
    assert helper(strategies.StrategyProfile.TREND_MA5,{},five[-1],five,fifteen,10.)['state']!='PLAN_INVALIDATED'

def test_v3_uses_absolute_full_width_and_does_not_mutate_plan():
    plan={'symbol':'600001.SH','trigger_zone':{'low':8.,'high':8.1},'stop_level':7.,'no_chase':11.}
    original=deepcopy(plan)
    updated,proof=rebase_plan(plan,'V3',evidence(),NOW)
    assert updated['trigger_low']==9.75 and updated['trigger_high']==10.25
    assert updated['trigger_zone']=={'low':9.75,'high':10.25}
    assert updated['stop_level']==7. and updated['no_chase']==11.
    assert plan==original and proof['status']=='RESEARCH_ONLY'

def test_v4_reanchors_stop_chase_but_keeps_target_and_original_values():
    plan={'symbol':'600001.SH','stop_level':7.,'no_chase':11.,'first_resistance':12.}
    updated,proof=rebase_plan(plan,'V4',evidence(),NOW)
    assert updated['stop_level']==pytest.approx(9.2)
    assert updated['no_chase']==pytest.approx(10.8)
    assert updated['first_resistance']==12.
    assert proof['original_levels']['stop_level']==7.

@pytest.mark.parametrize('field,value',[
    ('auction_status','CARD_OBSERVED_PROXY'),('auction_at','2026-10-14T09:25:00+08:00'),
    ('auction_received_at','2026-10-15T10:17:00+08:00'),
    ('auction_at','2026-10-15T09:31:00+08:00'),('atr_as_of','2026-10-15T10:17:00+08:00'),
    ('atr_last_closed_day','2026-10-15'),('atr_last_closed_day',None),('atr14',0),('auction_price',float('nan'))])
def test_rebase_requires_timestamped_same_day_original_auction_and_known_atr(field,value):
    e=evidence();e[field]=value
    with pytest.raises(VariantEvidenceError):rebase_plan({},'V4',e,NOW)

def test_v1_clones_are_isolated_and_unknown_variant_fails():
    before=dict(strategies.evaluate_strategy.__globals__)
    assert build_evaluator('V1').__globals__ is not strategies.evaluate_strategy.__globals__
    assert strategies.evaluate_strategy.__globals__==before
    with pytest.raises(VariantEvidenceError):build_evaluator('V99')

def test_v1_uses_existing_nonweak_definition_not_missing_prior_shortcut():
    five=[bar(NOW-timedelta(minutes=16-i*5),open=9.45,high=9.9,low=9.4,close=9.8) for i in range(4)]
    prior=bar(NOW-timedelta(minutes=16))
    latest=bar(NOW-timedelta(minutes=1),low=9.3,high=9.9,open=9.6,close=9.8)
    plan={'symbol':'600001.SH','trend_entry_rule_version':'trend-ma5/2'}
    args=(plan,five,[prior,latest],five[-1],{},False,False)
    old=strategies._evaluate_trend(*args)
    revised=build_evaluator('V1').__globals__['_evaluate_trend'](*args)
    assert 'TREND_15M_PRESSURE_EASING' in old['unmet_conditions']
    assert 'TREND_15M_PRESSURE_EASING' in revised['met_conditions']
    one=build_evaluator('V1').__globals__['_evaluate_trend'](plan,five,[latest],five[-1],{},False,False)
    assert 'TREND_15M_PRESSURE_EASING' in one['unmet_conditions']

def test_v2_does_not_change_520_independent_failure_path():
    helper=build_evaluator('V2').__globals__['_pre_entry_risk_decision']
    five=[bar(NOW-timedelta(minutes=6)),bar(NOW-timedelta(minutes=1))]
    args=(strategies.StrategyProfile.MA520_SWING,{},five[-1],five,[bar(NOW-timedelta(minutes=1))],10.)
    assert helper(*args)==strategies._pre_entry_risk_decision(*args)
