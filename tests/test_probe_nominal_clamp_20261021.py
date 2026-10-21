from dataclasses import replace
from decimal import Decimal as D
import pytest

from liangjian_funnel.runtime.risk_engine import (
    RiskLimits,PositionRisk,build_risk_envelope,adjudicate_signal,
    cap_risk_envelope,clamp_entry_quantity,
)
from test_team_v2_risk_engine import plan,account,NOW


def test_actual_002270_price_geometry_probe_10pct_not_20pct():
    p=plan(symbol='002270.SZ',price=D('18.80'),stop_price=D('18.66'),
        entry_low=D('18.664'),entry_high=D('18.81142857142857'),no_chase=D('19.19'),
        invalidation_price=D('18.66'),risk_unit='PROBE')
    e=build_risk_envelope(p,account(),(),RiskLimits(),now=NOW,regime='ROTATION_NO_MAINLINE')
    assert e.max_shares_cap==5300
    a=e.quantity_audit
    assert a['cap20_qty']==10600 and a['probe10_qty']==5300 and a['final_qty']==5300
    assert 'PROBE_NOTIONAL_CAP' in a['binding_limits']
    assert D(a['raw_risk_qty'])>100000
    d=adjudicate_signal('actual-geometry','BUY_SIGNAL',p,e,account(),(),RiskLimits(),now=NOW,regime='ROTATION_NO_MAINLINE')
    assert d.shares==5300 and d.quantity_audit['final_qty']==5300
    assert d.shares*p.price<=account().nav*D('.10')


@pytest.mark.parametrize('origin',['plan','a6','pm','a4','fallback','pattern'])
def test_every_probe_source_uses_same_nominal_clamp(origin):
    p=plan(stop_price=D('9.99'),invalidation_price=D('9.99'))
    if origin=='plan':p=replace(p,risk_unit='PROBE')
    if origin=='fallback':p=replace(p,degradation_tags=('A2_UNAVAILABLE',))
    if origin=='pattern':p=replace(p,degradation_tags=('PATTERN_B',))
    e=build_risk_envelope(p,account(),(),RiskLimits(),now=NOW,regime='TREND_MAINLINE')
    capped=cap_risk_envelope(e,plan=p,limits=RiskLimits(),**({origin:'PROBE'} if origin in {'a6','pm','a4'} else {}))
    assert capped.risk_unit=='PROBE' and capped.max_shares_cap==10000
    assert cap_risk_envelope(capped,plan=p,limits=RiskLimits()).max_shares_cap==10000


def test_standard_unchanged_and_existing_positions_or_reservations_consume_room():
    p=plan(stop_price=D('9.99'),invalidation_price=D('9.99'))
    e=build_risk_envelope(p,account(),(),RiskLimits(),now=NOW,regime='TREND_MAINLINE')
    assert e.risk_unit=='STANDARD' and e.max_shares_cap==20000 and e.quantity_audit['probe10_qty'] is None
    old=PositionRisk('old',p.symbol,p.cluster,6000,0,D('60000'),D('9'))
    changed=build_risk_envelope(replace(p,risk_unit='PROBE'),account(),(old,),RiskLimits(),
        now=NOW,regime='TREND_MAINLINE',action='ADD_SIGNAL')
    assert changed.max_shares_cap==4000
    assert old.qty==6000  # never resize today's existing holding


def test_stop_too_tight_is_only_a_review_flag_not_veto():
    p=plan(stop_price=D('9.99'),invalidation_price=D('9.99'),daily_atr14=D('.50'),risk_unit='PROBE')
    e=build_risk_envelope(p,account(),(),RiskLimits(),now=NOW,regime='TREND_MAINLINE')
    assert e.eligible and e.max_shares_cap==10000
    assert e.quantity_audit['review_flags']==['STOP_TOO_TIGHT']
    assert D(e.quantity_audit['stop_distance_atr'])==D('.02')
    assert not e.blocked_reasons
    assert cap_risk_envelope(e,plan=p).quantity_audit['review_flags']==['STOP_TOO_TIGHT']


def test_old_oversize_envelope_cannot_bypass_fresh_transaction_cap():
    p=plan(stop_price=D('9.99'),invalidation_price=D('9.99'),risk_unit='PROBE')
    e=build_risk_envelope(p,account(),(),RiskLimits(),now=NOW,regime='TREND_MAINLINE')
    old=replace(e,max_shares_cap=20000,quantity_audit={})
    d=adjudicate_signal('legacy-envelope','BUY_SIGNAL',p,old,account(),(),RiskLimits(),now=NOW,regime='TREND_MAINLINE')
    assert d.shares==10000
