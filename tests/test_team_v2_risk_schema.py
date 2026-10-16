from copy import deepcopy
from decimal import Decimal
import pytest

from liangjian_funnel.pipeline.team_v2_risk_schema import validate_risk_premarket
from test_team_v2_risk_meeting import envelope

CAT={'categories':['ACCOUNT','MARKET'],'items':[{'id':'C1','category':'ACCOUNT'},{'id':'M1','category':'MARKET'}]}
ENV={'stage_id':'A6','run_id':'risk-local'}
CTX=dict(required_envelope=ENV,envelopes=(envelope(),),engine_utilization_cap=Decimal('75'),
    capacity_slots=3,cluster_caps={'bank':2},unready_plan_ids=set())


def output(adjustment='KEEP'):
    refs=dict(supporting_refs=['C1'],opposing_refs=['M1'])
    return dict(envelope=deepcopy(ENV),schema_version='a6-risk/1.0.0',session='PREMARKET',
        envelope_adjustments=[dict(plan_id='p1',adjustment=adjustment,reason_code='CAPACITY_CHECKED',
            what_would_change_my_mind=['market risk changes'],**refs)],
        utilization_cap=dict(cap_pct=75,basis='engine upper bound',**refs),exception_actions=[],overnight_reviews=[],
        capacity_for_tomorrow=dict(plans_acceptable=2,cluster_quota={'bank':2},risk_unit='STANDARD',
            response_to_pm='within capacity',notes=['bounded by engine'],**refs),risk_event_attribution=[],
        handoff_notes=dict(to_a3=['use permitted capacity'],to_a4=[],to_a5=[]),
        what_would_change_my_mind=['breadth deteriorates'],unresolved_questions=[])


@pytest.mark.parametrize('action',['KEEP','TIGHTEN_TO_PROBE','REDUCE_BATCHES','SUSPEND'])
def test_authorized_adjustments_valid_with_exact_binding_and_evidence(action):
    assert validate_risk_premarket(output(action),CAT,**CTX).valid


@pytest.mark.parametrize('kind',['loosen','cap','nan','bool','extra','envelope','duplicate','missing',
    'wrong_plan','falsifier','refs','capacity','cluster','wrong_session','exception','overnight','unready_keep'])
def test_schema_or_loosening_errors_are_invalid_not_repaired(kind):
    raw=output();context=dict(CTX)
    if kind=='loosen':raw['envelope_adjustments'][0]['adjustment']='LOOSEN'
    elif kind=='cap':raw['utilization_cap']['cap_pct']=76
    elif kind=='nan':raw['utilization_cap']['cap_pct']=float('nan')
    elif kind=='bool':raw['utilization_cap']['cap_pct']=True
    elif kind=='extra':raw['shares']=1000
    elif kind=='envelope':raw['envelope']={}
    elif kind=='duplicate':raw['envelope_adjustments']*=2
    elif kind=='missing':raw['envelope_adjustments']=[]
    elif kind=='wrong_plan':raw['envelope_adjustments'][0]['plan_id']='unknown'
    elif kind=='falsifier':raw['envelope_adjustments'][0]['what_would_change_my_mind']=[]
    elif kind=='refs':raw['utilization_cap']['opposing_refs']=[]
    elif kind=='capacity':raw['capacity_for_tomorrow']['plans_acceptable']=99
    elif kind=='cluster':raw['capacity_for_tomorrow']['cluster_quota']={'outside':1}
    elif kind=='wrong_session':raw['session']='POST_CLOSE'
    elif kind=='exception':raw['exception_actions']=[{}]
    elif kind=='overnight':raw['overnight_reviews']=[{}]
    elif kind=='unready_keep':context['unready_plan_ids']={'p1'}
    assert not validate_risk_premarket(raw,CAT,**context).valid


def test_unready_plan_may_only_tighten_and_sentinel_search_scope_is_bound():
    raw=output('SUSPEND')
    raw['envelope_adjustments'][0].update(opposing_refs=['NO_OPPOSING_EVIDENCE_FOUND'],
        opposing_search_scope=['ACCOUNT','MARKET'])
    assert validate_risk_premarket(raw,CAT,**(CTX|dict(unready_plan_ids={'p1'}))).valid


@pytest.mark.parametrize('raw',[None,[],{}, {'envelope_adjustments':[None]}])
def test_bad_nested_types_fail_closed(raw):
    assert not validate_risk_premarket(raw,CAT,**CTX).valid
