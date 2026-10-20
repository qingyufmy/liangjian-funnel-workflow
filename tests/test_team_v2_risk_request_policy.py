from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import pytest

from liangjian_funnel.runtime.team_v2_risk_policy import (
    DEFAULT_POLICY, load_risk_policy, risk_output_budget, project_risk_values)
from liangjian_funnel.runtime.team_v2_pm_policy import derive_pm_fallback
from liangjian_funnel.runtime.risk_engine import conservative_envelope
from test_team_v2_risk_meeting import envelope


def test_full_catalogue_values_unchanged_and_refs_resolve_exactly(tmp_path):
    plans=[{'plan_id':'p1','narrative':'keep all original words','geometry':{'low':10}}]
    values={'PLANS':plans,'EVIDENCE_CATALOG':{'items':[
        {'id':'plan:p1','category':'PLAN','value':plans[0]}]},'OTHER':{'all':'untouched'}}
    before=deepcopy(values);out=project_risk_values(values)
    assert values==before and out['EVIDENCE_CATALOG']==before['EVIDENCE_CATALOG']
    assert out['OTHER']==before['OTHER']
    assert out['PLANS']=={'source':'EVIDENCE_CATALOG','plan_evidence_ids':['plan:p1']}
    values['PLANS'][0]['geometry']['low']=11
    # Deliberately separate copies: duplicate input disagreement is never hidden.
    values['EVIDENCE_CATALOG']=deepcopy(before['EVIDENCE_CATALOG'])
    with pytest.raises(ValueError,match='VALUE_MISMATCH'):project_risk_values(values)
    assert risk_output_budget(DEFAULT_POLICY,33,11,393216)==20480
    assert risk_output_budget(DEFAULT_POLICY,33,11,8192)==8192
    assert load_risk_policy(tmp_path)==DEFAULT_POLICY


def test_degraded_fallback_requires_actual_conservative_probe_not_plain_eligible():
    base=envelope();kwargs=dict(plan_clusters={'p1':'bank'},priority=['p1'],free_slots=6,
        max_new_entries=3,utilization_cap=Decimal('65'),a6_degraded_conservative=True)
    with pytest.raises(ValueError,match='CONSERVATIVE_ENVELOPE_INVALID'):
        derive_pm_fallback(envelopes=(base,),**kwargs)
    reduced=conservative_envelope(base,consecutive_failures=1)
    result=derive_pm_fallback(envelopes=(reduced,),**kwargs)
    assert len(result['slot_plan'])==1
    assert result['plan_allocations'][0]['intended_risk_unit']=='PROBE'
    halted=conservative_envelope(base,consecutive_failures=2)
    assert derive_pm_fallback(envelopes=(halted,),**kwargs)['slot_plan']==[]
    assert derive_pm_fallback(envelopes=(reduced,),**(kwargs|{'utilization_cap':0}))['slot_plan']==[]


def test_real_sessions_mock_a6_failure_then_pm_failure_compile_probe(tmp_path):
    from test_team_v2_risk_session import fixture as risk_fixture
    from test_team_v2_pm_session import fixture as pm_fixture
    from liangjian_funnel.runtime.team_v2_risk_session import run_risk_premarket
    from liangjian_funnel.runtime.team_v2_pm_session import run_pm_premarket
    from liangjian_funnel.runtime.team_v2_allocation import compile_pm_allocation
    risk_kwargs,risk_calls,_=risk_fixture(tmp_path/'risk',offline=True)
    risk=run_risk_premarket(**risk_kwargs)
    assert risk.status=='DEGRADED' and risk.consecutive_failures==1
    pm_kwargs,pm_calls=pm_fixture(tmp_path/'pm',offline=True)
    pm_kwargs.update(envelopes=risk.envelopes,utilization_cap=risk.utilization_cap,
        a6_validated=False,a6_degraded_conservative=True,fallback_priority_order=('p1',),max_new_entries=3)
    result=run_pm_premarket(**pm_kwargs)
    assert not result['model_validated'] and result['status']=='READY'
    assert result['fallback_envelope_authority']=='SERVER_A6_DEGRADED_CONSERVATIVE'
    assert compile_pm_allocation(result,account_id='paper',trade_date=pm_kwargs['trade_date'],
        meeting_id=pm_kwargs['meeting_id'],input_sha256='frozen').allocations==(('p1','PROBE',1),)
    assert run_pm_premarket(**pm_kwargs)==result
    assert len(risk_calls)==len(pm_calls)==1
