"""P0-2 frozen A6 arithmetic, independent derived audit; no provider I/O."""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.team_v2_cluster_quota import (
    build_cluster_quota_context, checked_cluster_quota_context, normalize_cluster_quota)
from liangjian_funnel.pipeline.team_v2_risk_schema import validate_risk_premarket
from liangjian_funnel.runtime.risk_engine import RiskEnvelope
from liangjian_funnel.runtime.team_v2_risk_meeting import apply_premarket_review
from liangjian_funnel.runtime.team_v2_store import canonical
from test_team_v2_risk_schema import output, CAT, CTX


def context(*, slots=6, counts=(3, 3, 3, 3), cap=2):
    clusters={f'c{i}':cap for i in range(len(counts))}
    plans={f'p{i}-{j}':f'c{i}' for i,count in enumerate(counts) for j in range(count)}
    return build_cluster_quota_context(engine_slots=slots,cluster_caps=clusters,
        plan_clusters=plans,plan_priority_order=list(reversed(plans)))


@pytest.mark.parametrize('slots,counts,expected',[(6,(3,)*11,6),(6,(1,1),2),(6,(1,3),3),(0,(3,3),0)])
def test_engine_bound_and_a3_priority(slots,counts,expected):
    ctx=context(slots=slots,counts=counts)
    assert sum(ctx['engine_cluster_quota'].values())==expected
    assert ctx['cluster_priority_order'][0]==f'c{len(counts)-1}'
    assert all(v<=min(2,ctx['cluster_plan_counts'][c]) for c,v in ctx['engine_cluster_quota'].items())
    raw={'capacity_for_tomorrow':{'plans_acceptable':99,'cluster_quota':{c:2 for c in ctx['cluster_caps']}},'unchanged':['risk']}
    before=deepcopy(raw);derived,audit=normalize_cluster_quota(raw,ctx)
    assert raw==before and derived['unchanged']==before['unchanged']
    assert audit['code']=='SERVER_NORMALIZED_CLUSTER_QUOTA'
    assert audit['effective_quota_sum']==expected
    assert audit['effective_plans_acceptable']==expected


def test_never_raises_model_voluntary_tightening_or_adds_omitted_cluster():
    ctx=context(slots=2,counts=(3,3,3))
    raw={'capacity_for_tomorrow':{'plans_acceptable':2,'cluster_quota':{'c2':0,'c1':1,'c0':9}}}
    derived,audit=normalize_cluster_quota(raw,ctx)
    assert derived['capacity_for_tomorrow']['cluster_quota']=={'c2':0,'c1':1,'c0':1}
    raw['capacity_for_tomorrow']['cluster_quota']={'c0':1}
    assert normalize_cluster_quota(raw,ctx) is None
    raw['capacity_for_tomorrow']['plans_acceptable']=0
    derived,_=normalize_cluster_quota(raw,ctx)
    assert derived['capacity_for_tomorrow']['cluster_quota']=={'c0':0}


@pytest.mark.parametrize('quota,slots',[({'foreign':1},1),({'c0':-1},1),({'c0':True},1),({'c0':1.5},1),({'c0':1},True),({'c0':1},-1)])
def test_bad_numeric_or_foreign_domain_is_not_repaired(quota,slots):
    assert normalize_cluster_quota({'capacity_for_tomorrow':dict(plans_acceptable=slots,cluster_quota=quota)},context()) is None


@pytest.mark.parametrize('field',['cluster_plan_counts','cluster_upper_bounds','cluster_priority_order','engine_cluster_quota'])
def test_derived_server_context_cannot_be_forged(field):
    ctx=context();ctx[field]={} if isinstance(ctx[field],dict) else []
    with pytest.raises(ValueError,match='SERVER_CONTEXT_INVALID'):checked_cluster_quota_context(ctx)


def test_strict_validator_plan_count_bound_and_other_invalid_judgments_survive():
    ctx=build_cluster_quota_context(engine_slots=3,cluster_caps={'bank':2},plan_clusters={'p1':'bank'},plan_priority_order=['p1'])
    raw=output();before=deepcopy(raw)
    args=CTX|{'cluster_plan_counts':{'bank':1}}
    assert not validate_risk_premarket(raw,CAT,**args).valid
    effective,_=normalize_cluster_quota(raw,ctx)
    assert validate_risk_premarket(effective,CAT,**args).valid
    assert raw==before
    for bad in ('LOOSEN','BAD_REFS','BAD_RISK'):
        invalid=deepcopy(raw)
        if bad=='LOOSEN':invalid['envelope_adjustments'][0]['adjustment']='LOOSEN'
        elif bad=='BAD_REFS':invalid['utilization_cap']['supporting_refs']=['FOREIGN']
        else:invalid['capacity_for_tomorrow']['risk_unit']='OVERSIZE'
        derived,_=normalize_cluster_quota(invalid,ctx)
        assert not validate_risk_premarket(derived,CAT,**args).valid


def real_case():
    frozen=json.loads((Path(__file__).parent/'fixtures/team_v2_a6_cluster_quota_20261020.json').read_text(encoding='utf-8'))
    env=tuple(RiskEnvelope(**(row|{'risk_budget':Decimal(row['risk_budget']),
        'blocked_reasons':tuple(row['blocked_reasons']),'binding_limits':tuple(row['binding_limits'])})) for row in frozen['envelopes'])
    rows=sorted(frozen['priorities'],key=lambda row:(row['plan_priority_rank'],row['symbol'],row['plan_id']))
    ctx=build_cluster_quota_context(engine_slots=frozen['capacity_slots'],cluster_caps=frozen['cluster_caps'],
        plan_clusters={row['plan_id']:row['cluster'] for row in frozen['plan_clusters']},
        plan_priority_order=[row['plan_id'] for row in rows])
    args=dict(required_envelope=frozen['raw']['envelope'],envelopes=env,
        engine_utilization_cap=Decimal(str(frozen['engine_utilization_cap'])),capacity_slots=frozen['capacity_slots'],
        cluster_caps=frozen['cluster_caps'],unready_plan_ids={e.plan_id for e in env if not e.eligible})
    return frozen,ctx,args


def test_actual_20261020_raw_stays_invalid_derived_ready_positive_eligible_and_cap():
    frozen,ctx,args=real_case();raw=frozen['raw'];before=canonical(raw)
    assert hashlib.sha256(before.encode()).hexdigest()==frozen['provenance']['raw_output_sha256']
    old=validate_risk_premarket(raw,frozen['catalog'],**args)
    assert not old.valid and 'A6_CAPACITY_DETAIL:cluster_quota:SUM_GT_PLANS_ACCEPTABLE' in old.issues
    derived,audit=normalize_cluster_quota(raw,ctx)
    checked=validate_risk_premarket(derived,frozen['catalog'],**args,cluster_plan_counts=ctx['cluster_plan_counts'])
    assert checked.valid,checked.issues
    assert canonical(raw)==before
    assert sum(raw['capacity_for_tomorrow']['cluster_quota'].values())==22
    assert audit['effective_quota_sum']==6
    assert raw|{'capacity_for_tomorrow':derived['capacity_for_tomorrow']}==derived
    result=apply_premarket_review(args['envelopes'],derived,engine_utilization_cap=args['engine_utilization_cap'],validated=checked.valid,previous_failures=0)
    assert result.status=='READY' and sum(e.eligible for e in result.envelopes)>0
    assert result.utilization_cap==Decimal('65')
