from copy import deepcopy
from decimal import Decimal
import pytest

from liangjian_funnel.pipeline.team_v2_pm_schema import validate_pm_premarket,derive_pm_slots
from test_team_v2_pm_schema import CATALOG,ENVELOPE,REFS,env,ranking_response


def context():
    return dict(required_envelope=ENVELOPE,envelopes=(env(),),plan_clusters={'p1':'bank'},
        free_slots=6,baseline_range=(Decimal(50),Decimal(75)),utilization_cap=Decimal(75),high_conviction_ids=set())


@pytest.mark.parametrize('kind',['duplicate','ineligible','rank-gap','bool-rank','foreign-cluster','foreign-ref','model-slot'])
def test_ranking_never_repairs_or_accepts_invalid_model_domain(kind):
    raw=ranking_response();ctx=context()
    if kind=='duplicate':raw['plan_ranking'].append(dict(raw['plan_ranking'][0],rank=2))
    if kind=='ineligible':ctx['envelopes']=(env(eligible=False),)
    if kind=='rank-gap':raw['plan_ranking'][0]['rank']=2
    if kind=='bool-rank':raw['plan_ranking'][0]['rank']=True
    if kind=='foreign-cluster':raw['plan_ranking'][0]['cluster']='made-up'
    if kind=='foreign-ref':raw['plan_ranking'][0]['supporting_refs']=['outside']
    if kind=='model-slot':raw['slot_plan']=[]
    assert not validate_pm_premarket(raw,CATALOG,**ctx).valid


def test_eight_real_day_cluster_shape_deterministic_four_primaries_four_unique_backups():
    clusters={f'p{i}':'CONSUMER_SERVICES' if i<4 else 'FINANCIAL_HIGH_DIVIDEND' for i in range(8)}
    ranking=[dict(rank=i+1,plan_id=pid,cluster=cluster,**REFS) for i,(pid,cluster) in enumerate(clusters.items())]
    original=deepcopy(ranking);envelopes=tuple(env(pid) for pid in clusters)
    result=derive_pm_slots(ranking,envelopes=envelopes,plan_clusters=clusters,free_slots=6)
    assert ranking==original and result['derived_shortfall']=='CLUSTER_CAP'
    assert [r['primary_plan_id'] for r in result['slot_plan']]==['p0','p1','p4','p5']
    assert [r['backup_plan_id'] for r in result['slot_plan']]==['p2','p3','p6','p7']
    raw=ranking_response();raw['plan_ranking']=ranking
    raw['plan_allocations']=[dict(raw['plan_allocations'][0],plan_id=pid) for pid in clusters]
    assert validate_pm_premarket(raw,CATALOG,**(context()|dict(envelopes=envelopes,plan_clusters=clusters))).valid


def test_zero_slots_or_empty_ranking_never_manufactures_allocation():
    raw=ranking_response();raw['plan_ranking']=[];raw['plan_allocations']=[]
    assert validate_pm_premarket(raw,CATALOG,**context()).valid
    assert derive_pm_slots([],envelopes=(),plan_clusters={},free_slots=6)==dict(slot_plan=[],derived_shortfall='NO_ELIGIBLE_PLANS')
    assert derive_pm_slots(ranking_response()['plan_ranking'],envelopes=(env(),),plan_clusters={'p1':'bank'},free_slots=0)['slot_plan']==[]
