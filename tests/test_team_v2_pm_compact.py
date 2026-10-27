"""0404 R1/R2: bounded frozen shortlist, raw five-field response, risk unchanged."""
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
import sqlite3

import pytest

from test_team_v2_pm_session import fixture,response
from test_team_v2_pm_schema import env,ENVELOPE,CATALOG
from liangjian_funnel.pipeline.team_v2_pm_schema import project_pm_compact
from liangjian_funnel.runtime.team_v2_pm_policy import pm_shortlist,pm_output_budget,DEFAULT_POLICY
from liangjian_funnel.runtime.team_v2_pm_session import run_pm_premarket
from liangjian_funnel.runtime.team_v2_allocation import compile_pm_allocation


def context():
    catalog=deepcopy(CATALOG);catalog['categories'].append('PLAN')
    catalog['items'].append(dict(id='PLAN:p1',category='PLAN',value=dict(plan_id='p1')))
    return catalog,dict(plan_refs={'PLAN:p1':'p1'},required_envelope=ENVELOPE,envelopes=(env(),),
        plan_clusters={'p1':'bank'},free_slots=2,baseline_range=(Decimal(50),Decimal(75)),
        utilization_cap=Decimal(75),high_conviction_ids=set())


def test_compact_raw_separate_source_projection_never_invents_opposing_refs():
    catalog,args=context();raw=response();frozen=deepcopy(raw)
    projected,result=project_pm_compact(raw,catalog,**args)
    assert result.valid and raw==frozen
    assert projected['plan_allocations'][0]['max_batches']==1
    assert projected['plan_allocations'][0]['conviction']=='NORMAL'
    assert projected['plan_allocations'][0]['source_link']=='PLAN:p1'
    assert 'opposing_refs' not in projected['plan_allocations'][0]
    assert 'supporting_refs' not in projected['plan_ranking'][0]


@pytest.mark.parametrize('key,value',[
    ('plan_ref','p1'),('plan_ref','M'),('plan_ref','FOREIGN'),('plan_ref',[]),
    ('decision','BUY'),('decision',{}),('size_tier','HIGH_CONVICTION'),('size_tier',True),
    ('reason_code','anything'),('reason_code',[]),('note','x'*41),('note',''),('note',None),
    ('decision','WATCH'),('size_tier','NONE')])
def test_compact_strict_five_fields_domain_enum_and_note(key,value):
    catalog,args=context();raw=response();raw['plan_decisions'][0][key]=value
    assert not project_pm_compact(raw,catalog,**args)[1].valid


def test_global_refs_risk_upgrade_coverage_and_extra_fields_still_hard_fail():
    catalog,args=context()
    for mutate in (
        lambda o:o['plan_decisions'].append(deepcopy(o['plan_decisions'][0])),
        lambda o:o['plan_decisions'].clear(),
        lambda o:o['plan_decisions'][0].update(shares=100),
        lambda o:o['deployment_target'].update(target_pct=99),
        lambda o:o['deployment_target'].update(opposing_refs=['FOREIGN']),
        lambda o:o.update(plan_ranking=[])):
        raw=response();mutate(raw)
        assert not project_pm_compact(raw,catalog,**args)[1].valid
    args['envelopes']=(env(risk_unit='PROBE'),)
    assert 'PM_ALLOCATION_ABOVE_ENVELOPE' in project_pm_compact(response(),catalog,**args)[1].issues
    raw=response();raw['plan_decisions'][0]['size_tier']='PROBE'
    assert project_pm_compact(raw,catalog,**args)[1].valid
    raw['plan_decisions'][0].update(decision='WATCH',size_tier='NONE',reason_code='WATCH_RISK')
    projected,checked=project_pm_compact(raw,catalog,**args)
    assert checked.valid and projected['slot_plan']==[]


def test_33_all_reviewable_in_original_a3_priority_a6_and_geometry_without_mutating():
    rows=[dict(plan_id=f'p{i:02}',symbol=f's{i:02}',cluster='c'+str(i%11),
        publication_facts={'plan_priority_rank':34-i}) for i in range(1,34)]
    catalog={'categories':['PLAN'],'items':[dict(id='PLAN:'+p['plan_id'],category='PLAN',value=p) for p in rows]}
    values={'EVIDENCE_CATALOG':catalog,'PLANS_TODAY':rows,'PLANS':rows};frozen=deepcopy(values)
    envelopes=tuple(replace(env(p['plan_id']),symbol=p['symbol']) for p in rows)
    clusters={p['plan_id']:p['cluster'] for p in rows}
    geometry=set(clusters)
    projected,audit,refs=pm_shortlist(values,envelopes=envelopes,plan_clusters=clusters,
        geometry_plan_ids=geometry,max_positions=6)
    assert audit['selected_plan_ids']==[f'p{i:02}' for i in range(33,0,-1)]
    assert len(refs)==33 and len(projected['EVIDENCE_CATALOG']['items'])==33
    assert audit['excluded']==[] and audit['global_truncation'] is False and audit['batch_capacity']==12
    assert values==frozen
    # Actual geometry 15 EMPTY / 7 PARTIAL / 4 FULL / 7 unknown does NOT make 12.
    geometry={f'p{i:02}' for i in range(16,27)}
    a6=tuple(replace(e,eligible=e.plan_id!='p20') for e in envelopes)
    _,narrow,refs=pm_shortlist(values,envelopes=a6,plan_clusters=clusters,
        geometry_plan_ids=geometry,max_positions=6)
    assert len(refs)==10 and 'p20' not in refs.values()
    _,missing,refs=pm_shortlist(values,envelopes=a6,plan_clusters=clusters,
        geometry_plan_ids=None,max_positions=6)
    assert refs=={} and missing['selected_plan_ids']==[]
    labelled=deepcopy(values)
    for item in labelled['EVIDENCE_CATALOG']['items']:
        item['value']['publication_facts']['team_plan_geometry']={'classification':'FULL'}
    assert pm_shortlist(labelled,envelopes=a6,plan_clusters=clusters,
        geometry_plan_ids=None,max_positions=6)[2]=={}
    assert pm_output_budget(DEFAULT_POLICY,33,393216)==6144
    assert pm_output_budget(DEFAULT_POLICY,33,4096)==4096
    assert pm_output_budget(DEFAULT_POLICY,33,393216,max_positions=2)==3072


def test_actual_mock_journals_compact_raw_and_derived_allocation_remain_distinct(tmp_path):
    args,seen=fixture(tmp_path);result=run_pm_premarket(**args)
    assert result['model_validated'] and result['shortlist_audit']['selected_plan_ids']==['p1']
    assert result['server_projection']['raw_output_sha256']!=result['server_projection']['derived_output_sha256']
    policy=compile_pm_allocation(result,account_id='paper',trade_date=args['trade_date'],
        meeting_id=args['meeting_id'],input_sha256='frozen')
    assert policy.allocations==( ('p1','STANDARD',1), )
    with sqlite3.connect(tmp_path/'model.sqlite3') as db:
        raw=json.loads(db.execute('SELECT output_json FROM model_calls').fetchone()[0])
    assert 'plan_decisions' in raw and 'plan_allocations' not in raw and len(seen)==1


def test_failure_fallback_still_uses_full_original_domain_not_shortlist(tmp_path):
    args,_=fixture(tmp_path,offline=True)
    args.update(geometry_plan_ids=frozenset(),a6_validated=True,fallback_priority_order=('p1',))
    out=run_pm_premarket(**args)
    assert out['shortlist_audit']['selected_plan_ids']==[]
    assert out['decision_authority']=='SERVER_FALLBACK_NOT_MODEL_VALIDATED'
    assert out['slot_plan'][0]['primary_plan_id']=='p1'
    assert out['plan_allocations'][0]['intended_risk_unit']=='PROBE'
