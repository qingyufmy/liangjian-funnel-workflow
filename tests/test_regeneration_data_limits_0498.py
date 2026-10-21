"""Regeneration-only source gaps survive publication, A6 and fallback admission."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
import json
import sqlite3

import pytest

from liangjian_funnel.runtime.regeneration_data_limits import (
    source_gap_detail, project_data_limited, a6_gap_requirements,
    gap_reviewed_ids, restrict_unreviewed)
from liangjian_funnel.runtime.risk_engine import plan_risk_fields, effective_risk_unit, conservative_envelope
from liangjian_funnel.runtime.team_v2_risk_session import run_risk_premarket
from liangjian_funnel.runtime.team_v2_sources import meeting_plan_facts
from liangjian_funnel.workflow import WorkflowApplication
from test_team_v2_risk_session import fixture as risk_fixture
from test_w2_shadow_inputs_publication import publisher_fixture, AT, SYMBOL

GAPS=['CNINFO_PDF:original-document:FETCH_FAILED','INDICATORS:MISSING']


def snapshot():
    return {'snapshot_manifest':{'as_of':'2026-10-21T18:21:57+08:00',
        'source_failures':{'000001.SZ':GAPS}}}


def test_projection_is_same_facts_only_probe_never_mutates_source():
    data=snapshot();payload={'symbol':'000001.SZ','risk_unit':'STANDARD'}
    original=deepcopy((data,payload))
    detail=source_gap_detail(data,'000001.SZ');out=project_data_limited(payload,detail)
    assert (data,payload)==original
    assert out['source_data_limits']['reason_codes']==GAPS and out['DATA_LIMITED'] is True
    assert out['risk_unit']=='PROBE' and plan_risk_fields(out)['degradation_tags']==('DATA_LIMITED',)
    assert effective_risk_unit(plan_risk_fields(out),a6='STANDARD',pm='STANDARD',a4='STANDARD')=='PROBE'
    assert project_data_limited(payload,source_gap_detail(data,'600000.SH')) is payload


@pytest.mark.parametrize('data',[{}, {'snapshot_manifest':{}},
    {'snapshot_manifest':{'source_failures':{'000001.SZ':'bad'}}}])
def test_regeneration_cannot_assume_missing_manifest_or_malformed_detail_complete(data):
    with pytest.raises(ValueError,match='REGEN_A1_SOURCE_FAILURE_'):
        source_gap_detail(data,'000001.SZ')


def test_publisher_regeneration_only_and_pre_geometry_selection_audit():
    app,result,data,_=publisher_fixture()
    data['snapshot_manifest']['source_failures']={SYMBOL:GAPS}
    before=deepcopy(data)
    natural=WorkflowApplication._publish_plans(app,result,now=AT,slot='close',snapshot_data=data)
    assert 'DATA_LIMITED' not in app.store.batch[0]['payload']
    assert natural['regeneration_source_gap_audit']==[]
    regenerated=WorkflowApplication._publish_plans(app,result,now=AT,slot='close',snapshot_data=data,
        regeneration=SimpleNamespace(old_plan_ids=('old',),run_id=result.run_id))
    assert app.store.batch[0]['payload']['source_data_limits']['reason_codes']==GAPS
    assert app.store.batch[0]['payload']['risk_unit']=='PROBE'
    assert regenerated['regeneration_source_gap_audit'][0]['symbol']==SYMBOL
    assert data==before


def limited_fixture(tmp_path,**options):
    kwargs,seen,raw=risk_fixture(tmp_path,**options)
    e=kwargs['envelopes'][0]
    kwargs['envelopes']=(replace(e,risk_unit='PROBE',plan_risk_unit='PROBE',degradation_tags=('DATA_LIMITED',)),)
    detail=source_gap_detail(snapshot(),'000001.SZ')
    fact=dict(plan_id=e.plan_id,publication_facts={'DATA_LIMITED':True,'source_data_limits':detail})
    catalog=deepcopy(kwargs['replacements']['EVIDENCE_CATALOG'])
    catalog['categories'].append('PLAN')
    catalog['items'].append(dict(id='MORNING:PLAN:'+e.plan_id,category='PLAN',value=fact))
    kwargs['replacements']=dict(kwargs['replacements'],EVIDENCE_CATALOG=catalog,PLANS=[fact])
    return kwargs,seen,raw


def test_a6_exact_plan_fact_and_each_source_gap_explicit_review_allow_probe(tmp_path):
    kwargs,seen,raw=limited_fixture(tmp_path)
    raw['envelope_adjustments'][0].update(reason_code='DATA_LIMITED_REVIEWED',
        supporting_refs=['MORNING:PLAN:p1'],what_would_change_my_mind=['已逐项审核：'+';'.join(GAPS)])
    # Existing authored capacity arithmetic is independent, use its valid bound.
    raw['capacity_for_tomorrow'].update(plans_acceptable=1,cluster_quota={'bank':1})
    result=run_risk_premarket(**kwargs)
    assert result.status=='READY' and result.envelopes[0].eligible
    assert result.envelopes[0].risk_unit=='PROBE'
    runtime=json.loads(seen[0]['messages'][-1]['content'].removeprefix('RUNTIME_INPUT\n'))
    assert runtime['server_output_contract']['data_limited_plan_reviews']['p1']['reason_codes']==GAPS
    assert run_risk_premarket(**kwargs)==result and len(seen)==1


@pytest.mark.parametrize('mode',['silent_keep','partial_review','wrong_ref','offline','bad_schema','missing_fact'])
def test_missing_explicit_a6_review_never_falls_back_to_entry(tmp_path,mode):
    kwargs,seen,raw=limited_fixture(tmp_path,offline=mode=='offline',invalid=mode=='bad_schema')
    raw['capacity_for_tomorrow'].update(plans_acceptable=1,cluster_quota={'bank':1})
    if mode in ('partial_review','wrong_ref'):
        raw['envelope_adjustments'][0].update(reason_code='DATA_LIMITED_REVIEWED',
            supporting_refs=['C1'] if mode=='wrong_ref' else ['MORNING:PLAN:p1'],
            what_would_change_my_mind=[';'.join(GAPS) if mode=='wrong_ref' else GAPS[0]])
    if mode=='missing_fact':
        kwargs['replacements']['EVIDENCE_CATALOG']['items'][-1]['value']['publication_facts'].clear()
    result=run_risk_premarket(**kwargs)
    assert not result.envelopes[0].eligible
    assert result.envelopes[0].risk_unit=='NONE' and result.envelopes[0].max_shares_cap==0
    assert 'A6_DATA_LIMITED_REVIEW_REQUIRED' in result.envelopes[0].blocked_reasons
    assert run_risk_premarket(**kwargs)==result and len(seen)==1
    with sqlite3.connect(tmp_path/'team.db') as db:
        audit=json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',
            ('a6-source-gap-review:'+kwargs['meeting_id'],)).fetchone()[0])
    assert audit['reviewed_plan_ids']==[] and audit['blocked_plan_ids']==['p1']


def test_a6_missing_meeting_gate_does_not_block_unlimited_other_plan(tmp_path):
    kwargs,_,_=limited_fixture(tmp_path)
    limited=kwargs['envelopes'][0];normal=replace(limited,plan_id='normal',degradation_tags=())
    bounded=restrict_unreviewed(tuple(conservative_envelope(e,consecutive_failures=1) for e in (limited,normal)))
    assert not bounded[0].eligible and bounded[0].max_shares_cap==0
    assert bounded[1].eligible and bounded[1].risk_unit=='PROBE'
    assert restrict_unreviewed((limited,),reviewed_ids={'p1'})==(limited,)


def test_real_plan_projection_retains_gap_details_for_a6():
    payload=project_data_limited(dict(symbol='000001.SZ',source_run_id='regen',primary_theme='BANK'),
        source_gap_detail(snapshot(),'000001.SZ'))
    row=dict(plan_id='p1',symbol='000001.SZ',lane_id='lane_1',status='PENDING_MORNING_REVIEW',
        valid_from=None,expires_at='2026-10-22T15:00:00+08:00',payload_json=json.dumps(payload))
    facts=meeting_plan_facts([row])
    assert facts[0]['publication_facts']['source_data_limits']['reason_codes']==GAPS
    assert facts[0]['publication_facts']['risk_unit']=='PROBE'


def test_unreviewed_gap_gate_survives_fresh_quote_activation_and_conservative_refresh(tmp_path):
    from test_team_v2_envelope_refresh import setup
    from liangjian_funnel.runtime.team_v2_envelope_refresh import refresh_execution_envelopes
    plan,morning,args=setup(tmp_path)
    plan=replace(plan,risk_unit='PROBE',degradation_tags=('DATA_LIMITED',))
    morning=replace(morning,plan_risk_unit='PROBE',degradation_tags=('DATA_LIMITED',))
    blocked,=restrict_unreviewed((morning,))
    fresh,=refresh_execution_envelopes((plan,),morning_envelopes=(blocked,),consecutive_failures=1,**args)
    assert not fresh.eligible and fresh.max_shares_cap==0 and fresh.risk_unit=='NONE'
    assert 'A6_DATA_LIMITED_REVIEW_REQUIRED' in fresh.blocked_reasons


def test_a3_selection_gap_audit_is_written_before_empty_geometry_rejects(tmp_path,monkeypatch):
    from dataclasses import replace as changed
    from types import SimpleNamespace as NS
    from liangjian_funnel.workflow import WorkflowError
    app,result,data,_=publisher_fixture()
    app.settings=NS(workflow_output_dir=tmp_path,research_primary_lane_id='lane_1',publish_comparison_lanes=False)
    data['snapshot_manifest']['source_failures']={SYMBOL:GAPS}
    audit=NS(stage='A3',thinking_variant='team_v2',output={},audit_path=None)
    lane=changed(result.lanes[0],stages=(audit,))
    result=changed(result,lanes=(lane,))
    monkeypatch.setattr('liangjian_funnel.pipeline.team_v2_plan_geometry.project_team_plan_geometry',
        lambda *a,**k:dict(status='BLOCKED',audit={'reason_code':'INVALID_GEOMETRY'}))
    with pytest.raises(WorkflowError,match='REGEN_NO_GEOMETRY_VALID_PLANS_KEEP_OLD'):
        WorkflowApplication._publish_plans(app,result,now=AT,slot='close',snapshot_data=data,
            regeneration=NS(old_plan_ids=('old',),run_id=result.run_id))
    receipt=json.loads((tmp_path/'runs'/f'{result.run_id}-source-data-limit-audit.json').read_text(encoding='utf-8'))
    assert receipt['selections'][0]['source_data_limits']['reason_codes']==GAPS
    assert app.store.batch==[]
