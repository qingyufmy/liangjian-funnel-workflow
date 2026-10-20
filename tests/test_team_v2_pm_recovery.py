from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
import sqlite3

import pytest

from test_team_v2_pm_session import fixture
from liangjian_funnel.runtime.team_v2_pm_session import run_pm_premarket
from liangjian_funnel.runtime.team_v2_pm_policy import (
    derive_pm_fallback,project_pm_values,priority_order,pm_output_budget,DEFAULT_POLICY)
from liangjian_funnel.runtime.team_v2_allocation import compile_pm_allocation


def test_transport_failure_is_safe_durable_observable_and_not_retried(tmp_path):
    kwargs,seen=fixture(tmp_path,offline=True)
    result=run_pm_premarket(**kwargs)
    assert result['status']=='PM_DEGRADED_NO_ALLOCATION'
    with sqlite3.connect(tmp_path/'model.sqlite3') as db:
        metrics=json.loads(db.execute('SELECT body_json FROM model_call_telemetry').fetchone()[0])
        assert metrics['status']=='TRANSPORT_FAILED'
        assert metrics['exception_class']=='ModelNetworkError'
        assert metrics['provider_attempts']==1 and metrics['elapsed_ms']>=0
        assert metrics['input_chars']>0 and metrics['input_bytes']>=metrics['input_chars']
        assert metrics['output_chars'] is None
    assert b'PRIVATE_PROVIDER_DETAILS' not in (tmp_path/'model.sqlite3').read_bytes()
    assert run_pm_premarket(**kwargs)==result and len(seen)==1


def test_explicit_valid_a6_failure_fallback_is_probe_owned_and_alert_once(tmp_path):
    kwargs,seen=fixture(tmp_path,offline=True)
    kwargs.update(a6_validated=True,fallback_priority_order=('p1',),max_new_entries=3)
    result=run_pm_premarket(**kwargs)
    assert result['status']=='READY' and result['model_validated'] is False
    assert result['decision_authority']=='SERVER_FALLBACK_NOT_MODEL_VALIDATED'
    assert result['plan_allocations'][0]['intended_risk_unit']=='PROBE'
    assert result['slot_plan'][0]['backup_plan_id']==''
    assert compile_pm_allocation(result,account_id='paper',trade_date=kwargs['trade_date'],
        meeting_id=kwargs['meeting_id'],input_sha256='frozen').allocations==(('p1','PROBE',1),)
    assert run_pm_premarket(**kwargs)==result and len(seen)==1
    with sqlite3.connect(tmp_path/'team.sqlite3') as db:
        assert db.execute("SELECT count(*) FROM team_notifications WHERE kind='ALERT'").fetchone()[0]==1
    assert b'PRIVATE_PROVIDER_DETAILS' not in (tmp_path/'team.sqlite3').read_bytes()


@pytest.mark.parametrize('enabled,valid_a6',[(False,True),(True,False)])
def test_disabled_or_unproven_a6_does_not_allocate(tmp_path,enabled,valid_a6):
    kwargs,_=fixture(tmp_path,offline=True)
    kwargs.update(fallback_enabled=enabled,a6_validated=valid_a6,fallback_priority_order=('p1',))
    assert run_pm_premarket(**kwargs)['slot_plan']==[]


def test_fallback_respects_existing_envelope_priority_slots_cluster_and_daily_entries(tmp_path):
    kwargs,_=fixture(tmp_path)
    base=kwargs['envelopes'][0]
    rows=tuple(replace(base,plan_id='p'+str(i),symbol='s'+str(i),eligible=i!=4) for i in range(1,7))
    clusters={e.plan_id:'a' if int(e.plan_id[1:])<=3 else 'b' for e in rows}
    result=derive_pm_fallback(envelopes=rows,plan_clusters=clusters,
        priority=['p4','p3','p2','p1','p6','p5'],free_slots=6,max_new_entries=3,
        a6_validated=True,utilization_cap=50)
    assert [s['primary_plan_id'] for s in result['slot_plan']]==['p3','p2','p6']
    assert all(s['intended_risk_unit']=='PROBE' for s in result['plan_allocations'])
    assert len(derive_pm_fallback(envelopes=rows,plan_clusters=clusters,
        priority=list(clusters),free_slots=1,max_new_entries=3,a6_validated=True,
        utilization_cap=50)['slot_plan'])==1
    assert derive_pm_fallback(envelopes=rows,plan_clusters=clusters,
        priority=list(clusters),free_slots=6,max_new_entries=3,a6_validated=True,
        utilization_cap=0)['slot_plan']==[]


def test_compact_projection_is_one_catalogue_preserves_owner_and_does_not_mutate():
    plan=dict(plan_id='p1',symbol='s1',cluster='bank',original_payload_sha256='original',
        publication_facts={'trigger_low':10,'stop_level':9,'plan_priority_rank':1,
            'trading_plan_brief':'LARGE_PRIVATE_NARRATIVE'*100})
    values={'EVIDENCE_CATALOG':{'items':[{'id':'MORNING:PLAN:p1','category':'PLAN','value':plan}]},
        'PLANS_TODAY':[plan],'PLANS':[plan]}
    original=deepcopy(values);projected=project_pm_values(values)
    assert values==original
    p=projected['EVIDENCE_CATALOG']['items'][0]['value']
    assert p['plan_id']=='p1' and p['original_payload_sha256']=='original'
    assert p['publication_facts']=={'trigger_low':10,'stop_level':9,'plan_priority_rank':1}
    assert 'LARGE_PRIVATE_NARRATIVE' not in json.dumps(projected)
    assert pm_output_budget(DEFAULT_POLICY,33,393216)==20480
    assert priority_order([dict(plan_id='b',symbol='b',plan_priority_rank=2),
        dict(plan_id='a',symbol='a',plan_priority_rank=1)])==['a','b']
