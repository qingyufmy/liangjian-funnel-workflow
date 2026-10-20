"""Actual A6 client transport mocked; durable failure latch and original JSON."""
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository,_ENVELOPE_KEYS
from liangjian_funnel.pipeline.team_v2_cluster_quota import build_cluster_quota_context
from liangjian_funnel.runtime.team_v2_risk_session import run_risk_premarket
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.runtime.risk_engine import RiskLimits
from liangjian_funnel.settings import Settings
from test_team_v2_risk_schema import output,CAT,CTX
from test_team_v2_store import AT

MODEL='deepseek-v4-pro-0813'


def fixture(tmp_path,*,invalid=False,offline=False):
    repo=TeamPromptRepository(Path(__file__).resolve().parents[1]/'prompts/team_v2')
    required={key:'FROZEN' for key in _ENVELOPE_KEYS}
    required.update(stage_id='A6',run_id='risk-local',model_name=MODEL,
        source_trade_date='2026-10-16',execution_trade_date='2026-10-19')
    values={key:[] for key in repo.document('A6').placeholders};values['EVIDENCE_CATALOG']=CAT
    raw=output();raw['envelope']=required
    if invalid:raw['envelope_adjustments'][0]['adjustment']='LOOSEN'
    seen=[]
    def handler(request):
        seen.append(json.loads(request.read()))
        if offline:raise httpx.ConnectError('PRIVATE_PROVIDER_DETAILS',request=request)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    kwargs=dict(repository=repo,client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1),
        journal=ModelJudgmentJournal(tmp_path/'model.db'),store=TeamMeetingStore(tmp_path/'team.db'),
        meeting_id='risk:20261019',call_id='A6:premarket:20261019',account_id='paper',run_id='risk-local',model=MODEL,
        trade_date='2026-10-19',source_trade_date='2026-10-16',clock=lambda:AT,required_envelope=required,
        replacements=values,envelopes=tuple(replace(e,purpose='PLANNING') for e in CTX['envelopes']),
        engine_utilization_cap=Decimal('75'),capacity_slots=3,cluster_caps={'bank':2},unready_plan_ids=set(),
        limits=RiskLimits(),timeout_seconds=30,max_output_tokens=4096,allow_model_call=True)
    return kwargs,seen,raw


def test_real_client_schema_and_meeting_state_restart_without_recall(tmp_path):
    kwargs,seen,_=fixture(tmp_path)
    result=run_risk_premarket(**kwargs)
    assert result.status=='READY' and result.envelopes==kwargs['envelopes']
    kwargs['store']=TeamMeetingStore(tmp_path/'team.db')
    assert run_risk_premarket(**kwargs)==result and len(seen)==1
    assert len(kwargs['journal'].summary())==1


@pytest.mark.parametrize('invalid,offline',[(True,False),(False,True)])
def test_failure_degrades_then_latches_second_failure_even_after_restart(tmp_path,invalid,offline):
    kwargs,seen,raw=fixture(tmp_path,invalid=invalid,offline=offline)
    first=run_risk_premarket(**kwargs)
    assert first.status=='DEGRADED' and first.envelopes[0].risk_unit=='PROBE'
    assert run_risk_premarket(**kwargs)==first and len(seen)==1
    kwargs.update(meeting_id='risk:second',call_id='A6:second',clock=lambda:AT+timedelta(seconds=40),
        store=TeamMeetingStore(tmp_path/'team.db'))
    second=run_risk_premarket(**kwargs)
    assert second.status=='HALT' and second.consecutive_failures==2
    assert not second.envelopes[0].eligible
    if invalid:
        raw['envelope_adjustments'][0]['adjustment']='KEEP'
        kwargs.update(meeting_id='risk:third',call_id='A6:third',clock=lambda:AT+timedelta(seconds=80),
            store=TeamMeetingStore(tmp_path/'team.db'))
        assert run_risk_premarket(**kwargs).status=='HALT'
        with sqlite3.connect(tmp_path/'model.db') as db:
            original=json.loads(db.execute('SELECT output_json FROM model_calls WHERE call_id=?',('A6:premarket:20261019',)).fetchone()[0])
            assert original['envelope_adjustments'][0]['adjustment']=='LOOSEN'
    assert b'PRIVATE_PROVIDER_DETAILS' not in (tmp_path/'team.db').read_bytes()


@pytest.mark.parametrize('offline',[False,True])
def test_capture_before_meeting_commit_crash_reuses_output_and_counts_once(tmp_path,monkeypatch,offline):
    kwargs,seen,_=fixture(tmp_path,invalid=not offline,offline=offline)
    def fail(**_):raise sqlite3.OperationalError('ISOLATED_MEETING_COMMIT_FAULT')
    monkeypatch.setattr(kwargs['store'],'record_premarket',fail)
    with pytest.raises(sqlite3.OperationalError):run_risk_premarket(**kwargs)
    kwargs['store']=TeamMeetingStore(tmp_path/'team.db')
    result=run_risk_premarket(**kwargs)
    assert result.consecutive_failures==1 and len(seen)==1


def test_changed_server_domain_cannot_reuse_call_or_meeting(tmp_path):
    kwargs,seen,_=fixture(tmp_path);run_risk_premarket(**kwargs)
    kwargs['engine_utilization_cap']=Decimal('60')
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):run_risk_premarket(**kwargs)
    assert len(seen)==1


def test_no_authority_or_bad_date_stops_before_http(tmp_path):
    kwargs,seen,_=fixture(tmp_path);kwargs['allow_model_call']=False
    with pytest.raises(ValueError,match='NOT_AUTHORIZED'):run_risk_premarket(**kwargs)
    kwargs['allow_model_call']=True;kwargs['clock']=lambda:AT.replace(hour=10)
    with pytest.raises(ValueError,match='A6_PREMARKET_WINDOW_INVALID'):run_risk_premarket(**kwargs)
    assert seen==[]


def quota_fixture(tmp_path,**options):
    kwargs,seen,raw=fixture(tmp_path,**options)
    kwargs['quota_context']=build_cluster_quota_context(engine_slots=3,cluster_caps={'bank':2},
        plan_clusters={'p1':'bank'},plan_priority_order=['p1'])
    return kwargs,seen,raw


def test_quota_normalization_keeps_provider_invalid_and_separate_effective_audit(tmp_path):
    kwargs,seen,raw=quota_fixture(tmp_path)
    before=json.dumps(raw,sort_keys=True)
    result=run_risk_premarket(**kwargs)
    assert result.status=='READY' and result.envelopes==kwargs['envelopes']
    assert json.dumps(raw,sort_keys=True)==before and len(seen)==1
    # Frozen request explicitly exposes deterministic counts, bounds and order.
    assert 'server_cluster_capacity' in seen[0]['messages'][0]['content']
    assert 'cluster_plan_counts' in seen[0]['messages'][0]['content']
    with sqlite3.connect(tmp_path/'model.db') as db:
        validated,original=db.execute('SELECT validated,output_json FROM model_calls').fetchone()
        assert validated==0 and json.loads(original)==raw
    with sqlite3.connect(tmp_path/'team.db') as db:
        source,audit_json=db.execute('SELECT source_sha256,body_json FROM control_minute_context WHERE decision_id=?',
            ('a6-quota-normalization:'+kwargs['meeting_id'],)).fetchone()
        audit=json.loads(audit_json)
    assert audit['code']=='SERVER_NORMALIZED_CLUSTER_QUOTA'
    assert audit['provider_validated'] is False and audit['provider_issues']
    assert audit['status']=='VALIDATED' and audit['effective_validation_issues']==[]
    assert audit['effective_raw']['capacity_for_tomorrow']['cluster_quota']=={'bank':1}
    assert audit['original_cluster_quota']=={'bank':2}
    assert audit['provider_raw_sha256']!=audit['effective_raw_sha256']
    assert len(source)==64
    kwargs['store']=TeamMeetingStore(tmp_path/'team.db')
    assert run_risk_premarket(**kwargs)==result and len(seen)==1


def test_captured_invalid_quota_after_commit_crash_revalidated_without_recall(tmp_path,monkeypatch):
    kwargs,seen,_=quota_fixture(tmp_path)
    def fail(**_):raise sqlite3.OperationalError('ISOLATED_MEETING_COMMIT_FAULT')
    monkeypatch.setattr(kwargs['store'],'record_premarket',fail)
    with pytest.raises(sqlite3.OperationalError):run_risk_premarket(**kwargs)
    kwargs['store']=TeamMeetingStore(tmp_path/'team.db')
    assert run_risk_premarket(**kwargs).status=='READY' and len(seen)==1
    with sqlite3.connect(tmp_path/'model.db') as db:
        assert db.execute('SELECT validated FROM model_calls').fetchone()[0]==0


def test_quota_projection_does_not_repair_other_bad_model_judgments(tmp_path):
    kwargs,seen,raw=quota_fixture(tmp_path,invalid=True)
    assert run_risk_premarket(**kwargs).status=='DEGRADED'
    with sqlite3.connect(tmp_path/'team.db') as db:
        audit=json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',
            ('a6-quota-normalization:'+kwargs['meeting_id'],)).fetchone()[0])
    assert audit['status']=='BLOCKED' and 'A6_ADJUSTMENT_INVALID' in audit['effective_validation_issues']
    assert audit['effective_raw']['envelope_adjustments'][0]['adjustment']=='LOOSEN'
    assert len(seen)==1


def test_changed_quota_context_conflicts_before_any_provider_recall(tmp_path):
    kwargs,seen,_=quota_fixture(tmp_path);run_risk_premarket(**kwargs)
    kwargs['quota_context']=build_cluster_quota_context(engine_slots=3,cluster_caps={'bank':2},
        plan_clusters={'p1':'bank'},plan_priority_order=['p1'])
    kwargs['quota_context']['cluster_upper_bounds']['bank']=2
    with pytest.raises(ValueError,match='SERVER_CONTEXT_INVALID'):run_risk_premarket(**kwargs)
    kwargs['quota_context']=build_cluster_quota_context(engine_slots=3,cluster_caps={'bank':2},
        plan_clusters={'outside':'bank'},plan_priority_order=['outside'])
    with pytest.raises(ValueError,match='SERVER_CONTEXT_MISMATCH'):run_risk_premarket(**kwargs)
    assert len(seen)==1
