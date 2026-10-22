from fixture_model_stream import sse_model_response
from copy import deepcopy
import json
import sqlite3
import time
import os
import hashlib
from pathlib import Path
from datetime import datetime

import httpx

import pytest

from test_team_v2_plan_review import session_fixture,response,ENVELOPE,DOMAIN,CATALOG
from liangjian_funnel.pipeline.team_v2_a3_retry import a3_validation_feedback,bind_a3_retry_feedback
from liangjian_funnel.pipeline.team_v2_plan_review_schema import validate_plan_review
from liangjian_funnel.runtime.team_v2_plan_review import plan_review_inputs


def test_specific_bad_row_actual_value_and_frozen_legal_cap_are_last_user_feedback(tmp_path):
    args,_=session_fixture(tmp_path);raw=response();raw['core_watch_pool'][0]['risk_unit']='STANDARD'
    old=deepcopy(raw);checked=validate_plan_review(raw,CATALOG,required_envelope=ENVELOPE,candidate_domain=DOMAIN)
    feedback=a3_validation_feedback(raw,CATALOG,checked.issues,required_envelope=ENVELOPE,candidate_domain=DOMAIN)
    row=feedback['row_violations'][0]
    assert row['actual_fields']['risk_unit']=='STANDARD' and row['legal_fields']['risk_unit']==['PROBE']
    request=args['repository'].prepare_messages('A3',plan_review_inputs(args['replacements'],DOMAIN,args['engine_decisions']),
        required_envelope=ENVELOPE,session='PLAN_REVIEW')
    updated=bind_a3_retry_feedback(request,feedback,correction_attempt=2)
    assert updated.input_hash!=request.input_hash and updated.prompt==request.prompt
    assert updated.messages[0]==request.messages[0] and updated.evidence_catalog_hash==request.evidence_catalog_hash
    runtime=json.loads(updated.messages[-1]['content'].removeprefix('RUNTIME_INPUT\n'))
    assert runtime['validation_feedback']['row_violations'][0]['actual_fields']['risk_unit']=='STANDARD'
    assert runtime['required_envelope']==ENVELOPE and runtime['correction_attempt']==2
    assert 'engine_eligible' not in updated.messages[-1]['content'] and raw==old
    with pytest.raises(ValueError,match='SCOPE_INVALID'):
        bind_a3_retry_feedback(updated,feedback,correction_attempt=3)


@pytest.mark.parametrize('attempt',[0,1,4,True])
def test_feedback_does_not_enable_additional_semantic_attempts(tmp_path,attempt):
    args,_=session_fixture(tmp_path)
    request=args['repository'].prepare_messages('A3',plan_review_inputs(args['replacements'],DOMAIN,args['engine_decisions']),
        required_envelope=ENVELOPE,session='PLAN_REVIEW')
    with pytest.raises(ValueError,match='SCOPE_INVALID'):
        bind_a3_retry_feedback(request,{'validation_errors':['A3_PLAN_PASS_RISK_INVALID']},correction_attempt=attempt)


def test_actual_transport_corrects_second_answer_without_r1_and_replays_no_http(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_plan_review as module
    args,seen=session_fixture(tmp_path)
    def handler(request):
        body=json.loads(request.read());seen.append(body)
        runtime=json.loads(body['messages'][-1]['content'].removeprefix('RUNTIME_INPUT\n'))
        raw=response()
        if len(seen)==1:raw['core_watch_pool'][0]['risk_unit']='STANDARD'
        else:
            row=runtime['validation_feedback']['row_violations'][0]
            assert row['actual_fields']['risk_unit']=='STANDARD'
            assert row['legal_fields']['risk_unit']==['PROBE']
            assert runtime['correction_attempt']==2
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    args['client'].transport=httpx.MockTransport(handler)
    monkeypatch.setattr(module,'recover_plan_review',lambda *a,**k:None)
    result=module.run_plan_review(**args)
    assert result['status']=='READY' and result['provider_validated'] is True
    assert result['server_projection'] is None and result['semantic_attempts']==2
    assert [row['valid'] for row in result['validation_history']]==[False,True]
    assert len(set(row['input_hash'] for row in result['validation_history']))==2
    assert module.run_plan_review(**args)==result and len(seen)==2
    assert seen[0]['messages'][0]==seen[1]['messages'][0]
    with sqlite3.connect(tmp_path/'models.sqlite3') as db:
        assert db.execute('SELECT validated FROM model_calls ORDER BY rowid').fetchall()==[(0,),(1,)]


def test_schema_attempt_timeouts_are_independent_and_clipped_to_parent_deadline(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_plan_review as module
    args,seen=session_fixture(tmp_path,invalid=True)
    clock=[100.0]
    monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    original=module._response
    observed=[]
    def respond(**kwargs):
        observed.append(kwargs['timeout_seconds']);result=original(**kwargs);clock[0]+=10
        return result
    monkeypatch.setattr(module,'_response',respond)
    result=module.run_plan_review(**args,deadline_monotonic=125)
    assert result['status']=='A3_REVIEW_DATA_GAP'
    assert observed==[25,15,5] and len(seen)==3
    assert result['semantic_attempts']==3


def test_deadline_final_projection_pins_last_actual_answer_not_uncalled_feedback(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_plan_review as module
    args,seen=session_fixture(tmp_path);raw=response();raw['core_watch_pool'][0]['risk_unit']='STANDARD'
    clock=[100.0];monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    def handler(request):
        seen.append(json.loads(request.read()));clock[0]=200
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    args['client'].transport=httpx.MockTransport(handler)
    result=module.run_plan_review(**args,deadline_monotonic=125)
    assert result['status']=='READY' and result['provider_validated'] is False and len(seen)==1
    with sqlite3.connect(tmp_path/'team.sqlite3') as db:
        pin=db.execute('SELECT source_sha256 FROM control_minute_context WHERE decision_id=?',
            (result['server_projection']['audit_context_key'],)).fetchone()[0]
    assert pin==result['validation_history'][0]['input_hash']


def test_expired_deadline_stops_new_call_but_can_replay_all_captures_after_commit_failure(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_plan_review as module
    args,seen=session_fixture(tmp_path,invalid=True)
    clock=[100.0];monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    record=module.PlanReviewRuntime.record
    monkeypatch.setattr(module.PlanReviewRuntime,'record',lambda *a,**k:(_ for _ in ()).throw(sqlite3.OperationalError('TEST')))
    with pytest.raises(sqlite3.OperationalError):module.run_plan_review(**args,deadline_monotonic=125)
    assert len(seen)==3
    clock[0]=200;monkeypatch.setattr(module.PlanReviewRuntime,'record',record)
    result=module.run_plan_review(**args,deadline_monotonic=125)
    assert result['status']=='A3_REVIEW_DATA_GAP' and len(seen)==3
    # A different review with no captures must not send after its deadline.
    args.update(review_id='other',call_id='other')
    result=module.run_plan_review(**args,deadline_monotonic=125)
    assert result['semantic_attempts']==0 and result['reason_codes']==['RESEARCH_DEADLINE_EXCEEDED']
    assert len(seen)==3


@pytest.mark.skipif(not os.environ.get('LIANGJIAN_A3_14A_ORIGINAL'),reason='Explicit local frozen 14a original not supplied')
def test_actual_14a_full16_original_corrected_with_r1_disabled(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_plan_review as module
    data=Path(os.environ['LIANGJIAN_A3_14A_ORIGINAL']).read_bytes()
    assert hashlib.sha256(data).hexdigest()=='63704fdab3d7f0c35edb9fe5498790cdb476ef7f2db0a73e0c8c004cbfdda6af'
    artifact=json.loads(data);source=artifact['A3_frozen_source'];raw=artifact['A3_actual_provider_raw']
    assert raw['core_watch_pool'][0]['symbol']=='000576.SZ' and raw['core_watch_pool'][0]['risk_unit']=='STANDARD'
    assert len(raw['core_watch_pool'])==16
    args,seen=session_fixture(tmp_path)
    args.update({key:source[key] for key in ('run_id','model','source_trade_date','trade_date',
        'required_envelope','replacements','candidate_domain','engine_decisions','timeout_seconds','max_output_tokens')})
    args.update(review_id='P011-real14a-correction',call_id='P011-real14a-correction:1',
        clock=lambda:datetime.fromisoformat('2026-10-19T23:20:00+08:00'))
    def handler(request):
        body=json.loads(request.read());seen.append(body)
        output=deepcopy(raw)
        if len(seen)>1:
            runtime=json.loads(body['messages'][-1]['content'].removeprefix('RUNTIME_INPUT\n'))
            rows=runtime['validation_feedback']['row_violations']
            assert len(rows)==1 and rows[0]['symbol']=='000576.SZ'
            assert rows[0]['actual_fields']['risk_unit']=='STANDARD' and rows[0]['legal_fields']['risk_unit']==['PROBE']
            output['core_watch_pool'][0]['risk_unit']='PROBE'
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(output)}}]},request=request)
    args['client'].transport=httpx.MockTransport(handler)
    monkeypatch.setattr(module,'recover_plan_review',lambda *a,**k:None)
    result=module.run_plan_review(**args)
    assert result['status']=='READY' and result['semantic_attempts']==2 and result['server_projection'] is None
    assert result['validation_history'][0]['reason_codes']==['A3_PLAN_PASS_RISK_INVALID']
    assert len(result['report']['core_watch_pool'])==16
    assert result['report']['core_watch_pool'][0]['risk_unit']=='PROBE'
    assert module.run_plan_review(**args)==result and len(seen)==2
