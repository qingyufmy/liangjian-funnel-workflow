"""Actual client HTTP mock, owned disk state; no production DB or supplier."""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.pipeline.team_v2_risk_schema import validate_risk_post_close
from liangjian_funnel.runtime.team_v2_eod import run_team_eod
from test_team_v2_pm_session import fixture as morning_fixture
from test_team_v2_pm_schema import close_response, CATALOG, REFS

SYMBOL='000001.SZ'


def risk_output(envelope):
    return dict(envelope=envelope,schema_version='a6-risk/1.0.0',session='POST_CLOSE',
        envelope_adjustments=[],exception_actions=[],
        utilization_cap=dict(cap_pct=60,basis='engine bound',**REFS),
        overnight_reviews=[dict(symbol=SYMBOL,pm_stance='HOLD',review='OVERRIDE_TO_REDUCE',reason_code='CLUSTER',**REFS)],
        capacity_for_tomorrow=dict(plans_acceptable=1,cluster_quota={'bank':1},risk_unit='PROBE',
            response_to_pm='bounded',notes=['capacity only'],**REFS),
        risk_event_attribution=[dict(event_id='event1',verdict='RULE_CORRECT',note='hard risk',**REFS)],
        handoff_notes=dict(to_a3=['reduce tomorrow'],to_a4=[],to_a5=['score override']),
        what_would_change_my_mind=['new facts'],unresolved_questions=[])


def check(raw,**changes):
    ctx=dict(required_envelope={},pm_stances={SYMBOL:'HOLD'},engine_utilization_cap=Decimal(75),
        capacity_slots=2,cluster_caps={'bank':2},event_ids={'event1'})
    return validate_risk_post_close(raw,CATALOG,**(ctx|changes))


def test_schema_exact_pm_event_scope_and_tightening_only():
    raw=risk_output({});saved=deepcopy(raw)
    assert check(raw).valid and raw==saved
    raw['overnight_reviews'][0].update(pm_stance='EXIT_TOMORROW',review='OVERRIDE_TO_REDUCE')
    assert not check(raw,pm_stances={SYMBOL:'EXIT_TOMORROW'}).valid


@pytest.mark.parametrize('kind',['extra','duplicate','missing','outside','pm_binding','cap','capacity',
    'event_outside','event_missing','event_duplicate','event_verdict','exception','adjustment','refs','nan','malformed'])
def test_schema_counterexamples_rejected_without_repair(kind):
    raw=risk_output({})
    if kind=='extra':raw['order_qty']=100
    elif kind=='duplicate':raw['overnight_reviews']*=2
    elif kind=='missing':raw['overnight_reviews']=[]
    elif kind=='outside':raw['overnight_reviews'][0]['symbol']='other'
    elif kind=='pm_binding':raw['overnight_reviews'][0]['pm_stance']='REDUCE_TOMORROW'
    elif kind=='cap':raw['utilization_cap']['cap_pct']=76
    elif kind=='nan':raw['utilization_cap']['cap_pct']=float('nan')
    elif kind=='capacity':raw['capacity_for_tomorrow']['plans_acceptable']=3
    elif kind=='event_outside':raw['risk_event_attribution'][0]['event_id']='other'
    elif kind=='event_missing':raw['risk_event_attribution']=[]
    elif kind=='event_duplicate':raw['risk_event_attribution']*=2
    elif kind=='event_verdict':raw['risk_event_attribution'][0]['verdict']='IGNORE'
    elif kind=='exception':raw['exception_actions']=[{}]
    elif kind=='adjustment':raw['envelope_adjustments']=[{}]
    elif kind=='refs':raw['overnight_reviews'][0]['opposing_refs']=[]
    elif kind=='malformed':raw['overnight_reviews']=[None]
    assert not check(raw).valid


def fixture(tmp_path,*,failure=None):
    base,_=morning_fixture(tmp_path)
    pm_envelope=deepcopy(base['required_envelope'])
    risk_envelope=deepcopy(pm_envelope);risk_envelope['stage_id']='A6'
    pm=close_response();pm['envelope']=pm_envelope
    risk=risk_output(risk_envelope)
    if failure=='pm_invalid':pm['overnight_stances'][0]['symbol']='outside'
    if failure=='risk_invalid':risk['overnight_reviews'][0]['review']='LOOSEN'
    seen=[]
    def handler(request):
        body=json.loads(request.read());runtime=json.loads(body['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        role=runtime['required_envelope']['stage_id'];seen.append((role,runtime,body['messages'][0]['content']))
        if failure==role+'_offline':raise httpx.ConnectError('SECRET_PROVIDER_TEXT',request=request)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(pm if role=='A3' else risk)}}]},request=request)
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from liangjian_funnel.settings import Settings
    client=OpenAICompatibleModelClient(Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path),
        transport=httpx.MockTransport(handler),max_attempts=1)
    kwargs={key:base[key] for key in ('repository','journal','store','account_id','run_id','model','trade_date','source_trade_date')}
    kwargs.update(client=client,meeting_id='eod:20261019',pm_call_id='pm-eod',risk_call_id='risk-eod',
        clock=lambda:datetime.fromisoformat('2026-10-19T16:30:00+08:00'),
        pm_required_envelope=pm_envelope,risk_required_envelope=risk_envelope,
        pm_replacements=base['replacements']|{'THEME_REVIEWS':[]},risk_replacements={key:[] for key in base['repository'].document('A6').placeholders},
        positions=[{'symbol':SYMBOL,'qty':100,'plan_id':'p1','cluster':'bank'}],
        risk_events=[{'event_id':'event1','reason_code':'HARD_STOP'}],catalog=CATALOG,
        engine_utilization_cap=Decimal(75),capacity_slots=2,cluster_caps={'bank':2},
        timeout_seconds=30,max_output_tokens=4096,allow_model_call=True)
    return kwargs,seen


def test_actual_client_pm_to_a6_atomic_override_and_restart(tmp_path):
    kwargs,seen=fixture(tmp_path);result=run_team_eod(**kwargs)
    assert result['status']=='READY' and result['overnight_actions'][0]['stance']=='REDUCE_TOMORROW'
    assert [row[0] for row in seen]==['A3','A6']
    assert 'PM_PROPOSAL=' in seen[1][2] and '"stance":"HOLD"' in seen[1][2]
    assert run_team_eod(**kwargs)==result and len(seen)==2
    with sqlite3.connect(kwargs['store'].path) as db:
        assert db.execute('SELECT count(*) FROM pm_override').fetchone()[0]==1
        original=json.loads(db.execute('SELECT result_json FROM pm_decisions').fetchone()[0])
        assert original['session']=='PM_POST_CLOSE' and original['overnight_stances'][0]['stance']=='HOLD'
        assert db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()==[]


def test_v1_research_fixture_keeps_original_decision_clock_and_actual_capture_clock(tmp_path):
    kwargs,seen=fixture(tmp_path)
    kwargs.update(account_id='FIXTURE_V1_NOT_TEAM_ACCOUNT',
        capture_clock=lambda:datetime.fromisoformat('2026-10-20T12:00:00+08:00'))
    for key in ('pm_replacements','risk_replacements'):
        kwargs[key]['ACCOUNT_SNAPSHOT']={'basis':'FIXTURE_FROM_V1_PAPER_NOT_TEAM_ACCOUNT'}
    assert run_team_eod(**kwargs)['status']=='READY' and len(seen)==2
    with sqlite3.connect(kwargs['journal'].path) as db:
        rows=db.execute('SELECT execution_trade_date,captured_at FROM model_calls').fetchall()
    assert len(rows)==2 and all(day=='2026-10-19' and at.startswith('2026-10-20') for day,at in rows)
    assert not (tmp_path/'paper.db').exists()


def test_normal_account_cannot_relabel_capture_as_a_research_fixture(tmp_path):
    kwargs,seen=fixture(tmp_path)
    kwargs['capture_clock']=lambda:datetime.fromisoformat('2026-10-20T12:00:00+08:00')
    with pytest.raises(ValueError,match='EOD_RESEARCH_CAPTURE_CONTEXT_REQUIRED'):run_team_eod(**kwargs)
    assert not seen


@pytest.mark.parametrize('failure',['pm_invalid','A3_offline','risk_invalid','A6_offline'])
def test_failures_not_synthetic_hold_or_executable_tomorrow_instruction(tmp_path,failure):
    kwargs,seen=fixture(tmp_path,failure=failure);result=run_team_eod(**kwargs)
    assert result['status']!='READY' and result['overnight_actions']==[]
    assert len(seen)==(1 if failure in {'pm_invalid','A3_offline'} else 2)
    assert run_team_eod(**kwargs)==result
    assert b'SECRET_PROVIDER_TEXT' not in kwargs['store'].path.read_bytes()
    with sqlite3.connect(kwargs['store'].path) as db:
        assert db.execute('SELECT count(*) FROM pm_override').fetchone()[0]==0


@pytest.mark.parametrize('failure',[None,'A6_offline'])
def test_final_commit_fault_reuses_captured_or_failed_calls(tmp_path,monkeypatch,failure):
    kwargs,seen=fixture(tmp_path,failure=failure)
    def broken(**_):raise sqlite3.OperationalError('EOD_COMMIT_FAULT')
    original=kwargs['store'].record_eod
    monkeypatch.setattr(kwargs['store'],'record_eod',broken)
    with pytest.raises(sqlite3.OperationalError):run_team_eod(**kwargs)
    monkeypatch.setattr(kwargs['store'],'record_eod',original)
    result=run_team_eod(**kwargs)
    assert result['status']==('READY' if failure is None else 'A6_EOD_UNREVIEWED') and len(seen)==2


def test_changed_source_account_and_early_clock_block_before_http(tmp_path):
    kwargs,seen=fixture(tmp_path);run_team_eod(**kwargs)
    kwargs['positions'][0]['qty']=200
    with pytest.raises(ValueError,match='EOD_BINDING_CONFLICT'):run_team_eod(**kwargs)
    kwargs,seen=fixture(tmp_path/'early');kwargs['clock']=lambda:datetime.fromisoformat('2026-10-19T14:30:00+08:00')
    with pytest.raises(ValueError,match='EOD_WINDOW_INVALID'):run_team_eod(**kwargs)
    assert seen==[]


def test_other_account_cannot_reuse_model_call_under_another_meeting(tmp_path):
    kwargs,seen=fixture(tmp_path);run_team_eod(**kwargs)
    kwargs.update(account_id='other',meeting_id='other-meeting')
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):run_team_eod(**kwargs)
    assert len(seen)==2


def test_mid_override_insert_failure_rolls_back_whole_review(tmp_path):
    kwargs,seen=fixture(tmp_path)
    with sqlite3.connect(kwargs['store'].path) as db:
        db.execute("CREATE TRIGGER fail_override BEFORE INSERT ON pm_override BEGIN SELECT RAISE(ABORT,'FAULT'); END")
    with pytest.raises(sqlite3.IntegrityError):run_team_eod(**kwargs)
    with sqlite3.connect(kwargs['store'].path) as db:
        assert db.execute('SELECT count(*) FROM eod_reviews').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM pm_override').fetchone()[0]==0
        db.execute('DROP TRIGGER fail_override')
    assert run_team_eod(**kwargs)['status']=='READY' and len(seen)==2


def test_empty_position_and_event_domains_are_real_zero_not_missing(tmp_path):
    assert check(risk_output({})).valid
    raw=risk_output({});raw['overnight_reviews']=[];raw['risk_event_attribution']=[]
    assert check(raw,pm_stances={},event_ids=set()).valid


@pytest.mark.parametrize('raw',[None,[],{}, {'overnight_reviews':[None]}])
def test_bad_json_shape_is_invalid(raw):
    assert not check(raw).valid
