"""Actual client + schema + durable journals + real engine/paper, HTTP mocked."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import json
import sqlite3

import httpx
import pytest
import liangjian_funnel.runtime.team_v2_trader_session as session_module

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository, _ENVELOPE_KEYS
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.runtime.team_v2_trader_session import run_trader_session
from liangjian_funnel.settings import Settings
from liangjian_paper_broker.matching import PaperRules
from test_team_v2_execution import fixture as execution_fixture,pm_result
from test_team_v2_paper_ledger import bar, drive, stamp
from test_team_v2_trader_schema import output, CAT, CONTEXT

MODEL='deepseek-v4-pro-0813'


def fixture(tmp_path, *, invalid=False, offline=False, moved=False, mode='control'):
    broker,args=execution_fixture(tmp_path)
    required={key:'FROZEN' for key in _ENVELOPE_KEYS}
    required.update(stage_id='A4',model_name=MODEL,run_id='trader-local',
        source_trade_date='2026-10-19',execution_trade_date='2026-10-19')
    repo=TeamPromptRepository(Path(__file__).resolve().parents[1]/'prompts/team_v2')
    values={key:[] for key in repo.document('A4').placeholders};values['EVIDENCE_CATALOG']=CAT
    raw=output();raw['envelope']=required
    if invalid:raw['signals'][0]['shares']=10000
    seen=[];clock_value=[stamp()]
    broker.clock=lambda:clock_value[0]
    def handler(request):
        seen.append(json.loads(request.read()))
        if moved:clock_value[0]=stamp(tm='10:01:01')
        if offline:raise httpx.ConnectError('PRIVATE_PROVIDER_DETAILS',request=request)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    kwargs=dict(repository=repo,client=OpenAICompatibleModelClient(settings,
        transport=httpx.MockTransport(handler),max_attempts=1),
        journal=ModelJudgmentJournal(tmp_path/'models.db'),store=TeamMeetingStore(tmp_path/'team.db'),
        broker=broker,decision_id='d1',minute_snapshot_id='minute-1000',call_id='A4:d1',
        account_id='paper',run_id='trader-local',model=MODEL,trade_date='2026-10-19',
        source_trade_date='2026-10-19',clock=lambda:clock_value[0],decision_at=stamp(),
        closed_5m_end=stamp(),mark_as_of=stamp(),required_envelope=required,replacements=values,
        active_plan_ids={'p1'},all_plans=args['all_plans'],envelopes={'p1':args['envelope']},
        held_plan_ids={},position_snapshot=[],tech_reference=CONTEXT['tech_reference'],limits=args['limits'],
        regime='TREND_MAINLINE',live_market_ready=True,snapshot_ready=True,mode=mode,calendar=args['calendar'],
        timeout_seconds=30,max_output_tokens=4096,allow_model_call=True)
    kwargs['store'].record_pm(meeting_id='pm-test',source_call_id='pm-test',account_id='paper',
        trade_date='2026-10-19',input_sha256='b'*64,result=pm_result(args['all_plans']))
    kwargs['pm_meeting_id']='pm-test'
    return kwargs,seen,raw


def test_real_client_to_guard_risk_paper_and_restart_no_second_call(tmp_path):
    kwargs,seen,_=fixture(tmp_path)
    kwargs['replacements']['TIME_RULES']={'TIGHTEN_AFTER':'14:00','POST_CLOSE_FIXED_PRICE':False}
    frozen=deepcopy(kwargs['replacements'])
    result=run_trader_session(**kwargs)
    assert result['status']=='EVALUATED' and result['model_validated']
    assert result['admissions'][0]['dispatched']
    # PROBE hint tightens the standard risk budget; model supplies no shares.
    assert result['admissions'][0]['judgment']['risk_unit']=='PROBE'
    assert result['admissions'][0]['judgment']['shares']==15000
    fills=drive(kwargs['broker'],bar(volume_shares=300000),PaperRules('2026-10-19'))
    assert fills[0]['qty']==15000 and kwargs['broker'].reconcile()['status']=='MATCH'
    kwargs['journal']=ModelJudgmentJournal(tmp_path/'models.db')
    kwargs['store']=TeamMeetingStore(tmp_path/'team.db')
    assert run_trader_session(**kwargs)==result and len(seen)==1
    assert kwargs['replacements']==frozen
    assert 'TIGHTEN_AFTER' in seen[0]['messages'][0]['content']
    assert '"risk_budget":' not in seen[0]['messages'][0]['content']
    assert '"max_shares_cap":' not in seen[0]['messages'][0]['content']
    with sqlite3.connect(tmp_path/'team.db') as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()==[]


def test_frozen_replay_keeps_real_capture_time_separate_from_historical_execution(tmp_path):
    kwargs,seen,_=fixture(tmp_path,mode='record')
    actual=stamp(day='20',tm='08:15:12')
    kwargs.update(capture_clock=lambda:actual)
    kwargs['replacements']['MINUTE_SOURCE']={
        'classification':'POST_HOC_FROZEN_CORRECTNESS_REPLAY','quote_basis':'REPLAY_BAR_DERIVED'}
    result=run_trader_session(**kwargs)
    assert result['model_validated'] and result['trade_date']=='2026-10-19' and len(seen)==1
    with sqlite3.connect(tmp_path/'models.db') as db:
        row=db.execute('SELECT execution_trade_date,captured_at FROM model_calls').fetchone()
    assert row==('2026-10-19',actual.isoformat())
    assert not kwargs['broker'].get_orders('2026-10-19')


def test_capture_clock_override_cannot_masquerade_as_natural_production(tmp_path):
    kwargs,seen,_=fixture(tmp_path)
    with pytest.raises(ValueError,match='TRADER_REPLAY_CAPTURE_CONTEXT_REQUIRED'):
        run_trader_session(**kwargs,capture_clock=kwargs['clock'])
    assert not seen and not kwargs['broker'].get_orders('2026-10-19')


@pytest.mark.parametrize('invalid,offline',[(True,False),(False,True)])
def test_invalid_or_failed_model_never_defaults_to_buy(tmp_path,invalid,offline):
    kwargs,seen,_=fixture(tmp_path,invalid=invalid,offline=offline)
    result=run_trader_session(**kwargs)
    assert result['status']=='TRADER_DEGRADED_NO_ACTION' and result['admissions']==[]
    assert kwargs['broker'].get_orders('2026-10-19')==()
    assert run_trader_session(**kwargs)==result and len(seen)==1
    if invalid:
        with sqlite3.connect(tmp_path/'models.db') as db:
            assert json.loads(db.execute('SELECT output_json FROM model_calls').fetchone()[0])['signals'][0]['shares']==10000
    assert b'PRIVATE_PROVIDER_DETAILS' not in (tmp_path/'team.db').read_bytes()


@pytest.mark.parametrize('offline',[False,True])
def test_crash_after_paper_commit_resumes_no_provider_or_order_duplicate(tmp_path,monkeypatch,offline):
    kwargs,seen,_=fixture(tmp_path,offline=offline)
    def broken(**_):raise sqlite3.OperationalError('ISOLATED_COMMIT_FAULT')
    monkeypatch.setattr(kwargs['store'],'record_trader',broken)
    with pytest.raises(sqlite3.OperationalError):run_trader_session(**kwargs)
    assert len(kwargs['broker'].get_orders('2026-10-19'))==(0 if offline else 1)
    kwargs['store']=TeamMeetingStore(tmp_path/'team.db')
    result=run_trader_session(**kwargs)
    assert result['status']==('TRADER_DEGRADED_NO_ACTION' if offline else 'EVALUATED') and len(seen)==1
    assert len(kwargs['broker'].get_orders('2026-10-19'))==(0 if offline else 1)


def test_capture_before_any_admission_crash_resumes_original_vote(tmp_path,monkeypatch):
    kwargs,seen,_=fixture(tmp_path)
    original=session_module.admit_trader_signal
    def broken(**_):raise sqlite3.OperationalError('ISOLATED_PRE_ADMISSION_FAULT')
    monkeypatch.setattr(session_module,'admit_trader_signal',broken)
    with pytest.raises(sqlite3.OperationalError):run_trader_session(**kwargs)
    assert kwargs['broker'].get_orders('2026-10-19')==()
    # Changing the provider ID after capture may not evade the decision claim.
    with pytest.raises(ValueError,match='TRADER_SESSION_BINDING_CONFLICT'):
        run_trader_session(**(kwargs|dict(call_id='NEW_PROVIDER_CALL')))
    monkeypatch.setattr(session_module,'admit_trader_signal',original)
    assert run_trader_session(**kwargs)['admissions'][0]['dispatched'] and len(seen)==1


def test_record_mode_still_journals_vote_but_never_places_order(tmp_path):
    kwargs,seen,_=fixture(tmp_path,mode='record')
    result=run_trader_session(**kwargs)
    assert result['model_validated'] and not result['admissions'][0]['dispatched']
    assert kwargs['broker'].get_orders('2026-10-19')==() and len(seen)==1


def test_model_completion_crosses_minute_cannot_use_old_quote(tmp_path):
    kwargs,_,_=fixture(tmp_path,moved=True)
    result=run_trader_session(**kwargs)
    assert result['model_validated'] and not result['admissions'][0]['dispatched']
    assert 'TRADER_EXECUTION_CLOCK_MOVED' in result['admissions'][0]['judgment']['reason_codes']


def test_immutable_context_cannot_be_changed_after_capture(tmp_path,monkeypatch):
    kwargs,seen,_=fixture(tmp_path)
    def broken(**_):raise sqlite3.OperationalError('ISOLATED_COMMIT_FAULT')
    monkeypatch.setattr(kwargs['store'],'record_trader',broken)
    with pytest.raises(sqlite3.OperationalError):run_trader_session(**kwargs)
    kwargs['store']=TeamMeetingStore(tmp_path/'team.db')
    kwargs['all_plans']={'p1':replace(kwargs['all_plans']['p1'],price='10.01')}
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):run_trader_session(**kwargs)
    assert len(seen)==1


def test_authorization_tick_and_scope_invalid_before_http(tmp_path):
    kwargs,seen,_=fixture(tmp_path);kwargs['allow_model_call']=False
    with pytest.raises(ValueError,match='NOT_AUTHORIZED'):run_trader_session(**kwargs)
    kwargs['allow_model_call']=True;kwargs['held_plan_ids']={'000002.SZ':'unknown'}
    with pytest.raises(ValueError,match='TRADER_SERVER_CONTEXT_INVALID'):run_trader_session(**kwargs)
    assert seen==[]


def test_inflight_model_claim_is_not_retried_or_falsely_completed(tmp_path):
    kwargs,seen,_=fixture(tmp_path)
    kwargs['journal'].claim(kwargs['call_id'],'OWNED_BY_OTHER_WORKER')
    with pytest.raises(ValueError,match='MODEL_CALL_BINDING_CONFLICT'):run_trader_session(**kwargs)
    assert seen==[] and kwargs['broker'].get_orders('2026-10-19')==()
    with sqlite3.connect(tmp_path/'team.db') as db:
        assert db.execute('SELECT count(*) FROM trader_sessions').fetchone()[0]==0


def test_position_exit_is_bound_to_held_plan_and_does_not_need_old_plan_activation(tmp_path):
    kwargs,seen,raw=fixture(tmp_path)
    run_trader_session(**kwargs)
    drive(kwargs['broker'],bar(volume_shares=300000),PaperRules('2026-10-19'))
    kwargs['broker'].clock=lambda:stamp('20')
    kwargs.update(decision_id='d2',call_id='A4:d2',minute_snapshot_id='minute-nextday',
        trade_date='2026-10-20',source_trade_date='2026-10-20',clock=lambda:stamp('20'),
        decision_at=stamp('20'),mark_as_of=stamp('20'),closed_5m_end=stamp('20'),
        active_plan_ids=set(),tech_reference={},held_plan_ids={'000001.SZ':'p1'},
        position_snapshot=kwargs['broker'].get_positions(),
        envelopes={'p1':replace(kwargs['envelopes']['p1'],trade_date='2026-10-20')})
    kwargs['calendar'].is_trading_day=lambda day:day.isoformat()=='2026-10-20'
    kwargs['required_envelope'].update(source_trade_date='2026-10-20',execution_trade_date='2026-10-20')
    raw.update(decision_id='d2',minute_snapshot_id='minute-nextday',signals=[],position_actions=[dict(
        symbol='000001.SZ',action='SELL_SIGNAL',reason_code='STRUCTURE_FAILED',
        supporting_refs=['M1'],opposing_refs=['T1'],distance_to_stop_note='risk line broken')])
    result=run_trader_session(**kwargs)
    assert result['model_validated'] and result['admissions'][0]['order_side']=='SELL'
    assert result['admissions'][0]['judgment']['shares']==15000 and len(seen)==2
