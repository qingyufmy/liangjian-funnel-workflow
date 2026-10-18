"""Real existing HTTP client over a mock transport, no provider/model expense."""
from datetime import timedelta
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.runtime.team_v2_exception_session import run_risk_exception
from liangjian_funnel.runtime.team_v2_risk_exception import RiskExceptionRuntime
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from test_team_v2_risk_exception import response,targets
from test_team_v2_risk_session import fixture as morning_fixture


def fixture(tmp_path,*,offline=False,invalid=False):
    args,_,_=morning_fixture(tmp_path)
    for key in ('envelopes','unready_plan_ids'):args.pop(key)
    raw=response();raw['envelope']=args['required_envelope']
    if invalid:raw['exception_actions'].pop()
    seen=[];clock=[args['clock']().replace(hour=9,minute=36)];mono=[0.0]
    def handler(request):
        seen.append(json.loads(request.read()))
        if offline:raise httpx.ConnectError('PRIVATE_PROVIDER_DETAILS',request=request)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from liangjian_funnel.settings import Settings
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    args.update(client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1),
        meeting_id='risk:exception:20261019:0935',call_id='A6:exception:20261019:0935',
        clock=lambda:clock[0],targets=targets(),hard_halt=False,timeout_seconds=20,
        monotonic=lambda:mono[0],deadline_monotonic=47)
    return args,seen,raw,clock,mono


def test_real_exception_client_journal_exact_scope_single_call_and_restart(tmp_path):
    args,seen,_,_,_=fixture(tmp_path)
    result=run_risk_exception(**args)
    assert result['status']=='READY' and len(result['controls'])==2
    assert 'previous_a6_controls' in seen[0]['messages'][0]['content']
    args['store']=TeamMeetingStore(tmp_path/'team.db')
    assert run_risk_exception(**args)==result and len(seen)==1
    assert args['journal'].summary()[0]['session']=='INTRADAY_EXCEPTION'


@pytest.mark.parametrize('offline',[True,False])
def test_failed_or_invalid_call_is_not_recalled_or_double_counted_after_commit_crash(tmp_path,monkeypatch,offline):
    args,seen,_,_,_=fixture(tmp_path,offline=offline,invalid=not offline)
    real=RiskExceptionRuntime.record
    def fail(*a,**kw):raise sqlite3.OperationalError('TEST_COMMIT_FAULT')
    monkeypatch.setattr(RiskExceptionRuntime,'record',fail)
    with pytest.raises(sqlite3.OperationalError):run_risk_exception(**args)
    monkeypatch.setattr(RiskExceptionRuntime,'record',real)
    args['store']=TeamMeetingStore(tmp_path/'team.db')
    result=run_risk_exception(**args)
    assert result['status']=='DEGRADED' and result['consecutive_failures']==1 and len(seen)==1
    assert b'PRIVATE_PROVIDER_DETAILS' not in (tmp_path/'team.db').read_bytes()


def test_second_failure_latches_across_morning_and_exception_and_skips_future_http(tmp_path):
    args,seen,raw,clock,_=fixture(tmp_path,invalid=True)
    assert run_risk_exception(**args)['status']=='DEGRADED'
    clock[0]+=timedelta(minutes=5);args.update(meeting_id='second',call_id='A6:second')
    assert run_risk_exception(**args)['status']=='HALT'
    clock[0]+=timedelta(minutes=5);args.update(meeting_id='third',call_id='A6:third')
    raw['exception_actions']=response()['exception_actions']
    assert run_risk_exception(**args)['status']=='HALT' and len(seen)==2


def test_not_enough_global_time_records_conservative_result_without_call(tmp_path):
    args,seen,_,_,mono=fixture(tmp_path);mono[0]=46
    result=run_risk_exception(**args)
    assert result['status']=='DEGRADED' and not seen
    assert run_risk_exception(**args)==result


def test_small_remaining_global_budget_caps_http_instead_of_spending_twenty_seconds(tmp_path,monkeypatch):
    args,seen,_,_,mono=fixture(tmp_path);mono[0]=30
    original_complete=args['client'].complete;budgets=[]
    def complete(*a,**kw):
        budgets.append(kw['timeout_seconds']);return original_complete(*a,**kw)
    monkeypatch.setattr(args['client'],'complete',complete)
    assert run_risk_exception(**args)['status']=='READY'
    assert budgets==[15] and len(seen)==1


@pytest.mark.parametrize('fault',['authority','window','budget','future_date'])
def test_bad_runtime_context_never_calls_model(tmp_path,fault):
    args,seen,_,clock,_=fixture(tmp_path)
    if fault=='authority':args['allow_model_call']=False
    elif fault=='window':clock[0]=clock[0].replace(minute=37)
    elif fault=='budget':args['timeout_seconds']=21
    elif fault=='future_date':args['source_trade_date']='2026-10-20'
    with pytest.raises(ValueError):run_risk_exception(**args)
    assert not seen


def test_valid_capture_before_state_commit_crash_recovers_restrictions_without_recall(tmp_path,monkeypatch):
    args,seen,_,_,_=fixture(tmp_path);real=RiskExceptionRuntime.record
    monkeypatch.setattr(RiskExceptionRuntime,'record',lambda *a,**k:(_ for _ in ()).throw(sqlite3.OperationalError('TEST')))
    with pytest.raises(sqlite3.OperationalError):run_risk_exception(**args)
    monkeypatch.setattr(RiskExceptionRuntime,'record',real)
    result=run_risk_exception(**args)
    assert result['status']=='READY' and len(result['controls'])==2 and len(seen)==1


@pytest.mark.parametrize('late_seconds',[48,70])
def test_late_capture_cannot_become_valid_on_commit_recovery(tmp_path,monkeypatch,late_seconds):
    args,seen,_,clock,mono=fixture(tmp_path);real=RiskExceptionRuntime.record
    original_complete=args['client'].complete
    def late(*a,**kw):
        value=original_complete(*a,**kw);clock[0]+=timedelta(seconds=late_seconds);mono[0]=late_seconds
        return value
    monkeypatch.setattr(args['client'],'complete',late)
    monkeypatch.setattr(RiskExceptionRuntime,'record',lambda *a,**k:(_ for _ in ()).throw(sqlite3.OperationalError('TEST')))
    with pytest.raises(sqlite3.OperationalError):run_risk_exception(**args)
    monkeypatch.setattr(RiskExceptionRuntime,'record',real)
    result=run_risk_exception(**args)
    assert result['status']=='DEGRADED' and result['controls']==[] and len(seen)==1
