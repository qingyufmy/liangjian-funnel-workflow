"""0404 PM-only measurements: synthetic HTTP, no provider or production I/O."""
from datetime import datetime
import hashlib
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import ModelCallResult, ModelClientError, OpenAICompatibleModelClient
from liangjian_funnel.pipeline.pm_stream_metrics import PMStreamMetrics, safe_pm_measurements
from liangjian_funnel.pipeline.team_v2_contract import TeamValidation
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.pipeline.team_v2_model import call_team_model
from liangjian_funnel.pipeline.team_v2_prompts import TeamMessageRequest, TeamPromptRequest
from liangjian_funnel.settings import Settings

MODEL = 'deepseek-v4-pro-0813'


class Clock:
    value = 0.0
    def __call__(self):return self.value


class Stream(httpx.SyncByteStream):
    def __init__(self, clock, events):self.clock, self.events = clock, events
    def __iter__(self):
        for stamp, value in self.events:
            self.clock.value = stamp
            if isinstance(value, Exception):raise value
            yield value


def event(value):
    return ('data: '+json.dumps(value)+'\n\n').encode()


def client(tmp_path, handler, clock=None, **kwargs):
    settings = Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'SYNTHETIC_NOT_PROVIDER'}, root=tmp_path)
    return OpenAICompatibleModelClient(settings, transport=httpx.MockTransport(handler),
        monotonic=clock or Clock(), sleep=lambda _:None, **kwargs)


def complete(instance, **kwargs):
    return instance.complete(MODEL, [{'role':'user','content':'synthetic'}], stage='A3',
        team_session='PM_PREMARKET', timeout_seconds=10, max_output_tokens=100, **kwargs)


def test_pm_stream_records_real_packet_content_usage_and_rate(tmp_path):
    clock, seen = Clock(), []
    def handler(request):
        seen.append(json.loads(request.read()))
        return httpx.Response(200, headers={'content-type':'text/event-stream'}, stream=Stream(clock,[
            (1, b': heartbeat\n\n'),
            (2, event({'choices':[{'delta':{'reasoning_content':'PRIVATE_REASONING'}}]})),
            (3, event({'choices':[{'delta':{'content':'{"ok":true}'}}]})),
            (5, event({'choices':[], 'usage':{'completion_tokens':10}})),
            (5, b'data: [DONE]\n\n')]))
    response = complete(client(tmp_path,handler,clock,max_attempts=3))
    metrics = response.pm_stream_telemetry
    assert response.output == {'ok':True} and response.attempts == len(seen) == 1
    assert seen[0]['stream_options'] == {'include_usage':True}
    assert metrics['first_packet_ms'] == 1000 and metrics['ttft_ms'] == 3000
    assert metrics['output_tokens'] == 10 and metrics['tokens_per_second'] == 5
    assert metrics['mode'] == 'SSE' and safe_pm_measurements(metrics) == metrics
    assert 'PRIVATE_REASONING' not in repr(response) and 'PRIVATE_REASONING' not in repr(metrics)


@pytest.mark.parametrize('content_type', ['application/json','text/event-stream'])
def test_usage_absent_stays_unknown_not_estimated(tmp_path,content_type):
    body = (json.dumps({'choices':[{'message':{'content':'{"ok":true}'}}]}).encode()
        if content_type == 'application/json' else event({'choices':[{'delta':{'content':'{"ok":true}'}}]})+b'data: [DONE]\n\n')
    response = complete(client(tmp_path,lambda _:httpx.Response(200,headers={'content-type':content_type},content=body)))
    metrics = response.pm_stream_telemetry
    assert metrics['output_tokens'] is None and metrics['tokens_per_second'] is None
    assert metrics['token_usage_status'] == 'NOT_REPORTED'


def test_json_fallback_reports_provider_usage_but_no_invented_ttft_or_rate(tmp_path):
    response = complete(client(tmp_path,lambda _:httpx.Response(200,json={
        'choices':[{'message':{'content':'{"ok":true}'}}], 'usage':{'output_tokens':12}})))
    metrics = response.pm_stream_telemetry
    assert metrics['mode'] == 'JSON_TTFT_UNOBSERVABLE' and metrics['output_tokens'] == 12
    assert metrics['ttft_ms'] is None and metrics['tokens_per_second'] is None


def test_mislabeled_buffered_sse_does_not_claim_ttft(tmp_path):
    body = event({'choices':[{'delta':{'content':'{"ok":true}'}}]})
    body += event({'choices':[], 'usage':{'completion_tokens':5}})+b'data: [DONE]\n\n'
    response = complete(client(tmp_path,lambda _:httpx.Response(200,headers={'content-type':'application/json'},content=body)))
    metrics = response.pm_stream_telemetry
    assert metrics['mode'] == 'BUFFERED_SSE_TTFT_UNOBSERVABLE'
    assert metrics['output_tokens'] == 5 and metrics['ttft_ms'] is None and metrics['tokens_per_second'] is None


@pytest.mark.parametrize('packet,expected', [(False,'PM_FIRST_PACKET_TIMEOUT'),(True,'PM_TOTAL_TIMEOUT')])
def test_timeout_classifies_actual_partial_packet_not_first_complete_line(tmp_path,packet,expected):
    clock, seen = Clock(), []
    def handler(request):
        seen.append(request)
        events = ([(1,b'data: partial-with-no-newline')] if packet else [])+[(4,httpx.ReadTimeout('PRIVATE_TIMEOUT'))]
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream(clock,events))
    with pytest.raises(ModelClientError) as caught:
        complete(client(tmp_path,handler,clock,max_attempts=3))
    assert caught.value.reason_code == expected and caught.value.attempts == len(seen) == 1
    assert caught.value.diagnostics['pm_stream']['first_packet_received'] is packet
    assert 'PRIVATE_TIMEOUT' not in str(caught.value)


def test_wall_deadline_is_not_a_new_first_packet_threshold(tmp_path):
    clock = Clock()
    def handler(_):
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream(clock,[
            (1,b': heartbeat\n\n'),(11,event({'choices':[{'delta':{'content':'{}'}}]}))]))
    with pytest.raises(ModelClientError) as caught:complete(client(tmp_path,handler,clock))
    assert caught.value.reason_code == 'PM_TOTAL_TIMEOUT'


@pytest.mark.parametrize('failure',[429,503,400,422,'json','network','truncated'])
def test_pm_hard_one_dispatch_no_hidden_capacity_variant_or_json_retry(tmp_path,failure):
    seen = []
    def handler(request):
        seen.append(request)
        if failure == 'network':raise httpx.ConnectError('PRIVATE_NETWORK',request=request)
        if failure == 'json':return httpx.Response(200,json={'choices':[{'message':{'content':'not-json'}}]})
        if failure == 'truncated':return httpx.Response(200,json={'choices':[{'finish_reason':'length','message':{'content':'{}'}}]})
        return httpx.Response(failure,json={'error':{'message':'max_tokens exceeds maximum limit'}})
    with pytest.raises(ModelClientError) as caught:complete(client(tmp_path,handler,max_attempts=3))
    assert len(seen) == caught.value.attempts == 1
    assert safe_pm_measurements(caught.value.diagnostics['pm_stream']) is not None


@pytest.mark.parametrize('stage,session',[('A2','CLOSE'),('A3','PLAN_REVIEW'),('A4','INTRADAY'),('A6','PREMARKET')])
def test_other_roles_keep_existing_retries_and_no_pm_request_option(tmp_path,stage,session):
    seen = []
    def handler(request):
        seen.append(json.loads(request.read()))
        return httpx.Response(503,json={}) if len(seen)==1 else httpx.Response(200,json={'choices':[{'message':{'content':'{}'}}]})
    response = client(tmp_path,handler,max_attempts=2).complete(MODEL,[{'role':'user','content':'synthetic'}],stage=stage,team_session=session)
    assert len(seen) == response.attempts == 2 and response.pm_stream_telemetry is None
    assert all('stream_options' not in body for body in seen)


def request():
    def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    catalog = {'items':[]}
    envelope = dict(model_name=MODEL,run_id='synthetic-pm',stage_id='A3',source_trade_date='2026-10-20',execution_trade_date='2026-10-21')
    messages = ({'role':'system','content':'synthetic system'}, {'role':'user','content':'RUNTIME_INPUT\n'+json.dumps({
        'required_envelope':envelope,'session':'PM_PREMARKET','catalog_sha256':digest(catalog)})})
    prompt = TeamPromptRequest('A3','synthetic','synthetic',messages[0]['content'],hashlib.sha256(messages[0]['content'].encode()).hexdigest(),{})
    return TeamMessageRequest(prompt,messages,digest(list(messages)),digest(catalog)),catalog


@pytest.mark.parametrize('failure',[False,True])
def test_pm_journal_persists_safe_success_or_failure_and_claim_prevents_redispatch(tmp_path,failure):
    prepared,catalog = request()
    seen = []
    def handler(http_request):
        seen.append(http_request)
        if failure:raise httpx.ReadTimeout('PRIVATE_TOKEN_VALUE',request=http_request)
        return httpx.Response(200,json={'choices':[{'message':{'content':'{}'}}],'usage':{'completion_tokens':9}})
    instance = client(tmp_path,handler,max_attempts=3)
    journal = ModelJudgmentJournal(tmp_path/'model.db')
    kwargs = dict(call_id='synthetic:PM:1',run_id='synthetic-pm',model=MODEL,session='PM_PREMARKET',
        source_trade_date='2026-10-20',execution_trade_date='2026-10-21',catalog=catalog,timeout_seconds=10,
        max_output_tokens=100,validator=lambda _:TeamValidation(True,(),(),0),
        clock=lambda:datetime.fromisoformat('2026-10-20T20:00:00+08:00'),allow_model_call=True)
    if failure:
        with pytest.raises(ValueError,match='^TEAM_MODEL_CALL_FAILED$'):call_team_model(prepared,instance,journal,**kwargs)
    else:assert call_team_model(prepared,instance,journal,**kwargs).validation.valid
    with sqlite3.connect(journal.path) as db:
        metrics = json.loads(db.execute('SELECT body_json FROM model_call_telemetry').fetchone()[0])
        assert metrics['provider_attempts'] == 1
        assert metrics['failure_code'] == ('PM_FIRST_PACKET_TIMEOUT' if failure else None)
        assert metrics['pm_stream']['output_tokens'] == (None if failure else 9)
    with pytest.raises(ValueError):call_team_model(prepared,instance,journal,**kwargs)
    assert len(seen) == 1 and b'PRIVATE_TOKEN_VALUE' not in journal.path.read_bytes()


@pytest.mark.parametrize('bad',[True,'10',-1,1.2,None])
def test_invalid_or_missing_usage_never_becomes_reported_tokens(bad):
    metrics = PMStreamMetrics(clock=Clock(),started=0)
    metrics.usage({'usage':{'completion_tokens':bad}})
    assert metrics.snapshot()['output_tokens'] is None


def test_result_default_and_untrusted_diagnostics_allowlist():
    assert ModelCallResult(MODEL,{},None,None,0,1,'synthetic').pm_stream_telemetry is None
    metrics = PMStreamMetrics(clock=Clock(),started=0).snapshot()
    assert safe_pm_measurements({**metrics,'secret':'PRIVATE'}) == metrics
    for update in ({'output_tokens':True},{'elapsed_ms':float('nan')},{'first_packet_ms':1},
        {'ttft_ms':1},{'tokens_per_second':1},{'token_usage_status':'PROVIDER_REPORTED'}):
        assert safe_pm_measurements({**metrics,**update}) is None
