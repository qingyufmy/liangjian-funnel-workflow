"""0568/0569 transport-only contracts, synthetic SSE and no network."""
import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from liangjian_funnel.settings import Settings
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient, ModelClientError

PRO='deepseek/deepseek-v4-pro'
FLASH='deepseek-v4.1-flash'
KEY='synthetic-credential-not-real'

def settings(tmp_path):
    return Settings.from_env({'LIANGJIAN_MODEL_BASE_URL':'https://ai-api.finpoints.tech/v1',
        'LIANGJIAN_MODEL_API_KEY':KEY},root=tmp_path)

def event(delta=None,**kw):
    body={'choices':[{'delta':delta or {},'finish_reason':kw.pop('finish_reason',None)}],**kw}
    return ('data: '+json.dumps(body)+'\n\n').encode()

class Stream(httpx.SyncByteStream):
    def __init__(self, parts):self.parts=parts
    def __iter__(self):
        for part in self.parts:
            if isinstance(part,Exception):raise part
            yield part() if callable(part) else part

def good_parts(reasoning=False):
    return [event({'reasoning_content':'PRIVATE_REASONING_DO_NOT_SAVE'}) if reasoning else b': heartbeat\n\n',
        event({'content':'{"ok":'}),event({'content':'true}'},finish_reason='stop'),
        b'data: {"choices":[],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12}}\n\n',b'data: [DONE]\n\n']

@pytest.mark.parametrize('model',[PRO,FLASH])
@pytest.mark.parametrize('role,session',[('A2','CLOSE_REVIEW'),('A3','PLAN_REVIEW'),('A4','INTRADAY'),('A4',None),('A5','MIDDAY'),('A6','PREMARKET'),('A3','PM_PREMARKET')])
def test_all_roles_explicit_off_real_incremental_decode_and_measurements(tmp_path,model,role,session):
    seen=[]
    def wire(request):
        b=json.loads(request.content);seen.append(b)
        assert b['thinking']=={'type':'disabled'} and b['stream'] is True
        assert b['stream_options']=={'include_usage':True}
        assert 'enable_thinking' not in b and 'reasoning_effort' not in b
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream(good_parts(True)))
    result=OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire)).complete(
        model,[{'role':'user','content':'synthetic'}],stage=role,team_session=session)
    assert result.output=={'ok':True} and len(seen)==1
    observed=result.provider_stream_telemetry
    assert observed['mode']=='SSE' and observed['ttft_ms'] is not None
    assert observed['reasoning_chars_discarded']==29
    assert observed['provider_usage']=={'prompt_tokens':8,'completion_tokens':4,'total_tokens':12}
    assert observed['output_tokens']==4 and observed['thinking_disabled_requested']
    raw=Path(observed['receipt_path']).read_text()
    assert 'PRIVATE_REASONING_DO_NOT_SAVE' not in raw and KEY not in raw and 'synthetic' not in raw

def test_a4_midstream_disconnect_is_not_retried(tmp_path):
    calls=[]
    def wire(request):
        calls.append(request)
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream([
            event({'content':'{"ok":'}),httpx.RemoteProtocolError('disconnected')]))
    client=OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire),max_attempts=3)
    with pytest.raises(ModelClientError) as exc:client.complete(FLASH,[{'role':'user','content':'synthetic'}],stage='A4')
    assert len(calls)==1 and exc.value.diagnostics['provider_stream']['status']=='FAILED'

def test_a4_missing_done_does_not_blindly_repeat(tmp_path):
    calls=[]
    def wire(request):
        calls.append(request);return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream(good_parts()[:-1]))
    with pytest.raises(ModelClientError,match='STREAM_DONE_MISSING'):
        OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire),max_attempts=3).complete(FLASH,[{'role':'user','content':'x'}],stage='A4')
    assert len(calls)==1

def test_a4_total_deadline_not_extended_by_chunks(tmp_path):
    clock=[0.0];calls=[]
    def late():clock[0]=31.;return event({'content':'true}'})
    def wire(request):
        calls.append(request);return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream([event({'content':'{"ok":'}),late]))
    with pytest.raises(ModelClientError,match='DEADLINE|TIMEOUT') as exc:
        OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire),monotonic=lambda:clock[0]).complete(
            FLASH,[{'role':'user','content':'x'}],stage='A4',timeout_seconds=30)
    assert len(calls)==1 and exc.value.diagnostics['provider_stream']['status']=='FAILED'

@pytest.mark.parametrize('status',[403,429,503])
def test_full_redacted_error_body_and_all_headers_before_failure(tmp_path,status):
    def wire(request):
        return httpx.Response(status,headers={'x-request-id':'synthetic-id','server':'synthetic-server','set-cookie':'SECRET_COOKIE'},
            json={'error':{'type':'synthetic','code':'quota','message':'Rejected '+KEY},'messages':['PRIVATE_REQUEST_ECHO']})
    with pytest.raises(ModelClientError):
        OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire),max_attempts=1).complete(
            FLASH,[{'role':'user','content':'synthetic'}],stage='A4')
    files=list(tmp_path.rglob('rejection-*.json'));assert len(files)==1
    raw=files[0].read_text();body=json.loads(raw)
    assert KEY not in raw and 'SECRET_COOKIE' not in raw and 'PRIVATE_REQUEST_ECHO' not in raw
    assert body['status_code']==status and body['response_headers']['server']==['synthetic-server']
    assert body['response_headers']['x-request-id']==['synthetic-id'] and body['body_sha256']

def test_finpoints_ignoring_stream_is_not_accepted(tmp_path):
    def wire(request):return httpx.Response(200,json={'choices':[{'message':{'content':'{"ok":true}'}}]})
    with pytest.raises(ModelClientError,match='FINPOINTS_STREAM_REQUIRED'):
        OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire)).complete(PRO,[{'role':'user','content':'x'}])

def test_unreported_usage_stays_unknown_not_zero(tmp_path):
    def wire(request):return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream([
        event({'content':'{"ok":true}'}),b'data: [DONE]\n\n']))
    result=OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire)).complete(PRO,[{'role':'user','content':'x'}])
    assert result.provider_stream_telemetry['provider_usage'] is None
    assert result.provider_stream_telemetry['output_tokens'] is None

def test_audit_failure_cannot_report_success(tmp_path):
    def wire(request):return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream(good_parts()))
    with patch('liangjian_funnel.pipeline.provider_stream_metrics.ProviderStreamMetrics.persist',side_effect=OSError('synthetic')):
        with pytest.raises(ModelClientError,match='MODEL_STREAM_TELEMETRY_PERSIST_FAILED'):
            OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire)).complete(PRO,[{'role':'user','content':'x'}])

def test_error_body_observation_is_also_deadline_bounded(tmp_path):
    clock=[0.]
    def late():clock[0]=31.;return b'{"error":{"code":"synthetic"}}'
    def wire(request):return httpx.Response(429,stream=Stream([late]))
    with pytest.raises(ModelClientError,match='PROVIDER_REJECTION_CAPTURE_OR_REDACTION_FAILED'):
        OpenAICompatibleModelClient(settings(tmp_path),transport=httpx.MockTransport(wire),monotonic=lambda:clock[0]).complete(
            FLASH,[{'role':'user','content':'x'}],stage='A4',timeout_seconds=30)
    assert not list(tmp_path.rglob('rejection-*.json'))
