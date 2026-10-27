from pathlib import Path
import hashlib
import json
import socket

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient, ModelClientError, ModelNetworkError
from test_pipeline_models import _settings


class TrackedStream(httpx.SyncByteStream):
    def __init__(self, *, partial_failure=False):
        self.closed=False
        self.partial_failure=partial_failure
        self.raw=b'data: {"choices":[{"delta":{"content":"{\\"ok\\":true}"}}]}\n\ndata: [DONE]\n\n'

    def __iter__(self):
        if self.partial_failure:
            yield self.raw[:10]
            raise httpx.ReadError('unit-only-message-must-not-be-saved')
        yield self.raw[:9]
        yield self.raw[9:]

    def close(self):
        self.closed=True


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args,**kwargs):
        pytest.fail('real network is forbidden in evidence preflight')
    monkeypatch.setattr(socket.socket,'connect',blocked)


def test_real_stream_request_saved_before_dispatch_and_raw_response_closed(tmp_path):
    stream=TrackedStream();seen=[]
    def transport(request):
        originals=list(tmp_path.rglob('*-request.json'))
        assert len(originals)==1
        archived=json.loads(originals[0].read_bytes())
        wire=json.loads(request.read())
        assert archived['body']==wire
        assert wire['messages'][0]['content']=='complete rendered TECH_REFERENCE: MET'
        seen.append(request)
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=stream)
    client=OpenAICompatibleModelClient(_settings(tmp_path),transport=httpx.MockTransport(transport),max_attempts=2)
    result=client.complete('deepseek-v4-pro',[dict(role='user',content='complete rendered TECH_REFERENCE: MET')],stage='A4',team_session='INTRADAY')
    assert result.output=={'ok':True} and stream.closed and len(seen)==1
    attempt=result.original_evidence['attempts'][0]
    assert attempt['response_closed'] and Path(attempt['response']['path']).read_bytes()==stream.raw
    assert attempt['response']['sha256']==hashlib.sha256(stream.raw).hexdigest()
    assert 'model-secret' not in ''.join(p.read_text('utf-8') for p in tmp_path.rglob('*.json'))


@pytest.mark.parametrize('failures',[1,2])
def test_connection_retry_has_durable_per_attempt_typed_causes(tmp_path,failures):
    calls=[];stream=TrackedStream()
    def transport(request):
        calls.append(request)
        assert len(list(tmp_path.rglob('*-request.json')))==len(calls)
        if len(calls)<=failures:
            try:raise ConnectionRefusedError('never-save-this-text')
            except ConnectionRefusedError as cause:
                raise httpx.ConnectError('never-save-url-or-secret',request=request) from cause
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=stream)
    client=OpenAICompatibleModelClient(_settings(tmp_path),transport=httpx.MockTransport(transport),max_attempts=2,sleep=lambda _:None)
    if failures==1:
        result=client.complete('deepseek-v4-pro',[dict(role='user',content='facts')],stage='A4',team_session='INTRADAY',timeout_seconds=30)
        assert result.output=={'ok':True} and result.attempts==2 and stream.closed
        receipt=result.original_evidence
    else:
        with pytest.raises(ModelNetworkError) as raised:
            client.complete('deepseek-v4-pro',[dict(role='user',content='facts')],stage='A4',team_session='INTRADAY',timeout_seconds=30)
        receipt=raised.value.diagnostics['original_evidence']
        assert raised.value.attempts==2
    assert len(calls)==2 and len(receipt['attempts'])==2
    for item in receipt['attempts'][:failures]:
        assert item['error']['cause_classes']==['ConnectError','ConnectionRefusedError']
        assert item['error']['phase']=='CONNECT_OR_RESPONSE_HEADERS'
        assert item['elapsed_ms']>=0 and item['response']['bytes']==0


def test_partial_response_is_preserved_and_not_retried(tmp_path):
    calls=[];stream=TrackedStream(partial_failure=True)
    def transport(request):
        calls.append(request)
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=stream)
    client=OpenAICompatibleModelClient(_settings(tmp_path),transport=httpx.MockTransport(transport),max_attempts=2)
    with pytest.raises(ModelNetworkError) as raised:
        client.complete('deepseek-v4-pro',[dict(role='user',content='facts')],stage='A4')
    assert len(calls)==1 and stream.closed
    attempt=raised.value.diagnostics['original_evidence']['attempts'][0]
    assert attempt['response_closed'] and attempt['response']['bytes']==10
    assert Path(attempt['response']['path']).read_bytes()==stream.raw[:10]


def test_request_persistence_failure_stops_before_paid_dispatch(tmp_path,monkeypatch):
    from liangjian_funnel.pipeline import model_evidence
    seen=[]
    monkeypatch.setattr(model_evidence,'_write',lambda *args:(_ for _ in ()).throw(OSError('unit disk full')))
    client=OpenAICompatibleModelClient(_settings(tmp_path),transport=httpx.MockTransport(lambda request:seen.append(request)))
    with pytest.raises(ModelClientError,match='MODEL_EVIDENCE_PERSIST_FAILED'):
        client.complete('deepseek-v4-pro',[dict(role='user',content='facts')],stage='A4')
    assert not seen


def test_archive_time_consumes_original_deadline_before_any_dispatch(tmp_path,monkeypatch):
    from liangjian_funnel.pipeline import model_evidence
    now=[0.0];seen=[];write=model_evidence._write
    def slow_archive(*args):
        result=write(*args);now[0]=2.0;return result
    monkeypatch.setattr(model_evidence,'_write',slow_archive)
    client=OpenAICompatibleModelClient(_settings(tmp_path),monotonic=lambda:now[0],
        transport=httpx.MockTransport(lambda request:seen.append(request)))
    with pytest.raises(ModelNetworkError,match='MODEL_TOTAL_DEADLINE_EXCEEDED') as raised:
        client.complete('deepseek-v4-pro',[dict(role='user',content='facts')],stage='A4',timeout_seconds=1)
    assert not seen and raised.value.attempts==0
