"""Received refusals survive ordinary retries, without changing request data."""
from pathlib import Path
from hashlib import sha256
import json

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import ModelHTTPError,OpenAICompatibleModelClient
from liangjian_funnel.pipeline import provider_rejection_evidence as evidence
from liangjian_funnel.settings import Settings


def configured(tmp_path):
    return Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'model-test-secret'},root=tmp_path)


def receipts(tmp_path):
    return sorted(tmp_path.glob('outputs/provider_rejections/*/*.json'))


@pytest.mark.parametrize('status,reason',[
    (429,'RATE_LIMIT_RETRY_EXHAUSTED'),(503,'UPSTREAM_5XX_RETRY_EXHAUSTED')])
def test_all_received_failures_are_saved_before_original_failure_is_raised(tmp_path,status,reason):
    calls=[];waits=[]
    raw=json.dumps({'error':{'type':'quota_or_overload','code':'provider-specific',
        'message':'retry later model-test-secret'},'tail':'not-truncated'}).encode()
    def request(req):
        calls.append(req.read())
        return httpx.Response(status,content=raw,headers={'retry-after':'1','x-ratelimit-remaining-tokens':'0',
            'x-request-id':str(len(calls))},request=req)
    client=OpenAICompatibleModelClient(configured(tmp_path),transport=httpx.MockTransport(request),
        sleep=waits.append,max_attempts=3)
    with pytest.raises(ModelHTTPError) as caught:
        client.complete('deepseek-v4-pro',[{'role':'user','content':'BUSINESS_NOT_TO_SAVE'}],
            stage='A2',team_session='CLOSE_REVIEW',input_hash='i'*64,prompt_hash='p'*64)
    assert caught.value.reason_code==reason and caught.value.status_code==status and caught.value.attempts==3
    assert len(calls)==3 and len(set(calls))==1 and waits==[1,1]
    saved=caught.value.diagnostics['provider_rejection_evidence']
    assert len(saved)==3 and len(receipts(tmp_path))==3
    for item in saved:
        p=Path(item['receipt_path']);raw_receipt=p.read_bytes();body=json.loads(raw_receipt)
        assert sha256(raw_receipt).hexdigest()==item['receipt_sha256']
        assert body['body_sha256']==sha256(raw).hexdigest()
        assert body['body']['tail']=='not-truncated' and body['error']['code']=='provider-specific'
        assert body['response_headers']['x-ratelimit-remaining-tokens']==['0']
        assert 'model-test-secret' not in raw_receipt.decode()
        assert 'BUSINESS_NOT_TO_SAVE' not in raw_receipt.decode()
        assert body['request_binding']['input_hash']=='i'*64


def test_429_then_success_keeps_rejection_without_changing_success_output(tmp_path):
    calls=[]
    def request(req):
        calls.append(req.read())
        return httpx.Response(429,json={'error':{'message':'rate'}}) if len(calls)==1 else httpx.Response(
            200,json={'choices':[{'message':{'content':'{"ok":true}'}}]})
    client=OpenAICompatibleModelClient(configured(tmp_path),transport=httpx.MockTransport(request),sleep=lambda _:None)
    result=client.complete('deepseek-v4-pro',[{'role':'user','content':'facts'}],stage='A2')
    assert result.output=={'ok':True} and result.attempts==2
    assert len(receipts(tmp_path))==1 and calls[0]==calls[1]


@pytest.mark.parametrize('status,reason',[(429,'RATE_LIMIT_RETRY_EXHAUSTED'),(503,'UPSTREAM_5XX_RETRY_EXHAUSTED')])
def test_evidence_failure_stops_retries_but_keeps_original_provider_failure(tmp_path,monkeypatch,status,reason):
    calls=[]
    def request(req):
        calls.append(req)
        return httpx.Response(status,json={'error':{'message':'provider failure'}})
    def fail(*args,**kwargs):
        raise evidence.ProviderRejectionEvidenceError('PROVIDER_REJECTION_PERSISTENCE_FAILED',
            status_code=status,body_sha256='b'*64)
    monkeypatch.setattr(evidence,'persist_provider_rejection',fail)
    client=OpenAICompatibleModelClient(configured(tmp_path),transport=httpx.MockTransport(request),
        sleep=lambda _:pytest.fail('cannot retry after evidence failure'))
    with pytest.raises(ModelHTTPError) as caught:
        client.complete('deepseek-v4-pro',[{'role':'user','content':'facts'}])
    assert caught.value.reason_code==reason and len(calls)==1
    assert caught.value.diagnostics['provider_evidence_failure']=='PROVIDER_REJECTION_PERSISTENCE_FAILED'
    assert caught.value.diagnostics['body_sha256']=='b'*64


def test_success_path_does_not_capture_or_write_rejection(tmp_path,monkeypatch):
    monkeypatch.setattr(evidence,'persist_provider_rejection',lambda *a,**kw:pytest.fail('200 is not a rejection'))
    client=OpenAICompatibleModelClient(configured(tmp_path),transport=httpx.MockTransport(
        lambda req:httpx.Response(200,json={'choices':[{'message':{'content':'{"ok":true}'}}]})))
    assert client.complete('deepseek-v4-pro',[{'role':'user','content':'facts'}]).output=={'ok':True}
    assert not receipts(tmp_path)
