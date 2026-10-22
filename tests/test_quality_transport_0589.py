"""No real HTTP: actual ModelClient + mock transport; independent slot evidence."""
from contextlib import closing,contextmanager
from contextvars import ContextVar
from datetime import datetime,timedelta
import hashlib
import json
from pathlib import Path
import sqlite3

import httpx
import pytest
from pydantic import SecretStr

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelHTTPError,ModelNetworkError,ModelClientError
from liangjian_funnel.runtime.quality_transport_0589 import (
    ACTIVE,QualityTransportGuard,QualityTransportStop,_Transport,_json,_hash,_read_claim,retry_after_seconds)
from liangjian_funnel.settings import Settings,RESEARCH_MODELS,MONITOR_MODEL


AT=datetime.fromisoformat('2026-10-22T17:00:00+08:00')
PRO=RESEARCH_MODELS[0]


def binding():
    return dict(head='a'*40,source_trade_date='2026-10-21',execution_trade_date='2026-10-22',
        quality_run_id='quality-0589',provider_base_url='https://ai-api.finpoints.tech/v1',
        models={role:PRO for role in ('A2','A3','A6','PM')},
        role_run_ids={role:'real-'+role for role in ('A2','A3','A6','PM')},
        a4_model=MONITOR_MODEL,a4_run_id='real-A4')


def request(role='A2',attempt=1,batch='b'):
    b=binding();stage,session={'A2':('A2','CLOSE_REVIEW'),'A3':('A3','PLAN_REVIEW'),
        'A6':('A6','PREMARKET'),'PM':('A3','PM_PREMARKET'),'A4':('A4','INTRADAY')}[role]
    model=b['a4_model'] if role=='A4' else b['models'][role]
    runtime=dict(required_envelope=dict(stage_id=stage,model_name=model,
        run_id=b['a4_run_id'] if role=='A4' else b['role_run_ids'][role],
        source_trade_date=b['source_trade_date'],execution_trade_date=b['execution_trade_date']),
        session=session,catalog_sha256=hashlib.sha256(batch.encode()).hexdigest())
    if attempt>1:runtime.update(correction_attempt=attempt,validation_feedback=dict(validation_errors=['ORIGINAL_SCHEMA_ERROR']))
    messages=[dict(role='system',content='FROZEN_SYSTEM'),dict(role='user',content='RUNTIME_INPUT\n'+_json(runtime))]
    metadata=dict(stage=stage,team_session=session,input_hash=_hash(_json(messages).encode()),
        prompt_hash=_hash(messages[0]['content'].encode()),timeout_seconds=1200,max_output_tokens=2048)
    return model,messages,metadata


class Clock:
    def __init__(self):self.at=AT;self.delays=[]
    def now(self):return self.at
    def sleep(self,seconds):self.delays.append(seconds);self.at+=timedelta(seconds=seconds)


def settings(tmp_path):
    return Settings.from_env({},root=tmp_path).model_copy(update={
        'model_api_key':SecretStr('sk-mocksecret-0589-value'), 'model_timeout_seconds':1200})


def sse():
    event=dict(choices=[dict(delta=dict(content='{"ok":true}'),finish_reason=None)],
        usage=dict(prompt_tokens=5,completion_tokens=4,total_tokens=9))
    return ('data: '+json.dumps(event)+'\n\ndata: [DONE]\n\n').encode()


class Wire(httpx.BaseTransport):
    def __init__(self,owner,responses):self.owner,self.responses=owner,responses
    def handle_request(self,req):
        guard=self.owner.guard
        row=guard.snapshot()['dispatches'][-1]
        assert row['response_status'] is None and row['dispatch_reserved'] is True
        original=Path(row['reservation_original']['path'])
        assert _hash(original.read_bytes())==row['reservation_original']['sha256']
        self.owner.actual.append((bytes(req.content),list(req.headers.raw),str(req.url)))
        outcome=self.responses.pop(0)
        if isinstance(outcome,BaseException):raise outcome
        if isinstance(outcome,tuple):status,headers,stream=outcome
        else:status,headers,stream=outcome,{},httpx.ByteStream(sse() if outcome==200 else b'{"error":{"code":"quota","message":"long error"}}')
        return httpx.Response(status,request=req,headers={'content-type':'text/event-stream' if status==200 else 'application/json',**headers},stream=stream)
    def close(self):
        if self.owner.close_fail:raise RuntimeError('DO_NOT_PRINT_SECRET')


class Factory:
    def __init__(self,responses):self.responses=list(responses);self.actual=[];self.guard=None;self.close_fail=False
    def __call__(self):return Wire(self,self.responses)


def make(tmp_path,responses=(200,),**kwargs):
    clock=Clock();factory=Factory(responses)
    seen=[]
    def resolver(settings,model,runtime,metadata):
        cid='existing:'+metadata['input_hash'];seen.append(cid);return cid
    guard=QualityTransportGuard(binding=binding(),directory=tmp_path/'guard',deadline=AT+timedelta(hours=2),
        now=clock.now,sleep=clock.sleep,transport_factory=factory,resolve_call=resolver,
        outcome_reader=lambda settings,cid:dict(validated=0,issues=['ORIGINAL_ERROR']),**kwargs)
    factory.guard=guard
    return guard,clock,factory,seen


def call(client,role='A2',attempt=1,batch='b'):
    model,messages,metadata=request(role,attempt,batch)
    return client.complete(model,messages,**metadata)


def test_actual_client_success_fields_and_restoration(tmp_path):
    original=(OpenAICompatibleModelClient.__init__,OpenAICompatibleModelClient.complete)
    guard,clock,wire,_=make(tmp_path)
    with guard.install():
        client=OpenAICompatibleModelClient(settings(tmp_path),max_attempts=8)
        assert client.max_attempts==1
        result=call(client)
        assert result.output==dict(ok=True)
        assert result.provider_stream_telemetry is not None
    assert original==(OpenAICompatibleModelClient.__init__,OpenAICompatibleModelClient.complete)
    ledger=guard.snapshot();row=ledger['dispatches'][0]
    assert ledger['actual_http']==1 and row['response_status']==200 and row['response_complete']
    assert row['run_id']=='real-A2' and row['client_attempt_index']==1
    assert row['completion_original'] and len(wire.actual)==1
    assert 'sk-mocksecret' not in (guard.directory/'LEDGER.json').read_text()


def test_429_exact_retry_delays_rejections_and_real_client_telemetry(tmp_path):
    guard,clock,wire,_=make(tmp_path,[429,429,429,429,200])
    with guard.install():result=call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert clock.delays==[60,120,240,480]
    assert all(row==wire.actual[0] for row in wire.actual)
    ledger=guard.snapshot()
    assert ledger['actual_http']==5 and ledger['rate_limit_retries']==4
    assert result.attempts==1  # Original ModelClient attempts, not fabricated HTTP count.
    assert [row['is_429_retry'] for row in ledger['dispatches']]==[False,True,True,True,True]
    assert all(row['rejection_evidence'] for row in ledger['dispatches'][:-1])


@pytest.mark.parametrize('status',[429,500,503,599])
def test_rejection_original_complete_and_redacted(tmp_path,status):
    secret='sk-mocksecret-0589-value'
    raw=json.dumps(dict(error=dict(type='quota',code='quota_exceeded',message='x'*10000+secret),
        authorization='Bearer '+secret,url='https://u:p@provider/x?api_key='+secret)).encode()
    guard,clock,wire,_=make(tmp_path,[(status,{'x-request-id':secret,'x-ratelimit-remaining':'0','retry-after':'60'},httpx.ByteStream(raw))])
    # For429 force evidence-only terminal via per-call deadline; no second HTTP.
    if status==429:guard.deadline=AT+timedelta(seconds=30)
    with guard.install():
        with pytest.raises((ModelHTTPError,QualityTransportStop)):
            call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert len(wire.actual)==1
    row=guard.snapshot()['dispatches'][0];ref=row['rejection_evidence'];receipt=Path(ref['receipt_path'])
    body=receipt.read_text();assert secret not in body and 'Bearer '+secret not in body
    assert 'x'*10000 in body and 'x-ratelimit-remaining' in body
    assert _hash(raw)==ref['body_sha256'] and _hash(receipt.read_bytes())==ref['receipt_sha256']
    assert row['response_status']==status and row['response_complete']


def test_retry_after_longer_seconds_and_httpdate(tmp_path):
    guard,clock,wire,_=make(tmp_path,[(429,{'retry-after':'90'},httpx.ByteStream(b'{}')),200])
    with guard.install():call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert clock.delays==[90]
    assert retry_after_seconds('Thu, 22 Oct 2026 09:02:00 GMT',AT)==120
    assert retry_after_seconds('NaN',AT)==0


def test_schema_original_failures_count_and_batches_share_global(tmp_path):
    guard,clock,wire,_=make(tmp_path,[200]*8)
    with guard.install():
        client=OpenAICompatibleModelClient(settings(tmp_path))
        for role in ('A2','A3'):
            for attempt in (1,2,3):call(client,role,attempt)
        call(client,'A3',1,'another')
        call(client,'PM')
    rows=guard.snapshot()['dispatches']
    assert len(rows)==8 and [r['schema_correction_attempt'] for r in rows[:3]]==[1,2,3]
    assert rows[-1]['role']=='A3' and rows[-1]['session']=='PM_PREMARKET' and rows[-1]['run_id']=='real-PM'


def test_success_cannot_be_retried_as_schema_correction(tmp_path):
    guard,clock,wire,_=make(tmp_path,[200,200])
    guard.outcome_reader=lambda *args:dict(validated=1,issues=[])
    with guard.install():
        client=OpenAICompatibleModelClient(settings(tmp_path));call(client)
        with pytest.raises(QualityTransportStop,match='PERSISTED_RAW_SCHEMA_FAILURE'):call(client,attempt=2)
    assert len(wire.actual)==1


@pytest.mark.parametrize('role',['A2','A6','PM','A4'])
def test_role_total_three_even_when_catalog_changes(tmp_path,role):
    guard,clock,wire,_=make(tmp_path,[200]*4)
    with guard.install():
        client=OpenAICompatibleModelClient(settings(tmp_path))
        for index in range(3):call(client,role,1,str(index))
        with pytest.raises(QualityTransportStop,match='ROLE_TOTAL3'):call(client,role,1,'4')
    assert len(wire.actual)==3


def test_actual_http24_counts_all_roles_and_blocks_25_before_wire(tmp_path):
    guard,clock,wire,_=make(tmp_path,[200]*25)
    with guard.install():
        client=OpenAICompatibleModelClient(settings(tmp_path))
        for index in range(24):call(client,'A3',1,str(index))
        with pytest.raises(QualityTransportStop,match='HTTP24'):call(client,'A3',1,'25')
    assert len(wire.actual)==24 and guard.snapshot()['actual_http']==24


def test_global_429_twelve_retries(tmp_path):
    guard,clock,wire,_=make(tmp_path,([429]*4+[200])*3+[429,200])
    guard.deadline=AT+timedelta(hours=6)
    with guard.install():
        client=OpenAICompatibleModelClient(settings(tmp_path))
        for index in range(3):call(client,'A3',1,str(index))
        with pytest.raises(QualityTransportStop,match='RETRY429_OR_HTTP24'):call(client,'A3',1,'fourth')
    assert len(wire.actual)==16 and guard.snapshot()['rate_limit_retries']==12


def test_per_call_four_retries_then_stop(tmp_path):
    guard,clock,wire,_=make(tmp_path,[429]*6)
    with guard.install():
        with pytest.raises(ModelHTTPError,match='RATE_LIMIT_RETRY_EXHAUSTED'):
            call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert len(wire.actual)==5 and clock.delays==[60,120,240,480]


@pytest.mark.parametrize('error',[httpx.ReadTimeout('sensitive'),httpx.ConnectError('secret'),RuntimeError('unknown')])
def test_unknown_transport_counted_once_no_retry(tmp_path,error):
    guard,clock,wire,_=make(tmp_path,[error,200])
    with guard.install():
        with pytest.raises(BaseException):
            call(OpenAICompatibleModelClient(settings(tmp_path)))
    row=guard.snapshot()['dispatches'][0]
    assert len(wire.actual)==1 and row['response_status'] is None and not row['response_complete']


class BrokenStream(httpx.SyncByteStream):
    def __iter__(self):
        yield b'data: {"choices":['
        raise httpx.ReadError('secret-original-not-logged')
    def close(self):pass


def test_stream_interruption_consumes_slot_and_no_followup(tmp_path):
    guard,clock,wire,_=make(tmp_path,[(200,{},BrokenStream()),200])
    with guard.install():
        with pytest.raises(ModelNetworkError) as exc:call(OpenAICompatibleModelClient(settings(tmp_path)))
    row=guard.snapshot()['dispatches'][0]
    assert len(wire.actual)==1 and not row['response_complete']
    assert row['transport_failure']=='STREAM_INTERRUPTED_NO_RETRY'
    assert guard.snapshot()['terminal_stop']
    assert exc.value.diagnostics.get('provider_stream'), 'Unmodified ModelClient must persist actual stream telemetry'


def test_rejection_evidence_failure_preserves_provider_reason_and_aborts(tmp_path):
    def failed(*args,**kwargs):raise OSError('secret')
    guard,clock,wire,_=make(tmp_path,[429,200],persist_rejection=failed)
    with guard.install():
        with pytest.raises(ModelHTTPError) as exc:call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert exc.value.reason_code=='RATE_LIMIT_RETRY_EXHAUSTED'
    assert exc.value.diagnostics['provider_evidence_failure']=='PROVIDER_REJECTION_EVIDENCE_FAILURE'
    assert len(wire.actual)==1 and not clock.delays


def test_close_failure_explicit_and_no_retry(tmp_path):
    guard,clock,wire,_=make(tmp_path,[429,200]);wire.close_fail=True
    with guard.install():
        with pytest.raises(ModelHTTPError) as exc:call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert exc.value.diagnostics['provider_close_failure']==['PROVIDER_TRANSPORT_CLOSE_FAILED']
    assert len(wire.actual)==1


def test_success_transport_close_failure_not_masked_by_network_error(tmp_path):
    guard,clock,wire,_=make(tmp_path,[200]);wire.close_fail=True
    with guard.install():
        with pytest.raises(ModelClientError) as exc:call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert exc.value.reason_code=='PROVIDER_TRANSPORT_CLOSE_FAILED'
    assert exc.value.diagnostics['provider_close_failure']==['PROVIDER_TRANSPORT_CLOSE_FAILED']
    assert exc.value.diagnostics.get('provider_stream')
    assert len(wire.actual)==1 and guard.snapshot()['terminal_stop']


def test_exact_claim_readonly_resolver_and_no_schema_creation(tmp_path):
    s=settings(tmp_path);path=tmp_path/'state/team_v2/model.db';path.parent.mkdir(parents=True)
    model,messages,metadata=request();runtime=json.loads(messages[1]['content'].removeprefix('RUNTIME_INPUT\n'))
    e=runtime['required_envelope']
    exact=_hash(_json(dict(run_id=e['run_id'],session=metadata['team_session'],role=metadata['stage'],
        input_hash=metadata['input_hash'],model=model,timeout_seconds=metadata['timeout_seconds'],
        max_output_tokens=metadata['max_output_tokens'],catalog_hash=runtime['catalog_sha256'])).encode())
    with closing(sqlite3.connect(path)) as db:
        db.execute('CREATE TABLE model_call_claims(call_id TEXT,request_binding TEXT,state TEXT)')
        db.execute('INSERT INTO model_call_claims VALUES(?,?,?)',('real-claim',exact,'CLAIMED'));db.commit()
    before=path.read_bytes()
    assert _read_claim(s,model,runtime,metadata)=='real-claim'
    assert path.read_bytes()==before
    metadata['timeout_seconds']=999
    with pytest.raises(ValueError,match='EXACT_EXISTING_CLAIM'):_read_claim(s,model,runtime,metadata)


def test_duplicate_directory_and_bad_endpoint_reject_before_http(tmp_path):
    guard,clock,wire,_=make(tmp_path)
    with pytest.raises(ValueError,match='EXCLUSIVE_DIRECTORY'):make(tmp_path)
    model,messages,metadata=request();call_context=guard.identity(settings(tmp_path),model,messages,metadata)
    with pytest.raises(QualityTransportStop,match='EXACT_PROVIDER_ENDPOINT'):
        guard.reserve(call_context,httpx.Request('POST','https://evil.example/chat/completions',json=dict(model=model)),retry=False)
    assert wire.actual==[] and guard.snapshot()['actual_http']==0


def test_preexisting_client_cannot_escape_instrumentation(tmp_path):
    client=OpenAICompatibleModelClient(settings(tmp_path),max_attempts=8)
    guard,clock,wire,_=make(tmp_path)
    with guard.install():
        with pytest.raises(QualityTransportStop,match='PREEXISTING_OR_MUTATED_CLIENT'):call(client)
    assert wire.actual==[]


def test_request_bytes_or_header_identity_cannot_change_on_429_retry(tmp_path):
    guard,clock,wire,_=make(tmp_path)
    model,messages,metadata=request()
    c=guard.identity(settings(tmp_path),model,messages,metadata)
    req=httpx.Request('POST',guard.endpoint,json=dict(model=model,messages=messages),headers={'Authorization':'Bearer mock'})
    ordinal=guard.reserve(c,req,retry=False)
    guard.received(ordinal,429)
    guard.finalize(ordinal,response_complete=True,rejection_evidence=dict(receipt_path='original',receipt_sha256='a'*64))
    changed=httpx.Request('POST',guard.endpoint,json=dict(model=model,messages=messages),headers={'Authorization':'Bearer changed'})
    with pytest.raises(QualityTransportStop,match='EXACT_SAME_REQUEST_BYTES'):guard.reserve(c,changed,retry=True)
    assert guard.snapshot()['actual_http']==1


def test_ledger_failure_stops_before_any_wire(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.quality_transport_0589 as module
    guard,clock,wire,_=make(tmp_path)
    def write_failed(*args,**kwargs):raise OSError('disk full')
    monkeypatch.setattr(module,'_exclusive',write_failed)
    with guard.install():
        with pytest.raises(OSError):call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert wire.actual==[]


def test_deadline_checked_before_dispatch_and_each_stream_chunk(tmp_path):
    guard,clock,wire,_=make(tmp_path)
    clock.at=guard.deadline
    with guard.install():
        with pytest.raises(QualityTransportStop,match='DEADLINE'):call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert wire.actual==[]


def test_wrong_model_or_message_hash_does_not_dispatch(tmp_path):
    guard,clock,wire,_=make(tmp_path)
    model,messages,metadata=request();messages[0]['content']='altered-after-hash'
    with guard.install():
        with pytest.raises(QualityTransportStop,match='ORIGINAL_CALL_BINDING'):
            OpenAICompatibleModelClient(settings(tmp_path)).complete(model,messages,**metadata)
    assert wire.actual==[]


def test_wire_context_only_covers_exact_dispatch_read_close_not_decode_or_sleep(tmp_path,monkeypatch):
    import liangjian_funnel.pipeline.model_client as client_module
    permitted=ContextVar('wire_permitted',default=False)
    events=[]
    @contextmanager
    def context():
        assert not permitted.get()
        token=permitted.set(True)
        try:yield
        finally:permitted.reset(token)
    class CheckedStream(httpx.SyncByteStream):
        def __init__(self,body):self.body=body
        def __iter__(self):
            assert permitted.get();events.append('read');yield self.body
        def close(self):assert permitted.get();events.append('stream-close')
    clock=Clock()
    def wait(delay):assert not permitted.get();events.append('sleep');clock.sleep(delay)
    guard,_,wire,_=make(tmp_path,[(429,{},CheckedStream(b'{}')),(200,{},CheckedStream(sse()))],wire_context=context)
    guard.sleep=wait;guard.now=clock.now
    original_handle=Wire.handle_request
    def checked_handle(self,req):
        assert permitted.get();events.append('dispatch');return original_handle(self,req)
    monkeypatch.setattr(Wire,'handle_request',checked_handle)
    original_close=Wire.close
    def checked_close(self):assert permitted.get();events.append('transport-close');return original_close(self)
    monkeypatch.setattr(Wire,'close',checked_close)
    original_decode=client_module._decode_model_response
    def checked_decode(*args,**kwargs):
        assert not permitted.get();events.append('decode');return original_decode(*args,**kwargs)
    monkeypatch.setattr(client_module,'_decode_model_response',checked_decode)
    with guard.install():call(OpenAICompatibleModelClient(settings(tmp_path)))
    assert not permitted.get() and events.count('dispatch')==2 and events.count('sleep')==1
    assert events.count('read')==2 and 'decode' in events


@pytest.mark.parametrize('outcome',[None,dict(validated=0,issues=[]),dict(validated=1,issues=[]),dict(validated=0)])
def test_missing_or_unknown_previous_original_cannot_buy_schema_retry(tmp_path,outcome):
    guard,clock,wire,_=make(tmp_path,[200,200]);guard.outcome_reader=lambda *args:outcome
    with guard.install():
        client=OpenAICompatibleModelClient(settings(tmp_path));call(client)
        with pytest.raises(QualityTransportStop,match='PERSISTED_RAW_SCHEMA_FAILURE'):call(client,attempt=2)
    assert len(wire.actual)==1
