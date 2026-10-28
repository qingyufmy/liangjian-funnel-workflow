from io import BytesIO
import json

import httpx
import pytest

from liangjian_funnel.runtime.close_stage_budget import (
    CloseStageBudget,CloseStageBudgetExceeded,budget_scope,bounded_data_sleep,
    configure_source_client,configure_source_session)


def test_parent_elapsed_and_each_stage_never_reset_budget():
    clock=[100.0]
    b=CloseStageBudget.from_parent(5400,parent_deadline_ms=6000*1000,
        monotonic=lambda:clock[0],wall_time=lambda:700.0)
    assert b.started==0 and b.total_deadline==5400 and b.data_deadline==1800
    clock[0]=1700;b.enter_research();assert b.research_deadline==5240
    clock[0]=1800;b.enter_research();assert b.research_started==1700
    clock[0]=5240
    with pytest.raises(CloseStageBudgetExceeded):b.check('RESEARCH')
    b.enter_finalize();clock[0]=5300
    with pytest.raises(CloseStageBudgetExceeded):b.check('FINALIZE')


def test_source_positional_keyword_and_stream_calls_register_before_transport():
    clock=[0.0];b=CloseStageBudget(0,90,lambda:clock[0]);seen=[]
    def transport(request):
        assert b.source_attempts_registered==len(seen)+1
        assert all(v<=30 for v in request.extensions['timeout'].values())
        seen.append(request)
        return httpx.Response(200,content=b'bounded',request=request)
    with budget_scope(b),configure_source_client(httpx.Client(transport=httpx.MockTransport(transport))) as client:
        assert client.send(httpx.Request('GET','https://controlled.invalid/a')).content==b'bounded'
        assert client.send(request=httpx.Request('GET','https://controlled.invalid/b')).content==b'bounded'
        with client.stream('GET','https://controlled.invalid/c') as r:assert r.read()==b'bounded'
        clock[0]=30
        with pytest.raises(CloseStageBudgetExceeded):client.get('https://controlled.invalid/d')
    assert len(seen)==3 and b.source_attempts_registered==3


def test_trickling_source_stream_closes_at_data_deadline():
    clock=[0.0];b=CloseStageBudget(0,90,lambda:clock[0]);closed=[]
    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            yield b'a';clock[0]=31;yield b'b'
        def close(self):closed.append(True)
    def transport(request):return httpx.Response(200,stream=Stream(),request=request)
    with budget_scope(b),configure_source_client(httpx.Client(transport=httpx.MockTransport(transport))) as client:
        with pytest.raises(CloseStageBudgetExceeded):
            with client.stream('GET','https://controlled.invalid') as r:list(r.iter_bytes())
    assert closed


def test_redirect_cannot_dispatch_another_source_request_after_deadline():
    clock=[0.0];b=CloseStageBudget(0,90,lambda:clock[0]);seen=[]
    def transport(request):
        seen.append(request);clock[0]=30
        return httpx.Response(307,headers={'location':'https://controlled.invalid/next'},request=request)
    with budget_scope(b),configure_source_client(httpx.Client(transport=httpx.MockTransport(transport),follow_redirects=True)) as client:
        with pytest.raises(CloseStageBudgetExceeded):client.get('https://controlled.invalid/first')
    assert len(seen)==1


def test_requests_real_session_send_keeps_keyword_and_stream_contract():
    import requests
    from requests.adapters import BaseAdapter
    b=CloseStageBudget(0,90,lambda:0);seen=[]
    class Adapter(BaseAdapter):
        def send(self,request,**kwargs):
            assert b.source_attempts_registered==len(seen)+1
            seen.append(request);r=requests.Response();r.status_code=200;r.raw=BytesIO(b'read-all');r.request=request;return r
        def close(self):pass
    with budget_scope(b),configure_source_session(requests.Session()) as session:
        session.mount('https://',Adapter())
        request=session.prepare_request(requests.Request('GET','https://controlled.invalid'))
        assert session.send(request=request,stream=False).content==b'read-all'
        with session.get('https://controlled.invalid',stream=True) as r:assert b''.join(r.iter_content())==b'read-all'
    assert len(seen)==2


def test_data_retry_sleep_cannot_consume_reserved_research_time():
    clock=[29.0];b=CloseStageBudget(0,90,lambda:clock[0]);waits=[]
    def sleep(seconds):waits.append(seconds);clock[0]+=seconds
    with budget_scope(b):
        bounded_data_sleep(sleep)(60)
        bounded_data_sleep(sleep)(60)
        b.enter_research()
        assert b.check('RESEARCH')==59
    assert waits==[1]


def test_real_model_client_still_reads_and_archives_original_stream_after_data_cutoff(tmp_path):
    import sqlite3
    from test_team_v2_risk_session import fixture
    from liangjian_funnel.runtime.team_v2_risk_session import run_risk_premarket
    kwargs,seen,raw=fixture(tmp_path)
    clock=[31.0];b=CloseStageBudget(0,90,lambda:clock[0])
    with budget_scope(b):
        b.enter_research()
        assert run_risk_premarket(**kwargs).status=='READY'
    assert len(seen)==1
    with sqlite3.connect(tmp_path/'model.db') as db:
        stored=db.execute('SELECT output_json,validated FROM model_calls').fetchone()
    assert json.loads(stored[0])==raw and stored[1]==1


def test_research_deadline_blocks_before_real_model_transport(tmp_path):
    from test_team_v2_risk_session import fixture
    kwargs,seen,_=fixture(tmp_path);clock=[0.0];b=CloseStageBudget(0,90,lambda:clock[0])
    with budget_scope(b):
        b.enter_research();clock[0]=59
        with pytest.raises(Exception,match='RESEARCH_STAGE_BUDGET_EXHAUSTED'):
            kwargs['client'].complete(kwargs['model'],[{'role':'user','content':'controlled'}])
    assert not seen


def test_latest_day_for_all_symbols_precedes_full_history_and_financial_sync(tmp_path):
    import time
    from test_pipeline_data_sync import FakeClient,NOW
    from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer
    from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
    cache=LocalFactCache(tmp_path/'facts.sqlite3');sync=HithinkIncrementalSynchronizer(cache)
    client=FakeClient();symbols=['000001.SZ','000002.SZ']
    receipt=sync.prime_latest_closed_days(client,symbols,as_of=NOW,deadline=time.monotonic()+60)
    assert client.calls==[('DAILY',s) for s in symbols]
    assert all(r['status']=='AVAILABLE_LATEST_ONLY' for r in receipt['records'])
    assert all(cache.get_sync_state('HITHINK_DAILY_1D',s) is None for s in symbols)
    assert all(cache.get_sync_state('HITHINK_DAILY_LATEST_PRIORITY',s)['status']=='AVAILABLE_LATEST_ONLY' for s in symbols)
    sync.sync(client,symbols,as_of=NOW)
    assert client.calls[:2]==[('DAILY',s) for s in symbols]
    assert any(kind!='DAILY' for kind,symbol in client.calls[2:])


def test_data_sync_expired_deadline_preserves_unavailable_not_empty_success(tmp_path):
    from test_pipeline_data_sync import FakeClient,NOW
    from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer
    from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
    cache=LocalFactCache(tmp_path/'facts.sqlite3');sync=HithinkIncrementalSynchronizer(cache);client=FakeClient()
    result=sync.sync(client,['000001.SZ'],as_of=NOW,deadline=0)
    assert not client.calls
    assert any('DATA_STAGE_BUDGET_EXHAUSTED' in r for r in result.failures['000001.SZ'])
    assert cache.get_sync_state('HITHINK_DAILY_1D','000001.SZ')['status']=='FAILED'


def test_disclosure_at_deadline_keeps_domain_and_explicit_unavailability(monkeypatch):
    import liangjian_funnel.pipeline.disclosure_pipeline as module
    monkeypatch.setattr(module.time,'monotonic',lambda:100)
    def unavailable(symbol):return symbol,{'ok':False,'complete':False,'reason_code':'DATA_STAGE_BUDGET_EXHAUSTED'},False,None,False
    def forbidden(symbol):raise AssertionError('NO_SOURCE_AFTER_DATA_DEADLINE')
    with module.DisclosurePipeline(forbidden,workers=1,deadline=99,
            unavailable_factory=unavailable) as pipeline:
        pipeline.submit('000001.SZ',input_hash='a'*64)
        pipeline.validate_domain(['000001.SZ'],{'000001.SZ':'a'*64})
        result=pipeline.results(['000001.SZ'])
        assert result[0][1]['ok'] is False
        with pytest.raises(module.DisclosurePipelineError,match='OUTSIDE_FINAL_DOMAIN'):
            pipeline.submit('extra',input_hash='b'*64)


def test_immediate_budget_notice_occurs_once_per_phase_before_finalization():
    clock=[30.0];events=[];b=CloseStageBudget(0,90,lambda:clock[0])
    b.on_exhausted=lambda phase,receipt:events.append((phase,receipt['phase']))
    for _ in range(2):
        with pytest.raises(CloseStageBudgetExceeded):b.check('DATA')
    assert events==[('DATA','DATA')]
    b.enter_research();assert not b.finalize_started


def test_close_observer_always_stops_and_receipt_error_preserves_original_failure(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import liangjian_funnel.workflow as module
    stopped=[]
    class Observer:
        def __init__(self,*args):pass
        def start(self):pass
        def stop(self,result,failed):stopped.append(failed)
    monkeypatch.delenv('LIANGJIAN_PARENT_CLOSE_DEADLINE_MS',raising=False)
    monkeypatch.setattr(module,'_CloseResourceObservation',Observer)
    monkeypatch.setattr(module,'atomic_write_json',lambda *a,**k: (_ for _ in ()).throw(OSError('CONTROLLED_RECEIPT_FAILURE')))
    settings=SimpleNamespace(workflow_output_dir=tmp_path,close_resource_sampling_enabled=True,research_close_deadline_seconds=90)
    app=SimpleNamespace(settings=settings)
    @module._enforce_close_stage_budget
    @module._observe_close_research_resources
    def failed(self,slot):raise ValueError('ORIGINAL_BUSINESS_FAILURE')
    with pytest.raises(ValueError,match='ORIGINAL_BUSINESS_FAILURE'):failed(app,'CLOSE')
    assert stopped==[True]
    @module._enforce_close_stage_budget
    @module._observe_close_research_resources
    def succeeded(self,slot):return {'status':'READY'}
    with pytest.raises(OSError,match='CONTROLLED_RECEIPT_FAILURE'):succeeded(app,'close')
    assert stopped==[True,False]


def test_bad_parent_budget_does_not_start_resource_observer(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import liangjian_funnel.workflow as module
    monkeypatch.setenv('LIANGJIAN_PARENT_CLOSE_DEADLINE_MS','not-a-deadline')
    monkeypatch.setattr(module,'_CloseResourceObservation',lambda *a:pytest.fail('OBSERVER_MUST_NOT_START'))
    app=SimpleNamespace(settings=SimpleNamespace(workflow_output_dir=tmp_path,research_close_deadline_seconds=90))
    wrapped=module._enforce_close_stage_budget(lambda self,slot:None)
    with pytest.raises(ValueError):wrapped(app,'close')


def test_latest_closed_day_keeps_real_intraday_timestamp(tmp_path):
    import time
    from datetime import timedelta
    from test_pipeline_data_sync import FakeClient,NOW
    from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer
    from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
    class ActualClockClient(FakeClient):
        def history_1d(self,symbol,**kwargs):
            result=super().history_1d(symbol,**kwargs)
            items=list(result.items)
            items[-1]=items[-1].model_copy(update={'date_ms':int(NOW.replace(hour=15,minute=0).timestamp()*1000)})
            return result.model_copy(update={'items':tuple(items)})
    cache=LocalFactCache(tmp_path/'facts.db');sync=HithinkIncrementalSynchronizer(cache)
    result=sync.prime_latest_closed_days(ActualClockClient(),['000001.SZ'],as_of=NOW,deadline=time.monotonic()+60)
    assert result['records'][0]['status']=='AVAILABLE_LATEST_ONLY'


def test_finished_workflow_cannot_report_success_after_finalize_deadline(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import liangjian_funnel.workflow as module
    clock=[0.0];b=CloseStageBudget(0,90,lambda:clock[0])
    monkeypatch.setattr(CloseStageBudget,'from_parent',classmethod(lambda cls,*a,**k:b))
    app=SimpleNamespace(settings=SimpleNamespace(workflow_output_dir=tmp_path,close_resource_sampling_enabled=False,research_close_deadline_seconds=90))
    @module._enforce_close_stage_budget
    def run(self,slot):
        b.enter_research();b.enter_finalize();clock[0]=2
        return {'status':'PUBLISHED'}
    with pytest.raises(CloseStageBudgetExceeded,match='FINALIZE_STAGE_BUDGET_EXHAUSTED'):run(app,'CLOSE')
    receipts=list((tmp_path/'close_stage_budgets').glob('*.json'))
    assert len(receipts)==1 and json.loads(receipts[0].read_text())['exhausted_stages']==['FINALIZE']


def test_precreated_source_client_retains_closed_owner_budget_for_late_worker():
    seen=[];b=CloseStageBudget(0,90,lambda:0)
    with configure_source_client(httpx.Client(transport=httpx.MockTransport(
            lambda request:(seen.append(request) or httpx.Response(200,content=b'ok',request=request))))) as client:
        with budget_scope(b):assert client.get('https://controlled.invalid/in-owner').content==b'ok'
        with pytest.raises(CloseStageBudgetExceeded):client.get('https://controlled.invalid/late-worker')
    assert len(seen)==1


def test_real_board_history_fallback_obeys_same_data_deadline_and_original_source(monkeypatch):
    from datetime import date
    from liangjian_funnel.data.board_history import fetch_board_history
    import liangjian_funnel.data.board_history as module
    original=httpx.Client;seen=[];clock=[0.0];b=CloseStageBudget(0,90,lambda:clock[0])
    def transport(request):
        assert request.url.host=='push2his.eastmoney.com' and request.url.path=='/api/qt/stock/kline/get'
        assert b.source_attempts_registered==len(seen)+1
        seen.append(request)
        return httpx.Response(200,json={'data':{'code':'BK0001','name':'CONTROLLED','klines':['2026-10-27,1,2,3,1,10']}},request=request)
    monkeypatch.setattr(module.httpx,'Client',lambda **kwargs:original(transport=httpx.MockTransport(transport),**kwargs))
    with budget_scope(b):
        assert fetch_board_history('BK0001',date(2026,10,27))['closes']=={'2026-10-27':2.0}
        clock[0]=30
        with pytest.raises(CloseStageBudgetExceeded):fetch_board_history('BK0001',date(2026,10,27))
    assert len(seen)==1


def test_sampler_disabled_does_not_disable_mandatory_close_budget(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import liangjian_funnel.workflow as module
    from liangjian_funnel.runtime.close_stage_budget import current_budget
    b=CloseStageBudget(0,90,lambda:30)
    monkeypatch.setattr(CloseStageBudget,'from_parent',classmethod(lambda cls,*a,**k:b))
    monkeypatch.setattr(module,'_CloseResourceObservation',lambda *a:pytest.fail('SAMPLER_DISABLED'))
    app=SimpleNamespace(settings=SimpleNamespace(workflow_output_dir=tmp_path,close_resource_sampling_enabled=False))
    @module._enforce_close_stage_budget
    @module._observe_close_research_resources
    def run(self,slot):current_budget().check('DATA')
    with pytest.raises(CloseStageBudgetExceeded,match='DATA_STAGE_BUDGET_EXHAUSTED'):run(app,'CLOSE')
    assert len(list((tmp_path/'close_stage_budgets').glob('*.json')))==1
    assert not (tmp_path/'resource_observations').exists()


def test_real_team_journal_preserves_safe_stage_budget_reason_without_http(tmp_path):
    import sqlite3
    from test_team_v2_risk_session import fixture
    from liangjian_funnel.runtime.team_v2_risk_session import run_risk_premarket
    kwargs,seen,_=fixture(tmp_path);clock=[0.0];b=CloseStageBudget(0,90,lambda:clock[0])
    with budget_scope(b):
        b.enter_research();clock[0]=59
        assert run_risk_premarket(**kwargs).status=='DEGRADED'
    assert seen==[]
    with sqlite3.connect(tmp_path/'model.db') as db:
        metrics=json.loads(db.execute('SELECT body_json FROM model_call_telemetry').fetchone()[0])
    assert metrics['failure_code']=='RESEARCH_STAGE_BUDGET_EXHAUSTED'
