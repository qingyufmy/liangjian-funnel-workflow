"""Injected transports only: never contact a real Node/HTTP service."""
from datetime import datetime,timedelta
import hashlib,json
from pathlib import Path
import pytest
from liangjian_funnel.runtime import shadow_node_observer as mod

DAY='2026-10-12'
def at(t):return datetime.fromisoformat(DAY+'T'+t+'+08:00')
def raw(now=None,start=None,pid=100):
    return json.dumps(dict(recentJobRuns=[],node=dict(pid=pid,
        startedAtEpochMs=(start or at('08:00:00')).timestamp()*1000,
        observedAt=(now or at('16:00:00')).isoformat()))).encode()
def sampler(tmp_path,clock,calls,**kw):
    def transport(*args,**kwargs):
        calls.append(kwargs)
        return dict(status=200,raw=raw(clock()),content_type='application/json',encoding='identity')
    return mod.LoopbackNodeSampler('http://127.0.0.1:3210/api/shadow/job-runs-readonly',
        tmp_path/'archive',clock=clock,transport=transport,**kw)

def test_original_bytes_same_identity_and_minute_rate(tmp_path):
    now=[at('16:00:00')];calls=[];s=sampler(tmp_path,lambda:now[0],calls)
    r=s.sample_if_due(observed_at=now[0]);assert r['status']=='READY'
    assert hashlib.sha256(Path(r['raw_path']).read_bytes()).hexdigest()==r['raw_sha256']
    now[0]=at('16:00:05');assert s.sample_if_due(observed_at=now[0])['status']=='READY'
    assert len(calls)==1
    now[0]=at('16:01:00');s.sample_if_due(observed_at=now[0]);assert len(calls)==2
    # Restart the independent client: no extra request in an already sampled slot.
    s=sampler(tmp_path,lambda:now[0],calls);s.sample_if_due(observed_at=now[0]);assert len(calls)==2

@pytest.mark.parametrize('url',['http://localhost:3210/api/shadow/job-runs-readonly',
    'http://192.168.1.254:3210/api/shadow/job-runs-readonly',
    'http://127.0.0.1:3210/api/overview','http://user@127.0.0.1:3210/api/shadow/job-runs-readonly',
    'http://127.0.0.1:3210/api/shadow/job-runs-readonly?token=x',
    'https://127.0.0.1:3210/api/shadow/job-runs-readonly'])
def test_endpoint_not_explicit_loopback_fixed_path_rejected(tmp_path,url):
    with pytest.raises(ValueError):mod.LoopbackNodeSampler(url,tmp_path/'archive')

def test_missing_identity_wrong_clock_restart_and_raw_cap():
    good=dict(trade_date=DAY,requested_at=at('16:00:00'),received_at=at('16:00:01'))
    assert mod.inspect_node_sample(raw(),**good)['status']=='READY'
    for value in (b'{"recentJobRuns":[]}',raw(at('16:00:02')),raw(start=at('16:00:01')),
                  b'{"recentJobRuns":[],"node":null}',b'x'*(1024*1024+1)):
        assert mod.inspect_node_sample(value,**good)['status']=='DATA_LIMITED'
    assert mod.inspect_node_sample(raw(),previous_node={'pid':101,'startedAtEpochMs':1},**good)['status']=='DATA_LIMITED'
    assert mod.inspect_node_sample(raw(),**{**good,'received_at':at('15:59:59')})['status']=='DATA_LIMITED'

def test_failure_no_old_success_fallback_no_token_output(tmp_path,monkeypatch):
    now=[at('16:00:00')];calls=[];s=sampler(tmp_path,lambda:now[0],calls)
    assert s.sample_if_due(observed_at=now[0])['status']=='READY'
    monkeypatch.setenv('LIANGJIAN_DASHBOARD_TOKEN','fixture-not-real-secret')
    def fail(*args,**kw):raise OSError('fixture-not-real-secret')
    s.transport=fail;now[0]=at('16:01:00')
    r=s.sample_if_due(observed_at=now[0]);assert r['status']=='DATA_LIMITED' and not r.get('raw_path')
    assert 'fixture-not-real-secret' not in json.dumps(r)
    assert all(b'fixture-not-real-secret' not in p.read_bytes() for p in (tmp_path/'archive').rglob('*.json'))

def test_before_window_after_fence_and_total_elapsed_budget(tmp_path):
    now=[at('15:59:59')];calls=[];s=sampler(tmp_path,lambda:now[0],calls)
    assert s.sample_if_due(observed_at=now[0])['status']=='NOT_DUE' and not calls
    now[0]=at('16:45:01');assert s.sample_if_due(observed_at=now[0])['status']=='NOT_DUE' and not calls
    now[0]=at('16:00:00');values=iter([0.,2.1])
    s=sampler(tmp_path,lambda:now[0],calls,monotonic=lambda:next(values))
    assert s.sample_if_due(observed_at=now[0])['status']=='DATA_LIMITED'

def test_node_restart_sticky_across_client_restart(tmp_path):
    now=[at('16:00:00')];calls=[];s=sampler(tmp_path,lambda:now[0],calls)
    s.sample_if_due(observed_at=now[0]);now[0]=at('16:01:00')
    s.transport=lambda *a,**kw:dict(status=200,raw=raw(now[0],pid=101),content_type='application/json',encoding='identity')
    assert s.sample_if_due(observed_at=now[0])['reason_codes']==['NODE_RESTART_OR_HISTORY_UNPROVEN']
    now[0]=at('16:02:00');s=sampler(tmp_path,lambda:now[0],calls)
    assert s.sample_if_due(observed_at=now[0])['status']=='DATA_LIMITED'

def test_archived_bytes_tamper_refuses_setup(tmp_path):
    now=at('16:00:00');s=sampler(tmp_path,lambda:now,[]);r=s.sample_if_due(observed_at=now)
    Path(r['raw_path']).write_bytes(b'changed')
    with pytest.raises(ValueError):sampler(tmp_path,lambda:now,[])

def test_new_node_original_can_complete_only_with_real_sample_pin(tmp_path):
    from test_w3_shadow_a5_observer import fixture
    from liangjian_funnel.runtime.shadow_a5_observer import inspect_a5_completion
    value=fixture();now=value['observed_at'];obj=json.loads(value['node_receipt_raw'])
    obj['node']=json.loads(raw(now))['node'];body=json.dumps(obj).encode()
    s=mod.LoopbackNodeSampler('http://127.0.0.1:3210/api/shadow/job-runs-readonly',tmp_path/'archive',
        clock=lambda:now,transport=lambda *a,**kw:dict(status=200,raw=body,content_type='application/json',encoding='identity'))
    receipt=s.sample_if_due(observed_at=now)
    value['node_receipt_raw']=body
    good=inspect_a5_completion(**value,node_sample=receipt,require_node_identity=True)
    assert good['status']=='A5_COMPLETED_OBSERVED' and good['node_pid']==100
    assert inspect_a5_completion(**value,require_node_identity=True)['status']=='DATA_LIMITED'
    receipt['raw_sha256']='0'*64
    assert inspect_a5_completion(**value,node_sample=receipt,require_node_identity=True)['status']=='DATA_LIMITED'

def test_reporting_tick_client_failure_never_uses_old_node_file(tmp_path):
    from test_w3_shadow_reporting import fixture,at
    from liangjian_funnel.runtime.shadow_reporting import run_reporting_tick
    cfg=fixture(tmp_path)
    s=mod.LoopbackNodeSampler('http://127.0.0.1:3210/api/shadow/job-runs-readonly',cfg.archive_root/'node-original',
        clock=lambda:at('16:08:00'),transport=lambda *a,**kw:dict(status=302,raw=b'{}',content_type='application/json',encoding='identity'))
    run_reporting_tick(cfg,observed_at=at('15:30:00'),node_sampler=s)
    assert run_reporting_tick(cfg,observed_at=at('16:08:00'),node_sampler=s)['status']=='WAIT_A5'
    r=run_reporting_tick(cfg,observed_at=at('16:45:01'),node_sampler=s)
    assert r['a5_status']=='A5_NOT_COMPLETE_OR_AMBIGUOUS'

def test_slow_original_response_is_preserved_but_not_ready(tmp_path):
    now=at('16:00:00');values=iter([0.,2.1])
    s=sampler(tmp_path,lambda:now,[],monotonic=lambda:next(values))
    r=s.sample_if_due(observed_at=now)
    assert r['status']=='DATA_LIMITED' and Path(r['raw_path']).read_bytes()==raw(now)

def test_cached_original_tamper_is_not_ready(tmp_path):
    now=at('16:00:00');s=sampler(tmp_path,lambda:now,[]);r=s.sample_if_due(observed_at=now)
    Path(r['raw_path']).write_bytes(b'changed')
    assert s.sample_if_due(observed_at=now+timedelta(seconds=5))['status']=='DATA_LIMITED'

def test_default_transport_does_not_follow_redirect_or_proxy(monkeypatch):
    calls=[]
    class Socket:
        def settimeout(self,t):assert 0<t<=2
    class Response:
        status=302
        def getheader(self,k,default):return {'Content-Type':'application/json','Location':'http://external/'}.get(k,default)
        def read1(self,n):return b''
    class Connection:
        def __init__(self,host,port,timeout):calls.append((host,port,timeout));self.sock=Socket()
        def request(self,method,path,headers):calls.append((method,path,headers))
        def getresponse(self):return Response()
        def close(self):calls.append('closed')
    monkeypatch.setenv('HTTP_PROXY','http://external-proxy:9999')
    monkeypatch.setattr(mod.http.client,'HTTPConnection',Connection)
    r=mod._http_transport('http://127.0.0.1:3210/api/shadow/job-runs-readonly',headers={},timeout=2,monotonic=lambda:0)
    assert r['status']==302 and calls[0]==('127.0.0.1',3210,2)
    assert calls[1][0]=='GET' and len(calls)==3

@pytest.mark.parametrize('body',[
    b'{"recentJobRuns":[],"recentJobRuns":[],"node":{}}',
    b'{"recentJobRuns":[],"node":{"pid":true,"startedAtEpochMs":1,"observedAt":"2026-10-12T16:00:00+08:00"}}',
    b'{"recentJobRuns":[],"node":{"pid":1,"startedAtEpochMs":NaN,"observedAt":"2026-10-12T16:00:00+08:00"}}',
    raw(datetime.fromisoformat('2026-10-13T16:00:00+08:00'))])
def test_malformed_untrusted_source_is_not_ready(body):
    assert mod.inspect_node_sample(body,trade_date=DAY,requested_at=at('16:00:00'),received_at=at('16:00:01'))['status']=='DATA_LIMITED'

def test_cli_endpoint_rejects_archive_fallback_and_template_is_loopback(capsys):
    import importlib.util
    root=Path(__file__).parents[1];spec=importlib.util.spec_from_file_location('report',root/'scripts/run_shadow_reporting.py')
    cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
    argv=[]
    for p in ('state-db','shadow-db','approved-output-root','node-log-root','archive-root','report-root','bridge-outbox'):
        argv+=['--'+p,'unused']
    argv+=['--lane','lane_1','--node-receipt','old.json','--node-endpoint','http://127.0.0.1:3210/api/shadow/job-runs-readonly',
        '--node-sample-archive','unused','--as-of',at('16:00:00').isoformat()]
    assert cli.main(argv)==2 and 'SHADOW_SETUP_FAILED' in capsys.readouterr().out
    unit=(root/'config/deploy/shadow-reporting/liangjian-shadow-reporting.service.example').read_text(encoding='utf8')
    assert '--node-endpoint ${SHADOW_NODE_ENDPOINT}' in unit and '--node-receipt' not in unit
    assert 'IPAddressDeny=any' in unit and 'IPAddressAllow=localhost' in unit

def test_unwritable_archive_cannot_start_request_or_forget_pending_slot(tmp_path,monkeypatch):
    now=at('16:00:00');calls=[];s=sampler(tmp_path,lambda:now,calls)
    monkeypatch.setattr(mod,'_write',lambda *args:(_ for _ in ()).throw(OSError('fixture')))
    assert s.sample_if_due(observed_at=now)['status']=='DATA_LIMITED' and not calls

def test_killed_request_reservation_cannot_be_replayed_in_same_slot(tmp_path):
    now=[at('16:00:00')];calls=[];s=sampler(tmp_path,lambda:now[0],calls)
    s.transport=lambda *a,**kw:(_ for _ in ()).throw(KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):s.sample_if_due(observed_at=now[0])
    now[0]=at('16:00:05');s=sampler(tmp_path,lambda:now[0],calls)
    r=s.sample_if_due(observed_at=now[0])
    assert r['status']=='DATA_LIMITED' and r['reason_codes']==['NODE_REQUEST_UNFINISHED'] and not calls

def test_monotonic_backwards_keeps_response_but_cannot_complete(tmp_path):
    now=at('16:00:00');values=iter([1.,0.])
    s=sampler(tmp_path,lambda:now,[],monotonic=lambda:next(values))
    assert s.sample_if_due(observed_at=now)['reason_codes']==['NODE_SAMPLE_TOTAL_BUDGET_EXCEEDED']

def test_actual_request_clock_past_fence_cannot_start_http(tmp_path):
    calls=[];s=sampler(tmp_path,lambda:at('16:45:01'),calls)
    assert s.sample_if_due(observed_at=at('16:44:59'))['status']=='DATA_LIMITED' and not calls

def test_uppercase_transport_secret_is_never_an_error_code(tmp_path):
    now=at('16:00:00');s=sampler(tmp_path,lambda:now,[])
    s.transport=lambda *a,**kw:(_ for _ in ()).throw(OSError('PRIVATE_BEARER_SECRET'))
    r=s.sample_if_due(observed_at=now)
    assert 'PRIVATE_BEARER_SECRET' not in json.dumps(r)
    assert all(b'PRIVATE_BEARER_SECRET' not in p.read_bytes() for p in (tmp_path/'archive').rglob('*.json'))
