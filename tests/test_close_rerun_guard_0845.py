"""Exact deployed model-client bytes, real stream, controlled transport only."""
from pathlib import Path
import importlib.util,json,subprocess,types,sys
from contextlib import closing
import httpx,pytest
from liangjian_funnel.settings import Settings
from fixture_model_stream import sse_model_response

OPS=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021')
spec=importlib.util.spec_from_file_location('rerun_guard',OPS/'close_rerun_guard_0845.py')
guard_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard_module)

def guard(tmp_path):
    return guard_module.CloseRerunGuard(directory=tmp_path,model_base='https://ai-api.finpoints.tech/v1',
        hithink_base='https://fuyao.aicubes.cn',cninfo_base='https://www.cninfo.com.cn',gov_base='https://sousuo.www.gov.cn')

def test_exact_h_stream_reaches_transport_after_budget_and_request_archive(tmp_path):
    root=Path(__file__).resolve().parents[1]
    raw=subprocess.check_output(['git','-C',str(root),'show','6eaaf7582e3a16c8e8e288d2521c3876a62a19ac:src/liangjian_funnel/pipeline/model_client.py'])
    module=types.ModuleType('liangjian_funnel.pipeline.exact_model_client_6eaaf75');module.__package__='liangjian_funnel.pipeline'
    sys.modules[module.__name__]=module
    exec(compile(raw,'exact_6eaaf75_model_client.py','exec'),module.__dict__)
    gate=guard(tmp_path);seen=[]
    def controlled(request):
        assert gate.counts['MODEL']==1
        assert (tmp_path/'MODEL_REQUEST_001.json').read_bytes()==request.content
        assert any(row['kind']=='HTTP_DISPATCH' for row in gate.rows)
        seen.append(request.content)
        return sse_model_response(200,json={'choices':[{'message':{'content':'{"ok":true}'}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED_NOT_A_SECRET'},root=tmp_path)
    client=module.OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(controlled),max_attempts=1)
    with gate.install():
        out=client.complete(settings.review_model,[{'role':'user','content':'CONTROLLED'}],timeout_seconds=5)
    assert out.output=={'ok':True} and len(seen)==1
    assert (tmp_path/'HTTP_AUDIT.jsonl').is_file()

@pytest.mark.parametrize('keyword',[False,True])
def test_send_positional_keyword_and_stream_close_preserved(tmp_path,keyword):
    gate=guard(tmp_path);closed=[]
    class Stream(httpx.SyncByteStream):
        def __iter__(self):yield b'CONTROLLED'
        def close(self):closed.append(True)
    with httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(200,stream=Stream(),request=r))) as client,gate.install():
        request=client.build_request('GET','https://fuyao.aicubes.cn/api/meta/tickers/list')
        with closing(client.send(request=request,stream=True) if keyword else client.send(request,stream=True)) as response:
            assert b''.join(response.iter_bytes())==b'CONTROLLED'
    assert closed==[True] and gate.counts['GLOBAL']==1

@pytest.mark.parametrize('url',['https://evil.example/api','https://fuyao.aicubes.cn:444/api/x','https://fuyao.aicubes.cn:80/api/x','https://ai-api.finpoints.tech/v1/models','https://fuyao.aicubes.cn/not-api','https://proxy.finance.qq.com/not-original'])
def test_unknown_host_port_path_rejected_before_transport(tmp_path,url):
    gate=guard(tmp_path);seen=[]
    with httpx.Client(transport=httpx.MockTransport(lambda r:seen.append(r))) as client,gate.install(),pytest.raises(guard_module.GuardStop):client.get(url)
    assert seen==[]

def test_global_budget_40_and_no_retry_after_failure(tmp_path):
    gate=guard(tmp_path);seen=[]
    with httpx.Client(transport=httpx.MockTransport(lambda r:(seen.append(r),httpx.Response(200,request=r))[1])) as client,gate.install():
        for i in range(40):client.get('https://fuyao.aicubes.cn/api/global',params={'page':i})
        with pytest.raises(guard_module.GuardStop):client.get('https://fuyao.aicubes.cn/api/global?page=40')
    assert len(seen)==40
    gate=guard(tmp_path)
    with httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(429,request=r))) as client,gate.install():
        client.get('https://fuyao.aicubes.cn/api/global')
        with pytest.raises(guard_module.GuardStop):client.get('https://fuyao.aicubes.cn/api/global')
    assert gate.counts['GLOBAL']==1 and gate.rate_limited==1

def test_redirect_cannot_escape_approved_origin(tmp_path):
    gate=guard(tmp_path);seen=[]
    with httpx.Client(transport=httpx.MockTransport(lambda r:(seen.append(r),httpx.Response(302,headers={'location':'https://evil.example/'},request=r))[1])) as client,gate.install(),pytest.raises(guard_module.GuardStop):
        client.get('https://fuyao.aicubes.cn/api/global',follow_redirects=True)
    assert len(seen)==1

def test_global_adapter_retries_zero_and_original_member_endpoint_now_forbidden(tmp_path):
    import requests
    from urllib3.util.retry import Retry
    gate=guard(tmp_path);seen=[]
    class Adapter(requests.adapters.HTTPAdapter):
        def send(self,request,**kwargs):
            seen.append(self.max_retries.total)
            response=requests.Response();response.status_code=200;response._content=b'CONTROLLED';response.request=request
            return response
    with requests.Session() as client,gate.install():
        adapter=Adapter(max_retries=Retry(total=3));client.mount('https://',adapter)
        response=client.get('https://fuyao.aicubes.cn/api/meta/tickers/list')
        assert response.content==b'CONTROLLED' and adapter.max_retries.total==3
        with pytest.raises(guard_module.GuardStop):
            client.get('https://proxy.finance.qq.com/cgi/cgi-bin/fundflow/hsfundtab')
    assert seen==[0] and gate.counts['GLOBAL']==1
