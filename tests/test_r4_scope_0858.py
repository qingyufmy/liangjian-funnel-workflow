"""Original provider/client paths, controlled transport, no real source/model."""
from pathlib import Path
from datetime import datetime
import importlib.util,json
import httpx,pytest
from test_close_rerun_guard_0845 import guard,guard_module
from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.data.bse import BseClient
from liangjian_funnel.settings import Settings
O=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021')
spec=importlib.util.spec_from_file_location('global_cache_0857',O/'global_typed_cache_0857.py')
cache=importlib.util.module_from_spec(spec);spec.loader.exec_module(cache)

def test_original_bse307_is_explicit_unavailable_and_never_follows(tmp_path):
    gate=guard(tmp_path);sent=[]
    def transport(request):
        sent.append(request)
        return httpx.Response(307,headers={'location':'https://evil.example/private-target?opaque=CONTROLLED'},request=request)
    client=BseClient(transport=httpx.MockTransport(transport),sleep=lambda _:None)
    with gate.install():result=client.fetch_announcements('920012.BJ','2026-10-26','2026-10-27')
    assert not result.ok and not result.complete and result.reason_code=='BSE_HTTP_ERROR'
    assert len(sent)==1 and not gate.stopped and gate.counts['CNINFO_CACHE_MISS']==1
    raw=json.loads((tmp_path/'REDIRECT_001.private.json').read_bytes())
    assert raw['location'].endswith('opaque=CONTROLLED') and raw['followed'] is False
    assert 'opaque=CONTROLLED' not in (tmp_path/'HTTP_AUDIT.jsonl').read_text()

def test_twenty_same_type_redirects_remain_unavailable_twentyfirst_stops(tmp_path):
    gate=guard(tmp_path);seen=[]
    with httpx.Client(transport=httpx.MockTransport(lambda r:(seen.append(r),httpx.Response(307,headers={'location':'https://evil.example/'},request=r))[1])) as client,gate.install():
        for i in range(20):assert client.get('https://www.bse.cn/disclosureInfoController/companyAnnouncement.do').status_code==307
        with pytest.raises(guard_module.GuardStop):client.get('https://www.bse.cn/disclosureInfoController/companyAnnouncement.do')
    assert len(seen)==21 and gate.stopped

def test_typed_global_original_replayed_with_original_clock_and_zero_extra_http(tmp_path):
    settings=Settings.from_env({'HITHINK_FINANCE_API_KEY':'CONTROLLED','ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS':'0'},root=tmp_path)
    sent=[]
    def transport(request):
        sent.append(request)
        return httpx.Response(200,json={'code':0,'data':{'timestamp':1,'item':[{'thscode':'881101.TI','name':'CONTROLLED'}]}},request=request)
    gate=guard(tmp_path)
    with HithinkClient(settings,transport=httpx.MockTransport(transport),sleep=lambda _:None) as client,cache.install_global_typed_cache(
            directory=tmp_path/'globals',source_head='H',source_day='2026-10-27'),gate.install():
        first=client.ths_index_catalog(tag='industry');second=client.ths_index_catalog(tag='industry')
        assert first.ok and first.model_dump(mode='json')==second.model_dump(mode='json')
    assert len(sent)==1 and gate.counts['GLOBAL']==1
    files=list((tmp_path/'globals').glob('*.json'));assert len(files)==1
    body=json.loads(files[0].read_bytes());body['result']['fetch_time']='2026-10-27T00:00:00+08:00';files[0].write_text(json.dumps(body))
    with HithinkClient(settings,transport=httpx.MockTransport(transport),sleep=lambda _:None) as client,cache.install_global_typed_cache(
            directory=tmp_path/'globals',source_head='H',source_day='2026-10-27'),pytest.raises(cache.GlobalCacheStop):
        client.ths_index_catalog(tag='industry')
    assert len(sent)==1

def test_supervisor_notification_uses_actual_outbox_sender_once(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    from liangjian_funnel.runtime.team_v2_internal import InternalOutbox
    import socket
    spec=importlib.util.spec_from_file_location('audit_notify_0858',O/'notify_audit_terminal_0858_vm.py')
    notify=importlib.util.module_from_spec(spec);spec.loader.exec_module(notify)
    root=tmp_path/'root';audit=tmp_path/'owned-audit';audit.mkdir()
    (audit/'TERMINAL.json').write_text(json.dumps(dict(exit_code=76,error_code='AUDITED_REDIRECT_FORBIDDEN')))
    store=TeamMeetingStore(root/'state/team_v2/team.db');InternalOutbox(store)
    env=root.parent/'liangjian-shadow/internal_alert.env';env.parent.mkdir();env.write_text('SHADOW_RESEARCH_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/CONTROLLED\n')
    actual_client=httpx.Client;sent=[]
    def transport(request):sent.append(request.read());return httpx.Response(200,json={'code':0,'msg':'success'},request=request)
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:(_ for _ in ()).throw(AssertionError('REAL_NETWORK_FORBIDDEN')))
    monkeypatch.setattr(httpx,'Client',lambda *a,**kw:actual_client(*a,**dict(kw,transport=httpx.MockTransport(transport))))
    first=notify.notify(root,audit,exit_code=76);second=notify.notify(root,audit,exit_code=76)
    assert first['delivery']['sent']==1 and second['status']=='ALREADY_CLAIMED_OR_SENT' and len(sent)==1


def test_policy_is_durable_before_main_and_original_later_call_reuses(tmp_path):
    from test_gov_policy_adapter import page
    from liangjian_funnel.data.gov_policy import GovPolicyClient
    from datetime import date,timedelta
    settings=Settings.from_env({},root=tmp_path);sent=[];gate=guard(tmp_path)
    def transport(request):
        sent.append(request)
        return httpx.Response(200,json=page(request,gongwen=[],bumenfile=[],gongwen_total=0,bumen_total=0),request=request)
    with cache.install_global_typed_cache(directory=tmp_path/'globals',source_head='H'),gate.install():
        first=cache.prime_original_policy(settings,source_day='2026-10-27',transport=httpx.MockTransport(transport))
        before=len(sent);assert before>0 and len(list((tmp_path/'globals').glob('*.json')))==1
        with GovPolicyClient(transport=httpx.MockTransport(transport)) as client:
            second=client.fetch_documents((date(2026,10,27)-timedelta(days=90)).isoformat(),'2026-10-27')
        assert len(sent)==before and first.model_dump(mode='json')==second.model_dump(mode='json')


def test_original_news_rss_is_not_global_budget_and_retains_exact_typed_original(tmp_path):
    from liangjian_funnel.data.open_news import OpenNewsClient
    gate=guard_module.CloseRerunGuard(directory=tmp_path,model_base='https://ai-api.finpoints.tech/v1',hithink_base='https://fuyao.aicubes.cn',
        cninfo_base='https://www.cninfo.com.cn',gov_base='https://sousuo.www.gov.cn',rss_hosts=['rss.example'],news_urls=['https://rss.example/feed'])
    gate.counts['GLOBAL']=40;sent=[]
    xml='<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>CONTROLLED</title><link href="https://rss.example/item"/><updated>2026-10-27T15:00:00+08:00</updated><summary>CONTROLLED</summary></entry></feed>'
    def transport(request):sent.append(request);return httpx.Response(200,text=xml,request=request)
    with OpenNewsClient(transport=httpx.MockTransport(transport)) as client,cache.install_global_typed_cache(directory=tmp_path/'typed',source_head='H'),gate.install():
        first=client.fetch_rss('https://rss.example/feed',source_id='open_news.rss.controlled',channel='controlled')
        second=client.fetch_rss('https://rss.example/feed',source_id='open_news.rss.controlled',channel='controlled')
        assert first.ok and first.model_dump(mode='json')==second.model_dump(mode='json')
    assert len(sent)==1 and gate.counts['GLOBAL']==40 and gate.counts['NEWS']==1
    assert gate.classify('https://rss.example/not-approved') is None
    assert gate.classify('https://rss.example:444/feed') is None
    assert gate.classify('https://www.cls.cn/other-path') is None


def test_news_redirect_never_followed_and_failed_original_stays_unavailable(tmp_path):
    from liangjian_funnel.data.open_news import OpenNewsClient
    gate=guard_module.CloseRerunGuard(directory=tmp_path,model_base='https://ai-api.finpoints.tech/v1',hithink_base='https://fuyao.aicubes.cn',
        cninfo_base='https://www.cninfo.com.cn',gov_base='https://sousuo.www.gov.cn',news_urls=['https://rss.example/feed'])
    seen=[]
    def transport(request):seen.append(request);return httpx.Response(302,headers={'location':'https://evil.example/unknown'},request=request)
    with OpenNewsClient(transport=httpx.MockTransport(transport)) as client,cache.install_global_typed_cache(directory=tmp_path/'typed',source_head='H'),gate.install():
        first=client.fetch_rss('https://rss.example/feed',source_id='open_news.rss.controlled',channel='controlled')
        second=client.fetch_rss('https://rss.example/feed',source_id='open_news.rss.controlled',channel='controlled')
    assert len(seen)==1 and not first.ok and first.reason_code=='OPEN_NEWS_HTTP_ERROR'
    assert first.model_dump(mode='json')==second.model_dump(mode='json') and not gate.stopped


def test_news_failure_keeps_typed_cause_without_private_exception_text(tmp_path):
    from liangjian_funnel.data.open_news import OpenNewsClient
    gate=guard_module.CloseRerunGuard(directory=tmp_path,model_base='https://ai-api.finpoints.tech/v1',hithink_base='https://fuyao.aicubes.cn',
        cninfo_base='https://www.cninfo.com.cn',gov_base='https://sousuo.www.gov.cn',news_urls=['https://rss.example/feed'])
    def transport(request):
        try:raise OSError('AUDITED_UNAPPROVED_NETWORK')
        except OSError as cause:raise httpx.ConnectError('PRIVATE_NOT_ARCHIVED',request=request) from cause
    with OpenNewsClient(transport=httpx.MockTransport(transport)) as client,gate.install():
        result=client.fetch_rss('https://rss.example/feed',source_id='open_news.rss.controlled',channel='controlled')
    assert not result.ok and result.reason_code=='OPEN_NEWS_REQUEST_FAILED'
    row=next(x for x in gate.rows if x['kind']=='HTTP_EXCEPTION')
    assert row['cause_chain'][1]['audited_network_denied'] and row['frames']
    assert 'PRIVATE_NOT_ARCHIVED' not in (tmp_path/'HTTP_AUDIT.jsonl').read_text()


def test_every_real_production_rss_endpoint_matches_original_normalization(tmp_path):
    from liangjian_funnel.data.open_news import normalize_url
    root=Path(__file__).resolve().parents[1]
    cfg=json.loads((root/'config/news_sources.json').read_bytes())
    urls=[row['url'] for row in cfg['sources'] if row.get('type')=='rss' and row.get('url')]
    assert len(urls)>20
    gate=guard_module.CloseRerunGuard(directory=tmp_path,model_base='https://ai-api.finpoints.tech/v1',hithink_base='https://fuyao.aicubes.cn',
        cninfo_base='https://www.cninfo.com.cn',gov_base='https://sousuo.www.gov.cn',news_urls=urls)
    for original in urls:
        assert gate.classify(normalize_url(original))=='NEWS',original
    assert gate.classify('https://research.google/blog/other') is None
    assert gate.classify('https://api.quantamagazine.org/not-feed') is None


def test_original_rss_with_trailing_slash_reaches_transport_without_scope_escape(tmp_path):
    from liangjian_funnel.data.open_news import OpenNewsClient
    sent=[]
    gate=guard_module.CloseRerunGuard(directory=tmp_path,model_base='https://ai-api.finpoints.tech/v1',hithink_base='https://fuyao.aicubes.cn',
        cninfo_base='https://www.cninfo.com.cn',gov_base='https://sousuo.www.gov.cn',news_urls=['https://research.google/blog/rss/'])
    with OpenNewsClient(transport=httpx.MockTransport(lambda r:(sent.append(r),httpx.Response(503,request=r))[1]),max_retries=1) as client,gate.install():
        result=client.fetch_rss('https://research.google/blog/rss/',source_id='open_news.rss.controlled',channel='controlled')
    assert len(sent)==1 and str(sent[0].url)=='https://research.google/blog/rss'
    assert not result.ok and result.reason_code=='OPEN_NEWS_HTTP_5XX' and not gate.stopped


def test_unknown_original_news_is_unavailable_no_http_until_twentyfirst(tmp_path):
    from liangjian_funnel.data.open_news import OpenNewsClient
    gate=guard(tmp_path);sent=[]
    with OpenNewsClient(transport=httpx.MockTransport(lambda r:sent.append(r))) as client,gate.install():
        for n in range(20):
            out=client.fetch_rss('https://unknown.example/feed',source_id='open_news.rss.controlled',channel='controlled')
            assert not out.ok and not gate.stopped
        with pytest.raises(guard_module.GuardStop,match='SOURCE_UNAVAILABLE_LIMIT'):
            client.fetch_rss('https://unknown.example/feed',source_id='open_news.rss.controlled',channel='controlled')
    assert not sent and gate.counts['GLOBAL']==0 and gate.counts['NEWS']==0
    assert gate.source_unavailable_counts['NEWS']==21
    assert len(list(tmp_path.glob('SOURCE_UNAVAILABLE_*.private.json')))==21


def test_unknown_company_canonical_and_alias_are_unavailable_but_global_is_fatal(tmp_path):
    settings=Settings.from_env({'HITHINK_FINANCE_API_KEY':'CONTROLLED','ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS':'0'},root=tmp_path).model_copy(update={'hithink_base_url':'https://unknown.example'})
    gate=guard(tmp_path);sent=[]
    with HithinkClient(settings,transport=httpx.MockTransport(lambda r:sent.append(r)),sleep=lambda _:None) as client,gate.install():
        assert not client.income_statements('000858.SZ').ok
        assert not client.fetch_income_statements('000858.SZ').ok
        assert not gate.stopped and not sent
        with pytest.raises(guard_module.GuardStop):client.ticker_catalog()
    assert not sent and gate.source_unavailable_counts['COMPANY_INCREMENTAL']>=2


@pytest.mark.parametrize('url',['https://query.sse.com.cn/security/stock/queryCompanyBulletin.do','https://www.szse.cn/api/disc/announcement/annList'])
def test_original_declared_exchange_paths_only(url,tmp_path):
    gate=guard(tmp_path)
    assert gate.classify(url)=='CNINFO_CACHE_MISS'
    from urllib.parse import urlsplit
    u=urlsplit(url)
    assert gate.classify(u.scheme+'://'+u.netloc+'/unapproved') is None


def test_extended_clock_keeps_actual_origin_and_conditional_a2_a3_policy(tmp_path):
    spec=importlib.util.spec_from_file_location('audit_clock',O/'audit_clock_0869.py');clock=importlib.util.module_from_spec(spec);spec.loader.exec_module(clock)
    binding=dict(absolute_deadline_at='2026-10-27T22:45:00+08:00',authorized_cycle_started_at='2026-10-27T19:45:21+08:00')
    now=datetime.fromisoformat('2026-10-27T21:15:00+08:00').timestamp();deadline,env=clock.clock_values(binding,now)
    assert deadline==datetime.fromisoformat(binding['absolute_deadline_at']).timestamp()
    assert int(env['LIANGJIAN_PARENT_JOB_STARTED_MS'])==int(datetime.fromisoformat(binding['authorized_cycle_started_at']).timestamp()*1000)
    assert int(env['LIANGJIAN_PARENT_JOB_BUDGET_MS'])==10779000
    assert int(env['LIANGJIAN_RESEARCH_CLOSE_DEADLINE_SECONDS'])==86400
    # Stage timeout must not autonomously stop qualifying A2/A3 at22:45;
    # the supervisor still owns the approved deadline/data-stage termination.
    for phase,expected in [('COMPANY_FACT_SYNC',False),('RESEARCH_A1',False),('RESEARCH_A2',True),('RESEARCH_A3',True)]:
        assert clock.model_progress_requires_claude(dict(started_at='2026-10-27T21:15:01+08:00',status='RUNNING',phase=phase),now) is expected
    assert not clock.model_progress_requires_claude(dict(started_at='2026-10-27T19:00:00+08:00',status='RUNNING',phase='RESEARCH_A2'),now)
    path=tmp_path/'extension.json';path.write_text(json.dumps(dict(binding_sha256='a'*64,approved_by='Claude',ruling_sha256='b'*64,absolute_deadline_at='2026-10-28T00:15:00+08:00')))
    extended=clock.approved_extension(path,binding_sha256='a'*64,current_deadline=deadline)
    assert extended>deadline and clock.approved_extension(path,binding_sha256='a'*64,current_deadline=extended) is None
    with pytest.raises(ValueError):clock.approved_extension(path,binding_sha256='c'*64,current_deadline=deadline)


def test_approved_close_dispatch_window_real_scheduler_uses_real_current_time(tmp_path):
    from liangjian_funnel.runtime.scheduler import Scheduler,ScheduleKind
    from datetime import datetime
    spec=importlib.util.spec_from_file_location('clock_window',O/'audit_clock_0869.py');clock=importlib.util.module_from_spec(spec);spec.loader.exec_module(clock)
    scheduler=Scheduler(object())
    binding=dict(source_day='2026-10-27',absolute_deadline_at='2026-10-27T22:45:00+08:00')
    late=datetime.fromisoformat('2026-10-27T21:30:00+08:00')
    assert not scheduler._due_jobs(late)
    with clock.approved_close_dispatch_window(binding):
        jobs=scheduler._due_jobs(late)
        assert len(jobs)==1 and jobs[0].kind is ScheduleKind.CLOSE_1510
        assert jobs[0].due.isoformat()=='2026-10-27T15:10:00+08:00'
        assert not scheduler._is_missed(jobs[0],late)
        assert scheduler._is_missed(jobs[0],datetime.fromisoformat('2026-10-27T22:45:01+08:00'))
        assert not scheduler._due_jobs(datetime.fromisoformat('2026-10-27T22:45:01+08:00'))
        assert not scheduler._due_jobs(datetime.fromisoformat('2026-10-28T21:30:00+08:00'))
        early=datetime.fromisoformat('2026-10-27T14:00:00+08:00')
        assert not any(j.kind is ScheduleKind.CLOSE_1510 for j in scheduler._due_jobs(early))
    assert not scheduler._due_jobs(late)
