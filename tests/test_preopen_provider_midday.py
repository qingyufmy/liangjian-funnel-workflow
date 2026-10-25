from datetime import datetime
from types import SimpleNamespace
from liangjian_funnel.settings import Settings
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.runtime.provider_health import run_daily_availability
from liangjian_funnel.runtime.team_v2_review_policy import load_review_policy,review_wall_budget
from liangjian_funnel.runtime.team_v2_review_command import run_review_stage
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar


def test_monday_0800_two_model_probe_is_durable_once_and_controlled(tmp_path):
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED_ONLY',
        'LIANGJIAN_RESEARCH_MODEL':'deepseek/deepseek-v4-pro',
        'LIANGJIAN_REVIEW_MODEL':'deepseek/deepseek-v4-pro',
        'LIANGJIAN_MONITOR_MODEL':'deepseek-v4.1-flash',
        'LIANGJIAN_TEAM_INTRADAY_MODEL':'deepseek-v4.1-flash'},root=tmp_path)
    # The live root uses this provider-facing Flash alias. No supplier/model
    # change is inferred from the Settings class's legacy defaults.
    RuntimeStore(settings.state_db_path);calls=[]
    class Controlled:
        def complete(self,model,messages,**kw):
            calls.append((model,kw));return SimpleNamespace(output={'ok':True},latency_ms=1,provider_stream_telemetry={'controlled':True})
    at=datetime.fromisoformat('2026-10-26T08:00:00+08:00')
    market_calls=[]
    class Source:
        def __init__(self,name):self.name=name
        def fetch_bars(self,symbol,interval,count,*,as_of):
            market_calls.append((self.name,symbol,interval,count,as_of))
            return SimpleNamespace(complete=True,reason_code='OK',attempts=(),bars=(SimpleNamespace(
                symbol=symbol,interval=interval,bar_end=as_of,evidence_kind='MARKET_BAR',
                source_id=self.name+':controlled'),))
    sources={n:Source(n) for n in ('MOOTDX','TENCENT')}
    first=run_daily_availability(settings,client=Controlled(),now=at,market_adapters=sources)
    again=run_daily_availability(settings,client=Controlled(),now=at,market_adapters=sources)
    assert first['status']==again['status']=='READY' and len(calls)==2
    assert {m for m,_ in calls}=={'deepseek/deepseek-v4-pro','deepseek-v4.1-flash'}
    assert all(k['stage']=='AVAILABILITY' and k['timeout_seconds']==20 for _,k in calls)
    assert all(r['reused'] for r in again['models'])
    assert len(market_calls)==2 and all(c[3]==1 and c[4].isoformat()=='2026-10-23T15:00:00+08:00' for c in market_calls)
    assert all(r['reused'] for r in again['market_sources'])


def test_market_probe_failure_alerts_immediately_once_without_retry(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.market_source_health import run_market_source_probes
    at=datetime.fromisoformat('2026-10-26T08:00:00+08:00')
    settings=Settings.from_env({},root=tmp_path);calls=[];alerts=set()
    monkeypatch.setattr('liangjian_funnel.runtime.provider_health.queue_internal',
        lambda settings,**kw:alerts.add((kw['kind'],kw['source_id'])))
    class Failure:
        def fetch_bars(self,*a,**k):
            calls.append(a);raise RuntimeError('CONTROLLED_MOOTDX_UNAVAILABLE')
    sources={name:Failure() for name in ('MOOTDX','TENCENT')}
    first=run_market_source_probes(settings,now=at,root=tmp_path,adapters=sources)
    again=run_market_source_probes(settings,now=at,root=tmp_path,adapters=sources)
    assert len(calls)==2 and len(alerts)==2
    assert all(r['status']=='FAILED' for r in first)
    assert all(r['reused'] for r in again)


def test_interrupted_market_attempt_is_reported_without_repeating_request(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.market_source_health import run_market_source_probes
    at=datetime.fromisoformat('2026-10-26T08:00:00+08:00')
    settings=Settings.from_env({},root=tmp_path);alerts=[]
    monkeypatch.setattr('liangjian_funnel.runtime.provider_health.queue_internal',lambda settings,**kw:alerts.append(kw))
    for name in ('mootdx','tencent'):(tmp_path/('market-'+name+'.attempt')).write_text('CONTROLLED_STARTED')
    class Deny:
        def fetch_bars(self,*a,**k):raise AssertionError('MUST_NOT_RETRY_INTERRUPTED_PROBE')
    rows=run_market_source_probes(settings,now=at,root=tmp_path,adapters={n:Deny() for n in ('MOOTDX','TENCENT')})
    assert len(alerts)==2 and all(r['status']=='UNKNOWN_INTERRUPTED' for r in rows)


def test_monday_midday_600_seconds_never_extends_beyond_1250(tmp_path):
    policy=load_review_policy(tmp_path)
    assert review_wall_budget(policy,now=datetime.fromisoformat('2026-10-26T12:40:00+08:00'),review_kind='MIDDAY')==600
    assert review_wall_budget(policy,now=datetime.fromisoformat('2026-10-26T12:49:00+08:00'),review_kind='MIDDAY')==60
    settings=Settings.from_env({},root=tmp_path)
    class Deny:
        def complete(self,*a,**k):raise AssertionError('NO_CALL_AFTER_MIDDAY_DEADLINE')
    result=run_review_stage(settings,'MIDDAY',clock=lambda:datetime.fromisoformat('2026-10-26T12:50:00+08:00'),calendar=ExchangeTradingCalendar(),client=Deny())
    assert result['status']=='NOOP' and result['reason_code']=='TEAM_A5_MIDDAY_WINDOW_CLOSED'
