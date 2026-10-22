from datetime import datetime,timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import pytest
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.runtime.indicator_history import prepare_pattern_history
from liangjian_funnel.runtime.regeneration_data_limits import source_gap_detail,project_data_limited

AT=datetime(2026,10,22,16,tzinfo=ZoneInfo('Asia/Shanghai'))
CAL=SimpleNamespace(previous_trading_day=lambda day:day-timedelta(days=1),
    is_trading_day=lambda day:day.weekday()<5)


def source():
    ends=[]
    for hour in (9,13):
        start=AT.replace(hour=hour,minute=30 if hour==9 else 0)
        ends.extend(start+timedelta(minutes=5*i) for i in range(1,25))
    return [MinuteBar(symbol='600201.SH',interval='5m',bar_end=end,open=10,high=11,low=9,
        close=10,volume=1000,amount=10000,source_id='frozen-test-native') for end in ends]


@pytest.mark.parametrize('count',[47,48])
def test_full_closed_48_required_before_any_write_and_duplicate_plans_share_request(tmp_path,count):
    store=MinuteBarStore(tmp_path);calls=[];bars=source()[:count]
    provider=SimpleNamespace(fetch_bars=lambda *a,**kw:calls.append((a,kw)) or
        SimpleNamespace(complete=True,bars=bars))
    plans=[dict(plan_id=k,symbol='600201.SH',expires_at='2026-10-23T15:00:00+08:00') for k in ('p1','p2')]
    result=prepare_pattern_history(plans,now=AT,calendar=CAL,minute_store=store,provider=provider)
    assert len(calls)==1 and len(result['plans'])==2
    cached=store.load_latest('600201.SH','5m',limit=96)
    assert len(cached)==(48 if count==48 else 0)
    assert result['status']==('READY' if count==48 else 'DATA_LIMITED')
    if count==48:
        again=prepare_pattern_history(plans,now=AT,calendar=CAL,minute_store=store,provider=provider)
        assert again['status']=='READY' and len(calls)==1


def test_normal_natural_publication_marks_missing_source_manifest_and_probe_not_eligible_default():
    detail=source_gap_detail({},'600201.SH',strict=False)
    payload=project_data_limited({'risk_unit':'STANDARD'},detail)
    assert payload['DATA_LIMITED'] is True and payload['risk_unit']=='PROBE'
    assert detail['a6_explicit_review_required'] is True
    assert detail['reason_codes']==['A1_SOURCE_FAILURE_MANIFEST_MISSING']


def test_daily_a1_refresh_is_real_pipeline_and_failure_never_reuses_old_silently(tmp_path,monkeypatch):
    from liangjian_funnel.workflow import WorkflowApplication,WorkflowError
    import liangjian_funnel.runtime.provider_health as health
    pointer=[SimpleNamespace(generation_id='old',as_of=AT-timedelta(days=1))];calls=[]
    dummy=SimpleNamespace(a1_registry=SimpleNamespace(get_active_generation=lambda:pointer[0]),
        trading_calendar=CAL,settings=SimpleNamespace(root=tmp_path))
    def refresh(**kw):
        calls.append(kw);pointer[0]=SimpleNamespace(generation_id='fresh',as_of=kw['now']);return {'status':'READY'}
    dummy.run_a1_maintenance=refresh
    WorkflowApplication._ensure_daily_a1(dummy,AT)
    assert len(calls)==1 and calls[0]['now'].hour==15
    WorkflowApplication._ensure_daily_a1(dummy,AT);assert len(calls)==1
    pointer[0]=SimpleNamespace(generation_id='old',as_of=AT-timedelta(days=1))
    dummy.run_a1_maintenance=lambda **kw:{'status':'FAILED'};alerts=[]
    monkeypatch.setattr(health,'queue_internal',lambda *a,**kw:alerts.append(kw))
    with pytest.raises(WorkflowError,match='A1_DAILY_REFRESH_FAILED_KEEP_OLD_PLANS'):
        WorkflowApplication._ensure_daily_a1(dummy,AT)
    assert len(alerts)==1 and pointer[0].generation_id=='old'
