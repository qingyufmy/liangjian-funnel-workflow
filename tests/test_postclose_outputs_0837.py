from pathlib import Path
from datetime import datetime
from copy import deepcopy
from types import SimpleNamespace
import importlib.util
import json

import pytest

from liangjian_funnel.review.audit_reader import read_audit_stage
from liangjian_funnel.review.daily import _a2_projection
from liangjian_funnel.review.fact_guard import business_metrics
from liangjian_funnel.review.time_presentation import bjt_presentation
from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher,_display_text,_condition_summary
from liangjian_funnel.runtime.state import RuntimeStore
from test_workflow_lark_notifications import FakeNotifier,_plan,SHANGHAI


def test_unavailable_a2_is_unknown_not_empty_and_real_empty_remains_zero():
    unavailable,_=_a2_projection({})
    assert unavailable['counts']['FOCUS'] is None
    assert business_metrics(dict(a2=unavailable,metrics={'a2_focus_count':0,'a2_watch_count':0}))['a2_focus_count'] is None
    empty,_=_a2_projection(dict(stages=[dict(stage='A2',status='READY',output=dict(focus_pool=[],watch_only_pool=[]))]))
    assert empty['availability']=='AVAILABLE' and empty['counts']['FOCUS']==0


def test_streaming_audit_is_not_rejected_only_for_large_source_metadata(tmp_path,monkeypatch):
    path=tmp_path/'audit.json';path.write_text(json.dumps(dict(stages=[dict(stage='A2',status='READY',output=dict(focus_pool=[{'symbol':'000858.SZ'}]))])))
    actual_stat=Path.stat
    def sized(self,*args,**kwargs):
        if self==path:return SimpleNamespace(st_size=639090418,st_mtime_ns=123)
        return actual_stat(self,*args,**kwargs)
    monkeypatch.setattr(Path,'stat',sized)
    assert read_audit_stage(path,'A2')['output']['focus_pool'][0]['symbol']=='000858.SZ'
    with pytest.raises(ValueError,match='AUDIT_FILE_LIMIT'):
        read_audit_stage(path,'A2',max_file_bytes=512*1024*1024)
    with pytest.raises(ValueError,match='AUDIT_STAGE_LIMIT'):
        read_audit_stage(path,'A2',max_stage_bytes=1)
    with pytest.raises(ValueError,match='AUDIT_READ_DEADLINE'):
        read_audit_stage(path,'A2',max_read_seconds=0)


def test_bjt_projection_preserves_original_instant_and_never_changes_original():
    source=dict(at='2026-10-27T01:25:00+00:00',note='竞价UTC01:25；午夜UTC20:30')
    frozen=deepcopy(source);result=bjt_presentation(source)
    assert source==frozen and result['at']=='2026-10-27T09:25:00+08:00'
    assert result['note']=='竞价北京时间09:25；午夜北京时间次日04:30'
    assert datetime.fromisoformat(source['at'])==datetime.fromisoformat(result['at'])


def publisher(tmp_path):
    store=RuntimeStore(tmp_path/'runtime.db');publisher=WorkflowLarkPublisher(store,None)
    fake=FakeNotifier();publisher.notifier=fake
    return publisher,store,fake


def test_unknown_code_is_visible_counted_and_queued_as_label_alert(tmp_path):
    p,store,fake=publisher(tmp_path);now=datetime(2026,10,27,10,0,tzinfo=SHANGHAI)
    p._send(delivery_key='unit-mapping',kind='UNIT',source_id='unit',title='标签检查',
        lines=[_display_text('FUTURE_REASON_123')],summary={},now=now)
    text='\n'.join(fake.calls[0][1]);assert 'FUTURE_REASON_123' in text and '标签映射告警' in text and '未识别' not in text
    row=store.get_delivery_by_key('unit-mapping');payload=json.loads(row['payload_json'])
    assert payload['unmapped_code_counts']=={'FUTURE_REASON_123':1}


def test_timeout_card_has_condition_totals_and_separates_authored_price(tmp_path):
    p,_,fake=publisher(tmp_path);plan=_plan(1);pid=plan['plan_id']
    met=['B_KEY_LEVEL','B_RETEST','B_RETEST_VOLUME','B_ACCEPTED','B_NO_CHASE','B_STOP_ONLY_RAISED','ENTRY_RR','ENTRY_ZONE','A3_TREND_ROUTE_APPROVED','VOLUME_NOT_OVERHEATED']
    event=dict(effective=False,action='DATA_BLOCK',reason_code='MONITOR_OVERRUN',plan_id=pid,
        symbol=plan['symbol'],lane_id='lane_1',minute_end='2026-10-27T10:06:00+08:00',
        payload_json=json.dumps(dict(plan_id=pid,strategy=dict(met_conditions=met,unmet_conditions=[]))))
    p.publish_a4_events([event],plans={pid:plan},now=datetime(2026,10,27,10,6,tzinfo=SHANGHAI))
    title,lines,_=fake.calls[0];text='\n'.join(lines)
    assert '模型超时' in title and '数据告警' not in title
    assert '共10项' in text and '另6项见原件' not in text and '未识别' not in text
    assert 'Team准入价格绑定未留存' in text and '计划区间' in text


def test_morning_final_status_deduped_by_actual_switched_scope(tmp_path):
    p,_,fake=publisher(tmp_path);now=datetime(2026,10,27,8,38,19,tzinfo=SHANGHAI)
    kwargs=dict(trade_date='2026-10-27',scope_id='scope-real',plan_count=14,body_sha256='a'*64,observed_at=now)
    assert p.publish_batch_switch_status(**kwargs)['status']=='SENT'
    assert p.publish_batch_switch_status(**kwargs)['duplicate']
    assert len(fake.calls)==1 and '14只' in '\n'.join(fake.calls[0][1])


def test_watchdog_uses_current_single_config_and_confirms_transient_error(monkeypatch):
    path=Path(__file__).resolve().parents[1]/'scripts/watch_a4_runtime.py'
    spec=importlib.util.spec_from_file_location('watchdog_0837',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    monkeypatch.setattr(module,'pinned_ssh_options',lambda:['-o','BatchMode=yes','-o','StrictHostKeyChecking=yes'])
    monkeypatch.setattr(module,'pinned_ssh_options',lambda:['-o','BatchMode=yes','-o','StrictHostKeyChecking=yes'])
    root,host=module.deployment_host();assert root.startswith('/media/qy/') and host=='aurum-vm'
    calls=[]
    def run(args,**kwargs):
        calls.append(args)
        assert '/www/wwwroot/' not in args[-1] and root in args[-1]
        if len(calls)==1:raise module.subprocess.TimeoutExpired(args,20)
        return SimpleNamespace(stdout=b'{"reachable":true,"app_healthy":true}')
    monkeypatch.setattr(module.subprocess,'run',run)
    result=module.probe()
    assert len(calls)==2 and result['reachable'] and not result['probe_confirmation']['confirmed_failure']
    assert result['probe_confirmation']['first']['exception_class']=='TimeoutExpired'


def test_watchdog_preserves_stage_without_saving_private_error(monkeypatch):
    path=Path(__file__).resolve().parents[1]/'scripts/watch_a4_runtime.py'
    spec=importlib.util.spec_from_file_location('watchdog_failure_0837',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    monkeypatch.setattr(module,'pinned_ssh_options',lambda:['-o','BatchMode=yes','-o','StrictHostKeyChecking=yes'])
    def run(args,**kwargs):raise module.subprocess.CalledProcessError(2,args,stderr=b'PRIVATE_NOT_FOR_LOG')
    monkeypatch.setattr(module.subprocess,'run',run)
    result=module.probe()
    assert result['probe_confirmation']['attempts']==2 and result['failure_stage']=='REMOTE_RUNTIME'
    assert 'PRIVATE_NOT_FOR_LOG' not in json.dumps(result)


def test_watchdog_zero_plan_waits_for_new_cas_cutoff_without_weakening_health():
    from liangjian_funnel.runtime.watchdog import assess
    snapshot=dict(reachable=True,app_healthy=True,minutes=[],leases=[],plan_readiness={'current_plan_count':0})
    early=assess(datetime(2026,10,27,8,44,tzinfo=SHANGHAI),snapshot,trading_day=True)
    due=assess(datetime(2026,10,27,8,45,tzinfo=SHANGHAI),snapshot,trading_day=True)
    assert 'DAILY_PLAN_NOT_READY' not in early['problems'] and 'DAILY_PLAN_NOT_READY' in due['problems']
    assert 'APP_UNHEALTHY' in assess(datetime(2026,10,27,8,44,tzinfo=SHANGHAI),dict(snapshot,app_healthy=False),trading_day=True)['problems']


def test_watchdog_remote_sql_is_lane_bounded_and_query_only():
    import ast
    path=Path(__file__).resolve().parents[1]/'scripts/watch_a4_runtime.py'
    source=path.read_text(encoding='utf8')
    remote=next(ast.literal_eval(n.value) for n in ast.parse(source).body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='REMOTE_PROBE' for t in n.targets))
    assert remote.count('from monitor_events where lane_id=?')==3
    assert 'pragma query_only=ON' in remote and 'set_progress_handler' in remote
    assert 'log.read_text' not in remote and '4*1024*1024' in remote
