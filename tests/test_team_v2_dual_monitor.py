"""0338: customer engine first, one source capture and one absolute budget."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from liangjian_funnel.settings import Settings
from liangjian_funnel.runtime.team_v2_first_day import forced_record
from liangjian_funnel.runtime.team_v2_morning import ACCOUNT


NOW = datetime.fromisoformat('2026-10-21T09:36:00+08:00')


def app_fixture(tmp_path):
    return SimpleNamespace(settings=Settings.from_env({}, root=tmp_path))


def test_customer_first_shared_data_and_only_remaining_absolute_budget(tmp_path, monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    import liangjian_funnel.runtime.team_v2_monitor as team
    app = app_fixture(tmp_path); tick = [100.0]; seen = []; calls = []
    app._fetch_live_bars = lambda *a, **k: calls.append(('bars', a)) or {'original': 'bars'}
    app._fetch_live_quote = lambda *a, **k: calls.append(('quote', a)) or {'original': 'quote'}
    cutoff = NOW - timedelta(minutes=1)
    legacy = {'notifications': [{'channel': 'original', 'body': '客户原卡'}],
        'observability': {'axes': {'job_status': 'SUCCEEDED'}}}
    def old(**kw):
        seen.append('customer'); assert kw['deadline_monotonic'] == 147
        shared = kw['minute_sources']
        shared.legacy_bars('000001.SZ', '1m', 5, cutoff, deadline=130)
        shared.legacy_quote('000001.SZ', NOW, deadline=130)
        tick[0] = 119
        return legacy
    def control(owner, **kw):
        seen.append('team'); assert kw['deadline_monotonic'] == 147
        shared = kw['minute_sources']
        assert shared.team_bars('000001.SZ', '1m', 5, cutoff, deadline=147) == {'original': 'bars'}
        assert shared.team_quote('000001.SZ', NOW, deadline=147) == {'original': 'quote'}
        return {'status': 'READY'}
    app.monitor_once = old; monkeypatch.setattr(team, 'run_monitor_control', control)
    result = run_dual_monitor(app, now=NOW, monotonic=lambda: tick[0])
    assert seen == ['customer', 'team'] and len(calls) == 2
    assert {k: result[k] for k in legacy} == legacy
    assert result['team']['status'] == 'READY' and result['dual_path']['remaining_at_team_start_seconds'] == 28


@pytest.mark.parametrize('failure', ['exception', 'failed_receipt', 'deadline'])
def test_legacy_failure_or_budget_exhaustion_never_starts_team(tmp_path, monkeypatch, failure):
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    import liangjian_funnel.runtime.team_v2_monitor as team
    app = app_fixture(tmp_path); tick = [100.0]; calls = []
    def old(**kwargs):
        if failure == 'exception': raise RuntimeError('old failure')
        if failure == 'deadline': tick[0] = 147
        return {'notifications': ['original'], 'observability': {'axes': {
            'job_status': 'FAILED' if failure == 'failed_receipt' else 'SUCCEEDED'}}}
    app.monitor_once = old
    monkeypatch.setattr(team, 'run_monitor_control', lambda *a, **kw: calls.append(kw))
    if failure == 'exception':
        with pytest.raises(RuntimeError, match='old failure'):
            run_dual_monitor(app, now=NOW, monotonic=lambda: tick[0])
    else:
        result = run_dual_monitor(app, now=NOW, monotonic=lambda: tick[0])
        assert result['notifications'] == ['original'] and result['team']['status'] == 'SKIPPED'
    assert calls == [] and not (tmp_path / 'state/team_v2/team.db').exists()


@pytest.mark.parametrize('reason,persistent',[('PRIVATE_PROVIDER_RESPONSE',False),('ENTRY_PATTERN_PLAN_SOURCE_CONFLICT',True)])
def test_team_exception_preserves_customer_result_and_only_conflict_latches(tmp_path, monkeypatch,reason,persistent):
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    import liangjian_funnel.runtime.team_v2_monitor as team
    app = app_fixture(tmp_path); seen = []; legacy = {'notifications': ['original']}
    app.monitor_once = lambda **kwargs: legacy
    def broken(*a, **kw): raise RuntimeError(reason)
    monkeypatch.setattr(team, 'run_monitor_control', broken)
    first = run_dual_monitor(app, now=NOW)
    assert first['notifications'] == ['original'] and first['mode'] == 'record'
    assert 'PRIVATE' not in json.dumps(first)
    latch=forced_record(app.settings, account_id=ACCOUNT, now=NOW)
    assert bool(latch)==persistent
    if persistent:assert latch['reason_code']==reason
    def protected(*a, **kw):
        seen.append(kw['record_fallback']['reason_code'] if kw['record_fallback'] else None); return {'status': 'RECORD_FALLBACK_ACTIVE'}
    monkeypatch.setattr(team, 'run_monitor_control', protected)
    second = run_dual_monitor(app, now=NOW + timedelta(minutes=1))
    assert second['notifications'] == ['original'] and seen == [reason if persistent else None]
    with sqlite3.connect(tmp_path / 'state/team_v2/team.db') as db:
        if persistent:assert db.execute('SELECT count(*) FROM control_record_latch').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM team_notifications').fetchone()[0] == 1


def test_record_with_owned_exposure_still_runs_customer_first(tmp_path, monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    import liangjian_funnel.runtime.team_v2_monitor as team
    import liangjian_funnel.runtime.team_v2_record_protection as exposure
    app = app_fixture(tmp_path); app.settings = app.settings.model_copy(update={'a4_trader_mode': 'record'})
    seen = []; app.monitor_once = lambda **kwargs: seen.append('customer') or {'notifications': ['original']}
    monkeypatch.setattr(exposure, 'has_owned_exposure', lambda *_: True)
    monkeypatch.setattr(team, 'run_monitor_control', lambda *a, **kw:
        seen.append(kw['record_fallback']['reason_code']) or {'status': 'RECORD_FALLBACK_ACTIVE'})
    assert run_dual_monitor(app, now=NOW)['team']['status'] == 'RECORD_FALLBACK_ACTIVE'
    assert seen == ['customer', 'EXPLICIT_RECORD_MODE']


def test_legacy_publication_retries_are_untouched_team_reuses_last_original():
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    replies = iter([{'complete': False}, {'complete': True}]); calls = []
    app = SimpleNamespace(_fetch_live_bars=lambda *a, **k: calls.append(a) or next(replies))
    shared = SharedMinuteSources(app)
    args = ('000001.SZ', '1m', 5, NOW)
    assert shared.legacy_bars(*args, deadline=time.monotonic()+1) == {'complete': False}
    assert shared.legacy_bars(*args, deadline=time.monotonic()+1) == {'complete': True}
    assert shared.team_bars(*args, deadline=time.monotonic()+1) == {'complete': True}
    assert len(calls) == 2


def test_pending_legacy_request_is_not_duplicated_by_team():
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    entered = threading.Event(); finish = threading.Event(); calls = []
    def slow(*a, **kw):
        calls.append(a); entered.set(); finish.wait(2); return {'original': True}
    shared = SharedMinuteSources(SimpleNamespace(_fetch_live_quote=slow))
    with ThreadPoolExecutor(1) as pool:
        original = pool.submit(shared.legacy_quote, '000001.SZ', NOW, deadline=time.monotonic()+1)
        assert entered.wait(1)
        try:
            with pytest.raises(ValueError, match='TEAM_SHARED_SOURCE_DEADLINE'):
                shared.team_quote('000001.SZ', NOW, deadline=time.monotonic()+.01)
            assert len(calls) == 1
        finally: finish.set()
        assert original.result(1) == {'original': True}


def test_team_reuses_source_failure_without_retry_or_private_details():
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    calls = []
    def broken(*a, **kw): calls.append(a); raise RuntimeError('PRIVATE')
    shared = SharedMinuteSources(SimpleNamespace(_fetch_live_quote=broken))
    with pytest.raises(RuntimeError, match='PRIVATE'):
        shared.legacy_quote('000001.SZ', NOW, deadline=time.monotonic()+1)
    with pytest.raises(ValueError, match='^TEAM_SHARED_SOURCE_UNAVAILABLE$'):
        shared.team_quote('000001.SZ', NOW, deadline=time.monotonic()+1)
    assert len(calls) == 1


def test_shared_full_market_observation_is_after_legacy_and_never_injected_into_customer_state(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    import liangjian_funnel.runtime.team_v2_emotion_baseline as source
    shared = SharedMinuteSources(SimpleNamespace()); observed = []; raw = object()
    original = {'status': 'READY', 'entry_permission': 'ALLOW', 'as_of': NOW.isoformat()}
    shared.market_state = original; shared.capture_market_snapshot(raw)
    monkeypatch.setattr(source, 'observe_full_market', lambda settings, value, **kw:
        observed.append(value) or {'source_as_of': NOW.isoformat()})
    assert observed == []
    team = shared.team_market(SimpleNamespace(root=tmp_path), clock=lambda: NOW, calendar=None, deadline=time.monotonic()+1)
    assert observed == [raw] and team['team_v2_source']['source_as_of'] == NOW.isoformat()
    assert original == {'status': 'READY', 'entry_permission': 'ALLOW', 'as_of': NOW.isoformat()}


def test_team_missing_shared_market_is_blocked_without_another_collection(tmp_path):
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    shared = SharedMinuteSources(SimpleNamespace())
    assert shared.team_market(SimpleNamespace(root=tmp_path), clock=lambda: NOW, calendar=None,
        deadline=time.monotonic()+1)['reason_code'] == 'TEAM_SHARED_MARKET_UNAVAILABLE'


def test_expired_external_deadline_cannot_start_team_sources_or_models(tmp_path):
    from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
    app = app_fixture(tmp_path); app.trading_calendar = object()
    result = run_monitor_control(app, now=NOW, deadline_monotonic=time.monotonic()-1)
    assert result == {'status': 'SKIPPED', 'reason_code': 'TEAM_REMAINDER_EXHAUSTED'}
    assert not (tmp_path / 'state/team_v2').exists()


def test_next_minute_cannot_be_fetched_under_old_round_budget():
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    calls = []
    shared = SharedMinuteSources(SimpleNamespace(
        _fetch_live_quote=lambda *a, **k: calls.append(a)), minute=NOW)
    with pytest.raises(ValueError, match='TEAM_SHARED_SOURCE_MINUTE_MOVED'):
        shared.team_quote('000001.SZ', NOW+timedelta(minutes=1), deadline=time.monotonic()+1)
    assert calls == []


def test_late_full_market_completion_cannot_replace_the_legacy_selected_market(tmp_path):
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    shared = SharedMinuteSources(SimpleNamespace())
    shared.seal({'status': 'READY_DEGRADED', 'source': 'ORIGINAL_INDEX_FALLBACK'})
    shared.capture_market_snapshot(object())
    assert shared.market_snapshot is None
    assert shared.team_market(SimpleNamespace(root=tmp_path), clock=lambda: NOW, calendar=None,
        deadline=time.monotonic()+1)['source'] == 'ORIGINAL_INDEX_FALLBACK'


def test_g5_with_empty_legacy_scope_collects_once_only_within_team_remainder(monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    import liangjian_funnel.runtime.live_market as source
    calls = []; shared = SharedMinuteSources(SimpleNamespace(market_data=object()), minute=NOW)
    shared.allow_market_collection = True
    monkeypatch.setattr(source, 'load_or_refresh_live_market_state',
        lambda *a, **kw: calls.append(kw) or {'status': 'READY', 'original': 'team-only-scope'})
    for _ in range(2):
        assert shared.team_market(None, clock=lambda: NOW, calendar=None,
            deadline=time.monotonic()+1)['original'] == 'team-only-scope'
    assert len(calls) == 1


def test_failed_alert_enqueue_is_repaired_once_without_automatic_record_unlock(tmp_path, monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    from liangjian_funnel.runtime.team_v2_internal import InternalOutbox
    from liangjian_funnel.runtime.team_v2_internal_events import InternalEventProjector
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    import liangjian_funnel.runtime.team_v2_monitor as team
    app=app_fixture(tmp_path);app.monitor_once=lambda **kw:{'notifications':['original']}
    enqueue=InternalOutbox.enqueue
    def broken(*a, **kw):raise RuntimeError('PRIVATE')
    def identity_conflict(*a, **kw):raise ValueError('ENTRY_PATTERN_PLAN_SOURCE_CONFLICT')
    monkeypatch.setattr(team,'run_monitor_control',identity_conflict)
    monkeypatch.setattr(InternalOutbox,'enqueue',broken)
    result=run_dual_monitor(app,now=NOW)
    assert result['team_control_fallback']['internal_alert']=='PENDING_ENQUEUE_REPAIR'
    assert forced_record(app.settings,account_id=ACCOUNT,now=NOW)
    monkeypatch.setattr(InternalOutbox,'enqueue',enqueue)
    store=TeamMeetingStore(tmp_path/'state/team_v2/team.db')
    projector=InternalEventProjector(InternalOutbox(store))
    for _ in range(2):projector._team(ACCOUNT,NOW.date().isoformat(),NOW)
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT source_id FROM team_notifications WHERE kind='ALERT'").fetchall()==[('TEAM_MONITOR_RECORD',)]


def test_team_store_failure_does_not_erase_customer_receipt_or_claim_durable_latch(tmp_path, monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    import liangjian_funnel.runtime.team_v2_monitor as team
    import liangjian_funnel.runtime.team_v2_store as stores
    app=app_fixture(tmp_path);app.monitor_once=lambda **kw:{'notifications':['original']}
    def broken(*a, **kw):raise RuntimeError('PRIVATE')
    monkeypatch.setattr(team,'run_monitor_control',broken)
    monkeypatch.setattr(stores,'TeamMeetingStore',broken)
    result=run_dual_monitor(app,now=NOW)
    assert result['notifications']==['original'] and result['mode']=='record'
    assert result['team_control_fallback']['persistent_record'] is False
    assert result['team_control_fallback']['internal_alert']=='FAILED'
    assert 'PRIVATE' not in json.dumps(result)


def test_actual_team_control_minute_receives_original_dispatch_deadline(tmp_path,monkeypatch):
    from test_team_v2_monitor_dispatch import prepared
    import liangjian_funnel.runtime.team_v2_monitor as module
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    captured=[];deadline=time.monotonic()+5
    monkeypatch.setattr(module,'run_control_minute',lambda **kw:
        captured.append(kw['deadline_monotonic']) or {'status':'READY'})
    result=module.run_monitor_control(app,clock=lambda:clock.now,client=api,
        tradability_loader=proof,deadline_monotonic=deadline)
    assert result['status']=='READY' and captured==[deadline] and not seen


def test_dispatch_into_actual_team_collector_reuses_original_fetch_results(tmp_path,monkeypatch):
    from test_team_v2_monitor_dispatch import prepared
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    from liangjian_funnel.data.session_windows import closed_window_ends
    import liangjian_funnel.runtime.team_v2_monitor as team
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch,trade_date='2026-10-20')
    calls=[];bars=app._fetch_live_bars;quote=app._fetch_live_quote
    app._fetch_live_bars=lambda *a,**kw:calls.append(('bars',a)) or bars(*a,**kw)
    app._fetch_live_quote=lambda *a,**kw:calls.append(('quote',a)) or quote(*a,**kw)
    def legacy(**kw):
        cutoff=clock.now-timedelta(minutes=1);shared=kw['minute_sources']
        shared.legacy_bars('000001.SZ','1m',len(closed_window_ends(cutoff,'1m')),cutoff,
            deadline=kw['deadline_monotonic'])
        shared.legacy_quote('000001.SZ',clock.now,deadline=kw['deadline_monotonic'])
        return {'notifications':['customer-finished'],'live_market_state':{
            'status':'READY','as_of':clock.now.isoformat(),'entry_permission':'ALLOW'}}
    app.monitor_once=legacy;actual=team.run_monitor_control
    monkeypatch.setattr(team,'run_monitor_control',lambda owner,**kw:actual(owner,clock=lambda:clock.now,
        client=api,tradability_loader=proof,**kw))
    result=run_dual_monitor(app,now=clock.now)
    assert result['notifications']==['customer-finished']
    assert [kind for kind,args in calls]==['bars','quote']
    assert result['team']['original_quote_archive']['status']=='ARCHIVED'
    assert 'packet_sha256' in result['team']


def test_record_required_receipt_without_proven_conflict_blocks_only_tick(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    import liangjian_funnel.runtime.team_v2_monitor as team
    app=app_fixture(tmp_path);app.monitor_once=lambda **kw:{'notifications':['customer-finished']}
    monkeypatch.setattr(team,'run_monitor_control',lambda *a,**kw:{
        'status':'CONTROL_RECORD_REQUIRED','mode':'record'})
    result=run_dual_monitor(app,now=NOW)
    assert result['mode']=='record' and forced_record(app.settings,account_id=ACCOUNT,now=NOW) is None
    assert result['team_control_fallback']['record_required'] and not result['team_control_fallback']['persistent_record']


def test_next_round_reuses_exact_customer_market_cache_without_fetch_or_reobservation(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import SharedMinuteSources
    import liangjian_funnel.runtime.team_v2_emotion_baseline as source
    settings=SimpleNamespace(root=tmp_path);calls=[]
    monkeypatch.setattr(source,'observe_full_market',lambda *a,**kw:
        calls.append(a[1]) or {'status':'READY','frame_sha256':'original'})
    original={'status':'READY','as_of':NOW.isoformat(),'entry_permission':'ALLOW',
        'cache_hit':False,'cache_reused':False,'diagnostics':{'cache_hit':False}}
    first=SharedMinuteSources(SimpleNamespace(),minute=NOW)
    first.capture_market_snapshot('original-provider-object');first.seal(original)
    first.team_market(settings,clock=lambda:NOW,calendar=None,deadline=time.monotonic()+1)
    later=NOW+timedelta(minutes=1)
    cached={**original,'cache_hit':True,'cache_reused':True,
        'diagnostics':{'cache_hit':True,'cache_bucket':'original-bucket'}}
    second=SharedMinuteSources(SimpleNamespace(),minute=later);second.seal(cached)
    result=second.team_market(settings,clock=lambda:later,calendar=None,deadline=time.monotonic()+1)
    assert result['team_v2_source']['frame_sha256']=='original' and len(calls)==1
    second.market_state={**cached,'entry_permission':'BLOCK_NEW_ENTRY'}
    assert second.team_market(settings,clock=lambda:later,calendar=None,
        deadline=time.monotonic()+1)['team_v2_source']['status']=='DATA_BLOCK'
    second.market_state=cached
    path=next((tmp_path/'state/team_v2/shared_market').glob('*.json'))
    path.write_text(path.read_text().replace('original','tampered'))
    assert second.team_market(settings,clock=lambda:later,calendar=None,
        deadline=time.monotonic()+1)['team_v2_source']['status']=='DATA_BLOCK'
    assert len(calls)==1 and 'team_v2_source' not in original and 'team_v2_source' not in cached


def test_uncooperative_model_transport_cannot_consume_the_post_review_reserve(tmp_path,monkeypatch):
    from test_team_v2_monitor_dispatch import prepared
    from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    import httpx
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch,trade_date='2026-10-21')
    original=api.transport.handler
    def late(request):time.sleep(2.2);return original(request)
    delayed=OpenAICompatibleModelClient(app.settings,transport=httpx.MockTransport(late),max_attempts=1)
    started=time.perf_counter()
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=delayed,
        tradability_loader=proof,deadline_monotonic=time.monotonic()+3)
    elapsed=time.perf_counter()-started
    assert elapsed<1.7
    assert result['result']['trader']['model_validated'] is False
    time.sleep(2.3)
    with sqlite3.connect(tmp_path/'state/team_v2/paper.db') as db:
        assert db.execute('SELECT count(*) FROM paper_orders').fetchone()[0]==0


def test_invalid_team_model_vote_blocks_entry_without_unsupported_account_lock(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    import liangjian_funnel.runtime.team_v2_monitor as team
    app=app_fixture(tmp_path);app.monitor_once=lambda **kw:{'notifications':['customer-finished']}
    monkeypatch.setattr(team,'run_monitor_control',lambda *a,**kw:{'status':'MINUTE_PROTECTED',
        'result':{'trader':{'model_validated':False,'status':'CANDIDATE_REVIEW_FAILED_ENTRIES_BLOCKED'}}})
    result=run_dual_monitor(app,now=NOW)
    assert result['mode']=='record' and result['notifications']==['customer-finished']
    assert forced_record(app.settings,account_id=ACCOUNT,now=NOW) is None
    assert result['team_control_fallback']['record_required']
