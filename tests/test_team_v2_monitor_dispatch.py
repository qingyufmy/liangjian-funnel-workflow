from fixture_model_stream import sse_model_response
from copy import deepcopy
from contextlib import closing
from datetime import timedelta
import json
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from test_team_v2_morning import fixture,client,advance,CAL,source_bytes
from test_team_v2_minute_projection import source
from test_team_v2_trader_schema import output
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
from liangjian_funnel.data.mootdx import FetchResult
from liangjian_funnel.data.tencent_minute import MarketQuote,QuoteResult
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication,WorkflowError


def prepared(tmp_path,monkeypatch, *, trade_date='2026-10-19'):
    settings,clock=fixture(tmp_path,trade_date=trade_date);api,morning_calls=client(settings)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)
    advance(clock,8,38);run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=api)
    with closing(sqlite3.connect(settings.state_db_path)) as db,db:
        db.execute("UPDATE execution_plans SET status='ACTIVE_TODAY'")
    advance(clock,9,36);seen=[]
    def bars(symbol,interval,count,cutoff,**kwargs):
        base=source().bar_end.replace(year=clock.now.year,month=clock.now.month,day=clock.now.day)
        values=tuple(source(bar_end=base+timedelta(minutes=i)) for i in range(count))
        return FetchResult(symbol=symbol,interval='1m',requested_bars=count,returned_bars=count,bars=values,complete=True,reason_code='OK')
    def quote(symbol,at,**kwargs):
        return QuoteResult(symbol=symbol,complete=True,reason_code='OK',quote=MarketQuote(symbol=symbol,
            quote_time=clock.now,price=10,open=10,previous_close=10,volume=10000,amount=100000))
    from liangjian_funnel.data.cache import MinuteBarStore
    app=SimpleNamespace(settings=settings,trading_calendar=CAL,market_data=object(),_fetch_live_bars=bars,_fetch_live_quote=quote,
        minute_store=MinuteBarStore(settings.minute_cache_dir))
    import liangjian_funnel.runtime.live_market as live
    monkeypatch.setattr(live,'load_or_refresh_live_market_state',lambda *a,**kw:dict(status='READY',
        as_of=clock.now.isoformat(),trade_date=clock.now.date().isoformat(),entry_permission='ALLOW'))
    # This fixture isolates source -> eligible engine candidate -> review ->
    # paper wiring, not the strategy shape itself (covered by strategy tests).
    import liangjian_funnel.runtime.strategies as strategy
    original_evaluate=strategy.evaluate_strategy
    monkeypatch.setattr(strategy,'evaluate_strategy',lambda *a,**kw:
        original_evaluate(*a,**kw).model_copy(update={'action':'BUY_SIGNAL'}))
    def handler(request):
        seen.append(json.loads(request.read()));runtime=json.loads(seen[-1]['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        text=seen[-1]['messages'][0]['content'];refs=json.JSONDecoder().raw_decode(text.split('TECH_REFERENCE=',1)[1])[0]
        raw=output('BUY_SIGNAL');raw['envelope']=runtime['required_envelope']
        raw['decision_id']=runtime['inputs']['DECISION_ID'];raw['minute_snapshot_id']=runtime['inputs']['MINUTE_SNAPSHOT_ID']
        raw['signals'][0].update(supporting_refs=['LIVE:MARKET'],opposing_refs=['TECH:p1'],tech_reference_review=[
            dict(item=k,status=v,my_read='isolated alternative evidence LIVE:MARKET') for k,v in refs['p1'].items() if v=='UNMET'])
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    trader=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1)
    def proof(*args,**kwargs):
        return dict(status='KNOWN',halted=False,limit_up=False,limit_down=False,entry_tradable=True)
    return app,clock,seen,trader,proof


def test_control_keeps_customer_first_and_explicit_record_skips_team_without_exposure(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_monitor as module
    settings=Settings.from_env({},root=tmp_path);seen=[]
    app=SimpleNamespace(settings=settings,monitor_once=lambda **kw:seen.append('legacy') or {'status':'READY'})
    monkeypatch.setattr(module,'run_monitor_control',lambda *a,**k:seen.append('control') or {'status':'BLOCKED'})
    result=WorkflowApplication.monitor_dispatch(app)
    assert result['status']=='READY' and result['team']['status']=='BLOCKED' and seen==['legacy','control']
    app.settings=settings.model_copy(update={'a4_trader_mode':'record'})
    WorkflowApplication.monitor_dispatch(app);assert seen==['legacy','control','legacy']
    kwargs=[];app.monitor_once=lambda **kw:kwargs.append(kw) or {'status':'READY'}
    WorkflowApplication.monitor_dispatch(app)
    assert len(kwargs)==1 and kwargs[0]['now'] is None and kwargs[0]['archive_original_quotes']
    assert kwargs[0]['deadline_monotonic']>0 and kwargs[0]['minute_sources'] is not None
    app.settings=settings.model_copy(update={'broker_adapter':'gm_live'})
    result=WorkflowApplication.monitor_dispatch(app)
    assert result['status']=='READY' and result['mode']=='record'
    assert result['team']['reason_code']=='TEAM_BROKER_ADAPTER_NOT_ENABLED'


def test_0327_exposure_failure_runs_legacy_each_tick_and_deduplicates_daily_alert(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_record_protection as module
    from datetime import datetime
    from zoneinfo import ZoneInfo
    calls=[]
    settings=Settings.from_env({'LIANGJIAN_A4_TRADER_MODE':'record'},root=tmp_path)
    app=SimpleNamespace(settings=settings,monitor_once=lambda **kw:calls.append(kw) or {'status':'READY'})
    def broken(*a,**kw):raise sqlite3.OperationalError('PRIVATE_LOCK_DETAIL')
    monkeypatch.setattr(module,'has_owned_exposure',broken)
    now=datetime(2026,10,19,10,0,tzinfo=ZoneInfo('Asia/Shanghai'))
    for tick in (now,now+timedelta(minutes=1)):
        result=WorkflowApplication.monitor_dispatch(app,now=tick)
        assert result['status']=='READY'
        assert result['team_record_exposure']['reason_code']=='TEAM_OWNED_EXPOSURE_CHECK_FAILED'
        assert result['team_record_exposure']['internal_alert']['status']=='QUEUED'
        assert 'PRIVATE' not in json.dumps(result)
    assert len(calls)==2 and all(row['archive_original_quotes'] for row in calls)
    with sqlite3.connect(tmp_path/'state/team_v2/team.db') as db:
        rows=db.execute('SELECT kind,source_id,status FROM team_notifications').fetchall()
        assert rows==[('ALERT','TEAM_OWNED_EXPOSURE_CHECK_FAILED','PENDING')]


def test_0327_alert_store_failure_cannot_block_legacy_monitor(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_record_protection as module
    import liangjian_funnel.runtime.team_v2_internal as internal
    def broken(*a,**kw):raise RuntimeError('PRIVATE_UNAVAILABLE')
    monkeypatch.setattr(module,'has_owned_exposure',broken)
    monkeypatch.setattr(internal,'InternalOutbox',broken)
    app=SimpleNamespace(settings=Settings.from_env({'LIANGJIAN_A4_TRADER_MODE':'record'},root=tmp_path),
        monitor_once=lambda **kw:{'status':'READY'})
    result=WorkflowApplication.monitor_dispatch(app)
    assert result['status']=='READY' and result['team_record_exposure']['internal_alert']['status']=='FAILED'
    assert 'PRIVATE' not in json.dumps(result)


def test_actual_control_task_source_to_mocked_HTTP_to_owned_paper_not_legacy(tmp_path,monkeypatch):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch);before=source_bytes(app.settings)
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert result['mode']=='control' and result['result']['trader']['model_validated']
    assert result['result']['trader']['admissions'][0]['dispatched']
    assert len(seen)==1 and source_bytes(app.settings)==before


def test_original_quote_archive_failure_cannot_grant_entry_or_call_legacy(tmp_path,monkeypatch):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch,trade_date='2026-10-20');before=source_bytes(app.settings)
    import liangjian_funnel.runtime.team_v2_quote_evidence as archive
    def broken(*args,**kwargs):raise RuntimeError('PRIVATE_FILESYSTEM_ERROR_NOT_TO_LOG')
    monkeypatch.setattr(archive,'archive_monitor_quotes',broken)
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert result['original_quote_archive']['status']=='FAILED'
    assert result['status']=='CONTROL_RECORD_REQUIRED' and result['record_effective_next_tick']
    assert not seen
    assert source_bytes(app.settings)==before and 'PRIVATE' not in json.dumps(result)


@pytest.mark.parametrize('archive_fails',[False,True])
def test_owned_record_protection_also_archives_without_model_or_new_buy(tmp_path,monkeypatch,archive_fails):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch);before=source_bytes(app.settings)
    if archive_fails:
        import liangjian_funnel.runtime.team_v2_quote_evidence as archive
        def broken(*args,**kwargs):raise RuntimeError('PRIVATE_ARCHIVE_ERROR')
        monkeypatch.setattr(archive,'archive_monitor_quotes',broken)
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,
        tradability_loader=proof,record_fallback={'reason_code':'EXPLICIT_RECORD_MODE'})
    assert result['original_quote_archive']['status']==('FAILED' if archive_fails else 'ARCHIVED')
    assert not seen and source_bytes(app.settings)==before and 'PRIVATE' not in json.dumps(result)
    with sqlite3.connect(app.settings.root/'state/team_v2/paper.db') as db:
        assert db.execute('SELECT count(*) FROM paper_orders').fetchone()[0]==0


def test_shadow_rule_material_is_not_a_new_production_entry_gate(tmp_path,monkeypatch):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api)
    assert result['result']['trader']['model_validated'] and len(seen)==1
    assert result['result']['trader']['admissions'][0]['dispatched']
    assert not (app.settings.root/'state/team_v2/pit').exists()


def test_actual_task_same_minute_retry_reuses_model_and_single_order(tmp_path,monkeypatch):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    first=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    clock.now+=timedelta(seconds=1)
    second=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert first['packet_sha256']==second['packet_sha256'] and len(seen)==1
    with closing(sqlite3.connect(app.settings.root/'state/team_v2/paper.db')) as db:
        assert db.execute('SELECT count(*) FROM paper_orders').fetchone()[0]==1


def test_after0945_without_current_primary_record_cannot_keep_yesterday_permission(tmp_path,monkeypatch):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch);advance(clock,9,46)
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert result['result']['trader']['model_validated'] is None and not seen
    assert result['result']['trader']['admissions']==[]


def test_current_entry_block_does_not_turn_plan_into_untradable_for_held_exits(tmp_path,monkeypatch):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    def blocked(*a,**k):return {**proof(*a,**k),'entry_tradable':False}
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=blocked)
    assert result['result']['trader']['model_validated'] is None and not seen
    assert result['result']['trader']['admissions']==[]


def test_new_regime_sidecar_failure_does_not_crash_the_monitor_or_allow_new_entry(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_live_regime_source as source_module
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    def failed(*a,**kw):raise RuntimeError('PRIVATE_NOT_FOR_LOG')
    monkeypatch.setattr(source_module,'record_current_regime',failed)
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert result['status']=='DEGRADED' and result['result']['trader']['model_validated'] is None
    assert not seen and result['result']['trader']['admissions']==[]
    assert {'reason_code':'CONTROL_LIVE_REGIME_UPDATE_FAILED'} in result['source_reasons']
    assert 'PRIVATE_NOT_FOR_LOG' not in json.dumps(result)


def test_changed_risk_yaml_cannot_silently_reuse_morning_authority(tmp_path,monkeypatch):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    path=app.settings.root/'config/risk_limits.yaml';path.write_text(path.read_text()+'\n# changed after morning\n')
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert result['result']['trader_skip_reason']=='CONTROL_FRESH_ACCOUNT_ENVELOPE_UNPROVEN' and not seen


@pytest.mark.parametrize('kind',['symbol','cluster','account','failure_latch'])
def test_persisted_exception_controls_block_entry_after_restart_not_morning_only(tmp_path,monkeypatch,kind):
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    from liangjian_funnel.runtime.team_v2_risk_exception import RiskExceptionRuntime
    from liangjian_funnel.runtime.team_v2_morning import ACCOUNT
    from test_team_v2_risk_exception import response
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    rt=RiskExceptionRuntime(TeamMeetingStore(app.settings.root/'state/team_v2/team.db'))
    target_id=ACCOUNT if kind in ('account','failure_latch') else '000001.SZ' if kind=='symbol' else 'bank'
    target='account' if kind=='failure_latch' else kind
    domain=[dict(target=target,target_id=target_id,data_ready=True,trigger_cleared=False)]
    raw=response();raw['exception_actions']=[dict(raw['exception_actions'][0],target=target,target_id=target_id)]
    for index in range(2 if kind=='failure_latch' else 1):
        rt.claim(str(index),account_id=ACCOUNT,trade_date='2026-10-19',observed_at=clock.now-timedelta(minutes=1),
            input_sha256=str(index),targets=domain,hard_halt=False)
        rt.record(str(index),source_call_id=str(index),input_sha256=str(index),output=raw,validated=kind!='failure_latch')
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    if kind=='failure_latch':
        # Invalid advisory exceptions cannot manufacture an account halt.
        assert result['result']['trader']['model_validated'] is True and seen
    else:
        assert result['result']['trader']['model_validated'] is None and not seen
        assert result['result']['trader']['admissions']==[]


def test_engine_start_confirmation_never_calls_review_model(tmp_path,monkeypatch):
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    import liangjian_funnel.runtime.strategies as strategy
    original=strategy.evaluate_strategy
    monkeypatch.setattr(strategy,'evaluate_strategy',lambda *a,**kw:
        original(*a,**kw).model_copy(update={'action':'START_CONFIRMATION'}))
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert not seen and result['result']['trader']['candidate_review']['candidate_count']==0


def test_missing_morning_keeps_existing_inventory_hard_exit_without_any_model(tmp_path,monkeypatch):
    from test_team_v2_paper_ledger import stamp,bar
    from liangjian_funnel.runtime.broker.local_paper import create_broker
    from liangjian_paper_broker.matching import PaperRules
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch);settings=app.settings
    broker=create_broker('local_paper',database=settings.root/'state/team_v2/paper.db',
        paper_config=settings.root/'config/paper_broker.yaml',risk_config=settings.root/'config/risk_limits.yaml',
        clock=lambda:clock.now,calendar=CAL,run_id='isolated-inventory')
    # Isolated prior-day inventory plus original owner row, not a production
    # backfill. Remove only the owned morning marker to inject the failure.
    clock.now=stamp(day='16',tm='09:59:00')
    broker.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='prior',
        plan_id='old-held',signal_id='prior',no_chase_price='10.2',tradable=True)
    clock.now=stamp(day='16',tm='10:03:00');broker.on_bar(bar(day='16',tm='10:00:00'))
    with closing(sqlite3.connect(settings.state_db_path)) as db,db:
        row=db.execute('SELECT * FROM execution_plans WHERE plan_id=?',('p1',)).fetchone()
        db.execute('INSERT INTO execution_plans VALUES (?,?,?,?,?,?,?)',
            ('old-held',row[1],row[2],'INVALIDATED','2026-10-16T09:32:00+08:00','2026-10-16T15:00:00+08:00',row[-1]))
    with closing(sqlite3.connect(settings.root/'state/team_v2/team.db')) as db,db:db.execute('DELETE FROM morning_engine')
    clock.now=stamp(tm='09:36:00');before=source_bytes(settings)
    def stopped(symbol,interval,count,cutoff,**kwargs):
        rows=tuple(source(bar_end=source().bar_end+timedelta(minutes=i),open=8.9,low=8.5,high=9.1,close=8.9,amount=89000)
            for i in range(count))
        return FetchResult(symbol=symbol,interval='1m',requested_bars=count,returned_bars=count,bars=rows,complete=True,reason_code='OK')
    app._fetch_live_bars=stopped
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert result['reason_code']=='CONTROL_MORNING_ENGINE_MISSING' and not seen
    assert result['result']['hard_exits'][0]['dispatched']
    assert broker.get_orders('2026-10-19')[0]['side']=='SELL' and source_bytes(settings)==before
