"""Actual capture clock and closed exchange session never impersonate each other."""
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace as NS
import json

import pytest
import liangjian_funnel.workflow as workflow
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.closed_session_capture import (
    KEY, build_capture_proof, validate_capture_proof, generation_capture_proof,
)
from liangjian_funnel.pipeline.team_v2_research_time import research_times
from test_a1_daily_revalidation_0595 import app_fixture
from test_close_regeneration_natural_0603 import fixture as regen_fixture, HEAD
from liangjian_funnel.runtime.close_regeneration import bind_close_regeneration

CAL = ExchangeTradingCalendar()
def at(value): return datetime.fromisoformat(value+'+08:00')

def clock(monkeypatch, value):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    monkeypatch.setattr(workflow,'datetime',Clock)

def proof(start='2026-10-23T00:20:00',capture=None,end=None,source='2026-10-22'):
    return build_capture_proof(source_trade_date=source,started_at=at(start),
        captured_at=at(capture or start),completed_at=at(end or start),calendar=CAL)

@pytest.mark.parametrize('stamp,source,target',[
    ('2026-10-22T23:44:00','2026-10-22','2026-10-23'),
    ('2026-10-23T00:20:00','2026-10-22','2026-10-23'),
    ('2026-10-24T12:00:00','2026-10-23','2026-10-26'),
    ('2026-10-02T12:00:00','2026-09-30','2026-10-08'),
])
def test_calendar_source_not_capture_day(stamp,source,target):
    p=proof(stamp,source=source)
    assert p['source_trade_date']==source and p['target_trade_date']==target
    assert p['capture_as_of']==at(stamp).isoformat()
    snap=NS(as_of=at(stamp),data={KEY:p,'MARKET_DATA_AS_OF':p['source_market_close_at']})
    captured,market=research_times(snap,CAL)
    assert captured==snap.as_of and market.date().isoformat()==source

def test_collect_may_finish_after_midnight_without_backdating():
    p=proof('2026-10-22T23:59:00','2026-10-23T00:01:00','2026-10-23T00:02:00')
    assert validate_capture_proof(p,captured_at=at('2026-10-23T00:01:00'),calendar=CAL,
        now=at('2026-10-23T01:00:00'))==p

@pytest.mark.parametrize('key,value',[
    ('authority','FAKE'),('source_trade_date','2026-10-21'),
    ('target_trade_date','2026-10-26'),('source_market_close_at','2026-10-22T14:59:00+08:00'),
    ('capture_as_of','2026-10-22T15:00:00+08:00'),
    ('capture_completed_at','2026-10-23T00:19:00+08:00'),
    ('publication_deadline','2026-10-23T07:00:00+08:00'),
    ('capture_started_at','2026-10-23T00:20:00'),
])
def test_no_magic_authority_or_relabelled_time(key,value):
    p=proof();p[key]=value
    with pytest.raises(ValueError):validate_capture_proof(p,captured_at=at('2026-10-23T00:20:00'),calendar=CAL)

@pytest.mark.parametrize('now',['2026-10-23T00:19:59','2026-10-23T06:30:00','2026-10-23T09:00:00'])
def test_future_and_publication_deadline_remain_blocking(now):
    with pytest.raises(ValueError):validate_capture_proof(proof(),captured_at=at('2026-10-23T00:20:00'),calendar=CAL,now=at(now))

def test_unbound_trading_midnight_and_changed_market_still_fail():
    with pytest.raises(ValueError):research_times(NS(as_of=at('2026-10-23T00:20:00'),data={}),CAL)
    with pytest.raises(ValueError):research_times(NS(as_of=at('2026-10-23T00:20:00'),data={KEY:proof(),'MARKET_DATA_AS_OF':'2026-10-21T15:00:00+08:00'}),CAL)

def seed_fixture(tmp_path,monkeypatch,stamp):
    app,original,old,data=app_fixture(tmp_path,monkeypatch)
    app.trading_calendar=CAL
    p=proof(stamp);capture=at(stamp)
    data[KEY]=p;data['MARKET_DATA_AS_OF']=p['source_market_close_at']
    data['snapshot_manifest']['as_of']=capture.isoformat()
    raw=dict(snapshot_id=old.snapshot.snapshot_id,snapshot_hash=workflow._hash_json(data),as_of=capture.isoformat(),data=data)
    old.path.write_text(json.dumps(raw),encoding='utf-8')
    clock(monkeypatch,capture+timedelta(seconds=1))
    prepared=app._load_research_snapshot_by_id(old.snapshot.snapshot_id,expected_date=None,closed_session_source_date='2026-10-22')
    return app,original,prepared,p

@pytest.mark.parametrize('stamp',['2026-10-22T23:44:00','2026-10-23T00:20:00'])
@pytest.mark.parametrize('reuse',[False,True])
def test_real_registry_seals_capture_and_reuses_exact_seed_no_fetch(tmp_path,monkeypatch,stamp,reuse):
    app,original,prepared,p=seed_fixture(tmp_path,monkeypatch,stamp)
    seen=[]
    def prepare(**kw):seen.append(kw);return prepared
    app.prepare_snapshot=prepare
    old=(original.as_of,original.manifest_hash,original.payload_hash)
    result=app.run_a1_maintenance(now=at(stamp)+timedelta(seconds=1),daily_refresh=True,
        source_trade_date='2026-10-22',snapshot_id=prepared.snapshot.snapshot_id if reuse else None)
    assert result['status']=='PUBLISHED',result
    active=app.a1_registry.get_active_generation()
    assert active.as_of==prepared.snapshot.as_of and active.snapshot_hash==prepared.snapshot.snapshot_hash
    assert generation_capture_proof(active,calendar=CAL,now=at(stamp)+timedelta(seconds=1),source_trade_date='2026-10-22')==p
    assert len(seen)==(0 if reuse else 1)
    if not reuse:
        assert seen[0]['as_of']==at(stamp)+timedelta(seconds=1)
        assert seen[0]['market_data_as_of']==at('2026-10-22T15:00:00')
    assert (original.as_of,original.manifest_hash,original.payload_hash)==old
    # The published current pointer does not recollect or regenerate A1.
    assert app._ensure_daily_a1(at(stamp)+timedelta(seconds=1))['status']=='CURRENT'
    assert len(seen)==(0 if reuse else 1)

def test_seed_and_registry_hash_mutations_fail_closed(tmp_path,monkeypatch):
    app,_,prepared,p=seed_fixture(tmp_path,monkeypatch,'2026-10-23T00:20:00')
    raw=json.loads(prepared.path.read_text());raw['data'][KEY]['source_trade_date']='2026-10-21'
    prepared.path.write_text(json.dumps(raw))
    with pytest.raises(workflow.WorkflowError,match='SNAPSHOT_HASH_MISMATCH'):
        app._load_research_snapshot_by_id(prepared.snapshot.snapshot_id,expected_date=None,closed_session_source_date='2026-10-22')

def test_natural_zero_regeneration_uses_bound_capture_not_source_date(tmp_path):
    app,_,_,generation=regen_fixture(tmp_path)
    current=at('2026-10-23T00:21:00');p=proof()
    generation.as_of=at('2026-10-23T00:20:00')
    generation.manifest={'delta':{'daily_refresh_proof':dict(closed_session=p,
        snapshot_id=generation.snapshot_id,snapshot_hash=generation.snapshot_hash)}}
    bound=bind_close_regeneration(app,'source',now=current,head=HEAD,recovery_attempt=2)
    assert bound.trade_date=='2026-10-22' and bound.target_trade_date=='2026-10-23'
    assert bound.source_data_as_of==generation.as_of and bound.closed_session==p
    assert bound.source_kind=='NATURAL_ZERO_PLAN_SOURCE_REGEN' and bound.old_plan_ids==()
    assert list(app.store.list_execution_plans())==[]
    generation.manifest['delta']['daily_refresh_proof']['snapshot_hash']='f'*64
    with pytest.raises(ValueError):bind_close_regeneration(app,'source',now=current,head=HEAD,recovery_attempt=2)

def test_public_source_argument_cannot_bypass_live_clock(tmp_path,monkeypatch):
    app,_,_,_=app_fixture(tmp_path,monkeypatch);app.trading_calendar=CAL
    clock(monkeypatch,at('2026-10-23T00:20:00'))
    with pytest.raises(workflow.WorkflowError,match='LIVE_FACTS_POINT_IN_TIME_UNSUPPORTED'):
        app.run_a1_maintenance(now=at('2026-10-22T15:00:00'),daily_refresh=True,source_trade_date='2026-10-22')
    with pytest.raises(workflow.WorkflowError,match='A1_SOURCE_DATE_REQUIRES_DAILY_REFRESH'):
        app.run_a1_maintenance(source_trade_date='2026-10-22')

def test_real_natural_close_entry_keeps_midnight_clock_and_no_account_rollover(tmp_path,monkeypatch):
    from test_natural_close_role_composition import shell
    app,dispatched,sync,_=shell(tmp_path,monkeypatch)
    current=at('2026-10-23T00:20:00');clock(monkeypatch,current)
    app.trading_calendar=CAL
    calls=[];app._ensure_trading_day=lambda *a,**k:calls.append((a,k))
    original_publish=app._publish_plans;publishing=[]
    def publish(*args,**kwargs):publishing.append(kwargs);return original_publish(*args,**kwargs)
    app._publish_plans=publish
    app._ensure_daily_a1=lambda *_:None
    result=app.run_research('close',primary_only=True,from_active_a1=True,
        schedule_comparison=False,reuse_resume_snapshot=False,run_id_override='DUAL_CLOCK_NATURAL')
    assert sync[0]['as_of']==current and sync[0]['market_data_as_of']==at('2026-10-22T15:00:00')
    assert sync[0]['closed_session_source_date']=='2026-10-22'
    assert calls[0][0][0]==at('2026-10-22T15:00:00') and calls[0][1]['synchronize_accounts'] is False
    assert result['market_trade_date']=='2026-10-22' and result['target_trade_date']=='2026-10-23'
    assert publishing[0]['minimum_trade_date'].isoformat()=='2026-10-23'
    assert len(dispatched)==1

def test_registry_rejects_changed_closed_proof_without_touching_old_generation(tmp_path,monkeypatch):
    from liangjian_funnel.pipeline.a1_registry import validate_a1_generation_contract,A1RegistryError
    app,_,prepared,_=seed_fixture(tmp_path,monkeypatch,'2026-10-23T00:20:00')
    app.prepare_snapshot=lambda **_:prepared
    result=app.run_a1_maintenance(daily_refresh=True,source_trade_date='2026-10-22')
    assert result['status']=='PUBLISHED'
    active=app.a1_registry.get_active_generation()
    for field,value in [('snapshot_hash','f'*64),('snapshot_file_sha256','invalid')]:
        manifest=deepcopy(active.manifest);payload=deepcopy(active.payload)
        manifest['delta']['daily_refresh_proof'][field]=value
        payload['delta']['daily_refresh_proof']=deepcopy(manifest['delta']['daily_refresh_proof'])
        with pytest.raises(A1RegistryError):validate_a1_generation_contract(manifest,payload)

def test_unknown_calendar_and_missing_proof_cannot_enable_midnight():
    calendar=NS(is_trading_day=lambda _:(_ for _ in ()).throw(ValueError('CALENDAR_UNAVAILABLE')))
    with pytest.raises(ValueError):build_capture_proof(source_trade_date='2026-10-22',
        started_at=at('2026-10-23T00:20:00'),captured_at=at('2026-10-23T00:20:00'),
        completed_at=at('2026-10-23T00:20:00'),calendar=calendar)
    with pytest.raises(ValueError):validate_capture_proof(None,captured_at=at('2026-10-23T00:20:00'),calendar=CAL)

def test_cli_source_date_is_forwarded_without_relabelling_as_of(tmp_path,monkeypatch):
    from liangjian_funnel import cli
    from test_cli_workflow_coverage import _FakeApplication
    from liangjian_funnel.settings import Settings
    monkeypatch.setattr(cli,'WorkflowApplication',_FakeApplication)
    _FakeApplication.response={'status':'PUBLISHED'}
    assert cli.main(['run-a1-maintenance','--daily-refresh','--mode','incremental',
        '--source-trade-date','2026-10-22','--as-of','2026-10-23T00:20:00+08:00'],
        settings=Settings.from_env({},root=tmp_path))==0
    kwargs=_FakeApplication.calls[0][2]
    assert kwargs['source_trade_date']=='2026-10-22' and kwargs['now']==at('2026-10-23T00:20:00')

@pytest.mark.parametrize('manifest',[None,[],{'delta':None},{'delta':{'daily_refresh_proof':None}},
    {'delta':{'daily_refresh_proof':{'closed_session':None}}},
    {'delta':{'daily_refresh_proof':{'closed_session':{'authority':'0635'}}}}])
def test_existing_bad_manifest_or_explicit_bad_proof_never_falls_back(manifest):
    generation=NS(manifest=manifest,as_of=at('2026-10-22T16:00:00'))
    with pytest.raises(ValueError):generation_capture_proof(generation,calendar=CAL,
        now=at('2026-10-23T00:20:00'),source_trade_date='2026-10-22')

@pytest.mark.parametrize('capture',['2026-10-22T16:00:00','2026-10-23T00:20:00'])
@pytest.mark.parametrize('cutoff',['2026-10-22T15:00:00','2026-10-21T15:00:00','2026-10-24T15:00:00'])
def test_caller_market_cutoff_is_never_unlocked_by_formal_close(tmp_path,monkeypatch,capture,cutoff):
    from test_natural_close_role_composition import shell
    app,dispatched,sync,publication=shell(tmp_path,monkeypatch)
    clock(monkeypatch,at(capture));app.trading_calendar=CAL
    with pytest.raises(workflow.WorkflowError,match='MARKET_DATA_AS_OF_REQUIRES_NEXT_SESSION_PREP'):
        app.run_research('close',market_data_as_of=at(cutoff),primary_only=True)
    assert not dispatched and not sync and not publication

@pytest.mark.parametrize('container',['manifest','payload'])
@pytest.mark.parametrize('value',[None,[],42,'not-a-mapping'])
def test_registry_bad_delta_is_typed_contract_error(tmp_path,monkeypatch,container,value):
    from liangjian_funnel.pipeline.a1_registry import validate_a1_generation_contract,A1RegistryError
    _,original,_,_=app_fixture(tmp_path,monkeypatch)
    manifest,payload=deepcopy(original.manifest),deepcopy(original.payload)
    (manifest if container=='manifest' else payload)['delta']=value
    with pytest.raises(A1RegistryError,match='A1_CLOSED_SESSION_PROOF_INVALID'):
        validate_a1_generation_contract(manifest,payload)
