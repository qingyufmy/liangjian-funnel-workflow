"""Actual RO legacy SQL -> owned engine -> real clients at mocked HTTP -> PM."""
from copy import deepcopy
from contextlib import closing
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from liangjian_funnel.settings import Settings
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
from liangjian_funnel.runtime.team_v2_sources import read_planning_scope
from test_team_v2_risk_schema import output as risk_output
from test_team_v2_pm_schema import ranking_response as pm_output

ROOT=Path(__file__).resolve().parents[1]
CAL=SimpleNamespace(is_trading_day=lambda _:True,previous_trading_day=lambda day:
    date(2026,10,19) if day==date(2026,10,20) else date(2026,10,16))


def fixture(tmp_path, *, trade_date='2026-10-19'):
    shutil.copytree(ROOT/'config',tmp_path/'config')
    shutil.copytree(ROOT/'prompts/team_v2',tmp_path/'prompts/team_v2')
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    settings.state_db_path.parent.mkdir(parents=True,exist_ok=True)
    source_day=CAL.previous_trading_day(date.fromisoformat(trade_date)).isoformat()
    raw=dict(symbol='000001.SZ',source_run_id='close-original',reference_price=10,
        reference_price_as_of=source_day+'T15:00:00+08:00',trigger_low=9.8,trigger_high=10.1,
        stop_level=9,no_chase=10.2,cluster_label='bank',market_regime='ROTATION_NO_MAINLINE',
        plan_expiry=source_day+'T15:00:00+08:00')
    with closing(sqlite3.connect(settings.state_db_path)) as db,db:
        db.execute('CREATE TABLE execution_plans(plan_id TEXT PRIMARY KEY,lane_id TEXT,symbol TEXT,status TEXT,valid_from TEXT,expires_at TEXT,payload_json TEXT)')
        db.execute('INSERT INTO execution_plans VALUES (?,?,?,?,?,?,?)',('p1',settings.research_primary_lane_id,
            raw['symbol'],'PENDING_MORNING_REVIEW',trade_date+'T09:32:00+08:00',trade_date+'T15:00:00+08:00',json.dumps(raw)))
    clock=SimpleNamespace(now=datetime.fromisoformat(trade_date+'T08:33:00+08:00'))
    return settings,clock


def advance(clock,hour,minute):clock.now=clock.now.replace(hour=hour,minute=minute)


def source_bytes(settings):return settings.state_db_path.read_bytes()


def client(settings,*,risk_cap=65,probe=True,offline=False):
    seen=[]
    def handler(request):
        req=json.loads(request.read());runtime=json.loads(req['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        seen.append(req)
        if offline:raise httpx.ConnectError('PRIVATE_NO_LOG',request=request)
        envelope=runtime['required_envelope']
        raw=risk_output() if envelope['stage_id']=='A6' else pm_output()
        raw['envelope']=envelope
        if envelope['stage_id']=='A6':raw['utilization_cap']['cap_pct']=risk_cap
        else:
            raw['plan_allocations'][0].update(max_batches=1,intended_risk_unit='PROBE' if probe else 'STANDARD')
            bounds=[min(40,risk_cap),min(65,risk_cap)]
            raw['deployment_target'].update(target_pct=min(60,risk_cap),baseline_range=bounds)
        body=json.dumps(raw).replace('"C1"','"MORNING:ACCOUNT"').replace('"M1"','"MORNING:MARKET"')
        body=body.replace('"A"','"MORNING:ACCOUNT"').replace('"M"','"MORNING:MARKET"')
        return httpx.Response(200,json={'choices':[{'message':{'content':body}}]},request=request)
    return OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1),seen


def test_actual_three_stage_source_to_model_to_allocation_retains_legacy_bytes(tmp_path):
    settings,clock=fixture(tmp_path);before=source_bytes(settings);api,seen=client(settings)
    first=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    assert first['status']=='COMPLETE' and first['envelopes'][0]['purpose']=='PLANNING'
    assert first['source']['plan_rows'][0]['payload_json'].find('2026-10-16T15:00')>=0
    assert first['source']['plans'][0]['expires_at']=='2026-10-19T15:00:00+08:00'
    assert run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)==first
    advance(clock,8,35)
    assert run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)['status']=='READY'
    advance(clock,8,38)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=api)
    assert result['status']=='READY' and result['slot_plan'][0]['primary_plan_id']=='p1'
    visible=json.JSONDecoder().raw_decode(seen[-1]['messages'][0]['content'].split('PLANS_TODAY=',1)[1])[0]
    assert visible=={'source':'EVIDENCE_CATALOG','plan_evidence_ids':['MORNING:PLAN:p1']}


    assert run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=api)==result
    assert len(seen)==2 and source_bytes(settings)==before
    with sqlite3.connect(tmp_path/'state/team_v2/team.db') as db:
        assert db.execute('SELECT count(*) FROM morning_engine').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM risk_meetings').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM pm_decisions').fetchone()[0]==1
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()


def test_first_day_engine_probe_cannot_be_promoted_by_pm_standard_fixture(tmp_path):
    settings,clock=fixture(tmp_path,trade_date='2026-10-20');api,_=client(settings,probe=False)
    engine=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    assert engine['envelopes'][0]['risk_unit']=='PROBE'
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)
    advance(clock,8,38)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=api)
    assert result['status']=='READY'
    assert result['model_validated'] is False
    assert result['decision_authority']=='SERVER_FALLBACK_NOT_MODEL_VALIDATED'
    assert result['plan_allocations'][0]['intended_risk_unit']=='PROBE'


def test_empty_plan_day_still_prepares_emotion_seed_without_inventing_morning_permission(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_emotion_baseline as source
    settings,clock=fixture(tmp_path);calls=[]
    with closing(sqlite3.connect(settings.state_db_path)) as db,db:db.execute('DELETE FROM execution_plans')
    def prepare(*a,**kw):calls.append(kw['clock']());return {'status':'READY','body_sha256':'seed-only'}
    monkeypatch.setattr(source,'prepare_emotion_baseline',prepare)
    result=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    assert result['status']=='BLOCKED' and len(calls)==1
    assert not (settings.root/'state/team_v2/paper.db').exists()


def test_a6_lower_cap_clips_pm_baseline_without_loosening(tmp_path):
    settings,clock=fixture(tmp_path);api,seen=client(settings,risk_cap=40)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)
    advance(clock,8,38)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=api)
    assert result['status']=='READY' and result['deployment_target']['target_pct']==40


def test_missing_a6_is_explicit_conservative_not_a_fabricated_success(tmp_path):
    settings,clock=fixture(tmp_path);api,seen=client(settings,probe=True)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,38)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=api)
    assert result['status']=='READY' and result['plan_allocations'][0]['intended_risk_unit']=='PROBE'
    assert 'A6_MORNING_MISSING' in seen[0]['messages'][0]['content']
    with sqlite3.connect(tmp_path/'state/team_v2/team.db') as db:
        assert db.execute('SELECT count(*) FROM risk_meetings').fetchone()[0]==0


@pytest.mark.parametrize('field,value',[('reference_price',None),('reference_price',True),('cluster_label',''),
    ('reference_price_as_of','2026-10-19T09:25:00+08:00'),('trigger_high',11),('source_run_id','')])
def test_unproven_plan_does_not_create_an_executable_substitute(tmp_path,field,value):
    settings,clock=fixture(tmp_path)
    with sqlite3.connect(settings.state_db_path) as db:
        raw=json.loads(db.execute('SELECT payload_json FROM execution_plans').fetchone()[0]);raw[field]=value
        db.execute('UPDATE execution_plans SET payload_json=?',(json.dumps(raw),))
    before=source_bytes(settings)
    result=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    assert result['status']=='BLOCKED' and result['source']['reason_codes']
    assert not (tmp_path/'state/team_v2/paper.db').exists() and source_bytes(settings)==before


def test_changed_plan_between_edges_blocks_without_model_call(tmp_path):
    settings,clock=fixture(tmp_path);run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    with sqlite3.connect(settings.state_db_path) as db:db.execute("UPDATE execution_plans SET status='ACTIVE_TODAY'")
    advance(clock,8,35);api,seen=client(settings)
    assert run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)['reason_code']=='MORNING_PLAN_ROWS_CHANGED'
    assert not seen


def test_pending_null_activation_is_planning_only_never_manufactured(tmp_path):
    from dataclasses import replace
    from liangjian_funnel.runtime.risk_engine import AccountRisk, RiskLimits, build_risk_envelope
    from liangjian_funnel.runtime.team_v2_sources import decode_plan
    settings,clock=fixture(tmp_path)
    with sqlite3.connect(settings.state_db_path) as db:db.execute('UPDATE execution_plans SET valid_from=NULL')
    before=source_bytes(settings)
    result=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    assert result['status']=='COMPLETE' and result['source']['plans'][0]['valid_from'] is None
    plan=decode_plan(result['source']['plans'][0])
    account=AccountRisk(1000000,1000000,1000000,1000000)
    execution=build_risk_envelope(plan,account,(),RiskLimits(),now=clock.now.replace(hour=10),regime='ROTATION_NO_MAINLINE')
    assert not execution.eligible and 'PLAN_NOT_YET_VALID' in execution.blocked_reasons
    with pytest.raises(ValueError,match='ACTIVATION_TIME_MISSING'):replace(plan,status='ACTIVE_TODAY')
    assert source_bytes(settings)==before


def test_old_held_owner_is_read_without_activation_or_omitting_exposure(tmp_path):
    from liangjian_funnel.runtime.team_v2_sources import read_held_bindings
    settings,clock=fixture(tmp_path)
    with sqlite3.connect(settings.state_db_path) as db:
        row=list(db.execute('SELECT * FROM execution_plans').fetchone())
        row[0]='old-held';row[3]='CLOSED';row[4]='2026-10-15T09:32:00+08:00';row[5]='2026-10-15T15:00:00+08:00'
        db.execute('INSERT INTO execution_plans VALUES (?,?,?,?,?,?,?)',row)
    before=source_bytes(settings)
    owners=read_held_bindings(settings,[dict(plan_id='old-held',symbol='000001.SZ')])
    assert owners[0]['original_row']['status']=='CLOSED' and owners[0]['cluster']=='bank'
    assert source_bytes(settings)==before
    with pytest.raises(ValueError,match='HELD_PLAN_SCOPE_UNPROVEN'):
        read_held_bindings(settings,[dict(plan_id='does-not-exist',symbol='000001.SZ')])


def test_missing_planning_source_does_not_create_a_legacy_database(tmp_path):
    settings,clock=fixture(tmp_path);settings.state_db_path.unlink()
    with pytest.raises(ValueError,match='SOURCE_DATABASE_UNAVAILABLE'):
        read_planning_scope(settings,now=clock.now,calendar=CAL)
    assert not settings.state_db_path.exists()


def test_missing_engine_does_not_invent_account_or_call_model(tmp_path):
    settings,clock=fixture(tmp_path);advance(clock,8,35);api,seen=client(settings)
    assert run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)['reason_code']=='MORNING_ENGINE_MISSING'
    assert not seen and not (tmp_path/'state/team_v2').exists()


@pytest.mark.parametrize('stage,timevalue',[('engine','08:32:59'),('engine','08:35:00'),('risk','08:37:00'),('pm','08:40:00')])
def test_outside_window_no_owned_or_legacy_write(tmp_path,stage,timevalue):
    settings,clock=fixture(tmp_path);clock.now=datetime.fromisoformat('2026-10-19T'+timevalue+'+08:00')
    assert run_morning_stage(settings,stage,clock=lambda:clock.now,calendar=CAL)['status']=='NOOP'
    assert not (tmp_path/'state/team_v2').exists()


def test_cli_has_three_independent_entries_without_legacy_constructor(tmp_path,monkeypatch,capsys):
    import liangjian_funnel.cli as cli
    import liangjian_funnel.runtime.team_v2_morning as morning
    settings,_=fixture(tmp_path);seen=[]
    def forbidden(*a,**kw):raise AssertionError('LEGACY_CONSTRUCTOR_FORBIDDEN')
    monkeypatch.setattr(cli,'WorkflowApplication',forbidden);monkeypatch.setattr(cli,'RuntimeStore',forbidden)
    monkeypatch.setattr(morning,'run_morning_stage',lambda s,stage:seen.append(stage) or {'status':'COMPLETE'})
    for name in ('engine','risk','pm'):
        assert cli.main(['run-team-morning-'+name],settings=settings)==0
    assert seen==['engine','risk','pm']
