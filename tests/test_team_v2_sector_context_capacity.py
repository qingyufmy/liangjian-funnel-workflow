"""Byte limits/projection contract; offline fixtures and durable queue only."""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.team_v2_live_regime_source import capture_sector_context, load_sector_context
from liangjian_funnel.runtime.team_v2_sector_context import (
    AUTHORITY, load_capacity, project_cycle, projected_body, seal)
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore, canonical
from liangjian_funnel.runtime.team_v2_regime import classify_primary, load_rules

ROOT=Path(__file__).resolve().parents[1]
FROZEN=ROOT.parent/'liangjian_team_v2_20261016/artifacts/team-v2-iteration-20261020/frozen'
NOW=datetime(2026,10,20,9,45,tzinfo=ZoneInfo('Asia/Shanghai'))


def settings_for(tmp_path):
    (tmp_path/'config').mkdir()
    for name in ('team_v2_sector_context.yaml','regime_live.yaml'):
        shutil.copyfile(ROOT/'config'/name,tmp_path/'config'/name)
    return SimpleNamespace(root=tmp_path,workflow_output_dir=tmp_path/'outputs')


def install(settings,body,*,raw=None,parent=None):
    path=settings.root/'state/team_v2/sector_context/2026-10-20.json'
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(raw if raw is not None else canonical(body).encode())
    marker=settings.workflow_output_dir/'auction_base/2026-10-20.json'
    marker.parent.mkdir(parents=True,exist_ok=True)
    marker.write_text(json.dumps(parent if parent is not None else {'status':'READY','prepared_snapshot':{
        'snapshot_id':body['source_snapshot_id'],'snapshot_hash':body['source_snapshot_hash']}}),encoding='utf-8')
    return path,marker


def legacy(cycle=None):
    return seal(dict(schema_version='team-sector-context/1',trade_date='2026-10-20',
        known_at='2026-10-20T07:20:00+08:00',source_snapshot_id='frozen-source',source_snapshot_hash='source-hash',
        authority=AUTHORITY,cycle=cycle or {'available':True,'as_of':'2026-10-20T07:00:00+08:00',
            'history_metrics':{'top3_daily_overlap':0.6,'persistent_mainline_candidates':[{'industry_thscode':'881166.TI'}]},
            'unused_history':'x'*2455000}))


def rows(store):
    with sqlite3.connect(store.path) as db:
        return db.execute('SELECT kind,status,source_id,body_json FROM team_notifications').fetchall()


def test_projection_retains_exact_consumed_values_and_does_not_mutate_source():
    body=legacy();before=deepcopy(body);cycle=project_cycle(body['cycle'])
    assert set(cycle)=={'as_of','available','history_metrics'}
    assert set(cycle['history_metrics'])=={'top3_daily_overlap','persistent_mainline_candidates'}
    assert cycle['history_metrics']==body['cycle']['history_metrics'] and body==before
    cycle['history_metrics']['persistent_mainline_candidates'].clear()
    assert body==before


def test_legacy_larger_than_old_2mib_verifies_then_projects_without_rewriting(tmp_path):
    settings=settings_for(tmp_path);body=legacy();path,_=install(settings,body)
    original=path.read_bytes();assert len(original)>2*1024*1024
    result=load_sector_context(settings,now=NOW)
    assert result['source_schema_version']=='team-sector-context/1'
    assert result['body_sha256']==body['body_sha256']
    assert result['projected_bytes']<=load_capacity(settings)['projected_max_bytes']
    assert result['cycle']==project_cycle(body['cycle']) and path.read_bytes()==original


def test_today_real_signed_2457074b_original_projects_to_measured_912b(tmp_path):
    source=FROZEN/'sector.json';parent=FROZEN/'auction.json'
    if not source.is_file() or not parent.is_file():
        pytest.skip('Frozen 2026-10-20 evidence not present in this checkout')
    raw=source.read_bytes();original_parent=parent.read_bytes()
    assert len(raw)==2457074
    assert hashlib.sha256(raw).hexdigest()=='4a3cdc58a4fef58ca01663c5cc1720dc94538b70ce5a936d386990f3713acab6'
    assert hashlib.sha256(original_parent).hexdigest()=='f427db64ff4e44d25eea5aa4ec3ef2f4d1f5dd45f2a9c4bd8a11d1b767290a35'
    settings=settings_for(tmp_path);body=json.loads(raw)
    path,_=install(settings,body,raw=raw,parent=json.loads(original_parent))
    result=load_sector_context(settings,now=NOW)
    assert result['source_bytes']==2457074 and result['projected_bytes']==912
    assert result['projected_max_bytes']==1824 and result['body_sha256']==body['body_sha256']
    rules=load_rules(settings.root/'config/regime_live.yaml')
    assert classify_primary(.6,result['cycle'],rules)==classify_primary(.6,body['cycle'],rules)=='ROTATION_NO_MAINLINE'
    assert path.read_bytes()==raw and source.read_bytes()==raw and parent.read_bytes()==original_parent


def test_new_writer_and_reader_share_exact_projected_limit_and_compact_body(tmp_path):
    settings=settings_for(tmp_path);source=legacy();cycle=source['cycle']
    prepared=SimpleNamespace(snapshot=SimpleNamespace(snapshot_id='frozen-source',snapshot_hash='source-hash',
        data={'SECTOR_CYCLE_SNAPSHOT':cycle}))
    receipt=capture_sector_context(settings,prepared,known_at=source['known_at'])
    path=settings.root/'state/team_v2/sector_context/2026-10-20.json';body=json.loads(path.read_bytes())
    assert body['schema_version']=='team-sector-context/2' and len(path.read_bytes())<1824
    assert 'unused_history' not in body['cycle']
    # Natural producer writes before auction_base's READY parent marker.
    assert not (settings.workflow_output_dir/'auction_base/2026-10-20.json').exists()
    assert capture_sector_context(settings,prepared,known_at=source['known_at'])['body_sha256']==receipt['body_sha256']
    install(settings,body)
    result=load_sector_context(settings,now=NOW)
    assert receipt['projected_max_bytes']==result['projected_max_bytes']==1824
    assert receipt['projected_bytes']==result['projected_bytes']==len(path.read_bytes())
    assert capture_sector_context(settings,prepared,known_at=source['known_at'])['body_sha256']==receipt['body_sha256']
    # Even discarded research data cannot silently change under one frozen
    # producer identity: raw-cycle digest remains part of retry equality.
    cycle['unused_history']='changed-unconsumed-data'
    with pytest.raises(ValueError,match='TEAM_SECTOR_CONTEXT_CONFLICT'):
        capture_sector_context(settings,prepared,known_at=source['known_at'])


@pytest.mark.parametrize('kind',['legacy_input','projected'])
def test_three_times_capacity_is_specific_and_durable_internal_alert_deduplicates(tmp_path,kind):
    settings=settings_for(tmp_path);policy=load_capacity(settings);body=legacy()
    if kind=='legacy_input':
        maximum=policy['legacy_input_max_bytes'];raw=canonical(body).encode()
        raw+=b' '*(maximum*3-len(raw));expected='TEAM_SECTOR_CONTEXT_INPUT_SIZE_EXCEEDED'
    else:
        body['cycle']['history_metrics']['persistent_mainline_candidates']=[{'industry_thscode':'x'*(policy['projected_max_bytes']*3)}]
        body=projected_body(body);raw=canonical(body).encode();maximum=policy['projected_max_bytes']
        expected='TEAM_SECTOR_CONTEXT_PROJECTED_SIZE_EXCEEDED'
    path,_=install(settings,body,raw=raw);store=TeamMeetingStore(tmp_path/'state/team_v2/team.db')
    first=load_sector_context(settings,now=NOW,store=store)
    second=load_sector_context(settings,now=NOW,store=store)
    assert first['cycle']=={} and first['reason_code']==expected
    assert first['actual_bytes']>=maximum*3 and first['limit_bytes']==maximum
    assert first['internal_alert_status']=='PENDING' and first['internal_alert_event_id']==second['internal_alert_event_id']
    queued=rows(store);assert len(queued)==1 and queued[0][:2]==('ALERT','PENDING')
    assert expected in queued[0][3] and str(first['actual_bytes']) in queued[0][3]
    assert path.read_bytes()==raw and not (tmp_path/'state/team_v2/paper.db').exists()


def test_oversize_producer_does_not_publish_file_but_enqueues_specific_failure(tmp_path):
    settings=settings_for(tmp_path);cycle=legacy()['cycle']
    cycle['history_metrics']['persistent_mainline_candidates']=[{'industry_thscode':'x'*6000}]
    prepared=SimpleNamespace(snapshot=SimpleNamespace(snapshot_id='frozen-source',snapshot_hash='source-hash',data={'SECTOR_CYCLE_SNAPSHOT':cycle}))
    result=capture_sector_context(settings,prepared,known_at='2026-10-20T07:20:00+08:00')
    assert result['status']=='DATA_LIMITED' and result['reason_code']=='TEAM_SECTOR_CONTEXT_PROJECTED_SIZE_EXCEEDED'
    assert result['internal_alert_status']=='PENDING'
    assert not (tmp_path/'state/team_v2/sector_context/2026-10-20.json').exists()
    assert len(rows(TeamMeetingStore(tmp_path/'state/team_v2/team.db')))==1


@pytest.mark.parametrize('defect,expected',[
    ('hash','TEAM_SECTOR_CONTEXT_HASH_MISMATCH'),
    ('parent_hash','TEAM_SECTOR_PARENT_BINDING_UNPROVEN'),
    ('parent_id','TEAM_SECTOR_PARENT_BINDING_UNPROVEN'),
    ('parent_running','TEAM_SECTOR_PARENT_BINDING_UNPROVEN'),
    ('future_known','TEAM_SECTOR_CONTEXT_TIME_UNPROVEN'),
    ('trade_date','TEAM_SECTOR_CONTEXT_TIME_UNPROVEN'),
    ('future_cycle','TEAM_SECTOR_CYCLE_TIME_UNPROVEN'),
    ('authority','TEAM_SECTOR_CONTEXT_SCHEMA_INVALID'),
])
def test_old_full_signature_parent_and_timing_are_still_required(tmp_path,defect,expected):
    settings=settings_for(tmp_path);body=legacy();parent={'status':'READY','prepared_snapshot':{'snapshot_id':'frozen-source','snapshot_hash':'source-hash'}}
    if defect=='hash':body['cycle']['unused_history']='tampered'
    elif defect=='parent_hash':parent['prepared_snapshot']['snapshot_hash']='other'
    elif defect=='parent_id':parent['prepared_snapshot']['snapshot_id']='other'
    elif defect=='parent_running':parent['status']='RUNNING'
    else:
        body.pop('body_sha256')
        if defect=='future_known':body['known_at']='2026-10-20T10:00:00+08:00'
        elif defect=='trade_date':body['trade_date']='2026-10-19'
        elif defect=='future_cycle':body['cycle']['as_of']='2026-10-20T10:00:00+08:00'
        elif defect=='authority':body['authority']='UNTRUSTED'
        body=seal(body)
    install(settings,body,parent=parent)
    assert load_sector_context(settings,now=NOW)['reason_code']==expected


def test_capacity_config_cannot_silently_expand_multiplier(tmp_path):
    settings=settings_for(tmp_path);config=settings.root/'config/team_v2_sector_context.yaml'
    config.write_text(config.read_text().replace('capacity_multiplier: 2','capacity_multiplier: 3'))
    assert load_sector_context(settings,now=NOW)['reason_code']=='TEAM_SECTOR_CAPACITY_CONFIG_INVALID'
