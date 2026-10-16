from copy import deepcopy
from datetime import datetime,timedelta
import json
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime.team_v2_live_regime_source import capture_sector_context,load_sector_context,record_current_regime
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from test_team_v2_regime import tick

ROOT=Path(__file__).resolve().parents[1]


def fixture(tmp_path):
    settings=SimpleNamespace(root=tmp_path,workflow_output_dir=tmp_path/'outputs')
    (tmp_path/'config').mkdir();shutil.copyfile(ROOT/'config/regime_live.yaml',tmp_path/'config/regime_live.yaml')
    at=tick('09:40');known=at.replace(hour=7,minute=25)
    cycle={'available':True,'as_of':'2026-10-16T15:00:00+08:00',
        'history_metrics':{'top3_daily_overlap':.6,'persistent_mainline_candidates':[{'industry_thscode':'881101.TI'}]}}
    prepared=SimpleNamespace(snapshot=SimpleNamespace(snapshot_id='base-valid',snapshot_hash='parent-hash',
        data={'SECTOR_CYCLE_SNAPSHOT':cycle}))
    capture_sector_context(settings,prepared,known_at=known)
    marker=settings.workflow_output_dir/'auction_base'/f'{at.date()}.json';marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({'status':'READY','prepared_snapshot':{'snapshot_id':'base-valid','snapshot_hash':'parent-hash'}}))
    return settings,TeamMeetingStore(tmp_path/'state/team_v2/team.db'),marker


def live(at,**kw):
    value={'status':'READY','as_of':(at+timedelta(minutes=1)).isoformat(),'breadth':.6,'fallback':False,
        'team_v2_source':{'status':'READY','source_as_of':at.isoformat(),'frame_sha256':'original-frame',
            'emotion':{'status':'READY','as_of':at.isoformat(),'universe_complete':True,
                'universe_excludes_st':True,'minimum_listing_days':5,'limit_up_count':45,
                'limit_down_count':3,'highest_streak':3,'broken_board_rate':.2}}}
    value.update(kw);return value


def test_actual_source_two_closed_ticks_persist_and_enable_only_after_confirmation(tmp_path):
    settings,store,marker=fixture(tmp_path)
    for tm in ('09:40','09:45'):
        at=tick(tm);out=record_current_regime(settings,store,live=live(at),cutoff=at,
            observed_at=at+timedelta(minutes=1),prior='ROTATION_NO_MAINLINE')
    assert out['regime']=='TREND_MAINLINE' and out['leader_open'] and out['radar_open']
    # Later arrivals in this tick do not overwrite the already frozen facts.
    again=record_current_regime(settings,store,live=live(at,breadth=.1),cutoff=at,
        observed_at=at+timedelta(minutes=2),prior='ROTATION_NO_MAINLINE')
    assert again==out
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT count(*) FROM regime_observations').fetchone()[0]==2
        assert db.execute('SELECT notification_status FROM regime_transitions').fetchall()==[('PENDING',),('PENDING',)]
    historical=load_sector_context(settings,now=at)
    assert historical['cycle']['as_of']=='2026-10-16T15:00:00+08:00' # not relabelled as today's history


def test_prior_bucket_cannot_confirm_next_tick_and_nonclosed_minutes_do_not_add_rows(tmp_path):
    settings,store,_=fixture(tmp_path);at=tick('09:40')
    first=record_current_regime(settings,store,live=live(at),cutoff=at,
        observed_at=at+timedelta(minutes=1),prior='TREND_MAINLINE')
    assert not first['leader_open']
    assert record_current_regime(settings,store,live=live(at),cutoff=at+timedelta(minutes=1),
        observed_at=at+timedelta(minutes=2),prior='TREND_MAINLINE')==first
    newer=at+timedelta(minutes=5)
    out=record_current_regime(settings,store,live=live(at),cutoff=newer,
        observed_at=newer+timedelta(minutes=1),prior='TREND_MAINLINE')
    assert out['regime']=='DATA_BLOCK' and not out['leader_open']


@pytest.mark.parametrize('defect',['parent_running','parent_hash','future_known','tamper'])
def test_unverified_parent_cannot_supply_sector_mainline(tmp_path,defect):
    settings,store,marker=fixture(tmp_path);at=tick('09:40')
    if defect.startswith('parent'):
        body=json.loads(marker.read_bytes())
        if defect=='parent_running':body['status']='RUNNING'
        else:body['prepared_snapshot']['snapshot_hash']='other'
        marker.write_text(json.dumps(body))
    elif defect=='future_known':at=at.replace(hour=7,minute=0)
    else:
        path=tmp_path/'state/team_v2/sector_context'/f'{at.date()}.json'
        body=json.loads(path.read_bytes());body['cycle']['history_metrics']['top3_daily_overlap']=1;path.write_text(json.dumps(body))
    assert load_sector_context(settings,now=at)['cycle']=={}


def test_emotion_unavailable_does_not_block_supported_primary_trend(tmp_path):
    settings,store,_=fixture(tmp_path)
    for tm in ('09:40','09:45'):
        at=tick(tm);facts=live(at);facts['team_v2_source']['emotion']={'status':'DATA_BLOCK'}
        out=record_current_regime(settings,store,live=facts,cutoff=at,
            observed_at=at+timedelta(minutes=1),prior='ROTATION_NO_MAINLINE')
    assert out['regime']=='TREND_MAINLINE' and not out['leader_open'] and not out['radar_open']
