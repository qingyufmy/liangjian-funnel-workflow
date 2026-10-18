from copy import deepcopy
from datetime import datetime
from pathlib import Path
import pytest
import sqlite3

from liangjian_funnel.runtime.team_v2_regime import load_rules,classify_primary,classify_emotion,advance

CONFIG=Path(__file__).resolve().parents[1]/'config/regime_live.yaml'


def sector(overlap=.4,persistent=True):
    return dict(available=True,history_metrics=dict(top3_daily_overlap=overlap,
        persistent_mainline_candidates=[{'industry_thscode':'bank'}] if persistent else []))


def emotion(**changes):
    return dict(status='READY',universe_excludes_st=True,minimum_listing_days=5,
        universe_complete=True,limit_up_count=40,broken_board_rate=.35,
        highest_streak=3,limit_down_count=10,**changes)


def tick(clock):return datetime.fromisoformat('2026-10-19T'+clock+':00+08:00')


def test_rules_authorized_and_original_primary_thresholds():
    rules=load_rules(CONFIG)
    assert rules['emotion']['enabled'] and rules['emotion']['status']=='APPROVED'
    assert classify_primary(.35,{},rules)=='RISK_OFF_RETREAT'
    assert classify_primary(.36,sector(),rules)=='TREND_MAINLINE'
    assert classify_primary(.8,sector(.39),rules)=='ROTATION_NO_MAINLINE'
    assert classify_primary(.8,sector(persistent=False),rules)=='ROTATION_NO_MAINLINE'
    assert classify_primary(None,sector(),rules)=='DATA_BLOCK'
    assert classify_primary(.8,{},rules)=='DATA_BLOCK'


def test_route_policy_is_explicit_and_cannot_silently_revive_global_stop(tmp_path):
    import yaml
    rules=load_rules(CONFIG)
    assert rules['route_gating']['existing_strategies']=='CURRENT_ENGINE_PERMISSION_ONLY'
    assert rules['route_gating']['risk_off_existing_risk_unit']=='PROBE'
    raw=yaml.safe_load(CONFIG.read_text(encoding='utf-8'))
    raw['route_gating']['existing_strategies']='BLOCK_ALL_ON_RISK_OFF'
    changed=tmp_path/'regime.yaml';changed.write_text(yaml.safe_dump(raw),encoding='utf-8')
    with pytest.raises(ValueError,match='LIVE_REGIME_CONFIG_INVALID'):load_rules(changed)


def test_primary_matches_original_workflow_for_all_declared_valid_combinations():
    from liangjian_funnel.workflow import _determine_market_regime
    rules=load_rules(CONFIG)
    for breadth in (0.,.2,.35,.350001,.5,.8,1.):
        for overlap in (.39,.4,.8):
            for persistent in (False,True):
                raw=sector(overlap,persistent)
                original,_=_determine_market_regime({'breadth':breadth},raw,rotation_overlap_threshold=.4)
                assert classify_primary(breadth,raw,rules)==original


@pytest.mark.parametrize('changes,expected',[
    ({},'EMOTION_ON'),({'limit_up_count':19},'EMOTION_OFF'),
    ({'limit_down_count':20},'EMOTION_OFF'),({'broken_board_rate':.51},'EMOTION_OFF'),
    ({'limit_up_count':30},'KEEP'),({'limit_up_count':20,'broken_board_rate':.5},'KEEP'),
    ({'universe_complete':False},'DATA_BLOCK'),({'minimum_listing_days':4},'DATA_BLOCK'),
    ({'universe_excludes_st':False},'DATA_BLOCK'),({'highest_streak':True},'DATA_BLOCK'),
    ({'broken_board_rate':float('nan')},'DATA_BLOCK')])
def test_emotion_exact_thresholds_and_verified_universe(changes,expected):
    raw=emotion();raw.update(changes)
    assert classify_emotion(raw,'TREND_MAINLINE',load_rules(CONFIG))==expected


def test_missing_emotion_fields_not_guessed_from_breadth_or_previous_state():
    raw=emotion();del raw['highest_streak']
    assert classify_emotion(raw,'TREND_MAINLINE',load_rules(CONFIG))=='DATA_BLOCK'
    assert classify_emotion(emotion(),'RISK_OFF_RETREAT',load_rules(CONFIG))=='EMOTION_OFF'


def step(previous,clock,primary='TREND_MAINLINE',state='EMOTION_ON'):
    return advance(previous,tick=tick(clock),primary=primary,emotion=state,prior='ROTATION_NO_MAINLINE',rules=load_rules(CONFIG))


def test_two_consecutive_bars_live_authority_after0945_and_reproducible():
    original={};first=step(original,'09:35');second=step(first['state'],'09:40')
    third=step(second['state'],'09:45')
    assert original=={} and not first['leader_open'] and not second['live_authority']
    assert second['regime']=='ROTATION_NO_MAINLINE'
    assert third['regime']=='TREND_MAINLINE' and third['leader_open'] and third['radar_open'] and third['live_authority']
    assert step(second['state'],'09:45')==third


def test_lunch_gap_and_missing_bar_cannot_confirm_pending_switch():
    first=step({},'11:30')
    after=step(first['state'],'13:05')
    assert not after['leader_open'] and after['state']['emotion_pending_count']==1
    skipped=step(after['state'],'13:15')
    assert not skipped['leader_open'] and skipped['state']['emotion_pending_count']==1
    assert step(skipped['state'],'13:20')['leader_open']


def test_hysteresis_and_missing_data_close_route_not_positions():
    a=step({},'09:40');b=step(a['state'],'09:45')
    keep=step(b['state'],'09:50',state='KEEP')
    assert keep['leader_open'] and keep['state']['emotion_confirmed']=='EMOTION_ON'
    missing=step(keep['state'],'09:55',state='DATA_BLOCK')
    assert not missing['leader_open'] and missing['emotion_state']=='DATA_BLOCK'
    assert missing['exit_authority_changed'] is False
    off=step(missing['state'],'10:00',state='EMOTION_OFF')
    assert step(off['state'],'10:05',state='EMOTION_OFF')['emotion_state']=='EMOTION_OFF'


@pytest.mark.parametrize('clock',['09:30','12:00','13:00','15:05'])
def test_nonclosed_session_tick_rejected(clock):
    with pytest.raises(ValueError,match='REGIME_TICK_INVALID'):step({},clock)


def facts(at):
    return dict(market=dict(status='READY',as_of=at.isoformat(),fallback=False,breadth=.7),
        sector_cycle=sector(),sector_cycle_known_at='2026-10-16T16:30:00+08:00',
        emotion=emotion()|dict(as_of=at.isoformat()))


def test_owned_store_idempotence_source_binding_and_pending_internal_transitions(tmp_path):
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    store=TeamMeetingStore(tmp_path/'team.db');rules=load_rules(CONFIG)
    for tm in ('09:40','09:45'):
        at=tick(tm);args=dict(facts=facts(at),tick=at,observed_at=at,prior='ROTATION_NO_MAINLINE',rules=rules)
        result=store.record_live_regime(**args)
    assert result['leader_open']
    assert store.record_live_regime(**args)==result
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT count(*) FROM regime_observations').fetchone()[0]==2
        assert db.execute('SELECT notification_status FROM regime_transitions').fetchall()==[('PENDING',),('PENDING',)]
    changed=deepcopy(args);changed['facts']['market']['breadth']=.1
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):store.record_live_regime(**changed)


def test_cache_bucket_reuse_and_missing_emotion_not_current_permission(tmp_path):
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    store=TeamMeetingStore(tmp_path/'team.db');rules=load_rules(CONFIG)
    for tm in ('09:40','09:45'):
        at=tick(tm);store.record_live_regime(facts=facts(at),tick=at,observed_at=at,prior='TREND_MAINLINE',rules=rules)
    at=tick('09:50')
    result=store.record_live_regime(facts=facts(tick('09:45')),tick=at,observed_at=at,prior='TREND_MAINLINE',rules=rules)
    assert not result['leader_open'] and result['regime']=='DATA_BLOCK'


def test_transition_insert_fault_rolls_back_observation_and_confirmation(tmp_path):
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    store=TeamMeetingStore(tmp_path/'team.db');rules=load_rules(CONFIG)
    at=tick('09:40');store.record_live_regime(facts=facts(at),tick=at,observed_at=at,prior='TREND_MAINLINE',rules=rules)
    with sqlite3.connect(store.path) as db:
        db.execute("CREATE TRIGGER fail_transition BEFORE INSERT ON regime_transitions BEGIN SELECT RAISE(ABORT,'FAULT'); END")
    at=tick('09:45');args=dict(facts=facts(at),tick=at,observed_at=at,prior='TREND_MAINLINE',rules=rules)
    with pytest.raises(sqlite3.IntegrityError):store.record_live_regime(**args)
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT count(*) FROM regime_observations').fetchone()[0]==1
        db.execute('DROP TRIGGER fail_transition')
    assert store.record_live_regime(**args)['leader_open']
