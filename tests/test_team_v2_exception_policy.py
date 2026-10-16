from copy import deepcopy
from decimal import Decimal as D
from pathlib import Path

import pytest

from liangjian_funnel.runtime.team_v2_exception_policy import validate_exception_policy,load_exception_policy
from liangjian_funnel.runtime.team_v2_exception_coordinator import exception_triggers,claim_wakeups,_tick_store,cleared_origins
from liangjian_funnel.runtime.risk_engine import RiskLimits
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from test_team_v2_exception_coordinator import snapshot,state
from test_team_v2_risk_engine import plan
from test_team_v2_paper_ledger import stamp

POLICY=dict(version='exception-triggers/1',approved_by='claude',daily_loss_review_at='0.70',
    cluster_exposure_review_at='0.90',position_stop_proximity='0.50',drawdown_review_at='0.75',
    max_reviews_per_object_type_day=2)


def trigger(snap,*,plans=None,live=None):
    return exception_triggers(snap,plans=plans or {},limits=RiskLimits(),regime=live or state(),
        account_id='paper',tick=stamp(),trigger_policy=POLICY)


def test_yaml_review_policy_separate_from_risk_limits():
    root=Path(__file__).resolve().parents[1]
    policy=load_exception_policy(root/'config/risk_limits.yaml')
    assert {k:v for k,v in policy.items() if k!='config_sha256'}==POLICY
    assert RiskLimits.from_yaml(root/'config/risk_limits.yaml')==RiskLimits()


@pytest.mark.parametrize('key,value',[('daily_loss_review_at',True),('daily_loss_review_at','nan'),
    ('cluster_exposure_review_at','0'),('position_stop_proximity','1.01'),('drawdown_review_at',None),
    ('max_reviews_per_object_type_day',3),('approved_by','model'),('extra',1)])
def test_invalid_ratios_provenance_or_quota_block_config(key,value):
    with pytest.raises(ValueError,match='EXCEPTION_TRIGGER_POLICY_INVALID'):
        validate_exception_policy(POLICY|{key:value})


def test_exact_loss_drawdown_and_pending_cluster_ratios_not_limit_changes():
    snap=snapshot();snap['account']['nav']='986000'
    result=trigger(snap)
    assert result['events'][0]['reason_codes']==['DAILY_LOSS_REVIEW'] and not result['hard_halt']
    snap['account']['nav']='940000';snap['risk_day']['day_open_nav']='940000'
    assert trigger(snap)['events'][0]['reason_codes']==['DRAWDOWN_REVIEW']
    p=plan();snap=snapshot();snap['open_orders']=[dict(side='BUY',plan_id=p.plan_id,symbol=p.symbol,
        remaining_qty=36000,order_type='LIMIT',limit_price='10',no_chase_price='10.2')]
    assert trigger(snap,plans={p.plan_id:p})['events'][0]['reason_codes']==['CLUSTER_EXPOSURE_REVIEW']


def test_stop_proximity_uses_original_entry_risk_evidence_and_current_mark():
    snap=snapshot();p=plan()
    snap['positions']=[dict(plan_id=p.plan_id,symbol=p.symbol,qty=1000,market_value='9500',mark_minute_end=stamp().isoformat())]
    assert 'POSITION_STOP_REFERENCE_UNAVAILABLE:'+p.plan_id in trigger(snap,plans={p.plan_id:p})['annotations']
    snap['entry_risk_references']={p.plan_id:[dict(fill_id='fill1',entry_price='10',stop_price='9',initial_stop_distance='1')]}
    result=trigger(snap,plans={p.plan_id:p})
    assert result['events'][0]['reason_codes']==['POSITION_STOP_PROXIMITY']
    snap['positions'][0]['market_value']='9501'
    assert not trigger(snap,plans={p.plan_id:p})['events']


def test_emotion_flip_requires_real_prior_same_current_tick():
    live=state(emotion_state='EMOTION_OFF')
    live['transitions']=[dict(kind='emotion',previous='EMOTION_ON',current='EMOTION_OFF',tick=stamp().isoformat())]
    assert trigger(snapshot(),live=live)['events'][0]['reason_codes']==['EMOTION_STATE_CHANGED']
    live['transitions'][0]['previous']=None
    assert not trigger(snapshot(),live=live)['events']


def test_per_object_type_day_quota_idempotent_and_observation_only_after_two(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.db');_tick_store(store)
    events=[dict(target='cluster',target_id='bank',reason_codes=['CLUSTER_EXPOSURE_REVIEW'])]
    def claim(mid,account='paper',day='2026-10-19'):
        return claim_wakeups(store,account=account,day=day,meeting_id=mid,events=events,maximum=2)
    assert claim('a')==(events,[]) and claim('a')==(events,[])
    assert claim('b')==(events,[])
    assert claim('c')==([], [dict(target='cluster',target_id='bank',reason_code='CLUSTER_EXPOSURE_REVIEW')])
    assert claim('c',account='other')==(events,[]) and claim('c',day='2026-10-20')==(events,[])


def test_release_requires_original_reason_and_server_proof_not_absent_transition():
    live=state(emotion_state='EMOTION_OFF');live['transitions']=[dict(kind='emotion',previous='EMOTION_ON',current='EMOTION_OFF',tick=stamp().isoformat())]
    events=trigger(snapshot(),live=live)['events'];origin=[dict(target='account',target_id='paper',events=events)]
    neutral=trigger(snapshot(),live=state(emotion_state='EMOTION_OFF'))
    assert cleared_origins(origin,trigger=neutral,regime=state(emotion_state='EMOTION_OFF'))==[]
    assert cleared_origins(origin,trigger=neutral,regime=state(emotion_state='EMOTION_ON'))[0]['target_id']=='paper'
    neutral['hard_halt']=True
    assert cleared_origins(origin,trigger=neutral,regime=state(emotion_state='EMOTION_ON'))==[]
    origin[0]['events'][0]['reason_codes']=['UNKNOWN_OR_HARD_HALT']
    neutral['hard_halt']=False
    assert cleared_origins(origin,trigger=neutral,regime=state(emotion_state='EMOTION_ON'))==[]


def test_origin_is_committed_with_restriction_and_resume_deletes_it(tmp_path):
    from test_team_v2_risk_exception import runtime,response,targets
    from test_team_v2_store import AT
    from datetime import timedelta
    import sqlite3
    rt=runtime(tmp_path)
    evidence={'events':[dict(target='cluster',target_id='bank',reason_codes=['CLUSTER_CAP'])]}
    rt.claim('one',account_id='paper',trade_date='2026-10-19',observed_at=AT,
        input_sha256='one',targets=targets(),hard_halt=False,trigger_evidence=evidence)
    with sqlite3.connect(rt.store.path) as db:
        db.execute("CREATE TRIGGER fail_origin BEFORE INSERT ON risk_exception_control_origins BEGIN SELECT RAISE(ABORT,'FAULT'); END")
    with pytest.raises(sqlite3.IntegrityError):
        rt.record('one',source_call_id='first',input_sha256='one',output=response(),validated=True)
    assert not rt.restrictions('paper','2026-10-19')['controls']
    with sqlite3.connect(rt.store.path) as db:db.execute('DROP TRIGGER fail_origin')
    rt.record('one',source_call_id='first',input_sha256='one',output=response(),validated=True)
    origins=rt.restrictions('paper','2026-10-19')['origins']
    assert origins==[dict(target='cluster',target_id='bank',events=evidence['events'])]
    rt.claim('two',account_id='paper',trade_date='2026-10-19',observed_at=AT+timedelta(minutes=5),
        input_sha256='two',targets=[dict(t,trigger_cleared=True) for t in targets()],hard_halt=False)
    rt.record('two',source_call_id='second',input_sha256='two',output=response('RESUME'),validated=True)
    assert rt.restrictions('paper','2026-10-19')['origins']==[]


def test_release_cannot_mix_exhausted_and_new_type_to_bypass_daily_quota(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.db');_tick_store(store)
    event=dict(target='account',target_id='paper',reason_codes=['DAILY_LOSS_REVIEW'])
    for mid in ('a','b'):
        assert claim_wakeups(store,account='paper',day='2026-10-19',meeting_id=mid,events=[event],maximum=2)[0]
    release=dict(event,reason_codes=['DAILY_LOSS_REVIEW','DRAWDOWN_REVIEW'],release_check=True)
    accepted,exhausted=claim_wakeups(store,account='paper',day='2026-10-19',meeting_id='release',events=[release],maximum=2)
    assert not accepted and exhausted[0]['reason_code']=='RELEASE_REVIEW_QUOTA'


def test_original_filled_lot_carries_its_entry_stop_not_current_average_cost(tmp_path):
    from test_team_v2_control_minute import prepared,run
    from test_team_v2_paper_ledger import bar
    args,seen,raw,clock=prepared(tmp_path)
    run(args,bar(tm='10:00:00'))
    clock[0]=stamp(tm='10:02:00');run(args,bar(tm='10:01:00'))
    clock[0]=stamp(tm='10:03:00');run(args,bar(tm='10:02:00'))
    snapshot=args['broker'].planning_snapshot();p=next(iter(args['all_plans'].values()))
    references=snapshot['entry_risk_references'][p.plan_id]
    assert references and all(D(r['initial_stop_distance'])==D(r['entry_price'])-D(r['stop_price']) for r in references)
    assert all(D(r['stop_price'])==p.stop_price for r in references)
