"""Exact A6 exception scope and owned tightening, never a hard-halt unlock."""
from copy import deepcopy
from datetime import timedelta
import sqlite3

import pytest

from liangjian_funnel.pipeline.team_v2_risk_schema import validate_risk_exception
from liangjian_funnel.runtime.team_v2_risk_exception import apply_exception_review, RiskExceptionRuntime
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from test_team_v2_risk_schema import output, CAT, ENV
from test_team_v2_store import AT


def targets():
    return [dict(target='symbol',target_id='600000',trigger_cleared=False,data_ready=True),
            dict(target='cluster',target_id='bank',trigger_cleared=False,data_ready=True)]


def response(action='HALT_NEW_ENTRY'):
    raw=output();raw.update(session='INTRADAY_EXCEPTION',envelope_adjustments=[])
    raw['exception_actions']=[dict(target=t['target'],target_id=t['target_id'],action=action,
        reason_code='RISK_CHANGED',release_condition='server trigger clears',supporting_refs=['C1'],
        opposing_refs=['M1'],what_would_change_my_mind=['fresh risk evidence']) for t in targets()]
    return raw


def validate(raw,**changes):
    args=dict(required_envelope=ENV,targets=targets(),resume_allowed=set(),hard_halt=False,
        engine_utilization_cap=75,capacity_slots=3,cluster_caps={'bank':2})
    args.update(changes)
    return validate_risk_exception(raw,CAT,**args)


@pytest.mark.parametrize('action',['REDUCE_HALF','EXIT','HALT_NEW_ENTRY','NO_ACTION'])
def test_each_target_has_exactly_one_evidence_bound_action(action):
    assert validate(response(action)).valid


@pytest.mark.parametrize('fault',['duplicate','missing','extra','wrong_kind','wrong_id','bad_type',
    'refs','falsifier','reason','release','session','morning','cap','capacity','unknown_root'])
def test_exception_contract_rejects_instead_of_repairing(fault):
    raw=response();row=raw['exception_actions'][0]
    if fault=='duplicate':raw['exception_actions'].append(deepcopy(row))
    elif fault=='missing':raw['exception_actions'].pop()
    elif fault=='extra':raw['exception_actions'].append(dict(row,target_id='600001'))
    elif fault=='wrong_kind':row['target']='account'
    elif fault=='wrong_id':row['target_id']='bank'
    elif fault=='bad_type':row['target_id']=[]
    elif fault=='refs':row['supporting_refs']=[]
    elif fault=='falsifier':row['what_would_change_my_mind']=[]
    elif fault=='reason':row['reason_code']=''
    elif fault=='release':row['release_condition']=''
    elif fault=='session':raw['session']='PREMARKET'
    elif fault=='morning':raw['envelope_adjustments']=[{}]
    elif fault=='cap':raw['utilization_cap']['cap_pct']=76
    elif fault=='capacity':raw['capacity_for_tomorrow']['plans_acceptable']=99
    elif fault=='unknown_root':raw['orders']=[]
    assert not validate(raw).valid


def test_resume_needs_same_a6_target_server_cleared_and_no_hard_halt():
    raw=response('RESUME');cleared=[dict(t,trigger_cleared=True) for t in targets()]
    allowed={('symbol','600000'),('cluster','bank')}
    assert validate(raw,targets=cleared,resume_allowed=allowed).valid
    assert not validate(raw,targets=cleared,resume_allowed=allowed,hard_halt=True).valid
    assert not validate(raw,resume_allowed=allowed).valid
    assert not validate(raw,targets=cleared,resume_allowed={('symbol','600000')}).valid
    limited=[dict(t,data_ready=False) for t in cleared]
    assert not validate(raw,targets=limited,resume_allowed=allowed).valid
    assert not validate(response('NO_ACTION'),targets=limited).valid


def test_controls_only_tighten_no_action_preserves_and_resume_removes_only_own_target():
    first=apply_exception_review(response(),targets=targets(),previous_controls=[],validated=True,
        previous_failures=0,hard_halt=False)
    assert len(first['controls'])==2 and first['status']=='READY'
    second=apply_exception_review(response('NO_ACTION'),targets=targets(),previous_controls=first['controls'],
        validated=True,previous_failures=0,hard_halt=False)
    assert second['controls']==first['controls']
    raw=response('RESUME');raw['exception_actions'][1]['action']='NO_ACTION'
    third=apply_exception_review(raw,targets=[dict(t,trigger_cleared=True) for t in targets()],
        previous_controls=first['controls'],validated=True,previous_failures=0,hard_halt=False)
    assert [(r['target'],r['target_id']) for r in third['controls']]==[('cluster','bank')]
    denied=apply_exception_review(raw,targets=targets(),previous_controls=first['controls'],validated=True,
        previous_failures=0,hard_halt=False)
    assert denied['status']=='DEGRADED' and denied['controls']==first['controls']


def runtime(tmp_path):
    return RiskExceptionRuntime(TeamMeetingStore(tmp_path/'team.db'))


def record(rt,mid,raw,*,now=AT,hard=False,domain=None):
    domain=domain or targets()
    context=rt.claim(mid,account_id='paper',trade_date='2026-10-19',observed_at=now,
        input_sha256='input:'+mid,targets=domain,hard_halt=hard)
    return rt.record(mid,source_call_id='call:'+mid,input_sha256='input:'+mid,output=raw,validated=True),context


def test_atomic_controls_failure_latch_retry_and_restart(tmp_path):
    rt=runtime(tmp_path);first,ctx=record(rt,'one',response())
    assert len(ctx['targets'])==2 and first['consecutive_failures']==0
    assert record(rt,'one',response())[0]==first
    broken=response();broken['exception_actions'].pop()
    bad,_=record(rt,'two',broken,now=AT+timedelta(minutes=5))
    assert bad['status']=='DEGRADED' and bad['risk_unit']=='PROBE' and bad['max_batches']==1
    rt=runtime(tmp_path)
    halt,_=record(rt,'three',broken,now=AT+timedelta(minutes=10))
    assert halt['status']=='HALT' and halt['consecutive_failures']==2
    later,_=record(rt,'four',response('RESUME'),now=AT+timedelta(minutes=15),
        domain=[dict(t,trigger_cleared=True) for t in targets()])
    assert later['status']=='HALT' and later['consecutive_failures']==2
    assert len(later['controls'])==2
    with sqlite3.connect(rt.store.path) as db:
        assert db.execute('SELECT halted FROM risk_day_state').fetchall()==[(1,)]
        assert db.execute('SELECT count(*) FROM risk_exception_reviews').fetchone()==(4,)


def test_claim_conflicts_and_out_of_order_do_not_write_results(tmp_path):
    rt=runtime(tmp_path);record(rt,'one',response())
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):
        rt.claim('one',account_id='other',trade_date='2026-10-19',observed_at=AT,
            input_sha256='input:one',targets=targets(),hard_halt=False)
    with pytest.raises(ValueError,match='OUT_OF_ORDER'):
        record(rt,'older',response(),now=AT-timedelta(minutes=5))


def test_transaction_fault_does_not_commit_controls_or_failure_count(tmp_path):
    rt=runtime(tmp_path)
    rt.claim('fault',account_id='paper',trade_date='2026-10-19',observed_at=AT,
        input_sha256='input:fault',targets=targets(),hard_halt=False)
    with sqlite3.connect(rt.store.path) as db:
        db.execute("CREATE TRIGGER reject_result BEFORE INSERT ON risk_exception_reviews BEGIN SELECT RAISE(ABORT,'TEST_FAULT'); END")
    with pytest.raises(sqlite3.IntegrityError):
        rt.record('fault',source_call_id='call:fault',input_sha256='input:fault',output=response(),validated=True)
    with sqlite3.connect(rt.store.path) as db:
        assert db.execute('SELECT count(*) FROM risk_exception_controls').fetchone()==(0,)
        assert db.execute('SELECT count(*) FROM risk_day_state').fetchone()==(0,)


def test_concurrent_meetings_cannot_overwrite_newer_controls_or_morning_latch(tmp_path):
    rt=runtime(tmp_path)
    for mid in ('one','two'):
        rt.claim(mid,account_id='paper',trade_date='2026-10-19',observed_at=AT,
            input_sha256='input:'+mid,targets=targets(),hard_halt=False)
    rt.record('one',source_call_id='call:one',input_sha256='input:one',output=response(),validated=True)
    with pytest.raises(ValueError,match='STATE_CHANGED'):
        rt.record('two',source_call_id='call:two',input_sha256='input:two',output=response('RESUME'),validated=True)
    assert len(rt.restrictions('paper','2026-10-19')['controls'])==2


def test_foreign_account_target_is_rejected_before_claim(tmp_path):
    with pytest.raises(ValueError,match='ACCOUNT_MISMATCH'):
        runtime(tmp_path).claim('one',account_id='paper',trade_date='2026-10-19',observed_at=AT,
            input_sha256='one',targets=[dict(target='account',target_id='other',data_ready=True,trigger_cleared=False)],
            hard_halt=False)


def test_intraday_utilization_ceiling_only_decreases_across_valid_reviews(tmp_path):
    rt=runtime(tmp_path);raw=response();raw['utilization_cap']['cap_pct']=25
    assert record(rt,'one',raw)[0]['utilization_cap']=='25'
    raw['utilization_cap']['cap_pct']=75
    assert record(rt,'two',raw,now=AT+timedelta(minutes=5))[0]['utilization_cap']=='25'
    assert runtime(tmp_path).restrictions('paper','2026-10-19')['utilization_cap']=='25'
