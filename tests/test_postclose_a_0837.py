from contextlib import closing
from dataclasses import replace
from decimal import Decimal as D
import json

import pytest

from liangjian_funnel.runtime.team_v2_review_latch import ReviewDayLatch
from liangjian_funnel.runtime.team_v2_engine_only_policy import engine_only_geometry,half_position_quantity
from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal
from liangjian_funnel.runtime.team_v2_trader_session import run_trader_session
from test_team_v2_execution import fixture as execution_fixture
from test_team_v2_trader_session import fixture as model_fixture
from test_team_v2_candidate_review import prepared


def test_three_explicit_rejected_votes_do_not_latch_but_network_failures_do(tmp_path):
    args,_,_=prepared(tmp_path);latch=ReviewDayLatch(args['store'])
    for n in range(3):
        state=latch.record(account_id=args['account_id'],trade_date=args['trade_date'],decision_id='parsed'+str(n),
            valid=False,counts_as_failure=False,observed_at=args['decision_at'])
        assert state==dict(consecutive_failures=0,engine_only=False)
    for n in range(3):
        state=latch.record(account_id=args['account_id'],trade_date=args['trade_date'],decision_id='network'+str(n),
            valid=False,counts_as_failure=True,observed_at=args['decision_at'])
    assert state['engine_only'] and state['consecutive_failures']==3
    with pytest.raises(ValueError,match='ATTEMPT_CONFLICT'):
        latch.record(account_id=args['account_id'],trade_date=args['trade_date'],decision_id='parsed0',
            valid=False,counts_as_failure=True,observed_at=args['decision_at'])


def test_original_reference_vote_archived_separately_without_action_change(tmp_path):
    args,seen,raw=model_fixture(tmp_path)
    raw['signals'][0]['action']='NO_ACTION'
    raw['signals'][0]['tech_reference_review'][0]['status']='MET'
    args['capture_only']=True
    result=run_trader_session(**args)
    assert result['model_validated'] and result['explicit_original_decision'] and len(seen)==1
    assert result['captured_output']['signals'][0]['action']=='NO_ACTION'
    assert result['captured_output']['signals'][0]['tech_reference_review'][0]['status']=='UNMET'
    with closing(args['journal']._connect()) as db:
        original=json.loads(db.execute('select output_json from model_calls').fetchone()[0])
        projection=json.loads(db.execute('select body_json from model_output_projections').fetchone()[0])
    assert original['signals'][0]['tech_reference_review'][0]['status']=='MET'
    assert original['signals'][0]['action']==projection['projected']['signals'][0]['action']=='NO_ACTION'
    assert projection['changes'] and not args['broker'].get_orders(args['trade_date'])


def test_original_buy_rejected_alert_is_deduped_and_never_admitted(tmp_path):
    args,seen,raw=model_fixture(tmp_path,invalid=True)
    result=run_trader_session(**args)
    assert not result['model_validated'] and not result['admissions']
    assert result['rejected_entry_alert']
    assert run_trader_session(**args)==result and len(seen)==1
    with closing(args['store']._connect()) as db:
        events=db.execute('select kind,body_json from team_notifications').fetchall()
    assert len(events)==1 and events[0][0]=='A4_ORIGINAL_ENTRY_REJECTED'
    assert 'BUY_SIGNAL' in events[0][1]


@pytest.mark.parametrize('price,expected',[(D('10'),'REDUCED'),(D('10.1'),'ENGINE_ONLY_OUTSIDE_STRICT_ENTRY_RANGE'),(D('10'),'ENGINE_ONLY_NO_CHASE')])
def test_engine_only_geometry_no_chase_and_half_total_position(tmp_path,price,expected):
    broker,args=execution_fixture(tmp_path)
    plan=replace(args['plan'],price=price)
    authored=dict(trigger_low='9.9',trigger_high='10',no_chase='10' if expected=='ENGINE_ONLY_NO_CHASE' else '11')
    policy=engine_only_geometry(plan,entry_pattern_binding=None,
        plan_rows=[dict(plan_id=plan.plan_id,payload_json=json.dumps(authored))])
    result=admit_trader_signal(**(args|dict(plan=plan,all_plans={plan.plan_id:plan},engine_only_policy=policy)))
    if expected=='REDUCED':
        assert result['dispatched'] and result['judgment']['shares']==10000
        assert result['judgment']['quantity_audit']['engine_only_t1c']['normal_additional']==20000
    else:
        assert not result['dispatched'] and expected in result['judgment']['reason_codes']


def test_half_position_owns_existing_lots_and_never_rounds_up():
    assert half_position_quantity(800,200)[0]==300
    assert half_position_quantity(100,0)[0]==0
    assert half_position_quantity(100,200)[0]==0


def test_three_candidates_record_truncated_and_queue_notification(tmp_path,monkeypatch):
    from liangjian_funnel.runtime import team_v2_candidate_review as review
    args,_,_=prepared(tmp_path)
    for n in (2,3):
        pid='p'+str(n);symbol='600628.SH' if n==3 else '000002.SZ'
        args['all_plans'][pid]=replace(args['all_plans']['p1'],plan_id=pid,symbol=symbol)
        args['envelopes'][pid]=replace(args['envelopes']['p1'],plan_id=pid,symbol=symbol)
        args['active_plan_ids'].add(pid);args['tech_reference'][pid]={}
        args['input_projection']['evaluations'][pid]={'action':'BUY_SIGNAL'}
    monkeypatch.setattr(review,'run_trader_session',lambda **kw:dict(model_validated=False,
        explicit_original_decision=True,reason_codes=['TRADER_ENVELOPE_BINDING_MISMATCH']))
    result=review.run_candidate_review(**args)
    assert result['candidate_review']['truncated'][0]['symbol']=='600628.SH'
    assert result['candidate_review']['truncated'][0]['status']=='TRUNCATED'
    with closing(args['store']._connect()) as db:
        notice=db.execute("select body_json from team_notifications where kind='A4_REVIEW_TRUNCATED'").fetchall()
    assert len(notice)==1 and '600628.SH' in notice[0][0] and 'TRUNCATED' in notice[0][0]
    assert review.run_candidate_review(**args)==result


def test_engine_only_rejection_also_queues_labelled_notice(tmp_path):
    from liangjian_funnel.runtime.team_v2_candidate_review import run_candidate_review
    args,_,_=prepared(tmp_path);latch=ReviewDayLatch(args['store'])
    for n in range(3):
        latch.record(account_id=args['account_id'],trade_date=args['trade_date'],decision_id='failed'+str(n),
            valid=False,observed_at=args['decision_at'])
    args['pm_meeting_id']=None
    result=run_candidate_review(**args)
    assert not result['admissions'][0]['dispatched']
    with closing(args['store']._connect()) as db:
        notice=db.execute("select body_json from team_notifications where kind='A4_ENGINE_ONLY_ADMISSION'").fetchall()
    assert len(notice)==1 and '降级模式·仅引擎' in notice[0][0]
