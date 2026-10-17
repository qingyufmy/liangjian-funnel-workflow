from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json

import pytest

from liangjian_funnel.runtime import team_v2_candidate_review as review
from liangjian_funnel.pipeline.team_v2_trader_schema import validate_trader_output
from test_team_v2_trader_schema import output,CAT,CONTEXT
from test_team_v2_trader_session import fixture


def prepared(tmp_path):
    args,seen,raw=fixture(tmp_path)
    args['tech_reference']=deepcopy(args['tech_reference'])
    args.update(deadline_monotonic=1000,monotonic=lambda:1,
        input_projection=dict(evaluations={'p1':{'action':'BUY_SIGNAL'}},
            plan_rows=[dict(plan_id='p1',payload_json=json.dumps({'plan_priority_rank':1}))]))
    return args,seen,raw


@pytest.mark.parametrize('action',['NO_ACTION','START_CONFIRMATION','DATA_BLOCK','PLAN_INVALIDATED'])
def test_non_buy_events_never_trigger_the_new_review_or_old_heartbeat(tmp_path,action):
    args,seen,_=prepared(tmp_path)
    args['input_projection']['evaluations']['p1']['action']=action
    result=review.run_candidate_review(**args)
    assert result['model_validated'] is None and result['candidate_review']['candidate_count']==0
    assert not seen and not args['broker'].get_orders(args['trade_date'])


@pytest.mark.parametrize('change',['envelope','price','expired'])
def test_engine_candidate_without_entry_permission_is_not_sent(tmp_path,change):
    args,seen,_=prepared(tmp_path)
    if change=='envelope':args['envelopes']['p1']=replace(args['envelopes']['p1'],eligible=False)
    elif change=='price':args['all_plans']['p1']=replace(args['all_plans']['p1'],price=100)
    else:args['all_plans']['p1']=replace(args['all_plans']['p1'],expires_at=args['decision_at']-timedelta(seconds=1))
    assert review.run_candidate_review(**args)['candidate_review']['candidate_count']==0 and not seen


def test_two_objects_max_one_call_no_capacity_or_carried_wait_old_mode(tmp_path,monkeypatch):
    args,_,_=prepared(tmp_path)
    for n in (2,3):
        pid='p'+str(n)
        args['all_plans'][pid]=replace(args['all_plans']['p1'],plan_id=pid,symbol=f'{n:06}.SZ')
        args['envelopes'][pid]=replace(args['envelopes']['p1'],plan_id=pid,symbol=f'{n:06}.SZ')
        args['active_plan_ids'].add(pid);args['tech_reference'][pid]={}
        args['input_projection']['evaluations'][pid]={'action':'BUY_SIGNAL'}
    seen=[]
    def capture(**child):
        seen.append(child)
        assert child['review_candidate_actions']=={'p1':'BUY_SIGNAL','p2':'BUY_SIGNAL'}
        return dict(model_validated=False,reason_codes=['TEAM_MODEL_CALL_FAILED'],source_call_id=child['call_id'])
    monkeypatch.setattr(review,'run_trader_session',capture)
    result=review.run_candidate_review(**args)
    assert len(seen)==1 and result['candidate_review']['selected_count']==2
    assert result['candidate_review']['unreviewed_candidate_ids']==['p3']
    assert not result['admissions'] and not args['broker'].get_orders(args['trade_date'])
    assert review.run_candidate_review(**args)==result and len(seen)==1


@pytest.mark.parametrize('candidate,returned',[('BUY_SIGNAL','ADD_SIGNAL'),('ADD_SIGNAL','BUY_SIGNAL'),('BUY_SIGNAL','SELL_SIGNAL')])
def test_model_cannot_upgrade_or_change_candidate_action(candidate,returned):
    context=CONTEXT|dict(held_symbols=set(),candidate_actions={'p1':candidate},position_candidate_actions={})
    value=validate_trader_output(output(returned),CAT,**context)
    assert not value.valid and 'TRADER_CANDIDATE_ACTION_ESCALATION' in value.issues
    assert validate_trader_output(output('NO_ACTION'),CAT,**context).valid


def test_exit_failure_uses_engine_action_and_valid_hold_veto_does_not(tmp_path,monkeypatch):
    args,_,_=prepared(tmp_path)
    args['active_plan_ids']=set();args['held_plan_ids']={'000001.SZ':'p1'}
    args['position_snapshot']=[dict(plan_id='p1',symbol='000001.SZ',qty=100)]
    args['input_projection']['evaluations']['p1']['action']='REDUCE_SIGNAL'
    monkeypatch.setattr(review,'run_trader_session',lambda **child:dict(model_validated=False,
        reason_codes=['TEAM_MODEL_CALL_FAILED'],source_call_id=child['call_id']))
    calls=[]
    monkeypatch.setattr(review,'admit_trader_signal',lambda **kwargs:calls.append(kwargs) or dict(dispatched=False,judgment={'status':'REJECTED'}))
    result=review.run_candidate_review(**args)
    assert [c['action'] for c in calls]==['REDUCE_SIGNAL']
    assert result['admissions'][0]['origin']=='ENGINE_EXIT_FALLBACK'
    args['decision_id']='next';args['call_id']='A4:next'
    monkeypatch.setattr(review,'run_trader_session',lambda **child:dict(model_validated=True,reason_codes=[],
        source_call_id=child['call_id'],captured_output={'signals':[],'position_actions':[{'symbol':'000001.SZ','action':'HOLD'}]}))
    result=review.run_candidate_review(**args)
    assert len(calls)==1 and not result['admissions'] and result['candidate_review']['veto_count']==1


def test_timeout_classification_is_durable_and_never_retains_private_error_text(tmp_path,monkeypatch):
    from liangjian_funnel.pipeline.model_client import ModelClientError
    args,_,_=prepared(tmp_path)
    class FailedClient:
        def complete(self,*a,**kw):raise ModelClientError('WALL_CLOCK_TIMEOUT',diagnostics={'private':'NEVER_RETAIN_THIS'})
    args['client']=FailedClient()
    def capture(**child):
        child['journal'].claim(child['call_id'],'test-binding')
        try:child['client'].complete()
        except ModelClientError:child['journal'].mark_failed(child['call_id'],'TEAM_MODEL_CALL_FAILED')
        return dict(model_validated=False,reason_codes=['TEAM_MODEL_CALL_FAILED'],source_call_id=child['call_id'])
    monkeypatch.setattr(review,'run_trader_session',capture)
    result=review.run_candidate_review(**args)
    assert result['candidate_review']['timeout_count']==1 and not result['admissions']
    assert result['candidate_review']['transport_failure_classification']=='WALL_CLOCK_TIMEOUT'
    assert 'NEVER_RETAIN_THIS' not in json.dumps(result)
    assert review.run_candidate_review(**args)==result


def test_add_review_keeps_owned_position_context_without_granting_exit_vote(tmp_path,monkeypatch):
    args,_,_=prepared(tmp_path)
    args['input_projection']['evaluations']['p1']['action']='ADD_SIGNAL'
    args['held_plan_ids']={'000001.SZ':'p1'}
    position=dict(plan_id='p1',symbol='000001.SZ',qty=100,available_qty=0,average_price='10')
    args['position_snapshot']=[position]
    calls=[]
    def capture(**child):
        calls.append(child)
        return dict(model_validated=False,reason_codes=['TEAM_MODEL_CALL_FAILED'],source_call_id=child['call_id'])
    monkeypatch.setattr(review,'run_trader_session',capture)
    assert review.run_candidate_review(**args)['candidate_review']['selected_count']==1
    assert calls[0]['review_entry_positions']==[position]
    assert calls[0]['held_plan_ids']=={} and calls[0]['position_snapshot']==[]
    assert calls[0]['review_candidate_actions']=={'p1':'ADD_SIGNAL'}
    assert calls[0]['review_position_actions']=={}
