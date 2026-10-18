"""Configured thresholds only, post-trader sequencing, no invented near-line."""
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
import json

import httpx
import pytest

from liangjian_funnel.runtime.team_v2_exception_coordinator import exception_triggers,run_exception_after_trader
from liangjian_funnel.runtime.risk_engine import RiskLimits
from test_team_v2_control_minute import prepared,run
from test_team_v2_paper_ledger import stamp,bar


def snapshot():
    return dict(account=dict(nav='1000000'),risk_day=dict(day_open_nav='1000000',halt_reason_codes=[]),
        peak_nav='1000000',global_halt='',positions=[],open_orders=[])


def state(**changes):
    return dict(live_authority=True,regime='TREND_MAINLINE',transitions=[],**changes)


def test_no_near_threshold_or_initial_confirmation_is_invented():
    snap=snapshot();snap['account']['nav']='981000'
    regime=state();regime['transitions']=[dict(kind='primary',previous=None,current='TREND_MAINLINE',tick=stamp().isoformat())]
    result=exception_triggers(snap,plans={},limits=RiskLimits(),regime=regime,account_id='paper',tick=stamp())
    assert result['events']==[] and not result['hard_halt']
    assert 'NEAR_LIMIT_NOT_CONFIGURED' in result['annotations']


@pytest.mark.parametrize('kind,code',[('loss','DAILY_LOSS_HALT'),('drawdown','DRAWDOWN_HALT'),('half','DRAWDOWN_HALF_RISK')])
def test_existing_yaml_hard_and_half_limits_are_exact(kind,code):
    snap=snapshot()
    if kind=='loss':snap['account']['nav']='980000'
    else:
        snap['peak_nav']='1000000';snap['account']['nav']='850000' if kind=='drawdown' else '920000'
        snap['risk_day']['day_open_nav']=snap['account']['nav']
    result=exception_triggers(snap,plans={},limits=RiskLimits(),regime=state(),account_id='paper',tick=stamp())
    assert code in result['events'][0]['reason_codes']
    assert result['hard_halt']==(kind!='half')


def test_only_current_confirmed_real_flip_can_trigger():
    regime=state();regime['transitions']=[dict(kind='primary',previous='ROTATION_NO_MAINLINE',
        current='TREND_MAINLINE',tick=stamp().isoformat())]
    good=exception_triggers(snapshot(),plans={},limits=RiskLimits(),regime=regime,account_id='paper',tick=stamp())
    assert good['events'][0]['reason_codes']==['MARKET_REGIME_CHANGED']
    regime['transitions'][0]['tick']=stamp(tm='09:55:00').isoformat()
    assert not exception_triggers(snapshot(),plans={},limits=RiskLimits(),regime=regime,account_id='paper',tick=stamp())['events']


def test_cluster_cap_counts_unfilled_buys_and_rejects_unknown_owner():
    from test_team_v2_risk_engine import plan
    snap=snapshot();p=plan()
    snap['open_orders']=[dict(side='BUY',plan_id=p.plan_id,symbol=p.symbol,remaining_qty=40000,
        order_type='LIMIT',limit_price='10',no_chase_price='10.2')]
    result=exception_triggers(snap,plans={p.plan_id:p},limits=RiskLimits(),regime=state(),account_id='paper',tick=stamp())
    assert result['events'][0]['target']=='cluster' and result['events'][0]['reason_codes']==['CLUSTER_CAP']
    snap['open_orders'][0]['plan_id']='foreign'
    assert exception_triggers(snap,plans={p.plan_id:p},limits=RiskLimits(),regime=state(),account_id='paper',tick=stamp())['status']=='DATA_LIMITED'


def hook_fixture(tmp_path,*,action='NO_ACTION'):
    args,seen,raw,clock=prepared(tmp_path)
    original=args['client'].transport;risk_calls=[]
    def handler(request):
        req=json.loads(request.read());runtime=json.loads(req['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        if runtime['required_envelope']['stage_id']=='A4':return original.handle_request(request)
        from test_team_v2_risk_exception import response
        body=response(action);body['envelope']=runtime['required_envelope']
        context=json.JSONDecoder().raw_decode(req['messages'][0]['content'].split('EXCEPTION_CONTEXT=',1)[1])[0]
        body['exception_actions']=[dict(body['exception_actions'][0],target=t['target'],target_id=t['target_id'],
            supporting_refs=['RISK:ACCOUNT'],opposing_refs=['RISK:TRIGGERS']) for t in context['targets']]
        body['utilization_cap'].update(cap_pct=65,supporting_refs=['RISK:ACCOUNT'],opposing_refs=['RISK:TRIGGERS'])
        body['capacity_for_tomorrow'].update(plans_acceptable=0,cluster_quota={},
            supporting_refs=['RISK:ACCOUNT'],opposing_refs=['RISK:TRIGGERS'])
        risk_calls.append(req)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(body)}}]},request=request)
    args['client'].transport=httpx.MockTransport(handler)
    live=state();live['transitions']=[dict(kind='primary',previous='ROTATION_NO_MAINLINE',current='TREND_MAINLINE',tick=stamp().isoformat())]
    def callback(**kwargs):return run_exception_after_trader(session_kwargs=args,live_state=live,
        engine_utilization_cap=75,capacity_slots=3,cluster_caps={'bank':2},**kwargs)
    return args,seen,raw,clock,risk_calls,callback


def test_real_control_calls_exception_only_after_trader_and_reuses_both_journals(tmp_path):
    args,seen,raw,clock,risk_calls,callback=hook_fixture(tmp_path)
    first=run(args,bar(tm='10:00:00'),exception_handler=callback)
    assert first['trader']['model_validated'] and first['risk_exception']['review']['status']=='READY'
    assert len(seen)==len(risk_calls)==1 and first['phase_timings']['risk_exception_ms']>=0
    assert run(args,bar(tm='10:00:00'),exception_handler=callback)['risk_exception']==first['risk_exception']
    assert len(seen)==len(risk_calls)==1


def test_no_trigger_does_not_spend_a_model_call(tmp_path):
    args,seen,raw,clock,risk_calls,_=hook_fixture(tmp_path)
    def callback(**kw):return run_exception_after_trader(session_kwargs=args,live_state=state(),
        engine_utilization_cap=75,capacity_slots=3,cluster_caps={'bank':2},**kw)
    result=run(args,bar(tm='10:00:00'),exception_handler=callback)
    assert result['risk_exception']['status']=='NO_TRIGGER' and len(seen)==1 and not risk_calls


def test_exception_hook_failure_does_not_discard_trader_or_minute_protection(tmp_path):
    args,seen,raw,clock=prepared(tmp_path)
    def callback(**kw):raise RuntimeError('PRIVATE_INTERNAL_DETAIL')
    result=run(args,bar(tm='10:00:00'),exception_handler=callback)
    assert result['status']=='DEGRADED' and result['trader']['model_validated']
    assert result['risk_exception']['reason_code']=='A6_EXCEPTION_COORDINATOR_FAILED'
    assert result['risk_exception']['consecutive_failures']==1
    assert 'PRIVATE_INTERNAL_DETAIL' not in json.dumps(result)


@pytest.mark.parametrize('action,expected',[('EXIT',1000),('REDUCE_HALF',500)])
def test_a6_position_proposals_pass_engine_t1_and_reservation_before_next_bar(tmp_path,action,expected):
    args,seen,raw,clock,risk_calls,callback=hook_fixture(tmp_path,action=action)
    broker=args['broker'];raw['signals'][0]['action']='NO_ACTION'
    from liangjian_paper_broker.matching import PaperRules
    clock[0]=stamp(day='16',tm='09:58:00')
    broker.ledger.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='prior',
        plan_id='p1',signal_id='prior',no_chase_price='10.2',tradable=True)
    clock[0]=stamp(day='16',tm='10:00:00')
    broker.ledger.on_bar(bar(day='16',tm='09:59:00'),PaperRules('2026-10-16'),observed_at=clock[0])
    clock[0]=stamp(tm='10:01:00')
    first=run(args,bar(tm='10:00:00'),exception_handler=callback)
    assert first['risk_exception']['review']['status']=='READY'
    admission=first['risk_exception']['admissions'][0]
    assert admission['dispatched'] and admission['judgment']['shares']==expected
    assert not broker.get_fills('2026-10-19')
    clock[0]=stamp(tm='10:01:02')
    assert run(args,bar(tm='10:00:00'),exception_handler=callback)['risk_exception']==first['risk_exception']
    assert len(risk_calls)==1 and len(broker.get_orders('2026-10-19'))==1


def test_a6_exit_cannot_sell_todays_filled_inventory(tmp_path):
    args,seen,raw,clock,risk_calls,callback=hook_fixture(tmp_path,action='EXIT')
    broker=args['broker'];raw['signals'][0]['action']='NO_ACTION'
    clock[0]=stamp(tm='09:58:00')
    broker.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='today',
        plan_id='p1',signal_id='today',no_chase_price='10.2',tradable=True)
    clock[0]=stamp(tm='10:00:00');broker.on_bar(bar(tm='09:59:00'));clock[0]=stamp(tm='10:01:00')
    result=run(args,bar(tm='10:00:00'),exception_handler=callback)
    admission=result['risk_exception']['admissions'][0]
    assert not admission['dispatched'] and admission['judgment']['reason_codes']==['T1_NOT_SELLABLE']


def test_coordinator_commit_crash_after_sell_reservation_does_not_duplicate_order(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.team_v2_exception_coordinator as module
    from liangjian_paper_broker.matching import PaperRules
    args,seen,raw,clock,risk_calls,callback=hook_fixture(tmp_path,action='EXIT')
    raw['signals'][0]['action']='NO_ACTION';broker=args['broker']
    clock[0]=stamp(day='16',tm='09:58:00')
    broker.ledger.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='prior',
        plan_id='p1',signal_id='prior',no_chase_price='10.2',tradable=True)
    clock[0]=stamp(day='16',tm='10:00:00')
    broker.ledger.on_bar(bar(day='16',tm='09:59:00'),PaperRules('2026-10-16'),observed_at=clock[0])
    clock[0]=stamp(tm='10:01:00');real=module._finish
    def fail(*a,**kw):raise RuntimeError('TEST_AFTER_ORDER_COMMIT')
    monkeypatch.setattr(module,'_finish',fail)
    first=run(args,bar(tm='10:00:00'),exception_handler=callback)
    assert first['risk_exception']['reason_code']=='A6_EXCEPTION_COORDINATOR_FAILED'
    assert len(broker.get_orders('2026-10-19'))==1
    clock[0]=stamp(tm='10:01:02');monkeypatch.setattr(module,'_finish',real)
    recovered=run(args,bar(tm='10:00:00'),exception_handler=callback)
    assert recovered['risk_exception']['review']['status']=='READY'
    assert len(broker.get_orders('2026-10-19'))==1 and len(risk_calls)==1


def test_actual_monitor_callback_sees_real_transition_after_trader_and_no_second_call(tmp_path,monkeypatch):
    from test_team_v2_monitor_dispatch import prepared as monitor_fixture
    from test_team_v2_morning import advance,source_bytes
    from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
    import liangjian_funnel.runtime.team_v2_live_regime_source as source_module
    app,clock,trader_calls,api,proof=monitor_fixture(tmp_path,monkeypatch);advance(clock,9,46)
    before=source_bytes(app.settings);original=api.transport;risk_calls=[]
    regime=state();regime['transitions']=[dict(kind='primary',previous='ROTATION_NO_MAINLINE',
        current='TREND_MAINLINE',tick=clock.now.replace(minute=45).isoformat())]
    monkeypatch.setattr(source_module,'record_current_regime',lambda *a,**k:regime)
    def handler(request):
        req=json.loads(request.read());runtime=json.loads(req['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        if runtime['required_envelope']['stage_id']=='A4':return original.handle_request(request)
        from test_team_v2_risk_exception import response
        body=response('HALT_NEW_ENTRY');body['envelope']=runtime['required_envelope']
        context=json.JSONDecoder().raw_decode(req['messages'][0]['content'].split('EXCEPTION_CONTEXT=',1)[1])[0]
        body['exception_actions']=[dict(body['exception_actions'][0],target=t['target'],target_id=t['target_id'],
            supporting_refs=['RISK:ACCOUNT'],opposing_refs=['RISK:TRIGGERS']) for t in context['targets']]
        for key in ('utilization_cap','capacity_for_tomorrow'):
            body[key].update(supporting_refs=['RISK:ACCOUNT'],opposing_refs=['RISK:TRIGGERS'])
        body['utilization_cap']['cap_pct']=0
        body['capacity_for_tomorrow'].update(plans_acceptable=0,cluster_quota={})
        risk_calls.append(req)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(body)}}]},request=request)
    api.transport=httpx.MockTransport(handler)
    first=run_monitor_control(app,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert first['result']['risk_exception']['review']['status']=='READY'
    clock.now=clock.now.replace(second=1)
    second=run_monitor_control(app,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert second['result']['risk_exception']==first['result']['risk_exception']
    assert len(trader_calls)==len(risk_calls)==1 and source_bytes(app.settings)==before
    assert trader_calls[0]['model'] == risk_calls[0]['model'] == 'deepseek-v4-flash-0731'
    assert app.settings.review_model != 'deepseek-v4-flash-0731'
