from datetime import timedelta
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path

import pytest

from test_team_v2_trader_session import fixture
from test_team_v2_paper_ledger import stamp,bar
from liangjian_funnel.runtime.team_v2_trader_session import run_trader_session
from liangjian_funnel.runtime.team_v2_control_minute import run_control_minute
from liangjian_funnel.runtime.broker.local_paper import LocalPaperAdapter
from liangjian_paper_broker.matching import PaperRules


def test_legal_model_result_after_global_deadline_cannot_dispatch(tmp_path):
    args,seen,raw=fixture(tmp_path)
    result=run_trader_session(**args,deadline_monotonic=10,monotonic=lambda:11)
    assert not result['admissions'] and not args['broker'].get_orders('2026-10-19')


def prepared(tmp_path):
    args,seen,raw=fixture(tmp_path);clock=[stamp(tm='10:01:00')]
    args['clock']=lambda:clock[0]
    root=Path(__file__).resolve().parents[1]
    args['broker']=LocalPaperAdapter(args['broker'].path,paper_config=root/'config/paper_broker.yaml',
        risk_config=root/'config/risk_limits.yaml',clock=args['clock'],calendar=args['calendar'],run_id='control-local')
    args.update(decision_at=stamp(tm='10:01:00'),closed_5m_end=stamp(),mark_as_of=stamp())
    return args,seen,raw,clock


def run(args,closed,**kw):
    return run_control_minute(session_kwargs=args,closed_bars={closed.symbol:closed},deadline_monotonic=100,
        monotonic=lambda:0,**kw)


def test_closed_minute_then_single_five_minute_client_and_real_next_bar_fill(tmp_path):
    args,seen,raw,clock=prepared(tmp_path);before=deepcopy(args['all_plans'])
    first=run(args,bar(tm='10:00:00'))
    assert first['trader']['model_validated'] and first['trader']['admissions'][0]['dispatched']
    assert args['broker'].get_fills('2026-10-19')==()
    # Restart of the same frozen minute does not call again or duplicate cash reservation.
    assert run(args,bar(tm='10:00:00'))['trader']==first['trader'] and len(seen)==1
    clock[0]=stamp(tm='10:02:00');second=run(args,bar(tm='10:01:00',volume_shares=300000))
    assert second['trader'] is None and not second['fills']  # no backdating the 10:01 intent
    clock[0]=stamp(tm='10:03:00');third=run(args,bar(tm='10:02:00',volume_shares=300000))
    assert third['fills'][0]['qty']==15000 and third['trader'] is None and len(seen)==1
    assert args['broker'].reconcile()['status']=='MATCH' and args['all_plans']==before


@pytest.mark.parametrize('kind',['unknown','halted','limitup'])
def test_bar_trade_flags_cannot_be_overruled_by_legal_buy_json(tmp_path,kind):
    args,seen,raw,clock=prepared(tmp_path)
    changes={'tradability_known':False} if kind=='unknown' else {'halted':True} if kind=='halted' else {'limit_up':True}
    result=run(args,bar(tm='10:00:00',**changes))
    if kind=='unknown':
        assert result['status']=='DATA_LIMITED' and result['trader'] is None and not seen
    else:
        assert result['trader']['model_validated'] and not result['trader']['admissions'][0]['dispatched']
    assert not args['broker'].get_orders('2026-10-19')


def test_budget_exhausted_does_not_call_model_but_matches_existing_intent(tmp_path):
    args,seen,raw,clock=prepared(tmp_path);broker=args['broker']
    broker.ledger.clock=lambda:stamp(tm='09:59:00')
    broker.ledger.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='preexisting',
        plan_id='p1',signal_id='old',no_chase_price='10.2',tradable=True)
    broker.ledger.clock=args['clock']
    result=run_control_minute(session_kwargs=args,closed_bars={'000001.SZ':bar(tm='10:00:00')},
        deadline_monotonic=1,monotonic=lambda:2)
    assert result['fills'][0]['qty']==1000 and not seen
    assert result['trader_skip_reason']=='CONTROL_GLOBAL_BUDGET_INSUFFICIENT'


def test_hard_stop_is_one_minute_atomic_and_does_not_wait_for_model_or_PM(tmp_path):
    args,seen,raw,clock=prepared(tmp_path);broker=args['broker'];args['pm_meeting_id']=None
    # Explicit isolated inventory from a prior trading day, not a production backfill.
    clock[0]=stamp(day='16',tm='10:00:00')
    broker.ledger.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='held',
        plan_id='p1',signal_id='held',no_chase_price='10.2',tradable=True)
    clock[0]=stamp(day='16',tm='10:02:00')
    broker.ledger.on_bar(bar(day='16'),PaperRules('2026-10-16'),observed_at=clock[0])
    clock[0]=stamp(tm='10:03:00');stop=args['all_plans']['p1'].stop_price
    low=bar(tm='10:02:00',low=stop-D('.1'),high=stop+D('.2'),vwap=stop,close=stop)
    result=run(args,low)
    assert result['hard_exits'][0]['dispatched'] and result['trader'] is None and not seen
    sell=next(o for o in broker.get_orders('2026-10-19') if o['side']=='SELL')
    assert sell['qty']==1000 and not broker.get_fills('2026-10-19')
    assert run(args,low)['hard_exits'][0]['order_id']==result['hard_exits'][0]['order_id']
    assert len(broker.get_orders('2026-10-19'))==1
    assert broker.reconcile()['status']=='MATCH'


def test_missing_or_future_bar_never_models_a_fabricated_bucket(tmp_path):
    args,seen,raw,clock=prepared(tmp_path)
    with pytest.raises(ValueError,match='CURRENT_CLOSED_BAR_UNPROVEN'):
        run(args,bar(tm='10:01:00'))
    result=run_control_minute(session_kwargs=args,closed_bars={},deadline_monotonic=100,monotonic=lambda:0)
    assert result['status']=='DATA_LIMITED' and not seen


@pytest.mark.parametrize('key,value',[('account_id','other-account'),('model','other-model'),
    ('call_id','other-call'),('pm_meeting_id','other-pm')])
def test_frozen_minute_cannot_reuse_another_account_or_model_context(tmp_path,key,value):
    args,seen,raw,clock=prepared(tmp_path)
    run(args,bar(tm='10:00:00'));args[key]=value
    with pytest.raises(ValueError,match='CONTROL_MINUTE_SOURCE_CONFLICT'):
        run(args,bar(tm='10:00:00'))
    assert len(seen)==1 and len(args['broker'].get_orders('2026-10-19'))==1


def test_deadline_rechecked_after_lock_before_order_commit(tmp_path):
    args,seen,raw=fixture(tmp_path)
    ticks=iter((0,0,0,11))
    result=run_trader_session(**args,deadline_monotonic=10,monotonic=lambda:next(ticks))
    assert not result['admissions'][0]['dispatched']
    assert result['admissions'][0]['judgment']['reason_codes']==['TRADER_GLOBAL_DEADLINE_EXCEEDED']


def test_model_finishes_legal_but_late_is_journalled_not_dispatched(tmp_path):
    import httpx
    args,seen,raw=fixture(tmp_path);timevalue=[0]
    original=args['client'].transport
    def handler(request):
        response=original.handle_request(request);timevalue[0]=11;return response
    args['client'].transport=httpx.MockTransport(handler)
    result=run_trader_session(**args,deadline_monotonic=10,monotonic=lambda:timevalue[0])
    assert result['status']=='TRADER_DEADLINE_NO_ACTION' and result['model_validated']
    assert not result['admissions'] and len(seen)==1 and not args['broker'].get_orders('2026-10-19')
    assert len(args['journal'].summary())==1


def test_one_symbol_bar_failure_blocks_only_that_plan_not_the_other(tmp_path,monkeypatch):
    from test_team_v2_execution import pm_result
    args,seen,raw,clock=prepared(tmp_path)
    p2=replace(args['all_plans']['p1'],plan_id='p2',symbol='000002.SZ')
    args['all_plans']={**args['all_plans'],'p2':p2}
    args['envelopes']={**args['envelopes'],'p2':replace(args['envelopes']['p1'],plan_id='p2',symbol=p2.symbol)}
    args.update(active_plan_ids={'p1','p2'},tech_reference={'p1':{'PRESSURE':'UNMET','VWAP':'MET'},
        'p2':{'PRESSURE':'UNMET','VWAP':'MET'}},pm_meeting_id='pm-two')
    row=deepcopy(raw['signals'][0]);row.update(plan_id='p2',symbol=p2.symbol);raw['signals'].append(row)
    pm=pm_result(args['all_plans']);pm['source_call_id']='pm-two'
    args['store'].record_pm(meeting_id='pm-two',source_call_id='pm-two',account_id=args['account_id'],
        trade_date='2026-10-19',input_sha256='c'*64,result=pm)
    original=args['broker'].on_bar
    def isolated(bar):
        if bar.symbol=='000001.SZ':raise RuntimeError('ISOLATED_SOURCE_ERROR')
        return original(bar)
    monkeypatch.setattr(args['broker'],'on_bar',isolated)
    result=run_control_minute(session_kwargs=args,closed_bars={
        '000001.SZ':bar(tm='10:00:00'),'000002.SZ':bar(tm='10:00:00',symbol='000002.SZ')},
        deadline_monotonic=100,monotonic=lambda:0)
    assert result['status']=='DEGRADED' and result['trader']['model_validated']
    admissions=result['trader']['admissions']
    assert admissions[0]['dispatched'] is False and admissions[1]['dispatched'] is True
    assert args['broker'].get_orders('2026-10-19')[0]['symbol']=='000002.SZ'
