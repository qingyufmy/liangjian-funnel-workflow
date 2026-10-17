from copy import deepcopy
from dataclasses import replace
import json
import threading
import time

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.runtime.team_v2_trader_batch import run_trader_batch
from liangjian_funnel.runtime.team_v2_trader_session import run_trader_session
from liangjian_funnel.settings import Settings
from test_team_v2_trader_session import fixture
from test_team_v2_execution import pm_result


def batch(tmp_path,*,count=9,fail_first=False,slow_clock=None,wave_clock=None):
    args,_,template=fixture(tmp_path);original=next(iter(args['all_plans'].values()))
    plans={f'p{i:02d}':replace(original,plan_id=f'p{i:02d}',symbol=f'{i+1:06d}.SZ') for i in range(count)}
    args.update(all_plans=plans,active_plan_ids=set(plans),
        envelopes={pid:replace(args['envelopes']['p1'],plan_id=pid,symbol=p.symbol) for pid,p in plans.items()},
        tech_reference={pid:deepcopy(args['tech_reference']['p1']) for pid in plans},
        pm_meeting_id='pm-batch',deadline_monotonic=47,monotonic=lambda:0,utilization_cap_pct='5')
    pm=pm_result(plans);pm['source_call_id']='pm-batch'
    pm['slot_plan']=[r for r in pm['slot_plan'] if r['primary_plan_id'] in {'p00','p08'}]
    for i,row in enumerate(pm['slot_plan'],1):row['slot']=i
    args['store'].record_pm(meeting_id='pm-batch',source_call_id='pm-batch',account_id='paper',trade_date=args['trade_date'],
        input_sha256='c'*64,result=pm)
    rows=[];contexts={}
    for pid,p in plans.items():
        raw=dict(symbol=p.symbol,source_run_id='original',cluster_label='bank',strategy_profile='TREND_MA5')
        rows.append(dict(plan_id=pid,symbol=p.symbol,lane_id='lane_1',status='ACTIVE_TODAY',expires_at=p.expires_at.isoformat(),payload_json=json.dumps(raw)))
        contexts[pid]={'closed_bars':{},'original_tradability_contract':{'status':'KNOWN'}}
    args['input_projection']=dict(plan_rows=rows,contexts=contexts,model_contexts=contexts,
        evaluations={pid:{'action':'NO_ACTION'} for pid in plans})
    args['replacements']['EVIDENCE_CATALOG']=dict(categories=['MARKET','STOCK_TECH'],items=[dict(id='M1',category='MARKET'),
        *(dict(id='TECH:'+pid,category='STOCK_TECH',value=ctx) for pid,ctx in contexts.items())])
    calls=[];inflight=[0,0];lock=threading.Lock();wave=threading.Event()
    def handler(request):
        body=json.loads(request.read());runtime=json.loads(body['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        values={**runtime['inputs'],'ACTIVE_PLANS':json.JSONDecoder().raw_decode(body['messages'][0]['content'].split('ACTIVE_PLANS=',1)[1])[0]}
        ids=[r['plan_id'] for r in values['ACTIVE_PLANS']]
        with lock:
            calls.append(ids);inflight[0]+=1;inflight[1]=max(inflight)
            if len(calls)>=4:wave.set()
        try:
            assert args['broker'].get_orders(args['trade_date'])==()
            if count>32:assert wave.wait(10)
            if wave_clock is not None:wave_clock[0]=20
            time.sleep(.01)
            if slow_clock is not None:slow_clock[0]=48
            if fail_first and 'p00' in ids:raise httpx.ConnectError('UNIT_TEST_ONLY',request=request)
            raw=deepcopy(template);raw.update(envelope=runtime['required_envelope'],decision_id=values['DECISION_ID'],signals=[])
            for row in values['ACTIVE_PLANS']:
                signal=deepcopy(template['signals'][0]);signal.update(plan_id=row['plan_id'],symbol=row['symbol'],
                    action='BUY_SIGNAL' if row['plan_id'] in {'p00','p08'} else 'NO_ACTION',opposing_refs=['TECH:'+row['plan_id']])
                raw['signals'].append(signal)
            return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
        finally:
            with lock:inflight[0]-=1
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    args['client']=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1)
    return args,calls,inflight


def test_parallel_capture_never_orders_then_global_utilization_serially_blocks_second_buy(tmp_path):
    args,calls,_=batch(tmp_path);result=run_trader_batch(**args)
    assert result['model_validated'] and sorted(map(len,calls))==[1,8]
    assert len(args['broker'].get_orders(args['trade_date']))==1
    second=next(r for r in result['admissions'] if r['binding']['plan_id']=='p08')
    assert not second['dispatched'] and 'A6_UTILIZATION_CAP' in second['judgment']['reason_codes']
    assert second['source_call_id'].endswith('S001')
    assert run_trader_batch(**args)==result and len(calls)==2


def test_failed_shard_does_not_suppress_other_valid_shard_or_get_recalled(tmp_path):
    args,calls,_=batch(tmp_path,fail_first=True);result=run_trader_batch(**args)
    assert result['status']=='TRADER_PARTIAL' and len(calls)==2
    orders=args['broker'].get_orders(args['trade_date'])
    assert len(orders)==1 and orders[0]['plan_id']=='p08'
    assert run_trader_batch(**args)==result and len(calls)==2


def test_more_than_four_shards_queue_with_max_four_concurrent_capture_and_no_domain_loss(tmp_path):
    args,calls,inflight=batch(tmp_path,count=43);result=run_trader_batch(**args)
    assert result['model_validated'] and len(calls)==6 and 1<inflight[1]<=4
    assert set(pid for row in calls for pid in row)==set(args['all_plans'])


def test_expired_global_deadline_never_admits_a_valid_capture(tmp_path):
    clock=[0];args,calls,_=batch(tmp_path,slow_clock=clock);args['monotonic']=lambda:clock[0]
    result=run_trader_batch(**args)
    assert not result['admissions'] and not args['broker'].get_orders(args['trade_date'])
    assert 'TRADER_GLOBAL_DEADLINE_EXCEEDED' in result['reason_codes']


def test_capture_only_path_cannot_place_orders_even_for_valid_buy(tmp_path):
    args,seen,_=fixture(tmp_path)
    result=run_trader_session(**args,capture_only=True)
    assert result['model_validated'] and result['capture_only'] and len(seen)==1
    assert not result['admissions'] and not args['broker'].get_orders(args['trade_date'])


def test_queued_shard_transport_budgets_survive_independent_second_pass_without_http(tmp_path):
    from liangjian_funnel.evaluation.team_v2_replay_passes import run_two_passes
    from liangjian_paper_broker.matching import PaperRules
    from test_team_v2_paper_ledger import drive,bar,stamp
    seen=[];outcomes=[]
    def execute(work,journal,override):
        elapsed=[0];args,calls,_=batch(work,count=43,wave_clock=elapsed)
        args.update(journal=journal,monotonic=lambda:elapsed[0])
        if override is not None:args['client']=override
        outcomes.append(run_trader_batch(**args));seen.extend(calls)
        drive(args['broker'],bar(volume_shares=300000),PaperRules(args['trade_date']))
        drive(args['broker'],bar(tm='15:00:00'),PaperRules(args['trade_date']))
        args['broker'].clock=lambda:stamp(tm='15:30:00')
        args['broker'].settle(args['trade_date'])
    result=run_two_passes(tmp_path/'budget-replay',allowed_root=tmp_path,run_pass=execute)
    assert len(seen)==6 and all(r['model_validated'] for r in outcomes)
    assert result['ledger_bytes_equal'] and result['curve_bytes_equal'] and result['has_funded_fill_path']
