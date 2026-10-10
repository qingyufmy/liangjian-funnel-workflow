"""Closed local daily inputs, additive publisher-only fixtures."""
from copy import deepcopy
from datetime import date, datetime, timedelta
import hashlib
import importlib
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from liangjian_funnel.pipeline.factors import FactorEngine
from liangjian_funnel.pipeline.local_fact_cache import canonical_json_hash
from liangjian_funnel.pipeline.research import LaneResult, ResearchRunResult
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.workflow import WorkflowApplication, _compact_factor, _plan_payload, _a4_prompt_plan

TZ=ZoneInfo('Asia/Shanghai')
AT=datetime(2026,10,9,15,10,tzinfo=TZ)
TARGET=date(2026,10,12)
SYMBOL='600000.SH'


def module(): return importlib.import_module('liangjian_funnel.runtime.shadow_inputs')


def fixture():
    calendar=ExchangeTradingCalendar()
    day=TARGET
    days=[]
    for _ in range(20):
        day=calendar.previous_trading_day(day)
        days.append(day)
    rows=[]
    for index,day in enumerate(reversed(days)):
        end=datetime.combine(day,datetime.min.time(),TZ).replace(hour=15)
        close=10+index*0.1
        payload={'thscode':SYMBOL,'date_ms':int(end.timestamp()*1000),'open':close-0.1,
                 'high':close+0.4,'low':close-0.3,'close':close,'volume':1000,'amount':10000,
                 'adjust':'none','closed':True}
        rows.append({'symbol':SYMBOL,'timestamp':end.isoformat(),'adjust':'none',
                     'fetched_at':AT.isoformat(),'content_hash':canonical_json_hash(payload),'payload':payload})
    factor=FactorEngine(SYMBOL).compute(daily_bars=[r['payload'] for r in rows],as_of=AT)
    production={'symbol':SYMBOL,'strategy_profile':'TREND_MA5','strategy_facts':{
        'daily_ma':{'ma5':factor.timeframes['daily'].moving_averages['ma5']}}}
    return rows,_compact_factor(factor.model_dump(mode='json')),production,days[:4][::-1]


def build(rows=None,plan=None,**overrides):
    original,factor,production,dates=fixture()
    arguments=dict(symbol=SYMBOL,rows=original if rows is None else rows,source_as_of=AT,
        bar_cutoff=datetime.fromisoformat(factor['timeframes']['daily']['latest']['end']),
        target_trade_date=TARGET,production_plan=production if plan is None else plan,
        expected_close_dates=dates)
    arguments.update(overrides)
    return module().build_shadow_inputs(**arguments)


def test_closed_raw_ma5_wilder_atr_and_latest_four_include_t_without_input_mutation():
    rows,factor,plan,dates=fixture()
    before=deepcopy((rows,plan))
    result=build(rows,plan)
    assert result['schema_version']=='a4-shadow-inputs/1' and result['status']=='AVAILABLE'
    assert result['daily_ma5']==pytest.approx(sum(r['payload']['close'] for r in rows[-5:])/5)
    tr=[max(r['payload']['high']-r['payload']['low'],abs(r['payload']['high']-rows[i-1]['payload']['close']),
            abs(r['payload']['low']-rows[i-1]['payload']['close'])) for i,r in enumerate(rows) if i]
    expected=sum(tr[:14])/14
    for value in tr[14:]: expected=(expected*13+value)/14
    assert result['atr14']==pytest.approx(expected)
    assert result['previous_daily_closes']==[r['payload']['close'] for r in rows[-4:]]
    assert result['previous_daily_close_dates']==[d.isoformat() for d in dates]
    assert result['previous_daily_close_dates'][-1]=='2026-10-09'
    assert result['adjust']=='none' and result['atr_method']=='WILDER_TR14_SEED_MEAN'
    assert len(result['atr_source_hash'])==64
    assert (rows,plan)==before


@pytest.mark.parametrize('case',['future','wrong_symbol','duplicate','unclosed','short','bad_geometry',
                               'late_fetched','wrong_hash','wrong_adjust','payload_wrong_symbol','missing_day'])
def test_invalid_daily_history_is_data_limited_never_changes_production_plan(case):
    rows,_,plan,_=fixture()
    if case=='future': rows[-1]['payload']['date_ms']=int((AT+timedelta(days=1)).timestamp()*1000)
    elif case=='wrong_symbol': rows[-1]['symbol']='000001.SZ'
    elif case=='duplicate': rows.append(deepcopy(rows[-1]))
    elif case=='unclosed': rows[-1]['payload']['closed']=False
    elif case=='short': rows=rows[-14:]
    elif case=='bad_geometry': rows[-1]['payload']['low']=1000
    elif case=='late_fetched': rows[-1]['fetched_at']=(AT+timedelta(seconds=1)).isoformat()
    elif case=='wrong_hash': rows[-1]['content_hash']='0'*64
    elif case=='wrong_adjust': rows[-1]['adjust']='qfq'
    elif case=='payload_wrong_symbol': rows[-1]['payload']['thscode']='000001.SZ'
    elif case=='missing_day': rows.pop(-2)
    if case in ('future','unclosed','bad_geometry','payload_wrong_symbol'):
        rows[-1]['content_hash']=canonical_json_hash(rows[-1]['payload'])
    before=deepcopy(plan)
    result=build(rows,plan)
    assert result['status']=='DATA_LIMITED'
    assert result['daily_ma5'] is None and result['atr14'] is None
    assert result['previous_daily_closes'] is None
    assert plan==before


@pytest.mark.parametrize('conflict',['ma5','atr14'])
def test_original_frozen_indicator_conflict_is_not_rewritten(conflict):
    _,_,plan,_=fixture()
    if conflict=='ma5': plan['strategy_facts']['daily_ma']['ma5']=99
    else: plan['strategy_facts']['atr14']=99
    result=build(plan=plan)
    assert result['status']=='DATA_LIMITED' and plan['strategy_facts'].get('shadow_inputs') is None
    assert any('CONFLICT' in code for code in result['gap_codes'])


def publisher_fixture():
    rows,factor,plan,dates=fixture()
    raw={**plan,'plan_id':'fixture-plan','risk_unit':'STANDARD','eligibility':'QUALIFIED',
         'review_status':'PASS','trigger_zone':{'low':10,'high':11},'invalidation_level':9}
    result=ResearchRunResult(run_id='shadow-publication-fixture',generated_at=AT,snapshot_id='fixture',
        snapshot_hash='a'*64,status='READY',lanes=(LaneResult('lane_1','fixture','READY',(),{
            'core_watch_pool':[raw],'secondary_watch_pool':[]}),),audit_paths=(),markdown_path=None)
    class Store:
        def __init__(self): self.batch=[]
        def list_execution_plans(self,**kwargs): return []
        def publish_plan_batch(self,batch,**kwargs): self.batch=deepcopy(batch); return batch
        def mark_workflow_runs_published(self,*args): pass
    class Cache:
        def __init__(self): self.calls=[]
        def query_daily_bars(self,symbol,**kwargs): self.calls.append((symbol,kwargs)); return deepcopy(rows[::-1])
    store,cache=Store(),Cache()
    data={'TRADABILITY_FLAGS':{SYMBOL:{'tradable':True}},'FACTOR_SNAPSHOT':{SYMBOL:factor},
          'snapshot_manifest':{'as_of':AT.isoformat()}}
    return SimpleNamespace(store=store,fact_cache=cache,trading_calendar=ExchangeTradingCalendar()),result,data,raw


def test_actual_publisher_adds_only_shadow_field_after_qualification_no_prompt_mutation():
    app,result,data,raw=publisher_fixture()
    before=deepcopy((data,raw))
    summary=WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    assert len(summary['created'])==1
    payload=app.store.batch[0]['payload']
    assert payload['strategy_facts']['shadow_inputs']['status']=='AVAILABLE'
    without=deepcopy(payload)
    without['strategy_facts'].pop('shadow_inputs')
    assert without=={**_plan_payload(raw),'source_run_id':result.run_id,'trend_entry_rule_version':'trend-ma5/2'}
    assert (data,raw)==before
    assert app.fact_cache.calls[0][1]['adjust']=='none'
    assert app.fact_cache.calls[0][1]['as_of']==AT
    assert app.fact_cache.calls[0][1]['end']==datetime(2026,10,9,15,0,tzinfo=TZ)+timedelta(microseconds=1)


@pytest.mark.parametrize('failure',['no_cache','cache_error','missing_factor','ma5_conflict'])
def test_old_publication_fixture_or_shadow_failure_cannot_block_plan(failure):
    app,result,data,raw=publisher_fixture()
    if failure=='no_cache': del app.fact_cache
    elif failure=='cache_error': app.fact_cache.query_daily_bars=lambda *a,**kw:(_ for _ in ()).throw(RuntimeError('source error'))
    elif failure=='missing_factor': data.pop('FACTOR_SNAPSHOT')
    else: raw['strategy_facts']['daily_ma']['ma5']=99
    summary=WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    assert len(summary['created'])==1 and summary['blocked']==[]
    assert app.store.batch[0]['payload']['strategy_facts']['shadow_inputs']['status']=='DATA_LIMITED'


def test_rejected_plan_does_not_query_daily_source():
    app,result,data,raw=publisher_fixture()
    raw['eligibility']='BLOCKED'
    summary=WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    assert summary['created']==[] and app.fact_cache.calls==[]


@pytest.mark.parametrize('case',['naive_source','naive_cutoff','early_source','target_today',
                                'wrong_expected','missing_identity','nan_geometry'])
def test_time_calendar_and_numeric_proof_cannot_be_manufactured(case):
    rows,_,plan,dates=fixture()
    overrides={}
    if case=='naive_source': overrides['source_as_of']=AT.replace(tzinfo=None)
    elif case=='naive_cutoff': overrides['bar_cutoff']=AT.replace(tzinfo=None)
    elif case=='early_source': overrides['source_as_of']=AT.replace(hour=14)
    elif case=='target_today': overrides['target_trade_date']=AT.date()
    elif case=='wrong_expected': overrides['expected_close_dates']=dates[:-1]+[TARGET]
    elif case=='missing_identity': rows[-1]['payload'].pop('thscode')
    else: rows[-1]['payload']['high']=float('nan')
    assert build(rows,plan,**overrides)['status']=='DATA_LIMITED'


def test_source_hash_binds_original_rows_cutoff_and_algorithm_not_only_closes():
    rows,_,plan,_=fixture()
    original=build(rows,plan)
    rows[0]['payload']['volume']+=1
    rows[0]['content_hash']=canonical_json_hash(rows[0]['payload'])
    changed=build(rows,plan)
    assert changed['status']=='AVAILABLE' and changed['atr14']==original['atr14']
    assert changed['atr_source_hash']!=original['atr_source_hash']
    assert changed['source_ref']['source_rows_hash']!=original['source_ref']['source_rows_hash']
    assert changed['source_ref']['algorithm']==changed['version']
    assert changed['source_ref']['seed_start']==rows[0]['timestamp']
    json.dumps(changed,allow_nan=False)


@pytest.mark.parametrize('case',['factor_future','manifest_older','cutoff_future'])
def test_publisher_frozen_time_conflict_does_not_query_or_change_plan_set(case):
    app,result,data,_=publisher_fixture()
    factor=data['FACTOR_SNAPSHOT'][SYMBOL]
    if case=='factor_future': factor['as_of']=(AT+timedelta(seconds=1)).isoformat()
    elif case=='manifest_older': data['snapshot_manifest']['as_of']=(AT-timedelta(seconds=1)).isoformat()
    else: factor['timeframes']['daily']['latest']['end']=(AT+timedelta(seconds=1)).isoformat()
    summary=WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    assert len(summary['created'])==1 and not app.fact_cache.calls
    assert app.store.batch[0]['payload']['strategy_facts']['shadow_inputs']['status']=='DATA_LIMITED'


def test_a4_prompt_projection_is_unchanged_by_published_attachment():
    app,result,data,raw=publisher_fixture()
    old={**_plan_payload(raw),'source_run_id':result.run_id,'trend_entry_rule_version':'trend-ma5/2'}
    WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    new=app.store.batch[0]['payload']
    assert _a4_prompt_plan({'payload_json':json.dumps(new)})==_a4_prompt_plan({'payload_json':json.dumps(old)})


def test_malformed_legacy_strategy_facts_is_not_replaced_by_shadow_attachment():
    app,result,data,raw=publisher_fixture()
    raw['strategy_facts']='legacy malformed value'
    summary=WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    assert len(summary['created'])==1
    assert app.store.batch[0]['payload']['strategy_facts']=='legacy malformed value'
    assert app.fact_cache.calls==[]


def test_non_mapping_raw_row_fails_closed_without_exception():
    rows,_,plan,_=fixture()
    rows[-1]=None
    assert build(rows,plan)['status']=='DATA_LIMITED'


def test_target_day_morning_as_of_is_preserved_with_prior_closed_bar_cutoff():
    rows,_,plan,_=fixture()
    morning=datetime.combine(TARGET,datetime.min.time(),TZ).replace(hour=7)
    result=build(rows,plan,source_as_of=morning)
    assert result['status']=='AVAILABLE' and result['daily_as_of']==morning.isoformat()
    assert result['daily_as_of_semantics']=='SOURCE_OBSERVATION_AS_OF'
    assert result['last_closed_daily_bar_end']=='2026-10-09T15:00:00+08:00'


def test_factor_latest_identity_conflict_is_not_queried():
    app,result,data,_=publisher_fixture()
    data['FACTOR_SNAPSHOT'][SYMBOL]['timeframes']['daily']['latest']['symbol']='000001.SZ'
    WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    assert not app.fact_cache.calls
    assert app.store.batch[0]['payload']['strategy_facts']['shadow_inputs']['status']=='DATA_LIMITED'
