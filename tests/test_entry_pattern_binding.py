from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timedelta
from decimal import Decimal as D

import pytest

from test_entry_confirmation import b_rows,daily,plan,CFG
from liangjian_funnel.runtime.entry_confirmation import evaluate_pattern
from liangjian_funnel.runtime.entry_pattern_binding import (
    make_b_execution_binding,checked_binding,bind_team_plan,probe_envelope,held_stop_overlays,digest,
)


def source():
    """Synthetic scale-adjusted B proof, never a claimed market replay."""
    raw={**plan(high=100.2),'symbol':'000001.SZ','plan_id':'p1','stop_level':90}
    rows=b_rows();facts=daily()
    for key in ('reference_price','stop_level','first_resistance','no_chase'):
        raw[key]/=10
    raw['trigger_zone']={k:v/10 for k,v in raw['trigger_zone'].items()}
    for row in rows:
        for key in ('open','high','low','close'):row[key]/=10
        row['bar_end']=(datetime.fromisoformat(row['bar_end'])-timedelta(days=1)).isoformat()
    for row in facts['daily_closes']:
        row['close']/=10;row['as_of']=(datetime.fromisoformat(row['as_of'])-timedelta(days=1)).isoformat()
    for key in ('previous_day_high','daily_ma5'):
        facts[key]['value']/=10;facts[key]['as_of']=(datetime.fromisoformat(facts[key]['as_of'])-timedelta(days=1)).isoformat()
    at=datetime.fromisoformat(rows[-1]['bar_end'])
    proof=evaluate_pattern(raw,rows,[],as_of=at,config=CFG,daily_facts=facts,vwap=10)
    assert proof['confirmed'],proof
    return raw,proof,make_b_execution_binding(raw,proof)


def test_b_binding_pins_original_and_condition_proof_without_mutating_plan():
    raw,proof,binding=source();original=deepcopy(raw)
    assert checked_binding(binding)==binding
    assert binding['source_plan_sha256']==digest(raw) and binding['proof_sha256']==digest(proof)
    assert D(binding['stop'])>=D(binding['original_stop']) and binding['risk_unit']=='PROBE'
    assert raw==original


def test_shared_strategy_b_uses_runtime_single_price_and_bound_new_stop():
    from test_entry_confirmation import DAY,server_context
    from liangjian_funnel.runtime.strategies import evaluate_strategy
    rows=b_rows();minutes=[]
    for index,row in enumerate(rows):
        end=DAY+timedelta(minutes=5*(index+1))
        for offset in range(4,-1,-1):
            minutes.append(dict(symbol='000001.SZ',interval='1m',bar_end=end-timedelta(minutes=offset),
                **{k:row[k] for k in ('open','high','low','close')},volume=row['volume']/5,
                amount=row['volume']/5*row['close'],closed=True))
    raw={**plan(high=100.2),'symbol':'000001.SZ','stock_behavior_type':'TREND','trade_date':'2026-10-20'}
    at=minutes[-1]['bar_end'];ctx=server_context(at,entry_confirmation_config=CFG,
        entry_confirmation_daily_facts=daily(),entry_pattern_execution_supported=True,entry_confirmation_plan_id='PUBLISHED:p1')
    result=evaluate_strategy(raw,minutes,now=at,market_context=ctx).model_dump(mode='json')
    assert result['action']=='BUY_SIGNAL',result
    binding=result['entry_pattern_binding']
    assert binding['plan_id']=='PUBLISHED:p1' and binding['source_plan_sha256']==digest(raw)
    assert result['live_stop_level']==float(binding['stop'])>raw['stop_level']
    assert result['live_entry_price']==minutes[-1]['close']>raw['trigger_zone']['high']
    assert result['live_reward_risk']>=raw['minimum_reward_risk']
    # A raw plan cannot self-report adapter capability or original daily facts.
    spoof=evaluate_strategy({**raw,'entry_pattern_execution_supported':True,'entry_confirmation_daily_facts':daily()},
        minutes,now=at,market_context=server_context(at,entry_confirmation_config=CFG)).model_dump(mode='json')
    assert spoof['action']=='DATA_BLOCK' and 'entry_pattern_binding' not in spoof
    # B's new single-price order window may not mask an already-adverse quote.
    adverse=evaluate_strategy(raw,minutes,now=at,market_context={**ctx,
        'realtime_quote':{'available':True,'price':float(binding['stop'])-.01}}).model_dump(mode='json')
    assert adverse['action'] not in {'BUY_SIGNAL','ADD_SIGNAL'} and 'entry_pattern_binding' not in adverse
    assert 'B_CURRENT_QUOTE_OUTSIDE_STOP_OR_NO_CHASE' in adverse['reason_codes']
    overheated=evaluate_strategy({**raw,'volume_overheated':True},minutes,now=at,market_context=ctx).model_dump(mode='json')
    assert overheated['action'] not in {'BUY_SIGNAL','ADD_SIGNAL'} and 'entry_pattern_binding' not in overheated
    no_trade=deepcopy(minutes)
    for row in no_trade[-5:-1]:row.update(volume=40,amount=row['close']*40)
    no_trade[-1].update(volume=0,amount=0)
    quiet=evaluate_strategy(raw,no_trade,now=at,market_context=ctx).model_dump(mode='json')
    assert quiet['action']=='NO_ACTION' and quiet['reason_codes']==['CONFIRM_CURRENT_MINUTE_NO_TRADES']
    assert quiet['entry_confirmation']['current_minute']['status']=='NO_TRADES' and 'entry_pattern_binding' not in quiet


@pytest.mark.parametrize('field,value',[('stop','8'),('entry_price','12'),('risk_unit','STANDARD'),('proof_sha256','x'*64)])
def test_binding_tamper_never_becomes_execution_authority(field,value):
    _,_,binding=source();binding[field]=value
    with pytest.raises(ValueError,match='BINDING_INVALID'):checked_binding(binding)


@pytest.mark.parametrize('fill_vwap',[D('10.01'),D('9.99')])
def test_team_actual_admission_and_restart_use_bound_stop_and_probe(tmp_path,fill_vwap):
    from test_team_v2_execution import fixture,policy
    from test_team_v2_paper_ledger import bar,drive,broker
    from liangjian_paper_broker.matching import PaperRules
    from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal
    raw,proof,binding=source();b,args=fixture(tmp_path)
    original=replace(args['plan'],entry_high=D(str(raw['trigger_zone']['high'])))
    p=bind_team_plan(original,binding);at=datetime.fromisoformat(binding['source_as_of'])
    b.clock=lambda:at
    changed={**args,'plan':p,'all_plans':{'p1':p},'now':at,'closed_5m_end':at,'mark_as_of':at,
        'pm_allocation':policy({'p1':p}),'entry_pattern_binding':binding}
    first=admit_trader_signal(**changed)
    assert first['dispatched'] and first['judgment']['risk_unit']=='PROBE',first
    stored=b.get_admissions('2026-10-19')[0]
    assert stored['binding']['entry_pattern_binding']==binding
    assert D(stored['binding']['plan']['stop_price'])==D(binding['stop'])
    assert first['judgment']['shares']<=args['envelope'].max_shares_cap//2
    next_bar=bar(tm='09:52:00',vwap=fill_vwap,high=D('10.09'),low=fill_vwap-D('.01'),close=D('10.07'),volume_shares=300000)
    drive(b,next_bar,PaperRules('2026-10-19'))
    restarted=broker(tmp_path/'paper.db');snapshot=restarted.get_entry_pattern_stop_snapshot()
    assert snapshot['positions']
    if fill_vwap<D(binding['stop']):assert not restarted.planning_snapshot()['entry_risk_references']
    restored=held_stop_overlays({'p1':original},snapshot)
    assert restored['p1'].stop_price==D(binding['stop'])>original.stop_price
    assert original.stop_price==D('9')
    # A persisted stricter stop is never lowered to this admission's stop.
    stricter=replace(original,stop_price=D(binding['stop'])+D('.01'))
    assert held_stop_overlays({'p1':stricter},snapshot)['p1'].stop_price==stricter.stop_price
    # Actual coordinator after a process restart and T+1: a low above the old
    # stop but below the admitted B stop must produce the real hard-exit intent.
    from pathlib import Path
    from types import SimpleNamespace
    from liangjian_funnel.runtime.broker.local_paper import LocalPaperAdapter
    from liangjian_funnel.runtime.team_v2_control_minute import run_control_minute
    root=Path(__file__).resolve().parents[1]
    now=at.replace(day=20,hour=10,minute=3)
    adapter=LocalPaperAdapter(tmp_path/'paper.db',paper_config=root/'config/paper_broker.yaml',
        risk_config=root/'config/risk_limits.yaml',clock=lambda:now,
        calendar=SimpleNamespace(is_trading_day=lambda day:True),run_id='synthetic-b-restart')
    stop=D(binding['stop']);risk_bar=bar(day='20',tm='10:02:00',low=stop-D('.01'),high=stop+D('.02'),vwap=stop,close=stop)
    result=run_control_minute(session_kwargs=dict(broker=adapter,store=object(),clock=lambda:now,
        account_id='paper',all_plans={'p1':original}),closed_bars={risk_bar.symbol:risk_bar},
        deadline_monotonic=100,monotonic=lambda:0,model_enabled=False)
    assert result['hard_exits'][0]['dispatched'] and result['hard_exits'][0]['judgment']['reason_codes']==['ENGINE_HARD_STOP']
    assert adapter.get_orders('2026-10-20')[0]['side']=='SELL'


def test_team_unbound_stop_or_stale_confirmation_is_rejected(tmp_path):
    from test_team_v2_execution import fixture,policy
    from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal
    raw,proof,binding=source();b,args=fixture(tmp_path)
    with pytest.raises(ValueError,match='INTENT_BINDING_CONFLICT'):
        admit_trader_signal(**{**args,'entry_pattern_binding':binding})
    p=bind_team_plan(replace(args['plan'],entry_high=D(str(raw['trigger_zone']['high']))),binding)
    with pytest.raises(ValueError,match='CLOCK_CONFLICT'):
        admit_trader_signal(**{**args,'plan':p,'all_plans':{'p1':p},'entry_pattern_binding':binding,
            'pm_allocation':policy({'p1':p})})


def test_probe_envelope_is_idempotent_and_never_reopens_a6_block():
    from test_team_v2_risk_engine import envelope
    e=envelope();first=probe_envelope(e);second=probe_envelope(first)
    assert first==second and first.risk_unit=='PROBE'
    blocked=replace(e,eligible=False,risk_unit='NONE',max_shares_cap=0,blocked_reasons=('A6_SUSPEND',))
    assert not probe_envelope(blocked).eligible and 'A6_SUSPEND' in probe_envelope(blocked).blocked_reasons


def test_customer_real_frozen_contract_and_settlement_keep_stop_probe(tmp_path):
    from types import SimpleNamespace
    from liangjian_funnel.runtime.entry_contract import freeze_entry_contract
    from liangjian_funnel.runtime.state import RuntimeStore,PlanStatus,MonitorAction
    from liangjian_funnel.runtime.simulation import PaperBroker,SimulationConfig
    from liangjian_funnel.workflow import WorkflowApplication
    from liangjian_funnel.data.mootdx import MinuteBar
    raw,proof,binding=source();at=datetime.fromisoformat(binding['source_as_of'])
    strategy=dict(entry_confirmation=proof,entry_pattern_binding=binding,
        live_entry_price=float(binding['entry_price']),live_stop_level=float(binding['stop']))
    contract=freeze_entry_contract(raw['symbol'],raw,strategy,at=at,review_completed_at=at)
    assert contract['status']=='READY' and contract['risk_unit']==.33
    assert D(str(contract['stop_level']))==D(binding['stop']) and contract['entry_pattern_binding']==binding
    store=RuntimeStore(tmp_path/'customer.sqlite3')
    store.create_execution_plan('p1','lane_1',raw['symbol'],status=PlanStatus.ACTIVE_TODAY,
        valid_from=at-timedelta(minutes=1),expires_at=at.replace(hour=15),payload=raw)
    store.record_monitor_event(event_key='b-entry',lane_id='lane_1',minute_end=at,
        action=MonitorAction.BUY_SIGNAL,effective=True,sync_a4_lifecycle=True,
        payload={'plan_id':'p1','symbol':raw['symbol'],'entry_contract':contract})
    paper=PaperBroker(store,account_id='paper:lane_1',model='SYNTHETIC_PATTERN_B',config=SimulationConfig(initial_cash=1_000_000))
    app=SimpleNamespace(store=store,brokers={'lane_1':paper})
    end=datetime.fromisoformat(contract['eligible_bar_end'])
    minute=MinuteBar(symbol=raw['symbol'],interval='1m',bar_end=end,open=10.02,high=10.1,low=10.01,close=10.07,
        volume=1_000_000,amount=10_070_000,source_id='SYNTHETIC_PATTERN_B',adjust_mode='none',volume_unit='shares')
    results=WorkflowApplication._settle_prior_signals(app,'lane_1',raw['symbol'],minute)
    assert results and store.get_position('paper:lane_1',raw['symbol']) is not None
    position=store.get_position('paper:lane_1',raw['symbol'])
    assert D(str(position['stop_level']))==D(binding['stop'])


@pytest.mark.parametrize('shape',['A','B','C'])
def test_shape_switch_disables_only_selected_shape(shape):
    from test_entry_confirmation import a_rows,c_rows,environment,run
    cfg={**CFG,'patterns':{**CFG['patterns'],shape:False}}
    rows={'A':a_rows,'B':b_rows,'C':c_rows}[shape]()
    p=plan(high=100.2) if shape=='B' else plan('MA520_SWING',low=99,high=100,stop=98) if shape=='C' else plan()
    out=evaluate_pattern(p,rows,environment(rows),as_of=datetime.fromisoformat(rows[-1]['bar_end']),
        config=cfg,daily_facts=daily(),vwap=100)
    assert not out['confirmed']
    assert cfg['patterns'][next(x for x in ('A','B','C') if x!=shape)]
