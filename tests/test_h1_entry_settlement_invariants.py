"""H-1 red counterexamples; local synthetic bars, actual 10-16 contract clock.

These are not historical fills or a reconstruction of unobserved source prices.
The independent frozen-input reproduction is sealed in the Team V2 artifacts.
"""
from datetime import datetime
from types import SimpleNamespace
import sqlite3
import pytest
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationConfig
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus, MonitorAction, StateTransitionError
from liangjian_funnel.workflow import WorkflowApplication


def at(clock):
    return datetime.fromisoformat('2026-10-16T'+clock+'+08:00')


def seeded(tmp_path):
    store=RuntimeStore(tmp_path/'h1-local.sqlite3')
    store.create_execution_plan('p1','lane_1','600621.SH',status=PlanStatus.ACTIVE_TODAY,
        valid_from=at('09:32:00'),expires_at=at('15:00:00'),
        payload={'source_run_id':'h1-local-test','strategy_profile':'TREND_MA5',
                 'trigger_low':14.8,'trigger_high':15,'stop_level':14.49,'no_chase_price':15.67})
    store.record_monitor_event(event_key='h1-entry',lane_id='lane_1',minute_end=at('11:16:00'),
        action=MonitorAction.BUY_SIGNAL,effective=True,sync_a4_lifecycle=True,
        payload={'plan_id':'p1','symbol':'600621.SH','entry_contract':{
            'version':'a4-entry/2','status':'READY','eligible_bar_end':at('11:18:00').isoformat(),
            'review_completed_at':at('11:16:17.296675').isoformat(),
            'order_created_at':at('11:16:17.296675').isoformat(),
            'limit_price':14.85,'stop_level':14.49,'risk_unit':0.33,
            'fee_model_version':'paper-config/2','fill_model_version':'next-complete-minute-capacity/2'}})
    broker=PaperBroker(store,account_id='paper:lane_1',model='H1_LOCAL_TEST',
                       config=SimulationConfig(initial_cash=1_000_000))
    bar=MinuteBar(symbol='600621.SH',interval='1m',bar_end=at('11:18:00'),
        open=14.84,high=14.86,low=14.82,close=14.85,volume=1_000_000,amount=14_850_000,
        source_id='SYNTHETIC:H1_FAULT_INJECTION',adjust_mode='none',volume_unit='shares')
    return store,SimpleNamespace(store=store,brokers={'lane_1':broker}),bar


def test_wall_clock_must_not_expire_entry_before_eligible_bar_was_settled(tmp_path):
    store,app,_=seeded(tmp_path)
    # 11:19 monitor is about to process the complete 11:18 bar. No matching
    # attempt is recorded yet; wall clock alone cannot prove it was unfilled.
    assert WorkflowApplication._expire_missed_a4_entries(app,at('11:19:00'))==0
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='SIGNALLED'


def test_lifecycle_transaction_fault_must_not_leave_a_fill_or_cash_change(tmp_path):
    store,app,bar=seeded(tmp_path)
    before=store.get_account('paper:lane_1')['cash']
    # Only this temporary fixture DB is mutated to inject an SQLite failure.
    with sqlite3.connect(tmp_path/'h1-local.sqlite3') as c:
        c.execute("CREATE TRIGGER h1_fault BEFORE UPDATE ON a4_signal_lifecycles BEGIN SELECT RAISE(ABORT,'H1_INJECTED_LIFECYCLE_WRITE_FAILURE'); END")
    with pytest.raises(StateTransitionError):
        WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    assert store.list_fills('paper:lane_1')==()
    assert store.get_account('paper:lane_1')['cash']==before
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='SIGNALLED'


def test_atomicity_counterexample_fixture_really_reaches_matching(tmp_path):
    store,app,bar=seeded(tmp_path)
    result=WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    assert result and result[0]['status']=='FILLED'
    assert store.list_fills('paper:lane_1')
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='OPEN'
