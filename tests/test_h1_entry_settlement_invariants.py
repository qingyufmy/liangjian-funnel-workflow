"""H-1 red counterexamples; local synthetic bars, actual 10-16 contract clock.

These are not historical fills or a reconstruction of unobserved source prices.
The independent frozen-input reproduction is sealed in the Team V2 artifacts.
"""
from datetime import datetime, timedelta
from types import SimpleNamespace
import sqlite3
import pytest
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationConfig
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus, MonitorAction, StateTransitionError
from liangjian_funnel.workflow import WorkflowApplication
from liangjian_funnel.runtime.entry_contract import entry_bar_for
from liangjian_funnel.runtime.monitor import MonitorEngine


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
    # A transaction failure may surface or be returned as a blocked result;
    # neither form is allowed to leave an orphan fill or change cash.
    try:
        results=WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    except StateTransitionError:
        results=[]
    assert all(row['status'] != 'FILLED' for row in results)
    assert store.list_fills('paper:lane_1')==()
    assert store.get_account('paper:lane_1')['cash']==before
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='SIGNALLED'


def test_atomicity_counterexample_fixture_really_reaches_matching(tmp_path):
    store,app,bar=seeded(tmp_path)
    result=WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    assert result and result[0]['status']=='FILLED'
    assert store.list_fills('paper:lane_1')
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='OPEN'


@pytest.mark.parametrize(('clock','expected'),[
    ('09:30:00','09:31:00'), ('11:29:59','13:01:00'),
    ('11:30:00','13:01:00'), ('13:00:00','13:01:00'),
    ('14:59:00','15:00:00'), ('14:59:01',None), ('15:00:00',None),
])
def test_single_entry_clock_session_boundaries(clock,expected):
    assert entry_bar_for({'signal_time':at(clock)}) == (at(expected) if expected else None)


def test_unfilled_requires_the_exact_original_matching_bar(tmp_path):
    store,app,bar=seeded(tmp_path)
    later=bar.model_copy(update={'bar_end':at('11:19:00')})
    assert WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',later)==[]
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='SIGNALLED'
    blocked=bar.model_copy(update={'open':14.9,'high':14.92,'low':14.88,'close':14.9})
    result=WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',blocked)
    assert result[0]['reason_code']=='LIMIT_NOT_REACHED'
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='UNFILLED'
    with sqlite3.connect(tmp_path/'h1-local.sqlite3') as c:
        assert c.execute('SELECT eligible_bar_end FROM a4_entry_matching_attempts').fetchone()[0] == at('11:18:00').isoformat()


def test_after_lifecycle_transition_fill_insert_fault_rolls_back_everything(tmp_path):
    store,app,bar=seeded(tmp_path)
    before=store.get_account('paper:lane_1')['cash']
    with sqlite3.connect(tmp_path/'h1-local.sqlite3') as c:
        c.execute("CREATE TRIGGER h1_fault AFTER INSERT ON virtual_fills BEGIN SELECT RAISE(ABORT,'H1_INJECTED_FILL_WRITE_FAILURE'); END")
    with pytest.raises(StateTransitionError):
        WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    assert store.list_fills('paper:lane_1')==()
    assert store.get_account('paper:lane_1')['cash']==before
    assert store.get_position('paper:lane_1','600621.SH') is None
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='SIGNALLED'


def test_restart_replays_neither_cash_nor_lifecycle(tmp_path):
    store,app,bar=seeded(tmp_path)
    WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    before=store.get_account('paper:lane_1')['cash']
    restarted=RuntimeStore(tmp_path/'h1-local.sqlite3')
    app.store=restarted
    app.brokers={'lane_1':PaperBroker(restarted,account_id='paper:lane_1',model='H1_LOCAL_TEST')}
    assert WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)==[]
    assert len(restarted.list_fills('paper:lane_1'))==1
    assert restarted.get_account('paper:lane_1')['cash']==before
    assert restarted.get_a4_signal_lifecycle('h1-entry')['status']=='OPEN'


def corrupt_local_fixture_only(tmp_path,store):
    # Recreate the impossible legacy projection in this temporary fixture only.
    with sqlite3.connect(tmp_path/'h1-local.sqlite3') as c:
        c.execute("UPDATE a4_signal_lifecycles SET status='UNFILLED',remaining_qty=0,entry_qty=0 WHERE entry_event_key='h1-entry'")
    return store.get_a4_signal_lifecycle('h1-entry')


def test_historical_orphan_is_quarantined_without_reopening_or_money_replay(tmp_path):
    store,app,bar=seeded(tmp_path)
    WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    original=corrupt_local_fixture_only(tmp_path,store)
    cash=store.get_account('paper:lane_1')['cash']
    for _ in range(2):
        WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    assert store.get_a4_signal_lifecycle('h1-entry')==original
    assert store.get_account('paper:lane_1')['cash']==cash
    assert len(store.list_fills('paper:lane_1'))==1
    incident=store.get_a4_plan_quarantine('p1')
    assert incident['reason_code']=='A4_TERMINAL_STATE_CONFLICT'
    assert 'A4_LIFECYCLE_RECONCILIATION_REQUIRED' in incident['evidence_json']


def test_quarantined_position_still_records_hard_stop_without_terminal_reopen(tmp_path):
    store,app,bar=seeded(tmp_path)
    WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    original=corrupt_local_fixture_only(tmp_path,store)
    WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    risk=bar.model_copy(update={'bar_end':at('11:19:00'),'low':14.4,'close':14.45})
    batch=MonitorEngine(store).process_minute('lane_1',{'600621.SH':risk},
        minute_snapshot_id='h1-hard-stop',now=at('11:19:00'),risk_bars={'600621.SH':risk})
    assert any(event.action=='FORCED_RISK_EXIT' for event in batch.events)
    assert store.get_a4_signal_lifecycle('h1-entry')==original


def test_fault_isolated_and_other_plan_239_minutes_are_committed(tmp_path):
    store,app,bar=seeded(tmp_path)
    others={f'good-{i}': f'{600000+i:06d}.SH' for i in range(12)}
    for plan_id,symbol in others.items():
        store.create_execution_plan(plan_id,'lane_1',symbol,status=PlanStatus.ACTIVE_TODAY,
            valid_from=at('09:32:00'),expires_at=at('15:00:00'),
            payload={'trigger_low':100,'trigger_high':101,'confirmation_bars':2})
    # A real SQLite fault on just the bad plan exercises production isolation.
    with sqlite3.connect(tmp_path/'h1-local.sqlite3') as c:
        c.execute("CREATE TRIGGER h1_fault BEFORE UPDATE ON a4_signal_lifecycles WHEN OLD.plan_id='p1' BEGIN SELECT RAISE(ABORT,'H1_INJECTED_PLAN_FAILURE'); END")
    rows,error=WorkflowApplication._settle_prior_signals_isolated(app,'lane_1','600621.SH',bar,observed_at=at('11:19:00'))
    assert rows==[] and error=='STATE_CONFLICT_QUARANTINED'
    engine=MonitorEngine(store)
    minutes=[at('09:32:00')+timedelta(minutes=i) for i in range(119)]
    minutes += [at('13:01:00')+timedelta(minutes=i) for i in range(120)]
    # Contract: 239 applicable minutes (09:32–11:30 and 13:01–15:00).
    assert len(minutes)==239
    for i,minute in enumerate(minutes):
        bad=bar.model_copy(update={'bar_end':minute})
        good={symbol:bar.model_copy(update={'symbol':symbol,'bar_end':minute}) for symbol in others.values()}
        engine.process_minute('lane_1',{'600621.SH':bad,**good},
            minute_snapshot_id=f'h1-scope-{i}',now=minute)
    import json
    good_rows=[row for row in store.list_monitor_events() if json.loads(row['payload_json']).get('plan_id') in others]
    assert len(good_rows)==239*12
    for plan_id in others:
        assert len({row['minute_end'] for row in good_rows if json.loads(row['payload_json']).get('plan_id')==plan_id})==239
    assert not any(row['action']=='DATA_BLOCK' for row in good_rows)
    assert store.list_fills('paper:lane_1')==()


def test_late_packet_matches_original_window_not_the_latest_bar(tmp_path):
    store,app,bar=seeded(tmp_path)
    latest=bar.model_copy(update={'bar_end':at('11:20:00'),'open':14.99})
    result=WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',latest,entry_bars=(bar,latest))
    assert result[0]['status']=='FILLED'
    assert result[0]['execution_bar_end']==at('11:18:00').isoformat()
    assert result[0]['price']==14.84


@pytest.mark.parametrize('update',[{'volume':0}, {'low':14.84,'high':14.84,'open':14.84,'close':14.84}])
def test_proven_unexecutable_bar_no_fabricated_candle_or_fill(tmp_path,update):
    store,app,bar=seeded(tmp_path)
    blocked=bar.model_copy(update=update)
    result=WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',blocked)
    assert result[0]['reason_code']=='BAR_NOT_EXECUTABLE'
    assert store.get_a4_signal_lifecycle('h1-entry')['status']=='UNFILLED'
    assert store.list_fills('paper:lane_1')==()


def test_quarantine_excludes_cohort_but_raw_decision_remains_readable(tmp_path):
    store,_,_=seeded(tmp_path)
    before=store.record_outcome_label({'run_id':'h1-local','lane_id':'lane_1','trade_date':'2026-10-16',
        'stage':'A4','symbol':'600621.SH','decision':'PASSED','snapshot_id':'frozen-local',
        'config_hash':'local','metadata':{'plan_id':'p1'}})
    store.quarantine_a4_signal('h1-entry',reason_code='A4_TERMINAL_STATE_CONFLICT',at=at('11:19:00'))
    assert store.list_outcome_labels(stage='A4')==()
    assert store.list_outcome_labels(stage='A4',include_quarantined=True)==(before,)
