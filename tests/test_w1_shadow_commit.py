"""Engine state is committed only after independent consumer acceptance."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import threading

import pytest

from test_wp1_ablation_engine import fixture,NOW
from test_shadow_session import databases,Ledger
from liangjian_funnel.runtime.shadow_variants import ShadowVariantEngine
from liangjian_funnel.runtime.shadow_session import ReadOnlyShadowSource,ShadowSession


def item():
    plan,bars,context=fixture()
    plan['strategy_facts']={'shadow_inputs':{'schema_version':'a4-shadow-inputs/1',
        'daily_ma5':10.5,'atr14':1,'previous_daily_closes':[10.3]*4,
        'previous_daily_close_dates':['2026-09-23','2026-09-24','2026-09-25','2026-09-28'],
        'daily_as_of':'2026-09-29T15:00:00+08:00','atr_source_hash':'a'*64}}
    return {'plan_id':'p','plan':plan,'bars':bars[:-1],
        'baseline':{'action':'DATA_BLOCK','state':'WARMUP','reason_codes':['FIXTURE_ONLY']},
        'now':NOW-timedelta(minutes=1),'decision_time':NOW,'market_context':context,
        'source_binding':{'fixture_only':True}}


def buy(*args,**kwargs):
    return {'action':'BUY_SIGNAL','state':'CONFIRMED','reason_codes':['FIXTURE_ONLY'],
        'met_conditions':[],'unmet_conditions':[],'veto_conditions':[],
        'reference_price':10.5,'ablation':{'status':'OK'}}


def following(data,at):
    copied=deepcopy(data); copied.update(now=at-timedelta(minutes=1),decision_time=at)
    return copied


def test_tentative_discard_does_not_consume_first_trigger():
    engine=ShadowVariantEngine(evaluator=buy)
    pending=engine.evaluate_tentative([item()],minute=NOW)
    assert len(pending.result['signals'])==6 and not engine._triggered
    assert engine.discard_tentative(pending)['ok']
    next_result=engine.evaluate_minute([following(item(),NOW+timedelta(minutes=1))],minute=NOW+timedelta(minutes=1))
    assert next_result['minute_summary']['first_trigger_count']==6


def test_committed_trigger_not_reemitted_next_minute_and_reentry_rejected():
    engine=ShadowVariantEngine(evaluator=buy)
    pending=engine.evaluate_tentative([item()],minute=NOW)
    committed=engine.commit_tentative(pending)
    assert committed['ok'] and committed['state_version']==1
    assert not engine.commit_tentative(pending)['ok']
    assert not engine.discard_tentative(pending)['ok']
    assert engine.evaluate_minute([following(item(),NOW+timedelta(minutes=1))],minute=NOW+timedelta(minutes=1))['signals']==[]


def test_pending_reservation_prevents_concurrent_direct_or_tentative_commit():
    engine=ShadowVariantEngine(evaluator=buy)
    pending=engine.evaluate_tentative([item()],minute=NOW)
    with ThreadPoolExecutor(max_workers=2) as pool:
        other=pool.submit(engine.evaluate_minute,[item()],minute=NOW).result()
        second=pool.submit(engine.evaluate_tentative,[item()],minute=NOW).result()
    assert other['minute_summary']['status']=='SHADOW_TRANSACTION_PENDING'
    assert second.result['minute_summary']['status']=='SHADOW_TRANSACTION_PENDING'
    assert not engine.commit_tentative(second)['ok']
    assert engine.commit_tentative(pending)['ok'] and len(engine._triggered)==6


def test_foreign_handle_and_mutated_result_cannot_commit():
    first=ShadowVariantEngine(evaluator=buy); second=ShadowVariantEngine(evaluator=buy)
    pending=first.evaluate_tentative([item()],minute=NOW)
    assert not second.commit_tentative(pending)['ok']
    pending.result['signals'][0]['variant_action']='WAIT'
    assert not first.commit_tentative(pending)['ok'] and not first._triggered
    assert first.discard_tentative(pending)['ok']


def test_partial_commit_only_preserves_confirmed_durable_keys():
    engine=ShadowVariantEngine(evaluator=buy)
    pending=engine.evaluate_tentative([item()],minute=NOW)
    assert not engine.commit_tentative(pending,accepted_keys={('p','INVALID')})['ok']
    receipt=engine.commit_tentative(pending,accepted_keys={('p','V1')})
    assert receipt['ok'] and engine._triggered=={('p','V1')}
    next_result=engine.evaluate_minute([following(item(),NOW+timedelta(minutes=1))],minute=NOW+timedelta(minutes=1))
    assert {row['variant_id'] for row in next_result['signals']}=={'V2','V3','V4','V5','V6'}


def test_cross_day_discard_does_not_erase_previously_committed_day():
    engine=ShadowVariantEngine(evaluator=buy)
    engine.evaluate_minute([item()],minute=NOW)
    tomorrow=NOW+timedelta(days=1)
    pending=engine.evaluate_tentative([following(item(),tomorrow)],minute=tomorrow)
    assert len(pending.result['signals'])==6 and engine._day==NOW.date()
    assert engine.discard_tentative(pending)['ok'] and len(engine._triggered)==6
    fresh=engine.evaluate_tentative([following(item(),tomorrow)],minute=tomorrow)
    assert engine.commit_tentative(fresh)['ok'] and engine._day==tomorrow.date()
    stale=engine.evaluate_tentative([item()],minute=NOW)
    assert stale.result['minute_summary']['status']=='SHADOW_CLOCK_REGRESSED'


def test_timed_out_worker_stays_single_capacity_after_discard():
    entered=threading.Event(); release=threading.Event(); calls=[]
    def blocked(*args,**kw):
        calls.append(1); entered.set(); release.wait(2); return buy()
    engine=ShadowVariantEngine(evaluator=blocked)
    try:
        pending=engine.evaluate_tentative([item()],minute=NOW,budget_seconds=.02)
        assert entered.is_set() and pending.result['minute_summary']['status']=='SHADOW_BUDGET_EXCEEDED'
        assert engine.discard_tentative(pending)['ok']
        next_pending=engine.evaluate_tentative([following(item(),NOW+timedelta(minutes=1))],minute=NOW+timedelta(minutes=1))
        assert next_pending.result['minute_summary']['status']=='SHADOW_WORKER_BUSY' and calls==[1]
        assert not engine._triggered
    finally:
        release.set(); engine._active.join(timeout=2)
    assert not engine._triggered


def service(tmp_path):
    state,minute=databases(tmp_path)
    source=ReadOnlyShadowSource(state,minute,lanes=('lane_1',))
    data=[item()]; wall=[NOW+timedelta(seconds=40)]; mono=[40.0]
    def read_round(at, **kw):
        from liangjian_funnel.runtime.shadow_session import _entry_admission
        copied=deepcopy(data)
        for current in copied:
            facts={'current_status':'ACTIVE_TODAY','terminal_at':None,'invalidated':False,'held':False,
                'valid_from':at.replace(hour=9,minute=30).isoformat(),
                'expires_at':at.replace(hour=15,minute=0).isoformat(),
                'minute':at.isoformat(),'observed_at':kw['observed_at'].isoformat(),'identity_proven':True}
            current.update(entry_scope_facts=facts,entry_admission=_entry_admission(facts))
        return copied,{'observed_at':kw['observed_at'].isoformat(),'fixture_only':True}
    source.read_round=read_round
    engine=ShadowVariantEngine(evaluator=buy); ledger=Ledger(tmp_path)
    signals=[]
    ledger.record_signal=lambda signal,plan,**kw:(signals.append(signal) or {'ok':True,'stored':True})
    ledger.record_minute=lambda summary,**kw:(ledger.minutes.append(summary) or {'ok':True,'stored':True})
    session=ShadowSession(source,engine=engine,ledger=ledger,started_at=NOW-timedelta(seconds=1),
        wall_clock=lambda:wall[0],clock=lambda:mono[0],
        # Explicit protocol fixture only. Identity business proof is NOT tested
        # by this mock writer/state-machine suite.
        identity_provider=lambda plan,at:{'status':'READY','evidence':{},'observed_at':at,'raw_response':b'FIXTURE'})
    return session,engine,ledger,data,wall,mono,signals


@pytest.mark.parametrize('end_seconds',[46,60])
def test_session_discard_after_budget_or_minute_fence_preserves_next_first_trigger(tmp_path,end_seconds):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    original=engine.evaluate_tentative
    def late(*args,**kw):
        result=original(*args,**kw); wall[0]=NOW+timedelta(seconds=end_seconds); mono[0]=end_seconds
        return result
    engine.evaluate_tentative=late
    first=session.poll(observed_at=NOW+timedelta(seconds=40))
    assert first['status']=='DATA_LIMITED' and not signals and not engine._triggered
    engine.evaluate_tentative=original
    data[0]=following(item(),NOW+timedelta(minutes=1))
    wall[0]=NOW+timedelta(minutes=1,seconds=1); mono[0]=61
    session.poll(observed_at=wall[0])
    assert len(signals)==6 and all(row['event_kind']=='FIRST_TRIGGER' for row in signals)


def test_session_partial_writer_failure_retries_only_unstored_next_minute(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    def write(signal,plan,**kw):
        if signal['variant_id']=='V2': return {'ok':False,'stored':False}
        signals.append(signal); return {'ok':True,'stored':True}
    ledger.record_signal=write
    receipt=session.poll(observed_at=wall[0])
    assert receipt['status']=='SHADOW_WRITE_FAILED'
    assert engine._triggered=={('p','V1')}
    ledger.record_signal=lambda signal,plan,**kw:(signals.append(signal) or {'ok':True,'stored':True})
    data[0]=following(item(),NOW+timedelta(minutes=1)); wall[0]=NOW+timedelta(minutes=1,seconds=1); mono[0]=61
    session.poll(observed_at=wall[0])
    assert [row['variant_id'] for row in signals]==['V1','V2','V3','V4','V5','V6']


def test_writer_unknown_outcome_quarantines_session_not_guessed_rollback(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    ledger.record_signal=lambda *args,**kw:(_ for _ in ()).throw(OSError('secret fixture'))
    receipt=session.poll(observed_at=wall[0])
    assert receipt['status']=='SHADOW_WRITE_FAILED' and not engine._triggered
    assert receipt['writer_state']=='UNKNOWN_SESSION_PAUSED'
    data[0]=following(item(),NOW+timedelta(minutes=1)); wall[0]=NOW+timedelta(minutes=1,seconds=1); mono[0]=61
    blocked=session.poll(observed_at=wall[0])
    assert blocked['gap_codes']==['SHADOW_WRITER_COMMIT_UNPROVEN_SESSION_PAUSED']


def test_unknown_second_write_cannot_rollback_previously_stored_first_key(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    def write(signal,plan,**kw):
        if signal['variant_id']=='V2': raise OSError('unknown commit outcome')
        signals.append(signal); return {'ok':True,'stored':True}
    ledger.record_signal=write
    receipt=session.poll(observed_at=wall[0])
    assert receipt['status']=='SHADOW_WRITE_FAILED' and session._writer_uncertain
    assert engine._triggered=={('p','V1')} and len(signals)==1


def test_same_handle_parallel_commit_has_one_winner_and_never_rolls_back():
    engine=ShadowVariantEngine(evaluator=buy)
    pending=engine.evaluate_tentative([item()],minute=NOW)
    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts=list(pool.map(lambda _:engine.commit_tentative(pending),range(8)))
    assert sum(receipt['ok'] for receipt in receipts)==1
    assert engine._state_version==1 and len(engine._triggered)==6


def test_no_signal_write_started_after_fence_during_other_provider_work(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    def provider(*args):
        wall[0]=NOW+timedelta(seconds=60); mono[0]=60
        return {'status':'DATA_LIMITED','evidence':{},'observed_at':NOW,'raw_response':None}
    session.identity_provider=provider
    receipt=session.poll(observed_at=NOW+timedelta(seconds=40))
    assert receipt['status']=='DATA_LIMITED' and not signals and not engine._triggered


def test_late_sync_writer_preserves_only_actual_stored_key_and_stops_remaining(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    def late_write(signal,plan,**kw):
        signals.append(signal); wall[0]=NOW+timedelta(seconds=60); mono[0]=60
        return {'ok':True,'stored':True}
    ledger.record_signal=late_write
    receipt=session.poll(observed_at=NOW+timedelta(seconds=40))
    assert receipt['status']=='DATA_LIMITED' and len(signals)==1 and not ledger.minutes
    assert receipt['state_commit']['status']=='PARTIALLY_COMMITTED'
    assert engine._triggered=={('p','V1')}
    ledger.record_signal=lambda signal,plan,**kw:(signals.append(signal) or {'ok':True,'stored':True})
    data[0]=following(item(),NOW+timedelta(minutes=1)); wall[0]=NOW+timedelta(minutes=1,seconds=1); mono[0]=61
    session.poll(observed_at=wall[0])
    assert [row['variant_id'] for row in signals]==['V1','V2','V3','V4','V5','V6']


@pytest.mark.parametrize('stage',['arrival_provider','minute_writer'])
def test_auxiliary_writer_or_provider_crossing_fence_cannot_claim_round_complete(tmp_path,stage):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    session.identity_provider=lambda *args:{'status':'READY','evidence':{},'observed_at':NOW,'raw_response':b'FIXTURE'}
    advanced=[]
    ledger.advance_outcomes=lambda *args,**kw:(advanced.append(1) or {'ok':True,'stored':True})
    def late_arrival(now):
        wall[0]=NOW+timedelta(seconds=60); mono[0]=60
        return []
    def late_minute(*args,**kw):
        ledger.minutes.append(1); wall[0]=NOW+timedelta(seconds=60); mono[0]=60
        return {'ok':True,'stored':True}
    session.bar_arrival_provider=late_arrival if stage=='arrival_provider' else lambda now:[]
    if stage=='minute_writer': ledger.record_minute=late_minute
    receipt=session.poll(observed_at=NOW+timedelta(seconds=40))
    assert receipt['status']=='DATA_LIMITED' and len(engine._triggered)==6
    assert receipt['gap_codes']==['SHADOW_WRITER_MINUTE_FENCE_REACHED']
    if stage=='arrival_provider': assert not advanced and not ledger.minutes


def test_minute_writer_failure_cannot_forget_successful_signal_writes(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    ledger.record_minute=lambda *args,**kw:{'ok':False,'stored':False}
    assert session.poll(observed_at=wall[0])['status']=='SHADOW_WRITE_FAILED'
    assert len(signals)==6 and len(engine._triggered)==6
    data[0]=following(item(),NOW+timedelta(minutes=1)); wall[0]=NOW+timedelta(minutes=1,seconds=1); mono[0]=61
    session.poll(observed_at=wall[0])
    assert len(signals)==6


def test_receipt_without_durability_does_not_allow_guessing_success(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    ledger.record_signal=lambda *args,**kw:{'ok':True}
    assert session.poll(observed_at=wall[0])['status']=='SHADOW_WRITE_FAILED'
    assert session._writer_uncertain and not engine._triggered


def test_ok_flag_with_explicit_unstored_does_not_commit_trigger(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    ledger.record_signal=lambda *args,**kw:{'ok':True,'stored':False}
    receipt=session.poll(observed_at=wall[0])
    assert receipt['status']=='SHADOW_WRITE_FAILED' and not engine._triggered


def test_stored_sqlite_with_pending_mirror_is_not_retried_as_unstored(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    ledger.record_signal=lambda signal,plan,**kw:(signals.append(signal) or
        {'ok':True,'stored':True,'mirror_status':'PENDING'})
    session.poll(observed_at=wall[0])
    assert len(engine._triggered)==6 and not session._writer_uncertain


def test_shadow_timeout_does_not_change_original_production_action_or_timestamps(tmp_path,monkeypatch):
    from pathlib import Path
    import hashlib
    from liangjian_funnel.runtime import strategies
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    data[0]['actual_outer_baseline']={'action':'START_CONFIRMATION','reason':'A4_SESSION_WARMUP',
        'minute_end':NOW.isoformat(),'created_at':(NOW+timedelta(seconds=40)).isoformat(),
        'production_deadline_at':(NOW+timedelta(seconds=47)).isoformat()}
    original=deepcopy(data[0]); entered=threading.Event(); release=threading.Event()
    def no_baseline(*args,**kw): raise AssertionError('production baseline changed')
    monkeypatch.setattr(strategies,'evaluate_strategy',no_baseline)
    def blocked(*args,**kw): entered.set(); release.wait(2); return buy()
    engine.evaluator=blocked; session.budget_seconds=.02
    workflow=Path(__file__).resolve().parents[1]/'src/liangjian_funnel/workflow.py'
    before=hashlib.sha256(workflow.read_bytes()).hexdigest()
    try:
        receipt=session.poll(observed_at=wall[0])
        assert entered.is_set() and receipt['status']=='DATA_LIMITED'
        assert receipt['engine_status']=='SHADOW_BUDGET_EXCEEDED'
        assert data[0]==original and not signals and not engine._triggered
        assert hashlib.sha256(workflow.read_bytes()).hexdigest()==before
    finally:
        release.set(); engine._active.join(timeout=2)


def test_evaluate_four_seconds_then_first_write_over_total_budget_stops_remaining(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    original=engine.evaluate_tentative
    def four_seconds(*args,**kw):
        pending=original(*args,**kw); wall[0]=NOW+timedelta(seconds=44); mono[0]=44
        return pending
    engine.evaluate_tentative=four_seconds
    session.identity_provider=lambda *args:{'status':'READY','evidence':{},'observed_at':NOW,'raw_response':b'FIXTURE'}
    session.bar_arrival_provider=lambda now:[]
    def write(signal,plan,**kw):
        signals.append(signal); wall[0]+=timedelta(seconds=1.5); mono[0]+=1.5
        return {'ok':True,'stored':True}
    ledger.record_signal=write
    receipt=session.poll(observed_at=NOW+timedelta(seconds=40))
    assert receipt['status']=='DATA_LIMITED'
    assert receipt['gap_codes']==['SHADOW_WRITER_TOTAL_BUDGET_EXCEEDED']
    assert len(signals)==1 and not ledger.minutes and engine._triggered=={('p','V1')}
    engine.evaluate_tentative=original
    ledger.record_signal=lambda signal,plan,**kw:(signals.append(signal) or {'ok':True,'stored':True})
    data[0]=following(item(),NOW+timedelta(minutes=1)); wall[0]=NOW+timedelta(minutes=1,seconds=1); mono[0]=61
    session.poll(observed_at=wall[0])
    assert [row['variant_id'] for row in signals]==['V1','V2','V3','V4','V5','V6']


def test_dependency_monotonic_regression_stops_writes_without_consuming_state(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    def regressed(*args):
        mono[0]=39
        return {'status':'READY','evidence':{},'observed_at':NOW,'raw_response':b'FIXTURE'}
    session.identity_provider=regressed
    receipt=session.poll(observed_at=NOW+timedelta(seconds=40))
    assert receipt['status']=='DATA_LIMITED' and not signals and not engine._triggered
    assert receipt['gap_codes']==['SHADOW_WRITER_CLOCK_REGRESSED']


def test_dependency_exhausting_total_budget_never_starts_first_signal_write(tmp_path):
    session,engine,ledger,data,wall,mono,signals=service(tmp_path)
    def slow(*args):
        wall[0]=NOW+timedelta(seconds=45); mono[0]=45
        return {'status':'READY','evidence':{},'observed_at':NOW,'raw_response':b'FIXTURE'}
    session.identity_provider=slow
    receipt=session.poll(observed_at=NOW+timedelta(seconds=40))
    assert receipt['status']=='DATA_LIMITED' and not signals and not engine._triggered
    assert receipt['gap_codes']==['SHADOW_WRITER_TOTAL_BUDGET_EXCEEDED']
