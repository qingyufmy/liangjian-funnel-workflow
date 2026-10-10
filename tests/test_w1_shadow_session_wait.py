"""Explicit original round completion and same-minute observed-clock fences."""
from datetime import timedelta
import hashlib
import json
import sqlite3

import pytest

from test_shadow_session import AT,CUTOFF,databases,Engine,Ledger,module,write_completion,completion_payload


def waiting_session(tmp_path):
    state,minute=databases(tmp_path)
    latest=tmp_path/'latest.json'
    wall=[AT]; mono=[0.0]
    engine,ledger=Engine(),Ledger(tmp_path)
    source=module().ReadOnlyShadowSource(state,minute,lanes=('lane_1',),monitor_latest=latest)
    service=module().ShadowSession(source,engine=engine,ledger=ledger,started_at=AT-timedelta(seconds=1),
        clock=lambda:mono[0],wall_clock=lambda:wall[0])
    return service,engine,ledger,state,latest,wall,mono


@pytest.mark.parametrize('completed_after',[20,40])
def test_source_wait_is_not_five_second_or_fifteen_second_gate(tmp_path,completed_after):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    for seconds in (0,5,15):
        wall[0]=AT+timedelta(seconds=seconds); mono[0]=seconds
        assert service.poll(observed_at=wall[0])['status']=='WAITING_PRODUCTION_COMPLETION'
        assert not engine.calls and not ledger.minutes
    wall[0]=AT+timedelta(seconds=completed_after); mono[0]=completed_after
    with sqlite3.connect(state) as db: db.execute('UPDATE monitor_events SET created_at=?',(wall[0].isoformat(),))
    write_completion(latest,state,finished=wall[0])
    result=service.poll(observed_at=wall[0])
    assert result['scope_status']=='COMPLETE' and len(engine.calls)==1
    assert result['production_completion']['raw_bytes_sha256']==hashlib.sha256(latest.read_bytes()).hexdigest()
    assert result['result_ready_at']==wall[0].isoformat()
    assert engine.calls[0][0][0]['now']==CUTOFF and engine.calls[0][1]['minute']==AT
    assert service.poll(observed_at=wall[0])['status']=='DUPLICATE'


def test_round_completion_missing_same_minute_never_backfills_after_rollover(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    wall[0]=AT+timedelta(seconds=55); mono[0]=55
    assert service.poll(observed_at=wall[0])['status']=='DATA_LIMITED'
    write_completion(latest,state,finished=AT+timedelta(seconds=40))
    wall[0]=AT+timedelta(seconds=60); mono[0]=60
    assert service.poll(observed_at=wall[0])['status']=='WAITING_PRODUCTION_COMPLETION'
    assert not engine.calls


@pytest.mark.parametrize('case',['wrong_hash','future_finish','wrong_snapshot','future_cutoff','partial_lane'])
def test_completion_contract_cannot_certify_wrong_original_round(case,tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    payload=completion_payload(state)
    if case=='wrong_hash': payload['observability']['decision_hash']='0'*64
    elif case=='future_finish': payload['observability']['timing_spans'][0]['finished_at']=(AT+timedelta(seconds=20)).isoformat()
    elif case=='wrong_snapshot': payload['minute_snapshot_id']='other'
    elif case=='future_cutoff': payload['execution_cutoff']=AT.isoformat()
    else: payload['lanes'][0]['events']=[]
    from liangjian_funnel.reporting import atomic_write_json
    atomic_write_json(latest,payload)
    wall[0]=AT+timedelta(seconds=1); mono[0]=1
    result=service.poll(observed_at=wall[0])
    assert result['status']=='DATA_LIMITED' and not engine.calls and not ledger.minutes


def test_evaluation_result_that_crosses_minute_fence_is_not_written(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    wall[0]=AT+timedelta(seconds=50); mono[0]=50
    write_completion(latest,state,finished=wall[0])
    original=engine.evaluate_minute
    def crossing(*args,**kw):
        result=original(*args,**kw); wall[0]=AT+timedelta(seconds=60); mono[0]=60
        return result
    engine.evaluate_minute=crossing
    result=service.poll(observed_at=AT+timedelta(seconds=50))
    assert result['status']=='DATA_LIMITED' and result['gap_codes']==['SHADOW_RESULT_OUTSIDE_MINUTE_OR_BUDGET']
    assert not ledger.minutes


def test_result_ready_clock_not_poll_clock_is_used_for_independent_writes(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    wall[0]=AT+timedelta(seconds=40); mono[0]=40
    write_completion(latest,state,finished=wall[0])
    original=engine.evaluate_minute; recorded=[]
    def evaluation(*args,**kw):
        result=original(*args,**kw); wall[0]=AT+timedelta(seconds=42); mono[0]=42
        return result
    engine.evaluate_minute=evaluation
    ledger.record_minute=lambda summary,**kw:(recorded.append(kw['observed_at']) or {'ok':True})
    result=service.poll(observed_at=AT+timedelta(seconds=40))
    assert result['result_ready_at']==wall[0].isoformat() and recorded==[wall[0]]


def test_default_completion_provider_is_explicitly_unwired(tmp_path):
    state,minute=databases(tmp_path)
    service=module().ShadowSession(module().ReadOnlyShadowSource(state,minute,lanes=('lane_1',)),
        engine=Engine(),ledger=Ledger(tmp_path),started_at=AT-timedelta(seconds=1),wall_clock=lambda:AT)
    assert service.poll(observed_at=AT)['gap_codes']==['PRODUCTION_COMPLETION_UNWIRED']


@pytest.mark.parametrize('seconds',[55,56,59])
def test_late_completion_with_less_than_full_evaluation_room_is_not_admitted(tmp_path,seconds):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    wall[0]=AT+timedelta(seconds=seconds); mono[0]=seconds
    write_completion(latest,state,finished=wall[0])
    assert service.poll(observed_at=wall[0])['status']=='DATA_LIMITED'
    assert not engine.calls and not ledger.minutes


def test_source_read_that_uses_remaining_room_does_not_start_engine(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    wall[0]=AT+timedelta(seconds=40); mono[0]=40
    write_completion(latest,state,finished=wall[0])
    original=service.source.read_round
    def slow(*args,**kw):
        result=original(*args,**kw); wall[0]=AT+timedelta(seconds=55); mono[0]=55
        return result
    service.source.read_round=slow
    result=service.poll(observed_at=AT+timedelta(seconds=40))
    assert result['gap_codes']==['SHADOW_SOURCE_READY_OUTSIDE_MINUTE_FENCE'] and not engine.calls


def test_partial_scope_and_late_second_plan_require_original_round_to_finish(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    # Even complete currently visible rows cannot stand in for the original
    # producer end fence; activation/source writes may still be running.
    wall[0]=AT+timedelta(seconds=15); mono[0]=15
    result=service.poll(observed_at=wall[0])
    assert result['status']=='WAITING_PRODUCTION_COMPLETION' and not engine.calls
    write_completion(latest,state,finished=AT+timedelta(seconds=20))
    with sqlite3.connect(state) as db: db.execute("DELETE FROM monitor_events")
    wall[0]=AT+timedelta(seconds=20); mono[0]=20
    result=service.poll(observed_at=wall[0])
    assert result['status']=='DATA_LIMITED' and not engine.calls


def test_replaced_completion_bytes_during_source_transaction_are_not_pinned_falsely(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    write_completion(latest,state)
    original=service.source.read_minute
    def changed(*args,**kw):
        items=original(*args,**kw)
        write_completion(latest,state,finished=AT+timedelta(seconds=1))
        return items
    service.source.read_minute=changed
    wall[0]=AT+timedelta(seconds=2); mono[0]=2
    result=service.poll(observed_at=wall[0])
    assert result['gap_codes']==['PRODUCTION_COMPLETION_CHANGED_DURING_SOURCE_READ']
    assert not engine.calls


def test_completed_source_requires_closed_previous_bar_not_current_wall_bar(tmp_path):
    import zlib
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    write_completion(latest,state)
    with sqlite3.connect(service.source.minute_db) as db:
        values=json.loads(zlib.decompress(db.execute('SELECT payload_zlib FROM minute_decision_snapshots LIMIT 1').fetchone()[0]))
        values[0]['bar_end']=AT.isoformat(); raw=json.dumps(values).encode()
        db.execute('UPDATE minute_decision_snapshots SET payload_zlib=?,payload_sha256=?',
            (zlib.compress(raw),hashlib.sha256(raw).hexdigest()))
    wall[0]=AT+timedelta(seconds=20); mono[0]=20
    assert service.poll(observed_at=wall[0])['status']=='DATA_LIMITED'
    assert not engine.calls


def test_backward_result_wall_clock_cannot_backdate_independent_signal(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    wall[0]=AT+timedelta(seconds=40); mono[0]=40
    write_completion(latest,state,finished=wall[0])
    original=engine.evaluate_minute
    def backward(*args,**kw):
        result=original(*args,**kw); wall[0]=AT+timedelta(seconds=39); mono[0]=41
        return result
    engine.evaluate_minute=backward
    result=service.poll(observed_at=AT+timedelta(seconds=40))
    assert result['gap_codes']==['SHADOW_RESULT_OUTSIDE_MINUTE_OR_BUDGET'] and not ledger.minutes


def test_completion_arriving_between_poll_clock_and_file_read_uses_actual_read_clock(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    wall[0]=AT+timedelta(seconds=40); mono[0]=40
    original=service.source.read_completion
    def arriving(*args,**kw):
        wall[0]=AT+timedelta(seconds=41); mono[0]=41
        with sqlite3.connect(state) as db:
            db.execute('UPDATE monitor_events SET created_at=?',(wall[0].isoformat(),))
        write_completion(latest,state,finished=wall[0])
        return original(*args,**kw)
    service.source.read_completion=arriving
    result=service.poll(observed_at=AT+timedelta(seconds=40))
    assert result['scope_status']=='COMPLETE' and len(engine.calls)==1
    assert result['observed_at']==(AT+timedelta(seconds=40)).isoformat()
    assert result['production_completion']['observed_at']==wall[0].isoformat()


def test_backward_source_read_clock_does_not_certify_completion(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    write_completion(latest,state,finished=AT+timedelta(seconds=39))
    wall[0]=AT+timedelta(seconds=39); mono[0]=40
    result=service.poll(observed_at=AT+timedelta(seconds=40))
    assert result['gap_codes']==['PRODUCTION_COMPLETION_OBSERVATION_CLOCK_CONFLICT']
    assert not engine.calls and not ledger.minutes


def test_evaluation_clock_cannot_precede_actual_completion_observation(tmp_path):
    service,engine,ledger,state,latest,wall,mono=waiting_session(tmp_path)
    write_completion(latest,state,finished=AT+timedelta(seconds=40))
    clocks=iter([AT+timedelta(seconds=41),AT+timedelta(seconds=40)])
    service.wall_clock=lambda:next(clocks)
    result=service.poll(observed_at=AT+timedelta(seconds=40))
    assert result['gap_codes']==['SHADOW_SOURCE_READY_OUTSIDE_MINUTE_FENCE']
    assert not engine.calls and not ledger.minutes
