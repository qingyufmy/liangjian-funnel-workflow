from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import sqlite3
from types import SimpleNamespace
import zlib

import pytest

from liangjian_funnel.evaluation.team_v2_replay_source import project_snapshot, sha
from liangjian_funnel.evaluation.team_v2_replay_materializer import FrozenMinuteMaterializer


def source(tmp_path, *, omit_first=False):
    path=tmp_path/'frozen.db'
    bars=[dict(symbol='000001.SZ',interval='1m',bar_end=f'2026-10-15T09:{n:02d}:00+08:00',
        open=10,high=10.1,low=9.9,close=10,volume=10000,amount=100000,volume_unit='shares',
        source_id='UNIT_TEST:reported',amount_kind='reported',evidence_kind='MARKET_BAR',adjust_mode='none')
        for n in ((32,) if omit_first else (31,32))]
    raw=json.dumps(bars).encode()
    snap=dict(snapshot_id='s',symbol='000001.SZ',interval='1m',decision_as_of='2026-10-15T09:33:00+08:00',
        captured_at='2026-10-15T09:33:01+08:00',payload_sha256=hashlib.sha256(raw).hexdigest(),payload_zlib=zlib.compress(raw))
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE minute_decision_snapshots(snapshot_id TEXT,symbol TEXT,interval TEXT,'
            'decision_as_of TEXT,captured_at TEXT,payload_sha256 TEXT,payload_zlib BLOB)')
        db.execute('INSERT INTO minute_decision_snapshots VALUES (?,?,?,?,?,?,?)',tuple(snap.values()))
        db.execute('CREATE TABLE execution_plans(plan_id TEXT,symbol TEXT,payload_json TEXT)')
        db.execute('INSERT INTO execution_plans VALUES (?,?,?)',('p','000001.SZ',json.dumps(dict(symbol='000001.SZ'))))
    row=dict(plan_id='p',symbol='000001.SZ',payload_json=json.dumps(dict(symbol='000001.SZ')))
    proof=project_snapshot(snap,symbol=row['symbol'],decision_at=snap['decision_as_of'],event_created_at='2026-10-15T09:33:02+08:00')
    frame=dict(plan_id='p',decision_at=snap['decision_as_of'],status='SOURCE_BOUND_RULES_PENDING',
        original_snapshot_id='s',original_plan_row_sha256=sha(row),source=proof,original_action='WAIT',
        original_events=[dict(created_at='2026-10-15T09:33:02+08:00',payload_json='{"strategy":{"met_conditions":["A"]}}')])
    frame['frame_sha256']=sha(frame)
    batch=dict(trade_date='2026-10-15',decision_at=snap['decision_as_of'],closed_bar_end='2026-10-15T09:32:00+08:00',
        quote_basis='REPLAY_BAR_DERIVED',source_frames=[frame],entry_source_frames=[frame],absent_original_plan_ids=['missing'])
    return path,hashlib.sha256(path.read_bytes()).hexdigest(),{'p':row},batch


def test_original_full_window_to_typed_paper_with_no_execution_permission_and_bounded_cache(tmp_path):
    path,pin,plans,batch=source(tmp_path)
    with FrozenMinuteMaterializer(path,expected_sha256=pin,plans=plans,settings=SimpleNamespace(),cache_size=1) as reader:
        first=reader.materialize(batch);second=reader.materialize(batch)
        assert reader.snapshot_reads==1 and len(reader.cache)==1
        assert first['source_digest']==second['source_digest']
        assert first['entry_plan_ids']==['p'] and first['closed_bars']['000001.SZ'].tradability_known
        assert first['projected']['p']['quote'].quote_basis=='REPLAY_BAR_DERIVED'
        assert first['projected']['p']['tech_reference']=={'A':'MET'}
        assert first['absent_original_plan_ids']==['missing']
        assert first['historical_live_state']['status']=='DATA_BLOCKED'
        assert first['model_permission']=='PM_AND_A6_RESULTS_STILL_REQUIRED'
    assert hashlib.sha256(path.read_bytes()).hexdigest()==pin


def test_valid_hash_but_incomplete_window_is_not_repaired(tmp_path):
    path,pin,plans,batch=source(tmp_path,omit_first=True)
    with FrozenMinuteMaterializer(path,expected_sha256=pin,plans=plans,settings=SimpleNamespace()) as reader:
        value=reader.materialize(batch)
    assert not value['closed_bars'] and not value['entry_plan_ids']
    assert value['limited'][0]['reason_codes']==['CONTROL_SOURCE_SESSION_INCOMPLETE']


@pytest.mark.parametrize('change',['frame','source','plan','arrival','entry-domain','clock','quote-basis'])
def test_source_binding_tampering_never_becomes_an_executable_bar(tmp_path,change):
    path,pin,plans,batch=source(tmp_path);frame=batch['source_frames'][0]
    if change=='frame':frame['original_action']='BUY'
    elif change=='source':frame['source']['original_snapshot_sha256']='0'*64
    elif change=='plan':plans['p']['payload_json']='{}'
    elif change=='arrival':frame['original_events'][0]['created_at']='2026-10-15T09:33:00+08:00'
    elif change=='entry-domain':batch['entry_source_frames']=[dict(plan_id='invented')]
    elif change=='clock':batch['decision_at']='2026-10-15T09:34:00+08:00'
    else:batch['quote_basis']='ACTUAL_QUOTE'
    if change in {'source','arrival'}:frame['frame_sha256']=sha({k:v for k,v in frame.items() if k!='frame_sha256'})
    with FrozenMinuteMaterializer(path,expected_sha256=pin,plans=plans,settings=SimpleNamespace()) as reader:
        with pytest.raises(ValueError,match='REPLAY_'):reader.materialize(batch)


def test_original_terminal_frame_protectable_but_not_reactivated_as_entry(tmp_path):
    path,pin,plans,batch=source(tmp_path)
    batch['entry_source_frames']=[]
    with FrozenMinuteMaterializer(path,expected_sha256=pin,plans=plans,settings=SimpleNamespace()) as reader:
        value=reader.materialize(batch)
    assert value['closed_bars'] and 'p' in value['projected']
    assert value['entry_plan_ids']==[] and not value['projected']['p']['entry_source_eligible']


def test_cache_hit_does_not_borrow_another_plans_later_capture_permission(tmp_path):
    path,pin,plans,batch=source(tmp_path)
    with FrozenMinuteMaterializer(path,expected_sha256=pin,plans=plans,settings=SimpleNamespace()) as reader:
        reader.materialize(batch)
        frame=deepcopy(batch['source_frames'][0]);frame['original_events'][0]['created_at']='2026-10-15T09:33:00+08:00'
        frame['frame_sha256']=sha({k:v for k,v in frame.items() if k!='frame_sha256'})
        with pytest.raises(ValueError,match='FRAME_SOURCE_MISMATCH'):reader.materialize(batch|{'source_frames':[frame]})


def test_coherently_rehashed_external_plan_cannot_replace_original_database_row(tmp_path):
    path,pin,plans,batch=source(tmp_path)
    plans['p']['payload_json']=json.dumps(dict(symbol='000001.SZ',trigger_low=1))
    frame=batch['source_frames'][0];frame['original_plan_row_sha256']=sha(plans['p'])
    frame['frame_sha256']=sha({k:v for k,v in frame.items() if k!='frame_sha256'})
    with FrozenMinuteMaterializer(path,expected_sha256=pin,plans=plans,settings=SimpleNamespace()) as reader:
        with pytest.raises(ValueError,match='DATABASE_PLAN_MISMATCH'):reader.materialize(batch)


def test_frozen_file_changed_between_constructor_and_ro_transaction_is_rejected(tmp_path):
    path,pin,plans,batch=source(tmp_path)
    reader=FrozenMinuteMaterializer(path,expected_sha256=pin,plans=plans,settings=SimpleNamespace())
    with sqlite3.connect(path) as db:
        db.execute("UPDATE execution_plans SET payload_json='{}'")
    with pytest.raises(ValueError,match='CHANGED_BEFORE_READ'):
        with reader:
            pytest.fail('Changed source must not be consumed')
