"""All source databases, isolated fixtures, no production writes or providers."""
from datetime import timedelta
import sqlite3
from contextlib import closing

import pytest

from test_shadow_session import AT, session, databases, module


def test_state_connection_is_persistent_and_has_no_idle_snapshot(tmp_path):
    state, minute = databases(tmp_path)
    source = module().ReadOnlyShadowSource(state, minute, lanes=['lane_1'])
    source.ensure_sources_ready()
    connection = source._state_connection
    with source.state_transaction() as db:
        assert db is connection and db.in_transaction
        assert db.execute('SELECT count(*) FROM execution_plans').fetchone()[0] == 1
    assert not connection.in_transaction
    with sqlite3.connect(state) as writer:
        writer.execute('DELETE FROM execution_plans')
    with source.state_transaction() as db:
        assert db is connection
        assert db.execute('SELECT count(*) FROM execution_plans').fetchone()[0] == 0
    source.close()
    with pytest.raises(sqlite3.ProgrammingError): connection.execute('SELECT 1')


def test_state_not_ready_never_evaluates_partial_scope(tmp_path, monkeypatch):
    service, engine, ledger, *_ = session(tmp_path)
    from liangjian_funnel.runtime.shadow_sqlite_source import SourceUnavailable
    def broken():
        raise SourceUnavailable('SOURCE_STATE_OPEN_FAILED', 'state', 'open',
                                sqlite3.OperationalError('unable to open database file'))
    monkeypatch.setattr(service.source, 'ensure_state_ready', broken)
    receipt = service.poll(observed_at=AT+timedelta(seconds=1))
    assert receipt['gap_codes'] == ['SOURCE_STATE_OPEN_FAILED']
    assert receipt['source_error']['database'] == 'state'
    assert receipt['source_error']['exception_class'] == 'OperationalError'
    assert not engine.calls and not ledger.minutes
    service.close()


def test_initial_source_loss_is_bounded_to_120_seconds(tmp_path, monkeypatch):
    service, engine, ledger, *_ = session(tmp_path)
    from liangjian_funnel.runtime.shadow_sqlite_source import SourceUnavailable
    ticks = [0.]
    service.clock = lambda: ticks[0]
    def broken():
        raise SourceUnavailable('SOURCE_STATE_OPEN_FAILED', 'state', 'open',
                                sqlite3.OperationalError('unable to open database file'))
    monkeypatch.setattr(service.source, 'ensure_state_ready', broken)
    service.poll(observed_at=AT+timedelta(seconds=1))
    ticks[0] = 119.5
    assert service.poll(observed_at=AT+timedelta(seconds=120.5))['status'] != 'SHADOW_MINUTE_SOURCE_LOST'
    ticks[0] = 120.
    assert service.poll(observed_at=AT+timedelta(seconds=121))['status'] == 'SHADOW_MINUTE_SOURCE_LOST'
    assert not engine.calls and not ledger.minutes


def test_fact_cold_read_does_not_create_wal_and_rejects_writer_start(tmp_path):
    from liangjian_funnel.runtime.shadow_preopen_sidecar import ReadOnlyLocalFactCache
    from liangjian_funnel.runtime.shadow_sqlite_source import SourceUnavailable
    path = tmp_path/'facts.sqlite3'
    with closing(sqlite3.connect(path)) as writer:
        with writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('CREATE TABLE fixture(value INTEGER)')
            writer.execute('INSERT INTO fixture VALUES(1)')
    cache = ReadOnlyLocalFactCache(path)
    assert not path.with_name(path.name+'-wal').exists()
    with cache._connect() as reader:
        assert reader.execute('SELECT value FROM fixture').fetchone()[0] == 1
    assert not path.with_name(path.name+'-wal').exists()
    writer = None
    try:
        with pytest.raises(SourceUnavailable, match='FACT_DB_CHANGED_DURING_READ'):
            with cache._connect() as reader:
                reader.execute('SELECT value FROM fixture').fetchone()
                writer = sqlite3.connect(path)
                writer.execute('INSERT INTO fixture VALUES(2)')
                writer.commit()
    finally:
        if writer is not None: writer.close()


def test_fact_warm_read_uses_wal_and_sees_committed_rows(tmp_path):
    from liangjian_funnel.runtime.shadow_preopen_sidecar import ReadOnlyLocalFactCache
    path = tmp_path/'facts.sqlite3'
    writer = sqlite3.connect(path)
    try:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('CREATE TABLE fixture(value INTEGER)')
        writer.execute('INSERT INTO fixture VALUES(7)'); writer.commit()
        with ReadOnlyLocalFactCache(path)._connect() as reader:
            assert reader.execute('SELECT value FROM fixture').fetchone()[0] == 7
    finally: writer.close()


@pytest.mark.parametrize('state_warm,minute_warm', [(False,False),(False,True),(True,False),(True,True)])
def test_full_read_round_cold_warm_combinations_preserve_all_bindings(tmp_path,state_warm,minute_warm):
    service, engine, ledger, state, minute = session(tmp_path)
    writers = []
    try:
        for path, warm in ((state,state_warm),(minute,minute_warm)):
            db = sqlite3.connect(path)
            db.execute('PRAGMA journal_mode=WAL')
            if warm: writers.append(db)
            else: db.close()
        receipt = service.poll(observed_at=AT+timedelta(seconds=1))
        assert receipt['scope_status'] == 'COMPLETE'
        assert receipt['production_completion']['minute_snapshot_id'] == 'snap-current'
        assert set(receipt['source_bindings']) == {'p0'}
        assert receipt['plan_window_census']['missing_plan_ids'] == []
        assert receipt['source_bindings']['p0']['minute_snapshot_id'] == 'snap-current'
        assert not service.source._state_connection.in_transaction
        assert not service.source._minute_connection.in_transaction
    finally:
        service.close()
        for db in writers: db.close()


def test_databases_ready_in_different_minutes_do_not_use_partial_or_catch_up(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.shadow_sqlite_source import SourceUnavailable
    service, engine, ledger, *_ = session(tmp_path)
    ticks = [0.]; service.clock = lambda: ticks[0]
    service.wall_clock = lambda: AT+timedelta(seconds=ticks[0]+1)
    original = service.source.ensure_state_ready
    def delayed():
        if ticks[0] < 60:
            raise SourceUnavailable('SOURCE_STATE_OPEN_FAILED','state','open',sqlite3.OperationalError('cold'))
        original()
    monkeypatch.setattr(service.source,'ensure_state_ready',delayed)
    for tick in (0.,.49,.5,59.5):
        ticks[0] = tick
        receipt = service.poll(observed_at=AT+timedelta(seconds=tick+1))
        assert receipt['scope_status'] == 'INCOMPLETE'
    assert not engine.calls and not ledger.minutes
    ticks[0] = 60
    receipt = service.poll(observed_at=AT+timedelta(seconds=61))
    assert not engine.calls and not ledger.minutes  # old completion is not replayed
    assert receipt['gap_codes'] == ['PRODUCTION_COMPLETION_PENDING']
    service.close()


def test_state_read_fault_reconnects_without_retaining_read_transaction(tmp_path):
    from liangjian_funnel.runtime.shadow_sqlite_source import SourceUnavailable
    state, minute = databases(tmp_path)
    source = module().ReadOnlyShadowSource(state,minute,lanes=['lane_1'])
    source.ensure_sources_ready()
    source._state_connection.close()
    with pytest.raises(SourceUnavailable,match='SOURCE_STATE_OPEN_FAILED'):
        with source.state_transaction(): pass
    assert source._state_connection is None
    source.ensure_sources_ready()
    with source.state_transaction() as db:
        assert db.execute('SELECT count(*) FROM monitor_events').fetchone()[0] == 1
    assert not db.in_transaction
    source.close()


def test_exception_messages_preserve_class_but_remove_secret_and_path():
    from liangjian_funnel.runtime.shadow_sqlite_source import SourceUnavailable
    exc = SourceUnavailable('FACT_DB_OPEN_FAILED','fact','open',sqlite3.OperationalError(
        'unable to open /private/database.db token=FAKE_TEST_SECRET https://example.test/secret'))
    assert exc.diagnostic['exception_class'] == 'OperationalError'
    assert 'unable to open' in exc.diagnostic['message']
    assert 'FAKE_TEST_SECRET' not in exc.diagnostic['message']
    assert '/private' not in exc.diagnostic['message'] and 'https://' not in exc.diagnostic['message']


def test_bounded_state_open_retry_cadence_and_timeout(tmp_path,monkeypatch):
    from liangjian_funnel.runtime import shadow_sqlite_source as mod
    ticks=[0.]; attempts=[]
    def fail(_):
        attempts.append(ticks[0])
        raise sqlite3.OperationalError('unable to open database file')
    monkeypatch.setattr(mod,'open_live_readonly',fail)
    with pytest.raises(mod.SourceUnavailable,match='SOURCE_STATE_OPEN_FAILED'):
        with mod.bounded_live_transaction(tmp_path/'fixture',database='state',
                clock=lambda:ticks[0],sleep=lambda delay:ticks.__setitem__(0,ticks[0]+delay)):
            raise AssertionError('partial source read')
    assert attempts[:3] == [0,.5,1.] and attempts[-1] == 120


def test_fact_read_failure_is_not_reported_as_empty_daily_data(tmp_path,monkeypatch):
    import importlib.util, json
    from pathlib import Path
    from test_w1_shadow_preopen_sidecar import row, GENERATED, TARGET
    from liangjian_funnel.runtime.shadow_sqlite_source import SourceUnavailable
    state, fact = tmp_path/'state.db',tmp_path/'fact.db'
    original = row()
    with closing(sqlite3.connect(state)) as db:
        with db:
            db.execute('CREATE TABLE execution_plans('+','.join(k+' TEXT' for k in original)+')')
            db.execute('INSERT INTO execution_plans VALUES('+','.join('?' for _ in original)+')',tuple(original.values()))
    fact.touch()
    spec=importlib.util.spec_from_file_location('b1_sidecar_cli',Path(__file__).resolve().parents[1]/'scripts/run_shadow_preopen_sidecar.py')
    cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
    def fail(*args,**kw):
        raise SourceUnavailable('FACT_DB_CHANGED_DURING_READ','fact','identity_check',sqlite3.OperationalError('writer started'))
    monkeypatch.setattr(cli,'generate_from_local_cache',fail)
    code=cli.main(['--state-db',str(state),'--fact-db',str(fact),'--preopen-inputs-db',str(tmp_path/'sidecar.db'),
        '--receipt-json',str(tmp_path/'receipt.json'),'--target-trade-date',TARGET.isoformat(),'--lane','lane_1'],clock=lambda:GENERATED)
    receipt=json.loads((tmp_path/'receipt.json').read_bytes())
    assert code == 2 and receipt['selected_plan_count'] == 1 and receipt['available_count'] == 0
    assert receipt['records'][0]['reason_codes'] == ['FACT_DB_CHANGED_DURING_READ']
    assert receipt['records'][0]['source_error']['database'] == 'fact'
    assert 'DAILY_SOURCE_INVALID' not in json.dumps(receipt)
