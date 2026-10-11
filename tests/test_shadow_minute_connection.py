"""Connection lifecycle counterexamples; isolated databases and clocks only."""
from datetime import timedelta
import importlib.util
from pathlib import Path
import sqlite3

import pytest

from test_shadow_session import AT, session, databases, module


def test_connection_persists_with_no_idle_transaction_and_refreshes(tmp_path):
    state, minute = databases(tmp_path)
    source = module().ReadOnlyShadowSource(state, minute, lanes=['lane_1'])
    source.ensure_minute_ready()
    connection = source._minute_connection
    for _ in range(2):
        with source.minute_transaction() as db:
            assert db is connection
            assert db.in_transaction
            assert db.execute('SELECT count(*) FROM minute_decision_snapshots').fetchone()[0] == 2
        assert not connection.in_transaction
    with sqlite3.connect(minute) as writer:
        writer.execute('DELETE FROM minute_decision_snapshots')
    with source.minute_transaction() as db:
        assert db.execute('SELECT count(*) FROM minute_decision_snapshots').fetchone()[0] == 0
    source.close()
    source.close()
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute('SELECT 1')


def test_validation_exception_releases_transaction_without_losing_connection(tmp_path):
    state, minute = databases(tmp_path)
    source = module().ReadOnlyShadowSource(state, minute, lanes=['lane_1'])
    source.ensure_minute_ready()
    db = source._minute_connection
    with pytest.raises(ValueError, match='FIXTURE_VALIDATION'):
        with source.minute_transaction():
            raise ValueError('FIXTURE_VALIDATION')
    assert source._minute_connection is db and not db.in_transaction
    source.close()


def test_sqlite_fault_drops_connection_and_can_reopen(tmp_path):
    state, minute = databases(tmp_path)
    source = module().ReadOnlyShadowSource(state, minute, lanes=['lane_1'])
    source.ensure_minute_ready()
    source._minute_connection.close()
    with pytest.raises(module().MinuteSourceUnavailable):
        with source.minute_transaction():
            pass
    assert source._minute_connection is None
    source.ensure_minute_ready()
    source.close()


def install_window(service, monkeypatch, *, available):
    clock = [0.]
    service.clock = lambda: clock[0]
    opened = []
    original = service.source.ensure_minute_ready
    def ensure():
        if service.source._minute_connection is not None:
            return original()
        opened.append(clock[0])
        if not available(clock[0]):
            raise module().MinuteSourceUnavailable()
        return original()
    monkeypatch.setattr(service.source, 'ensure_minute_ready', ensure)
    return clock, opened


def test_half_second_retry_catches_first_one_second_window(tmp_path, monkeypatch):
    service, engine, ledger, *_ = session(tmp_path)
    clock, opened = install_window(service, monkeypatch, available=lambda t: 1 <= t % 5 < 2)
    service.wall_clock = lambda: AT+timedelta(seconds=clock[0]+1)
    first = service.poll(observed_at=AT+timedelta(seconds=1))
    assert first['gap_codes'] == ['MINUTE_DB_WAL_NOT_PRESENT'] and not engine.calls
    clock[0] = .49
    service.poll(observed_at=AT+timedelta(seconds=1.49))
    assert opened == [0.]
    clock[0] = .5
    service.poll(observed_at=AT+timedelta(seconds=1.5))
    clock[0] = 1
    assert service.poll(observed_at=AT+timedelta(seconds=2))['scope_status'] == 'COMPLETE'
    assert opened == [0., .5, 1.] and len(engine.calls) == 1
    assert all(not 1 <= t % 5 < 2 for t in range(0, 21, 5))  # old cadence counterexample
    service.close()


def test_recovery_does_not_replay_a_finished_minute_or_reset_loss_clock(tmp_path, monkeypatch):
    service, engine, ledger, *_ = session(tmp_path)
    clock = [0.]
    service.clock = lambda: clock[0]
    service.poll(observed_at=AT+timedelta(seconds=1))
    assert len(engine.calls) == 1
    service.source.close()
    def broken(): raise module().MinuteSourceUnavailable()
    monkeypatch.setattr(service.source, 'ensure_minute_ready', broken)
    clock[0] = 10
    service.poll(observed_at=AT+timedelta(seconds=11))
    clock[0] = 129.5
    service.wall_clock = lambda: AT+timedelta(seconds=130.5)
    assert service.poll(observed_at=AT+timedelta(seconds=130.5))['status'] != 'SHADOW_MINUTE_SOURCE_LOST'
    clock[0] = 130
    terminal = service.poll(observed_at=AT+timedelta(seconds=131))
    assert terminal['status'] == 'SHADOW_MINUTE_SOURCE_LOST'
    assert service.termination_reason == 'SHADOW_MINUTE_SOURCE_LOST'
    assert len(engine.calls) == 1 and len(ledger.minutes) == 1
    service.close()


def test_successful_reconnect_resets_continuous_loss_and_no_historical_catchup(tmp_path, monkeypatch):
    service, engine, ledger, *_ = session(tmp_path)
    clock = [0.]; service.clock = lambda: clock[0]
    service.poll(observed_at=AT+timedelta(seconds=1))
    original = service.source.ensure_minute_ready
    service.source.close()
    failures = [True]
    def ensure():
        if failures[0]: raise module().MinuteSourceUnavailable()
        original()
    monkeypatch.setattr(service.source, 'ensure_minute_ready', ensure)
    clock[0] = 1; service.poll(observed_at=AT+timedelta(seconds=2))
    failures[0] = False
    clock[0] = 1.5
    assert service.poll(observed_at=AT+timedelta(seconds=2.5))['status'] == 'DUPLICATE'
    assert service._minute_lost_since is None and len(engine.calls) == 1
    assert not service.source._minute_connection.in_transaction
    service.close()


def test_initial_unready_does_not_claim_success_or_exit_as_recovered_loss(tmp_path, monkeypatch):
    service, engine, ledger, *_ = session(tmp_path)
    clock, _ = install_window(service, monkeypatch, available=lambda _: False)
    for value in (0, 120, 240):
        clock[0] = value
        receipt = service.poll(observed_at=AT+timedelta(seconds=value+1))
        assert receipt['status'] == 'DATA_LIMITED'
        assert receipt['gap_codes'] == ['MINUTE_DB_WAL_NOT_PRESENT']
    assert not engine.calls and not ledger.minutes and service.termination_reason is None
    service.close()


def test_real_read_round_sqlite_fault_returns_to_retry_without_strategy_execution(tmp_path, monkeypatch):
    service, engine, ledger, *_ = session(tmp_path)
    ticks = [0.]; service.clock = lambda: ticks[0]
    original = service.source.read_completion
    def completion(*args, **kwargs):
        service.source._minute_connection.close()
        return original(*args, **kwargs)
    monkeypatch.setattr(service.source, 'read_completion', completion)
    result = service.poll(observed_at=AT+timedelta(seconds=1))
    assert result['gap_codes'] == ['MINUTE_DB_WAL_NOT_PRESENT']
    assert service.source._minute_connection is None and service._minute_lost_since == 0
    assert not engine.calls and not ledger.minutes
    monkeypatch.setattr(service.source, 'read_completion', original)
    ticks[0] = .5
    assert service.poll(observed_at=AT+timedelta(seconds=1.5))['status'] == 'DUPLICATE'
    assert service._minute_lost_since is None and not engine.calls
    service.close()


@pytest.mark.parametrize('fault', [False, True, 'exception'])
def test_cli_finally_closes_real_source_and_loss_exits_nonzero(tmp_path, monkeypatch, fault):
    path = Path(__file__).resolve().parents[1]/'scripts/run_shadow_session.py'
    spec = importlib.util.spec_from_file_location('_minute_lifecycle_cli', path)
    cli = importlib.util.module_from_spec(spec); spec.loader.exec_module(cli)
    state, minute = databases(tmp_path)
    source = module().ReadOnlyShadowSource(state, minute, lanes=['lane_1'])
    source.ensure_minute_ready(); connection = source._minute_connection
    monkeypatch.setattr(cli, 'ReadOnlyShadowSource', lambda *a, **k: source)
    class Service:
        def poll(self, **kwargs):
            if fault == 'exception': raise RuntimeError('isolated fixture')
            if fault: return {'status': 'SHADOW_MINUTE_SOURCE_LOST', 'gap_codes': ['SHADOW_MINUTE_SOURCE_LOST']}
            raise KeyboardInterrupt
    monkeypatch.setattr(cli, 'ShadowSession', lambda *a, **k: Service())
    args = ['--state-db',str(state),'--minute-db',str(minute),'--lane','lane_1',
        '--shadow-db',str(tmp_path/'shadow.db'),'--shadow-jsonl',str(tmp_path/'shadow.jsonl'),
        '--receipt-jsonl',str(tmp_path/'receipts.jsonl'),'--duration-seconds','1']
    assert cli.main(args) == (2 if fault == 'exception' else 3 if fault else 130)
    with pytest.raises(sqlite3.ProgrammingError): connection.execute('SELECT 1')
