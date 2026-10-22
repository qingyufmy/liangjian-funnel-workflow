"""Maintenance scans versus fail-closed cheap minute startup, zero HTTP."""
from datetime import datetime, timedelta, timezone
import json
import gc
import sqlite3

import pytest

from liangjian_funnel.runtime.integrity import marker_path, verify_integrity, verified_identity
from liangjian_funnel.runtime.shared_json import atomic_json
from liangjian_funnel.runtime.state import RuntimeStore, PersistenceBlockedError, PersistenceError


def marker(store):
    return json.loads(marker_path(store.path).read_bytes())


def test_new_database_is_verified_and_ordinary_reopen_does_not_change_schema(tmp_path):
    path = tmp_path / 'runtime.sqlite3'
    first = RuntimeStore(path)
    assert first.healthy and marker(first)['status'] == 'PASS'
    with sqlite3.connect(path) as db: before = db.execute('PRAGMA schema_version').fetchone()[0]
    second = RuntimeStore(path)
    assert second.healthy
    with sqlite3.connect(path) as db: assert db.execute('PRAGMA schema_version').fetchone()[0] == before


@pytest.mark.parametrize('condition', ['missing', 'expired', 'future', 'bad_json', 'schema_identity'])
def test_unverified_existing_store_never_initializes_or_becomes_writable(tmp_path, monkeypatch, condition):
    store = RuntimeStore(tmp_path / 'runtime.sqlite3')
    receipt = marker(store)
    if condition == 'missing': marker_path(store.path).unlink()
    elif condition == 'bad_json': marker_path(store.path).write_text('not-json')
    else:
        if condition == 'expired': receipt['checked_at'] = (datetime.now(timezone.utc)-timedelta(hours=37)).isoformat()
        elif condition == 'future': receipt['checked_at'] = (datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()
        else: receipt['identity']['schema_version'] += 1
        atomic_json(marker_path(store.path), receipt)
    initialized = []
    monkeypatch.setattr(RuntimeStore, '_initialize', lambda self: initialized.append(True))
    try:
        cold = RuntimeStore(store.path)
    except (PersistenceBlockedError, PersistenceError):
        pass
    else:
        assert not cold.healthy
        with pytest.raises(PersistenceBlockedError): cold.assert_writable()
    assert initialized == []


def test_existing_healthy_marker_skips_ddl_on_each_command_process(tmp_path, monkeypatch):
    store = RuntimeStore(tmp_path / 'runtime.sqlite3')
    def forbidden(self): pytest.fail('ordinary startup must not execute migrations')
    monkeypatch.setattr(RuntimeStore, '_initialize', forbidden)
    assert RuntimeStore(store.path).healthy


def test_marker_schema_format_is_checked_not_just_pass_label(tmp_path):
    store = RuntimeStore(tmp_path / 'runtime.sqlite3')
    receipt = marker(store); receipt['schema_version'] = 'invented-schema'
    atomic_json(marker_path(store.path), receipt)
    with sqlite3.connect(store.path) as db: assert not verified_identity(store.path, db)


def test_expiry_is_rechecked_for_a_long_lived_previously_healthy_store(tmp_path, monkeypatch):
    import liangjian_funnel.runtime.state as state_module
    import liangjian_funnel.runtime.integrity as integrity_module
    store = RuntimeStore(tmp_path / 'runtime.sqlite3')
    assert store.healthy
    future = datetime.now(timezone.utc) + timedelta(hours=37)
    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return future.astimezone(tz) if tz is not None else future.replace(tzinfo=None)
    # Local module test clocks only. No OS-clock or receipt mutation and no
    # requirement to reread a valid receipt before every ordinary write.
    monkeypatch.setattr(state_module, 'datetime', Later)
    monkeypatch.setattr(integrity_module, 'datetime', Later)
    with pytest.raises(PersistenceBlockedError): store.assert_writable()


def test_full_maintenance_validates_old_schema_then_migrates_then_marks_final_identity(tmp_path):
    path = tmp_path / 'legacy.sqlite3'
    with sqlite3.connect(path) as db: db.execute('CREATE TABLE controlled_old_source(v TEXT)')
    verify_integrity(path)
    before = json.loads(marker_path(path).read_bytes())['identity']['schema_version']
    updated = RuntimeStore(path, integrity_maintenance=True)
    assert updated.healthy
    assert marker(updated)['identity']['schema_version'] > before
    assert RuntimeStore(path).healthy


def test_failed_full_maintenance_does_not_run_any_migrations(tmp_path, monkeypatch):
    import liangjian_funnel.runtime.integrity as module
    path = tmp_path / 'runtime.sqlite3'
    RuntimeStore(path)
    initialized = []
    def rejected(*a, **k): raise ValueError('CONTROLLED_BAD_PAGES')
    monkeypatch.setattr(module, 'verify_integrity', rejected)
    monkeypatch.setattr(RuntimeStore, '_initialize', lambda self: initialized.append(True))
    with pytest.raises((ValueError, PersistenceError)):
        RuntimeStore(path, integrity_maintenance=True)
    assert initialized == []


def test_inode_replacement_cannot_reuse_an_old_valid_receipt(tmp_path):
    store = RuntimeStore(tmp_path / 'runtime.sqlite3')
    other = RuntimeStore(tmp_path / 'other.sqlite3')
    gc.collect()  # RuntimeStore sqlite context cycles must release Windows handles.
    # Both files remain allocated, so no inode can be recycled between them.
    store.path.rename(store.path.with_suffix('.old'))
    other.path.rename(store.path)
    with sqlite3.connect(store.path) as db: assert not verified_identity(store.path, db)


def test_readonly_transaction_does_not_make_fresh_verified_store_unhealthy(tmp_path):
    path = tmp_path / 'runtime.sqlite3'
    RuntimeStore(path)
    with sqlite3.connect(path.as_uri()+'?mode=ro', uri=True) as reader:
        reader.execute('PRAGMA query_only=ON'); reader.execute('BEGIN')
        reader.execute('SELECT name FROM sqlite_schema').fetchall()
        assert RuntimeStore(path).healthy
