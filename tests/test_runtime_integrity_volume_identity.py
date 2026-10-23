"""Stable volume identity without weakening file, schema, age or scan gates."""
from contextlib import closing
import gc
import json
import os
from pathlib import Path
import sqlite3
import stat
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime import integrity as subject
from liangjian_funnel.runtime.state import RuntimeStore


UUID='120aebe1-8c98-412b-a490-3797cae5656b'


def receipt(path):
    return json.loads(subject.marker_path(path).read_bytes())


def set_device(monkeypatch, path, device):
    original=Path.stat
    def changed(self, *args, **kwargs):
        observed=original(self, *args, **kwargs)
        if self==path:
            fields=list(observed);fields[2]=device
            return os.stat_result(fields)
        return observed
    monkeypatch.setattr(Path, 'stat', changed)


def stable_volume(monkeypatch):
    monkeypatch.setattr(subject, '_volume_identity', lambda _:dict(kind='filesystem_uuid',uuid=UUID))


def test_same_volume_and_file_survive_kernel_device_renumbering(tmp_path, monkeypatch):
    stable_volume(monkeypatch)
    path=tmp_path/'state.db';RuntimeStore(path)
    before=subject.marker_path(path).read_bytes()
    assert receipt(path)['schema_version']=='runtime-integrity/2'
    set_device(monkeypatch,path,path.stat().st_dev+16)
    with closing(sqlite3.connect(path)) as db:
        assert subject.verified_identity(path,db)
    assert RuntimeStore(path).healthy
    assert subject.marker_path(path).read_bytes()==before


def test_v1_stays_strict_and_does_not_auto_migrate(tmp_path, monkeypatch):
    path=tmp_path/'state.db';RuntimeStore(path)
    with closing(sqlite3.connect(path)) as db:
        old=receipt(path);old['schema_version']='runtime-integrity/1'
        old['identity']=subject.identity(path,db)
        subject.atomic_json(subject.marker_path(path),old)
        assert subject.verified_identity(path,db)
        before=subject.marker_path(path).read_bytes()
        stable_volume(monkeypatch)
        set_device(monkeypatch,path,path.stat().st_dev+16)
        assert not subject.verified_identity(path,db)
        assert subject.marker_path(path).read_bytes()==before


@pytest.mark.parametrize('change',['uuid','unavailable_uuid','inode','path','state_contract','schema_version'])
def test_stable_receipt_still_rejects_changed_identity(tmp_path, monkeypatch, change):
    stable_volume(monkeypatch)
    path=tmp_path/'state.db';RuntimeStore(path)
    if change=='uuid':
        monkeypatch.setattr(subject,'_volume_identity',lambda _:dict(kind='filesystem_uuid',uuid='other-volume'))
    elif change=='unavailable_uuid':
        monkeypatch.setattr(subject,'_volume_identity',lambda d:dict(kind='strict_device',device=d))
    else:
        current=receipt(path)
        current['identity'][change]=current['identity'][change]+1 if change in ('inode','schema_version') else 'changed'
        subject.atomic_json(subject.marker_path(path),current)
    with closing(sqlite3.connect(path)) as db:
        assert not subject.verified_identity(path,db)


def test_no_uuid_fallback_retains_device_gate(tmp_path, monkeypatch):
    monkeypatch.setattr(subject,'_volume_identity',lambda d:dict(kind='strict_device',device=d))
    path=tmp_path/'state.db';RuntimeStore(path)
    assert receipt(path)['identity']['volume']['kind']=='strict_device'
    set_device(monkeypatch,path,path.stat().st_dev+16)
    with closing(sqlite3.connect(path)) as db:
        assert not subject.verified_identity(path,db)


def test_connection_for_another_database_cannot_certify_the_path(tmp_path, monkeypatch):
    stable_volume(monkeypatch)
    path=tmp_path/'state.db';RuntimeStore(path)
    other=tmp_path/'other.db';RuntimeStore(other)
    with closing(sqlite3.connect(other)) as db:
        assert not subject.verified_identity(path,db)


def test_real_file_replacement_still_fails_with_stable_uuid(tmp_path, monkeypatch):
    stable_volume(monkeypatch)
    path=tmp_path/'state.db';RuntimeStore(path)
    other=tmp_path/'other.db';RuntimeStore(other)
    gc.collect()
    path.rename(tmp_path/'original.db');other.rename(path)
    with closing(sqlite3.connect(path)) as db:
        assert not subject.verified_identity(path,db)


def test_full_scan_failure_preserves_old_receipt(tmp_path):
    path=tmp_path/'state.db'
    # No WAL: a valid WAL can legitimately recover a damaged main-file header.
    with closing(sqlite3.connect(path)) as db:
        db.execute('CREATE TABLE controlled(v INTEGER)');db.commit()
    subject.verify_integrity(path)
    original=subject.marker_path(path).read_bytes()
    gc.collect()
    with path.open('r+b') as stream:
        stream.seek(0);stream.write(b'controlled invalid sqlite header!')
    with pytest.raises(sqlite3.DatabaseError):
        subject.verify_integrity(path)
    assert subject.marker_path(path).read_bytes()==original


def test_file_change_during_verification_preserves_old_receipt(tmp_path, monkeypatch):
    path=tmp_path/'state.db';RuntimeStore(path)
    original=subject.marker_path(path).read_bytes();actual=subject._file_identity(path)
    calls=iter([actual,actual,(actual[0],actual[1]+1)])
    monkeypatch.setattr(subject,'_file_identity',lambda _:next(calls))
    with pytest.raises(ValueError,match='DATABASE_CHANGED_DURING_VERIFICATION'):
        subject.verify_integrity(path)
    assert subject.marker_path(path).read_bytes()==original


def test_ordinary_reopen_never_runs_full_scan(tmp_path, monkeypatch):
    stable_volume(monkeypatch)
    path=tmp_path/'state.db';RuntimeStore(path)
    def forbidden(*a,**k):
        pytest.fail('ordinary startup must not scan or renew the receipt')
    monkeypatch.setattr(subject,'verify_integrity',forbidden)
    assert RuntimeStore(path).healthy


@pytest.mark.parametrize('names,expected',[
    ([],dict(kind='strict_device',device=2049)),
    (['same-volume'],dict(kind='filesystem_uuid',uuid='same-volume')),
])
def test_uuid_discovery_matches_block_device_not_filename(monkeypatch,names,expected):
    entries=[SimpleNamespace(name=n,stat=lambda:SimpleNamespace(st_mode=stat.S_IFBLK,st_rdev=2049)) for n in names]
    entries.append(SimpleNamespace(name='unrelated',stat=lambda:SimpleNamespace(st_mode=stat.S_IFBLK,st_rdev=9999)))
    entries.append(SimpleNamespace(name='not-block',stat=lambda:SimpleNamespace(st_mode=stat.S_IFREG,st_rdev=2049)))
    monkeypatch.setattr(subject,'Path',lambda _:SimpleNamespace(iterdir=lambda:iter(entries)))
    assert subject._volume_identity(2049)==expected


def test_ambiguous_uuid_never_falls_back_to_weaker_identity(monkeypatch):
    entries=[SimpleNamespace(name=n,stat=lambda:SimpleNamespace(st_mode=stat.S_IFBLK,st_rdev=2049)) for n in ('one','two')]
    monkeypatch.setattr(subject,'Path',lambda _:SimpleNamespace(iterdir=lambda:iter(entries)))
    with pytest.raises(ValueError,match='VOLUME_IDENTITY_AMBIGUOUS'):
        subject._volume_identity(2049)
