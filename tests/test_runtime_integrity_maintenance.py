from datetime import datetime
import hashlib
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import sqlite3

import pytest

from liangjian_funnel.runtime import integrity_maintenance as subject
from liangjian_funnel.runtime.integrity import marker_path
from liangjian_funnel.runtime.state import RuntimeStore, PersistenceBlockedError


def clock(value):return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo('Asia/Shanghai'))


@pytest.fixture
def settings(tmp_path):
    path=tmp_path/'state/runtime.db';RuntimeStore(path)
    return SimpleNamespace(root=tmp_path,state_db_path=path)


def test_once_weekday_even_after_process_restart(settings,monkeypatch):
    at=clock('2026-10-26T06:50:00');calls=[];real=subject.verify_integrity
    def counted(path,**kwargs):calls.append(path);return real(path,**kwargs)
    monkeypatch.setattr(subject,'verify_integrity',counted)
    old=marker_path(settings.state_db_path).read_bytes()
    first=subject.run_integrity_maintenance(settings,now=at)
    second=subject.run_integrity_maintenance(settings,now=at.replace(hour=13))
    assert first['status']==second['status']=='PASS' and second['reused'] is True and len(calls)==1
    directory=settings.root/'state/runtime-integrity-maintenance/2026-10-26'
    assert (directory/'previous-receipt.json').read_bytes()==old
    assert first['previous_receipt_sha256']==hashlib.sha256(old).hexdigest()
    assert first['models_called']==0 and first['migrations_run'] is False


@pytest.mark.parametrize('at',['2026-10-25T07:00:00','2026-10-24T07:00:00','2026-10-26T06:49:59'])
def test_weekend_and_before_0650_are_noops(settings,monkeypatch,at):
    monkeypatch.setattr(subject,'verify_integrity',lambda _:pytest.fail('no scan due'))
    assert subject.run_integrity_maintenance(settings,now=clock(at))['status']=='NOT_DUE'
    assert not (settings.root/'state/runtime-integrity-maintenance').exists()


def test_real_failure_blocks_cached_writers_and_keeps_old_receipt(settings,monkeypatch):
    cached=RuntimeStore(settings.state_db_path);assert cached.healthy
    old=marker_path(settings.state_db_path).read_bytes()
    def failed(_,**kwargs):raise sqlite3.DatabaseError('controlled page failure')
    monkeypatch.setattr(subject,'verify_integrity',failed)
    at=clock('2026-10-26T06:50:00')
    result=subject.run_integrity_maintenance(settings,now=at)
    assert result['status']=='BLOCKED'
    assert json.loads(marker_path(settings.state_db_path).read_bytes())['status']=='FAIL'
    assert (settings.root/'state/runtime-integrity-maintenance/2026-10-26/previous-receipt.json').read_bytes()==old
    with pytest.raises(PersistenceBlockedError):cached.assert_writable()
    assert subject.run_integrity_maintenance(settings,now=at)['reused'] is True


def test_interrupted_attempt_is_not_pass_or_an_automatic_rescan(settings,monkeypatch):
    directory=settings.root/'state/runtime-integrity-maintenance/2026-10-26';directory.mkdir(parents=True)
    monkeypatch.setattr(subject,'verify_integrity',lambda _:pytest.fail('do not retry interrupted scan'))
    result=subject.run_integrity_maintenance(settings,now=clock('2026-10-26T07:03:00'))
    assert result['status']=='BLOCKED' and result['reason_code']=='INTEGRITY_ATTEMPT_INTERRUPTED_OR_UNREADABLE'


def test_same_day_pass_is_rejected_if_current_marker_was_removed(settings):
    at=clock('2026-10-26T06:50:00')
    assert subject.run_integrity_maintenance(settings,now=at)['status']=='PASS'
    marker_path(settings.state_db_path).unlink()
    assert subject.run_integrity_maintenance(settings,now=at)['status']=='BLOCKED'


@pytest.mark.parametrize('key,value',[('trade_date','2026-10-23'),('database_path','another-database'),('schema_version','invented')])
def test_foreign_or_wrong_day_receipt_cannot_suppress_verification(settings,key,value):
    at=clock('2026-10-26T06:50:00')
    subject.run_integrity_maintenance(settings,now=at)
    path=settings.root/'state/runtime-integrity-maintenance/2026-10-26/result.json'
    result=json.loads(path.read_bytes());result[key]=value
    subject.atomic_json(path,result)
    assert subject.run_integrity_maintenance(settings,now=at)['status']=='BLOCKED'


def test_cli_never_constructs_store_or_application(settings,monkeypatch,capsys):
    from liangjian_funnel import cli
    monkeypatch.setattr(cli,'RuntimeStore',lambda *a,**k:pytest.fail('no migrations/Store'))
    monkeypatch.setattr(cli,'WorkflowApplication',lambda *a,**k:pytest.fail('no Workflow/models'))
    monkeypatch.setattr(subject,'run_integrity_maintenance',lambda s,**kwargs:dict(status='PASS',models_called=0,migrations_run=False))
    assert cli.main(['run-runtime-integrity'],settings=settings)==0
    assert json.loads(capsys.readouterr().out)['models_called']==0
