"""Real full SQLite checks with deterministic local clocks, no sleeping or HTTP."""
from contextlib import closing
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime import integrity as subject
from liangjian_funnel.runtime import integrity_maintenance as maintenance
from liangjian_funnel.runtime.state import RuntimeStore


class Clock:
    value=0.0
    def now(self):return self.value
    def advance(self,seconds):self.value+=seconds


def database(tmp_path,rows=4):
    path=tmp_path/'runtime.db'
    with closing(sqlite3.connect(path)) as db:
        db.execute('CREATE TABLE controlled(id INTEGER PRIMARY KEY,value TEXT)')
        db.executemany('INSERT INTO controlled VALUES (?,?)',((i,str(i)) for i in range(rows)))
        db.commit()
    subject.verify_integrity(path)
    return path


def local_clock(monkeypatch):
    clock=Clock();monkeypatch.setattr(subject.time,'monotonic',clock.now)
    return clock


def test_explicit_deadline_allows_real_full_scan_after_legacy180(tmp_path,monkeypatch):
    path=database(tmp_path);clock=local_clock(monkeypatch);real=subject._stable_identity;calls=[]
    def slow_identity(p,db):
        calls.append(True)
        if len(calls)==1:clock.advance(181)
        return real(p,db)
    monkeypatch.setattr(subject,'_stable_identity',slow_identity)
    result=subject.verify_integrity(path,deadline=300)
    assert result['status']=='PASS' and result['elapsed_ms']==181000 and len(calls)==2
    with closing(sqlite3.connect(path)) as db:assert subject.verified_identity(path,db)


def test_ordinary_default180_is_not_implicitly_expanded(tmp_path,monkeypatch):
    path=database(tmp_path);old=subject.marker_path(path).read_bytes();clock=local_clock(monkeypatch)
    real=subject._stable_identity
    def slow_identity(p,db):clock.advance(181);return real(p,db)
    monkeypatch.setattr(subject,'_stable_identity',slow_identity)
    with pytest.raises(subject.IntegrityDeadlineExceeded,match='RUNTIME_INTEGRITY_DEADLINE_EXCEEDED'):
        subject.verify_integrity(path)
    assert subject.marker_path(path).read_bytes()==old


def test_real_sqlite_progress_interrupt_has_explicit_timeout_and_no_new_pass(tmp_path,monkeypatch):
    path=database(tmp_path,rows=20000);old=subject.marker_path(path).read_bytes();clock=local_clock(monkeypatch)
    real_connect=subject.sqlite3.connect;callbacks=[];statements=[]
    class ObservedConnection:
        def __init__(self,*a,**k):self.connection=real_connect(*a,**k)
        def __enter__(self):return self
        def __exit__(self,*args):self.connection.__exit__(*args);self.connection.close()
        def execute(self,sql,*args):statements.append(sql);return self.connection.execute(sql,*args)
        def set_progress_handler(self,callback,interval):
            assert interval==10000
            def advance_and_check():
                callbacks.append(True);clock.advance(301);return callback()
            return self.connection.set_progress_handler(advance_and_check,interval)
    monkeypatch.setattr(subject.sqlite3,'connect',ObservedConnection)
    with pytest.raises(subject.IntegrityDeadlineExceeded,match='RUNTIME_INTEGRITY_DEADLINE_EXCEEDED'):
        subject.verify_integrity(path,deadline=300)
    assert callbacks and 'PRAGMA quick_check' in statements and 'PRAGMA quick_check(1)' not in statements
    assert subject.marker_path(path).read_bytes()==old


@pytest.mark.parametrize('boundary',['after_scan','after_identity','before_marker'])
def test_deadline_is_rechecked_after_scan_identity_and_before_pass(tmp_path,monkeypatch,boundary):
    path=database(tmp_path);old=subject.marker_path(path).read_bytes();clock=local_clock(monkeypatch)
    if boundary=='after_scan':
        real=subject.sqlite3.connect
        class LateScan:
            def __init__(self,*a,**k):self.db=real(*a,**k)
            def __enter__(self):return self
            def __exit__(self,*args):self.db.__exit__(*args);self.db.close()
            def set_progress_handler(self,*args):return self.db.set_progress_handler(*args)
            def execute(self,sql,*args):
                value=self.db.execute(sql,*args)
                if sql=='PRAGMA quick_check':clock.advance(301)
                return value
        monkeypatch.setattr(subject.sqlite3,'connect',LateScan)
    elif boundary=='after_identity':
        real=subject._stable_identity;calls=[]
        def late_identity(*args):
            result=real(*args);calls.append(True)
            if len(calls)==2:clock.advance(301)
            return result
        monkeypatch.setattr(subject,'_stable_identity',late_identity)
    else:
        real=subject._file_identity;calls=[]
        def late_file(*args):
            result=real(*args);calls.append(True)
            if len(calls)==4:clock.advance(301)
            return result
        monkeypatch.setattr(subject,'_file_identity',late_file)
    with pytest.raises(subject.IntegrityDeadlineExceeded):subject.verify_integrity(path,deadline=300)
    assert subject.marker_path(path).read_bytes()==old


def test_two_scans_and_migration_share_one_nonrenewable_deadline(tmp_path,monkeypatch):
    path=database(tmp_path);clock=local_clock(monkeypatch);stages=[]
    def scan(_,*,deadline):
        stages.append(('scan',deadline,clock.now()));clock.advance(200 if len(stages)==1 else 51)
        subject.check_integrity_deadline(deadline)
    def migrate(_):stages.append(('migration',clock.now()));clock.advance(50)
    monkeypatch.setattr(subject,'verify_integrity',scan);monkeypatch.setattr(RuntimeStore,'_initialize',migrate)
    with pytest.raises(subject.IntegrityDeadlineExceeded):RuntimeStore(path,integrity_maintenance=True,integrity_deadline=300)
    assert stages==[('scan',300,0),('migration',200),('scan',300,250)]


def test_first_scan_failure_never_migrates(tmp_path,monkeypatch):
    path=database(tmp_path);local_clock(monkeypatch);migrated=[]
    def fail(*a,**k):raise subject.IntegrityDeadlineExceeded()
    monkeypatch.setattr(subject,'verify_integrity',fail)
    monkeypatch.setattr(RuntimeStore,'_initialize',lambda _:migrated.append(True))
    with pytest.raises(subject.IntegrityDeadlineExceeded):RuntimeStore(path,integrity_maintenance=True,integrity_deadline=300)
    assert not migrated


def test_migration_time_also_consumes_budget_before_second_scan(tmp_path,monkeypatch):
    path=database(tmp_path);clock=local_clock(monkeypatch);scans=[]
    monkeypatch.setattr(subject,'verify_integrity',lambda *a,**k:scans.append(k['deadline']))
    monkeypatch.setattr(RuntimeStore,'_initialize',lambda _:clock.advance(301))
    with pytest.raises(subject.IntegrityDeadlineExceeded):RuntimeStore(path,integrity_maintenance=True,integrity_deadline=300)
    assert scans==[300]


def test_second_scan_failure_never_prints_cli_pass(tmp_path,monkeypatch,capsys):
    from liangjian_funnel import cli
    path=database(tmp_path);local_clock(monkeypatch);scans=[]
    def scan(*a,**k):
        scans.append(k['deadline'])
        if len(scans)==2:raise subject.IntegrityDeadlineExceeded()
    monkeypatch.setattr(subject,'verify_integrity',scan)
    with pytest.raises(subject.IntegrityDeadlineExceeded):
        cli.main(['verify-runtime-integrity','--timeout-seconds','300'],settings=SimpleNamespace(state_db_path=path))
    assert scans==[300,300] and capsys.readouterr().out==''


def test_ordinary_store_cannot_receive_maintenance_deadline(tmp_path):
    with pytest.raises(ValueError,match='INTEGRITY_DEADLINE_MAINTENANCE_ONLY'):
        RuntimeStore(tmp_path/'not-created/runtime.db',integrity_deadline=300)
    assert not (tmp_path/'not-created').exists()


@pytest.mark.parametrize('command,maximum',[('verify-runtime-integrity',1200),('run-runtime-integrity',540)])
def test_cli_timeout_defaults_and_explicit_bounds(command,maximum):
    from liangjian_funnel.cli import build_parser
    assert build_parser().parse_args([command]).timeout_seconds==maximum
    assert build_parser().parse_args([command,'--timeout-seconds','1']).timeout_seconds==1
    assert build_parser().parse_args([command,'--timeout-seconds',str(maximum)]).timeout_seconds==maximum
    for invalid in ('0',str(maximum+1),'-1','1.5','nan','inf','True',' 3'):
        with pytest.raises(SystemExit):build_parser().parse_args([command,'--timeout-seconds',invalid])


def test_cli_deployment_passes_one_absolute_deadline(tmp_path,monkeypatch,capsys):
    from liangjian_funnel import cli
    path=database(tmp_path);clock=local_clock(monkeypatch);clock.advance(10);received=[]
    def store(p,**kwargs):received.append((p,kwargs));return SimpleNamespace(persistence_failed=False)
    monkeypatch.setattr(cli,'RuntimeStore',store)
    assert cli.main(['verify-runtime-integrity'],settings=SimpleNamespace(state_db_path=path))==0
    assert received==[(path,dict(integrity_maintenance=True,integrity_deadline=1210))]
    assert json.loads(capsys.readouterr().out)['status']=='PASS'


def test_maintenance_uses540_and_preserves_fail_receipt_once(tmp_path,monkeypatch):
    path=database(tmp_path);old=subject.marker_path(path).read_bytes();clock=local_clock(monkeypatch);seen=[]
    def over_budget(p,*,deadline):
        seen.append((p,deadline));clock.advance(541);subject.check_integrity_deadline(deadline)
    monkeypatch.setattr(maintenance,'verify_integrity',over_budget)
    settings=SimpleNamespace(root=tmp_path,state_db_path=path)
    at=datetime(2026,10,26,6,50,tzinfo=ZoneInfo('Asia/Shanghai'))
    first=maintenance.run_integrity_maintenance(settings,now=at)
    assert first['status']=='BLOCKED' and first['reason_code']=='RUNTIME_INTEGRITY_DEADLINE_EXCEEDED'
    assert seen==[(path,540)]
    assert json.loads(subject.marker_path(path).read_bytes())['status']=='FAIL'
    assert (tmp_path/'state/runtime-integrity-maintenance/2026-10-26/previous-receipt.json').read_bytes()==old
    assert maintenance.run_integrity_maintenance(settings,now=at)['reused'] is True and len(seen)==1


@pytest.mark.parametrize('value',[True,0,541,1.5,float('inf'),'540'])
def test_maintenance_api_timeout_is_strict_bounded_integer(tmp_path,value):
    with pytest.raises(ValueError,match='INTEGRITY_MAINTENANCE_TIMEOUT_INVALID'):
        maintenance.run_integrity_maintenance(SimpleNamespace(root=tmp_path),timeout_seconds=value)
    assert not (tmp_path/'state').exists()


def test_cli_maintenance_passes_explicit_timeout_without_store(tmp_path,monkeypatch,capsys):
    from liangjian_funnel import cli
    seen=[]
    monkeypatch.setattr(maintenance,'run_integrity_maintenance',lambda s,**k:seen.append(k) or dict(status='PASS'))
    monkeypatch.setattr(cli,'RuntimeStore',lambda *a,**k:pytest.fail('maintenance must not migrate'))
    assert cli.main(['run-runtime-integrity','--timeout-seconds','480'],settings=SimpleNamespace(root=tmp_path))==0
    assert seen==[dict(timeout_seconds=480)] and json.loads(capsys.readouterr().out)['status']=='PASS'
