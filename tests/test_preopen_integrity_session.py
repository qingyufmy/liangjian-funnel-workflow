from datetime import datetime,timedelta,timezone
from pathlib import Path
from types import SimpleNamespace
import json,sqlite3
import pytest
from liangjian_funnel.runtime import integrity as integrity
from liangjian_funnel.runtime import integrity_maintenance as subject
from liangjian_funnel.runtime.state import RuntimeStore


def test_monday_0650_signing_covers_open_and_expires_at_true_36_hours(tmp_path,monkeypatch):
    current=[datetime.fromisoformat('2026-10-26T06:50:00+08:00')]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return current[0].astimezone(tz or timezone.utc)
    monkeypatch.setattr(integrity,'datetime',Clock)
    settings=SimpleNamespace(root=tmp_path,state_db_path=tmp_path/'state/workflow.sqlite3')
    RuntimeStore(settings.state_db_path)
    result=subject.run_integrity_maintenance(settings,now=current[0]);assert result['status']=='PASS'
    path=integrity.marker_path(settings.state_db_path);before=path.read_bytes();receipt=json.loads(before)
    checked=datetime.fromisoformat(receipt['checked_at'])
    assert checked==current[0] and checked+timedelta(hours=36)==datetime.fromisoformat('2026-10-27T18:50:00+08:00')
    monkeypatch.setattr(subject,'verify_integrity',lambda *a,**k:pytest.fail('NO_COLD_SCAN_IN_SESSION'))
    for at in ('09:00','09:30','12:50','14:59'):
        current[0]=datetime.fromisoformat('2026-10-26T'+at+':00+08:00')
        reused=subject.run_integrity_maintenance(settings,now=current[0])
        assert reused['status']=='PASS' and reused['full_scans']==0
        assert path.read_bytes()==before
    current[0]=datetime.fromisoformat('2026-10-27T18:50:01+08:00')
    with sqlite3.connect(settings.state_db_path) as db:assert integrity.verified_identity(settings.state_db_path,db) is False


@pytest.mark.parametrize('marker',['missing','corrupt','old'])
def test_late_monday_without_adequate_receipt_blocks_without_scan_or_migration(tmp_path,monkeypatch,marker):
    settings=SimpleNamespace(root=tmp_path,state_db_path=tmp_path/'state/workflow.sqlite3')
    RuntimeStore(settings.state_db_path);path=integrity.marker_path(settings.state_db_path)
    if marker=='missing':path.unlink()
    elif marker=='corrupt':path.write_text('{}')
    else:
        body=json.loads(path.read_bytes());body['checked_at']='2026-10-24T21:19:56+08:00';path.write_text(json.dumps(body))
    monkeypatch.setattr(subject,'verify_integrity',lambda *a,**k:pytest.fail('NO_COLD_SCAN_IN_SESSION'))
    result=subject.run_integrity_maintenance(settings,now=datetime.fromisoformat('2026-10-26T09:00:00+08:00'))
    assert result['status']=='BLOCKED' and result['full_scans']==0
    assert not (tmp_path/'state/runtime-integrity-maintenance').exists()
