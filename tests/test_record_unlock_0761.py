from contextlib import closing
from datetime import datetime, timedelta
import hashlib
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.record_unlock import unlock_record
from liangjian_funnel.runtime.team_v2_first_day import POLICY, latch_record, forced_record
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore

NOW = datetime(2026, 10, 27, 9, 58, tzinfo=ZoneInfo('Asia/Shanghai'))


def frozen_latch(tmp_path):
    settings = SimpleNamespace(root=tmp_path)
    store = TeamMeetingStore(tmp_path/'state/team_v2/team.db')
    latch_record(store, POLICY, account_id='local-paper', now=NOW,
                 reason_code='TEST_IDENTITY_CONFLICT', first_day_only=False)
    with closing(store._connect()) as db:
        raw = db.execute('SELECT body_json FROM control_record_latch').fetchone()[0]
        db.execute('CREATE TABLE untouched_plan(plan_id TEXT)')
        db.execute("INSERT INTO untouched_plan VALUES ('keep')")
        db.commit()
    return settings, store, raw, hashlib.sha256(raw.encode()).hexdigest()


def release(settings, sha, **kwargs):
    return unlock_record(settings, approval_id='TONY-0761-TEST', reason='verified conflict repaired',
                         expected_latch_sha256=sha, now=NOW, operator='TEST_OPERATOR', **kwargs)


def test_release_preserves_original_and_other_state_and_has_auditable_hashes(tmp_path):
    settings, store, raw, sha = frozen_latch(tmp_path)
    result = release(settings, sha)
    assert result['status'] == 'RELEASED' and result['before_sha256'] == sha
    assert forced_record(settings, account_id='local-paper', now=NOW) is None
    with closing(store._connect()) as db:
        assert db.execute('SELECT body_json FROM control_record_latch').fetchone()[0] == raw
        assert db.execute('SELECT * FROM untouched_plan').fetchall() == [('keep',)]
        text, actual = db.execute('SELECT body_json,body_sha256 FROM control_record_authorizations').fetchone()
    body = json.loads(text)
    assert hashlib.sha256(text.encode()).hexdigest() == actual == result['audit_sha256']
    assert body['operator'] == 'TEST_OPERATOR' and body['approval_id'] == 'TONY-0761-TEST'
    assert body['after_sha256'] == result['after_sha256']
    assert body['before_sha256'] != body['after_sha256']


@pytest.mark.parametrize('field,value', [('approval_id', ''), ('reason', '  '),
    ('expected_latch_sha256', '0'*64), ('account_id', 'other')])
def test_invalid_or_stale_authorization_cannot_release(tmp_path, field, value):
    settings, store, raw, sha = frozen_latch(tmp_path)
    args = dict(approval_id='TONY-TEST', reason='verified', expected_latch_sha256=sha, now=NOW)
    args[field] = value
    with pytest.raises(ValueError):
        unlock_record(settings, **args)
    assert forced_record(settings, account_id='local-paper', now=NOW)
    with closing(store._connect()) as db:
        assert db.execute('SELECT body_json FROM control_record_latch').fetchone()[0] == raw


def test_release_is_idempotent_and_new_anomaly_relocks(tmp_path):
    settings, store, raw, sha = frozen_latch(tmp_path)
    first = release(settings, sha)
    assert release(settings, sha)['status'] == 'ALREADY_RELEASED'
    latch_record(store, POLICY, account_id='local-paper', now=NOW+timedelta(minutes=1),
                 reason_code='NEW_RISK_CONFLICT', first_day_only=False)
    assert forced_record(settings, account_id='local-paper', now=NOW)
    with closing(store._connect()) as db:
        events = [json.loads(r[0]) for r in db.execute('SELECT body_json FROM control_record_authorizations ORDER BY sequence')]
    assert [e['action'] for e in events] == ['UNLOCK', 'RELOCK']
    assert events[1]['previous_audit_sha256'] == first['audit_sha256']
    assert events[1]['reason'] == 'NEW_RISK_CONFLICT'
    assert forced_record(settings, account_id='local-paper', now=NOW+timedelta(days=1)) is None


def test_tampered_authorization_never_silently_releases(tmp_path):
    settings, store, raw, sha = frozen_latch(tmp_path)
    release(settings, sha)
    with closing(store._connect()) as db, db:
        db.execute("UPDATE control_record_authorizations SET body_json='{}'")
    with pytest.raises(ValueError, match='CHAIN_INVALID'):
        forced_record(settings, account_id='local-paper', now=NOW)


def test_new_conflict_requires_new_approval_and_its_current_audit_pin(tmp_path):
    settings,store,_,sha=frozen_latch(tmp_path)
    release(settings,sha)
    latch_record(store,POLICY,account_id='local-paper',now=NOW+timedelta(minutes=1),
        reason_code='NEW_REAL_CONFLICT',first_day_only=False)
    current=forced_record(settings,account_id='local-paper',now=NOW)
    with pytest.raises(ValueError,match='NEW_CONFLICT_AUDIT_CHANGED'):
        unlock_record(settings,approval_id='NEW-APPROVAL',reason='new conflict reviewed',
            expected_latch_sha256=sha,now=NOW)
    result=unlock_record(settings,approval_id='NEW-APPROVAL',reason='new conflict reviewed',
        expected_latch_sha256=sha,expected_authorization_sha256=current['authorization_audit_sha256'],now=NOW)
    assert result['status']=='RELEASED' and forced_record(settings,account_id='local-paper',now=NOW) is None


def test_cli_requires_approval_and_no_workflow_constructor(tmp_path, monkeypatch, capsys):
    from liangjian_funnel import cli
    settings, _, _, sha = frozen_latch(tmp_path)
    monkeypatch.setattr(cli, 'WorkflowApplication', lambda *a, **kw: pytest.fail('workflow constructed'))
    with pytest.raises(SystemExit):
        cli.main(['unlock-record', '--reason', 'x', '--expected-latch-sha256', sha], settings=settings)
    assert forced_record(settings, account_id='local-paper', now=NOW)


def test_successful_cli_uses_only_audited_release_not_workflow(tmp_path,monkeypatch):
    from liangjian_funnel import cli
    import liangjian_funnel.runtime.record_unlock as module
    settings,_,_,sha=frozen_latch(tmp_path)
    real=module.unlock_record
    def at_frozen_time(*args,**kwargs):return real(*args,**kwargs,now=NOW)
    monkeypatch.setattr(module,'unlock_record',at_frozen_time)
    monkeypatch.setattr(cli,'WorkflowApplication',lambda *a,**kw:pytest.fail('workflow constructed'))
    assert cli.main(['unlock-record','--approval-id','TONY-CONTROLLED-CLI','--reason','reviewed conflict',
        '--expected-latch-sha256',sha],settings=settings)==0
    assert forced_record(settings,account_id='local-paper',now=NOW) is None
