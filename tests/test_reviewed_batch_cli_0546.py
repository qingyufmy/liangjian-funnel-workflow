"""Mutating CLI phases require explicit authority before configuration access."""
from unittest.mock import patch
from datetime import datetime
import json
import sqlite3
import pytest
from liangjian_funnel.runtime.reviewed_batch_cli import main


@pytest.mark.parametrize('phase,expected', [('review-isolated', 'MODEL_CALL'),
    ('stage-reviewed', 'WRITE'), ('switch-reviewed', 'WRITE')])
def test_authority_gate_precedes_all_configuration_and_store_access(phase, expected):
    with patch('liangjian_funnel.settings.Settings.from_env') as settings:
        with pytest.raises(ValueError, match=expected + '_AUTHORITY_REQUIRED'):
            main([phase, '--root', '/never-read', '--scope-id', 'candidate'])
        settings.assert_not_called()


def test_invalid_scope_rejected_before_configuration_even_with_write_flag():
    with patch('liangjian_funnel.settings.Settings.from_env') as settings:
        with pytest.raises(ValueError, match='SCOPE_INVALID'):
            main(['stage-reviewed', '--root', '/never-read', '--scope-id', 'bad/scope', '--allow-reviewed-write'])
        settings.assert_not_called()


def test_cli_imports_actual_isolated_reviews_then_switches_with_cas(tmp_path, monkeypatch, capsys):
    import test_reviewed_batch_morning_0546 as controlled
    from test_reviewed_batch_morning_0546 import setup, prepare, model, SCOPE, DAY, CAL, files
    from liangjian_funnel.runtime.reviewed_batch_morning import run_candidate_morning_reviews
    from liangjian_funnel.runtime.state import RuntimeStore
    from liangjian_funnel.runtime.reviewed_batch_switch import rowset_sha256
    original_fixture = controlled.fixture
    def full_schema_fixture(*args, **kwargs):
        configured, clock = original_fixture(*args, **kwargs)
        # The older RO-morning fixture has seven projection columns, whereas
        # the actual publication destination has these original row fields.
        with sqlite3.connect(configured.state_db_path) as db:
            db.execute('ALTER TABLE execution_plans ADD COLUMN plan_version INTEGER NOT NULL DEFAULT 1')
            db.execute('ALTER TABLE execution_plans ADD COLUMN created_at TEXT')
            db.execute('ALTER TABLE execution_plans ADD COLUMN updated_at TEXT')
            db.execute('UPDATE execution_plans SET created_at=?,updated_at=?', (clock.now.isoformat(), clock.now.isoformat()))
        return configured, clock
    monkeypatch.setattr(controlled, 'fixture', full_schema_fixture)
    settings, clock, source, destination, baseline_path, _ = setup(tmp_path, monkeypatch)
    prepare(settings, clock, source, baseline_path)
    client, calls = model(settings)
    reviewed = run_candidate_morning_reviews(settings=settings, scope_id=SCOPE, clock=lambda: clock.now, calendar=CAL, client=client)
    assert reviewed['status'] == 'READY' and calls == ['A6', 'A3']
    store = RuntimeStore(destination.state_db_path,integrity_maintenance=True)
    old = list(store.list_execution_plans(status='PENDING_MORNING_REVIEW'))
    paper = files(destination.root / 'state/team_v2')
    with patch('liangjian_funnel.runtime.reviewed_batch_cli.datetime') as wall:
        wall.now.return_value = clock.now
        args = ['stage-reviewed', '--root', str(destination.root), '--scope-id', SCOPE,
            '--staging-root', str(settings.root), '--research-run-id', 'candidate-research', '--allow-reviewed-write']
        assert main(args) == 0
        staged = json.loads(capsys.readouterr().out)
        assert staged['published_plans'] == 0 and staged['active_pointer_changed'] is False
        assert [store.get_execution_plan(row['plan_id']) for row in old] == old
        cas = tmp_path / 'cas.json'
        cas.write_text(json.dumps(dict(expected_old_rows_sha256=rowset_sha256(old),
            expected_old_plan_ids=[row['plan_id'] for row in old], expected_pointer_revision=None)), encoding='utf-8')
        monkeypatch.setattr('liangjian_funnel.runtime.reviewed_batch_switch._now', lambda: clock.now)
        assert main(['switch-reviewed', '--root', str(destination.root), '--scope-id', SCOPE,
            '--cas', str(cas), '--allow-reviewed-write']) == 0
        switched = json.loads(capsys.readouterr().out)
        assert switched['pointer_revision'] == 1
        assert store.get_execution_plan('new-plan')['status'] == 'PENDING_MORNING_REVIEW'
        assert all(store.get_execution_plan(row['plan_id'])['status'] == 'SUPERSEDED' for row in old)
        assert main(['inspect', '--root', str(destination.root), '--scope-id', SCOPE]) == 0
        inspection = json.loads(capsys.readouterr().out)
        assert inspection['active_scope']['scope_id'] == SCOPE and inspection['production_writes'] == 0
    assert files(destination.root / 'state/team_v2') == paper
