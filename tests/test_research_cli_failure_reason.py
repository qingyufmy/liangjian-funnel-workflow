"""Expose only a fixed typed contract code, never raw exception details."""
import json
from types import SimpleNamespace

import pytest
from liangjian_funnel import cli
from liangjian_funnel.settings import Settings


@pytest.mark.parametrize('exception,expected', [
    (ValueError('RESEARCH_UPDATE_SEED_MARKET_MISMATCH'), 'RESEARCH_UPDATE_SEED_MARKET_MISMATCH'),
    (ValueError('RESEARCH_UPDATE_A1_POINTER_CHANGED'), 'RESEARCH_UPDATE_A1_POINTER_CHANGED'),
    (ValueError('RESEARCH_UPDATE_SECRET Bearer not-for-output'), 'ValueError'),
    (ValueError('https://provider.invalid/?token=not-for-output'), 'ValueError'),
    (KeyError('RESEARCH_UPDATE_SEED_MARKET_MISMATCH'), 'KeyError'),
    (TypeError('private settings object'), 'TypeError'),
])
def test_actual_cli_preserves_known_contract_failure_without_printing_sensitive_text(
        tmp_path, monkeypatch, capsys, exception, expected):
    def fail(**kwargs):
        raise exception
    app = SimpleNamespace(run_a1_research_update=fail)
    monkeypatch.setattr(cli, 'WorkflowApplication', lambda settings: app)
    settings = Settings.from_env({}, root=tmp_path)
    result = cli.main(['run-a1-research-update', '--snapshot-id', 'snapshot-controlled-1',
        '--snapshot-hash', 'a' * 64, '--snapshot-file-sha256', 'b' * 64,
        '--source-trade-date', '2026-10-23', '--expected-a1-generation-id', 'controlled',
        '--run-id', 'controlled-readonly'], settings=settings)
    assert result == 4  # Existing typed-input failure exit contract is unchanged.
    assert json.loads(capsys.readouterr().out) == {'status': 'FAILED', 'reason_code': expected}
