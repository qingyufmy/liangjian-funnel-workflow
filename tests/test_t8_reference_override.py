from datetime import datetime, timedelta
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.hithink_board_reference import maintain_configured_references
from liangjian_funnel.settings import Settings

NOW = datetime(2026, 11, 3, 7, 50, tzinfo=ZoneInfo('Asia/Shanghai'))


@pytest.fixture
def context(tmp_path, monkeypatch):
    import subprocess
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout=json.dumps({'qualified_themes': 27, 'theme_count': 27}))
    monkeypatch.setattr(subprocess, 'run', run)
    settings = Settings.from_env({}, root=tmp_path).model_copy(update={'rotation_membership_source': 'LOCAL_REFERENCE'})
    return settings, calls


def approval(**changes):
    return dict(override_expires_at=NOW+timedelta(minutes=30), override_reason='Tony T8 approval 07:40',
                override_run_id='t8-20261103', **changes)


def test_one_use_audit_native_route_and_default_gate(context, monkeypatch):
    settings, calls = context
    monkeypatch.setenv('https_proxy', 'unit-secret')
    assert maintain_configured_references(settings, now=NOW)['reason_code'] == 'REFERENCE_REFRESH_TRADING_PREPARATION_PROTECTED'
    result = maintain_configured_references(settings, now=NOW, **approval())
    assert result['status'] == 'READY' and len(calls) == 1
    receipt = settings.root/'outputs/t8-reference-override-t8-20261103.json'
    assert json.loads(receipt.read_text())['expires_at'] == (NOW+timedelta(minutes=30)).isoformat()
    command, kwargs = calls[0]
    assert command[command.index('--execution-scope')+1] == 'PRODUCTION_MAINTENANCE'
    assert command[command.index('--maintenance-receipt')+1] == str(receipt)
    assert kwargs['env']['TZ'] == 'Asia/Shanghai'
    assert not any(k.upper().endswith('_PROXY') for k in kwargs['env'])
    assert maintain_configured_references(settings, now=NOW, **approval())['reason_code'] == 'REFERENCE_OVERRIDE_ALREADY_CONSUMED'
    assert len(calls) == 1
    assert maintain_configured_references(settings, now=NOW)['status'] == 'BLOCKED'


@pytest.mark.parametrize('field,value', [
    ('override_expires_at', NOW-timedelta(seconds=1)),
    ('override_expires_at', NOW+timedelta(seconds=359)),
    ('override_expires_at', NOW+timedelta(hours=2)),
    ('override_expires_at', NOW+timedelta(days=1)),
    ('override_expires_at', NOW.replace(tzinfo=None)),
    ('override_expires_at', '2026-11-03T08:20:00+08:00'),
    ('override_reason', ''), ('override_reason', '  '),
    ('override_run_id', '../unsafe'), ('override_run_id', None),
])
def test_invalid_approval_cannot_launch_collector(context, field, value):
    settings, calls = context
    args = approval(); args[field] = value
    assert maintain_configured_references(settings, now=NOW, **args)['status'] == 'BLOCKED'
    assert not calls and not (settings.root/'outputs').exists()


def test_failure_consumes_approval_without_leaking_stderr(context, monkeypatch):
    import subprocess
    settings, _ = context
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: SimpleNamespace(returncode=1, stdout='bad-json', stderr='unit-secret'))
    result = maintain_configured_references(settings, now=NOW, **approval())
    assert result['reason_code'] == 'REFERENCE_REFRESH_FAILED' and 'unit-secret' not in str(result)
    assert result['override']['run_id'] == 't8-20261103'
    assert maintain_configured_references(settings, now=NOW, **approval())['reason_code'] == 'REFERENCE_OVERRIDE_ALREADY_CONSUMED'


def test_gate_uses_shanghai_clock(context):
    settings, calls = context
    utc = NOW.astimezone(ZoneInfo('UTC'))
    assert maintain_configured_references(settings, now=utc)['status'] == 'BLOCKED'
    assert not calls
