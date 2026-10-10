"""Prevent syntax-valid but ineffective deadlines for isolated shadow units."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "shadow_service_timeout_guard", ROOT / "scripts/validate_shadow_service_timeouts.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)

EXPECTED = {
    "liangjian-shadow-preopen-20261012.service.example": 900,
    "liangjian-shadow-session-20261012.service.example": 19860,
    "liangjian-shadow-reporting.service.example": 4800,
    "liangjian-shadow-week.service.example": 300,
}


@pytest.mark.parametrize("name,seconds", EXPECTED.items())
def test_every_shadow_template_has_effective_oneshot_deadline(name, seconds):
    text = (ROOT / "config/deploy/shadow-reporting" / name).read_text(encoding="utf-8")
    assert guard.validate_service(text) == {
        "type": "oneshot", "effective_field": "TimeoutStartSec", "timeout_seconds": seconds}


@pytest.mark.parametrize("fields", ["RuntimeMaxSec=5min",
                                       "RuntimeMaxSec=5min\nTimeoutStartSec=5min"])
def test_oneshot_runtime_max_is_rejected_even_with_start_timeout(fields):
    with pytest.raises(ValueError, match="ONESHOT_RUNTIME_MAX_INEFFECTIVE"):
        guard.validate_service("[Service]\nType=oneshot\n" + fields)


@pytest.mark.parametrize("value", ["0", "0min", "infinity", "-1", "NaN", "", "5oops"])
def test_unbounded_or_invalid_oneshot_timeout_is_rejected(value):
    with pytest.raises(ValueError, match="FINITE_POSITIVE"):
        guard.validate_service("[Service]\nType=oneshot\nTimeoutStartSec=" + value)


@pytest.mark.parametrize("kind,field", [("oneshot", "TimeoutStartSec"),
                                         ("exec", "RuntimeMaxSec"), ("simple", "RuntimeMaxSec")])
def test_deadline_is_selected_by_service_lifecycle_not_field_presence(kind, field):
    assert guard.validate_service(f"[Service]\nType={kind}\n{field}=5") == {
        "type": kind, "effective_field": field, "timeout_seconds": 5}


@pytest.mark.parametrize("kind", ["simple", "exec"])
def test_non_oneshot_start_timeout_alone_does_not_bound_runtime(kind):
    with pytest.raises(ValueError, match="EFFECTIVE_TIMEOUT_REQUIRED:RuntimeMaxSec"):
        guard.validate_service(f"[Service]\nType={kind}\nTimeoutStartSec=5")


def test_missing_oneshot_deadline_and_unknown_lifecycle_fail_closed():
    with pytest.raises(ValueError, match="EFFECTIVE_TIMEOUT_REQUIRED:TimeoutStartSec"):
        guard.validate_service("[Service]\nType=oneshot")
    with pytest.raises(ValueError, match="SERVICE_TYPE_NOT_VALIDATED"):
        guard.validate_service("[Service]\nType=forking\nRuntimeMaxSec=5")


def test_cli_reports_all_units_and_nonzero_before_any_install(tmp_path, capsys):
    valid = tmp_path / "valid.service"
    bad = tmp_path / "bad.service"
    valid.write_text("[Service]\nType=oneshot\nTimeoutStartSec=5", encoding="utf-8")
    bad.write_text("[Service]\nType=oneshot\nRuntimeMaxSec=5", encoding="utf-8")
    assert guard.main([str(valid), str(bad)]) == 2
    assert 'ONESHOT_RUNTIME_MAX_INEFFECTIVE' in capsys.readouterr().out
    assert guard.main([str(valid)]) == 0
