import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.cli import main
import liangjian_funnel.cli as cli_module
from liangjian_funnel.contracts import CapabilityCheck, CapabilityReport, CapabilityStatus
from liangjian_funnel.reporting import write_capability_report
from liangjian_funnel.settings import Settings


def test_atomic_report_contains_no_secret_or_temp_file(tmp_path: Path):
    report = CapabilityReport(
        provider="TEST",
        generated_at=datetime(2026, 8, 24, tzinfo=ZoneInfo("Asia/Shanghai")),
        overall_status=CapabilityStatus.BLOCKED,
        checks=(CapabilityCheck(name="x", status=CapabilityStatus.BLOCKED, evidence={"api_key": "unit-secret-value"}),),
    )
    json_path, md_path = write_capability_report(report, tmp_path)
    assert json_path.exists() and md_path.exists()
    assert "unit-secret-value" not in json_path.read_text(encoding="utf-8")
    assert not list(tmp_path.glob("*.tmp"))


def test_doctor_is_offline_and_does_not_print_key(tmp_path: Path, capsys):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "runtime.yaml").write_text(
        "schema_version: liangjian-runtime/1.0.0\n"
        "mode: PHASE0_CAPABILITY_ONLY\n"
        "timezone: Asia/Shanghai\n"
        "research_slots: {morning: '09:25', close: '15:10'}\n"
        "monitor: {cadence_seconds: 60}\n"
        "models:\n  research:\n    - deepseek-v4-pro-0813\n    - moonshotai/kimi-k3-free\n    - z-ai/glm-5.3-free\n  monitor: deepseek-v4-flash-0731\n"
        "permissions: {external_orders: false, gm_fallback: false, live_trading: false, fast_track: false}\n",
        encoding="utf-8",
    )
    (tmp_path / "config" / "capability_specs.yaml").write_text("schema_version: test\n", encoding="utf-8")
    (tmp_path / "FINAL_IMPLEMENTATION_PLAN.md").write_text("plan", encoding="utf-8")
    secret = "doctor-secret-value"
    settings = Settings.from_env({"HITHINK_FINANCE_API_KEY": secret, "LIANGJIAN_MODEL_API_KEY": secret}, root=tmp_path)
    assert main(["doctor"], settings=settings) == 0
    output = capsys.readouterr().out
    assert secret not in output
    parsed = json.loads(output)
    assert parsed["exact_model_config"] is True
    assert parsed["runtime_contract"] is True
    assert parsed["phase0_ready_for_live_probe"] is True


def test_run_due_returns_nonzero_when_dispatch_failed_or_missed(tmp_path: Path, monkeypatch, capsys):
    class FakeApplication:
        def __init__(self, _settings):
            pass

        def run_due(self):
            return {
                "time": "2026-08-24T09:45:00+08:00",
                "dispatch": [
                    {"status": "FAILED", "reason_code": "MODEL_CALL_FAILED"},
                    {"status": "DISPATCHED"},
                ],
            }

    monkeypatch.setattr(cli_module, "WorkflowApplication", FakeApplication)
    settings = Settings.from_env({}, root=tmp_path)
    assert main(["run-due"], settings=settings) == 2
    assert json.loads(capsys.readouterr().out)["dispatch"][0]["status"] == "FAILED"


def test_run_due_ignores_lease_busy_and_skipped_statuses(tmp_path: Path, monkeypatch, capsys):
    class FakeApplication:
        def __init__(self, _settings):
            pass

        def run_due(self):
            return {
                "time": "2026-08-24T12:00:00+08:00",
                "dispatch": [
                    {"status": "LEASE_BUSY", "reason_code": "LEASE_BUSY"},
                    {"status": "SKIPPED", "reason_code": "CALLBACK_NOT_CONFIGURED"},
                ],
            }

    monkeypatch.setattr(cli_module, "WorkflowApplication", FakeApplication)
    settings = Settings.from_env({}, root=tmp_path)
    assert main(["run-due"], settings=settings) == 0
    assert json.loads(capsys.readouterr().out)["dispatch"][0]["status"] == "LEASE_BUSY"


def test_workflow_command_returns_safe_reason_for_unexpected_reason_coded_error(
    tmp_path: Path, monkeypatch, capsys
):
    class CacheConflict(RuntimeError):
        reason_code = "MINUTE_CACHE_CONFLICT"

    class FakeApplication:
        def __init__(self, _settings):
            pass

        def prepare_snapshot(self, **_kwargs):
            raise CacheConflict("secret market payload")

    monkeypatch.setattr(cli_module, "WorkflowApplication", FakeApplication)
    settings = Settings.from_env({}, root=tmp_path)
    assert main(["prepare-snapshot"], settings=settings) == 2
    assert json.loads(capsys.readouterr().out) == {
        "status": "FAILED",
        "reason_code": "MINUTE_CACHE_CONFLICT",
    }
