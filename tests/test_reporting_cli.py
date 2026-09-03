import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.cli import _latest_workflow_acceptance, _monitor_plan_projection, build_parser, main
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
            "research_slots: {premarket: '08:30', morning: '09:26', close: '15:10'}\n"
        "monitor: {cadence_seconds: 60}\n"
        "models:\n  research:\n    - deepseek-v4-pro-0813\n    - moonshotai/kimi-k3-free\n    - z-ai/glm-5.3-free\n  monitor: deepseek-v4-flash-0731\n"
        "permissions: {external_orders: false, gm_fallback: false, live_trading: false, fast_track: false}\n",
        encoding="utf-8",
    )
    (tmp_path / "config" / "capability_specs.yaml").write_text("schema_version: test\n", encoding="utf-8")
    (tmp_path / "config" / "funnel_config_v2.yaml").write_text(
        "schema_version: astock-agent-funnel-v2\n"
        "runtime: {simulation_only: true, order_permission: DISABLED}\n"
        "data_sources:\n  authority: {realtime_quote: hithink_ths, minute_bars: mootdx}\n"
        "universe_gate: {require_trading_calendar: true}\n",
        encoding="utf-8",
    )
    (tmp_path / "config" / "exchange_rules.yaml").write_text(
        "schema_version: liangjian-exchange-rules/1.0.0\n"
        "simulation_only: true\nexternal_orders: false\nt_plus_one: true\nlot_size: 100\n"
        "sources: {sse: {}, szse: {}, bse: {}}\n",
        encoding="utf-8",
    )
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


def test_run_premarket_dispatches_dedicated_schedule_kind(tmp_path: Path, monkeypatch, capsys):
    captured = {}

    class FakeApplication:
        def __init__(self, _settings):
            pass

        def run_scheduled(self, kind):
            captured["kind"] = kind
            return {"status": "READY", "dispatch": []}

    monkeypatch.setattr(cli_module, "WorkflowApplication", FakeApplication)
    settings = Settings.from_env({}, root=tmp_path)
    assert main(["run-premarket"], settings=settings) == 0
    assert captured["kind"].value == "premarket_0830"
    assert json.loads(capsys.readouterr().out)["status"] == "READY"


def test_run_premarket_recovery_resend_bypasses_scheduler_lease(tmp_path: Path, monkeypatch, capsys):
    captured = {}

    class FakeApplication:
        def __init__(self, _settings):
            pass

        def run_premarket(self, *, recovery_resend=False):
            captured["recovery_resend"] = recovery_resend
            return {"status": "READY", "report_mode": "RECOVERY_RESEND"}

    monkeypatch.setattr(cli_module, "WorkflowApplication", FakeApplication)
    settings = Settings.from_env({}, root=tmp_path)
    assert main(["run-premarket", "--recovery-resend"], settings=settings) == 0
    assert captured["recovery_resend"] is True
    assert json.loads(capsys.readouterr().out)["report_mode"] == "RECOVERY_RESEND"


def test_historical_research_cli_preserves_explicit_timezone_cutoff(tmp_path: Path, monkeypatch, capsys):
    captured = {}

    class FakeApplication:
        def __init__(self, _settings):
            pass

        def run_research(self, slot, *, as_of=None, historical_replay=False, snapshot_id=None):
            captured.update(
                slot=slot,
                as_of=as_of,
                historical_replay=historical_replay,
                snapshot_id=snapshot_id,
            )
            return {"status": "READY", "run_id": "historical"}

    monkeypatch.setattr(cli_module, "WorkflowApplication", FakeApplication)
    settings = Settings.from_env({}, root=tmp_path)
    assert main(
        ["run-research", "--slot", "close", "--as-of", "2026-08-25T15:10:00+08:00"],
        settings=settings,
    ) == 0
    assert captured == {
        "slot": "close",
        "as_of": datetime(2026, 8, 25, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
        "historical_replay": True,
        "snapshot_id": None,
    }
    assert json.loads(capsys.readouterr().out)["run_id"] == "historical"


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
    assert main(["prepare-snapshot"], settings=settings) == 3
    assert json.loads(capsys.readouterr().out) == {
        "status": "FAILED",
        "reason_code": "MINUTE_CACHE_CONFLICT",
    }


def test_production_cli_rejects_removed_candidate_cap() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["prepare-snapshot", "--max-candidates", "20"])

    with pytest.raises(SystemExit):
        build_parser().parse_args(["run-research", "--slot", "close", "--max-candidates", "20"])


def test_import_broker_gold_validates_and_persists_monthly_non_runtime_data(tmp_path, capsys) -> None:
    source = tmp_path / "gold.csv"
    source.write_text(
        "month,broker,symbol,name,publish_time,source_ref\n"
        "2026-08,测试券商,600001.SH,甲公司,2026-08-01T00:00:00+08:00,https://example.test/report\n",
        encoding="utf-8",
    )
    settings = Settings.from_env({}, root=tmp_path)

    assert main(
        ["import-broker-gold", str(source), "--as-of", "2026-08-29T18:00:00+08:00"],
        settings=settings,
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["benchmark_not_runtime_input"] is True
    assert payload["record_count"] == 1
    target = settings.broker_gold_dir / "2026-08.json"
    assert target.is_file()
    assert json.loads(target.read_text(encoding="utf-8"))["records"][0]["symbol"] == "600001.SH"


def test_latest_workflow_acceptance_requires_primary_lane_and_records_optional_comparisons():
    assert _latest_workflow_acceptance([])["status"] == "NOT_RUN"
    ready = [
        {"run_id": "new", "lane_id": "lane_1", "status": "READY_TO_PUBLISH"},
        {"run_id": "new", "lane_id": "lane_2", "status": "PUBLISHED"},
        {"run_id": "new", "lane_id": "lane_3", "status": "READY_TO_PUBLISH"},
        {"run_id": "old", "lane_id": "lane_1", "status": "BLOCKED"},
    ]
    assert _latest_workflow_acceptance(ready) == {
        "status": "READY",
        "run_id": "new",
        "expected_lanes": 3,
        "recorded_lanes": 3,
        "ready_lanes": 3,
        "required_lanes": 1,
        "ready_required_lanes": 1,
    }

    partial = [
        {"run_id": "new", "lane_id": "lane_1", "status": "READY_TO_PUBLISH"},
        {"run_id": "new", "lane_id": "lane_2", "status": "BLOCKED"},
        {"run_id": "new", "lane_id": "lane_3", "status": "BLOCKED"},
    ]
    assert _latest_workflow_acceptance(partial)["status"] == "READY_DEGRADED"
    assert _latest_workflow_acceptance(partial)["ready_lanes"] == 1
    assert _latest_workflow_acceptance(partial)["ready_required_lanes"] == 1

    missing_comparisons = [
        {"run_id": "new", "lane_id": "lane_1", "status": "READY_TO_PUBLISH"},
    ]
    assert _latest_workflow_acceptance(missing_comparisons)["status"] == "PARTIAL"
    assert _latest_workflow_acceptance(missing_comparisons, expected_lanes=1)["status"] == "READY"

    degraded_primary = [{
        "run_id": "new",
        "lane_id": "lane_1",
        "status": "PUBLISHED",
        "outcome": {
            "lane_id": "lane_1",
            "quality_state": "DEGRADED",
            "publication_state": "PUBLISHED",
            "lifecycle_state": "TERMINAL",
        },
    }]
    assert _latest_workflow_acceptance(degraded_primary, expected_lanes=1)["status"] == "READY_DEGRADED"

    optional_only = [
        {"run_id": "new", "lane_id": "lane_1", "status": "BLOCKED"},
        {"run_id": "new", "lane_id": "lane_2", "status": "PUBLISHED"},
        {"run_id": "new", "lane_id": "lane_3", "status": "BLOCKED"},
    ]
    assert _latest_workflow_acceptance(optional_only)["status"] == "PARTIAL"
    assert _latest_workflow_acceptance(optional_only)["ready_required_lanes"] == 0

    blocked = [
        {"run_id": "new", "lane_id": "lane_1", "status": "BLOCKED"},
        {"run_id": "new", "lane_id": "lane_2", "status": "FAILED"},
        {"run_id": "new", "lane_id": "lane_3", "status": "BLOCKED"},
    ]
    assert _latest_workflow_acceptance(blocked)["status"] == "BLOCKED"


def test_monitor_plan_projection_recovers_source_run_id_from_plan_identity():
    projected = _monitor_plan_projection({
        "plan_id": "2026-08-31-close-example:lane_1:abc123",
        "lane_id": "lane_1",
        "symbol": "000001.SZ",
        "status": "PENDING_MORNING_REVIEW",
        "payload_json": '{"name":"平安银行","strategy_profile":"TREND_MA5"}',
    })

    assert projected["source_run_id"] == "2026-08-31-close-example"
