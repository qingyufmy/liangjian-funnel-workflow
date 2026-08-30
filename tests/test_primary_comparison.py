import json
import os
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES
from liangjian_funnel.pipeline.research import ResearchPipeline
from liangjian_funnel.pipeline.research import FrozenInputSnapshot as ResearchSnapshot
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import PreparedSnapshot, WorkflowApplication, WorkflowError


NOW = datetime(2026, 8, 24, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
DEEPSEEK = "deepseek-v4-pro-0813"
KIMI = "moonshotai/kimi-k3-free"
GLM = "z-ai/glm-5.3-free"


def _settings(tmp_path: Path) -> Settings:
    return Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "model-secret",
            "LIANGJIAN_COMPARISON_ENABLED": "true",
        },
        root=tmp_path,
    )


def _prompt_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "prompts"
    directory.mkdir()
    for filename in PROMPT_FILENAMES:
        (directory / filename).write_text("prompt " + filename, encoding="utf-8")
    return directory


def _envelope(model: str, stage: str, snapshot_id: str) -> dict[str, object]:
    return {
        "schema_version": "test/1",
        "stage_id": {"A1": "AGENT_1", "A2": "AGENT_2", "A3": "AGENT_3"}[stage],
        "status": "OK",
        "input_snapshot_ids": [snapshot_id],
        "model_name": model,
        "config_version": "test-config",
        "prompt_version": "test-prompt",
        "market_regime": "REPAIR",
    }


class _Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def complete(self, model, messages, **metadata):
        runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
        stage = runtime["stage"]
        self.calls.append((model, stage))
        symbol = runtime["g0_symbols"][0] if stage == "A1" else runtime["upstream_symbols"][0]
        output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
        if stage == "A1":
            output.update({
                "active_research_pool": [{"symbol": symbol, "structural_score": 80, "data_quality_score": 80, "evidence_confidence": 0.8}],
                "monitor_pool": [],
                "rejected_candidates": [],
            })
        elif stage == "A2":
            output["focus_pool"] = [{"symbol": symbol, "theme_score": 65}]
        else:
            output["core_watch_pool"] = [{"symbol": symbol, "technical_score": 80, "reward_risk": 3.0, "stop_distance_pct": 0.03}]
        return ModelCallResult(
            model=model,
            output=output,
            prompt_hash=metadata.get("prompt_hash"),
            input_hash=metadata.get("input_hash"),
            latency_ms=1,
            attempts=1,
            thinking_variant="thinking_object",
        )


def test_primary_pipeline_sends_only_deepseek_and_keeps_lane_one(tmp_path: Path):
    client = _Client()
    result = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: NOW,
    ).run(
        {"snapshot_id": "primary-snapshot", "snapshot_hash": "a" * 64, "g0": ["600519.SH"]},
        run_id="primary-run",
        generated_at=NOW,
        models=(DEEPSEEK,),
        lane_start_index=1,
        primary_lane_ids=("lane_1",),
    )

    assert result.status == "READY"
    assert len(result.lanes) == 1
    assert result.lanes[0].lane == "lane_1"
    assert {model for model, _stage in client.calls} == {DEEPSEEK}
    assert [stage for _model, stage in client.calls] == ["A1", "A2", "A3"]
    assert result.outcome().counts["expected_lanes"] == 1


def _prepared(tmp_path: Path) -> PreparedSnapshot:
    return PreparedSnapshot(
        snapshot=ResearchSnapshot(
            snapshot_id="snapshot-primary",
            snapshot_hash="b" * 64,
            as_of=NOW,
            data={"g0_symbols": ["600519.SH"]},
        ),
        path=tmp_path / "snapshot-primary.json",
        full_universe_count=1,
        research_universe_count=1,
        trade_universe_count=1,
        selected_count=1,
        factor_ready_count=1,
    )


def test_comparison_request_is_idempotent_and_claim_is_single_owner(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)

    first = app._create_comparison_request(
        parent_run_id="2026-08-24-close-primary",
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    second = app._create_comparison_request(
        parent_run_id="2026-08-24-close-primary",
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    assert first == second
    claimed = app._claim_comparison_request(first)
    assert claimed is not None
    assert claimed[1]["status"] == "RUNNING"
    assert claimed[1]["attempts"] == 1
    assert app._claim_comparison_request(first) is None


def test_comparison_request_rejects_snapshot_identity_reuse(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)
    parent_id = "2026-08-24-close-immutable"
    request = app._create_comparison_request(
        parent_run_id=parent_id,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    path = app._comparison_request_path(parent_id)
    path.write_text(json.dumps({**request, "snapshot_hash": "c" * 64}), encoding="utf-8")

    with pytest.raises(WorkflowError, match="COMPARISON_REQUEST_IMMUTABLE_MISMATCH"):
        app._create_comparison_request(
            parent_run_id=parent_id,
            prepared=prepared,
            slot="close",
            primary_status="READY",
        )


def test_comparison_claim_rejects_fresh_lock_and_missing_or_terminal_request(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)

    fresh_parent = "2026-08-24-close-fresh-lock"
    fresh_request = app._create_comparison_request(
        parent_run_id=fresh_parent,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    fresh_lock = app._comparison_request_path(fresh_parent).with_suffix(".claim.lock")
    fresh_lock.write_text("active", encoding="utf-8")
    assert app._claim_comparison_request(fresh_request) is None
    assert fresh_lock.is_file(), "a contender must not remove another process's lease"
    assert app._claim_comparison_request({"parent_run_id": "missing-request"}) is None

    terminal_parent = "2026-08-24-close-terminal"
    terminal_request = app._create_comparison_request(
        parent_run_id=terminal_parent,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    terminal_path = app._comparison_request_path(terminal_parent)
    terminal_path.write_text(json.dumps({**terminal_request, "status": "SUCCEEDED"}), encoding="utf-8")
    assert app._claim_comparison_request(terminal_request) is None


def test_comparison_claim_handles_corrupt_attempt_and_lock_io_failures(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)

    corrupt_parent = "2026-08-24-close-corrupt-attempt"
    corrupt_request = app._create_comparison_request(
        parent_run_id=corrupt_parent,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    corrupt_path = app._comparison_request_path(corrupt_parent)
    corrupt_path.write_text(json.dumps({**corrupt_request, "attempts": "not-an-int"}), encoding="utf-8")
    claimed = app._claim_comparison_request(corrupt_request)
    assert claimed is not None
    assert claimed[1]["attempts"] == 1

    stat_error_parent = "2026-08-24-close-lock-stat-error"
    stat_error_request = app._create_comparison_request(
        parent_run_id=stat_error_parent,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    stat_error_lock = app._comparison_request_path(stat_error_parent).with_suffix(".claim.lock")
    stat_error_lock.write_text("uninspectable", encoding="utf-8")
    real_stat = Path.stat

    def stat_side_effect(path: Path, *args, **kwargs):
        if path == stat_error_lock:
            raise OSError("stat failed")
        return real_stat(path, *args, **kwargs)

    with patch("liangjian_funnel.workflow.Path.stat", autospec=True, side_effect=stat_side_effect):
        assert app._claim_comparison_request(stat_error_request) is None
    assert stat_error_lock.is_file()
    stat_error_lock.unlink(missing_ok=True)

    stale_parent = "2026-08-24-close-stale-lock-recovery"
    stale_request = app._create_comparison_request(
        parent_run_id=stale_parent,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    stale_lock = app._comparison_request_path(stale_parent).with_suffix(".claim.lock")
    stale_lock.write_text("stale", encoding="utf-8")
    old = time.time() - 2 * 60 * 60
    os.utime(stale_lock, (old, old))
    stale_claimed = app._claim_comparison_request(stale_request)
    assert stale_claimed is not None
    assert stale_claimed[1]["status"] == "RUNNING"
    assert stale_claimed[1]["attempts"] == 1

    close_error_parent = "2026-08-24-close-lock-close-error"
    close_error_request = app._create_comparison_request(
        parent_run_id=close_error_parent,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    with patch("liangjian_funnel.workflow.os.close", side_effect=OSError("close failed")):
        close_error_claimed = app._claim_comparison_request(close_error_request)
    assert close_error_claimed is not None

    unlink_error_parent = "2026-08-24-close-lock-unlink-error"
    unlink_error_request = app._create_comparison_request(
        parent_run_id=unlink_error_parent,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    unlink_error_lock = app._comparison_request_path(unlink_error_parent).with_suffix(".claim.lock")
    with patch.object(Path, "unlink", side_effect=OSError("unlink failed")):
        unlink_error_claimed = app._claim_comparison_request(unlink_error_request)
    assert unlink_error_claimed is not None
    unlink_error_lock.unlink(missing_ok=True)


def test_comparison_update_is_fenced_for_missing_invalid_and_dead_owner(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)
    parent_id = "2026-08-24-close-update-fence"
    request = app._create_comparison_request(
        parent_run_id=parent_id,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    path = app._comparison_request_path(parent_id)

    assert app._update_comparison_request(
        tmp_path / "missing-request.json",
        status="FAILED",
    ) is None

    path.write_text(json.dumps({**request, "status": "RUNNING", "attempts": "invalid", "owner_pid": os.getpid()}), encoding="utf-8")
    assert app._update_comparison_request(
        path,
        status="SUCCEEDED",
        expected_attempt=1,
    ) is None

    path.write_text(json.dumps({**request, "status": "RUNNING", "attempts": 1, "owner_pid": "not-a-pid"}), encoding="utf-8")
    assert app._update_comparison_request(
        path,
        status="SUCCEEDED",
        expected_attempt=1,
    ) is None

    path.write_text(json.dumps({**request, "status": "RUNNING", "attempts": 1, "owner_pid": os.getpid()}), encoding="utf-8")
    updated = app._update_comparison_request(path, status="FAILED", reason_code="TEST_FAILURE")
    assert updated is not None
    assert updated["status"] == "FAILED"
    assert updated["reason_code"] == "TEST_FAILURE"
    assert updated["owner_pid"] is None


def test_run_comparison_no_parent_busy_invalid_models_and_not_ready_are_safe(tmp_path: Path, monkeypatch):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)

    assert app.run_comparison() == {"status": "NOOP", "reason_code": "NO_PENDING_COMPARISON"}

    parent_id = "2026-08-24-close-run-branches"
    request = app._create_comparison_request(
        parent_run_id=parent_id,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    monkeypatch.setattr(app, "_claim_comparison_request", lambda _request: None)
    assert app.run_comparison(parent_run_id=parent_id) == {"status": "SKIPPED", "reason_code": "COMPARISON_BUSY"}

    # Restore the real claim method before exercising durable failure updates.
    monkeypatch.setattr(app, "_claim_comparison_request", WorkflowApplication._claim_comparison_request.__get__(app))
    path = app._comparison_request_path(parent_id)
    path.write_text(json.dumps({**request, "models": [KIMI]}), encoding="utf-8")
    invalid_models = app.run_comparison(parent_run_id=parent_id)
    assert invalid_models["status"] == "FAILED"
    assert invalid_models["reason_code"] == "COMPARISON_MODEL_SET_INVALID"

    not_ready_parent = "2026-08-24-close-run-not-ready"
    not_ready_request = app._create_comparison_request(
        parent_run_id=not_ready_parent,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )

    def fake_not_ready(*args, **kwargs):
        return {"run_id": kwargs["run_id_override"], "status": "BLOCKED"}

    app.run_research = fake_not_ready
    not_ready = app.run_comparison(parent_run_id=not_ready_parent)
    assert not_ready["status"] == "FAILED"
    not_ready_path = app._comparison_request_path(not_ready_parent)
    persisted = app._read_comparison_request(not_ready_path)
    assert persisted is not None
    assert persisted["status"] == "FAILED"
    assert persisted["reason_code"] == "COMPARISON_NOT_READY"


def test_run_comparison_cancels_request_on_keyboard_interrupt(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)
    parent_id = "2026-08-24-close-run-cancelled"
    app._create_comparison_request(
        parent_run_id=parent_id,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )

    def interrupting_run_research(*args, **kwargs):
        raise KeyboardInterrupt()

    app.run_research = interrupting_run_research
    with pytest.raises(KeyboardInterrupt):
        app.run_comparison(parent_run_id=parent_id)
    request = app._read_comparison_request(app._comparison_request_path(parent_id))
    assert request is not None
    assert request["status"] == "CANCELLED"
    assert request["reason_code"] == "RUN_CANCELLED"


def test_comparison_failure_does_not_modify_primary_and_success_is_child_only(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)
    parent_id = "2026-08-24-close-primary"
    app._create_comparison_request(
        parent_run_id=parent_id,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    parent_path = app.settings.workflow_output_dir / "runs" / f"{parent_id}.json"
    parent_path.parent.mkdir(parents=True, exist_ok=True)
    parent_path.write_text(json.dumps({"run_id": parent_id, "status": "READY", "plan_publication": {"created": ["p1"]}}), encoding="utf-8")

    calls: list[dict[str, object]] = []

    def fake_run_research(*args, **kwargs):
        calls.append(kwargs)
        return {"run_id": kwargs["run_id_override"], "status": "READY_DEGRADED", "plan_publication": {"publication": "COMPARISON_ONLY"}}

    app.run_research = fake_run_research
    result = app.run_comparison(parent_run_id=parent_id)
    assert result["status"] == "SUCCEEDED"
    assert calls[0]["models"] == (KIMI, GLM)
    assert calls[0]["lane_start_index"] == 2
    assert calls[0]["primary_lane_ids"] == ("lane_2", "lane_3")
    assert calls[0]["publish_plans"] is False
    assert calls[0]["record_runtime"] is False
    assert json.loads(parent_path.read_text(encoding="utf-8"))["status"] == "READY"
    assert json.loads((app.settings.workflow_output_dir / "runs" / f"{result['child_run_id']}.json").read_text(encoding="utf-8"))["parent_run_id"] == parent_id
    request = app._read_comparison_request(app._comparison_request_path(parent_id))
    assert request is not None and request["status"] == "SUCCEEDED"

    # A second invocation sees no pending request and cannot rerun DeepSeek.
    assert app.run_comparison(parent_run_id=parent_id)["status"] == "NOOP"


def test_comparison_error_is_persisted_without_changing_primary(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    prepared = _prepared(tmp_path)
    parent_id = "2026-08-24-close-failure"
    app._create_comparison_request(parent_run_id=parent_id, prepared=prepared, slot="close", primary_status="READY")

    def failing_run_research(*args, **kwargs):
        raise WorkflowError("COMPARISON_PROVIDER_FAILED")

    app.run_research = failing_run_research
    result = app.run_comparison(parent_run_id=parent_id)
    assert result["status"] == "FAILED"
    assert result["reason_code"] == "COMPARISON_PROVIDER_FAILED"
    request = app._read_comparison_request(app._comparison_request_path(parent_id))
    assert request is not None and request["status"] == "FAILED"
