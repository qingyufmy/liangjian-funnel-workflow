"""High-value orchestration contract tests.

These tests deliberately keep providers and model calls out of the loop.  The
workflow's durable boundaries (resource gate, resume snapshot, comparison
claim/fencing, and primary publication) are still exercised with real files,
so a green test cannot be obtained by merely calling an unasserted mock.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import liangjian_funnel.workflow as workflow_module
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot,
    LaneResult,
    ResearchRunResult,
)
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import PreparedSnapshot, WorkflowApplication, WorkflowError


TZ = ZoneInfo("Asia/Shanghai")
DEEPSEEK = "deepseek-v4-pro-0813"
KIMI = "moonshotai/kimi-k3-free"
GLM = "z-ai/glm-5.3-free"
NOW = datetime(2026, 8, 29, 15, 10, tzinfo=TZ)


def _settings(tmp_path: Path) -> Settings:
    return Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "model-test-key",
            "HITHINK_FINANCE_API_KEY": "hithink-test-key",
            "LIANGJIAN_RESEARCH_PIPELINE_MODE": "legacy",
        },
        root=tmp_path,
    )


def _app(tmp_path: Path) -> WorkflowApplication:
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    app._ensure_trading_day = lambda *_args, **_kwargs: None
    # ``run_research`` normally receives these from ``__init__``.  Keeping
    # the test application deliberately small makes it explicit that no
    # provider, model, or runtime account is touched by the orchestration
    # assertions below.
    app.prompts = object()
    app.model_client = object()
    app.store = None
    app.research_checkpoints = None
    app._stage_snapshot_enricher = None
    return app


def _prepared(tmp_path: Path) -> PreparedSnapshot:
    snapshot = FrozenInputSnapshot(
        snapshot_id="snapshot-orchestration-1",
        snapshot_hash="a" * 64,
        as_of=NOW,
        data={"g0_symbols": ["600519.SH"]},
    )
    return PreparedSnapshot(
        snapshot=snapshot,
        path=tmp_path / "snapshot-orchestration-1.json",
        full_universe_count=1,
        research_universe_count=1,
        trade_universe_count=1,
        selected_count=1,
        factor_ready_count=1,
    )


def _resources(allowed: bool) -> SimpleNamespace:
    return SimpleNamespace(
        allowed=allowed,
        reason_codes=("RESOURCE_BUDGET_EXCEEDED",) if not allowed else (),
        snapshot=SimpleNamespace(as_dict=lambda: {"rss": 0, "available_memory_mb": 512}),
    )


def test_research_rejects_invalid_historical_and_comparison_inputs(tmp_path: Path) -> None:
    app = _app(tmp_path)

    with pytest.raises(WorkflowError, match="HISTORICAL_AS_OF_REQUIRED"):
        app.run_research("close", historical_replay=True)
    with pytest.raises(WorkflowError, match="HISTORICAL_AS_OF_NOT_PAST"):
        app.run_research("close", as_of=datetime.now(TZ), historical_replay=True)
    with pytest.raises(WorkflowError, match="SNAPSHOT_ID_REQUIRES_HISTORICAL_REPLAY"):
        app.run_research("close", as_of=NOW, snapshot_id="snapshot-orchestration-1")
    with pytest.raises(WorkflowError, match="COMPARISON_SNAPSHOT_REQUIRED"):
        app.run_research("close", as_of=NOW, comparison_run=True)


def test_resource_gate_finishes_progress_and_does_not_start_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app(tmp_path)
    monkeypatch.setattr(workflow_module, "evaluate_resources", lambda _root: _resources(False))

    with pytest.raises(WorkflowError, match="RESOURCE_BUDGET_EXCEEDED"):
        app.run_research("close", as_of=NOW)

    progress = json.loads(app.settings.workflow_progress_path.read_text(encoding="utf-8"))
    assert progress["status"] == "BLOCKED"
    assert progress["phase"] == "FAILED"
    assert progress["reason_code"] == "RESOURCE_BUDGET_EXCEEDED"


def test_resume_snapshot_failure_is_terminally_visible(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app(tmp_path)
    monkeypatch.setattr(workflow_module, "evaluate_resources", lambda _root: _resources(True))
    monkeypatch.setattr(
        workflow_module,
        "measure_resources",
        lambda _root: SimpleNamespace(as_dict=lambda: {"rss": 1, "available_memory_mb": 512}),
    )

    def fail_resume(*_args, **_kwargs):
        raise WorkflowError("RESUME_SNAPSHOT_CORRUPT")

    app._load_research_resume_snapshot = fail_resume
    with pytest.raises(WorkflowError, match="RESUME_SNAPSHOT_CORRUPT"):
        app.run_research("close", as_of=NOW)

    progress = json.loads(app.settings.workflow_progress_path.read_text(encoding="utf-8"))
    assert progress["status"] == "BLOCKED"
    assert progress["reason_code"] == "RESUME_SNAPSHOT_CORRUPT"


def test_immutable_snapshot_loader_and_resume_marker_preserve_point_in_time_identity(tmp_path: Path) -> None:
    app = _app(tmp_path)
    prepared = _prepared(tmp_path)
    app.settings.snapshot_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "g0_symbols": ["600519.SH"],
        "G0_SCOPE_CONTRACT": "CONFIGURED_RESEARCH_UNIVERSE_V1",
        "snapshot_manifest": {
            "full_universe_count": 2,
            "research_universe_count": 1,
            "trade_universe_count": 1,
        },
        "factor_ready_symbols": ["600519.SH"],
    }
    snapshot_path = app.settings.snapshot_dir / "snapshot-20260829-test.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "snapshot_id": "snapshot-20260829-test",
                "snapshot_hash": workflow_module._hash_json(data),
                "as_of": NOW.isoformat(),
                "data": data,
            }
        ),
        encoding="utf-8",
    )
    loaded = app._load_research_snapshot_by_id(
        "snapshot-20260829-test",
        expected_date="2026-08-29",
    )
    assert loaded.snapshot.snapshot_id == "snapshot-20260829-test"
    assert loaded.research_universe_count == loaded.selected_count == 1
    assert loaded.full_universe_count == 2
    assert loaded.trade_universe_count == 1

    with pytest.raises(WorkflowError, match="SNAPSHOT_ID_INVALID"):
        app._load_research_snapshot_by_id("bad", expected_date="2026-08-29")
    with pytest.raises(WorkflowError, match="SNAPSHOT_NOT_FOUND"):
        app._load_research_snapshot_by_id("snapshot-20260829-missing", expected_date="2026-08-29")

    marker_prepared = PreparedSnapshot(
        snapshot=FrozenInputSnapshot(
            snapshot_id="snapshot-20260829-test",
            snapshot_hash=workflow_module._hash_json(data),
            as_of=NOW,
            data=data,
        ),
        path=snapshot_path,
        full_universe_count=2,
        research_universe_count=1,
        trade_universe_count=1,
        selected_count=1,
        factor_ready_count=1,
    )
    app._write_research_resume_marker("close", marker_prepared, status="RETRYABLE", reason_code="RESEARCH_NOT_READY")
    resumed = app._load_research_resume_snapshot("close", NOW)
    assert resumed is not None
    assert resumed.snapshot.snapshot_hash == marker_prepared.snapshot.snapshot_hash
    assert resumed.selected_count == 1

    marker = json.loads(
        app._research_resume_marker_path("close", "2026-08-29").read_text(encoding="utf-8")
    )
    marker["status"] = "COMPLETED"
    app._research_resume_marker_path("close", "2026-08-29").write_text(json.dumps(marker), encoding="utf-8")
    assert app._load_research_resume_snapshot("close", NOW) is None


def test_primary_only_publishes_before_idempotent_comparison_enqueue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app(tmp_path)
    prepared = _prepared(tmp_path)
    monkeypatch.setattr(workflow_module, "evaluate_resources", lambda _root: _resources(True))
    monkeypatch.setattr(
        workflow_module,
        "measure_resources",
        lambda _root: SimpleNamespace(as_dict=lambda: {"rss": 1, "available_memory_mb": 512}),
    )
    app._load_research_resume_snapshot = lambda *_args, **_kwargs: prepared
    app._publish_plans = lambda *_args, **_kwargs: {
        "atomic": True,
        "created": ["plan-primary"],
        "activated": [],
        "blocked": [],
        "publication": "PRIMARY",
    }
    comparison_calls: list[dict[str, object]] = []

    def enqueue(**kwargs):
        comparison_calls.append(kwargs)
        return {"request_id": kwargs["parent_run_id"], "status": "PENDING"}

    app._create_comparison_request = enqueue
    monkeypatch.setattr(workflow_module, "_write_broker_gold_benchmark", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(workflow_module, "write_stage_markdown_reports", lambda *_args, **_kwargs: ())

    pipeline_calls: list[dict[str, object]] = []

    class FakePipeline:
        def __init__(self, _settings, **kwargs):
            pipeline_calls.append({"constructor": kwargs})

        def run(self, snapshot, **kwargs):
            pipeline_calls.append({"run": kwargs})
            lane = LaneResult(
                lane="lane_1",
                model=DEEPSEEK,
                status="READY",
                stages=(),
                final_output={"core_watch_pool": []},
            )
            return ResearchRunResult(
                run_id=str(kwargs["run_id"]),
                generated_at=NOW,
                snapshot_id=snapshot.snapshot_id,
                snapshot_hash=snapshot.snapshot_hash,
                status="READY",
                lanes=(lane,),
                audit_paths=(),
                markdown_path=None,
                primary_lane_ids=("lane_1",),
            )

    monkeypatch.setattr(workflow_module, "ResearchPipeline", FakePipeline)
    summary = app.run_research(
        "close",
        as_of=NOW,
        primary_only=True,
        schedule_comparison=True,
    )

    assert summary["run_role"] == "primary"
    assert summary["models"] == [DEEPSEEK]
    assert summary["plan_publication"]["created"] == ["plan-primary"]
    assert len(comparison_calls) == 1
    assert comparison_calls[0]["slot"] == "close"
    run_kwargs = pipeline_calls[-1]["run"]
    assert run_kwargs["models"] == (DEEPSEEK,)
    assert run_kwargs["lane_start_index"] == 1
    assert run_kwargs["primary_lane_ids"] == ("lane_1",)
    persisted = json.loads(
        (app.settings.workflow_output_dir / "runs" / f"{summary['run_id']}.json").read_text(encoding="utf-8")
    )
    assert persisted["comparison_request"]["status"] == "PENDING"


def test_comparison_claim_handles_stale_lock_and_fencing(tmp_path: Path) -> None:
    app = _app(tmp_path)
    prepared = _prepared(tmp_path)
    parent_id = "2026-08-29-close-primary"
    request = app._create_comparison_request(
        parent_run_id=parent_id,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    path = app._comparison_request_path(parent_id)
    lock_path = path.with_suffix(".claim.lock")
    lock_path.write_text("stale", encoding="utf-8")
    old = time.time() - 10_000
    os.utime(lock_path, (old, old))
    claimed = app._claim_comparison_request(request)
    assert claimed is not None
    assert claimed[1]["status"] == "RUNNING"
    assert claimed[1]["attempts"] == 1

    # A reclaimer cannot finalize another attempt: the durable attempt number
    # is the fencing token, even if the owner PID is otherwise alive.
    assert app._update_comparison_request(
        path,
        status="SUCCEEDED",
        expected_attempt=999,
        child_run_id="stale-child",
    ) is None
    request_after = app._read_comparison_request(path)
    assert request_after is not None and request_after["status"] == "RUNNING"


def test_comparison_running_owner_and_invalid_parent_are_safe(tmp_path: Path) -> None:
    app = _app(tmp_path)
    # Path normalization is intentionally bounded and keeps approved filename
    # punctuation without allowing separators to escape the queue directory.
    assert app._comparison_request_path("---").name == "---.json"

    prepared = _prepared(tmp_path)
    parent_id = "2026-08-29-close-running"
    request = app._create_comparison_request(
        parent_run_id=parent_id,
        prepared=prepared,
        slot="close",
        primary_status="READY",
    )
    path = app._comparison_request_path(parent_id)
    path.write_text(
        json.dumps({**request, "status": "RUNNING", "owner_pid": os.getpid(), "attempts": 1}),
        encoding="utf-8",
    )
    assert app._claim_comparison_request(request) is None
    assert app._comparison_owner_alive(os.getpid()) is True
    assert app._comparison_owner_alive("not-a-pid") is False
