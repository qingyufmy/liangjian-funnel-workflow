"""Failure-path and lane-isolation coverage for :mod:`pipeline.research`.

The tests use a legacy-mode temporary settings object for orchestration-only
cases.  This avoids opening a provider or a live feature generation while
still exercising the stage dependency chain and its durable audit output.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot,
    LaneResult,
    ResearchPipeline,
    StageAudit,
    _a1_batch_is_splittable,
    _a2_batch_is_splittable,
    _classify_stage_outcome,
    _lane_status_from_stages,
    _progress_status_for_stage_status,
    STATUS_DEGRADED_UNDERFILLED_DATA_GAP,
    STATUS_NOT_RUN_UPSTREAM_BLOCKED,
    STATUS_VALIDATED,
    STATUS_VALIDATED_NO_OPPORTUNITY,
)
from liangjian_funnel.settings import Settings


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 29, 15, 10, tzinfo=TZ)
DEEPSEEK = "deepseek-v4-pro-0813"


def _settings(tmp_path: Path, *, mode: str = "legacy") -> Settings:
    return Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "model-test-key",
            "HITHINK_FINANCE_API_KEY": "hithink-test-key",
            "LIANGJIAN_RESEARCH_PIPELINE_MODE": mode,
        },
        root=tmp_path,
    )


def _prompt_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "prompts"
    directory.mkdir()
    for filename in PROMPT_FILENAMES:
        (directory / filename).write_text("prompt " + filename, encoding="utf-8")
    return directory


def _snapshot(*, enabled: bool = False, symbols: list[str] | None = None) -> FrozenInputSnapshot:
    data: dict[str, object] = {"g0_symbols": symbols if symbols is not None else ["600519.SH"]}
    if enabled:
        data["DETERMINISTIC_RESEARCH_V2_ENABLED"] = True
    return FrozenInputSnapshot(
        snapshot_id="snapshot-research-orchestration",
        snapshot_hash="b" * 64,
        as_of=NOW,
        data=data,
    )


def _audit(
    stage: str,
    *,
    status: str = STATUS_VALIDATED,
    symbols: tuple[str, ...] = ("600519.SH",),
    reason_codes: tuple[str, ...] = (),
) -> StageAudit:
    return StageAudit(
        lane="lane_1",
        model=DEEPSEEK,
        stage=stage,
        status=status,
        snapshot_id="snapshot-research-orchestration",
        prompt_hash=None,
        input_hash="input",
        output_hash="output",
        latency_ms=1,
        attempts=1,
        thinking_variant="test",
        symbols=symbols,
        reason_codes=reason_codes,
        output={"analysis_summary": {"outcome": "TEST"}},
    )


def test_invalid_model_and_lane_contracts_block_every_stage_without_model_calls(tmp_path: Path) -> None:
    class NoCall:
        def complete(self, *_args, **_kwargs):
            raise AssertionError("invalid configuration must fail before a model call")

    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=NoCall(),
        output_dir=tmp_path / "research-output",
        now=lambda: NOW,
    )
    invalid_model = pipeline.run(
        _snapshot(),
        run_id="invalid-model-run",
        generated_at=NOW,
        models=("not-configured",),
        primary_lane_ids=("lane_1",),
    )
    assert invalid_model.status == "BLOCKED"
    assert len(invalid_model.lanes) == 1
    assert all(
        "RESEARCH_MODEL_CONFIG_INVALID" in stage.reason_codes
        for stage in invalid_model.lanes[0].stages
    )

    invalid_primary = pipeline.run(
        _snapshot(),
        run_id="invalid-primary-run",
        generated_at=NOW,
        models=(DEEPSEEK,),
        primary_lane_ids=("lane_2",),
    )
    assert invalid_primary.status == "BLOCKED"
    assert all("PRIMARY_LANE_CONFIG_INVALID" in stage.reason_codes for stage in invalid_primary.lanes[0].stages)


def test_missing_g0_is_explicitly_blocked_and_not_treated_as_empty_opportunity(tmp_path: Path) -> None:
    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=object(),
        output_dir=tmp_path / "research-output",
        now=lambda: NOW,
    )
    result = pipeline.run(
        _snapshot(symbols=[]),
        run_id="missing-g0-run",
        generated_at=NOW,
        models=(DEEPSEEK,),
    )
    assert result.status == "BLOCKED"
    assert [stage.status for stage in result.lanes[0].stages] == ["BLOCKED", "BLOCKED", "BLOCKED"]
    assert result.lanes[0].stages[0].reason_codes == ("G0_UNPROVABLE",)


def test_prompt_repository_failure_isolated_from_model_and_audit_output(tmp_path: Path) -> None:
    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=tmp_path / "does-not-exist",
        model_client=object(),
        output_dir=tmp_path / "research-output",
        now=lambda: NOW,
    )
    result = pipeline.run(
        _snapshot(),
        run_id="missing-prompt-run",
        generated_at=NOW,
        models=(DEEPSEEK,),
    )
    assert result.status == "BLOCKED"
    assert all(stage.reason_codes == ("PROMPT_REPOSITORY_BLOCKED",) for stage in result.lanes[0].stages)
    assert result.audit_paths and all(path.is_file() for path in result.audit_paths)
    audit = json.loads(result.audit_paths[0].read_text(encoding="utf-8"))
    assert audit["stages"][0]["reason_codes"] == ["PROMPT_REPOSITORY_BLOCKED"]


def test_lane_dependency_skips_empty_downstream_pools_with_deterministic_noop_audits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=object(),
        output_dir=tmp_path / "research-output",
        now=lambda: NOW,
    )
    events: list[dict[str, object]] = []
    monkeypatch.setattr(pipeline, "_emit_progress", lambda **event: events.append(event))
    monkeypatch.setattr(pipeline, "_run_stage_with_checkpoint", lambda **kwargs: _audit(kwargs["stage"], symbols=()))

    lane = pipeline._run_lane(
        lane_id="lane_1",
        model=DEEPSEEK,
        snapshot=_snapshot(),
        g0={"600519.SH"},
        bundle=object(),
        run_id="empty-downstream-run",
        global_reason=None,
    )
    assert lane.status == "READY"
    assert [stage.status for stage in lane.stages] == [STATUS_VALIDATED] * 3
    assert all(stage.symbols == () for stage in lane.stages)
    assert any(event.get("status") == "SKIPPED" for event in events)
    assert all(stage.diagnostics and stage.diagnostics.get("outcome_code") == "NO_ACTION_UPSTREAM_POOL_EMPTY" for stage in lane.stages[1:])


def test_lane_dependency_blocks_downstream_after_an_upstream_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=object(),
        output_dir=tmp_path / "research-output",
        now=lambda: NOW,
    )
    monkeypatch.setattr(
        pipeline,
        "_run_stage_with_checkpoint",
        lambda **kwargs: _audit(
            kwargs["stage"],
            status="BLOCKED_MODEL",
            symbols=(),
            reason_codes=("MODEL_CALL_FAILED",),
        ),
    )
    lane = pipeline._run_lane(
        lane_id="lane_1",
        model=DEEPSEEK,
        snapshot=_snapshot(),
        g0={"600519.SH"},
        bundle=object(),
        run_id="blocked-downstream-run",
        global_reason=None,
    )
    assert lane.status == "BLOCKED"
    assert lane.stages[0].status == "BLOCKED_MODEL"
    assert [stage.reason_codes for stage in lane.stages[1:]] == [("UPSTREAM_STAGE_BLOCKED",)] * 2
    assert lane.final_output is None


def test_stage_enrichment_failure_never_falls_back_to_full_universe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=object(),
        output_dir=tmp_path / "research-output",
        now=lambda: NOW,
    )
    pipeline.stage_snapshot_enricher = object()
    monkeypatch.setattr(pipeline, "_run_stage_with_checkpoint", lambda **kwargs: _audit(kwargs["stage"]))
    monkeypatch.setattr(
        pipeline,
        "_enrich_stage_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("enrichment unavailable")),
    )
    lane = pipeline._run_lane(
        lane_id="lane_1",
        model=DEEPSEEK,
        snapshot=_snapshot(),
        g0={"600519.SH"},
        bundle=object(),
        run_id="enrichment-failure-run",
        global_reason=None,
    )
    assert [stage.status for stage in lane.stages] == [STATUS_VALIDATED, "BLOCKED", "BLOCKED"]
    assert lane.stages[1].reason_codes == ("STAGE_SNAPSHOT_ENRICHMENT_FAILED",)
    assert lane.stages[2].reason_codes == ("UPSTREAM_STAGE_BLOCKED",)
    assert lane.final_output is None


def test_deterministic_mode_dispatches_to_v2_lane_handler(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipeline = ResearchPipeline(
        _settings(tmp_path, mode="deterministic_v2"),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=object(),
        output_dir=tmp_path / "research-output",
        now=lambda: NOW,
    )
    calls: list[dict[str, object]] = []
    expected = LaneResult("lane_1", DEEPSEEK, "READY_DEGRADED", (), {"focus_pool": []})

    def v2_handler(**kwargs):
        calls.append(kwargs)
        return expected

    monkeypatch.setattr(pipeline, "_run_lane_v2", v2_handler)
    result = pipeline._run_lane(
        lane_id="lane_1",
        model=DEEPSEEK,
        snapshot=_snapshot(enabled=True),
        g0={"600519.SH"},
        bundle=object(),
        run_id="v2-dispatch-run",
        global_reason=None,
    )
    assert result is expected
    assert calls and calls[0]["run_id"] == "v2-dispatch-run"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (STATUS_NOT_RUN_UPSTREAM_BLOCKED, "NOT_RUN"),
        (STATUS_VALIDATED_NO_OPPORTUNITY, STATUS_VALIDATED_NO_OPPORTUNITY),
        (STATUS_DEGRADED_UNDERFILLED_DATA_GAP, STATUS_DEGRADED_UNDERFILLED_DATA_GAP),
        ("BLOCKED_MODEL", "FAILED"),
    ],
)
def test_stage_status_projection_preserves_noop_data_gap_and_failure_semantics(status: str, expected: str) -> None:
    assert _progress_status_for_stage_status(status) == expected


def test_stage_outcome_and_batch_split_policies_are_fail_closed() -> None:
    status, reasons = _classify_stage_outcome(
        "A2",
        {"focus_pool": []},
        reasons=("A2_CRITICAL_DATA_INSUFFICIENT",),
    )
    assert status == STATUS_DEGRADED_UNDERFILLED_DATA_GAP
    assert reasons == ("A2_CRITICAL_DATA_INSUFFICIENT",)
    status, reasons = _classify_stage_outcome("A2", {"focus_pool": []}, reasons=())
    assert status == "VALIDATED_NO_OPPORTUNITY"
    assert reasons == ("A2_NO_FOCUS_OPPORTUNITY",)
    assert _lane_status_from_stages((_audit("A1"), _audit("A2", status=STATUS_DEGRADED_UNDERFILLED_DATA_GAP))) == "READY_DEGRADED"
    assert _a1_batch_is_splittable(("MODEL_PROMPT_TOO_LARGE",)) is True
    assert _a1_batch_is_splittable(("MODEL_CALL_FAILED",)) is False
    assert _a2_batch_is_splittable(("MODEL_PROMPT_TOO_LARGE",)) is True
    assert _a2_batch_is_splittable(("A2_CRITICAL_DATA_INSUFFICIENT",)) is False
