from __future__ import annotations

import json
from collections.abc import Mapping

import pytest

from liangjian_funnel.pipeline.outcome_contract import (
    CONTRACT_NAME,
    CONTRACT_VERSION,
    contract_hash,
    normalize_lane,
    normalize_run,
    normalize_stage,
    validate_contract,
)
from liangjian_funnel.pipeline.outcomes import (
    ActionabilityState,
    CliExitCode,
    DataSufficiencyState,
    JobLifecycleState,
    LaneOutcome,
    LifecycleState,
    OpportunityState,
    PublicationState,
    QualityState,
    RunOutcome,
    aggregate_lane_outcome,
    aggregate_run_outcome,
    aggregate_workflow_acceptance,
    cli_exit_code,
    project_run_status,
    stage_outcome_from_legacy,
)


def stage(
    name: str,
    status: str,
    *,
    input_count: int = 2,
    evaluated_count: int = 2,
    selected_count: int = 1,
    reasons: tuple[str, ...] = (),
    coverage: Mapping[str, object] | None = None,
):
    return stage_outcome_from_legacy(
        status,
        stage=name,
        reason_codes=reasons,
        counts={"input": input_count, "evaluated": evaluated_count, "selected": selected_count},
        data_coverage=coverage or {"actual": evaluated_count, "required": input_count},
    )


def test_public_v3_contract_normalizes_all_grains_and_hashes_canonically() -> None:
    a1 = stage("A1", "VALIDATED")
    a2 = stage("A2", "VALIDATED_NO_OPPORTUNITY", selected_count=0)
    a3 = stage("A3", "VALIDATED_NO_ACTION", selected_count=0)
    lane = aggregate_lane_outcome((a1, a2, a3), lane_id="lane_1", model="deepseek")
    run = aggregate_run_outcome((lane,), run_id="run-1", expected_lane_count=1)

    normalized_stage = normalize_stage(a1.as_dict())
    normalized_lane = normalize_lane(lane.as_dict())
    normalized_run = normalize_run(run.as_dict())
    assert CONTRACT_NAME == "research-outcome"
    assert CONTRACT_VERSION == "research-outcome/3.0.0"
    assert normalized_stage["schema_version"] == CONTRACT_VERSION
    assert normalized_lane["stages"]
    assert normalized_run["run_id"] == "run-1"
    assert validate_contract(normalized_stage, kind="stage") == ()
    assert validate_contract(normalized_lane, kind="lane") == ()
    assert validate_contract(normalized_run, kind="run") == ()
    assert contract_hash({"b": 2, "a": "中文"}) == contract_hash({"a": "中文", "b": 2})
    assert contract_hash(normalized_run) == contract_hash(json.loads(json.dumps(normalized_run, ensure_ascii=False)))


def test_contract_validator_reports_root_required_and_enum_errors() -> None:
    assert validate_contract([]) == ("root must be an object",)
    invalid = {
        "schema_version": "research-outcome/2.0.0",
        "job_status": "BROKEN",
        "quality_state": "BROKEN",
        "data_sufficiency_state": "BROKEN",
        "lifecycle_state": "BROKEN",
        "research_opportunity_state": "BROKEN",
        "focus_opportunity_state": "BROKEN",
        "actionability_state": "BROKEN",
        "publication_state": "BROKEN",
    }
    errors = validate_contract(invalid, kind="stage")
    assert "schema_version must be research-outcome/3.0.0" in errors
    assert "invalid job_status" in errors
    assert "invalid data_sufficiency_state" in errors
    assert "invalid lifecycle_state" in errors
    assert "invalid research_opportunity_state" in errors
    assert "invalid focus_opportunity_state" in errors
    assert "invalid actionability_state" in errors
    assert "invalid publication_state" in errors
    assert "missing field: reason_codes" in errors
    assert "missing field: stage" in errors

    valid = normalize_stage(stage("A1", "VALIDATED").as_dict())
    valid.pop("stage")
    valid["actionability_state"] = None
    assert "missing field: stage" in validate_contract(valid, kind="stage")
    assert "invalid actionability_state" not in validate_contract(valid, kind="stage")
    missing_lane = normalize_lane(aggregate_lane_outcome((stage("A1", "VALIDATED"),), lane_id="lane-1").as_dict())
    missing_lane.pop("lane_id")
    assert "missing field: lane_id" in validate_contract(missing_lane, kind="lane")
    missing_run = normalize_run(aggregate_run_outcome((), run_id="run-1").as_dict())
    missing_run.pop("run_id")
    assert "missing field: run_id" in validate_contract(missing_run, kind="run")


@pytest.mark.parametrize(
    ("status", "quality", "opportunity", "publication"),
    [
        ("PENDING", QualityState.DEGRADED, OpportunityState.NOT_APPLICABLE, PublicationState.NOT_APPLICABLE),
        ("RUNNING", QualityState.DEGRADED, OpportunityState.NOT_APPLICABLE, PublicationState.NOT_APPLICABLE),
        ("VALIDATED_NO_OPPORTUNITY", QualityState.VALIDATED, OpportunityState.ABSENT, PublicationState.NOT_APPLICABLE),
        ("VALIDATED_NO_ACTION", QualityState.VALIDATED, OpportunityState.ABSENT, PublicationState.NOT_APPLICABLE),
        ("VALIDATED_NO_SETUP", QualityState.VALIDATED, OpportunityState.ABSENT, PublicationState.NOT_APPLICABLE),
        ("VALIDATED_UNDERFILLED_MARKET", QualityState.DEGRADED, OpportunityState.PRESENT, PublicationState.NOT_APPLICABLE),
        ("READY", QualityState.VALIDATED, OpportunityState.PRESENT, PublicationState.READY),
        ("READY_DEGRADED", QualityState.DEGRADED, OpportunityState.PRESENT, PublicationState.READY),
        ("PUBLISHED", QualityState.VALIDATED, OpportunityState.PRESENT, PublicationState.PUBLISHED),
        ("BLOCKED", QualityState.BLOCKED, OpportunityState.UNKNOWN, PublicationState.BLOCKED),
        ("BLOCKED_MODEL", QualityState.FAILED, OpportunityState.UNKNOWN, PublicationState.BLOCKED),
        ("CANCELLED", QualityState.CANCELLED, OpportunityState.UNKNOWN, PublicationState.BLOCKED),
    ],
)
def test_legacy_status_projection_keeps_axes_independent(status, quality, opportunity, publication) -> None:
    selected = 0 if status in {"VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"} else 1
    result = stage_outcome_from_legacy(
        status,
        stage="A2",
        counts={"input": 2, "evaluated": 2, "selected": selected},
        data_coverage={"actual": 2, "required": 2},
    )
    assert result.quality_state is quality
    assert result.opportunity_state is opportunity
    assert result.publication_state is publication
    assert result.job_status in set(JobLifecycleState)


def test_zero_result_requires_evaluated_denominator_and_upstream_gap_is_not_a3_failure() -> None:
    no_denominator = stage_outcome_from_legacy("VALIDATED_NO_OPPORTUNITY", stage="A2", selected_count=0)
    assert no_denominator.quality_state is QualityState.BLOCKED
    assert no_denominator.opportunity_state is OpportunityState.UNKNOWN
    assert no_denominator.data_sufficiency_state is DataSufficiencyState.INSUFFICIENT

    empty_input = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY", stage="A2", counts={"input": 0, "evaluated": 0, "selected": 0}
    )
    assert empty_input.quality_state is QualityState.BLOCKED
    assert empty_input.opportunity_state is OpportunityState.UNKNOWN

    upstream = stage_outcome_from_legacy(
        "NOT_RUN_UPSTREAM_BLOCKED",
        stage="A3",
        reason_codes=("A2_DATA_GAP",),
        counts={"input": 1, "evaluated": 0, "selected": 0},
    )
    assert upstream.quality_state is QualityState.DEGRADED
    assert upstream.opportunity_state is OpportunityState.NOT_APPLICABLE
    assert upstream.publication_state is PublicationState.NOT_APPLICABLE
    assert "A3_NOT_APPLICABLE_UPSTREAM_DATA_GAP" in upstream.reason_codes


def test_lane_aggregation_distinguishes_running_failure_cancel_data_gap_and_hard_block() -> None:
    running = aggregate_lane_outcome((stage("A1", "RUNNING"),), lane_id="running")
    assert running.lifecycle_state is LifecycleState.RUNNING
    assert running.job_status is JobLifecycleState.RUNNING
    assert running.publication_state is PublicationState.NOT_APPLICABLE

    failed = aggregate_lane_outcome((stage("A1", "BLOCKED_MODEL"),), lane_id="failed")
    assert failed.quality_state is QualityState.FAILED
    assert failed.publication_state is PublicationState.BLOCKED
    assert failed.job_status is JobLifecycleState.FAILED

    cancelled = aggregate_lane_outcome((stage("A1", "CANCELLED"),), lane_id="cancelled")
    assert cancelled.quality_state is QualityState.CANCELLED
    assert cancelled.job_status is JobLifecycleState.CANCELLED

    gap = aggregate_lane_outcome((stage("A2", "DATA_GAP", selected_count=0),), lane_id="gap")
    assert gap.quality_state is QualityState.DEGRADED
    assert gap.publication_state is PublicationState.READY
    assert gap.legacy_status == "READY_DEGRADED"

    hard = aggregate_lane_outcome((stage("A1", "BLOCKED", reasons=("MODEL_GATE",)),), lane_id="hard")
    assert hard.quality_state is QualityState.BLOCKED
    assert hard.publication_state is PublicationState.BLOCKED
    assert hard.reason_codes == ("MODEL_GATE",)

    empty = aggregate_lane_outcome((), lane_id="empty")
    assert empty.lifecycle_state is LifecycleState.QUEUED
    assert empty.reason_codes == ("LANE_NOT_RUN",)
    projected = aggregate_lane_outcome((), lane_id="projected", legacy_status="READY")
    assert projected.legacy_status == "READY"
    assert projected.stages[0].stage == "A3"


def test_run_aggregation_primary_lane_controls_publication_and_comparison_is_optional() -> None:
    primary = aggregate_lane_outcome((stage("A1", "VALIDATED"), stage("A2", "VALIDATED")), lane_id="lane_1")
    optional_ready = aggregate_lane_outcome((stage("A1", "VALIDATED"),), lane_id="lane_2")
    optional_blocked = aggregate_lane_outcome((stage("A1", "BLOCKED", reasons=("MODEL_GATE",)),), lane_id="lane_3")

    ready = aggregate_run_outcome(
        (primary, optional_ready),
        run_id="run-ready",
        primary_lane_ids="lane_1",
        expected_lane_count=2,
    )
    assert ready.publication_state is PublicationState.READY
    assert ready.comparison_status == "READY"
    assert ready.job_status is JobLifecycleState.SUCCEEDED
    assert ready.counts["ready_required_lanes"] == 1

    partial_optional = aggregate_run_outcome(
        (primary, optional_blocked), run_id="run-partial", expected_lane_count=2, primary_lane_ids=("lane_1",)
    )
    assert partial_optional.publication_state is PublicationState.READY
    assert partial_optional.comparison_status == "BLOCKED"

    missing_primary = aggregate_run_outcome(
        (optional_ready,), run_id="run-missing", expected_lane_count=2, primary_lane_ids=("lane_1",)
    )
    assert missing_primary.quality_state is QualityState.BLOCKED
    assert missing_primary.publication_state is PublicationState.BLOCKED
    assert "PRIMARY_LANE_MISSING" in missing_primary.reason_codes

    assert aggregate_run_outcome((), run_id="not-started", expected_lane_count=3).legacy_status == "NOT_RUN"


def test_workflow_acceptance_uses_latest_run_and_handles_json_or_legacy_rows() -> None:
    valid_lane = aggregate_lane_outcome((stage("A1", "VALIDATED"),), lane_id="lane-1", model="m")
    valid_json = json.dumps(valid_lane.as_dict(), ensure_ascii=False)
    rows = [
        {"run_id": "new", "lane_id": "lane-1", "model": "m", "outcome_json": valid_json},
        {"run_id": "new", "lane_id": "lane-1", "model": "m", "status": "BLOCKED"},
        {"run_id": "new", "lane_id": "lane-2", "model": "m2", "outcome_json": "{bad", "status": "READY"},
        {"run_id": "old", "lane_id": "lane-1", "status": "BLOCKED"},
    ]
    result = aggregate_workflow_acceptance(rows, expected_lanes=2, required_lane_ids=("lane-1",))
    assert result.run_id == "new"
    assert result.counts["recorded_lanes"] == 2
    assert result.legacy_status == "READY"
    assert aggregate_workflow_acceptance([], expected_lanes=2).legacy_status == "NOT_RUN"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("READY", CliExitCode.SUCCESS),
        ("READY_DEGRADED", CliExitCode.SUCCESS),
        ("BLOCKED", CliExitCode.BUSINESS_BLOCKED),
        ("BLOCKED_MODEL", CliExitCode.TECHNICAL_FAILURE),
        ("CANCELLED", CliExitCode.CANCELLED),
    ],
)
def test_cli_projection_returns_stable_exit_codes(value: str, expected: CliExitCode) -> None:
    outcome = project_run_status(value, run_id="run", lane_id="lane-1")
    assert cli_exit_code(outcome) == int(expected)
    assert cli_exit_code({"status": value}) in {int(CliExitCode.SUCCESS), int(CliExitCode.BUSINESS_BLOCKED)}
    assert cli_exit_code({"outcome_v2": outcome.as_dict()}) == int(expected)
    # Canonical v3 payloads are first-class CLI inputs and must not require a
    # deprecated v2 wrapper just to retain their exit semantics.
    assert cli_exit_code(outcome.as_dict()) == int(expected)
    full_axes = {**outcome.as_dict(), "opportunity_state": outcome.opportunity_state.value}
    assert cli_exit_code(full_axes) == int(expected)


def test_run_and_lane_round_trip_from_mapping_defaults_missing_sequences() -> None:
    assert isinstance(LaneOutcome.from_mapping({"status": "READY", "stages": "bad"}), LaneOutcome)
    assert isinstance(RunOutcome.from_mapping({"status": "READY", "lanes": "bad"}), RunOutcome)
    with pytest.raises(TypeError, match="stage outcome"):
        normalize_stage([])
    with pytest.raises(TypeError, match="lane outcome"):
        normalize_lane([])
    with pytest.raises(TypeError, match="run outcome"):
        normalize_run([])


def test_stage_axis_fallbacks_and_direct_run_job_lifecycle_projection() -> None:
    # A stage payload produced by a v3 caller may omit the compatibility
    # opportunity axis; the stage-specific axis remains authoritative.
    a1 = normalize_stage(
        {
            "stage": "A1",
            "lifecycle_state": "TERMINAL",
            "quality_state": "VALIDATED",
            "publication_state": "READY",
            "research_opportunity_state": "PRESENT",
            "reason_codes": [],
            "counts": {"input": 1, "evaluated": 1, "selected": 1},
            "data_coverage": {"actual": 1, "required": 1},
            "legacy_status": "READY",
        }
    )
    a2 = normalize_stage(
        {
            "stage": "A2",
            "lifecycle_state": "TERMINAL",
            "quality_state": "VALIDATED",
            "publication_state": "READY",
            "focus_opportunity_state": "ABSENT",
            "reason_codes": [],
            "counts": {"input": 1, "evaluated": 1, "selected": 0},
            "data_coverage": {"actual": 1, "required": 1},
            "legacy_status": "VALIDATED_NO_OPPORTUNITY",
        }
    )
    a3 = normalize_stage(
        {
            "stage": "A3",
            "lifecycle_state": "TERMINAL",
            "quality_state": "VALIDATED",
            "publication_state": "READY",
            "actionability_state": "NO_ACTION",
            "reason_codes": [],
            "counts": {"input": 1, "evaluated": 1, "selected": 0},
            "data_coverage": {"actual": 1, "required": 1},
            "legacy_status": "VALIDATED_NO_ACTION",
        }
    )
    assert a1["opportunity_state"] == "PRESENT"
    assert a2["opportunity_state"] == "ABSENT"
    assert a3["opportunity_state"] == "ABSENT"

    data_gap_lane = aggregate_lane_outcome((), lane_id="lane_gap", legacy_status="DATA_GAP")
    assert data_gap_lane.quality_state is QualityState.DEGRADED
    assert data_gap_lane.publication_state is PublicationState.READY
    no_opportunity_lane = aggregate_lane_outcome((stage("A2", "VALIDATED_NO_OPPORTUNITY", selected_count=0),), lane_id="lane_noop")
    # Lane-level publication is READY even when its stage has no opportunity;
    # the absence is retained on the opportunity axis instead of projected as
    # a terminal legacy status that would hide a publishable empty result.
    assert no_opportunity_lane.legacy_status == "READY"
    assert no_opportunity_lane.opportunity_state is OpportunityState.ABSENT
    published_lane = aggregate_lane_outcome((stage("A3", "PUBLISHED"),), lane_id="lane_published")
    assert published_lane.publication_state is PublicationState.PUBLISHED

    running_lane = aggregate_lane_outcome((stage("A1", "RUNNING"),), lane_id="lane_running")
    failed_lane = aggregate_lane_outcome((stage("A1", "BLOCKED_MODEL"),), lane_id="lane_failed")
    cancelled_lane = aggregate_lane_outcome((stage("A1", "CANCELLED"),), lane_id="lane_cancelled")
    for lane, expected in (
        (running_lane, JobLifecycleState.RUNNING),
        (failed_lane, JobLifecycleState.FAILED),
        (cancelled_lane, JobLifecycleState.CANCELLED),
    ):
        projected = RunOutcome(
            run_id="direct",
            lifecycle_state=LifecycleState.TERMINAL,
            quality_state=QualityState.VALIDATED,
            opportunity_state=OpportunityState.NOT_APPLICABLE,
            publication_state=PublicationState.NOT_APPLICABLE,
            lanes=(lane,),
            primary_lane_ids=(lane.lane_id,),
        )
        assert projected.job_status is expected


def test_stage_legacy_no_opportunity_covers_each_evidence_gap_guard() -> None:
    # Every explicit empty conclusion must carry a complete evaluated
    # denominator.  Exercise both the first-time reason append and the
    # already-recorded reason paths for missing, empty, and under-covered
    # evidence.
    missing_reason = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY",
        stage="A2",
        counts={"input": 3, "evaluated": 1, "selected": 0},
        data_coverage={"actual": 1, "required": 3},
    )
    assert missing_reason.quality_state is QualityState.BLOCKED
    assert "DATA_COVERAGE_INSUFFICIENT" in missing_reason.reason_codes

    existing_reason = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY",
        stage="A2",
        reason_codes=("DATA_COVERAGE_INSUFFICIENT",),
        counts={"input": 3, "evaluated": 1, "selected": 0},
        data_coverage={"actual": 1, "required": 3},
    )
    assert existing_reason.reason_codes.count("DATA_COVERAGE_INSUFFICIENT") == 1

    missing_eval_existing_reason = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY",
        stage="A2",
        reason_codes=("DATA_COVERAGE_INSUFFICIENT",),
        counts={"input": 3, "selected": 0},
    )
    assert missing_eval_existing_reason.opportunity_state is OpportunityState.UNKNOWN

    empty_input_existing_reason = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY",
        stage="A2",
        reason_codes=("DATA_COVERAGE_INSUFFICIENT",),
        counts={"input": 0, "evaluated": 0, "selected": 0},
    )
    assert empty_input_existing_reason.opportunity_state is OpportunityState.UNKNOWN

    upstream_existing_reason = stage_outcome_from_legacy(
        "NOT_RUN_UPSTREAM_BLOCKED",
        stage="A3",
        reason_codes=("A2_DATA_GAP", "A3_NOT_APPLICABLE_UPSTREAM_DATA_GAP"),
        counts={"input": 1, "evaluated": 0, "selected": 0},
    )
    assert upstream_existing_reason.quality_state is QualityState.DEGRADED
    assert upstream_existing_reason.reason_codes.count("A3_NOT_APPLICABLE_UPSTREAM_DATA_GAP") == 1


def test_run_reducer_covers_running_queued_absent_and_optional_partial_axes() -> None:
    ready = aggregate_lane_outcome((stage("A1", "VALIDATED"),), lane_id="lane-ready")
    running = aggregate_lane_outcome((stage("A1", "RUNNING"),), lane_id="lane-running")
    running_result = aggregate_run_outcome(
        (running,),
        run_id="run-running",
        primary_lane_ids=("lane-running",),
        expected_lane_count=1,
    )
    assert running_result.lifecycle_state is LifecycleState.RUNNING

    queued_result = aggregate_run_outcome(
        (aggregate_lane_outcome((), lane_id="lane-queued"),),
        run_id="run-queued",
        primary_lane_ids=("lane-missing",),
        expected_lane_count=2,
    )
    assert queued_result.lifecycle_state is LifecycleState.QUEUED
    assert queued_result.quality_state is QualityState.BLOCKED

    absent = aggregate_lane_outcome(
        (stage("A2", "VALIDATED_NO_OPPORTUNITY", selected_count=0),),
        lane_id="lane-absent",
    )
    absent_result = aggregate_run_outcome(
        (absent,),
        run_id="run-absent",
        primary_lane_ids=("lane-absent",),
        expected_lane_count=1,
    )
    assert absent_result.opportunity_state is OpportunityState.ABSENT

    blocked = aggregate_lane_outcome(
        (stage("A1", "BLOCKED", reasons=("MODEL_GATE",)),),
        lane_id="lane-blocked",
    )
    partial_optional = aggregate_run_outcome(
        (ready, aggregate_lane_outcome((stage("A1", "VALIDATED"),), lane_id="lane-optional"), blocked),
        run_id="run-partial-optional",
        primary_lane_ids=("lane-ready",),
        expected_lane_count=3,
    )
    assert partial_optional.comparison_status == "PARTIAL"
    assert partial_optional.publication_state is PublicationState.READY


def test_workflow_acceptance_projects_all_legacy_readiness_outcomes() -> None:
    ready_lane = aggregate_lane_outcome((stage("A1", "VALIDATED"),), lane_id="lane-1", model="m")
    ready_json = json.dumps(ready_lane.as_dict(), ensure_ascii=False)
    blocked_lane = aggregate_lane_outcome(
        (stage("A1", "BLOCKED", reasons=("MODEL_GATE",)),), lane_id="lane-2", model="m2"
    )
    blocked_json = json.dumps(blocked_lane.as_dict(), ensure_ascii=False)

    degraded = aggregate_workflow_acceptance(
        [
            {"run_id": "degraded", "lane_id": "lane-1", "outcome_json": ready_json},
            {"run_id": "degraded", "lane_id": "lane-2", "outcome_json": blocked_json},
        ],
        expected_lanes=2,
        required_lane_ids=("lane-1",),
    )
    assert degraded.legacy_status == "READY_DEGRADED"

    partial = aggregate_workflow_acceptance(
        [{"run_id": "partial", "lane_id": "lane-1", "outcome_json": ready_json}],
        expected_lanes=2,
        required_lane_ids=("lane-1",),
    )
    assert partial.legacy_status == "PARTIAL"

    blocked = aggregate_workflow_acceptance(
        [{"run_id": "blocked", "lane_id": "lane-2", "outcome_json": blocked_json}],
        expected_lanes=1,
        required_lane_ids=("lane-2",),
    )
    assert blocked.legacy_status == "BLOCKED"
