import json

import pytest

from liangjian_funnel.pipeline.outcomes import (
    OUTCOME_SCHEMA_VERSION,
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
    StageOutcome,
    aggregate_lane_outcome,
    aggregate_run_outcome,
    aggregate_workflow_acceptance,
    cli_exit_code,
    stage_outcome_from_legacy,
)


def test_fully_evaluated_zero_a2_is_validated_absent_not_blocked() -> None:
    outcome = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY",
        stage="A2",
        counts={"input": 239, "evaluated": 239, "selected": 0},
        data_coverage={"required": 0.98, "actual": 1.0},
    )

    assert outcome.lifecycle_state is LifecycleState.TERMINAL
    assert outcome.quality_state is QualityState.VALIDATED
    assert outcome.opportunity_state is OpportunityState.ABSENT
    assert outcome.publication_state is PublicationState.NOT_APPLICABLE
    assert outcome.reason_codes == ("A2_NO_FOCUS_OPPORTUNITY",)
    assert outcome.legacy_status == "VALIDATED_NO_OPPORTUNITY"


def test_zero_result_with_explicit_insufficient_coverage_is_unknown() -> None:
    outcome = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY",
        stage="A2",
        counts={"input": 239, "evaluated": 12, "selected": 0},
    )

    assert outcome.quality_state is QualityState.BLOCKED
    assert outcome.opportunity_state is OpportunityState.UNKNOWN
    assert "DATA_COVERAGE_INSUFFICIENT" in outcome.reason_codes
    assert outcome.publication_state is PublicationState.NOT_APPLICABLE


def test_explicit_zero_a2_input_is_not_treated_as_a_real_no_opportunity() -> None:
    outcome = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY",
        stage="A2",
        counts={"input": 0, "evaluated": 0, "selected": 0},
    )

    assert outcome.quality_state is QualityState.BLOCKED
    assert outcome.opportunity_state is OpportunityState.UNKNOWN
    assert "DATA_COVERAGE_INSUFFICIENT" in outcome.reason_codes


@pytest.mark.parametrize(
    ("legacy", "quality", "opportunity", "publication", "reason"),
    (
        ("BLOCKED_EVIDENCE_GAP", QualityState.BLOCKED, OpportunityState.UNKNOWN, PublicationState.BLOCKED, "EVIDENCE_GAP"),
        ("BLOCKED_DATA_COVERAGE", QualityState.BLOCKED, OpportunityState.UNKNOWN, PublicationState.BLOCKED, "DATA_COVERAGE_INSUFFICIENT"),
        ("BLOCKED_MODEL", QualityState.FAILED, OpportunityState.UNKNOWN, PublicationState.BLOCKED, "MODEL_CALL_FAILED"),
        ("NOT_RUN_UPSTREAM_BLOCKED", QualityState.BLOCKED, OpportunityState.NOT_APPLICABLE, PublicationState.BLOCKED, "UPSTREAM_STAGE_BLOCKED"),
        ("CANCELLED", QualityState.CANCELLED, OpportunityState.UNKNOWN, PublicationState.BLOCKED, "RUN_CANCELLED"),
    ),
)
def test_blocked_failure_upstream_and_cancelled_matrix(
    legacy: str,
    quality: QualityState,
    opportunity: OpportunityState,
    publication: PublicationState,
    reason: str,
) -> None:
    outcome = stage_outcome_from_legacy(legacy, stage="A2")
    assert outcome.quality_state is quality
    assert outcome.opportunity_state is opportunity
    assert outcome.publication_state is publication
    assert reason in outcome.reason_codes


def test_generic_validated_zero_requires_full_evaluation_before_absent() -> None:
    incomplete = stage_outcome_from_legacy(
        "VALIDATED",
        stage="A2",
        counts={"input": 100, "evaluated": 99, "selected": 0},
    )
    complete = stage_outcome_from_legacy(
        "VALIDATED",
        stage="A2",
        counts={"input": 100, "evaluated": 100, "selected": 0},
    )
    assert incomplete.opportunity_state is OpportunityState.UNKNOWN
    assert complete.opportunity_state is OpportunityState.ABSENT


def test_lane_aggregation_uses_worst_quality_but_preserves_present_opportunity() -> None:
    stages = (
        stage_outcome_from_legacy("VALIDATED", stage="A1", counts={"input": 5000, "evaluated": 5000, "selected": 239}),
        stage_outcome_from_legacy("VALIDATED_UNDERFILLED_MARKET", stage="A2", counts={"input": 239, "evaluated": 239, "selected": 5}),
        stage_outcome_from_legacy("VALIDATED_NO_SETUP", stage="A3", counts={"input": 5, "evaluated": 5, "selected": 0}),
    )
    lane = aggregate_lane_outcome(stages, lane_id="lane_1", model="primary")

    assert lane.lifecycle_state is LifecycleState.TERMINAL
    assert lane.quality_state is QualityState.DEGRADED
    assert lane.opportunity_state is OpportunityState.PRESENT
    assert lane.publication_state is PublicationState.READY
    assert lane.legacy_status == "READY_DEGRADED"
    assert lane.counts == {"completed_stages": 3, "evaluated": 5, "input": 5000, "selected": 0, "stage_count": 3}


def test_primary_lane_controls_run_publication_optional_failure_does_not_block() -> None:
    primary = aggregate_lane_outcome(
        (
            stage_outcome_from_legacy("VALIDATED", stage="A1", counts={"input": 1, "evaluated": 1, "selected": 1}),
            stage_outcome_from_legacy("VALIDATED", stage="A2", counts={"input": 1, "evaluated": 1, "selected": 1}),
            stage_outcome_from_legacy("VALIDATED", stage="A3", counts={"input": 1, "evaluated": 1, "selected": 1}),
        ),
        lane_id="lane_1",
        model="primary",
    )
    comparison = aggregate_lane_outcome((), lane_id="lane_2", model="shadow", legacy_status="FAILED")
    result = aggregate_run_outcome(
        (primary, comparison),
        run_id="run-1",
        primary_lane_ids=("lane_1",),
        expected_lane_count=2,
    )

    assert result.publication_state is PublicationState.READY
    assert result.quality_state is QualityState.VALIDATED
    assert result.legacy_status == "READY_DEGRADED"
    assert result.comparison_status == "BLOCKED"
    assert "PRIMARY_MODEL_FAILED" not in result.reason_codes


def test_primary_failure_is_failed_even_when_shadow_is_ready() -> None:
    primary = aggregate_lane_outcome((), lane_id="lane_1", legacy_status="FAILED")
    shadow = aggregate_lane_outcome((), lane_id="lane_2", legacy_status="PUBLISHED")
    result = aggregate_run_outcome((primary, shadow), run_id="run-2", expected_lane_count=2)

    assert result.quality_state is QualityState.FAILED
    assert result.publication_state is PublicationState.BLOCKED
    assert result.legacy_status == "PARTIAL"
    assert result.comparison_status == "READY"


def test_missing_primary_is_blocked_and_not_inferred_from_optional_lane() -> None:
    shadow = aggregate_lane_outcome((), lane_id="lane_2", legacy_status="PUBLISHED")
    result = aggregate_run_outcome((shadow,), run_id="run-3", expected_lane_count=2)

    assert result.quality_state is QualityState.BLOCKED
    assert result.publication_state is PublicationState.BLOCKED
    assert result.reason_codes == ("PRIMARY_LANE_MISSING",)
    assert result.legacy_status == "PARTIAL"


def test_workflow_acceptance_keeps_legacy_shape_and_latest_run_selection() -> None:
    rows = (
        {"run_id": "new", "lane_id": "lane_1", "status": "READY_TO_PUBLISH"},
        {"run_id": "new", "lane_id": "lane_2", "status": "PUBLISHED"},
        {"run_id": "new", "lane_id": "lane_3", "status": "READY_TO_PUBLISH"},
        {"run_id": "old", "lane_id": "lane_1", "status": "BLOCKED"},
    )
    result = aggregate_workflow_acceptance(rows)
    assert result.legacy_status == "READY"
    assert result.to_legacy_acceptance() == {
        "status": "READY",
        "run_id": "new",
        "expected_lanes": 3,
        "recorded_lanes": 3,
        "ready_lanes": 3,
        "required_lanes": 1,
        "ready_required_lanes": 1,
    }
    assert result.counts == {
        "expected_lanes": 3,
        "ready_lanes": 3,
        "ready_required_lanes": 1,
        "recorded_lanes": 3,
        "required_lanes": 1,
    }


def test_workflow_acceptance_distinguishes_primary_ready_from_all_comparisons_missing() -> None:
    result = aggregate_workflow_acceptance(
        ({"run_id": "new", "lane_id": "lane_1", "status": "READY_TO_PUBLISH"},),
        expected_lanes=3,
    )
    assert result.legacy_status == "PARTIAL"
    assert result.publication_state is PublicationState.READY
    assert result.to_legacy_acceptance()["ready_required_lanes"] == 1


def test_no_rows_is_queued_not_a_fake_blocked_failure() -> None:
    result = aggregate_workflow_acceptance((), expected_lanes=3)
    assert result.lifecycle_state is LifecycleState.QUEUED
    assert result.publication_state is PublicationState.NOT_APPLICABLE
    assert result.legacy_status == "NOT_RUN"
    assert result.to_legacy_acceptance()["recorded_lanes"] == 0


def test_serialization_is_stable_and_round_trips() -> None:
    stage = stage_outcome_from_legacy(
        "VALIDATED_NO_OPPORTUNITY",
        stage="A2",
        reason_codes=("custom_reason", "CUSTOM_REASON"),
        counts={"selected": 0, "evaluated": 10, "input": 10},
        data_coverage={"actual": 1.0, "required": 0.9},
    )
    payload = stage.as_dict()
    assert payload["schema_version"] == OUTCOME_SCHEMA_VERSION
    assert payload["reason_codes"] == ["CUSTOM_REASON", "A2_NO_FOCUS_OPPORTUNITY"]
    assert json.loads(stage.to_json()) == payload
    assert StageOutcome.from_mapping(payload) == stage


def test_run_serialization_round_trips() -> None:
    lane = aggregate_lane_outcome((), lane_id="lane_1", legacy_status="PUBLISHED")
    run = aggregate_run_outcome((lane,), run_id="r", expected_lane_count=1)
    assert RunOutcome.from_mapping(run.as_dict()) == run
    assert LaneOutcome.from_mapping(lane.as_dict()) == lane


@pytest.mark.parametrize(
    ("outcome", "expected"),
    (
        (aggregate_run_outcome((), expected_lane_count=1), CliExitCode.BUSINESS_BLOCKED),
        (aggregate_run_outcome((aggregate_lane_outcome((), lane_id="lane_1", legacy_status="BLOCKED"),), expected_lane_count=1), CliExitCode.BUSINESS_BLOCKED),
        (aggregate_run_outcome((aggregate_lane_outcome((), lane_id="lane_1", legacy_status="FAILED"),), expected_lane_count=1), CliExitCode.TECHNICAL_FAILURE),
        (aggregate_run_outcome((aggregate_lane_outcome((), lane_id="lane_1", legacy_status="CANCELLED"),), expected_lane_count=1), CliExitCode.CANCELLED),
    ),
)
def test_cli_exit_code_matrix(outcome: RunOutcome, expected: CliExitCode) -> None:
    assert cli_exit_code(outcome) == int(expected)


def test_cli_exit_code_keeps_legacy_payload_compatibility() -> None:
    assert cli_exit_code({"status": "READY"}) == 0
    assert cli_exit_code({"status": "BLOCKED"}) == 2


def test_a2_data_gap_is_degraded_and_blocks_only_a3_actionability() -> None:
    a1 = stage_outcome_from_legacy(
        "VALIDATED",
        stage="A1",
        counts={"input": 5000, "evaluated": 5000, "selected": 40},
    )
    a2 = stage_outcome_from_legacy(
        "DATA_GAP",
        stage="A2",
        reason_codes=("A2_DATA_GAP",),
        counts={"input": 40, "evaluated": 40, "selected": 0},
    )
    a3 = stage_outcome_from_legacy(
        "NOT_RUN_UPSTREAM_BLOCKED",
        stage="A3",
        reason_codes=("A2_DATA_GAP",),
    )

    assert a2.quality_state is QualityState.DEGRADED
    assert a2.data_sufficiency_state is DataSufficiencyState.INSUFFICIENT
    assert a2.focus_opportunity_state is OpportunityState.UNKNOWN
    assert a2.actionability_state is ActionabilityState.NOT_APPLICABLE
    assert a2.publication_state is PublicationState.READY
    assert a3.quality_state is QualityState.DEGRADED
    assert a3.actionability_state is ActionabilityState.NOT_APPLICABLE
    assert a3.publication_state is PublicationState.NOT_APPLICABLE
    assert a3.job_status is JobLifecycleState.SUCCEEDED

    lane = aggregate_lane_outcome((a1, a2, a3), lane_id="lane_1", model="primary")
    run = aggregate_run_outcome((lane,), run_id="data-gap", expected_lane_count=1)
    for result in (lane, run):
        assert result.quality_state is QualityState.DEGRADED
        assert result.publication_state is PublicationState.READY
        assert result.focus_opportunity_state is OpportunityState.UNKNOWN
        assert result.actionability_state is ActionabilityState.NOT_APPLICABLE
        assert result.legacy_status == "READY_DEGRADED"
        assert result.job_status is JobLifecycleState.SUCCEEDED


def test_run_v3_does_not_promote_a1_research_to_a3_actionability() -> None:
    lane = aggregate_lane_outcome(
        (
            stage_outcome_from_legacy("VALIDATED", stage="A1", counts={"input": 10, "evaluated": 10, "selected": 1}),
            stage_outcome_from_legacy("VALIDATED_NO_OPPORTUNITY", stage="A2", counts={"input": 1, "evaluated": 1, "selected": 0}),
            stage_outcome_from_legacy("NOT_RUN_UPSTREAM_BLOCKED", stage="A3", reason_codes=("UPSTREAM_STAGE_BLOCKED",)),
        ),
        lane_id="lane_1",
        model="primary",
    )
    run = aggregate_run_outcome((lane,), run_id="axes", expected_lane_count=1)
    assert run.research_opportunity_state is OpportunityState.PRESENT
    assert run.focus_opportunity_state is OpportunityState.ABSENT
    assert run.actionability_state is ActionabilityState.NOT_APPLICABLE
    payload = run.as_dict()
    assert payload["schema_version"] == OUTCOME_SCHEMA_VERSION
    assert "opportunity_state" not in payload
    assert payload["legacy_projection"]["opportunity_state"] == run.opportunity_state.value
