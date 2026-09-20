from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.outcomes import JobLifecycleState, OpportunityState
from liangjian_funnel.runtime.decision_observability import (
    DataState,
    DecisionObservation,
    EvidenceMetadata,
    TimingSpan,
    TradeEligibility,
    project_decision_axes,
    redact_observability,
    timing_percentiles,
)


TZ = ZoneInfo("Asia/Shanghai")


def test_external_failure_has_independent_job_data_opportunity_and_eligibility_axes():
    axes = project_decision_axes(
        job_status=JobLifecycleState.FAILED,
        data_state=DataState.MISSING,
        opportunity_state=OpportunityState.UNKNOWN,
        critical_data=True,
        reason_codes=("QUOTE_PROVIDER_FAILED",),
    )
    assert axes.job_status is JobLifecycleState.FAILED
    assert axes.data_state is DataState.MISSING
    assert axes.opportunity_state is OpportunityState.UNKNOWN
    assert axes.trade_eligibility is TradeEligibility.BLOCKED_DATA

    no_opportunity = project_decision_axes(
        job_status=JobLifecycleState.SUCCEEDED,
        data_state=DataState.READY,
        opportunity_state=OpportunityState.ABSENT,
        critical_data=True,
    )
    assert no_opportunity.job_status is JobLifecycleState.SUCCEEDED
    assert no_opportunity.trade_eligibility is TradeEligibility.INELIGIBLE


@pytest.mark.parametrize("status", ["PARTIAL", "TIMED_OUT", "INTERRUPTED"])
def test_canonical_job_contract_distinguishes_non_success_terminal_states(status: str):
    axes = project_decision_axes(
        job_status=status,
        data_state=DataState.MISSING,
        opportunity_state=OpportunityState.UNKNOWN,
        critical_data=True,
    )
    assert axes.job_status.value == status


def test_noncritical_degradation_remains_visible_without_blocking_trade():
    axes = project_decision_axes(
        job_status=JobLifecycleState.SUCCEEDED,
        data_state=DataState.MISSING,
        opportunity_state=OpportunityState.PRESENT,
        critical_data=False,
        reason_codes=("AUXILIARY_SOURCE_MISSING",),
    )
    assert axes.data_state is DataState.MISSING
    assert axes.trade_eligibility is TradeEligibility.ELIGIBLE
    assert axes.reason_codes == ("AUXILIARY_SOURCE_MISSING",)


def test_evidence_effective_time_cannot_precede_real_availability():
    published = datetime(2026, 9, 18, 9, 31, tzinfo=TZ)
    fetched = published + timedelta(seconds=2)
    ingested = fetched + timedelta(seconds=1)
    with pytest.raises(ValueError, match="effective_available_at"):
        EvidenceMetadata(
            object_id="600000.SH",
            field="close",
            period="1m",
            provider="TENCENT",
            upstream_source="QQ_FINANCE",
            published_at=published,
            fetched_at=fetched,
            ingested_at=ingested,
            processed_at=ingested,
            effective_available_at=published,
            snapshot_id="minute-1",
            raw_hash="a" * 64,
            completeness="COMPLETE",
            applicable_uses=("A4_EXECUTION",),
        )


def test_observation_hash_excludes_wall_clock_and_timing_but_tracks_frozen_input():
    axes = project_decision_axes(
        job_status=JobLifecycleState.SUCCEEDED,
        data_state=DataState.READY,
        opportunity_state=OpportunityState.ABSENT,
        critical_data=True,
    )
    base = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    first = DecisionObservation(
        run_id="run-a",
        decision_id="decision-a",
        lane_id="lane_1",
        scheduled_at=base,
        started_at=base + timedelta(seconds=1),
        deadline_at=base + timedelta(seconds=50),
        snapshot_ids=("minute-1",),
        required_scope=("600000.SH",),
        ready_scope=("600000.SH",),
        blocked_scope=(),
        no_signal_scope=("600000.SH",),
        timing_spans=(TimingSpan("fetch", 12.0),),
        source_attempts=({"provider": "TENCENT", "status": "READY"},),
        terminal_reason="NO_SIGNAL",
        versions={"git": "abc", "config": "cfg", "prompt": "p", "fill_model": "f", "rules": "r"},
        axes=axes,
    )
    replay = DecisionObservation(
        **{
            **first.__dict__,
            "run_id": "run-b",
            "decision_id": "decision-b",
            "started_at": base + timedelta(seconds=8),
            "timing_spans": (TimingSpan("fetch", 900.0),),
        }
    )
    assert first.decision_hash == replay.decision_hash
    projection = first.to_dict()
    assert projection["timing_coverage"]["required_minute"] == "NOT_MEASURED"
    assert projection["timing_coverage"]["round_total"] == "NOT_MEASURED"

    changed = DecisionObservation(**{**first.__dict__, "snapshot_ids": ("minute-2",)})
    assert changed.decision_hash != first.decision_hash


def test_timing_percentiles_are_measured_from_spans_and_missing_is_explicit():
    assert timing_percentiles([]) == {
        "status": "NOT_MEASURED",
        "count": 0,
        "p50_ms": None,
        "p95_ms": None,
        "p99_ms": None,
    }
    result = timing_percentiles([10.0, 20.0, 30.0, 40.0, 100.0])
    assert result["status"] == "MEASURED"
    assert result["p50_ms"] == 30.0
    assert result["p95_ms"] > result["p50_ms"]
    assert result["p99_ms"] <= 100.0


def test_observability_redaction_removes_secret_and_url_query_values():
    sentinel = "SENTINEL_SECRET_9f99"
    value = {
        "api_key": sentinel,
        "url": f"https://example.invalid/path?token={sentinel}&symbol=600000",
        "nested": [{"authorization": f"Bearer {sentinel}"}],
    }
    redacted = redact_observability(value)
    rendered = str(redacted)
    assert sentinel not in rendered
    assert "token=" not in rendered
    assert redacted["api_key"] == "[REDACTED]"
