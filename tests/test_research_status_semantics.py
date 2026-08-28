from __future__ import annotations

from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
from liangjian_funnel.pipeline.research import (
    STATUS_BLOCKED_EVIDENCE_GAP,
    STATUS_BLOCKED_TECHNICAL_DATA,
    STATUS_DEGRADED_UNDERFILLED_DATA_GAP,
    STATUS_VALIDATED_NO_OPPORTUNITY,
    STATUS_VALIDATED_NO_SETUP,
    STATUS_VALIDATED_UNDERFILLED_MARKET,
    _classify_stage_outcome,
)


def _a2_output(focus: list[dict], watch: list[dict] | None = None) -> dict:
    return {
        "analysis_summary": {"pool_target": {"minimum": 30, "maximum": 80}},
        "active_themes": [],
        "focus_pool": focus,
        "watch_only_pool": watch or [],
        "rejected_candidates": [],
    }


def test_a2_zero_focus_distinguishes_data_gap_from_no_opportunity():
    blocked, reasons = _classify_stage_outcome(
        "A2",
        _a2_output([], [{"symbol": "600000.SH", "reason_codes": ["A2_FACTOR_COVERAGE_BELOW_MINIMUM"]}]),
        reasons=(),
    )
    assert blocked == STATUS_BLOCKED_EVIDENCE_GAP
    assert reasons == ("A2_FACTOR_COVERAGE_BELOW_MINIMUM",)

    no_opportunity, reasons = _classify_stage_outcome("A2", _a2_output([]), reasons=())
    assert no_opportunity == STATUS_VALIDATED_NO_OPPORTUNITY
    assert reasons == ("A2_NO_FOCUS_OPPORTUNITY",)


def test_a2_underfilled_distinguishes_market_from_data_gap():
    market, _ = _classify_stage_outcome(
        "A2",
        _a2_output([{"symbol": "600000.SH"}]),
        reasons=(),
    )
    assert market == STATUS_VALIDATED_UNDERFILLED_MARKET

    data_gap, reasons = _classify_stage_outcome(
        "A2",
        _a2_output([{"symbol": "600000.SH", "reason_codes": ["A2_MARKET_FACTS_INSUFFICIENT"]}]),
        reasons=(),
    )
    assert data_gap == STATUS_DEGRADED_UNDERFILLED_DATA_GAP
    assert reasons == ("A2_MARKET_FACTS_INSUFFICIENT",)


def test_a3_zero_plan_distinguishes_missing_technical_data_from_no_setup():
    output = {"core_watch_pool": [], "secondary_watch_pool": [], "rejected_candidates": []}
    blocked_gate = DeterministicGateResult(
        stage="A3_LOCAL_TECHNICAL",
        decisions=({"symbol": "600000.SH", "reason_codes": ["A3_TECHNICAL_FACTORS_NOT_READY"]},),
        review_symbols=(),
        monitor_symbols=(),
        rejected_symbols=("600000.SH",),
    )
    blocked, reasons = _classify_stage_outcome("A3", output, reasons=(), gate=blocked_gate)
    assert blocked == STATUS_BLOCKED_TECHNICAL_DATA
    assert reasons == ("A3_TECHNICAL_FACTORS_NOT_READY",)

    valid_gate = DeterministicGateResult(
        stage="A3_LOCAL_TECHNICAL",
        decisions=(),
        review_symbols=(),
        monitor_symbols=(),
        rejected_symbols=(),
    )
    no_setup, reasons = _classify_stage_outcome("A3", output, reasons=(), gate=valid_gate)
    assert no_setup == STATUS_VALIDATED_NO_SETUP
    assert reasons == ("A3_NO_TECHNICAL_SETUP",)
