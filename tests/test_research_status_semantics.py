from __future__ import annotations

from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
from liangjian_funnel.pipeline.research import (
    STATUS_BLOCKED_TECHNICAL_DATA,
    STATUS_DEGRADED_UNDERFILLED_DATA_GAP,
    STATUS_VALIDATED,
    STATUS_VALIDATED_NO_OPPORTUNITY,
    STATUS_VALIDATED_NO_SETUP,
    STATUS_VALIDATED_UNDERFILLED_MARKET,
    _annotate_a1_pool_target,
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


def test_a1_research_target_underfill_is_diagnostic_not_a_downstream_blocker():
    output = _annotate_a1_pool_target(
        {
            "analysis_summary": {},
            "active_research_pool": [{"symbol": "600000.SH"}],
            "monitor_pool": [],
            "rejected_candidates": [],
        },
        {
            "A1_POOL_TARGETS": {
                "active_research_target": [100, 250],
                "clue_pool_target": [300, 800],
            }
        },
    )

    assert output["analysis_summary"]["reason_codes"] == [
        "A1_ACTIVE_TARGET_UNDERFILLED",
        "A1_CLUE_TARGET_UNDERFILLED",
    ]
    status, reasons = _classify_stage_outcome("A1", output, reasons=())
    assert status == STATUS_VALIDATED
    assert reasons == ()


def test_a2_zero_focus_distinguishes_data_gap_from_no_opportunity():
    blocked, reasons = _classify_stage_outcome(
        "A2",
        _a2_output([], [{"symbol": "600000.SH", "reason_codes": ["A2_FACTOR_COVERAGE_BELOW_MINIMUM"]}]),
        reasons=(),
    )
    assert blocked == STATUS_DEGRADED_UNDERFILLED_DATA_GAP
    assert reasons == ("A2_FACTOR_COVERAGE_BELOW_MINIMUM",)

    no_opportunity, reasons = _classify_stage_outcome("A2", _a2_output([]), reasons=())
    assert no_opportunity == STATUS_VALIDATED_NO_OPPORTUNITY
    assert reasons == ("A2_NO_FOCUS_OPPORTUNITY",)


def test_a2_optional_capital_flow_gap_does_not_masquerade_as_data_insufficiency():
    status, reasons = _classify_stage_outcome(
        "A2",
        _a2_output([], [{"symbol": "600000.SH", "reason_codes": ["A2_CAPITAL_FLOW_UNAVAILABLE"]}]),
        reasons=(),
    )
    assert status == STATUS_VALIDATED_NO_OPPORTUNITY
    assert reasons == ("A2_NO_FOCUS_OPPORTUNITY",)


def test_a2_final_canonical_evidence_gap_blocks_even_when_reviewed_output_is_empty():
    reviewed = _a2_output([])
    output = _a2_output(
        [],
        [
            {
                "symbol": "600000.SH",
                "local_decision": True,
                "sent_to_llm": False,
                "reason_codes": ["A2_FACTOR_COVERAGE_BELOW_MINIMUM"],
            }
        ],
    )

    status, reasons = _classify_stage_outcome(
        "A2",
        output,
        reasons=(),
        reviewed_output=reviewed,
    )

    assert status == STATUS_DEGRADED_UNDERFILLED_DATA_GAP
    assert reasons == ("A2_FACTOR_COVERAGE_BELOW_MINIMUM",)


def test_a2_reviewed_evidence_gap_still_blocks_zero_focus():
    reviewed = _a2_output(
        [],
        [{"symbol": "600000.SH", "reason_codes": ["A2_MARKET_FACTS_INSUFFICIENT"]}],
    )
    output = _a2_output(
        [],
        [
            {
                "symbol": "600000.SH",
                "local_decision": True,
                "sent_to_llm": False,
                "reason_codes": ["A2_FACTOR_COVERAGE_BELOW_MINIMUM"],
            }
        ],
    )

    status, reasons = _classify_stage_outcome(
        "A2",
        output,
        reasons=(),
        reviewed_output=reviewed,
    )

    assert status == STATUS_DEGRADED_UNDERFILLED_DATA_GAP
    assert reasons == ("A2_FACTOR_COVERAGE_BELOW_MINIMUM", "A2_MARKET_FACTS_INSUFFICIENT")


def _a2_gate(
    data_sufficiency_state: str,
    reason_codes: list[str] | None = None,
    *,
    route_ready: bool = True,
) -> DeterministicGateResult:
    decision = {
        "symbol": "600000.SH",
        "status": "REVIEW_CANDIDATE" if route_ready else "DATA_GAP",
        "data_sufficiency_state": data_sufficiency_state,
        "reason_codes": reason_codes or [],
        "route_eligibility": (
            {"MARKET_CORE": {"eligible": True}}
            if route_ready
            else {"MARKET_CORE": {"eligible": False}}
        ),
    }
    return DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=(decision,),
        review_symbols=("600000.SH",) if route_ready else (),
        monitor_symbols=(),
        rejected_symbols=("600000.SH",) if not route_ready else (),
    )


def test_a2_gate_summary_degraded_blocks_zero_focus():
    gate = _a2_gate("DEGRADED")

    status, reasons = _classify_stage_outcome("A2", _a2_output([]), reasons=(), gate=gate)

    assert status == STATUS_DEGRADED_UNDERFILLED_DATA_GAP
    assert reasons == ("A2_DATA_GAP",)


def test_a2_gate_summary_insufficient_blocks_zero_focus():
    gate = _a2_gate(
        "INSUFFICIENT",
        ["A2_CRITICAL_DATA_INSUFFICIENT", "A2_DATA_GAP"],
        route_ready=False,
    )

    status, reasons = _classify_stage_outcome("A2", _a2_output([]), reasons=(), gate=gate)

    assert status == STATUS_DEGRADED_UNDERFILLED_DATA_GAP
    assert reasons == ("A2_CRITICAL_DATA_INSUFFICIENT", "A2_DATA_GAP")


def test_a2_gate_summary_sufficient_zero_focus_is_true_no_opportunity():
    gate = _a2_gate("SUFFICIENT")

    status, reasons = _classify_stage_outcome("A2", _a2_output([]), reasons=(), gate=gate)

    assert status == STATUS_VALIDATED_NO_OPPORTUNITY
    assert reasons == ("A2_NO_FOCUS_OPPORTUNITY",)


def test_a2_optional_capital_flow_degraded_gate_does_not_become_critical_gap():
    gate = _a2_gate("DEGRADED", ["A2_CAPITAL_FLOW_UNAVAILABLE"])

    status, reasons = _classify_stage_outcome("A2", _a2_output([]), reasons=(), gate=gate)

    assert status == STATUS_VALIDATED_NO_OPPORTUNITY
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


def test_a2_hard_reject_data_gap_does_not_downgrade_surviving_market_core_scope():
    """Per-row gaps on an already rejected symbol are not a lane outage."""

    output = _a2_output(
        [{"symbol": "600000.SH", "data_sufficiency_state": "SUFFICIENT"}],
    )
    output["rejected_candidates"] = [{
        "symbol": "000001.SZ",
        "status": "REJECTED",
        "data_sufficiency_state": "INSUFFICIENT",
        "reason_codes": [
            "A2_BOTTLENECK_SCORECARD_MISSING",
            "A2_CRITICAL_DATA_INSUFFICIENT",
        ],
    }]
    gate = DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=(
            {
                "symbol": "600000.SH",
                "status": "REVIEW_CANDIDATE",
                "data_sufficiency_state": "SUFFICIENT",
                "reason_codes": [],
                "route_eligibility": {"MARKET_CORE": {"eligible": True}},
                "eligible_routes": ["MARKET_CORE"],
            },
            {
                "symbol": "000001.SZ",
                "status": "HARD_REJECT",
                "data_sufficiency_state": "INSUFFICIENT",
                "reason_codes": [
                    "A2_BOTTLENECK_SCORECARD_MISSING",
                    "A2_CRITICAL_DATA_INSUFFICIENT",
                ],
                "route_eligibility": {"MARKET_CORE": {"eligible": False}},
                "eligible_routes": [],
            },
        ),
        review_symbols=("600000.SH",),
        monitor_symbols=(),
        rejected_symbols=("000001.SZ",),
    )

    status, reasons = _classify_stage_outcome(
        "A2",
        output,
        reasons=(),
        gate=gate,
    )

    assert status == STATUS_VALIDATED_UNDERFILLED_MARKET
    assert reasons == ("A2_FOCUS_POOL_UNDERFILLED_MARKET",)


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


def test_a3_partial_symbol_data_gap_does_not_block_reviewable_pool():
    output = {
        "core_watch_pool": [],
        "secondary_watch_pool": [{"symbol": "600001.SH"}],
        "rejected_candidates": [
            {
                "symbol": "600000.SH",
                "reason_codes": ["A3_TECHNICAL_FACTORS_NOT_READY"],
            }
        ],
    }
    partial_gate = DeterministicGateResult(
        stage="A3_LOCAL_TECHNICAL",
        decisions=(
            {"symbol": "600001.SH", "reason_codes": ["REWARD_RISK_BELOW_MIN"]},
            {"symbol": "600000.SH", "reason_codes": ["A3_TECHNICAL_FACTORS_NOT_READY"]},
        ),
        review_symbols=("600001.SH",),
        monitor_symbols=(),
        rejected_symbols=("600000.SH",),
    )

    status, reasons = _classify_stage_outcome(
        "A3",
        output,
        reasons=("A3_TECHNICAL_FACTORS_NOT_READY",),
        gate=partial_gate,
    )

    assert status == STATUS_VALIDATED_NO_SETUP
    assert reasons == ("A3_NO_TECHNICAL_SETUP",)
