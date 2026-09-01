from __future__ import annotations

from liangjian_funnel.pipeline.a1_selection_logic import (
    CYCLICAL_UPSWING,
    DEFENSIVE_QUALITY,
    FUNDAMENTAL,
    GROWTH_TREND,
    UNKNOWN,
    UNCLASSIFIED,
    build_a1_selection_evidence,
    classify_company_archetype,
    classify_pullback,
    evaluate_financial_quality,
)


def test_archetype_classifies_cyclical_company_from_explicit_regime_and_sector() -> None:
    result = classify_company_archetype(
        {"market_regime": "REFLATION", "as_of": "2026-08-31"},
        {"industry_group": "metals"},
    )

    assert result.classification == CYCLICAL_UPSWING
    assert result.reason_codes == ("A1_ARCHETYPE_CYCLICAL_UPSWING",)
    assert result.data_gaps == ()
    assert result.as_dict()["evidence"]["matched_tokens"][CYCLICAL_UPSWING]


def test_archetype_classifies_growth_and_defensive_explicit_fields() -> None:
    growth = classify_company_archetype(
        {"quadrant": "RISK_ON_GROWTH"},
        {"industry": "innovation medicine"},
    )
    defensive = classify_company_archetype(
        {"leading_asset": "CASH_DEFENSIVE"},
        {"style": "quality"},
    )

    assert growth.classification == GROWTH_TREND
    assert defensive.classification == DEFENSIVE_QUALITY


def test_archetype_does_not_assume_when_data_is_missing_or_conflicting() -> None:
    missing = classify_company_archetype(None, {"name": "unknown"})
    conflict = classify_company_archetype(
        {"market_style": "RISK_ON_GROWTH"},
        {"industry_group": "energy", "style": "tech"},
    )

    assert missing.classification == UNCLASSIFIED
    assert "MARKET_REGIME_OR_COMPANY_STYLE" in missing.data_gaps
    assert conflict.classification == UNCLASSIFIED
    assert conflict.reason_codes == ("A1_ARCHETYPE_EVIDENCE_CONFLICT",)


def test_pullback_requires_explicit_fundamental_deterioration() -> None:
    result = classify_pullback(
        {
            "cashflow_deterioration": True,
            "earnings_revision_down": True,
            "source_refs": ["cninfo:2026-08-30"],
        }
    )

    assert result.classification == FUNDAMENTAL
    assert result.reason_codes == ("A1_PULLBACK_FUNDAMENTAL",)
    assert result.data_gaps == ()
    assert set(result.evidence["confirmed_indicators"][FUNDAMENTAL]) == {
        "cashflow_deterioration",
        "earnings_revision_down",
    }


def test_pullback_missing_or_conflicting_evidence_is_unknown() -> None:
    missing = classify_pullback({"market_drawdown_confirmed": False})
    conflict = classify_pullback(
        {
            "market_drawdown_confirmed": True,
            "guidance_cut": True,
        }
    )

    assert missing.classification == UNKNOWN
    assert "SYSTEMIC_EVIDENCE" not in missing.data_gaps  # False is supplied, not absent.
    assert "PULLBACK_CAUSE_EVIDENCE" in missing.data_gaps
    assert conflict.classification == UNKNOWN
    assert conflict.reason_codes == ("A1_PULLBACK_CAUSE_CONFLICT",)


def test_financial_coverage_preserves_negative_growth_and_missing_values() -> None:
    result = evaluate_financial_quality(
        {
            "revenue_growth": -0.12,
            "profit_growth": [0.21, -0.04],
            "operating_cash_flow": 100,
            "roe": None,
            "debt_ratio": "not-published",
        }
    )

    assert result.available == ("operating_cash_flow", "profit_growth", "revenue_growth")
    assert result.required == ("debt_ratio", "operating_cash_flow", "profit_growth", "revenue_growth", "roe")
    assert result.missing == ("debt_ratio", "roe")
    assert result.coverage_ratio == 0.6
    assert result.evidence["numeric_values"]["revenue_growth"] == -0.12
    assert result.evidence["negative_growth"]["revenue_growth"] == [-0.12]
    assert result.evidence["negative_growth"]["profit_growth"] == [-0.04]
    assert "A1_FINANCIAL_NEGATIVE_GROWTH_PRESENT" in result.reason_codes


def test_combined_envelope_is_audit_evidence_and_never_selection() -> None:
    result = build_a1_selection_evidence(
        market_regime={"market_style": "GROWTH"},
        company={"industry": "AI"},
        pullback={"relative_strength_breakdown": True},
        financial_metrics={"revenue_growth": 0.2},
    )

    assert result["selection_performed"] is False
    assert result["archetype"]["classification"] == GROWTH_TREND
    assert result["pullback"]["classification"] == "STRUCTURAL"
    assert result["financial_quality"]["coverage_ratio"] == 0.2
    assert "A1_FINANCIAL_COVERAGE_INCOMPLETE" in result["reason_codes"]
