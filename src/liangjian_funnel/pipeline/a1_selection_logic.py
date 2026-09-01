"""Small, deterministic A1 evidence classifiers.

The A1 stage has two different responsibilities which are easy to mix up:
describing the market/company archetype and recording whether the available
facts are sufficient to reason about a pullback or financial quality.  This
module keeps those responsibilities as pure, dependency-free functions.

There is deliberately no aggregate score and no stock-selection decision in
this file.  A missing fact is represented as a data gap; it is never turned
into a bearish value, and it is never silently treated as proof of a positive
case.  Callers can therefore use the returned evidence in an audit trail or
pass it to a later, explicitly owned selection rule.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import math
from typing import Any


MODULE_VERSION = "a1-selection-logic/1.0.0"

CYCLICAL_UPSWING = "CYCLICAL_UPSWING"
GROWTH_TREND = "GROWTH_TREND"
DEFENSIVE_QUALITY = "DEFENSIVE_QUALITY"
UNCLASSIFIED = "UNCLASSIFIED"

SYSTEMIC = "SYSTEMIC"
STRUCTURAL = "STRUCTURAL"
FUNDAMENTAL = "FUNDAMENTAL"
UNKNOWN = "UNKNOWN"

COMPANY_ARCHETYPES = (
    CYCLICAL_UPSWING,
    GROWTH_TREND,
    DEFENSIVE_QUALITY,
    UNCLASSIFIED,
)
PULLBACK_CAUSES = (SYSTEMIC, STRUCTURAL, FUNDAMENTAL, UNKNOWN)

# These are labels, not a hidden scoring model.  A caller may provide one of
# these explicit normalized labels, or the classifier may use a clearly
# labelled regime/style field to translate an upstream market decision.
_CYCLICAL_TOKENS = frozenset(
    {
        "CYCLICAL",
        "CYCLICAL_UPSWING",
        "COMMODITY",
        "COMMODITIES",
        "INFLATION",
        "REFLATION",
        "VALUE_CYCLICAL",
        "RISK_ON_VALUE",
        "MATERIALS",
        "ENERGY",
        "CHEMICAL",
        "METALS",
        "COAL",
    }
)
_GROWTH_TOKENS = frozenset(
    {
        "GROWTH",
        "GROWTH_TREND",
        "RISK_ON_GROWTH",
        "TECHNOLOGY",
        "TECH",
        "INNOVATION",
        "INNOVATION_MEDICINE",
        "AI",
    }
)
_DEFENSIVE_TOKENS = frozenset(
    {
        "DEFENSIVE",
        "DEFENSIVE_QUALITY",
        "QUALITY",
        "DIVIDEND",
        "RED_CHIP",
        "BOND_DEFENSIVE",
        "CASH_DEFENSIVE",
        "RISK_OFF",
        "LOW_VOLATILITY",
    }
)

DEFAULT_FINANCIAL_REQUIREMENTS = (
    "revenue_growth",
    "profit_growth",
    "operating_cash_flow",
    "roe",
    "debt_ratio",
)

_PULLBACK_INDICATORS: dict[str, tuple[str, ...]] = {
    SYSTEMIC: (
        "market_drawdown_confirmed",
        "index_drawdown_confirmed",
        "market_breadth_deteriorated",
        "systemic_risk_event",
    ),
    STRUCTURAL: (
        "sector_drawdown_confirmed",
        "relative_strength_breakdown",
        "sector_rotation_out_confirmed",
        "industry_demand_softening",
    ),
    FUNDAMENTAL: (
        "earnings_revision_down",
        "cashflow_deterioration",
        "guidance_cut",
        "audit_or_fraud_issue",
        "fundamental_deterioration_confirmed",
    ),
}


def _unique_sorted(values: Sequence[Any]) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in values if str(value).strip()}))


def _text(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def _token(value: Any) -> str | None:
    text = _text(value)
    return text.upper().replace("-", "_").replace(" ", "_") if text else None


def _finite_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _bool_true(value: Any) -> bool:
    return value is True


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _safe_evidence(value: Any) -> Any:
    """Keep returned evidence JSON-friendly and bounded to scalar values."""

    if isinstance(value, Mapping):
        return {
            str(key): _safe_evidence(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in {"raw", "payload", "records", "items"}
        }
    if _is_sequence(value):
        return [_safe_evidence(item) for item in list(value)[:100]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


@dataclass(frozen=True, slots=True)
class EvidenceAssessment:
    """Stable result shared by the archetype and pullback classifiers."""

    classification: str
    reason_codes: tuple[str, ...] = ()
    data_gaps: tuple[str, ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "reason_codes": list(self.reason_codes),
            "data_gaps": list(self.data_gaps),
            "evidence": _safe_evidence(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class FinancialQualityAssessment:
    """Coverage and signs of financial facts, without a quality score."""

    available: tuple[str, ...]
    required: tuple[str, ...]
    missing: tuple[str, ...]
    coverage_ratio: float
    reason_codes: tuple[str, ...] = ()
    data_gaps: tuple[str, ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)

    @property
    def status(self) -> str:
        if not self.required:
            return "NOT_REQUIRED"
        if not self.available:
            return "UNAVAILABLE"
        return "READY" if not self.missing else "PARTIAL"

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "available": list(self.available),
            "required": list(self.required),
            "missing": list(self.missing),
            "coverage_ratio": self.coverage_ratio,
            "reason_codes": list(self.reason_codes),
            "data_gaps": list(self.data_gaps),
            "evidence": _safe_evidence(self.evidence),
        }


def classify_company_archetype(
    market_regime: Mapping[str, Any] | str | None,
    company: Mapping[str, Any] | None = None,
) -> EvidenceAssessment:
    """Classify a company against an explicit market regime.

    The function requires a single unambiguous family.  It first honours an
    explicit normalized ``archetype``/``preferred_archetype`` field, then
    checks the regime/style and company descriptors.  If there is no explicit
    evidence or the two sides conflict, ``UNCLASSIFIED`` is returned with a
    data gap/reason code.  No ranking or selection occurs here.
    """

    regime = market_regime if isinstance(market_regime, Mapping) else {"regime": market_regime}
    company_data = company if isinstance(company, Mapping) else {}
    explicit = _token(
        company_data.get("archetype")
        or company_data.get("company_archetype")
        or regime.get("preferred_archetype")
        or regime.get("archetype")
    )
    if explicit in COMPANY_ARCHETYPES and explicit != UNCLASSIFIED:
        return EvidenceAssessment(
            classification=explicit,
            reason_codes=("A1_ARCHETYPE_EXPLICIT",),
            evidence={"archetype": explicit, "source": "explicit_normalized_field"},
        )

    tokens: set[str] = set()
    for source in (regime, company_data):
        for key in (
            "regime",
            "market_regime",
            "market_style",
            "cycle_phase",
            "leading_asset",
            "quadrant",
            "industry_group",
            "industry",
            "sector",
            "theme",
            "style",
        ):
            value = source.get(key)
            if _is_sequence(value):
                tokens.update(token for token in (_token(item) for item in value) if token)
            else:
                token = _token(value)
                if token:
                    tokens.add(token)

    families = {
        CYCLICAL_UPSWING: sorted(tokens.intersection(_CYCLICAL_TOKENS)),
        GROWTH_TREND: sorted(tokens.intersection(_GROWTH_TOKENS)),
        DEFENSIVE_QUALITY: sorted(tokens.intersection(_DEFENSIVE_TOKENS)),
    }
    matched = [family for family, values in families.items() if values]
    evidence = {
        "matched_tokens": {family: values for family, values in families.items() if values},
        "regime": _safe_evidence(regime),
        "company": _safe_evidence(company_data),
    }
    if len(matched) == 1:
        family = matched[0]
        return EvidenceAssessment(
            classification=family,
            reason_codes=(f"A1_ARCHETYPE_{family}",),
            evidence=evidence,
        )
    if len(matched) > 1:
        return EvidenceAssessment(
            classification=UNCLASSIFIED,
            reason_codes=("A1_ARCHETYPE_EVIDENCE_CONFLICT",),
            data_gaps=("UNAMBIGUOUS_REGIME_COMPANY_MAPPING",),
            evidence=evidence,
        )
    return EvidenceAssessment(
        classification=UNCLASSIFIED,
        reason_codes=("A1_ARCHETYPE_UNCLASSIFIED",),
        data_gaps=("MARKET_REGIME_OR_COMPANY_STYLE",),
        evidence=evidence,
    )


def classify_pullback(evidence: Mapping[str, Any] | None) -> EvidenceAssessment:
    """Classify a pullback only from explicit, confirmed evidence.

    False, ``None``, absent, and non-boolean values do not prove a cause.
    When multiple causes are confirmed, the result is ``UNKNOWN`` instead of
    arbitrarily selecting one.  This is intentional: downstream policy may
    choose how to handle an ambiguous pullback, but A1 must preserve the
    ambiguity and its evidence.
    """

    data = evidence if isinstance(evidence, Mapping) else {}
    confirmed: list[str] = []
    indicator_evidence: dict[str, list[str]] = {}
    for cause, indicators in _PULLBACK_INDICATORS.items():
        hits = [indicator for indicator in indicators if _bool_true(data.get(indicator))]
        if hits:
            confirmed.append(cause)
            indicator_evidence[cause] = hits

    direct_cause = _token(data.get("cause"))
    if (
        direct_cause in (SYSTEMIC, STRUCTURAL, FUNDAMENTAL)
        and _bool_true(data.get("cause_confirmed"))
    ):
        confirmed.append(direct_cause)
        indicator_evidence.setdefault(direct_cause, []).append("cause_confirmed")

    normalized = sorted(set(confirmed))
    safe = {"confirmed_indicators": indicator_evidence}
    if len(normalized) == 1:
        cause = normalized[0]
        return EvidenceAssessment(
            classification=cause,
            reason_codes=(f"A1_PULLBACK_{cause}",),
            evidence=safe,
        )
    if len(normalized) > 1:
        return EvidenceAssessment(
            classification=UNKNOWN,
            reason_codes=("A1_PULLBACK_CAUSE_CONFLICT",),
            data_gaps=("PULLBACK_PRIMARY_CAUSE",),
            evidence={**safe, "confirmed_causes": normalized},
        )
    missing = tuple(
        f"{cause}_EVIDENCE"
        for cause, indicators in _PULLBACK_INDICATORS.items()
        if not any(key in data for key in indicators)
    )
    gaps = _unique_sorted(missing + ("PULLBACK_CAUSE_EVIDENCE",))
    return EvidenceAssessment(
        classification=UNKNOWN,
        reason_codes=("A1_PULLBACK_CAUSE_UNKNOWN", "A1_PULLBACK_EVIDENCE_INSUFFICIENT"),
        data_gaps=gaps,
        evidence=safe,
    )


def evaluate_financial_quality(
    metrics: Mapping[str, Any] | None,
    required_metrics: Sequence[str] = DEFAULT_FINANCIAL_REQUIREMENTS,
) -> FinancialQualityAssessment:
    """Return point-in-time coverage and growth signs for financial facts.

    A metric is available when it contains at least one finite numeric
    observation.  For a series, all finite values are preserved in evidence;
    in particular, a negative growth value remains negative.  There is no
    ``abs`` normalization, quality score, or pass/fail selection rule here.
    """

    data = metrics if isinstance(metrics, Mapping) else {}
    required = _unique_sorted(required_metrics)
    available: list[str] = []
    missing: list[str] = []
    numeric_evidence: dict[str, Any] = {}
    negative_growth: dict[str, list[float]] = {}
    for name in required:
        raw = data.get(name)
        values: list[float]
        if _is_sequence(raw):
            values = [number for item in raw if (number := _finite_number(item)) is not None]
        else:
            number = _finite_number(raw)
            values = [] if number is None else [number]
        if not values:
            missing.append(name)
            continue
        available.append(name)
        numeric_evidence[name] = values if _is_sequence(raw) else values[0]
        if "GROWTH" in name.upper() or "YOY" in name.upper() or name.lower().endswith("_growth"):
            negatives = [value for value in values if value < 0]
            if negatives:
                negative_growth[name] = negatives

    coverage_ratio = round(len(available) / len(required), 6) if required else 1.0
    reasons: list[str] = []
    gaps = list(missing)
    if missing:
        reasons.append("A1_FINANCIAL_COVERAGE_INCOMPLETE")
        gaps.append("FINANCIAL_REQUIRED_METRICS")
    if negative_growth:
        reasons.append("A1_FINANCIAL_NEGATIVE_GROWTH_PRESENT")
    if not available and required:
        reasons.append("A1_FINANCIAL_DATA_UNAVAILABLE")
    return FinancialQualityAssessment(
        available=tuple(available),
        required=required,
        missing=tuple(missing),
        coverage_ratio=coverage_ratio,
        reason_codes=_unique_sorted(reasons),
        data_gaps=_unique_sorted(gaps),
        evidence={
            "numeric_values": numeric_evidence,
            "negative_growth": negative_growth,
            "missing_values_are_not_zero": True,
            "negative_growth_preserved": True,
        },
    )


def build_a1_selection_evidence(
    *,
    market_regime: Mapping[str, Any] | str | None,
    company: Mapping[str, Any] | None = None,
    pullback: Mapping[str, Any] | None = None,
    financial_metrics: Mapping[str, Any] | None = None,
    required_financial_metrics: Sequence[str] = DEFAULT_FINANCIAL_REQUIREMENTS,
) -> dict[str, Any]:
    """Build one auditable A1 evidence envelope without selecting a stock."""

    archetype = classify_company_archetype(market_regime, company)
    pullback_result = classify_pullback(pullback)
    financial = evaluate_financial_quality(financial_metrics, required_financial_metrics)
    reason_codes = _unique_sorted(
        archetype.reason_codes + pullback_result.reason_codes + financial.reason_codes
    )
    data_gaps = _unique_sorted(
        archetype.data_gaps + pullback_result.data_gaps + financial.data_gaps
    )
    return {
        "module_version": MODULE_VERSION,
        "archetype": archetype.as_dict(),
        "pullback": pullback_result.as_dict(),
        "financial_quality": financial.as_dict(),
        "reason_codes": list(reason_codes),
        "data_gaps": list(data_gaps),
        "evidence": {
            "market_regime": _safe_evidence(market_regime),
            "company": _safe_evidence(company or {}),
            "pullback": _safe_evidence(pullback or {}),
        },
        "selection_performed": False,
    }


__all__ = [
    "COMPANY_ARCHETYPES",
    "CYCLICAL_UPSWING",
    "DEFAULT_FINANCIAL_REQUIREMENTS",
    "DEFENSIVE_QUALITY",
    "EvidenceAssessment",
    "FinancialQualityAssessment",
    "GROWTH_TREND",
    "FUNDAMENTAL",
    "MODULE_VERSION",
    "PULLBACK_CAUSES",
    "STRUCTURAL",
    "SYSTEMIC",
    "UNKNOWN",
    "UNCLASSIFIED",
    "build_a1_selection_evidence",
    "classify_company_archetype",
    "classify_pullback",
    "evaluate_financial_quality",
]
