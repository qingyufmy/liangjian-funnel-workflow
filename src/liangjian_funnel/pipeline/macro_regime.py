"""Deterministic four-asset macro regime used by A1 monthly research.

All inputs are normalized point-in-time percentiles supplied by data adapters.
The reducer never substitutes news sentiment or model prose for missing data.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


SCHEMA_VERSION = "macro-asset-quadrant/1.0.0"
ASSETS = ("EQUITY", "GOLD", "BOND", "CASH")


def build_macro_asset_quadrant(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    rotation = snapshot.get("ASSET_ROTATION_SNAPSHOT")
    rotation = rotation if isinstance(rotation, Mapping) else {}
    assets = rotation.get("assets")
    assets = assets if isinstance(assets, Mapping) else {}
    macro = snapshot.get("GLOBAL_MACRO_SNAPSHOT")
    macro = macro if isinstance(macro, Mapping) else {}
    domestic = snapshot.get("MACRO_ECONOMIC_DATA")
    domestic = domestic if isinstance(domestic, Mapping) else {}

    scores: dict[str, float] = {}
    coverage: dict[str, dict[str, Any]] = {}
    for asset in ASSETS:
        value = assets.get(asset)
        value = value if isinstance(value, Mapping) else {}
        components = {
            "momentum_20d": _percentile(value.get("momentum_20d_percentile")),
            "momentum_60d": _percentile(value.get("momentum_60d_percentile")),
            "fund_flow": _percentile(value.get("fund_flow_percentile")),
        }
        available = {key: number for key, number in components.items() if number is not None}
        coverage[asset] = {
            "available_components": sorted(available),
            "missing_components": sorted(set(components).difference(available)),
        }
        if len(available) < 2:
            continue
        base_weights = {"momentum_20d": 0.45, "momentum_60d": 0.35, "fund_flow": 0.20}
        denominator = sum(base_weights[key] for key in available)
        scores[asset] = sum(available[key] * base_weights[key] for key in available) / denominator

    adjustments = _macro_adjustments(macro, domestic)
    for asset, delta in adjustments.items():
        if asset in scores:
            scores[asset] = max(0.0, min(100.0, scores[asset] + delta))
    ranking = sorted(scores, key=lambda asset: (-scores[asset], asset))
    status = "READY" if len(scores) == len(ASSETS) else "DEGRADED" if len(scores) >= 2 else "UNAVAILABLE"
    leader = ranking[0] if ranking else None
    quadrant = {
        "EQUITY": "RISK_ON_GROWTH",
        "GOLD": "INFLATION_OR_RISK_HEDGE",
        "BOND": "DISINFLATION_DEFENSIVE",
        "CASH": "CASH_DEFENSIVE",
    }.get(leader, "UNAVAILABLE")
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "quadrant": quadrant,
        "leading_asset": leader,
        "asset_scores": {key: round(scores[key], 4) for key in ranking},
        "asset_coverage": coverage,
        "macro_adjustments": adjustments,
        "source_availability": {
            "asset_rotation": rotation.get("available") is True,
            "global_macro": macro.get("available") is True,
            "domestic_macro": domestic.get("available") is True,
        },
        "rules": {
            "llm_override_forbidden": True,
            "news_substitution_forbidden": True,
            "missing_values_are_not_zero": True,
        },
    }


def _macro_adjustments(global_macro: Mapping[str, Any], domestic: Mapping[str, Any]) -> dict[str, float]:
    """Small bounded tilts; price/flow momentum remains the primary signal."""

    easing = _percentile(global_macro.get("fed_easing_probability_percentile"))
    dollar = _percentile(global_macro.get("usd_momentum_percentile"))
    credit = _percentile(domestic.get("credit_impulse_percentile"))
    m1_m2 = _percentile(domestic.get("m1_m2_gap_percentile"))
    equity = _centered_average(credit, m1_m2, scale=8.0)
    gold = _centered_average(easing, None if dollar is None else 100.0 - dollar, scale=8.0)
    bond = _centered_average(easing, None if credit is None else 100.0 - credit, scale=6.0)
    risk_inputs = [value for value in (credit, m1_m2) if value is not None]
    cash = -sum((value - 50.0) / 50.0 for value in risk_inputs) / len(risk_inputs) * 6.0 if risk_inputs else 0.0
    return {"EQUITY": round(equity, 4), "GOLD": round(gold, 4), "BOND": round(bond, 4), "CASH": round(cash, 4)}


def _centered_average(left: float | None, right: float | None, *, scale: float) -> float:
    values = [value for value in (left, right) if value is not None]
    if not values:
        return 0.0
    return sum((value - 50.0) / 50.0 for value in values) / len(values) * scale


def _percentile(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not number == number or abs(number) == float("inf"):
        return None
    if 0.0 <= number <= 1.0:
        number *= 100.0
    return number if 0.0 <= number <= 100.0 else None


__all__ = ["ASSETS", "SCHEMA_VERSION", "build_macro_asset_quadrant"]
