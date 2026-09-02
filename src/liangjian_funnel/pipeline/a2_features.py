"""Deterministic A2 role, tier and chain-resonance materialization.

The module is intentionally pure.  External collection and feature-store
publication happen elsewhere, allowing the same frozen inputs to be replayed
without a network connection.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
A2_FEATURE_SCHEMA = "a2-features/3.1.0"


def build_a2_feature_snapshot(
    *,
    candidates: Sequence[Mapping[str, Any]],
    daily_bars: Mapping[str, Sequence[Mapping[str, Any]]],
    reference_candidates: Sequence[Mapping[str, Any]] | None = None,
    reference_daily_bars: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
    industry_membership: Mapping[str, Any] | None,
    concept_membership: Mapping[str, Any] | None,
    ladder_snapshot: Mapping[str, Any] | None,
    dragon_tiger_snapshot: Mapping[str, Any] | None,
    attention_snapshot: Mapping[str, Any] | None,
    sector_cycle_snapshot: Mapping[str, Any] | None,
    capital_flow_snapshot: Mapping[str, Any] | None,
    as_of: datetime,
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    candidate_by_symbol = {
        symbol: dict(row)
        for row in candidates
        if (symbol := _symbol(row))
    }
    symbols = tuple(sorted(candidate_by_symbol))
    # Candidates are the only rows materialized into ``by_symbol``.  An
    # explicit reference universe is used for market-wide cross-sectional
    # denominators; omitting it preserves the historical candidate-scope
    # behavior exactly.
    reference_scope_explicit = reference_candidates is not None
    reference_by_symbol = {
        symbol: dict(row)
        for row in (reference_candidates if reference_scope_explicit else candidates)
        if (symbol := _symbol(row))
    }
    reference_symbols = tuple(sorted(reference_by_symbol))
    reference_bars = reference_daily_bars if reference_daily_bars is not None else daily_bars
    returns_by_window = {
        window: {
            symbol: value
            for symbol in reference_symbols
            if (value := _return_nd(reference_bars.get(symbol, ()), cutoff.date(), window)) is not None
        }
        for window in (5, 10, 20)
    }
    returns = returns_by_window[20]
    candidate_returns = {
        symbol: value
        for symbol in symbols
        if (value := _return_nd(daily_bars.get(symbol, ()), cutoff.date(), 20)) is not None
    }
    return_percentiles_by_window = {
        window: _percentiles(values)
        for window, values in returns_by_window.items()
    }
    return_percentiles = return_percentiles_by_window[20]
    liquidity_values = {
        symbol: value
        for symbol, row in reference_by_symbol.items()
        if (value := _number(row.get("amount") or row.get("turnover") or row.get("daily_turnover"))) is not None
        and value >= 0
    }
    liquidity_percentiles = _percentiles(liquidity_values)
    all_industry_by_symbol = _membership_by_symbol(industry_membership, taxonomy="INDUSTRY")
    all_concept_by_symbol = _membership_by_symbol(concept_membership, taxonomy="CONCEPT")
    industry_by_symbol = {
        symbol: all_industry_by_symbol.get(symbol, ())
        for symbol in symbols
    }
    concept_by_symbol = {
        symbol: all_concept_by_symbol.get(symbol, ())
        for symbol in symbols
    }
    reference_industry_by_symbol = {
        symbol: all_industry_by_symbol.get(symbol, ())
        for symbol in reference_symbols
    }
    reference_concept_by_symbol = {
        symbol: all_concept_by_symbol.get(symbol, ())
        for symbol in reference_symbols
    }
    ladder = _ladder_by_symbol(ladder_snapshot, cutoff.date())
    ladder_observed, ladder_state, ladder_reason = _dataset_observation(ladder_snapshot)
    dragon = _event_symbols(dragon_tiger_snapshot)
    attention = _event_symbols(attention_snapshot)
    raw_capital_by_symbol = (
        capital_flow_snapshot.get("by_symbol", {})
        if isinstance(capital_flow_snapshot, Mapping)
        and isinstance(capital_flow_snapshot.get("by_symbol"), Mapping)
        else {}
    )
    capital_by_symbol = {
        normalized: value
        for key, value in raw_capital_by_symbol.items()
        if (normalized := _symbol(key)) and isinstance(value, Mapping)
    }
    sector_capital_by_group = _sector_capital_flow_by_group(sector_cycle_snapshot)

    trend_by_symbol: dict[str, dict[str, Any]] = {}
    tier_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        ladder_row = ladder.get(symbol)
        relative = return_percentiles.get(symbol)
        trend_by_symbol[symbol] = _factor(
            relative,
            source="LOCAL_POINT_IN_TIME_DAILY_BARS",
            availability_state="OBSERVED_VALUE" if relative is not None else "SOURCE_FAILED",
            reason_code="OK" if relative is not None else "A2_TREND_DAILY_BARS_MISSING",
            extra={"trend_percentile": relative},
        )
        if ladder_row is not None:
            height = int(_number(ladder_row.get("board_num")) or 1)
            score = min(100.0, 55.0 + height * 7.5)
            tier = "T3_PLUS" if height >= 3 else "T2" if height == 2 else "T1"
            tier_by_symbol[symbol] = _factor(
                score,
                source="HITHINK_LIMIT_UP_LADDER",
                availability_state="OBSERVED_VALUE",
                reason_code="OK",
                extra={"tier": tier, "ladder_height": height, "trend_percentile": relative},
            )
        elif ladder_observed:
            tier_by_symbol[symbol] = _factor(
                0.0,
                source="HITHINK_LIMIT_UP_LADDER",
                availability_state="OBSERVED_ABSENT",
                reason_code="NO_LIMIT_UP_EVENT",
                extra={"tier": "NONE", "ladder_height": 0, "trend_percentile": relative},
            )
        else:
            tier_by_symbol[symbol] = _factor(
                None,
                source="HITHINK_LIMIT_UP_LADDER",
                availability_state=ladder_state,
                reason_code=ladder_reason,
                extra={"tier": "UNKNOWN", "ladder_height": None, "trend_percentile": relative},
            )

    # A stock without a limit-up event is not evidence that its sector has no
    # ladder.  Keep the stock-level tier as OBSERVED_ABSENT, while separately
    # recording whether one of its point-in-time industry/concept groups has a
    # ladder member.  That sector context can confirm a core/army role together
    # with relative strength and liquidity without fabricating an individual
    # tier score.
    ladder_members_by_group: dict[str, set[str]] = defaultdict(set)
    ladder_industry_by_symbol = (
        reference_industry_by_symbol
        if reference_scope_explicit
        else all_industry_by_symbol
    )
    ladder_concept_by_symbol = (
        reference_concept_by_symbol
        if reference_scope_explicit
        else all_concept_by_symbol
    )
    for member_symbol in ladder:
        for membership in (*ladder_industry_by_symbol.get(member_symbol, ()), *ladder_concept_by_symbol.get(member_symbol, ())):
            code = str(membership.get("taxonomy_code") or "").strip().upper()
            taxonomy = str(membership.get("taxonomy") or "").strip().upper()
            if code and taxonomy:
                ladder_members_by_group[f"{taxonomy}:{code}"].add(member_symbol)
    sector_ladder_support: dict[str, list[str]] = defaultdict(list)
    for symbol in symbols:
        groups = {
            f"{str(membership.get('taxonomy') or '').strip().upper()}:{str(membership.get('taxonomy_code') or '').strip().upper()}"
            for membership in (*industry_by_symbol.get(symbol, ()), *concept_by_symbol.get(symbol, ()))
            if str(membership.get("taxonomy_code") or "").strip()
        }
        sector_ladder_support[symbol] = sorted(
            group for group in groups
            if group in ladder_members_by_group
            and ladder_members_by_group[group]
        )
        tier_by_symbol[symbol].update({
            "sector_ladder_support": bool(sector_ladder_support[symbol]),
            "sector_ladder_groups": list(sector_ladder_support[symbol]),
        })

    leader_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        components: list[tuple[float, float]] = []
        relative = return_percentiles.get(symbol)
        liquidity = liquidity_percentiles.get(symbol)
        tier_score = _number(tier_by_symbol[symbol].get("score"))
        tier_height = int(_number(tier_by_symbol[symbol].get("ladder_height")) or 0)
        capital_row = capital_by_symbol.get(symbol)
        capital_score = _number(capital_row.get("capital_flow_score")) if isinstance(capital_row, Mapping) else None
        if relative is not None:
            components.append((relative, 0.45))
        if liquidity is not None:
            components.append((liquidity, 0.30))
        # A zero score with OBSERVED_ABSENT means the stock had no ladder; it
        # is not an individual tier observation.  Let a sector ladder combine
        # with relative strength/liquidity for a CORE_ARMY confirmation while
        # preserving the stock-level absence in ``tier_structure``.
        if tier_score is not None and tier_height > 0:
            components.append((tier_score, 0.15))
        elif sector_ladder_support.get(symbol):
            # This is a sector-level confirmation component, not an individual
            # limit-up/tier observation.  Its provenance is retained below.
            support_score = _weighted([
                (relative, 0.60) for relative in (relative,) if relative is not None
            ] + [
                (liquidity, 0.40) for liquidity in (liquidity,) if liquidity is not None
            ])
            if support_score is not None:
                components.append((support_score, 0.15))
        if capital_score is not None and isinstance(capital_row, Mapping) and capital_row.get("available") is True:
            components.append((capital_score, 0.10))
        score = _weighted(components)
        if score is not None:
            confirmation = int(symbol in dragon) * 6 + int(symbol in attention) * 4
            score = min(100.0, score + confirmation)
        height = tier_height
        if height >= 2:
            role = "EMOTION_LEADER"
        elif relative is not None and relative >= 85:
            role = "TREND_LEADER"
        elif liquidity is not None and liquidity >= 80 and capital_score is not None and capital_score >= 60:
            role = "INSTITUTIONAL_CORE"
        elif sector_ladder_support.get(symbol) and relative is not None and relative >= 65 and liquidity is not None and liquidity >= 70:
            role = "CORE_ARMY"
        elif liquidity is not None and liquidity >= 80:
            role = "CAPACITY_CORE"
        elif score is not None:
            role = "FOLLOWER"
        else:
            role = "UNCONFIRMED"
        leader_by_symbol[symbol] = _factor(
            score,
            source="LOCAL_THEME_CROSS_SECTION",
            availability_state="OBSERVED_VALUE" if score is not None else "SOURCE_FAILED",
            reason_code="OK" if score is not None else "A2_LEADER_COMPONENTS_MISSING",
            extra={
                "role": role,
                "relative_strength_percentile": relative,
                "liquidity_percentile": liquidity,
                "tier_score": tier_score,
                "capital_flow_score": capital_score,
                "dragon_tiger_confirmation": symbol in dragon,
                "attention_confirmation": symbol in attention,
                "sector_ladder_support": bool(sector_ladder_support.get(symbol)),
                "sector_ladder_groups": list(sector_ladder_support.get(symbol, ())),
                "tier_confirmation_mode": (
                    "DIRECT_STOCK_LADDER"
                    if tier_height > 0
                    else "SECTOR_LADDER_RELATIVE_LIQUIDITY"
                    if sector_ladder_support.get(symbol)
                    else "NONE"
                ),
            },
        )

    # Theme metrics are market aggregates.  Their denominator must come from
    # the explicit reference universe; keep candidate membership separately so
    # the UI/LLM can audit how much of each theme reached A2.
    group_members: dict[str, set[str]] = defaultdict(set)
    candidate_group_members: dict[str, set[str]] = defaultdict(set)
    group_meta: dict[str, dict[str, str]] = {}
    for symbol in reference_symbols:
        for item in (*reference_industry_by_symbol.get(symbol, ()), *reference_concept_by_symbol.get(symbol, ())):
            code = str(item.get("taxonomy_code") or "").strip().upper()
            if not code:
                continue
            key = f"{item.get('taxonomy')}:{code}"
            group_members[key].add(symbol)
            group_meta[key] = {
                "taxonomy": str(item.get("taxonomy") or ""),
                "taxonomy_code": code,
                "taxonomy_name": str(item.get("taxonomy_name") or ""),
            }
    for symbol in symbols:
        for item in (*industry_by_symbol.get(symbol, ()), *concept_by_symbol.get(symbol, ())):
            code = str(item.get("taxonomy_code") or "").strip().upper()
            if not code:
                continue
            key = f"{item.get('taxonomy')}:{code}"
            candidate_group_members[key].add(symbol)
    total_turnover = sum(
        max(0.0, _number(reference_by_symbol.get(symbol, {}).get("amount")) or _number(reference_by_symbol.get(symbol, {}).get("turnover")) or _number(reference_by_symbol.get(symbol, {}).get("daily_turnover")) or 0.0)
        for symbol in reference_symbols
    )
    theme_metrics: dict[str, dict[str, Any]] = {}
    for key, members in sorted(group_members.items()):
        observed_returns = [returns[symbol] for symbol in members if symbol in returns]
        observed_returns_5d = [returns_by_window[5][symbol] for symbol in members if symbol in returns_by_window[5]]
        observed_returns_10d = [returns_by_window[10][symbol] for symbol in members if symbol in returns_by_window[10]]
        observed_relative = [return_percentiles[symbol] for symbol in members if symbol in return_percentiles]
        observed_relative_5d = [return_percentiles_by_window[5][symbol] for symbol in members if symbol in return_percentiles_by_window[5]]
        observed_relative_10d = [return_percentiles_by_window[10][symbol] for symbol in members if symbol in return_percentiles_by_window[10]]
        observed_capital = [
            float(row["capital_flow_score"])
            for symbol in members
            if isinstance((row := capital_by_symbol.get(symbol)), Mapping)
            and row.get("available") is True
            and _number(row.get("capital_flow_score")) is not None
        ]
        ladder_count = sum(symbol in ladder for symbol in members)
        dragon_count = sum(symbol in dragon for symbol in members)
        breadth = sum(value > 0 for value in observed_returns) / len(observed_returns) if observed_returns else None
        breadth_5d = sum(value > 0 for value in observed_returns_5d) / len(observed_returns_5d) if observed_returns_5d else None
        breadth_10d = sum(value > 0 for value in observed_returns_10d) / len(observed_returns_10d) if observed_returns_10d else None
        relative_mean = sum(observed_relative) / len(observed_relative) if observed_relative else None
        relative_mean_5d = sum(observed_relative_5d) / len(observed_relative_5d) if observed_relative_5d else None
        relative_mean_10d = sum(observed_relative_10d) / len(observed_relative_10d) if observed_relative_10d else None
        sector_capital = sector_capital_by_group.get(key)
        sector_capital_score = _number(sector_capital.get("score")) if isinstance(sector_capital, Mapping) else None
        capital_mean = (
            sum(observed_capital) / len(observed_capital)
            if observed_capital
            else sector_capital_score
        )
        ladder_ratio = ladder_count / len(members) if members else None
        dragon_ratio = dragon_count / len(members) if members else None
        group_turnover = sum(
            max(0.0, _number(reference_by_symbol.get(symbol, {}).get("amount")) or _number(reference_by_symbol.get(symbol, {}).get("turnover")) or _number(reference_by_symbol.get(symbol, {}).get("daily_turnover")) or 0.0)
            for symbol in members
        )
        cycle_score = _cycle_score(sector_cycle_snapshot, group_meta[key]["taxonomy_code"])
        components: list[tuple[float, float]] = []
        if breadth is not None:
            components.append((breadth * 100.0, 0.25))
        if relative_mean is not None:
            components.append((relative_mean, 0.25))
        if capital_mean is not None:
            components.append((capital_mean, 0.20))
        if ladder_ratio is not None:
            components.append((min(100.0, ladder_ratio * 500.0), 0.10))
        if dragon_ratio is not None:
            components.append((min(100.0, dragon_ratio * 500.0), 0.05))
        if cycle_score is not None:
            components.append((cycle_score, 0.15))
        score = _weighted(components)
        weekly_components: list[tuple[float, float]] = []
        if relative_mean_5d is not None:
            weekly_components.append((relative_mean_5d, 0.30))
        if relative_mean_10d is not None:
            weekly_components.append((relative_mean_10d, 0.15))
        if relative_mean is not None:
            weekly_components.append((relative_mean, 0.20))
        if breadth_5d is not None:
            weekly_components.append((breadth_5d * 100.0, 0.20))
        if capital_mean is not None:
            weekly_components.append((capital_mean, 0.15))
        weekly_confirmation_score = _weighted(weekly_components)
        weekly_state = _weekly_theme_state(
            _mean(observed_returns_5d),
            _mean(observed_returns_10d),
            _mean(observed_returns),
            breadth_5d,
        )
        coverage = len(observed_returns) / len(members) if members else 0.0
        candidate_members = candidate_group_members.get(key, set())
        theme_metrics[key] = {
            **group_meta[key],
            "available": score is not None and coverage >= 0.80,
            "availability_state": "OBSERVED_VALUE" if score is not None and coverage >= 0.80 else "SOURCE_FAILED",
            "reason_code": "OK" if score is not None and coverage >= 0.80 else "A2_CHAIN_MEMBER_COVERAGE_INSUFFICIENT",
            "score": round(score, 4) if score is not None else None,
            # ``member_count`` remains for compatibility and now explicitly
            # denotes the reference-universe member count.
            "member_count": len(members),
            "reference_member_count": len(members),
            "candidate_member_count": len(candidate_members),
            "return_coverage": round(coverage, 6),
            "breadth": round(breadth, 6) if breadth is not None else None,
            "breadth_5d": round(breadth_5d, 6) if breadth_5d is not None else None,
            "breadth_10d": round(breadth_10d, 6) if breadth_10d is not None else None,
            "relative_strength_mean": round(relative_mean, 4) if relative_mean is not None else None,
            "relative_strength_mean_5d": round(relative_mean_5d, 4) if relative_mean_5d is not None else None,
            "relative_strength_mean_10d": round(relative_mean_10d, 4) if relative_mean_10d is not None else None,
            "weekly_confirmation_score": round(weekly_confirmation_score, 4) if weekly_confirmation_score is not None else None,
            "weekly_momentum_state": weekly_state,
            "turnover_share": round(group_turnover / total_turnover, 6) if total_turnover > 0 else None,
            "capital_flow_mean": round(capital_mean, 4) if capital_mean is not None else None,
            "capital_flow_coverage": round(len(observed_capital) / len(members), 6) if members else 0.0,
            "capital_flow_scope": "SYMBOL" if observed_capital else "SECTOR" if sector_capital_score is not None else None,
            "capital_flow_source": sector_capital.get("source") if isinstance(sector_capital, Mapping) else None,
            "ladder_member_count": ladder_count,
            "dragon_tiger_member_count": dragon_count,
            "cycle_score": cycle_score,
        }

    chain_by_symbol: dict[str, dict[str, Any]] = {}
    weekly_by_symbol: dict[str, dict[str, Any]] = {}
    breadth_by_symbol: dict[str, dict[str, Any]] = {}
    turnover_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        keys = []
        for item in (*industry_by_symbol.get(symbol, ()), *concept_by_symbol.get(symbol, ())):
            code = str(item.get("taxonomy_code") or "").strip().upper()
            if code:
                keys.append(f"{item.get('taxonomy')}:{code}")
        available_rows = [theme_metrics[key] for key in keys if key in theme_metrics and theme_metrics[key].get("available") is True]
        best = max(available_rows, key=lambda row: float(row.get("score") or 0.0), default=None)
        breadth_by_symbol[symbol] = _factor(
            _number(best.get("breadth")) * 100.0 if best and _number(best.get("breadth")) is not None else None,
            source="POINT_IN_TIME_TAXONOMY_AGGREGATE",
            availability_state="OBSERVED_VALUE" if best and _number(best.get("breadth")) is not None else "SOURCE_FAILED",
            reason_code="OK" if best and _number(best.get("breadth")) is not None else "A2_BREADTH_MAPPING_OR_COVERAGE_MISSING",
            extra={"taxonomy_code": best.get("taxonomy_code") if best else None},
        )
        turnover_by_symbol[symbol] = _factor(
            _number(best.get("turnover_share")) * 100.0 if best and _number(best.get("turnover_share")) is not None else None,
            source="POINT_IN_TIME_TAXONOMY_AGGREGATE",
            availability_state="OBSERVED_VALUE" if best and _number(best.get("turnover_share")) is not None else "SOURCE_FAILED",
            reason_code="OK" if best and _number(best.get("turnover_share")) is not None else "A2_TURNOVER_MAPPING_OR_COVERAGE_MISSING",
            extra={"taxonomy_code": best.get("taxonomy_code") if best else None},
        )
        chain_by_symbol[symbol] = _factor(
            _number(best.get("score")) if best else None,
            source="POINT_IN_TIME_TAXONOMY_AGGREGATE",
            availability_state="OBSERVED_VALUE" if best else "SOURCE_FAILED",
            reason_code="OK" if best else "A2_CHAIN_MAPPING_OR_COVERAGE_MISSING",
            extra={
                "taxonomy": best.get("taxonomy") if best else None,
                "taxonomy_code": best.get("taxonomy_code") if best else None,
                "taxonomy_name": best.get("taxonomy_name") if best else None,
            },
        )
        weekly_by_symbol[symbol] = _factor(
            _number(best.get("weekly_confirmation_score")) if best else None,
            source="POINT_IN_TIME_WEEKLY_TAXONOMY_AGGREGATE",
            availability_state="OBSERVED_VALUE" if best and _number(best.get("weekly_confirmation_score")) is not None else "SOURCE_FAILED",
            reason_code="OK" if best and _number(best.get("weekly_confirmation_score")) is not None else "A2_WEEKLY_CONFIRMATION_MISSING",
            extra={
                "taxonomy_code": best.get("taxonomy_code") if best else None,
                "weekly_momentum_state": best.get("weekly_momentum_state") if best else "UNKNOWN",
                "breadth_5d": best.get("breadth_5d") if best else None,
                "relative_strength_mean_5d": best.get("relative_strength_mean_5d") if best else None,
            },
        )

    by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        capital = capital_by_symbol.get(symbol)
        if not isinstance(capital, Mapping):
            sector_candidates = [
                sector_capital_by_group.get(
                    f"{str(item.get('taxonomy') or '').strip().upper()}:{str(item.get('taxonomy_code') or '').strip().upper()}"
                )
                for item in (*industry_by_symbol.get(symbol, ()), *concept_by_symbol.get(symbol, ()))
            ]
            sector_candidates = [
                item
                for item in sector_candidates
                if isinstance(item, Mapping) and _number(item.get("score")) is not None
            ]
            if sector_candidates:
                capital = max(sector_candidates, key=lambda item: float(_number(item.get("score")) or 0.0))
        capital_factor = (
            _factor(
                _number(capital.get("capital_flow_score")) if _number(capital.get("capital_flow_score")) is not None else _number(capital.get("score")),
                source=str(
                    capital.get("source")
                    or (
                        capital_flow_snapshot.get("source_id")
                        if isinstance(capital_flow_snapshot, Mapping)
                        else None
                    )
                    or "CAPITAL_FLOW_SNAPSHOT"
                ),
                availability_state=str(capital.get("availability_state") or "OBSERVED_VALUE"),
                reason_code=str(capital.get("reason_code") or "OK"),
                extra={
                    "source_refs": list(capital.get("source_refs") or ()),
                    "source_scope": str(capital.get("source_scope") or "SYMBOL"),
                    "provider_method": capital.get("provider_method"),
                },
            )
            if isinstance(capital, Mapping)
            else _factor(None, source="CAPITAL_FLOW_SNAPSHOT", availability_state="NOT_CONFIGURED", reason_code="A2_CAPITAL_FLOW_UNAVAILABLE")
        )
        factors = {
            "breadth": breadth_by_symbol[symbol],
            "turnover_share": turnover_by_symbol[symbol],
            "capital_flow": capital_factor,
            "tier_structure": tier_by_symbol[symbol],
            # Trend strength is useful for leader identification but is not a
            # substitute for an observed limit-up ladder.  It deliberately
            # remains outside the canonical A2 factor weights.
            "trend_strength_proxy": trend_by_symbol[symbol],
            "leader_structure": leader_by_symbol[symbol],
            "index_chain_resonance": chain_by_symbol[symbol],
            "weekly_confirmation": weekly_by_symbol[symbol],
        }
        by_symbol[symbol] = {
            "symbol": symbol,
            "factors": factors,
            **factors,
            "available_factor_count": sum(item.get("available") is True for item in factors.values()),
            "missing_factor_count": sum(item.get("available") is not True for item in factors.values()),
            "leader_role": leader_by_symbol[symbol].get("role"),
            "tier": tier_by_symbol[symbol].get("tier"),
        }

    symbol_count = len(symbols)
    daily_bar_coverage = sum(symbol in candidate_returns for symbol in symbols) / symbol_count if symbol_count else 0.0
    identity_coverage = (
        sum(bool(industry_by_symbol.get(symbol) or concept_by_symbol.get(symbol)) for symbol in symbols) / symbol_count
        if symbol_count
        else 0.0
    )
    reference_symbol_count = len(reference_symbols)
    reference_daily_bar_coverage = (
        sum(symbol in returns for symbol in reference_symbols) / reference_symbol_count
        if reference_symbol_count
        else 0.0
    )
    reference_identity_coverage = (
        sum(bool(reference_industry_by_symbol.get(symbol) or reference_concept_by_symbol.get(symbol)) for symbol in reference_symbols)
        / reference_symbol_count
        if reference_symbol_count
        else 0.0
    )
    factor_coverage = {
        name: (
            sum(by_symbol[symbol]["factors"][name].get("available") is True for symbol in symbols) / symbol_count
            if symbol_count
            else 0.0
        )
        for name in ("capital_flow", "tier_structure", "leader_structure", "index_chain_resonance", "weekly_confirmation")
    }
    # Capital flow, event ladders and attention feeds are optional evidence
    # families.  They must remain individually auditable, but one unavailable
    # family must not make an otherwise reproducible market-role projection
    # look like a market-wide absence of opportunity.  Daily bars and point-in-
    # time taxonomy identity are the minimum materialization contract; the
    # market-role and chain projections are derived from those facts and may be
    # enriched by the optional feeds.
    candidate_base_sufficient = bool(symbols) and daily_bar_coverage >= 0.95 and identity_coverage >= 0.95
    # An explicit market reference is itself part of the materialization
    # contract.  Never let a well-covered 50-row candidate slice claim full
    # market sufficiency when its reference bars or taxonomy are missing.
    base_sufficient = candidate_base_sufficient and (
        not reference_scope_explicit
        or (
            bool(reference_symbols)
            and reference_daily_bar_coverage >= 0.95
            and reference_identity_coverage >= 0.95
        )
    )
    optional_missing = [
        name
        for name in ("capital_flow", "tier_structure", "leader_structure", "index_chain_resonance", "weekly_confirmation")
        if factor_coverage[name] < 0.90
    ]
    market_factor_names = ("breadth", "turnover_share", "leader_structure", "tier_structure", "index_chain_resonance", "weekly_confirmation")
    market_fact_counts = {
        symbol: sum(
            by_symbol[symbol]["factors"].get(name, {}).get("available") is True
            and _number(by_symbol[symbol]["factors"].get(name, {}).get("score")) is not None
            for name in market_factor_names
        )
        for symbol in symbols
    }
    market_projection_available = any(count >= 2 for count in market_fact_counts.values())
    market_missing_facts: list[str] = []
    if daily_bar_coverage < 0.95:
        market_missing_facts.append("daily_bars")
    if identity_coverage < 0.95:
        market_missing_facts.append("taxonomy_identity")
    if reference_scope_explicit and reference_daily_bar_coverage < 0.95:
        market_missing_facts.append("reference_daily_bars")
    if reference_scope_explicit and reference_identity_coverage < 0.95:
        market_missing_facts.append("reference_taxonomy_identity")
    if not market_projection_available:
        market_missing_facts.append("market_facts_minimum_2")
    data_state = (
        "INSUFFICIENT"
        if not base_sufficient
        else "DEGRADED"
        if optional_missing
        else "SUFFICIENT"
    )
    critical_sufficient = data_state != "INSUFFICIENT"
    emotion_leader_sufficiency = {
        "required_facts": ["tier_structure", "leader_structure"],
        "optional_facts": ["capital_flow", "dragon_tiger", "attention"],
        "available": factor_coverage["tier_structure"] >= 0.90 and factor_coverage["leader_structure"] >= 0.90,
        "missing_facts": [
            name
            for name in ("tier_structure", "leader_structure")
            if factor_coverage[name] < 0.90
        ],
        "data_sufficiency_state": (
            "SUFFICIENT"
            if factor_coverage["tier_structure"] >= 0.90 and factor_coverage["leader_structure"] >= 0.90 and not optional_missing
            else "DEGRADED"
            if factor_coverage["tier_structure"] > 0 or factor_coverage["leader_structure"] > 0
            else "INSUFFICIENT"
        ),
    }
    # EMOTION_LEADER is a role/evidence path inside MARKET_CORE.  It is not a
    # third top-level route and therefore cannot create an independent focus
    # pool or bypass the two configured route contracts.
    route_sufficiency = {
        "MARKET_CORE": {
            "required_facts": ["daily_bars", "taxonomy_identity", "market_facts_minimum_2"],
            "optional_facts": ["capital_flow", "tier_structure", "leader_structure", "index_chain_resonance", "weekly_confirmation", "attention", "dragon_tiger"],
            "available": base_sufficient and market_projection_available,
            "missing_facts": market_missing_facts,
            "data_sufficiency_state": (
                "SUFFICIENT"
                if base_sufficient and market_projection_available and not optional_missing
                else "DEGRADED"
                if base_sufficient
                else "INSUFFICIENT"
            ),
            "market_fact_coverage": {
                name: round(
                    sum(
                        by_symbol[symbol]["factors"].get(name, {}).get("available") is True
                        and _number(by_symbol[symbol]["factors"].get(name, {}).get("score")) is not None
                        for symbol in symbols
                    ) / symbol_count if symbol_count else 0.0,
                    6,
                )
                for name in market_factor_names
            },
            "role_sufficiency": {"EMOTION_LEADER": emotion_leader_sufficiency},
        },
        "SUPPLY_CHAIN_ALPHA": {
            "required_facts": ["daily_bars", "taxonomy_identity"],
            "optional_facts": ["capital_flow", "tier_structure", "leader_structure", "index_chain_resonance", "weekly_confirmation"],
            "available": base_sufficient,
            "missing_facts": [
                *(["daily_bars"] if daily_bar_coverage < 0.95 else []),
                *(["taxonomy_identity"] if identity_coverage < 0.95 else []),
            ],
            "data_sufficiency_state": "SUFFICIENT" if base_sufficient and not optional_missing else "DEGRADED" if base_sufficient else "INSUFFICIENT",
        },
    }
    payload: dict[str, Any] = {
        "schema_version": A2_FEATURE_SCHEMA,
        "available": critical_sufficient,
        "reason_code": "OK" if data_state == "SUFFICIENT" else "A2_OPTIONAL_FACTS_DEGRADED" if data_state == "DEGRADED" else "A2_CRITICAL_DATA_INSUFFICIENT",
        "data_sufficiency_state": data_state,
        "as_of": cutoff.isoformat(),
        "symbol_count": symbol_count,
        "daily_bar_coverage": round(daily_bar_coverage, 6),
        "identity_coverage": round(identity_coverage, 6),
        "factor_coverage": {key: round(value, 6) for key, value in factor_coverage.items()},
        "optional_missing_factors": optional_missing,
        "route_sufficiency": route_sufficiency,
        "coverage_thresholds": {
            "daily_bars": 0.95,
            "identity": 0.95,
            "reference_daily_bars": 0.95,
            "reference_identity": 0.95,
            "capital_flow": 0.90,
            "tier_structure": 0.90,
            "leader_structure": 0.90,
            "index_chain_resonance": 0.90,
            "weekly_confirmation": 0.90,
        },
        "ladder_dataset_state": ladder_state,
        "ladder_dataset_reason_code": ladder_reason,
        "capital_flow_available": bool(
            (isinstance(capital_flow_snapshot, Mapping) and capital_flow_snapshot.get("available") is True)
            or sector_capital_by_group
        ),
        "capital_flow_method": (
            capital_flow_snapshot.get("provider_method")
            if isinstance(capital_flow_snapshot, Mapping) and capital_flow_snapshot.get("available") is True
            else "VENDOR_DERIVED_SECTOR_RANK_PERCENTILE"
            if sector_capital_by_group
            else None
        ),
        "by_symbol": by_symbol,
        "theme_metrics": theme_metrics,
        "candidate_symbol_count": symbol_count,
        "reference_symbol_count": reference_symbol_count,
        "reference_daily_bar_coverage": round(reference_daily_bar_coverage, 6),
        "reference_identity_coverage": round(reference_identity_coverage, 6),
        "denominator_scope": (
            "FULL_MARKET_REFERENCE"
            if reference_scope_explicit
            else "CANDIDATE_SCOPE_FALLBACK"
        ),
    }
    payload["content_hash"] = _hash(payload)
    return payload


def _membership_by_symbol(value: Mapping[str, Any] | None, *, taxonomy: str) -> dict[str, tuple[dict[str, str], ...]]:
    records = _records(value)
    result: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in records:
        symbol = _symbol(row)
        memberships = row.get("memberships")
        if not symbol or not isinstance(memberships, Sequence) or isinstance(memberships, (str, bytes, bytearray)):
            continue
        for item in memberships:
            if not isinstance(item, Mapping):
                continue
            code = str(
                item.get("taxonomy_code")
                or item.get("industry_thscode")
                or item.get("concept_thscode")
                or ""
            ).strip().upper()
            name = str(
                item.get("taxonomy_name")
                or item.get("industry_name")
                or item.get("concept_name")
                or ""
            ).strip()
            if code:
                result[symbol].append({"taxonomy": taxonomy, "taxonomy_code": code, "taxonomy_name": name})
    return {symbol: tuple(rows) for symbol, rows in result.items()}


def _ladder_by_symbol(value: Mapping[str, Any] | None, as_of: date) -> dict[str, dict[str, Any]]:
    latest: tuple[date, Mapping[str, Any]] | None = None
    for row in _records(value):
        day = _date(row.get("date"))
        if day is None or day > as_of or (latest is not None and day <= latest[0]):
            continue
        latest = (day, row)
    result: dict[str, dict[str, Any]] = {}
    if latest is None:
        return result
    boards = latest[1].get("boards")
    if not isinstance(boards, Mapping):
        return result
    for name, entries in boards.items():
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes, bytearray)):
            continue
        for item in entries:
            if not isinstance(item, Mapping) or not (symbol := _symbol(item)):
                continue
            height = int(_number(item.get("board_num")) or _board_height(name) or 1)
            result[symbol] = {**dict(item), "board_num": height, "trade_date": latest[0].isoformat()}
    return result


def _event_symbols(value: Mapping[str, Any] | None) -> set[str]:
    return {symbol for row in _records(value) if (symbol := _symbol(row))}


def _records(value: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Mapping):
        return ()
    direct = value.get("records")
    if isinstance(direct, Sequence) and not isinstance(direct, (str, bytes, bytearray)):
        return tuple(item for item in direct if isinstance(item, Mapping))
    payload = value.get("payload")
    if isinstance(payload, Mapping):
        nested = payload.get("records")
        if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
            return tuple(item for item in nested if isinstance(item, Mapping))
    return ()


def _dataset_observation(value: Mapping[str, Any] | None) -> tuple[bool, str, str]:
    """Return whether absence from a dataset is an observed fact.

    An empty, successfully collected full-market event set is valid evidence
    that a stock had no event.  A missing/malformed/failed source is not.
    """

    if not isinstance(value, Mapping):
        return False, "NOT_CONFIGURED", "A2_TIER_SOURCE_NOT_CONFIGURED"
    if value.get("available") is False:
        return False, str(value.get("availability_state") or "SOURCE_FAILED"), str(
            value.get("reason_code") or "A2_TIER_SOURCE_FAILED"
        )
    records = value.get("records")
    if records is None and isinstance(value.get("payload"), Mapping):
        records = value["payload"].get("records")
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
        return False, "SOURCE_FAILED", "A2_TIER_RECORDS_MALFORMED"
    if any(not isinstance(item, Mapping) for item in records):
        return False, "SOURCE_FAILED", "A2_TIER_RECORDS_MALFORMED"
    return True, "OBSERVED_VALUE", "OK"


def _return_nd(bars: Sequence[Mapping[str, Any]], as_of: date, window: int) -> float | None:
    usable: list[tuple[int, float]] = []
    cutoff = int(datetime.combine(as_of, datetime.max.time(), tzinfo=SHANGHAI).timestamp() * 1000)
    for index, row in enumerate(bars):
        day = int(_number(row.get("date_ms") or row.get("timestamp") or row.get("time")) or index)
        close = _number(row.get("close_price") or row.get("close"))
        if close is not None and close > 0 and day <= cutoff:
            usable.append((day, close))
    usable.sort()
    horizon = max(1, int(window))
    if len(usable) < horizon + 1:
        return None
    first, last = usable[-(horizon + 1)][1], usable[-1][1]
    return (last / first - 1.0) * 100.0 if first > 0 else None


def _return_20d(bars: Sequence[Mapping[str, Any]], as_of: date) -> float | None:
    """Compatibility wrapper retained for external callers and old tests."""

    return _return_nd(bars, as_of, 20)


def _mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _weekly_theme_state(
    return_5d: float | None,
    return_10d: float | None,
    return_20d: float | None,
    breadth_5d: float | None,
) -> str:
    if return_5d is None or return_20d is None or breadth_5d is None:
        return "UNKNOWN"
    if return_20d <= 0 < return_5d and breadth_5d >= 0.5:
        return "EARLY_REVERSAL"
    if return_20d > 0 and return_5d <= 0:
        return "COOLING"
    if return_20d <= 0 and return_5d <= 0:
        return "WEAK"
    if (
        return_10d is not None
        and return_5d / 5.0 > return_10d / 10.0 > return_20d / 20.0 > 0
        and breadth_5d >= 0.6
    ):
        return "ACCELERATING"
    if return_5d > 0 and return_20d > 0 and breadth_5d >= 0.5:
        return "PERSISTENT"
    return "MIXED"


def _sector_capital_flow_by_group(
    value: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    health = value.get("sector_health_snapshot")
    if not isinstance(health, Mapping):
        health = value.get("A2_SECTOR_HEALTH_SNAPSHOT")
    if not isinstance(health, Mapping):
        return {}
    by_taxonomy = health.get("by_taxonomy")
    if not isinstance(by_taxonomy, Mapping):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for taxonomy in ("industry", "concept"):
        section = by_taxonomy.get(taxonomy)
        rows = section.get("sectors") if isinstance(section, Mapping) else None
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
            continue
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            code = str(row.get("taxonomy_code") or "").strip().upper()
            flow = row.get("capital_flow")
            if not code or not isinstance(flow, Mapping) or flow.get("available") is not True:
                continue
            score = _number(flow.get("score"))
            if score is None:
                continue
            result[f"{taxonomy.upper()}:{code}"] = dict(flow)
    return result


def _cycle_score(value: Mapping[str, Any] | None, taxonomy_code: str) -> float | None:
    if not isinstance(value, Mapping):
        return None
    metrics = value.get("history_metrics")
    if not isinstance(metrics, Mapping):
        return None
    rows = metrics.get("monthly_rotation_candidates") or metrics.get("persistent_mainline_candidates")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return None
    for row in rows:
        if not isinstance(row, Mapping) or str(row.get("industry_thscode") or "").upper() != taxonomy_code:
            continue
        for key in ("rotation_score", "relative_strength_percentile_20d", "persistence_score", "score"):
            if (number := _number(row.get(key))) is not None:
                return min(100.0, max(0.0, number))
    return None


def _factor(
    score: float | None,
    *,
    source: str,
    availability_state: str,
    reason_code: str,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    result = {
        "available": score is not None,
        "availability_state": availability_state,
        "reason_code": reason_code,
        "score": round(score, 4) if score is not None else None,
        "source": source,
    }
    if extra:
        result.update(extra)
    return result


def _weighted(values: Sequence[tuple[float, float]]) -> float | None:
    total = sum(weight for _value, weight in values)
    return sum(value * weight for value, weight in values) / total if total > 0 else None


def _percentiles(values: Mapping[str, float]) -> dict[str, float]:
    ordered = sorted((float(value), symbol) for symbol, value in values.items() if math.isfinite(float(value)))
    if not ordered:
        return {}
    if len(ordered) == 1:
        return {ordered[0][1]: 50.0}
    result: dict[str, float] = {}
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][0] == ordered[index][0]:
            end += 1
        average_rank = (index + end - 1) / 2.0
        score = average_rank / (len(ordered) - 1) * 100.0
        for _value, symbol in ordered[index:end]:
            result[symbol] = round(score, 4)
        index = end
    return result


def _symbol(value: Mapping[str, Any] | Any) -> str:
    if isinstance(value, Mapping):
        raw = value.get("symbol") or value.get("thscode") or value.get("ts_code") or value.get("code")
    else:
        raw = value
    text = str(raw or "").strip().upper()
    if len(text) == 6 and text.isdigit():
        suffix = "SH" if text.startswith(("5", "6", "9")) else "BJ" if text.startswith(("4", "8")) else "SZ"
        return f"{text}.{suffix}"
    return text if len(text) == 9 and text[6] == "." else ""


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _date(value: Any) -> date | None:
    text = str(value or "").strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _board_height(name: Any) -> int | None:
    text = str(name or "").lower()
    words = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7}
    return next((height for word, height in words.items() if word in text), None)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("A2 feature as_of must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _hash(value: Mapping[str, Any]) -> str:
    body = dict(value)
    body.pop("content_hash", None)
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


__all__ = ["A2_FEATURE_SCHEMA", "build_a2_feature_snapshot"]
