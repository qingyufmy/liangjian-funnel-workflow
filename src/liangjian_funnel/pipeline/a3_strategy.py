"""Deterministic A3 strategy routing and daily-plan qualification.

The A3 stage answers one question for one A2 candidate: *which, if any, of
the three intraday playbooks is applicable tomorrow?*  It deliberately does
not calculate a composite score.  Every decision is a list of explicit
premises, met conditions, missing conditions, and vetoes so that the same
frozen input can be audited or replayed without a model call.

This module is intentionally independent from the orchestration pipeline.  A
caller may pass either the compact per-symbol objects produced by the current
pipeline or a complete frozen snapshot; the small alias reader below keeps
that compatibility at the module boundary without changing the source
contracts themselves.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


STRATEGY_VERSION = "a3-a4-three-strategy/1.1.0"


class StrategyProfile(StrEnum):
    """The only A4 routes that A3 can publish, plus an explicit no-plan state."""

    LEADER_INTRADAY = "LEADER_INTRADAY"
    TREND_MA5 = "TREND_MA5"
    MA520_SWING = "MA520_SWING"
    NO_NEXT_DAY_PLAN = "NO_NEXT_DAY_PLAN"


class Eligibility(StrEnum):
    """A3 publication state for a single candidate."""

    QUALIFIED = "QUALIFIED"
    WATCH = "WATCH"
    REJECTED = "REJECTED"
    DATA_GAP = "DATA_GAP"


class A3StrategyDecision(BaseModel):
    """Frozen, JSON-serializable A3 decision returned by the public entry.

    The model intentionally keeps the explanatory collections untyped beyond
    their stable outer shape.  New evidence fields can therefore be carried
    in ``strategy_facts`` without changing the A3 routing contract.
    """

    model_config = ConfigDict(frozen=True, extra="allow")

    strategy_profile: StrategyProfile
    strategy_version: str
    symbol: str | None = None
    name: str | None = None
    candidate_origin: str | None = None
    market_role: str | None = None
    market_regime: str | None = None
    theme_stage: str | None = None
    monthly_state: str | None = None
    monthly_partial_observation: Any = None
    weekly_closed_state: str | None = None
    weekly_partial_observation: Any = None
    daily_state: str | None = None
    daily_ma: dict[str, float | None] = Field(default_factory=dict)
    daily_macd: dict[str, float | None] = Field(default_factory=dict)
    daily_volume_state: str | None = None
    relative_strength: dict[str, Any] = Field(default_factory=dict)
    entry_reference_zone: dict[str, float] | None = None
    no_chase_price: float | None = None
    price_discovery: bool = False
    daily_invalidation: float | None = None
    plan_premises: list[str] = Field(default_factory=list)
    a4_required_entry_rules: list[str] = Field(default_factory=list)
    a4_exit_rules: list[str] = Field(default_factory=list)
    plan_mode: str | None = None
    plan_expiry: Any = None
    eligibility: Eligibility
    required_conditions: list[str] = Field(default_factory=list)
    met_conditions: list[str] = Field(default_factory=list)
    unmet_conditions: list[str] = Field(default_factory=list)
    veto_conditions: list[str] = Field(default_factory=list)
    reason_codes: list[str] = Field(default_factory=list)
    llm_review: dict[str, Any] = Field(default_factory=dict)
    strategy_facts: dict[str, Any] = Field(default_factory=dict)


_CLOSED_STATES = {"CLOSED", "COMPLETE", "COMPLETED", "FINAL", "FINALIZED"}
_PARTIAL_STATES = {
    "PARTIAL",
    "OPEN",
    "INCOMPLETE",
    "MTD",
    "MONTH_TO_DATE",
    "WTD",
    "WEEK_TO_DATE",
}
_BEAR_STATES = {
    "BEAR",
    "BEARISH",
    "BEAR_STACK",
    "BEAR_PARTIAL",
    "DOWN",
    "DOWNTREND",
    "RETREAT",
    "FADE",
    "RISK_OFF",
    "RISK_OFF_RETREAT",
}
_RISK_OFF_STATES = {
    "RISK_OFF",
    "RISK_OFF_RETREAT",
    "RETREAT",
    "WEAK",
    "WEAK_MARKET",
    "PANIC",
}
_NO_ENTRY_PERMISSIONS = {
    "NO_NEW_ENTRY",
    "EXIT_RISK",
    "BLOCKED",
    "DISABLED",
    "NO_ENTRY",
}
_LEADER_ROLES = {
    "LEADER",
    "EMOTION_LEADER",
    "MARKET_LEADER",
    "THEME_LEADER",
}
_LEADER_STAGES = {"IGNITION", "CONFIRMATION", "ACCELERATION", "EARLY_ACCELERATION"}
_BAD_LEADER_STAGES = {
    "CLIMAX",
    "DIVERGENCE",
    "RETREAT",
    "FADE",
    "COOLING",
    "DISTRIBUTION",
    "EXIT_RISK",
}
_DEAD_CROSS_EVENTS = {
    "DEAD_CROSS_SHORT",
    "DEAD_CROSS",
    "MA5_CROSS_BELOW_MA20",
    "MA5_BELOW_MA20",
    "DEATH_CROSS",
}
_GOLDEN_CROSS_EVENTS = {
    "GOLDEN_CROSS_SHORT",
    "GOLDEN_CROSS",
    "MA5_CROSS_ABOVE_MA20",
    "MA5_ABOVE_MA20",
}
_RECLAIM_EVENTS = {
    "RECLAIM_MA20",
    "MA20_RECLAIM",
    "RECOVER_MA20",
    "PULLBACK_HOLD_MA20",
    "MA20_HOLD",
}
_NEW_HIGH_TOKENS = {
    "NEW_HIGH",
    "INNOVATION_HIGH",
    "PRICE_DISCOVERY",
    "ALL_TIME_HIGH",
    "52W_HIGH",
    "创新高",
}
_DISTRIBUTION_TOKENS = {
    "DISTRIBUTION",
    "HIGH_VOLUME_DISTRIBUTION",
    "SELLING_DISTRIBUTION",
    "放量派发",
    "派发",
}
_LOCKED_TOKENS = {
    "ONE_PRICE",
    "LOCKED_LIMIT_UP",
    "LIMIT_UP_LOCKED",
    "SEALED_LIMIT_UP",
    "一字",
    "锁板",
    "一字板",
}


def evaluate_a3_candidate(
    candidate: Mapping[str, Any] | None,
    technical_context: Mapping[str, Any] | None = None,
    price_contract: Mapping[str, Any] | None = None,
    trading_eligibility: Mapping[str, Any] | None = None,
    kline_labels: Mapping[str, Any] | Sequence[Any] | None = None,
    *,
    snapshot: Mapping[str, Any] | None = None,
    as_of: datetime | date | str | None = None,
) -> dict[str, Any]:
    """Evaluate one A2 candidate against the A3 daily strategy contract.

    ``technical_context`` may be a per-symbol factor object or a complete
    frozen snapshot.  ``price_contract``, ``trading_eligibility`` and
    ``kline_labels`` accept either a per-symbol object or a symbol-keyed map.
    The return value is JSON-ready and contains no aggregate score or weight.
    ``QUALIFIED`` is the only state that can be converted into an executable
    next-day plan.  ``WATCH`` is deliberately observable only.
    """

    raw_candidate = dict(candidate or {})
    symbol = _text(_first(raw_candidate, "symbol", "thscode", "ticker", "code"))
    source_snapshot = _mapping(snapshot)
    source_context = _mapping(technical_context)

    # A complete snapshot is a convenient input form for replay and for the
    # current pipeline.  Per-symbol arguments always win when explicitly
    # supplied, while snapshot maps fill only missing pieces.
    if not source_context and source_snapshot:
        source_context = source_snapshot
    context = _extract_technical(source_context, symbol)
    a2_context = _extract_symbol_payload(
        source_snapshot.get("A2_BOTTLENECK_CONTEXT") if source_snapshot else None,
        symbol,
    )
    if not a2_context:
        a2_context = _mapping(
            _first(raw_candidate, "a2_context", "A2_BOTTLENECK_CONTEXT", "a2", "market_context")
        )
    merged_a2 = _merge(a2_context, raw_candidate)

    raw_price = price_contract
    if raw_price is None and source_snapshot:
        raw_price = _extract_symbol_payload(source_snapshot.get("PRICE_LEVELS"), symbol)
    price = _normalize_price_contract(raw_price)

    raw_tradability = trading_eligibility
    if raw_tradability is None and source_snapshot:
        raw_tradability = _extract_symbol_payload(source_snapshot.get("TRADABILITY_FLAGS"), symbol)
    tradability = _mapping(raw_tradability)

    raw_kline = kline_labels
    if raw_kline is None and source_snapshot:
        raw_kline = _extract_symbol_payload(source_snapshot.get("KLINE_PATTERNS"), symbol)
    labels = _normalize_labels(raw_kline, raw_candidate, context)

    frames = _timeframes(context)
    daily = frames["daily"]
    weekly = frames["weekly"]
    monthly = frames["monthly"]
    daily_ma = _daily_moving_averages(daily, context)
    daily_close = _frame_close(daily, context, timeframe="daily")
    previous_ma = _previous_moving_averages(daily, context)
    daily_event = _normalize_event(_first(daily, "ma_event", "moving_average_event") or _first(context, "ma_event"))
    daily_state = _text(
        _first(daily, "state", "trend_state", "technical_state", "ma_alignment")
        or _first(context, "daily_state", "trend_state")
    ) or "UNKNOWN"
    monthly_state = _period_display_state(monthly, "monthly")
    weekly_state = _period_display_state(weekly, "weekly")
    monthly_partial_observation = _partial_period_observation(monthly)
    weekly_partial_observation = _partial_period_observation(weekly)

    market_regime = _normalize_state(
        _first(merged_a2, "market_regime", "market_state", "regime")
        or _first(context, "market_regime", "market_state", "regime")
        or _first(source_snapshot, "MARKET_REGIME", "market_regime")
    ) or "NEUTRAL"
    theme_stage = _normalize_state(
        _first(merged_a2, "theme_stage", "sector_stage", "stage")
        or _first(context, "theme_stage", "sector_stage", "stage")
    ) or "UNKNOWN"
    market_role = _normalize_role(merged_a2)
    permission = _normalize_state(
        _first(merged_a2, "sector_permission", "theme_permission", "entry_permission", "route_permission")
        or _first(context, "sector_permission", "theme_permission", "entry_permission")
    )
    ladder = _ladder_info(merged_a2)
    relative_strength = _relative_strength(merged_a2, context)
    kline = _kline_context(raw_kline, raw_candidate, context)
    setup = _technical_setup(
        daily=daily,
        daily_ma=daily_ma,
        previous_ma=previous_ma,
        daily_close=daily_close,
        daily_event=daily_event,
        labels=labels,
        kline=kline,
        context=context,
    )
    price_discovery = _is_price_discovery(raw_candidate, merged_a2, kline, labels)
    distribution = _has_distribution(raw_candidate, merged_a2, kline, labels)
    overextended = _is_overextended(raw_candidate, merged_a2, daily, daily_ma, daily_close, context)
    locked = _is_locked(raw_candidate, merged_a2, kline, labels)

    ladder_height = _number(ladder.get("height"))
    ladder_availability = _normalize_state(ladder.get("availability_state"))
    leader_route = (
        market_role == "EMOTION_LEADER"
        or (
            market_role in _LEADER_ROLES
            and (
                (ladder_height is not None and ladder_height >= 2)
                or ladder_availability in {"SOURCE_FAILED", "NOT_CONFIGURED", "UNAVAILABLE", "UNKNOWN"}
            )
        )
    )
    trend_route = _trend_route_signal(
        merged_a2,
        daily_ma=daily_ma,
        daily_close=daily_close,
        daily=daily,
        price_discovery=price_discovery,
        labels=labels,
        kline=kline,
    )
    ma520_route = _ma520_route_signal(
        daily_ma=daily_ma,
        daily_close=daily_close,
        daily_event=daily_event,
        setup=setup,
        merged_a2=merged_a2,
    )
    trend_structure_confirmed = (
        _ordered_above(daily_ma.get("ma5"), daily_ma.get("ma10"), daily_ma.get("ma20"))
        and _above(daily_close, daily_ma.get("ma60"))
    ) or price_discovery
    if leader_route:
        profile = StrategyProfile.LEADER_INTRADAY
    elif ma520_route and not trend_structure_confirmed:
        profile = StrategyProfile.MA520_SWING
    elif trend_route:
        profile = StrategyProfile.TREND_MA5
    elif ma520_route:
        profile = StrategyProfile.MA520_SWING
    else:
        profile = StrategyProfile.NO_NEXT_DAY_PLAN

    required: list[str] = []
    met: list[str] = []
    unmet: list[str] = []
    vetoes: list[str] = []
    reason_codes: list[str] = []
    condition_details: dict[str, dict[str, Any]] = {}
    data_gaps: list[str] = []
    watch_reasons: list[str] = []

    def condition(
        name: str,
        ok: bool,
        *,
        missing: bool = False,
        reason: str | None = None,
        watch: bool = False,
    ) -> None:
        required.append(name)
        detail = {"met": bool(ok), "reason": reason or ("OK" if ok else "NOT_MET")}
        condition_details[name] = detail
        if ok:
            met.append(name)
            return
        unmet.append(name)
        code = reason or name
        _append_unique(reason_codes, code)
        if missing:
            _append_unique(data_gaps, code)
        if watch:
            _append_unique(watch_reasons, code)

    def veto(code: str) -> None:
        _append_unique(vetoes, code)
        _append_unique(reason_codes, code)

    if not symbol:
        data_gaps.append("SYMBOL_MISSING")

    conditional_probe = False
    higher_timeframe_risk = "ALIGNED_OR_NEUTRAL"
    if profile is not StrategyProfile.NO_NEXT_DAY_PLAN:
        month_status = _period_status(monthly, require_explicit=True)
        week_status = _period_status(weekly, require_explicit=True)
        day_status = _period_status(daily, require_explicit=False)
        condition(
            "MONTH_CLOSED",
            month_status == "CLOSED",
            missing=True,
            reason="MONTH_NOT_CLOSED" if month_status != "CLOSED" else None,
        )
        condition(
            "WEEK_CLOSED",
            week_status == "CLOSED",
            missing=True,
            reason="WEEK_NOT_CLOSED" if week_status != "CLOSED" else None,
        )
        condition(
            "DAILY_CLOSED",
            day_status == "CLOSED",
            missing=True,
            reason="DAILY_NOT_CLOSED" if day_status != "CLOSED" else None,
        )
        condition(
            "TRADABLE",
            _tradable_state(tradability) is True,
            missing=_tradable_state(tradability) is None,
            reason=(
                "TRADABILITY_DATA_MISSING"
                if _tradable_state(tradability) is None
                else "NOT_TRADABLE" if _tradable_state(tradability) is False else None
            ),
        )
        condition(
            "DAILY_CLOSE_AVAILABLE",
            daily_close is not None,
            missing=True,
            reason="DAILY_CLOSE_MISSING" if daily_close is None else None,
        )
        higher_timeframe_bear = _is_bear_state(monthly_state) or _is_bear_state(weekly_state)
        higher_timeframe_hard_bear = (
            _is_hard_bear_state(monthly_state) or _is_hard_bear_state(weekly_state)
        )
        daily_bear_for_cycle = _is_bear_state(daily_state)
        if higher_timeframe_hard_bear and daily_bear_for_cycle:
            higher_timeframe_risk = "HARD_BEAR_WITH_DAILY_CONFIRMATION"
            veto("HIGHER_TIMEFRAME_BEARISH")
            condition(
                "HIGHER_TIMEFRAME_NOT_BEARISH",
                False,
                reason="HIGHER_TIMEFRAME_BEARISH",
            )
        elif higher_timeframe_bear:
            # A partial/lagging weekly stack is context, not an entry signal.
            # Keep the candidate as a probe and let A4 demand a fresh 15m/5m
            # confirmation.  This does not bypass daily, price, risk or
            # tradability gates below.
            higher_timeframe_risk = "CONDITIONAL_PROBE"
            conditional_probe = True
            _append_unique(reason_codes, "HIGHER_TIMEFRAME_CONDITIONAL_PROBE")
            condition("HIGHER_TIMEFRAME_RISK_CLASSIFIED", True)
        else:
            condition("HIGHER_TIMEFRAME_NOT_BEARISH", True)
        if market_regime in _RISK_OFF_STATES:
            veto("MARKET_RISK_OFF")
        condition(
            "MARKET_NOT_RISK_OFF",
            market_regime not in _RISK_OFF_STATES,
            reason="MARKET_RISK_OFF" if market_regime in _RISK_OFF_STATES else None,
        )
        if permission in _NO_ENTRY_PERMISSIONS:
            veto("SECTOR_NO_NEW_ENTRY")
        condition(
            "SECTOR_PERMISSION",
            permission not in _NO_ENTRY_PERMISSIONS,
            reason="SECTOR_NO_NEW_ENTRY" if permission in _NO_ENTRY_PERMISSIONS else None,
        )

        geometry_ok = price["geometry_valid"] is True
        condition(
            "PRICE_GEOMETRY_VALID",
            geometry_ok,
            missing=True,
            reason="PRICE_GEOMETRY_INVALID" if not geometry_ok else None,
        )

        if profile is StrategyProfile.LEADER_INTRADAY:
            _evaluate_leader(
                condition=condition,
                veto=veto,
                market_role=market_role,
                theme_stage=theme_stage,
                ladder=ladder,
                locked=locked,
                distribution=distribution,
                daily_state=daily_state,
                raw_candidate=raw_candidate,
                merged_a2=merged_a2,
            )
        elif profile is StrategyProfile.TREND_MA5:
            _evaluate_trend(
                condition=condition,
                veto=veto,
                daily_ma=daily_ma,
                daily_close=daily_close,
                daily=daily,
                context=context,
                merged_a2=merged_a2,
                relative_strength=relative_strength,
                price_discovery=price_discovery,
                overextended=overextended,
                distribution=distribution,
                labels=labels,
                kline=kline,
                setup=setup,
                price=price,
            )
        else:
            _evaluate_520(
                condition=condition,
                veto=veto,
                daily_ma=daily_ma,
                daily_close=daily_close,
                daily=daily,
                context=context,
                daily_event=daily_event,
                setup=setup,
                distribution=distribution,
                labels=labels,
            )

        # A first board, a four-plus board, and a locked one-price board are
        # observable but not executable.  They are intentionally WATCH, not
        # hard rejections, so the next session can still display the reason.
        if profile is StrategyProfile.LEADER_INTRADAY:
            height = ladder.get("height")
            if height is None:
                condition("LADDER_HEIGHT_AVAILABLE", False, missing=True, reason="LADDER_HEIGHT_MISSING")
            elif height <= 1:
                condition("BOARD_NOT_FIRST_OBSERVATION_ONLY", False, reason="FIRST_BOARD_OBSERVE_ONLY", watch=True)
            elif height >= 4:
                condition("BOARD_NOT_HIGH_RISK_4_PLUS", False, reason="FOUR_PLUS_BOARD_WATCH_ONLY", watch=True)
            if locked:
                condition("NOT_ONE_PRICE_LOCKED", False, reason="ONE_PRICE_LOCKED_OBSERVE_ONLY", watch=True)
            else:
                condition("NOT_ONE_PRICE_LOCKED", True)

        # Explicit known-negative conditions are hard rejection.  Unknown
        # conditions remain data gaps so they cannot be mistaken for a real
        # opportunity.
        if distribution:
            veto("HIGH_VOLUME_DISTRIBUTION")
        if profile is StrategyProfile.TREND_MA5 and overextended:
            veto("TREND_OVEREXTENDED")
        if profile is StrategyProfile.MA520_SWING and setup["dead_cross"]:
            veto("MA520_DEAD_CROSS")
        if profile is not StrategyProfile.LEADER_INTRADAY:
            daily_bear = _is_bear_state(daily_state)
            if daily_bear:
                veto("DAILY_TREND_WEAK")
            condition(
                "DAILY_NOT_BEARISH",
                not daily_bear,
                reason="DAILY_TREND_WEAK" if daily_bear else None,
            )
    else:
        _append_unique(reason_codes, "NO_APPLICABLE_STRATEGY")

    # Do not emit a malformed plan even when a route is recognized.  Invalid
    # geometry is a data contract failure, while a valid WATCH remains visible
    # but is never promotable to A4 by this module.
    if profile is StrategyProfile.NO_NEXT_DAY_PLAN:
        eligibility = Eligibility.REJECTED
    elif data_gaps:
        eligibility = Eligibility.DATA_GAP
    elif vetoes:
        eligibility = Eligibility.REJECTED
    elif watch_reasons or unmet:
        eligibility = Eligibility.WATCH
    else:
        eligibility = Eligibility.QUALIFIED

    plan_mode = _plan_mode(
        profile,
        eligibility,
        ladder,
        setup,
        conditional_probe=conditional_probe,
    )
    zone = price["entry_reference_zone"] if price["geometry_valid"] else None
    no_chase = price["no_chase_price"] if price["geometry_valid"] else None
    invalidation = price["daily_invalidation"] if price["geometry_valid"] else None
    facts = _strategy_facts(
        profile=profile,
        eligibility=eligibility,
        plan_mode=plan_mode,
        market_role=market_role,
        theme_stage=theme_stage,
        ladder=ladder,
        daily_ma=daily_ma,
        daily_close=daily_close,
        previous_ma=previous_ma,
        daily_event=daily_event,
        daily_state=daily_state,
        monthly_status=_period_status(monthly, require_explicit=True),
        weekly_status=_period_status(weekly, require_explicit=True),
        market_regime=market_regime,
        relative_strength=relative_strength,
        price_discovery=price_discovery,
        overextended=overextended,
        distribution=distribution,
        locked=locked,
        price=price,
        setup=setup,
        labels=labels,
        condition_details=condition_details,
    )
    facts["higher_timeframe_risk"] = higher_timeframe_risk
    facts["conditional_probe"] = conditional_probe
    if price_discovery and zone is not None and invalidation is not None:
        risk_unit = zone["high"] - invalidation
        if risk_unit > 0:
            facts["r_unit"] = _round(risk_unit)
            facts["observation_targets"] = {
                "r2": _round(zone["high"] + 2.0 * risk_unit),
                "r3": _round(zone["high"] + 3.0 * risk_unit),
                "target_basis": "R_MULTIPLE_NO_RESISTANCE_REQUIRED",
            }

    plan_expiry = _first(raw_candidate, "plan_expiry", "expiry", "valid_until")
    if plan_expiry is None:
        plan_expiry = _first(context, "plan_expiry", "expiry", "valid_until")
    if plan_expiry is None and as_of is not None:
        plan_expiry = _iso(as_of)

    llm_status = "DATA_GAP" if eligibility is Eligibility.DATA_GAP else "PASS"
    if eligibility is Eligibility.REJECTED and vetoes:
        llm_status = "VETO"

    return {
        "strategy_profile": profile.value,
        "strategy_version": STRATEGY_VERSION,
        "symbol": symbol or None,
        "name": _text(_first(raw_candidate, "name", "company_name", "security_name")) or None,
        "candidate_origin": _text(_first(raw_candidate, "candidate_origin", "origin")) or "A2",
        "market_role": market_role or None,
        "market_regime": market_regime,
        "theme_stage": theme_stage,
        "monthly_state": monthly_state,
        "monthly_partial_observation": monthly_partial_observation,
        "weekly_closed_state": weekly_state,
        "weekly_partial_observation": weekly_partial_observation,
        "daily_state": daily_state,
        "daily_ma": {key: _round(value) for key, value in daily_ma.items()},
        "daily_macd": _macd(daily, context),
        "daily_volume_state": _volume_state(daily, context, kline),
        "relative_strength": relative_strength,
        "entry_reference_zone": zone,
        "no_chase_price": no_chase,
        "price_discovery": bool(price_discovery),
        "daily_invalidation": invalidation,
        "plan_premises": _plan_premises(profile),
        "a4_required_entry_rules": _a4_entry_rules(profile),
        "a4_exit_rules": _a4_exit_rules(profile),
        "plan_mode": plan_mode,
        "plan_expiry": plan_expiry,
        "eligibility": eligibility.value,
        "required_conditions": _dedupe(required),
        "met_conditions": _dedupe(met),
        "unmet_conditions": _dedupe(unmet),
        "veto_conditions": _dedupe(vetoes),
        "reason_codes": _dedupe(reason_codes + data_gaps + watch_reasons),
        "llm_review": {"status": llm_status, "reason_codes": _dedupe(vetoes + data_gaps)},
        "strategy_facts": facts,
    }


def route_a3_strategy(
    candidate: Mapping[str, Any] | None,
    technical_context: Mapping[str, Any] | None = None,
    price_contract: Mapping[str, Any] | None = None,
    trading_eligibility: Mapping[str, Any] | None = None,
    kline_labels: Mapping[str, Any] | Sequence[Any] | None = None,
    *,
    snapshot: Mapping[str, Any] | None = None,
    as_of: datetime | date | str | None = None,
) -> dict[str, Any]:
    """Alias emphasizing that A3 chooses one and only one strategy route."""

    return evaluate_a3_candidate(
        candidate,
        technical_context,
        price_contract,
        trading_eligibility,
        kline_labels,
        snapshot=snapshot,
        as_of=as_of,
    )


def evaluate_a3_strategy(
    candidate: Mapping[str, Any] | None,
    *,
    factor: Mapping[str, Any] | Any,
    price_levels: Mapping[str, Any] | Any,
    tradability: Mapping[str, Any] | Any,
    kline: Mapping[str, Any] | Sequence[Any] | Any,
    a2_context: Mapping[str, Any] | None = None,
    market_regime: str | None = None,
    sector_permission: str | None = None,
) -> A3StrategyDecision:
    """Evaluate a candidate through the stable keyword-only A3 contract.

    This is the preferred integration entry.  The older, more permissive
    :func:`evaluate_a3_candidate` remains available for replay fixtures and
    accepts the same data in positional form.  ``factor`` is the frozen
    daily/weekly/monthly technical context, while the other three arguments
    are explicitly separated to prevent an unavailable source from being
    mistaken for a neutral value.
    """

    context = _mapping(factor)
    # Do not mutate a caller-owned frozen snapshot/model dump.  These values
    # are optional context overrides supplied by the caller, not model votes.
    if market_regime is not None or sector_permission is not None:
        context = dict(context)
        if market_regime is not None:
            context["market_regime"] = market_regime
        if sector_permission is not None:
            context["sector_permission"] = sector_permission

    candidate_map = dict(_mapping(candidate))
    if a2_context is not None:
        candidate_map["a2_context"] = dict(_mapping(a2_context))
    result = evaluate_a3_candidate(
        candidate_map,
        context,
        _mapping(price_levels),
        _mapping(tradability),
        kline,
    )
    return A3StrategyDecision.model_validate(result)


def build_a3_strategy_decision(
    candidate: Mapping[str, Any] | None,
    technical_context: Mapping[str, Any] | None = None,
    price_contract: Mapping[str, Any] | None = None,
    trading_eligibility: Mapping[str, Any] | None = None,
    kline_labels: Mapping[str, Any] | Sequence[Any] | None = None,
    *,
    snapshot: Mapping[str, Any] | None = None,
    as_of: datetime | date | str | None = None,
) -> dict[str, Any]:
    """Descriptive alias used by replay callers."""

    return route_a3_strategy(
        candidate,
        technical_context,
        price_contract,
        trading_eligibility,
        kline_labels,
        snapshot=snapshot,
        as_of=as_of,
    )


def _evaluate_leader(
    *,
    condition: Any,
    veto: Any,
    market_role: str,
    theme_stage: str,
    ladder: Mapping[str, Any],
    locked: bool,
    distribution: bool,
    daily_state: str,
    raw_candidate: Mapping[str, Any],
    merged_a2: Mapping[str, Any],
) -> None:
    condition("A2_LEADER_ROLE_CONFIRMED", market_role in _LEADER_ROLES, missing=not bool(market_role), reason="A2_LEADER_ROLE_MISSING" if not market_role else None)
    if theme_stage in _BAD_LEADER_STAGES or theme_stage in _RISK_OFF_STATES:
        veto("LEADER_THEME_RETREAT_OR_CLIMAX")
    condition(
        "THEME_IN_EARLY_CYCLE",
        theme_stage in _LEADER_STAGES,
        missing=not bool(theme_stage) or theme_stage == "UNKNOWN",
        reason="THEME_STAGE_NOT_EARLY" if theme_stage not in _LEADER_STAGES else None,
    )
    ladder_state = _normalize_state(_first(ladder, "state", "status", "ladder_state"))
    ladder_intact = _first(ladder, "intact", "not_broken", "unbroken")
    intact = False if _explicit_false(ladder_intact) or ladder_state in {"BROKEN", "RETREAT", "FAIL"} else True
    condition(
        "LADDER_INTACT",
        intact,
        missing=ladder_intact is None and ladder_state == "UNKNOWN" and not ladder,
        reason="LADDER_BROKEN" if not intact else None,
    )
    condition("LEADER_DAILY_NOT_DISTRIBUTION", not distribution, reason="HIGH_VOLUME_DISTRIBUTION" if distribution else None)
    bearish = _is_bear_state(daily_state)
    if bearish:
        veto("LEADER_DAILY_TREND_WEAK")
    condition("LEADER_DAILY_NOT_BEARISH", not bearish, reason="LEADER_DAILY_TREND_WEAK" if bearish else None)
    # If A2 explicitly marks the individual role as uncertain, preserve the
    # route but make it a data gap instead of silently promoting it.
    role_data = _first(merged_a2, "leader_structure", "role_breakdown")
    if role_data is None and not market_role:
        condition("A2_ROLE_EVIDENCE_PRESENT", False, missing=True, reason="A2_ROLE_EVIDENCE_MISSING")


def _evaluate_trend(
    *,
    condition: Any,
    veto: Any,
    daily_ma: Mapping[str, float | None],
    daily_close: float | None,
    daily: Mapping[str, Any],
    context: Mapping[str, Any],
    merged_a2: Mapping[str, Any],
    relative_strength: Mapping[str, Any],
    price_discovery: bool,
    overextended: bool,
    distribution: bool,
    labels: set[str],
    kline: Mapping[str, Any],
    setup: Mapping[str, Any],
    price: Mapping[str, Any],
) -> None:
    ma5 = daily_ma.get("ma5")
    ma10 = daily_ma.get("ma10")
    ma20 = daily_ma.get("ma20")
    ma60 = daily_ma.get("ma60")
    stack = _ordered_above(ma5, ma10, ma20)
    above60 = _above(daily_close, ma60)
    slopes = _ma_slopes(daily, context)
    slope_ok = _slope_not_down(slopes, ("ma5", "ma10"))
    slope_missing = any(_number(slopes.get(key)) is None for key in ("ma5", "ma10"))
    rs_ok = _relative_strength_is_strong(relative_strength)
    rs_missing = not bool(relative_strength)
    platform = _platform_evidence(merged_a2, daily, kline, labels, price_discovery)
    condition("DAILY_CLOSE_ABOVE_MA60", above60, missing=ma60 is None or daily_close is None, reason="CLOSE_NOT_ABOVE_MA60" if not above60 else None)
    condition("DAILY_MA5_ABOVE_MA10_ABOVE_MA20", stack, missing=any(value is None for value in (ma5, ma10, ma20)), reason="DAILY_MA_STACK_NOT_BULL" if not stack else None)
    condition("DAILY_MA_SLOPE_NOT_DOWN", slope_ok, missing=slope_missing, reason="DAILY_MA_SLOPE_MISSING" if slope_missing else "DAILY_MA_SLOPE_DOWN" if not slope_ok else None)
    condition("RELATIVE_STRENGTH_CONFIRMED", rs_ok, missing=rs_missing, reason="RELATIVE_STRENGTH_MISSING" if rs_missing else "RELATIVE_STRENGTH_WEAK" if not rs_ok else None)
    condition(
        "PLATFORM_OR_MAIN_RISE_CONFIRMED",
        platform,
        reason="MAIN_RISE_EVIDENCE_MISSING" if not platform else None,
        watch=not platform,
    )
    extension_known = _extension_known(merged_a2, daily, context, daily_close, daily_ma)
    condition("EXTENSION_DATA_OBSERVED", extension_known, missing=not extension_known, reason="EXTENSION_DATA_MISSING" if not extension_known else None)
    condition("NOT_OVEREXTENDED", not overextended, reason="TREND_OVEREXTENDED" if overextended else None)
    condition("NOT_DISTRIBUTION", not distribution, reason="HIGH_VOLUME_DISTRIBUTION" if distribution else None)
    if price_discovery:
        condition("PRICE_DISCOVERY_TREND", True)
        # This is the explicit exception: absence of first resistance is not
        # a data gap when the daily close is making price discovery.
        condition("RESISTANCE_NOT_REQUIRED_FOR_NEW_HIGH", True)
    else:
        first_resistance = _number(_first(kline, "first_resistance"))
        if first_resistance is None:
            first_resistance = _number(price.get("first_resistance"))
        condition(
            "FIRST_RESISTANCE_AVAILABLE",
            first_resistance is not None or _first(kline, "resistance_not_required") is True,
            missing=first_resistance is None,
            reason="FIRST_RESISTANCE_MISSING" if first_resistance is None and _first(kline, "resistance_not_required") is not True else None,
        )
    # These observations are deliberately explanatory, not an extra score or
    # a hidden weighted gate.  A4 performs the actual pullback confirmation.
    condition("A4_WILL_CONFIRM_DAILY_MA5_PULLBACK", True)


def _evaluate_520(
    *,
    condition: Any,
    veto: Any,
    daily_ma: Mapping[str, float | None],
    daily_close: float | None,
    daily: Mapping[str, Any],
    context: Mapping[str, Any],
    daily_event: str,
    setup: Mapping[str, Any],
    distribution: bool,
    labels: set[str],
) -> None:
    ma5 = daily_ma.get("ma5")
    ma20 = daily_ma.get("ma20")
    slopes = _ma_slopes(daily, context)
    ma20_slope = _number(slopes.get("ma20"))
    slope_ok = ma20_slope is not None and ma20_slope >= 0
    if setup["dead_cross"]:
        veto("MA520_DEAD_CROSS")
    condition("DAILY_MA5_MA20_AVAILABLE", ma5 is not None and ma20 is not None, missing=True, reason="MA520_VALUES_MISSING" if ma5 is None or ma20 is None else None)
    condition("DAILY_MA5_NOT_BELOW_MA20", _above_or_equal(ma5, ma20), reason="MA5_BELOW_MA20" if not _above_or_equal(ma5, ma20) else None)
    condition("DAILY_MA20_SLOPE_NOT_DOWN", slope_ok, missing=ma20_slope is None, reason="MA20_SLOPE_MISSING" if ma20_slope is None else "MA20_SLOPE_DOWN" if not slope_ok else None)
    condition("DAILY_CLOSE_ABOVE_MA20", _above(daily_close, ma20), missing=daily_close is None or ma20 is None, reason="CLOSE_NOT_ABOVE_MA20" if not _above(daily_close, ma20) else None)
    setup_confirmed = setup["golden_cross"] or setup["pullback_hold"] or setup["reclaim"]
    # A known dead cross is a real veto, not a missing-data condition.  Only
    # call an absent setup a data gap when the underlying MA/event evidence is
    # itself unavailable.
    setup_missing = not setup_confirmed and not daily_event and (ma5 is None or ma20 is None)
    condition("MA520_SETUP_CONFIRMED", setup_confirmed, missing=setup_missing, reason="MA520_SETUP_NOT_CONFIRMED" if not setup_confirmed else None)
    condition("NOT_HIGH_VOLUME_DISTRIBUTION", not distribution, reason="HIGH_VOLUME_DISTRIBUTION" if distribution else None)
    if setup["reclaim"] and not setup["golden_cross"] and not setup["pullback_hold"]:
        condition("RECLAIM_IS_PROBE_ONLY", True)
    if "KDJ_GOLDEN_CROSS" in labels and not (setup["golden_cross"] or setup["pullback_hold"] or setup["reclaim"]):
        # Keep the reason visible without using KDJ as a gate.
        condition("KDJ_NOT_SOLE_REASON", False, reason="KDJ_ONLY_NOT_A_SETUP", watch=True)


def _technical_setup(
    *,
    daily: Mapping[str, Any],
    daily_ma: Mapping[str, float | None],
    previous_ma: Mapping[str, float | None],
    daily_close: float | None,
    daily_event: str,
    labels: set[str],
    kline: Mapping[str, Any],
    context: Mapping[str, Any],
) -> dict[str, bool]:
    event = _normalize_event(daily_event)
    dead_cross = event in _DEAD_CROSS_EVENTS or (
        daily_ma.get("ma5") is not None
        and daily_ma.get("ma20") is not None
        and daily_ma["ma5"] < daily_ma["ma20"]
        and "DEAD_CROSS" in event
    )
    golden = event in _GOLDEN_CROSS_EVENTS
    if not golden:
        prev5 = previous_ma.get("ma5")
        prev20 = previous_ma.get("ma20")
        current5 = daily_ma.get("ma5")
        current20 = daily_ma.get("ma20")
        golden = _above(prev20, prev5) and _above(current5, current20)
    ma20 = daily_ma.get("ma20")
    latest_low = _frame_low(daily)
    pullback = (
        ma20 is not None
        and daily_close is not None
        and latest_low is not None
        and latest_low <= ma20 <= daily_close
    ) or event in _RECLAIM_EVENTS or "PULLBACK_HOLD_MA20" in labels
    reclaim = event in {"RECLAIM_MA20", "MA20_RECLAIM", "RECOVER_MA20"}
    if not reclaim:
        prev_close = _number(_first(daily, "previous_close")) or _number(_first(context, "previous_close"))
        reclaim = prev_close is not None and ma20 is not None and prev_close < ma20 <= (daily_close or -math.inf)
    return {
        "dead_cross": dead_cross,
        "golden_cross": golden,
        "pullback_hold": pullback,
        "reclaim": reclaim,
    }


def _trend_route_signal(
    candidate: Mapping[str, Any],
    *,
    daily_ma: Mapping[str, float | None],
    daily_close: float | None,
    daily: Mapping[str, Any],
    price_discovery: bool,
    labels: set[str],
    kline: Mapping[str, Any],
) -> bool:
    role = _normalize_role(candidate)
    explicit = _truthy(_first(candidate, "trend_core", "trend_candidate", "trend_ma5", "main_rise"))
    role_signal = role in {"TREND_CORE", "TREND_LEADER", "INSTITUTIONAL_CORE", "CORE_ARMY"}
    stack = _ordered_above(daily_ma.get("ma5"), daily_ma.get("ma10"), daily_ma.get("ma20"))
    above60 = _above(daily_close, daily_ma.get("ma60"))
    label_signal = bool(labels & {"PLATFORM_BREAKOUT", "BREAKOUT", "MAIN_RISE", "UPTREND", "主升", "平台突破"})
    return explicit or role_signal or (stack and above60) or price_discovery or label_signal


def _ma520_route_signal(
    *,
    daily_ma: Mapping[str, float | None],
    daily_close: float | None,
    daily_event: str,
    setup: Mapping[str, bool],
    merged_a2: Mapping[str, Any],
) -> bool:
    if _normalize_event(daily_event) in _DEAD_CROSS_EVENTS | _GOLDEN_CROSS_EVENTS | _RECLAIM_EVENTS:
        return True
    if setup["dead_cross"] or setup["golden_cross"] or setup["pullback_hold"] or setup["reclaim"]:
        return True
    ma5 = daily_ma.get("ma5")
    ma20 = daily_ma.get("ma20")
    return (
        ma5 is not None
        and ma20 is not None
        and daily_close is not None
        and ma5 >= ma20
        and daily_close >= ma20
        and _truthy(_first(merged_a2, "ma520_candidate", "520_candidate"))
    )


def _strategy_facts(
    *,
    profile: StrategyProfile,
    eligibility: Eligibility,
    plan_mode: str | None,
    market_role: str,
    theme_stage: str,
    ladder: Mapping[str, Any],
    daily_ma: Mapping[str, float | None],
    daily_close: float | None,
    previous_ma: Mapping[str, float | None],
    daily_event: str,
    daily_state: str,
    monthly_status: str,
    weekly_status: str,
    market_regime: str,
    relative_strength: Mapping[str, Any],
    price_discovery: bool,
    overextended: bool,
    distribution: bool,
    locked: bool,
    price: Mapping[str, Any],
    setup: Mapping[str, bool],
    labels: set[str],
    condition_details: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "route_basis": profile.value,
        "eligibility_basis": eligibility.value,
        "plan_mode": plan_mode,
        "market_role": market_role or None,
        "theme_stage": theme_stage,
        "ladder": dict(ladder),
        "daily_close": _round(daily_close),
        "daily_moving_averages": {key: _round(value) for key, value in daily_ma.items()},
        "previous_moving_averages": {key: _round(value) for key, value in previous_ma.items()},
        "daily_ma_event": daily_event or None,
        "daily_state": daily_state,
        "monthly_status": monthly_status,
        "weekly_status": weekly_status,
        "market_regime": market_regime,
        "relative_strength_observation": dict(relative_strength),
        "price_discovery": price_discovery,
        "overextended": overextended,
        "distribution": distribution,
        "one_price_locked": locked,
        "price_source": price.get("source"),
        "price_contract_available": price.get("available"),
        "ma520_setup": dict(setup),
        "kline_labels": sorted(labels),
        "condition_details": dict(condition_details),
        "decision_style": "EXPLICIT_CONDITIONS_NO_COMPOSITE_SCORE",
    }


def _plan_mode(
    profile: StrategyProfile,
    eligibility: Eligibility,
    ladder: Mapping[str, Any],
    setup: Mapping[str, bool],
    *,
    conditional_probe: bool = False,
) -> str | None:
    if eligibility is not Eligibility.QUALIFIED:
        return None
    if conditional_probe:
        return "PROBE"
    if profile is StrategyProfile.LEADER_INTRADAY and (_number(ladder.get("height")) or 0) >= 3:
        return "PROBE"
    if profile is StrategyProfile.MA520_SWING and setup.get("reclaim") and not setup.get("golden_cross"):
        return "PROBE"
    if profile in {StrategyProfile.LEADER_INTRADAY, StrategyProfile.TREND_MA5, StrategyProfile.MA520_SWING}:
        return "STANDARD"
    return None


def _plan_premises(profile: StrategyProfile) -> list[str]:
    return {
        StrategyProfile.LEADER_INTRADAY: [
            "A2已确认题材龙头身份",
            "题材处于启动/确认/早期加速，A4等待分歧转一致或有效回封",
            "A3只给日线风险边界，不能把一字板当作可成交入场",
        ],
        StrategyProfile.TREND_MA5: [
            "日线主升趋势成立，沿日线MA5观察回踩",
            "创新高时使用入场区间与失效位定义R，不虚构历史压力位",
            "顺势加仓且必须已有浮盈，禁止亏损摊平",
        ],
        StrategyProfile.MA520_SWING: [
            "5和20均指日线MA5、MA20",
            "A3确认金叉/MA20回踩/收复，A4只做盘中确认",
            "KDJ和盘中均线仅作观察，不替代日线价格结构",
        ],
        StrategyProfile.NO_NEXT_DAY_PLAN: ["没有满足条件的适用策略，不发布次日计划"],
    }[profile]


def _a4_entry_rules(profile: StrategyProfile) -> list[str]:
    return {
        StrategyProfile.LEADER_INTRADAY: [
            "闭合15分钟结构不再走弱",
            "闭合5分钟回收VWAP并连续两根确认",
            "板块梯队与中军未同步转弱，且价格未超过A3禁止追价线",
        ],
        StrategyProfile.TREND_MA5: [
            "价格进入A3日线MA5回踩区且未触发日线失效位",
            "闭合15分钟卖压减弱，闭合5分钟形成止跌/更高低点",
            "5分钟收回VWAP并有一根温和放量阳线确认",
        ],
        StrategyProfile.MA520_SWING: [
            "价格位于A3日线MA20/关键支撑上方且未超过禁止追价线",
            "闭合15分钟结构稳定，闭合5分钟形成更高低点",
            "5分钟收回VWAP并连续两根闭合K线确认",
        ],
        StrategyProfile.NO_NEXT_DAY_PLAN: [],
    }[profile]


def _a4_exit_rules(profile: StrategyProfile) -> list[str]:
    return {
        StrategyProfile.LEADER_INTRADAY: [
            "梯队断层或龙头连续两根闭合5分钟不能收回VWAP",
            "高位放量长上影并跌破15分钟结构低点",
            "A3日线失效位、市场急剧退潮或计划过期",
        ],
        StrategyProfile.TREND_MA5: [
            "放量跌破A3日线MA5参考位且连续15分钟不能收回",
            "反抽MA5失败后再次回落，或A3日线收盘确认趋势失效",
            "硬止损、市场冲击或板块主线结束；亏损不得加仓",
        ],
        StrategyProfile.MA520_SWING: [
            "两根闭合15分钟位于关键结构下方且反抽失败",
            "放量下跌并反抽不过关键位，或A3日线失效位触发",
            "日线MA5下穿MA20交由下一收盘后的A3退出计划处理",
        ],
        StrategyProfile.NO_NEXT_DAY_PLAN: [],
    }[profile]


def _extract_technical(value: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    if not value:
        return {}
    factor = _extract_symbol_payload(value.get("FACTOR_SNAPSHOT"), symbol)
    if factor:
        result = dict(factor)
    elif _looks_like_factor(value):
        result = dict(value)
    else:
        result = {}
    for key in (
        "timeframes",
        "daily",
        "weekly",
        "monthly",
        "month",
        "latest",
        "ma_event",
        "market_regime",
        "market_state",
        "regime",
        "daily_state",
        "previous_close",
    ):
        if key in value and key not in result:
            result[key] = value[key]
    return result


def _looks_like_factor(value: Mapping[str, Any]) -> bool:
    return any(key in value for key in ("timeframes", "daily", "weekly", "moving_averages", "latest", "ready"))


def _timeframes(context: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    frames = _mapping(context.get("timeframes"))
    result: dict[str, dict[str, Any]] = {}
    for name, aliases in {
        "daily": ("daily", "day", "1d", "D"),
        "weekly": ("weekly", "week", "1w", "W"),
        "monthly": ("monthly", "month", "1mth", "1mo", "M"),
    }.items():
        raw: Any = None
        for alias in aliases:
            if alias in frames:
                raw = frames[alias]
                break
            if alias in context:
                raw = context[alias]
                break
        result[name] = _frame_mapping(raw)
    return result


def _frame_mapping(value: Any) -> dict[str, Any]:
    mapped = _mapping(value)
    if mapped:
        return dict(mapped)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        rows = [dict(_mapping(item)) for item in value if _mapping(item)]
        latest = rows[-1] if rows else {}
        return {"bars": rows, "latest": latest, "closed": bool(latest.get("closed", True))}
    return {}


def _extract_symbol_payload(value: Any, symbol: str) -> dict[str, Any]:
    mapped = _mapping(value)
    if not mapped:
        return {}
    if symbol and symbol in mapped and isinstance(mapped[symbol], Mapping):
        return dict(mapped[symbol])
    # Some maps use an unqualified symbol or the symbol as ``thscode``.  Do
    # not index arbitrary scalar maps as symbol payloads.
    if any(key in mapped for key in ("available", "ready", "timeframes", "by_symbol", "records", "trigger_zone", "invalidation")):
        return dict(mapped)
    return {}


def _normalize_price_contract(value: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = dict(_mapping(value))
    zone_raw = _first(raw, "entry_reference_zone", "trigger_zone", "entry_zone", "plan_zone", "zone")
    zone_map = _mapping(zone_raw)
    low = _number(_first(zone_map, "low", "min", "lower"))
    high = _number(_first(zone_map, "high", "max", "upper"))
    if low is None and high is None:
        low = _number(_first(raw, "entry_low", "trigger_low", "low"))
        high = _number(_first(raw, "entry_high", "trigger_high", "high"))
    invalidation = _number(_first(raw, "daily_invalidation", "invalidation", "invalidation_level", "stop_level", "stop"))
    no_chase = _number(_first(raw, "no_chase_price", "max_chase_price", "no_chase", "max_entry"))
    resistance = _number(_first(raw, "first_resistance", "resistance"))
    available = _first(raw, "available", "planning_ready")
    if available is None:
        available = bool(low is not None or high is not None or invalidation is not None or no_chase is not None)
    geometry_valid = (
        available is not False
        and low is not None
        and high is not None
        and invalidation is not None
        and no_chase is not None
        and low <= high
        and invalidation < low
        and no_chase >= high
    )
    return {
        "available": bool(available),
        "geometry_valid": bool(geometry_valid),
        "entry_reference_zone": {"low": _round(low), "high": _round(high)} if geometry_valid else None,
        "daily_invalidation": _round(invalidation) if geometry_valid else None,
        "no_chase_price": _round(no_chase) if geometry_valid else None,
        "first_resistance": _round(resistance),
        "source": _text(_first(raw, "source", "source_id", "provider")) or None,
        "raw_missing": [
            key
            for key, number in (("entry_reference_zone", low), ("daily_invalidation", invalidation), ("no_chase_price", no_chase))
            if number is None
        ],
    }


def _normalize_labels(value: Any, candidate: Mapping[str, Any], context: Mapping[str, Any]) -> set[str]:
    rows: list[Any] = []
    if isinstance(value, Mapping):
        for key in ("labels", "tags", "kline_labels", "pattern_labels"):
            raw = value.get(key)
            if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
                rows.extend(raw)
            elif raw is not None:
                rows.append(raw)
        for key in ("new_high", "innovation_high", "price_discovery", "distribution", "overextended", "locked_limit_up"):
            if value.get(key) is True:
                rows.append(key)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        rows.extend(value)
    for source in (candidate, context):
        for key in ("labels", "tags", "kline_labels", "pattern_labels"):
            raw = source.get(key)
            if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
                rows.extend(raw)
            elif raw is not None:
                rows.append(raw)
    return {_normalize_token(item) for item in rows if _text(item)}


def _kline_context(value: Any, candidate: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(_mapping(value))
    if not result:
        result = dict(_mapping(_first(context, "kline", "KLINE_PATTERNS")))
    for key in (
        "first_resistance",
        "breakout_20",
        "breakout_20_up",
        "volume_percentile_60",
        "volume_state",
        "direction",
        "body_ratio",
        "upper_shadow_ratio",
    ):
        if key in candidate and key not in result:
            result[key] = candidate[key]
        elif key in context and key not in result:
            result[key] = context[key]
    return result


def _is_price_discovery(candidate: Mapping[str, Any], a2: Mapping[str, Any], kline: Mapping[str, Any], labels: set[str]) -> bool:
    for source in (candidate, a2, kline):
        if _truthy(_first(source, "price_discovery", "innovation_high", "new_high", "is_new_high")):
            return True
    return bool(labels & {_normalize_token(value) for value in _NEW_HIGH_TOKENS})


def _has_distribution(candidate: Mapping[str, Any], a2: Mapping[str, Any], kline: Mapping[str, Any], labels: set[str]) -> bool:
    for source in (candidate, a2, kline):
        if _truthy(_first(source, "distribution", "high_volume_distribution", "is_distribution")):
            return True
    if labels & {_normalize_token(value) for value in _DISTRIBUTION_TOKENS}:
        return True
    body = _number(_first(kline, "volume_percentile_60", "volume_percentile"))
    upper = _number(_first(kline, "upper_shadow_ratio", "upper_shadow"))
    return body is not None and upper is not None and body >= 0.85 and upper >= 0.35 and _normalize_token(_first(kline, "direction")) == "BEARISH"


def _is_overextended(candidate: Mapping[str, Any], a2: Mapping[str, Any], daily: Mapping[str, Any], daily_ma: Mapping[str, float | None], daily_close: float | None, context: Mapping[str, Any]) -> bool:
    for source in (candidate, a2, daily, context):
        if _truthy(_first(source, "overextended", "over_extension", "high_extension")):
            return True
        atr_extension = _number(_first(source, "atr_extension", "extension_atr", "atr_multiple"))
        if atr_extension is not None and atr_extension > 3.0:
            return True
        extension_pct = _number(_first(source, "extension_pct", "ma5_extension_pct", "ma20_extension_pct"))
        if extension_pct is not None and extension_pct > 0.15:
            return True
    if daily_close is None:
        return False
    ma5 = daily_ma.get("ma5")
    ma20 = daily_ma.get("ma20")
    return bool((ma5 is not None and daily_close > ma5 * 1.12) or (ma20 is not None and daily_close > ma20 * 1.22))


def _is_locked(candidate: Mapping[str, Any], a2: Mapping[str, Any], kline: Mapping[str, Any], labels: set[str]) -> bool:
    for source in (candidate, a2, kline):
        if _truthy(_first(source, "one_price", "one_word", "locked", "locked_limit_up", "one_price_board")):
            return True
    return bool(labels & {_normalize_token(value) for value in _LOCKED_TOKENS})


def _ladder_info(a2: Mapping[str, Any]) -> dict[str, Any]:
    factor_scores = _mapping(_first(a2, "a2_factor_scores", "factor_scores"))
    deterministic_tier = _mapping(factor_scores.get("tier_structure"))
    deterministic_leader = _mapping(factor_scores.get("leader_structure"))
    nested = _mapping(_first(a2, "ladder", "tier_structure", "leader_structure"))
    # Prefer the server-owned factor record.  Model output may contain only a
    # reduced score/source object and must not erase ladder_height or the
    # observed-absent/source-failed distinction.
    sources = (deterministic_tier, nested, deterministic_leader, a2)
    height = _number(
        next(
            (
                value
                for source in sources
                if (
                    value := _first(
                        source,
                        "ladder_height",
                        "board_num",
                        "board_count",
                        "consecutive_boards",
                        "height",
                        "boards",
                    )
                ) is not None
            ),
            None,
        )
    )
    state = "UNKNOWN"
    intact: Any = None
    availability_state = "UNKNOWN"
    for source in sources:
        if state == "UNKNOWN":
            state = _normalize_state(
                _first(source, "ladder_state", "ladder_status", "tier_state", "state", "status")
            ) or "UNKNOWN"
        if intact is None:
            intact = _first(source, "ladder_intact", "ladder_unbroken", "not_broken", "intact", "unbroken")
        if availability_state == "UNKNOWN":
            availability_state = _normalize_state(
                _first(source, "availability_state", "data_state", "source_state")
            ) or "UNKNOWN"
    return {
        "height": int(height) if height is not None and height >= 0 else None,
        "state": state,
        "intact": intact,
        "availability_state": availability_state,
        "source": _text(_first(deterministic_tier, "source") or _first(nested, "source")) or None,
    }


def _relative_strength(a2: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
    raw = _first(a2, "relative_strength", "relative_strength_percentile", "rs", "relative")
    if raw is None:
        raw = _first(context, "relative_strength", "relative_strength_percentile", "rs", "relative")
    if raw is None:
        factor_scores = _mapping(_first(a2, "a2_factor_scores", "factor_scores"))
        raw = _first(factor_scores, "relative_strength", "relative")
    if raw is None:
        role_breakdown = _mapping(_first(a2, "role_breakdown", "leader_structure"))
        raw = _first(role_breakdown, "relative_strength", "relative_strength_percentile", "rs")
    if isinstance(raw, Mapping):
        result = dict(raw)
    elif raw is not None:
        result = {"percentile": _number(raw) or 0.0}
    else:
        result = {}
    for key in ("relative_strength_percentile", "industry_percentile", "market_percentile", "rs_percentile"):
        value = _number(_first(a2, key) or _first(context, key))
        if value is not None and key not in result:
            result[key] = value
    return {key: _round(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else value for key, value in result.items()}


def _relative_strength_is_strong(value: Mapping[str, Any]) -> bool:
    if _truthy(_first(value, "strong", "confirmed", "is_strong")):
        return True
    numbers = [_number(value.get(key)) for key in ("percentile", "relative_strength_percentile", "industry_percentile", "market_percentile", "value", "score")]
    numbers = [number for number in numbers if number is not None]
    if not numbers:
        return False
    maximum = max(numbers)
    # Percentile feeds in the existing adapters use both 0..1 and 0..100
    # conventions.  Normalize only this direct observation; never combine it
    # with another factor or turn it into a composite score.
    if 0.0 <= maximum <= 1.0:
        maximum *= 100.0
    return maximum >= 60.0


def _platform_evidence(candidate: Mapping[str, Any], daily: Mapping[str, Any], kline: Mapping[str, Any], labels: set[str], price_discovery: bool) -> bool:
    if price_discovery:
        return True
    if _truthy(_first(candidate, "platform_breakout", "main_rise", "maintrend_confirmed", "trend_confirmed")):
        return True
    # A2's explicit trend-core role is already a cross-sectional confirmation
    # that the stock is not an arbitrary MA stack.  It is not a numeric score
    # and does not replace the A3 daily MA/price gates below.
    if _normalize_role(candidate) in {"TREND_CORE", "TREND_LEADER", "INSTITUTIONAL_CORE"}:
        return True
    if labels & {"PLATFORM_BREAKOUT", "BREAKOUT", "MAIN_RISE", "UPTREND", "主升", "平台突破"}:
        return True
    breakout = _mapping(_first(kline, "breakout_20"))
    return breakout.get("up") is True or _truthy(_first(kline, "breakout_20_up"))


def _extension_known(candidate: Mapping[str, Any], daily: Mapping[str, Any], context: Mapping[str, Any], close: float | None, ma: Mapping[str, float | None]) -> bool:
    for source in (candidate, daily, context):
        if any(key in source for key in ("overextended", "over_extension", "high_extension", "extension_pct", "atr_extension", "ma5_extension_pct", "ma20_extension_pct")):
            return True
    return close is not None and ma.get("ma5") is not None and ma.get("ma20") is not None


def _daily_moving_averages(daily: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, float | None]:
    moving = _mapping(_first(daily, "moving_averages", "moving_average", "ma"))
    if not moving:
        moving = _mapping(_first(context, "moving_averages", "daily_moving_averages", "ma"))
    return {f"ma{period}": _number(_first(moving, f"ma{period}", f"MA{period}", str(period)) or _first(daily, f"ma{period}", f"MA{period}")) for period in (5, 10, 20, 60)}


def _previous_moving_averages(daily: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, float | None]:
    previous = _mapping(_first(daily, "previous_moving_averages", "previous_ma", "prior_moving_averages"))
    if not previous:
        previous = _mapping(_first(context, "previous_moving_averages", "previous_ma", "prior_moving_averages"))
    return {f"ma{period}": _number(_first(previous, f"ma{period}", f"MA{period}", str(period)) or _first(daily, f"previous_ma{period}", f"prev_ma{period}")) for period in (5, 10, 20, 60)}


def _ma_slopes(daily: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, float | None]:
    raw = _mapping(_first(daily, "ma_slopes", "moving_average_slopes", "slopes"))
    if not raw:
        raw = _mapping(_first(context, "ma_slopes", "moving_average_slopes", "slopes"))
    result: dict[str, float | None] = {}
    for period in (5, 10, 20, 60):
        value = _number(_first(raw, f"ma{period}", f"MA{period}", str(period)) or _first(daily, f"ma{period}_slope", f"slope_ma{period}"))
        if value is not None:
            result[f"ma{period}"] = value
    return result


def _frame_close(frame: Mapping[str, Any], context: Mapping[str, Any], *, timeframe: str) -> float | None:
    latest = _mapping(_first(frame, "latest", "last"))
    value = _first(frame, "close", "latest_close")
    if value is None:
        value = _first(latest, "close", "latest_close", "close_price")
    if value is None:
        value = _number(_first(context, f"{timeframe}_close", f"{timeframe}_latest_close"))
    return _number(value)


def _frame_low(frame: Mapping[str, Any]) -> float | None:
    latest = _mapping(_first(frame, "latest", "last"))
    value = _first(frame, "low", "latest_low")
    if value is None:
        value = _first(latest, "low", "latest_low", "low_price")
    if value is None:
        bars = _first(frame, "bars", "data")
        if isinstance(bars, Sequence) and not isinstance(bars, (str, bytes, bytearray)) and bars:
            value = _first(_mapping(bars[-1]), "low", "low_price")
    return _number(value)


def _period_status(frame: Mapping[str, Any], *, require_explicit: bool) -> str:
    if not frame:
        return "UNKNOWN"
    if frame.get("available") is False:
        return "UNKNOWN"
    explicit: Any = None
    for key in ("closed", "completed", "is_closed", "is_completed"):
        if key in frame:
            explicit = frame[key]
            break
    if explicit is True:
        return "CLOSED"
    if explicit is False:
        return "PARTIAL"
    for key in ("status", "state", "period_state", "completion_state"):
        state = _normalize_state(frame.get(key))
        if state in _CLOSED_STATES:
            return "CLOSED"
        if state in _PARTIAL_STATES:
            return "PARTIAL"
    if _truthy(_first(frame, "partial", "is_partial", "mtd", "wtd")):
        return "PARTIAL"
    # ``latest`` is the last *formal closed* month/week produced by the
    # factor engine.  The current in-progress period is carried separately in
    # ``partial_bars/latest_partial`` and must not make the formal frame
    # unknown.  This is explicit evidence even for the strict A3 contract.
    if _latest_closed(frame):
        return "CLOSED"
    # ``ready`` describes indicator completeness (for example whether a
    # monthly MA60 can be calculated), not whether a formal period bar has
    # closed.  A compact A3 factor can therefore legitimately carry
    # ``ready=false`` together with a hash-bound ``latest.closed=true``.
    # Only use readiness as a fallback after the explicit bar evidence above;
    # never let a long-history indicator requirement turn every otherwise
    # valid monthly frame into a DATA_GAP.
    if frame.get("ready") is False:
        return "UNKNOWN"
    if not require_explicit and _truthy(frame.get("ready")):
        return "CLOSED"
    return "UNKNOWN"


def _latest_closed(frame: Mapping[str, Any]) -> bool:
    latest = _mapping(_first(frame, "latest", "last"))
    if latest.get("closed") is True or latest.get("is_closed") is True:
        return True
    bars = _first(frame, "bars", "data")
    if isinstance(bars, Sequence) and not isinstance(bars, (str, bytes, bytearray)) and bars:
        last = _mapping(bars[-1])
        return last.get("closed") is not False and last.get("is_closed") is not False
    return False


def _period_display_state(frame: Mapping[str, Any], name: str) -> str:
    status = _period_status(frame, require_explicit=True)
    if status == "PARTIAL":
        return "MTD_OBSERVATION" if name == "monthly" else "WTD_OBSERVATION"
    if status != "CLOSED":
        return "UNKNOWN"
    raw = _normalize_state(_first(frame, "state", "trend_state", "technical_state", "ma_alignment"))
    return raw or "CLOSED"


def _partial_period_observation(frame: Mapping[str, Any]) -> dict[str, Any] | None:
    latest = _mapping(_first(frame, "latest_partial"))
    if not latest:
        partial = _first(frame, "partial_bars")
        if isinstance(partial, Sequence) and not isinstance(partial, (str, bytes, bytearray)) and partial:
            latest = _mapping(partial[-1])
    if not latest:
        return None
    return {
        "observation_only": True,
        "latest": latest,
    }


def _macd(daily: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, float | None]:
    raw = _mapping(_first(daily, "macd", "MACD"))
    if not raw:
        raw = _mapping(_first(context, "daily_macd", "macd", "MACD"))
    return {key: _round(_number(_first(raw, key, key.upper()))) for key in ("dif", "dea", "hist")}


def _volume_state(daily: Mapping[str, Any], context: Mapping[str, Any], kline: Mapping[str, Any]) -> str:
    raw = _first(daily, "volume_state", "volume_condition") or _first(kline, "volume_state", "volume_condition") or _first(context, "daily_volume_state")
    return _normalize_state(raw) or "UNKNOWN"


def _tradable_state(value: Mapping[str, Any]) -> bool | None:
    for key in ("tradable", "trading_eligible", "is_tradable", "can_trade"):
        if key in value:
            return bool(value[key]) if isinstance(value[key], (bool, int, float)) else _normalize_state(value[key]) in {"TRUE", "YES", "PASS", "TRADABLE", "ELIGIBLE"}
    for key in ("status", "state"):
        state = _normalize_state(value.get(key))
        if state in {"SUSPENDED", "HALTED", "NOT_TRADABLE", "BLOCKED", "INVALID"}:
            return False
        if state in {"TRADABLE", "ELIGIBLE", "PASS", "OK"}:
            return True
    return None


def _is_bear_state(value: str) -> bool:
    normalized = _normalize_state(value)
    return normalized in _BEAR_STATES or any(token in normalized for token in ("BEAR", "DISTRIBUTION", "RETREAT", "FADE"))


def _is_hard_bear_state(value: str) -> bool:
    """Return only a confirmed higher-cycle bear regime.

    ``BEAR_PARTIAL`` is deliberately excluded: it describes an incomplete MA
    stack and is handled as a conditional A4 probe when all daily/risk facts
    remain valid. Explicit risk-off/retreat states remain hard.
    """

    return _normalize_state(value) in {
        "BEAR",
        "BEARISH",
        "BEAR_STACK",
        "DOWN",
        "DOWNTREND",
        "RETREAT",
        "FADE",
        "RISK_OFF",
        "RISK_OFF_RETREAT",
    }


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "model_dump"):
        try:
            dumped = value.model_dump()
        except Exception:  # pragma: no cover - defensive compatibility boundary
            return {}
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    return {}


def _merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(base)
    result.update(overlay)
    return result


def _first(mapping: Mapping[str, Any] | None, *keys: str) -> Any:
    if not mapping:
        return None
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _normalize_state(value: Any) -> str:
    return _normalize_token(value).replace("-", "_").replace(" ", "_")


def _normalize_event(value: Any) -> str:
    return _normalize_state(value)


def _normalize_role(mapping: Mapping[str, Any]) -> str:
    raw = _first(mapping, "market_role", "leader_role", "role", "marketRole", "leaderRole")
    if raw is None:
        nested = _mapping(_first(mapping, "leader_structure", "role_breakdown", "identifiability"))
        raw = _first(nested, "market_role", "leader_role", "role", "marketRole", "leaderRole")
    return _normalize_state(raw)


def _normalize_token(value: Any) -> str:
    return _text(value).upper().replace("-", "_").replace(" ", "_")


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return math.isfinite(float(value)) and float(value) != 0
    return _normalize_state(value) in {"TRUE", "YES", "Y", "PASS", "OK", "AVAILABLE", "OBSERVED", "CONFIRMED"}


def _explicit_false(value: Any) -> bool:
    if value is False:
        return True
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return math.isfinite(float(value)) and float(value) == 0
    return _normalize_state(value) in {"FALSE", "NO", "N", "BROKEN", "FAIL", "UNBROKEN_FALSE"}


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _round(value: Any) -> float | None:
    number = _number(value)
    return round(number, 6) if number is not None else None


def _above(left: float | None, right: float | None) -> bool:
    return left is not None and right is not None and left > right


def _above_or_equal(left: float | None, right: float | None) -> bool:
    return left is not None and right is not None and left >= right


def _ordered_above(*values: float | None) -> bool:
    return all(value is not None for value in values) and all(left > right for left, right in zip(values, values[1:]))


def _slope_not_down(slopes: Mapping[str, float | None], names: Sequence[str]) -> bool:
    available = [_number(slopes.get(name)) for name in names]
    return bool(available) and all(value is not None and value >= 0 for value in available)


def _iso(value: datetime | date | str) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value).strip()))


def _append_unique(values: list[str], value: str) -> None:
    if value and value not in values:
        values.append(value)


__all__ = [
    "STRATEGY_VERSION",
    "StrategyProfile",
    "Eligibility",
    "A3StrategyDecision",
    "evaluate_a3_strategy",
    "evaluate_a3_candidate",
    "route_a3_strategy",
    "build_a3_strategy_decision",
]
