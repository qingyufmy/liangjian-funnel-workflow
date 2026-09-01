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
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


STRATEGY_VERSION = "a3-a4-three-strategy/1.3.0"


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


class StockBehaviorType(StrEnum):
    """The behavior regime that owns the A3-to-A4 route.

    ``EMOTION`` is a short-lived, theme/ladder driven behavior and can only
    use the leader intraday playbook.  ``TREND`` is the right-side daily
    behavior and can use the MA5 or MA520 playbooks.  ``UNRESOLVED`` is a
    deliberate fail-closed value: a candidate without enough evidence must
    not be turned into a next-day plan by a convenient default.
    """

    EMOTION = "EMOTION"
    TREND = "TREND"
    UNRESOLVED = "UNRESOLVED"


class RoutePermission(StrEnum):
    """Whether this A3 decision may be consumed by the A4 planner."""

    ALLOW_A4 = "ALLOW_A4"
    WATCH_ONLY = "WATCH_ONLY"
    BLOCKED = "BLOCKED"


class A3GateResult(BaseModel):
    """One auditable A3 gate outcome.

    ``available`` describes whether the input needed to evaluate the gate was
    present.  It is deliberately separate from ``met``: a known negative is
    not the same thing as a missing fact.  The four fields are kept small and
    stable so the result can be consumed by the UI, replay reports and SQL
    projections without exposing model prose.
    """

    model_config = ConfigDict(frozen=True)

    met: bool
    reason: str
    kind: str
    available: bool


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
    stock_behavior_type: StockBehaviorType = StockBehaviorType.UNRESOLVED
    route_permission: RoutePermission = RoutePermission.BLOCKED
    expected_holding_sessions: dict[str, int] | None = None
    time_stop_sessions: int | None = None
    setup_pattern: str | None = None
    cycle_alignment: dict[str, Any] = Field(default_factory=dict)
    emotion_cycle_stage: str | None = None
    market_environment: str | None = None
    market_regime: str | None = None
    market_funding_state: str | None = None
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
    gate_results: dict[str, dict[str, Any]] = Field(default_factory=dict)
    first_blocking_gate: str | None = None
    all_failed_gates: list[str] = Field(default_factory=list)
    publication_state: str | None = None
    A3_ABLATION_MODE: bool = False
    a3_ablation_mode: bool = False
    ablation_gates: list[str] = Field(default_factory=list)
    ablation_shadow_eligibility: str | None = None
    reason_codes: list[str] = Field(default_factory=list)
    llm_review: dict[str, Any] = Field(default_factory=dict)
    strategy_facts: dict[str, Any] = Field(default_factory=dict)


# Short descriptive alias for consumers that refer to the stage as
# ``A3Decision``.  Keeping the existing class name preserves the public import
# used by the current pipeline and replay fixtures.
A3Decision = A3StrategyDecision


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
_TREND_MAIN_RISE_STATES = {
    "MAIN_RISE",
    "UPTREND",
    "BULL_STACK",
    "STRONG_UPTREND",
    "TRENDING_UP",
}
_TREND_PULLBACK_LABELS = {
    "MA5_PULLBACK",
    "PULLBACK_HOLD_MA5",
    "STRONG_PULLBACK",
    "TREND_PULLBACK",
    "回踩MA5",
    "回踩5日线",
    "趋势回踩",
}

# These are descriptive setup labels, not scores.  A label can make a route
# easier to audit and lets A4 select the matching confirmation procedure; it
# never creates a plan on its own.
_PATTERN_ALIASES = {
    "W底": "W_BOTTOM",
    "双底": "W_BOTTOM",
    "W底形态": "W_BOTTOM",
    "W_BOTTOM_PATTERN": "W_BOTTOM",
    "DOUBLE_BOTTOM": "W_BOTTOM",
    "FLAG_PATTERN": "FLAG",
    "旗形": "FLAG",
    "旗形整理": "FLAG",
    "BOX_BREAKOUT_PATTERN": "BOX_BREAKOUT",
    "箱体突破": "BOX_BREAKOUT",
    "箱体突破形态": "BOX_BREAKOUT",
    "PLATFORM_BREAKOUT_PATTERN": "PLATFORM_BREAKOUT",
    "平台突破": "PLATFORM_BREAKOUT",
    "创新高": "NEW_HIGH",
    "INNOVATION_HIGH": "NEW_HIGH",
    "PRICE_DISCOVERY": "NEW_HIGH",
    "回踩5日线": "MA5_PULLBACK",
    "回踩MA5": "MA5_PULLBACK",
    "趋势回踩": "MA5_PULLBACK",
    "回踩20日线": "MA20_PULLBACK",
    "回踩MA20": "MA20_PULLBACK",
    "MA20回踩": "MA20_PULLBACK",
    "收复20日线": "MA20_RECLAIM",
    "MA20收复": "MA20_RECLAIM",
    "金叉": "MA520_GOLDEN_CROSS",
    "MA5上穿MA20": "MA520_GOLDEN_CROSS",
    "炸板": "FAILED_SEAL",
    "断板": "BROKEN_BOARD",
    "炸板断板": "BROKEN_BOARD",
    "高开低走": "HIGH_OPEN_LOW_CLOSE",
    "大阴包小阳": "LARGE_BEARISH_ENGULFING",
    "无人接力": "NO_RELAY",
    "天地板": "EARTH_SKY_BOARD",
    "放量长上影": "HIGH_VOLUME_UPPER_SHADOW",
    "箱体跌破": "BOX_BREAKDOWN",
    "跌破箱体": "BOX_BREAKDOWN",
    "头肩顶": "HEAD_SHOULDERS_TOP",
    "三重顶": "TRIPLE_TOP",
    "多重顶": "TRIPLE_TOP",
    "MACD顶背离": "MACD_TOP_DIVERGENCE",
    "顶背离": "MACD_TOP_DIVERGENCE",
    "跌破MA20": "MA20_BREAKDOWN",
    "跌破20日线": "MA20_BREAKDOWN",
    "跌破MA60": "MA60_BREAKDOWN",
    "跌破60日线": "MA60_BREAKDOWN",
    "死叉": "MA_DEATH_CROSS",
}
_TREND_PATTERN_PRIORITY = (
    "W_BOTTOM",
    "FLAG",
    "BOX_BREAKOUT",
    "PLATFORM_BREAKOUT",
    "NEW_HIGH",
    "MA5_PULLBACK",
    "MAIN_RISE",
    "TREND_PULLBACK",
)
_LEADER_PATTERN_LABELS = {
    "LEADER_REACCELERATION",
    "LADDER_CONTINUATION",
    "FIRST_RELAY",
    "SECOND_BOARD",
    "THIRD_BOARD_CONFIRMATION",
}
_EMOTION_TOP_LABELS = {
    "FAILED_SEAL",
    "BROKEN_BOARD",
    "HIGH_OPEN_LOW_CLOSE",
    "LARGE_BEARISH_ENGULFING",
    "NO_RELAY",
    "EARTH_SKY_BOARD",
    "HIGH_VOLUME_UPPER_SHADOW",
}
_TREND_TOP_LABELS = {
    "BOX_BREAKDOWN",
    "HEAD_SHOULDERS_TOP",
    "TRIPLE_TOP",
    "MACD_TOP_DIVERGENCE",
    "MA20_BREAKDOWN",
    "MA60_BREAKDOWN",
    "MA_DEATH_CROSS",
}
_ROUTE_HOLDING_DEFAULTS: dict[StrategyProfile, tuple[int, int, int]] = {
    StrategyProfile.LEADER_INTRADAY: (1, 3, 3),
    StrategyProfile.TREND_MA5: (3, 10, 7),
    StrategyProfile.MA520_SWING: (5, 20, 7),
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
    ablation: Mapping[str, Any] | None = None,
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
    market_emotion = _mapping(
        _first(merged_a2, "market_emotion", "market_emotion_snapshot")
        or _first(context, "market_emotion", "market_emotion_snapshot")
    )
    market_emotion_supplied = bool(market_emotion)
    emotion_cycle_stage = _normalize_state(
        _first(market_emotion, "emotion_cycle_stage", "cycle_stage", "stage")
    ) or "UNKNOWN"
    emotion_new_long_permission = _normalize_state(
        _first(market_emotion, "new_long_permission", "entry_permission")
    ) or "UNKNOWN"
    market_funding = _mapping(
        _first(merged_a2, "market_funding", "market_funding_snapshot")
        or _first(context, "market_funding", "market_funding_snapshot")
    )
    market_funding_state = _normalize_state(
        _first(market_funding, "state", "funding_state")
    ) or "UNRESOLVED"
    market_environment = (
        "BEAR_RISK"
        if market_regime in _RISK_OFF_STATES
        else "BULL_TREND"
        if market_regime == "TREND_MAINLINE"
        else "ROTATION_MIXED"
    )
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
    ma520_right_side = _ma520_right_side_paths(
        daily_ma=daily_ma,
        daily_close=daily_close,
        daily=daily,
        setup=setup,
        context=context,
    )
    price_discovery = _is_price_discovery(raw_candidate, merged_a2, kline, labels)
    distribution = _has_distribution(raw_candidate, merged_a2, kline, labels)
    overextended = _is_overextended(raw_candidate, merged_a2, daily, daily_ma, daily_close, context)
    locked = _is_locked(raw_candidate, merged_a2, kline, labels)
    behavior_risk = _behavior_risk_facts(
        a2_context,
        context,
        raw_kline=raw_kline,
        distribution=distribution,
        daily_event=daily_event,
    )

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
    trend_paths = _trend_path_signals(
        merged_a2,
        daily_ma=daily_ma,
        daily_close=daily_close,
        daily=daily,
        price_discovery=price_discovery,
        labels=labels,
        kline=kline,
        daily_state=daily_state,
    )
    ma520_setup_present = any(
        bool(setup.get(key, False))
        for key in ("dead_cross", "golden_cross", "pullback_hold", "reclaim")
    )
    # A clear main-rise/platform/price-discovery path belongs to TREND_MA5.
    # A MA5 retest that is also carrying a fresh MA520 setup remains MA520 so
    # a repair/crossover candidate is not silently relabeled as a mature
    # trend merely because its close touched MA5.  A standalone MA5 retest
    # still routes to TREND_MA5.
    trend_structure_confirmed = any(
        trend_paths.get(key, False)
        for key in ("daily_main_rise", "platform_breakout", "price_discovery")
    ) or (trend_paths.get("strong_pullback", False) and not ma520_route)
    # Keep an explicit MA520 setup in its own route when its right-side
    # confirmation is absent.  Otherwise a broad daily trend hint could
    # silently relabel a falling-knife MA20 touch as TREND_MA5 and bypass the
    # MA520 right-side gate.  A confirmed MA520 setup may still share a mature
    # trend route when the daily trend path is independently authoritative.
    ma520_route_preferred = ma520_route and (
        not trend_structure_confirmed
        or (ma520_setup_present and not ma520_right_side.get("confirmed", False))
    )
    if leader_route:
        profile = StrategyProfile.LEADER_INTRADAY
    elif ma520_route_preferred:
        profile = StrategyProfile.MA520_SWING
    elif trend_route:
        profile = StrategyProfile.TREND_MA5
    elif ma520_route:
        profile = StrategyProfile.MA520_SWING
    else:
        profile = StrategyProfile.NO_NEXT_DAY_PLAN

    # Classify the stock's behavior before any plan can be published.  The
    # route remains driven by the deterministic daily setup above, but an
    # explicit behavior declaration is authoritative for compatibility: an
    # emotion stock must use the leader route and a trend stock must use one
    # of the two right-side trend routes.  Historical snapshots often do not
    # carry this new field, so the helper makes a conservative inference from
    # the existing market role/route and records how it did so.
    routed_profile_before_behavior = profile
    stock_behavior_type, behavior_source, behavior_conflict = _resolve_behavior_type(
        raw_candidate,
        a2_context,
        merged_a2,
        context,
        market_role=market_role,
        routed_profile=profile,
    )
    behavior_route_compatible = _behavior_route_compatible(
        stock_behavior_type,
        profile,
    )
    if (
        not behavior_conflict
        and routed_profile_before_behavior is not StrategyProfile.NO_NEXT_DAY_PLAN
        and stock_behavior_type is not StockBehaviorType.UNRESOLVED
        and not behavior_route_compatible
    ):
        # An explicit style that asks for the wrong playbook is a known
        # contract conflict, not a benign data gap.
        behavior_conflict = True
    if behavior_conflict or not behavior_route_compatible:
        profile = StrategyProfile.NO_NEXT_DAY_PLAN

    required: list[str] = []
    met: list[str] = []
    unmet: list[str] = []
    vetoes: list[str] = []
    reason_codes: list[str] = []
    condition_details: dict[str, dict[str, Any]] = {}
    gate_order: list[str] = []
    gate_results: dict[str, dict[str, Any]] = {}
    data_gaps: list[str] = []
    watch_reasons: list[str] = []

    def record_gate(
        name: str,
        *,
        met: bool,
        reason: str,
        kind: str,
        available: bool,
    ) -> None:
        """Record every evaluated gate without changing decision lists.

        A3 historically exposed separate condition/veto arrays.  This
        projection is additive: duplicate condition/veto notifications are
        merged under their first-seen name, while a later veto takes
        precedence over a positive condition with the same name.  The
        evaluator continues executing all callers after a failed gate; this
        helper only records the result.
        """

        key = str(name)
        if key not in gate_results:
            gate_order.append(key)
            gate_results[key] = {
                "met": bool(met),
                "reason": str(reason or ("OK" if met else "NOT_MET")),
                "kind": str(kind),
                "available": bool(available),
            }
            return
        previous = gate_results[key]
        if not met or str(kind).upper() == "VETO":
            previous["met"] = bool(met)
            previous["reason"] = str(reason or ("OK" if met else "NOT_MET"))
        if str(kind).upper() == "VETO":
            previous["kind"] = str(kind)
            # A veto is a known negative, even if an earlier condition with
            # the same name was missing.
            previous["available"] = True
        else:
            previous["available"] = bool(previous.get("available", True) and available)

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
        record_gate(
            name,
            met=bool(ok),
            reason=detail["reason"],
            kind="CONDITION",
            available=not missing,
        )
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
        record_gate(code, met=False, reason=code, kind="VETO", available=True)

    if not symbol:
        data_gaps.append("SYMBOL_MISSING")
        record_gate(
            "SYMBOL_PRESENT",
            met=False,
            reason="SYMBOL_MISSING",
            kind="DATA",
            available=False,
        )

    record_gate(
        "STRATEGY_ROUTE_APPLICABLE",
        met=profile is not StrategyProfile.NO_NEXT_DAY_PLAN,
        reason=(
            "OK"
            if profile is not StrategyProfile.NO_NEXT_DAY_PLAN
            else "NO_APPLICABLE_STRATEGY"
        ),
        kind="ROUTE",
        available=True,
    )

    if behavior_conflict:
        _append_unique(vetoes, "BEHAVIOR_ROUTE_CONFLICT")
        _append_unique(reason_codes, "BEHAVIOR_ROUTE_CONFLICT")
        record_gate(
            "BEHAVIOR_ROUTE_COMPATIBLE",
            met=False,
            reason="BEHAVIOR_ROUTE_CONFLICT",
            kind="VETO",
            available=True,
        )
    elif stock_behavior_type is StockBehaviorType.UNRESOLVED:
        _append_unique(reason_codes, "STOCK_BEHAVIOR_UNRESOLVED")
        record_gate(
            "STOCK_BEHAVIOR_RESOLVED",
            met=False,
            reason="STOCK_BEHAVIOR_UNRESOLVED",
            kind="DATA",
            available=False,
        )
    else:
        record_gate(
            "BEHAVIOR_ROUTE_COMPATIBLE",
            met=behavior_route_compatible,
            reason="OK" if behavior_route_compatible else "BEHAVIOR_ROUTE_INCOMPATIBLE",
            kind="ROUTE",
            available=True,
        )

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
        tradable = _tradable_state(tradability)
        if tradable is False:
            veto("NOT_TRADABLE")
        condition(
            "TRADABLE",
            tradable is True,
            missing=tradable is None,
            reason=(
                "TRADABILITY_DATA_MISSING"
                if tradable is None
                else "NOT_TRADABLE" if tradable is False else None
            ),
        )
        condition(
            "DAILY_CLOSE_AVAILABLE",
            daily_close is not None,
            missing=True,
            reason="DAILY_CLOSE_MISSING" if daily_close is None else None,
        )
        higher_timeframe_bear = _is_bear_state(monthly_state) or _is_bear_state(weekly_state)
        # Monthly/weekly frames are background context for A3.  A monthly
        # decline is never enough to veto a valid daily setup; it is carried
        # as a PROBE risk and A4 must demand fresh intraday confirmation.  A
        # hard higher-cycle veto requires the formally closed *weekly* frame
        # and an independently weak daily frame.  This prevents an incomplete
        # or stale higher-timeframe stack from collapsing the whole funnel.
        higher_timeframe_hard_bear = (
            week_status == "CLOSED"
            and _is_hard_bear_state(weekly_state)
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
                emotion_cycle_stage=emotion_cycle_stage,
                emotion_new_long_permission=emotion_new_long_permission,
                market_emotion_supplied=market_emotion_supplied,
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
                trend_paths=trend_paths,
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
                right_side=ma520_right_side,
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
        if (
            stock_behavior_type is StockBehaviorType.EMOTION
            and behavior_risk["emotion_top"]["confirmed"] is True
        ):
            veto("EMOTION_TOP_RISK_CONFIRMED")
        if (
            stock_behavior_type is StockBehaviorType.TREND
            and behavior_risk["trend_top"]["confirmed"] is True
        ):
            veto("TREND_TOP_RISK_CONFIRMED")
        # A high-acceleration/overextended close is never an A4 plan merely
        # because the route is labelled LEADER or MA520.  It must show a
        # concrete daily MA5 retest that held (A4 still confirms the intraday
        # leg).  This keeps trend/new-high strength from turning into a
        # top-of-the-mountain entry while preserving a valid repair path.
        if (
            profile is not StrategyProfile.NO_NEXT_DAY_PLAN
            and overextended
            and not trend_paths.get("strong_pullback_geometry", False)
        ):
            veto("OVEREXTENDED_WITHOUT_RETEST")
            if profile is StrategyProfile.TREND_MA5:
                # Keep the historical, route-specific reason for existing
                # audit consumers while exposing the shared hard boundary.
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

    # Keep the attempted route in the audit artifact when a new behavior
    # contract blocks publication.  This makes a conflict diagnosable without
    # allowing the conflicting route to leak into A4.
    metadata_profile = (
        routed_profile_before_behavior
        if profile is StrategyProfile.NO_NEXT_DAY_PLAN
        and routed_profile_before_behavior is not StrategyProfile.NO_NEXT_DAY_PLAN
        else profile
    )
    expected_holding_sessions, time_stop_sessions, holding_source = _holding_contract(
        metadata_profile,
        raw_candidate,
        merged_a2,
        context,
    )
    setup_pattern = _setup_pattern(
        metadata_profile,
        labels=labels,
        setup=setup,
        trend_paths=trend_paths,
        daily_event=daily_event,
        ladder=ladder,
    )
    cycle_alignment = _cycle_alignment(
        metadata_profile,
        monthly_state=monthly_state,
        weekly_state=weekly_state,
        daily_state=daily_state,
        monthly_status=_period_status(monthly, require_explicit=True),
        weekly_status=_period_status(weekly, require_explicit=True),
        daily_status=_period_status(daily, require_explicit=False),
    )
    cycle_alignment["market_environment"] = market_environment
    cycle_alignment["emotion_cycle"] = {
        "stage": emotion_cycle_stage,
        "new_long_permission": emotion_new_long_permission,
        "owned_by": "LEADER_INTRADAY" if metadata_profile is StrategyProfile.LEADER_INTRADAY else "CONTEXT_ONLY",
    }
    cycle_alignment["market_funding"] = {
        "state": market_funding_state,
        "available": market_funding.get("available") is True,
        "amount_ratio": _number(market_funding.get("amount_ratio")),
        "coverage": _number(market_funding.get("coverage")),
        "turnover_is_capital_flow": False,
        "owned_by": "MARKET_CONTEXT_ONLY",
    }
    route_permission = _route_permission(
        eligibility,
        stock_behavior_type=stock_behavior_type,
        profile=profile,
        behavior_conflict=behavior_conflict,
    )
    publication_state = {
        RoutePermission.ALLOW_A4: "PUBLISHED_A4_PLAN",
        RoutePermission.WATCH_ONLY: "WATCH_ONLY",
        RoutePermission.BLOCKED: "BLOCKED",
    }[route_permission]

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
        ma520_right_side=ma520_right_side,
        trend_paths=trend_paths,
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

    failed_gates = [
        name for name in gate_order if not bool(gate_results[name].get("met"))
    ]

    result = {
        "strategy_profile": profile.value,
        "strategy_version": STRATEGY_VERSION,
        "symbol": symbol or None,
        "name": _text(_first(raw_candidate, "name", "company_name", "security_name")) or None,
        "candidate_origin": _text(_first(raw_candidate, "candidate_origin", "origin")) or "A2",
        "market_role": market_role or None,
        "stock_behavior_type": stock_behavior_type.value,
        "route_permission": route_permission.value,
        "expected_holding_sessions": expected_holding_sessions,
        "time_stop_sessions": time_stop_sessions,
        "setup_pattern": setup_pattern,
        "cycle_alignment": cycle_alignment,
        "emotion_cycle_stage": emotion_cycle_stage,
        "market_environment": market_environment,
        "behavior_risk": behavior_risk,
        "market_regime": market_regime,
        "market_funding_state": market_funding_state,
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
        "gate_results": {
            name: {
                "met": bool(detail.get("met")),
                "reason": str(detail.get("reason") or ("OK" if detail.get("met") else "NOT_MET")),
                "kind": str(detail.get("kind") or "CONDITION").upper(),
                "available": bool(detail.get("available", True)),
            }
            for name, detail in gate_results.items()
        },
        "first_blocking_gate": failed_gates[0] if failed_gates else None,
        "all_failed_gates": failed_gates,
        "reason_codes": _dedupe(reason_codes + data_gaps + watch_reasons),
        "llm_review": {"status": llm_status, "reason_codes": _dedupe(vetoes + data_gaps)},
        "strategy_facts": facts,
    }
    facts["stock_behavior_type"] = stock_behavior_type.value
    facts["behavior_type_source"] = behavior_source
    facts["behavior_type_conflict"] = bool(behavior_conflict)
    facts["routed_profile_before_behavior_gate"] = routed_profile_before_behavior.value
    facts["route_permission"] = route_permission.value
    facts["expected_holding_sessions"] = expected_holding_sessions
    facts["time_stop_sessions"] = time_stop_sessions
    facts["holding_contract_source"] = holding_source
    facts["setup_pattern"] = setup_pattern
    facts["cycle_alignment"] = cycle_alignment
    facts["emotion_cycle_stage"] = emotion_cycle_stage
    facts["emotion_new_long_permission"] = emotion_new_long_permission
    facts["market_environment"] = market_environment
    facts["behavior_risk"] = behavior_risk
    facts["publication_state"] = publication_state
    _apply_a3_ablation(
        result,
        gate_order=gate_order,
        ablation=_resolve_ablation(ablation, source_snapshot, source_context),
    )
    return result


def _resolve_ablation(
    explicit: Mapping[str, Any] | None,
    snapshot: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Resolve an opt-in offline ablation declaration at the API boundary."""

    if isinstance(explicit, Mapping):
        return explicit
    for source in (snapshot, context):
        nested = source.get("agent_3") if isinstance(source, Mapping) else None
        if isinstance(nested, Mapping) and isinstance(nested.get("ablation"), Mapping):
            return nested["ablation"]
        value = source.get("A3_ABLATION_CONFIG") if isinstance(source, Mapping) else None
        if isinstance(value, Mapping):
            return value
    return {}


def _apply_a3_ablation(
    result: dict[str, Any],
    *,
    gate_order: Sequence[str],
    ablation: Mapping[str, Any],
) -> None:
    """Apply an explicitly requested offline gate ablation and fail closed.

    Ablation is an experiment projection, never a publication mode.  Selected
    gates are labelled ``ABLATED`` and treated as met only for the simulated
    failure analysis.  The returned artifact is still marked with the exact
    ``A3_ABLATION_MODE`` flag and forced to ``DATA_GAP`` so callers cannot
    create a production plan from it.
    """

    enabled = _truthy(
        _first(ablation, "enabled", "enable", "A3_ABLATION_MODE")
    )
    if not enabled:
        return

    raw_gates = _first(
        ablation,
        "disabled_gates",
        "gates",
        "gate_names",
        "disable_gates",
    )
    if isinstance(raw_gates, str):
        selected = [raw_gates.strip()] if raw_gates.strip() else []
    elif isinstance(raw_gates, Sequence) and not isinstance(raw_gates, (str, bytes)):
        selected = [str(value).strip() for value in raw_gates if str(value).strip()]
    else:
        selected = []
    selected = list(dict.fromkeys(selected))

    gates = result.get("gate_results")
    if not isinstance(gates, dict):
        gates = {}
        result["gate_results"] = gates
    for name in selected:
        detail = gates.get(name)
        if not isinstance(detail, dict):
            detail = {
                "met": False,
                "reason": "ABLATED_UNKNOWN_GATE",
                "kind": "ABLATED",
                "available": False,
            }
            gates[name] = detail
        else:
            detail["met"] = True
            detail["reason"] = "ABLATED"
            detail["kind"] = "ABLATED"
        if name not in gate_order:
            gate_order = (*gate_order, name)

    # Keep the simulated gate projection ordered by the original evaluator
    # order.  Unknown configured names appear after known gates, making a bad
    # experiment declaration visible without affecting real decisions.
    ordered_names = list(gate_order)
    ordered_names.extend(name for name in gates if name not in ordered_names)
    result["gate_results"] = {name: gates[name] for name in ordered_names if name in gates}
    failed = [
        name for name, detail in result["gate_results"].items()
        if not bool(detail.get("met"))
    ]
    result["all_failed_gates"] = failed
    result["first_blocking_gate"] = failed[0] if failed else None

    has_data_failure = any(
        str(detail.get("kind") or "").upper() == "DATA"
        or detail.get("available") is False
        for name, detail in result["gate_results"].items()
        if name in failed
    )
    shadow_eligibility = (
        Eligibility.QUALIFIED.value
        if not failed
        else Eligibility.DATA_GAP.value
        if has_data_failure
        else Eligibility.WATCH.value
    )
    result["A3_ABLATION_MODE"] = True
    result["a3_ablation_mode"] = True
    result["ablation_gates"] = selected
    result["ablation_shadow_eligibility"] = shadow_eligibility
    result["eligibility"] = Eligibility.DATA_GAP.value
    result["publication_state"] = "BLOCKED"
    result["route_permission"] = RoutePermission.BLOCKED.value
    result["plan_mode"] = None
    result["reason_codes"] = _dedupe([
        *(result.get("reason_codes") or []),
        "A3_ABLATION_MODE",
    ])
    result["llm_review"] = {
        **(_mapping(result.get("llm_review"))),
        "status": "DATA_GAP",
        "reason_codes": _dedupe([
            *(_mapping(result.get("llm_review"))).get("reason_codes", []),
            "A3_ABLATION_MODE",
        ]),
    }
    facts = result.get("strategy_facts")
    if isinstance(facts, dict):
        facts["A3_ABLATION_MODE"] = True
        facts["a3_ablation_mode"] = True
        facts["ablation_gates"] = list(selected)
        facts["ablation_shadow_eligibility"] = shadow_eligibility
        facts["publication_state"] = "BLOCKED"
        facts["route_permission"] = RoutePermission.BLOCKED.value


def route_a3_strategy(
    candidate: Mapping[str, Any] | None,
    technical_context: Mapping[str, Any] | None = None,
    price_contract: Mapping[str, Any] | None = None,
    trading_eligibility: Mapping[str, Any] | None = None,
    kline_labels: Mapping[str, Any] | Sequence[Any] | None = None,
    *,
    snapshot: Mapping[str, Any] | None = None,
    as_of: datetime | date | str | None = None,
    ablation: Mapping[str, Any] | None = None,
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
        ablation=ablation,
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
    market_emotion: Mapping[str, Any] | None = None,
    market_funding: Mapping[str, Any] | None = None,
    ablation: Mapping[str, Any] | None = None,
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
    if (
        market_regime is not None
        or sector_permission is not None
        or market_emotion is not None
        or market_funding is not None
    ):
        context = dict(context)
        if market_regime is not None:
            context["market_regime"] = market_regime
        if sector_permission is not None:
            context["sector_permission"] = sector_permission
        if market_emotion is not None:
            context["market_emotion"] = dict(_mapping(market_emotion))
        if market_funding is not None:
            context["market_funding"] = dict(_mapping(market_funding))

    candidate_map = dict(_mapping(candidate))
    if a2_context is not None:
        candidate_map["a2_context"] = dict(_mapping(a2_context))
    result = evaluate_a3_candidate(
        candidate_map,
        context,
        _mapping(price_levels),
        _mapping(tradability),
        kline,
        ablation=ablation,
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
    ablation: Mapping[str, Any] | None = None,
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
        ablation=ablation,
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
    emotion_cycle_stage: str,
    emotion_new_long_permission: str,
    market_emotion_supplied: bool,
) -> None:
    condition("A2_LEADER_ROLE_CONFIRMED", market_role in _LEADER_ROLES, missing=not bool(market_role), reason="A2_LEADER_ROLE_MISSING" if not market_role else None)
    if theme_stage in _BAD_LEADER_STAGES or theme_stage in _RISK_OFF_STATES:
        veto("LEADER_THEME_RETREAT_OR_CLIMAX")
    if market_emotion_supplied and (
        emotion_cycle_stage in {"CLIMAX", "DIVERGENCE", "RETREAT", "ICE_POINT"}
        or emotion_new_long_permission == "NO_NEW_ENTRY"
    ):
        veto("MARKET_EMOTION_CYCLE_NO_NEW_LEADER")
    if market_emotion_supplied:
        condition(
            "MARKET_EMOTION_CYCLE_ALLOWS_NEW_LEADER",
            emotion_new_long_permission in {"ALLOW_CORE", "PROBE_ONLY"},
            missing=emotion_new_long_permission == "UNKNOWN",
            reason=(
                "MARKET_EMOTION_CYCLE_MISSING"
                if emotion_new_long_permission == "UNKNOWN"
                else "MARKET_EMOTION_CYCLE_NO_NEW_LEADER"
                if emotion_new_long_permission not in {"ALLOW_CORE", "PROBE_ONLY"}
                else None
            ),
        )
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
    trend_paths: Mapping[str, bool],
) -> None:
    ma5 = daily_ma.get("ma5")
    # The trend route is a choice among independent daily setups.  MA60,
    # complete MA5/10/20 stacking, relative strength and A2/platform evidence
    # remain useful observations, but requiring all of them here made a valid
    # daily main-rise or pullback disappear from the funnel.  A4 owns the
    # intraday confirmation, so A3 only needs one explicit daily path plus the
    # common data/risk gates surrounding this function.
    path_confirmed = any(
        trend_paths.get(key, False)
        for key in ("daily_main_rise", "platform_breakout", "strong_pullback", "price_discovery")
    )
    condition(
        "TREND_DAILY_PATH_CONFIRMED",
        path_confirmed,
        missing=ma5 is None or daily_close is None,
        reason="TREND_DAILY_PATH_MISSING" if not path_confirmed else None,
    )
    condition(
        "DAILY_MA5_AVAILABLE_FOR_A4",
        ma5 is not None,
        missing=True,
        reason="DAILY_MA5_MISSING" if ma5 is None else None,
    )

    # Keep the established hard risk boundary.  An overextended close can be
    # published only when the same daily bar proves a reasonable MA5 retest;
    # otherwise a strong trend/innovation-high flag must not put a plan on the
    # top of the mountain.  The global evaluator adds TREND_OVEREXTENDED as a
    # veto when this retest geometry is absent.
    retest_geometry = bool(trend_paths.get("strong_pullback_geometry", False))
    condition(
        "NOT_OVEREXTENDED_OR_RETEST_CONFIRMED",
        not overextended or retest_geometry,
        reason="TREND_OVEREXTENDED" if overextended and not retest_geometry else None,
    )
    condition("NOT_DISTRIBUTION", not distribution, reason="HIGH_VOLUME_DISTRIBUTION" if distribution else None)
    if price_discovery:
        condition("PRICE_DISCOVERY_TREND", True)
        # This is the explicit exception: absence of first resistance is not
        # a data gap when the daily close is making price discovery.
        condition("RESISTANCE_NOT_REQUIRED_FOR_NEW_HIGH", True)
    # A3 already has a bounded trigger zone, invalidation and no-chase line
    # from PRICE_GEOMETRY_VALID.  A missing historical resistance is therefore
    # an observation, not a second data gate.
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
    right_side: Mapping[str, bool],
    distribution: bool,
    labels: set[str],
) -> None:
    ma5 = daily_ma.get("ma5")
    ma20 = daily_ma.get("ma20")
    if setup["dead_cross"]:
        veto("MA520_DEAD_CROSS")
    condition("DAILY_MA5_MA20_AVAILABLE", ma5 is not None and ma20 is not None, missing=True, reason="MA520_VALUES_MISSING" if ma5 is None or ma20 is None else None)
    setup_confirmed = setup["golden_cross"] or setup["pullback_hold"] or setup["reclaim"]
    # A known dead cross is a real veto, not a missing-data condition.  Only
    # call an absent setup a data gap when the underlying MA/event evidence is
    # itself unavailable.
    setup_missing = not setup_confirmed and not daily_event and (ma5 is None or ma20 is None)
    condition("MA520_SETUP_CONFIRMED", setup_confirmed, missing=setup_missing, reason="MA520_SETUP_NOT_CONFIRMED" if not setup_confirmed else None)
    right_side_missing = bool(right_side.get("data_missing", False))
    condition(
        "MA520_RIGHT_SIDE_CONFIRMED",
        bool(right_side.get("confirmed", False)),
        missing=right_side_missing,
        reason=(
            "MA520_RIGHT_SIDE_DATA_MISSING"
            if right_side_missing
            else "MA520_RIGHT_SIDE_NOT_CONFIRMED"
            if not right_side.get("confirmed", False)
            else None
        ),
        watch=not right_side_missing,
    )
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


def _ma520_right_side_paths(
    *,
    daily_ma: Mapping[str, float | None],
    daily_close: float | None,
    daily: Mapping[str, Any],
    setup: Mapping[str, bool],
    context: Mapping[str, Any],
) -> dict[str, bool]:
    """Return deterministic right-side paths allowed for MA520.

    A MA20 touch or named reclaim is only a setup observation.  It becomes
    executable after the daily close and MA5 slope show a recovery.  A fresh
    MA5/MA20 golden cross is the one reversal path that does not need a slope
    value, provided price is above both averages.
    """

    ma5 = daily_ma.get("ma5")
    ma20 = daily_ma.get("ma20")
    slopes = _ma_slopes(daily, context)
    ma5_slope = _number(slopes.get("ma5"))
    close_above_ma5 = _above(daily_close, ma5)
    close_above_ma20 = _above(daily_close, ma20)
    ma5_at_or_above_ma20 = _above_or_equal(ma5, ma20)
    setup_golden = bool(setup.get("golden_cross", False))
    setup_pullback = bool(setup.get("pullback_hold", False))
    setup_reclaim = bool(setup.get("reclaim", False))

    second_wave_restart = (
        setup_pullback
        and ma5_at_or_above_ma20
        and close_above_ma5
        and ma5_slope is not None
        and ma5_slope >= 0
    )
    golden_cross_reversal = (
        setup_golden
        and _above(ma5, ma20)
        and close_above_ma5
        and close_above_ma20
    )
    reclaim_reversal = (
        setup_reclaim
        and close_above_ma5
        and close_above_ma20
        and ma5_slope is not None
        and ma5_slope > 0
    )
    trend_reversal_confirmed = golden_cross_reversal or reclaim_reversal
    confirmed = second_wave_restart or trend_reversal_confirmed
    setup_present = setup_golden or setup_pullback or setup_reclaim
    # Slope is not needed for the golden-cross path.  For pullback/reclaim
    # paths its absence prevents a false right-side confirmation and is a
    # data gap rather than an executable negative signal.
    data_missing = (
        ma5 is None
        or ma20 is None
        or daily_close is None
        or (setup_present and not setup_golden and ma5_slope is None)
    )
    return {
        "second_wave_restart": bool(second_wave_restart),
        "golden_cross_reversal": bool(golden_cross_reversal),
        "reclaim_reversal": bool(reclaim_reversal),
        "trend_reversal_confirmed": bool(trend_reversal_confirmed),
        "confirmed": bool(confirmed),
        "data_missing": bool(data_missing),
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
    paths = _trend_path_signals(
        candidate,
        daily_ma=daily_ma,
        daily_close=daily_close,
        daily=daily,
        price_discovery=price_discovery,
        labels=labels,
        kline=kline,
        daily_state=_text(_first(daily, "state", "trend_state", "technical_state", "ma_alignment")),
    )
    return explicit or role_signal or any(paths.values())


def _trend_path_signals(
    candidate: Mapping[str, Any],
    *,
    daily_ma: Mapping[str, float | None],
    daily_close: float | None,
    daily: Mapping[str, Any],
    price_discovery: bool,
    labels: set[str],
    kline: Mapping[str, Any],
    daily_state: str,
) -> dict[str, bool]:
    """Return independent daily paths that can route the trend playbook.

    A3 needs one defensible daily setup, not a simultaneous MA60/MA-stack/RS
    checklist.  The path flags intentionally contain no weights or aggregate
    score.  ``strong_pullback_geometry`` is kept separate because it is the
    only condition that can make an explicitly overextended close publishable.
    """

    ma5 = daily_ma.get("ma5")
    ma10 = daily_ma.get("ma10")
    daily_low = _frame_low(daily)
    slopes = _ma_slopes(daily, {})
    ma5_slope = _number(slopes.get("ma5"))
    state = _normalize_state(daily_state)
    explicit_main_rise = _truthy(
        _first(
            candidate,
            "main_rise",
            "trend_confirmed",
            "maintrend_confirmed",
            "trend_ma5",
        )
    )
    # A state label is only a route hint.  The authoritative daily price
    # still has to hold above MA5; otherwise a stacked set of moving averages
    # with a falling close would be mistaken for a main-rise setup.
    price_holds_ma5 = _above_or_equal(daily_close, ma5)
    main_rise = price_holds_ma5 and (
        explicit_main_rise
        or state in _TREND_MAIN_RISE_STATES
        or (
            (ma10 is None or _above_or_equal(ma5, ma10))
            and (ma5_slope is None or ma5_slope >= 0)
        )
    )

    platform_breakout = _platform_evidence(candidate, daily, kline, labels, price_discovery)
    # A named reversal/continuation pattern is evidence, not a left-side
    # permission.  Require the daily close to hold MA5 before it can support
    # the trend route; A4 still performs the intraday confirmation.
    pattern_labels = {"W_BOTTOM", "FLAG", "BOX_BREAKOUT"}
    if labels & pattern_labels and not price_holds_ma5:
        platform_breakout = False

    # A strong daily MA5 pullback requires actual price geometry.  Merely
    # carrying an A2 label is enough to identify the route only when a valid
    # MA5 test is observable; this keeps labels explanatory rather than a
    # substitute for a price series.
    ma5_tested_and_held = (
        ma5 is not None
        and daily_close is not None
        and daily_low is not None
        and daily_low <= ma5 <= daily_close
        and daily_close <= ma5 * 1.05
        and (ma5_slope is None or ma5_slope >= 0)
    )
    explicit_pullback = bool(labels & {_normalize_token(value) for value in _TREND_PULLBACK_LABELS})
    strong_pullback_geometry = ma5_tested_and_held
    strong_pullback = ma5_tested_and_held or explicit_pullback

    return {
        "daily_main_rise": bool(main_rise),
        "platform_breakout": bool(platform_breakout),
        "strong_pullback": bool(strong_pullback),
        "strong_pullback_geometry": bool(strong_pullback_geometry),
        "price_discovery": bool(price_discovery),
    }


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
    ma520_right_side: Mapping[str, bool],
    trend_paths: Mapping[str, bool],
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
        "ma520_right_side": dict(ma520_right_side),
        "trend_paths": dict(trend_paths),
        "kline_labels": sorted(labels),
        "condition_details": dict(condition_details),
        "decision_style": "EXPLICIT_CONDITIONS_NO_COMPOSITE_SCORE",
    }


def _resolve_behavior_type(
    candidate: Mapping[str, Any],
    a2_context: Mapping[str, Any],
    merged_a2: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    market_role: str,
    routed_profile: StrategyProfile,
) -> tuple[StockBehaviorType, str, bool]:
    """Resolve the behavior owner of a route without inventing evidence.

    New snapshots may explicitly carry a behavior type in A1/A2 output.  Old
    snapshots do not, so the fallback is intentionally narrow: only clearly
    emotional roles, clearly trend-oriented roles, or an already deterministic
    A3 route may fill the missing field.  ``LEADER`` alone is ambiguous and is
    therefore resolved by the actual ladder-driven route, not by its name.
    """

    source_rows = (
        ("candidate", candidate),
        ("a2", a2_context),
        ("technical", context),
    )
    explicit: list[tuple[str, StockBehaviorType]] = []
    for source_name, source in source_rows:
        raw = _behavior_value(source)
        if raw is None:
            continue
        explicit.append((source_name, raw))

    values = {value for _, value in explicit if value is not StockBehaviorType.UNRESOLVED}
    has_unresolved = any(value is StockBehaviorType.UNRESOLVED for _, value in explicit)
    conflict = len(values) > 1 or (bool(values) and has_unresolved)
    if conflict:
        return StockBehaviorType.UNRESOLVED, "+".join(name for name, _ in explicit), True
    if explicit:
        return explicit[0][1], explicit[0][0], False

    role = _normalize_state(market_role)
    if role in {"EMOTION_LEADER", "MARKET_LEADER", "THEME_LEADER"}:
        return StockBehaviorType.EMOTION, "market_role", False
    if role in {"TREND_CORE", "TREND_LEADER", "INSTITUTIONAL_CORE", "CORE_ARMY"}:
        return StockBehaviorType.TREND, "market_role", False
    if routed_profile is StrategyProfile.LEADER_INTRADAY:
        return StockBehaviorType.EMOTION, "a3_route", False
    if routed_profile in {StrategyProfile.TREND_MA5, StrategyProfile.MA520_SWING}:
        return StockBehaviorType.TREND, "a3_route", False
    return StockBehaviorType.UNRESOLVED, "unresolved", False


def _behavior_value(source: Mapping[str, Any]) -> StockBehaviorType | None:
    if not source:
        return None
    raw = _first(
        source,
        "stock_behavior_type",
        "behavior_type",
        "stock_behavior",
        "behavior",
        "trade_style",
        "trade_type",
        "stock_type",
        "style",
    )
    if isinstance(raw, Mapping):
        raw = _first(raw, "stock_behavior_type", "behavior_type", "type", "style", "value", "name")
    if raw is None:
        nested = _mapping(_first(source, "classification", "behavior_classification"))
        raw = _first(nested, "stock_behavior_type", "behavior_type", "type", "style", "value")
    if raw is None:
        return None
    token = _normalize_token(raw)
    if token in {
        "EMOTION",
        "EMOTIONAL",
        "SENTIMENT",
        "SENTIMENTAL",
        "SPECULATION",
        "SPECULATIVE",
        "SHORT_TERM_EMOTION",
        "情绪",
        "情绪型",
    } or "EMOTION" in token or "情绪" in token:
        return StockBehaviorType.EMOTION
    if token in {
        "TREND",
        "TRENDING",
        "TREND_FOLLOW",
        "TREND_FOLLOWING",
        "QUALITY_TREND",
        "INVESTMENT",
        "趋势",
        "趋势型",
    } or "TREND" in token or "趋势" in token:
        return StockBehaviorType.TREND
    if token in {
        "UNRESOLVED",
        "UNKNOWN",
        "UNCLASSIFIED",
        "UNDEFINED",
        "DATA_GAP",
        "未定",
        "未知",
    }:
        return StockBehaviorType.UNRESOLVED
    # A supplied but unrecognised classification is not silently converted to
    # a trend route.  It is a missing/invalid fact that remains visible.
    return StockBehaviorType.UNRESOLVED


def _behavior_route_compatible(
    behavior: StockBehaviorType,
    profile: StrategyProfile,
) -> bool:
    if behavior is StockBehaviorType.EMOTION:
        return profile is StrategyProfile.LEADER_INTRADAY
    if behavior is StockBehaviorType.TREND:
        return profile in {StrategyProfile.TREND_MA5, StrategyProfile.MA520_SWING}
    return False


def _route_permission(
    eligibility: Eligibility,
    *,
    stock_behavior_type: StockBehaviorType,
    profile: StrategyProfile,
    behavior_conflict: bool,
) -> RoutePermission:
    if (
        eligibility is Eligibility.QUALIFIED
        and not behavior_conflict
        and stock_behavior_type is not StockBehaviorType.UNRESOLVED
        and _behavior_route_compatible(stock_behavior_type, profile)
    ):
        return RoutePermission.ALLOW_A4
    if eligibility is Eligibility.WATCH and not behavior_conflict:
        return RoutePermission.WATCH_ONLY
    return RoutePermission.BLOCKED


def _behavior_risk_facts(
    a2_context: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    raw_kline: Any,
    distribution: bool,
    daily_event: str,
) -> dict[str, Any]:
    """Separate fast emotion-top facts from slow trend-top facts.

    Only frozen K-line labels, explicit ``*_confirmed`` facts and already
    deterministic MA/distribution observations can confirm a risk.  Free-form
    model prose is intentionally excluded: the model may veto through its
    normal cited review contract, but it cannot manufacture a server-owned
    structure label here.
    """

    kline_labels = _normalize_labels(raw_kline, {}, {})
    emotion_signals = sorted(kline_labels.intersection(_EMOTION_TOP_LABELS))
    trend_signals = sorted(kline_labels.intersection(_TREND_TOP_LABELS))
    sources: list[str] = []
    if kline_labels:
        sources.append("KLINE_PATTERNS")

    explicit_emotion = {
        "FAILED_SEAL": ("failed_seal_confirmed", "broken_board_confirmed"),
        "HIGH_OPEN_LOW_CLOSE": ("high_open_low_close_confirmed",),
        "LARGE_BEARISH_ENGULFING": ("bearish_engulfing_confirmed",),
        "NO_RELAY": ("no_relay_confirmed", "ladder_broken"),
        "EARTH_SKY_BOARD": ("earth_sky_board_confirmed",),
    }
    explicit_trend = {
        "BOX_BREAKDOWN": ("box_breakdown_confirmed",),
        "HEAD_SHOULDERS_TOP": ("head_shoulders_top_confirmed",),
        "TRIPLE_TOP": ("triple_top_confirmed",),
        "MACD_TOP_DIVERGENCE": ("macd_top_divergence_confirmed",),
    }
    for source_name, source in (
        ("A2_CONTEXT", a2_context),
        ("TECHNICAL_CONTEXT", context),
    ):
        for signal, keys in explicit_emotion.items():
            if any(_truthy(source.get(key)) for key in keys):
                _append_unique(emotion_signals, signal)
                _append_unique(sources, source_name)
        for signal, keys in explicit_trend.items():
            if any(_truthy(source.get(key)) for key in keys):
                _append_unique(trend_signals, signal)
                _append_unique(sources, source_name)

    if distribution:
        _append_unique(emotion_signals, "HIGH_VOLUME_DISTRIBUTION")
        _append_unique(trend_signals, "HIGH_VOLUME_DISTRIBUTION")
        _append_unique(sources, "DETERMINISTIC_DISTRIBUTION")
    event = _normalize_event(daily_event)
    if event in _DEAD_CROSS_EVENTS:
        _append_unique(trend_signals, "MA_DEATH_CROSS")
        _append_unique(sources, "DAILY_MA_EVENT")
    # A close below an average is not, by itself, a top pattern: it can also
    # describe a still-unconfirmed reversal candidate.  Existing route gates
    # own that daily geometry.  This classifier only records an explicit
    # breakdown/dead-cross event or a confirmed point-in-time pattern.

    return {
        "schema": "behavior-risk/1.0.0",
        "emotion_top": {
            "confirmed": bool(emotion_signals),
            "signals": emotion_signals,
            "response": "NO_NEW_ENTRY_OR_FAST_EXIT" if emotion_signals else "NONE",
        },
        "trend_top": {
            "confirmed": bool(trend_signals),
            "signals": trend_signals,
            "response": "BLOCK_OR_TREND_EXIT" if trend_signals else "NONE",
        },
        "source_refs": sources,
        "scoring_used": False,
        "complex_patterns_require_confirmed_point_in_time_fact": True,
    }


def _holding_contract(
    profile: StrategyProfile,
    candidate: Mapping[str, Any],
    merged_a2: Mapping[str, Any],
    context: Mapping[str, Any],
) -> tuple[dict[str, int] | None, int | None, str]:
    """Return a bounded holding range and explicit time stop for a route."""

    defaults = _ROUTE_HOLDING_DEFAULTS.get(profile)
    if defaults is None:
        return None, None, "no_route"
    minimum, maximum, default_time_stop = defaults
    range_value: Any = None
    stop_value: Any = None
    source = "default"
    # Apply low-priority context first and let the explicit candidate override
    # it.  ``merged_a2`` already contains candidate keys, so iterating it
    # after the candidate would misreport the provenance as ``a2``.
    for source_name, mapping in (("technical", context), ("a2", merged_a2), ("candidate", candidate)):
        value = _first(
            mapping,
            "expected_holding_sessions",
            "holding_sessions",
            "holding_period_sessions",
            "plan_horizon_sessions",
            "holding_period",
        )
        if value is not None:
            range_value = value
            source = source_name
        value = _first(
            mapping,
            "time_stop_sessions",
            "time_stop",
            "max_holding_sessions",
            "time_stop_days",
        )
        if value is not None:
            stop_value = value
            source = source_name

    parsed_range = _parse_session_range(range_value)
    if parsed_range is not None:
        minimum, maximum = parsed_range
    else:
        if range_value is not None:
            source = f"{source}:invalid_range_defaulted"
    parsed_stop = _parse_positive_sessions(stop_value)
    time_stop = parsed_stop if parsed_stop is not None else default_time_stop
    if stop_value is not None and parsed_stop is None:
        source = f"{source}:invalid_stop_defaulted"
    return {"min": minimum, "max": maximum}, time_stop, source


def _parse_session_range(value: Any) -> tuple[int, int] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        low = _first(value, "min", "minimum", "low", "from", "min_sessions")
        high = _first(value, "max", "maximum", "high", "to", "max_sessions")
        if low is None and high is None:
            nested = _first(value, "range", "sessions", "value")
            if nested is not value:
                return _parse_session_range(nested)
        low_value = _parse_positive_sessions(low)
        high_value = _parse_positive_sessions(high)
        if low_value is None and high_value is not None:
            low_value = high_value
        if high_value is None and low_value is not None:
            high_value = low_value
        if low_value is None or high_value is None or low_value > high_value:
            return None
        return low_value, high_value
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        values = list(value)
        if len(values) >= 2:
            low_value = _parse_positive_sessions(values[0])
            high_value = _parse_positive_sessions(values[1])
            if low_value is not None and high_value is not None and low_value <= high_value:
                return low_value, high_value
        if len(values) == 1:
            parsed = _parse_positive_sessions(values[0])
            return (parsed, parsed) if parsed is not None else None
        return None
    if isinstance(value, str):
        numbers = re.findall(r"\d+", value)
        if len(numbers) >= 2:
            low_value, high_value = int(numbers[0]), int(numbers[1])
            if low_value > 0 and low_value <= high_value:
                return low_value, high_value
        if len(numbers) == 1:
            parsed = int(numbers[0])
            return (parsed, parsed) if parsed > 0 else None
        return None
    parsed = _parse_positive_sessions(value)
    return (parsed, parsed) if parsed is not None else None


def _parse_positive_sessions(value: Any) -> int | None:
    number = _number(value)
    if number is None or number <= 0 or not number.is_integer():
        return None
    return int(number)


def _setup_pattern(
    profile: StrategyProfile,
    *,
    labels: set[str],
    setup: Mapping[str, bool],
    trend_paths: Mapping[str, bool],
    daily_event: str,
    ladder: Mapping[str, Any],
) -> str | None:
    canonical = {_canonical_pattern(label) for label in labels if _text(label)}
    if profile is StrategyProfile.LEADER_INTRADAY:
        for pattern in ("LEADER_REACCELERATION", "LADDER_CONTINUATION", "THIRD_BOARD_CONFIRMATION", "SECOND_BOARD"):
            if pattern in canonical:
                return pattern
        height = _number(ladder.get("height"))
        if height is not None and height >= 2:
            return "LADDER_CONTINUATION"
        return "LEADER_INTRADAY_SETUP"
    if profile is StrategyProfile.TREND_MA5:
        for pattern in _TREND_PATTERN_PRIORITY:
            if pattern in canonical:
                return pattern
        if trend_paths.get("daily_main_rise"):
            return "MAIN_RISE"
        if trend_paths.get("strong_pullback"):
            return "MA5_PULLBACK"
        if trend_paths.get("price_discovery"):
            return "NEW_HIGH"
        return "TREND_DAILY_SETUP"
    if profile is StrategyProfile.MA520_SWING:
        if setup.get("golden_cross") or _normalize_event(daily_event) in _GOLDEN_CROSS_EVENTS:
            return "MA520_GOLDEN_CROSS"
        if setup.get("reclaim"):
            return "MA20_RECLAIM"
        if setup.get("pullback_hold"):
            return "MA20_PULLBACK"
        for pattern in ("W_BOTTOM", "FLAG", "BOX_BREAKOUT"):
            if pattern in canonical:
                return pattern
        return "MA520_SETUP"
    for pattern in _TREND_PATTERN_PRIORITY:
        if pattern in canonical:
            return pattern
    return None


def _cycle_alignment(
    profile: StrategyProfile,
    *,
    monthly_state: str,
    weekly_state: str,
    daily_state: str,
    monthly_status: str,
    weekly_status: str,
    daily_status: str,
) -> dict[str, Any]:
    matrix = {
        StrategyProfile.LEADER_INTRADAY: {
            "name": "EMOTION_DAILY_WITH_MONTH_WEEK_CONTEXT",
            "monthly_requirement": "CONTEXT_ONLY",
            "weekly_requirement": "CONTEXT_ONLY",
            "daily_requirement": "DAILY_NOT_BEARISH",
        },
        StrategyProfile.TREND_MA5: {
            "name": "TREND_DAILY_MA5_WITH_CYCLE_CONTEXT",
            "monthly_requirement": "CONTEXT_ONLY",
            "weekly_requirement": "PREFER_NOT_HARD_BEAR",
            "daily_requirement": "DAILY_RIGHT_SIDE_MA5",
        },
        StrategyProfile.MA520_SWING: {
            "name": "MA520_DAILY_REVERSAL_WITH_CYCLE_CONTEXT",
            "monthly_requirement": "CONTEXT_ONLY",
            "weekly_requirement": "PREFER_NOT_HARD_BEAR",
            "daily_requirement": "MA520_RIGHT_SIDE",
        },
    }.get(
        profile,
        {
            "name": "NO_ROUTE",
            "monthly_requirement": "N_A",
            "weekly_requirement": "N_A",
            "daily_requirement": "N_A",
        },
    )
    hard_weekly_bear_daily_bear = (
        weekly_status == "CLOSED"
        and _is_hard_bear_state(weekly_state)
        and _is_bear_state(daily_state)
    )
    return {
        "matrix": matrix["name"],
        "monthly": {
            "state": monthly_state,
            "status": monthly_status,
            "requirement": matrix["monthly_requirement"],
        },
        "weekly": {
            "state": weekly_state,
            "status": weekly_status,
            "requirement": matrix["weekly_requirement"],
        },
        "daily": {
            "state": daily_state,
            "status": daily_status,
            "requirement": matrix["daily_requirement"],
        },
        "all_three_bullish_required": False,
        "hard_weekly_bear_daily_bear_block": hard_weekly_bear_daily_bear,
        "intraday_timeframes_owned_by": "A4_15M_5M",
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
        for key in ("labels", "tags", "kline_labels", "pattern_labels", "setup_pattern", "pattern", "formation"):
            raw = value.get(key)
            if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
                rows.extend(raw)
            elif raw is not None:
                rows.append(raw)
        for key in (
            "new_high",
            "innovation_high",
            "price_discovery",
            "distribution",
            "overextended",
            "locked_limit_up",
            "w_bottom",
            "flag",
            "box_breakout",
        ):
            if value.get(key) is True:
                rows.append(key)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        rows.extend(value)
    for source in (candidate, context):
        for key in ("labels", "tags", "kline_labels", "pattern_labels", "setup_pattern", "pattern", "formation"):
            raw = source.get(key)
            if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
                rows.extend(raw)
            elif raw is not None:
                rows.append(raw)
    return {_canonical_pattern(item) for item in rows if _text(item)}


def _canonical_pattern(value: Any) -> str:
    token = _normalize_token(value)
    return _PATTERN_ALIASES.get(token, _PATTERN_ALIASES.get(_text(value), token))


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
    if labels & {
        "PLATFORM_BREAKOUT",
        "BREAKOUT",
        "BOX_BREAKOUT",
        "FLAG",
        "W_BOTTOM",
        "MAIN_RISE",
        "UPTREND",
        "主升",
        "平台突破",
    }:
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
        raw_value = _first(raw, f"ma{period}", f"MA{period}", str(period))
        if raw_value is None:
            raw_value = _first(daily, f"ma{period}_slope", f"slope_ma{period}")
        value = _number(raw_value)
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
    "StockBehaviorType",
    "RoutePermission",
    "A3GateResult",
    "A3Decision",
    "A3StrategyDecision",
    "evaluate_a3_strategy",
    "evaluate_a3_candidate",
    "route_a3_strategy",
    "build_a3_strategy_decision",
]
