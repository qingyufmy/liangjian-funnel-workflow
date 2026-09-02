from __future__ import annotations

from copy import deepcopy

from liangjian_funnel.pipeline.a3_strategy import (
    A3StrategyDecision,
    Eligibility,
    StrategyProfile,
    evaluate_a3_strategy,
    route_a3_strategy,
)


def _factor(
    *,
    close: float = 10.5,
    ma5: float = 10.1,
    ma10: float = 9.8,
    ma20: float = 9.4,
    ma60: float = 8.9,
    ma_event: str | None = None,
    daily_state: str = "BULL",
    slopes: dict[str, float] | None = None,
    low: float = 10.0,
    month_closed: bool = True,
    week_closed: bool = True,
) -> dict:
    return {
        "timeframes": {
            "monthly": {"closed": month_closed, "state": "BULL"},
            "weekly": {"closed": week_closed, "state": "BULL"},
            "daily": {
                "closed": True,
                "state": daily_state,
                "close": close,
                "low": low,
                "moving_averages": {"ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60},
                "ma_slopes": slopes if slopes is not None else {"ma5": 0.10, "ma10": 0.08, "ma20": 0.05},
                **({"ma_event": ma_event} if ma_event else {}),
            },
        }
    }


def _prices(*, resistance: float | None = 12.0) -> dict:
    result = {
        "trigger_zone": {"low": 10.0, "high": 10.2},
        "invalidation": 9.6,
        "max_chase_price": 10.6,
    }
    if resistance is not None:
        result["first_resistance"] = resistance
    return result


def _common(candidate: dict, *, factor: dict | None = None, prices: dict | None = None, kline: dict | None = None, a2: dict | None = None):
    return evaluate_a3_strategy(
        candidate,
        factor=factor or _factor(),
        price_levels=prices or _prices(),
        tradability={"tradable": True},
        kline=kline or {"labels": ["PLATFORM_BREAKOUT"]},
        a2_context=a2 or {},
    )


def test_each_strategy_has_a_single_qualified_route() -> None:
    leader = _common(
        {"symbol": "600001.SH", "name": "龙头", "market_role": "EMOTION_LEADER", "theme_stage": "CONFIRMATION", "ladder_height": 2, "ladder_intact": True},
        a2={"market_role": "EMOTION_LEADER", "theme_stage": "CONFIRMATION", "ladder_height": 2, "ladder_intact": True},
    )
    trend = _common(
        {"symbol": "600002.SH", "name": "趋势", "market_role": "TREND_CORE"},
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
    )
    ma520 = _common(
        {"symbol": "600003.SH", "name": "520", "market_role": "FOLLOWER"},
        factor=_factor(ma5=10.2, ma10=10.3, ma20=10.0, ma60=9.5, close=10.25, low=9.95, ma_event="PULLBACK_HOLD_MA20"),
        kline={"labels": []},
    )

    assert leader.strategy_profile is StrategyProfile.LEADER_INTRADAY
    assert trend.strategy_profile is StrategyProfile.TREND_MA5
    assert ma520.strategy_profile is StrategyProfile.MA520_SWING
    assert all(item.eligibility is Eligibility.QUALIFIED for item in (leader, trend, ma520))
    assert leader.strategy_profile != trend.strategy_profile != ma520.strategy_profile
    assert not any(key in trend.model_dump() for key in ("score", "weight", "composite_score"))
    assert ma520.strategy_facts["ma520_right_side"]["second_wave_restart"] is True
    for decision in (leader, trend, ma520):
        assert decision.strategy_profile is not StrategyProfile.NO_NEXT_DAY_PLAN
        assert decision.entry_reference_zone is not None
        assert decision.no_chase_price is not None
        assert decision.daily_invalidation is not None
        assert decision.a4_required_entry_rules


def test_plan_priority_is_ordinal_and_does_not_change_eligibility() -> None:
    p1 = _common(
        {"symbol": "600101.SH", "market_role": "TREND_CORE"},
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 85}},
        kline={"labels": ["PLATFORM_BREAKOUT"]},
    )
    p2 = _common(
        {"symbol": "600102.SH", "market_role": "FOLLOWER"},
        factor=_factor(
            ma5=10.2,
            ma10=10.3,
            ma20=10.0,
            ma60=9.5,
            close=10.25,
            low=9.95,
            ma_event="PULLBACK_HOLD_MA20",
        ),
        kline={"labels": []},
    )
    p3 = _common(
        {
            "symbol": "600103.SH",
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 3,
            "ladder_intact": True,
        },
        a2={
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 3,
            "ladder_intact": True,
        },
    )

    assert [p1.plan_priority, p2.plan_priority, p3.plan_priority] == ["P1", "P2", "P3"]
    assert all(item.eligibility is Eligibility.QUALIFIED for item in (p1, p2, p3))
    assert all(item.priority_reasons for item in (p1, p2, p3))


def test_plan_price_advice_uses_only_auditable_levels() -> None:
    factor = _factor()
    factor["as_of"] = "2026-09-01T15:00:00+08:00"
    resistance = _common(
        {"symbol": "600104.SH", "market_role": "TREND_CORE"},
        factor=factor,
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 85}},
    )
    discovery = _common(
        {"symbol": "600105.SH", "market_role": "TREND_CORE", "price_discovery": True},
        prices=_prices(resistance=None),
        kline={"labels": ["NEW_HIGH"]},
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 90}},
    )
    no_pressure = _common(
        {"symbol": "600106.SH", "market_role": "TREND_CORE"},
        prices=_prices(resistance=None),
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 85}},
    )

    assert resistance.reference_price == 10.5
    assert resistance.reference_price_as_of == "2026-09-01T15:00:00+08:00"
    assert resistance.pressure_reduce_price == 12.0
    assert resistance.pressure_basis == "FIRST_RESISTANCE"
    assert discovery.pressure_reduce_price is not None
    assert discovery.pressure_basis == "R2_OBSERVATION"
    assert no_pressure.pressure_reduce_price is None
    assert no_pressure.pressure_basis is None


def test_non_executable_candidate_has_no_priority() -> None:
    result = evaluate_a3_strategy(
        {"symbol": "600107.SH", "market_role": "TREND_CORE"},
        factor=_factor(),
        price_levels=_prices(),
        tradability={"tradable": False},
        kline={"labels": ["PLATFORM_BREAKOUT"]},
        a2_context={"market_role": "TREND_CORE", "relative_strength": {"percentile": 85}},
    )
    assert result.eligibility is Eligibility.REJECTED
    assert result.plan_priority is None
    assert result.priority_reasons == []


def test_leader_has_priority_over_trend_and_520() -> None:
    result = _common(
        {"symbol": "600004.SH", "market_role": "LEADER", "theme_stage": "IGNITION", "ladder_height": 2, "ladder_intact": True},
        a2={"market_role": "LEADER", "theme_stage": "IGNITION", "ladder_height": 2, "ladder_intact": True},
    )
    assert result.strategy_profile == StrategyProfile.LEADER_INTRADAY
    assert result.strategy_profile != StrategyProfile.TREND_MA5
    assert result.strategy_profile != StrategyProfile.MA520_SWING


def test_market_emotion_cycle_blocks_new_leader_at_climax_but_not_trend_route() -> None:
    leader = evaluate_a3_strategy(
        {
            "symbol": "600110.SH",
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 3,
            "ladder_intact": True,
        },
        factor=_factor(),
        price_levels=_prices(),
        tradability={"tradable": True},
        kline={"labels": []},
        a2_context={
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 3,
            "ladder_intact": True,
        },
        market_emotion={
            "available": True,
            "emotion_cycle_stage": "CLIMAX",
            "new_long_permission": "NO_NEW_ENTRY",
        },
    )
    trend = evaluate_a3_strategy(
        {"symbol": "600111.SH", "market_role": "TREND_CORE"},
        factor=_factor(),
        price_levels=_prices(),
        tradability={"tradable": True},
        kline={"labels": ["PLATFORM_BREAKOUT"]},
        a2_context={"market_role": "TREND_CORE"},
        market_emotion={
            "available": True,
            "emotion_cycle_stage": "CLIMAX",
            "new_long_permission": "NO_NEW_ENTRY",
        },
    )

    assert leader.eligibility is Eligibility.REJECTED
    assert leader.route_permission.value == "BLOCKED"
    assert "MARKET_EMOTION_CYCLE_NO_NEW_LEADER" in leader.reason_codes
    assert trend.eligibility is Eligibility.QUALIFIED
    assert trend.route_permission.value == "ALLOW_A4"
    assert trend.cycle_alignment["emotion_cycle"]["owned_by"] == "CONTEXT_ONLY"


def test_market_funding_is_traceable_context_not_an_a3_gate() -> None:
    result = evaluate_a3_strategy(
        {"symbol": "600112.SH", "market_role": "TREND_CORE"},
        factor=_factor(),
        price_levels=_prices(),
        tradability={"tradable": True},
        kline={"labels": ["PLATFORM_BREAKOUT"]},
        a2_context={"market_role": "TREND_CORE", "relative_strength": {"percentile": 82}},
        market_funding={
            "available": True,
            "state": "EXISTING_FUNDS_ROTATION",
            "amount_ratio": 1.01,
            "coverage": 0.96,
        },
    )

    assert result.eligibility is Eligibility.QUALIFIED
    assert result.market_funding_state == "EXISTING_FUNDS_ROTATION"
    assert result.cycle_alignment["market_funding"] == {
        "state": "EXISTING_FUNDS_ROTATION",
        "available": True,
        "amount_ratio": 1.01,
        "coverage": 0.96,
        "turnover_is_capital_flow": False,
        "owned_by": "MARKET_CONTEXT_ONLY",
    }


def test_market_risk_off_is_a3_context_and_a4_route_stays_available() -> None:
    qualified = evaluate_a3_strategy(
        {"symbol": "600113.SH", "market_role": "TREND_CORE"},
        factor=_factor(),
        price_levels=_prices(),
        tradability={"tradable": True},
        kline={"labels": ["PLATFORM_BREAKOUT"]},
        a2_context={"market_role": "TREND_CORE", "relative_strength": {"percentile": 82}},
        market_regime="RISK_OFF",
    )

    assert qualified.eligibility is Eligibility.QUALIFIED
    assert qualified.route_permission.value == "ALLOW_A4"
    assert qualified.market_environment == "BEAR_RISK"
    assert qualified.gate_results["MARKET_RISK_CLASSIFIED"] == {
        "met": True,
        "reason": "MARKET_RISK_OFF_CONTEXT_ONLY",
        "kind": "CONDITION",
        "available": True,
    }
    assert "MARKET_RISK_OFF" not in qualified.gate_results
    assert "MARKET_RISK_OFF" in qualified.reason_codes
    assert "MARKET_RISK_OFF" not in qualified.required_conditions
    assert "MARKET_RISK_OFF" not in qualified.unmet_conditions
    assert "MARKET_RISK_OFF" not in qualified.veto_conditions
    assert "MARKET_RISK_OFF" not in qualified.all_failed_gates
    assert qualified.strategy_facts["market_risk_context"] == {
        "regime": "RISK_OFF",
        "risk_off": True,
        "a3_gate": "OBSERVATION_ONLY",
        "a4_entry_gate": "HARD_BLOCK_NEW_ENTRY_IN_RISK_OFF",
    }

    technical_failure = evaluate_a3_strategy(
        {"symbol": "600114.SH", "market_role": "TREND_CORE"},
        factor=_factor(daily_state="BEAR"),
        price_levels=_prices(),
        tradability={"tradable": True},
        kline={"labels": ["PLATFORM_BREAKOUT"]},
        a2_context={"market_role": "TREND_CORE", "relative_strength": {"percentile": 82}},
        market_regime="RISK_OFF",
    )

    assert technical_failure.eligibility is Eligibility.REJECTED
    assert technical_failure.route_permission.value == "BLOCKED"
    assert "DAILY_TREND_WEAK" in technical_failure.veto_conditions
    assert "MARKET_RISK_OFF" not in technical_failure.veto_conditions
    assert "MARKET_RISK_OFF" not in technical_failure.all_failed_gates


def test_price_discovery_trend_does_not_require_first_resistance() -> None:
    result = _common(
        {"symbol": "600005.SH", "market_role": "TREND_CORE", "innovation_high": True},
        factor=_factor(close=10.8, ma5=10.2, ma10=9.9, ma20=9.5, ma60=8.8),
        prices=_prices(resistance=None),
        kline={"labels": ["NEW_HIGH"]},
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 90}},
    )
    assert result.strategy_profile is StrategyProfile.TREND_MA5
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.strategy_facts["price_discovery"] is True
    assert result.strategy_facts["observation_targets"]["target_basis"] == "R_MULTIPLE_NO_RESISTANCE_REQUIRED"


def test_partial_month_or_week_is_context_only_and_cannot_qualify() -> None:
    factor = _factor(month_closed=False)
    result = _common(
        {"symbol": "600006.SH", "market_role": "TREND_CORE"},
        factor=factor,
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
    )
    assert result.eligibility is Eligibility.DATA_GAP
    assert result.monthly_state == "MTD_OBSERVATION"
    assert "MONTH_NOT_CLOSED" in result.reason_codes
    assert "MONTH_CLOSED" in result.unmet_conditions


def test_formal_closed_period_remains_valid_while_current_period_is_observation_only() -> None:
    factor = _factor()
    factor["timeframes"]["monthly"]["latest"] = {"closed": True, "close": 10.0}
    factor["timeframes"]["monthly"]["latest_partial"] = {"closed": True, "close": 10.8}
    factor["timeframes"]["weekly"]["latest"] = {"closed": True, "close": 10.1}
    factor["timeframes"]["weekly"]["partial_bars"] = [{"closed": True, "close": 10.7}]
    result = _common(
        {"symbol": "600016.SH", "market_role": "TREND_CORE"},
        factor=factor,
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
    )
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.monthly_state == "BULL"
    assert result.monthly_partial_observation["observation_only"] is True
    assert result.weekly_partial_observation["observation_only"] is True


def test_trend_main_rise_does_not_require_ma60_stack_or_relative_strength() -> None:
    factor = _factor(
        close=10.5,
        ma5=10.1,
        ma10=9.9,
        ma20=10.3,
        ma60=11.5,
        low=10.4,
        slopes={"ma5": 0.03, "ma10": -0.01, "ma20": -0.02},
    )
    factor["timeframes"]["monthly"]["state"] = "BEAR_STACK"
    factor["timeframes"]["weekly"]["state"] = "ENTANGLED"
    result = _common(
        {"symbol": "600022.SH", "market_role": "TREND_CORE"},
        factor=factor,
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.TREND_MA5
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.plan_mode == "PROBE"
    assert result.strategy_facts["trend_paths"]["daily_main_rise"] is True
    assert "CLOSE_NOT_ABOVE_MA60" not in result.reason_codes
    assert "DAILY_MA_STACK_NOT_BULL" not in result.reason_codes
    assert "RELATIVE_STRENGTH_MISSING" not in result.reason_codes
    assert result.entry_reference_zone is not None
    assert result.no_chase_price is not None
    assert result.daily_invalidation is not None
    assert result.a4_required_entry_rules


def test_trend_platform_breakout_can_qualify_without_full_bull_stack() -> None:
    factor = _factor(
        close=10.3,
        ma5=10.0,
        ma10=10.5,
        ma20=10.4,
        ma60=12.0,
        low=10.25,
        slopes={"ma5": 0.02, "ma10": -0.01, "ma20": -0.02},
    )
    result = _common(
        {"symbol": "600023.SH", "market_role": "FOLLOWER"},
        factor=factor,
        kline={"labels": ["PLATFORM_BREAKOUT"]},
    )

    assert result.strategy_profile is StrategyProfile.TREND_MA5
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.strategy_facts["trend_paths"]["platform_breakout"] is True


def test_trend_strong_ma5_pullback_is_a_single_daily_path() -> None:
    factor = _factor(
        close=10.1,
        ma5=10.0,
        ma10=10.5,
        ma20=10.4,
        ma60=12.0,
        low=9.95,
        slopes={"ma5": 0.02, "ma10": 0.0, "ma20": -0.02},
    )
    result = _common(
        {"symbol": "600024.SH", "market_role": "FOLLOWER", "trend_candidate": True},
        factor=factor,
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.TREND_MA5
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.strategy_facts["trend_paths"]["strong_pullback"] is True
    assert result.strategy_facts["trend_paths"]["strong_pullback_geometry"] is True


def test_bull_stack_label_below_ma5_does_not_create_main_rise_plan() -> None:
    factor = _factor(
        close=9.5,
        ma5=10.0,
        ma10=9.2,
        ma20=8.8,
        ma60=8.0,
        low=9.2,
        daily_state="BULL_STACK",
    )
    result = _common(
        {"symbol": "600030.SH", "market_role": "TREND_CORE"},
        factor=factor,
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.TREND_MA5
    assert result.eligibility is Eligibility.WATCH
    assert result.strategy_facts["trend_paths"]["daily_main_rise"] is False
    assert "TREND_DAILY_PATH_CONFIRMED" in result.unmet_conditions


def test_ma520_accepts_golden_cross_without_ma20_slope_gate() -> None:
    factor = _factor(
        close=9.9,
        ma5=9.8,
        ma10=10.0,
        ma20=9.6,
        ma60=9.0,
        low=9.85,
        ma_event="GOLDEN_CROSS_SHORT",
        slopes={"ma5": 0.02, "ma10": -0.01, "ma20": -0.05},
    )
    result = _common(
        {"symbol": "600025.SH", "market_role": "FOLLOWER"},
        factor=factor,
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.MA520_SWING
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.strategy_facts["ma520_setup"]["golden_cross"] is True
    assert result.strategy_facts["ma520_right_side"]["golden_cross_reversal"] is True
    assert result.strategy_facts["ma520_right_side"]["trend_reversal_confirmed"] is True


def test_ma520_accepts_ma20_reclaim_as_probe_even_when_ma5_lags() -> None:
    factor = _factor(
        close=10.1,
        ma5=9.8,
        ma10=10.0,
        ma20=10.0,
        ma60=9.0,
        low=9.9,
        ma_event="RECLAIM_MA20",
        slopes={"ma5": 0.01, "ma10": -0.01, "ma20": -0.05},
    )
    factor["timeframes"]["daily"]["previous_close"] = 9.8
    result = _common(
        {"symbol": "600026.SH", "market_role": "FOLLOWER"},
        factor=factor,
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.MA520_SWING
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.plan_mode == "PROBE"
    assert result.strategy_facts["ma520_setup"]["reclaim"] is True
    assert result.strategy_facts["ma520_right_side"]["reclaim_reversal"] is True
    assert result.strategy_facts["ma520_right_side"]["trend_reversal_confirmed"] is True


def test_ma520_second_wave_requires_close_above_ma5_and_nonnegative_slope() -> None:
    result = _common(
        {"symbol": "600031.SH", "market_role": "FOLLOWER"},
        factor=_factor(
            close=10.25,
            ma5=10.2,
            ma10=10.3,
            ma20=10.0,
            ma60=9.5,
            low=9.95,
            ma_event="PULLBACK_HOLD_MA20",
            slopes={"ma5": 0.0, "ma10": -0.01, "ma20": -0.02},
        ),
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.MA520_SWING
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.strategy_facts["ma520_right_side"]["second_wave_restart"] is True


def test_ma520_falling_knife_stays_watch_only() -> None:
    result = _common(
        {"symbol": "600032.SH", "market_role": "FOLLOWER"},
        factor=_factor(
            close=10.1,
            ma5=10.2,
            ma10=10.3,
            ma20=10.0,
            ma60=9.5,
            low=9.95,
            ma_event="PULLBACK_HOLD_MA20",
            slopes={"ma5": -0.05, "ma10": -0.01, "ma20": -0.02},
        ),
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.MA520_SWING
    assert result.eligibility is Eligibility.WATCH
    assert result.plan_mode is None
    assert result.strategy_facts["ma520_right_side"]["confirmed"] is False
    assert "MA520_RIGHT_SIDE_NOT_CONFIRMED" in result.reason_codes


def test_ma520_bare_reclaim_with_falling_ma5_stays_watch_only() -> None:
    factor = _factor(
        close=10.1,
        ma5=10.2,
        ma10=10.3,
        ma20=10.0,
        ma60=9.5,
        low=10.05,
        ma_event="RECLAIM_MA20",
        slopes={"ma5": -0.01, "ma10": -0.01, "ma20": -0.02},
    )
    factor["timeframes"]["daily"]["previous_close"] = 9.8
    result = _common(
        {"symbol": "600033.SH", "market_role": "FOLLOWER"},
        factor=factor,
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.MA520_SWING
    assert result.eligibility is Eligibility.WATCH
    assert result.strategy_facts["ma520_right_side"]["confirmed"] is False
    assert "MA520_RIGHT_SIDE_NOT_CONFIRMED" in result.reason_codes


def test_ma520_bare_ma20_touch_is_not_relabelled_as_trend() -> None:
    result = _common(
        {"symbol": "600034.SH", "market_role": "FOLLOWER"},
        factor=_factor(
            close=10.1,
            ma5=9.8,
            ma10=9.4,
            ma20=10.0,
            ma60=9.0,
            low=9.9,
            ma_event="PULLBACK_HOLD_MA20",
            slopes={"ma5": 0.01, "ma10": 0.01, "ma20": -0.01},
        ),
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.MA520_SWING
    assert result.eligibility is Eligibility.WATCH
    assert result.strategy_facts["ma520_right_side"]["second_wave_restart"] is False
    assert "MA520_RIGHT_SIDE_NOT_CONFIRMED" in result.reason_codes


def test_closed_period_is_not_a_data_gap_when_long_ma_is_not_ready() -> None:
    factor = _factor()
    factor["timeframes"]["monthly"] = {
        "ready": False,
        "latest": {"closed": True, "close": 10.0},
        "latest_partial": {"closed": True, "close": 10.8},
        "moving_averages": {"ma5": 9.8, "ma20": 9.5, "ma60": None},
        "reasons": ["INSUFFICIENT_MA60"],
    }
    result = _common(
        {"symbol": "600017.SH", "market_role": "TREND_CORE"},
        factor=factor,
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
    )
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.strategy_facts["monthly_status"] == "CLOSED"
    assert "MONTH_NOT_CLOSED" not in result.reason_codes


def test_partial_weekly_bear_is_a_probe_not_a_hard_veto() -> None:
    factor = _factor()
    factor["timeframes"]["weekly"]["state"] = "BEAR_PARTIAL"
    result = _common(
        {"symbol": "600018.SH", "market_role": "TREND_CORE"},
        factor=factor,
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
    )

    assert result.eligibility is Eligibility.QUALIFIED
    assert result.plan_mode == "PROBE"
    assert "HIGHER_TIMEFRAME_CONDITIONAL_PROBE" in result.reason_codes
    assert "HIGHER_TIMEFRAME_BEARISH" not in result.veto_conditions
    assert result.strategy_facts["higher_timeframe_risk"] == "CONDITIONAL_PROBE"


def test_confirmed_weekly_bear_with_daily_bear_remains_rejected() -> None:
    factor = _factor(daily_state="BEAR_STACK")
    factor["timeframes"]["weekly"]["state"] = "BEAR_STACK"
    result = _common(
        {"symbol": "600019.SH", "market_role": "TREND_CORE"},
        factor=factor,
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
    )

    assert result.eligibility is Eligibility.REJECTED
    assert "HIGHER_TIMEFRAME_BEARISH" in result.veto_conditions
    assert "DAILY_TREND_WEAK" in result.veto_conditions


def test_generic_cross_section_leader_with_observed_no_board_uses_trend_route() -> None:
    result = _common(
        {"symbol": "600020.SH", "market_role": "LEADER"},
        a2={
            "market_role": "LEADER",
            "relative_strength": {"percentile": 90},
            "a2_factor_scores": {
                "tier_structure": {
                    "available": True,
                    "availability_state": "OBSERVED_ABSENT",
                    "ladder_height": 0,
                    "score": 0,
                    "source": "HITHINK_LIMIT_UP_LADDER",
                }
            },
        },
    )

    assert result.strategy_profile is StrategyProfile.TREND_MA5
    assert result.eligibility is Eligibility.QUALIFIED
    assert "LADDER_HEIGHT_MISSING" not in result.reason_codes
    assert result.strategy_facts["ladder"]["height"] == 0
    assert result.strategy_facts["ladder"]["availability_state"] == "OBSERVED_ABSENT"


def test_520_setup_is_not_shadowed_by_a2_trend_role_without_full_trend_stack() -> None:
    result = _common(
        {"symbol": "600021.SH", "market_role": "TREND_CORE"},
        factor=_factor(
            close=10.25,
            ma5=10.2,
            ma10=10.3,
            ma20=10.0,
            ma60=9.5,
            low=9.95,
            ma_event="PULLBACK_HOLD_MA20",
        ),
        kline={"labels": []},
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
    )

    assert result.strategy_profile is StrategyProfile.MA520_SWING
    assert result.eligibility is Eligibility.QUALIFIED


def test_missing_daily_price_or_tradability_is_data_gap() -> None:
    result = evaluate_a3_strategy(
        {"symbol": "600007.SH", "market_role": "TREND_CORE"},
        factor={"timeframes": {"monthly": {"closed": True}, "weekly": {"closed": True}}},
        price_levels={},
        tradability={},
        kline={},
        a2_context={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
    )
    assert result.eligibility is Eligibility.DATA_GAP
    assert "DAILY_CLOSE_MISSING" in result.reason_codes
    assert "TRADABILITY_DATA_MISSING" in result.reason_codes
    assert result.entry_reference_zone is None


def test_non_tradable_candidate_is_hard_rejected() -> None:
    # Pass the explicit contract to exercise the non-tradable veto without
    # changing any other evidence.
    result = evaluate_a3_strategy(
        {"symbol": "600027.SH", "market_role": "TREND_CORE"},
        factor=_factor(),
        price_levels=_prices(),
        tradability={"tradable": False},
        kline={"labels": []},
        a2_context={"market_role": "TREND_CORE"},
    )
    assert result.eligibility is Eligibility.REJECTED
    assert "NOT_TRADABLE" in result.veto_conditions


def test_leader_first_board_four_plus_and_locked_are_watch_only() -> None:
    base = {"symbol": "600008.SH", "market_role": "EMOTION_LEADER", "theme_stage": "CONFIRMATION", "ladder_intact": True}
    first = _common({**base, "ladder_height": 1}, a2={**base, "ladder_height": 1})
    high = _common({**base, "ladder_height": 4}, a2={**base, "ladder_height": 4})
    locked = _common({**base, "ladder_height": 2, "locked_limit_up": True}, a2={**base, "ladder_height": 2, "locked_limit_up": True})
    for result, reason in (
        (first, "FIRST_BOARD_OBSERVE_ONLY"),
        (high, "FOUR_PLUS_BOARD_WATCH_ONLY"),
        (locked, "ONE_PRICE_LOCKED_OBSERVE_ONLY"),
    ):
        assert result.eligibility is Eligibility.WATCH
        assert result.plan_mode is None
        assert reason in result.reason_codes


def test_leader_weak_or_distribution_is_rejected() -> None:
    weak = _common(
        {"symbol": "600009.SH", "market_role": "EMOTION_LEADER", "theme_stage": "RETREAT", "ladder_height": 2, "ladder_intact": True},
        a2={"market_role": "EMOTION_LEADER", "theme_stage": "RETREAT", "ladder_height": 2, "ladder_intact": True},
    )
    distribution = _common(
        {"symbol": "600010.SH", "market_role": "EMOTION_LEADER", "theme_stage": "CONFIRMATION", "ladder_height": 2, "ladder_intact": True},
        kline={"labels": ["DISTRIBUTION"]},
        a2={"market_role": "EMOTION_LEADER", "theme_stage": "CONFIRMATION", "ladder_height": 2, "ladder_intact": True},
    )
    assert weak.eligibility is Eligibility.REJECTED
    assert distribution.eligibility is Eligibility.REJECTED
    assert "LEADER_THEME_RETREAT_OR_CLIMAX" in weak.veto_conditions
    assert "HIGH_VOLUME_DISTRIBUTION" in distribution.veto_conditions


def test_trend_overextension_and_520_dead_cross_are_rejected() -> None:
    trend = _common(
        {"symbol": "600011.SH", "market_role": "TREND_CORE"},
        factor=_factor(close=13.0, ma5=10.0, ma10=9.8, ma20=9.4, ma60=8.7),
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 90}},
    )
    dead = _common(
        {"symbol": "600012.SH", "market_role": "FOLLOWER"},
        factor=_factor(close=9.7, ma5=9.5, ma10=9.8, ma20=10.0, ma60=9.0, ma_event="DEAD_CROSS_SHORT"),
        kline={"labels": []},
    )
    assert trend.eligibility is Eligibility.REJECTED
    assert "TREND_OVEREXTENDED" in trend.veto_conditions
    assert dead.strategy_profile is StrategyProfile.MA520_SWING
    assert dead.eligibility is Eligibility.REJECTED
    assert "MA520_DEAD_CROSS" in dead.veto_conditions


def test_trend_overextension_with_a_reasonable_ma5_retest_can_be_published() -> None:
    result = _common(
        {"symbol": "600028.SH", "market_role": "TREND_CORE", "overextended": True},
        factor=_factor(close=10.2, ma5=10.0, ma10=9.8, ma20=9.6, ma60=9.0, low=9.95),
        kline={"labels": []},
    )

    assert result.strategy_profile is StrategyProfile.TREND_MA5
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.strategy_facts["trend_paths"]["strong_pullback_geometry"] is True
    assert "TREND_OVEREXTENDED" not in result.veto_conditions


def test_overextension_without_retest_rejects_leader_route() -> None:
    result = _common(
        {
            "symbol": "600029.SH",
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 2,
            "ladder_intact": True,
            "overextended": True,
        },
        factor=_factor(close=13.0, ma5=10.0, ma10=9.8, ma20=9.4, ma60=8.7, low=12.5),
        kline={"labels": []},
        a2={
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 2,
            "ladder_intact": True,
        },
    )

    assert result.strategy_profile is StrategyProfile.LEADER_INTRADAY
    assert result.eligibility is Eligibility.REJECTED
    assert "OVEREXTENDED_WITHOUT_RETEST" in result.veto_conditions


def test_invalid_price_geometry_is_data_gap_and_no_strategy_is_explicit() -> None:
    invalid = _common(
        {"symbol": "600013.SH", "market_role": "TREND_CORE"},
        a2={"market_role": "TREND_CORE", "relative_strength": {"percentile": 80}},
        prices={"trigger_zone": {"low": 10.5, "high": 10.0}, "invalidation": 10.2, "max_chase_price": 9.9},
    )
    none = _common(
        {"symbol": "600014.SH", "market_role": "FOLLOWER"},
        factor=_factor(ma5=8.0, ma10=9.0, ma20=10.0, ma60=11.0, close=8.5),
        kline={"labels": []},
    )
    assert invalid.eligibility is Eligibility.DATA_GAP
    assert "PRICE_GEOMETRY_INVALID" in invalid.reason_codes
    assert invalid.entry_reference_zone is None
    assert none.strategy_profile is StrategyProfile.NO_NEXT_DAY_PLAN
    assert none.eligibility is Eligibility.REJECTED
    assert "NO_APPLICABLE_STRATEGY" in none.reason_codes


def test_route_alias_returns_same_explicit_contract() -> None:
    result = route_a3_strategy(
        {"symbol": "600015.SH", "market_role": "FOLLOWER"},
        _factor(ma5=10.2, ma10=10.3, ma20=10.0, ma60=9.5, close=10.25, low=9.95, ma_event="PULLBACK_HOLD_MA20"),
        _prices(),
        {"tradable": True},
        {"labels": []},
    )
    assert isinstance(result, dict)
    assert result["strategy_profile"] == "MA520_SWING"
    assert result["eligibility"] == "QUALIFIED"


def test_behavior_route_contract_and_holding_windows_are_explicit() -> None:
    leader = _common(
        {
            "symbol": "600101.SH",
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 2,
            "ladder_intact": True,
        },
        a2={
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 2,
            "ladder_intact": True,
        },
    )
    trend = _common(
        {"symbol": "600102.SH", "market_role": "TREND_CORE"},
        a2={"market_role": "TREND_CORE"},
    )
    ma520 = _common(
        {"symbol": "600103.SH", "market_role": "FOLLOWER"},
        factor=_factor(
            ma5=10.2,
            ma10=10.3,
            ma20=10.0,
            ma60=9.5,
            close=10.25,
            low=9.95,
            ma_event="PULLBACK_HOLD_MA20",
        ),
        kline={"labels": []},
    )

    assert leader.stock_behavior_type.value == "EMOTION"
    assert leader.route_permission.value == "ALLOW_A4"
    assert leader.expected_holding_sessions == {"min": 1, "max": 3}
    assert leader.time_stop_sessions == 3
    assert leader.setup_pattern == "LADDER_CONTINUATION"

    assert trend.stock_behavior_type.value == "TREND"
    assert trend.route_permission.value == "ALLOW_A4"
    assert trend.expected_holding_sessions == {"min": 3, "max": 10}
    assert trend.time_stop_sessions == 7
    assert ma520.stock_behavior_type.value == "TREND"
    assert ma520.expected_holding_sessions == {"min": 5, "max": 20}
    assert ma520.time_stop_sessions == 7
    for result in (leader, trend, ma520):
        assert result.cycle_alignment["all_three_bullish_required"] is False
        assert result.cycle_alignment["intraday_timeframes_owned_by"] == "A4_15M_5M"
        assert result.strategy_facts["decision_style"] == "EXPLICIT_CONDITIONS_NO_COMPOSITE_SCORE"


def test_holding_window_can_be_overridden_without_changing_route() -> None:
    result = _common(
        {
            "symbol": "600104.SH",
            "market_role": "TREND_CORE",
            "expected_holding_sessions": "2-6",
            "time_stop_sessions": 4,
        },
        a2={"market_role": "TREND_CORE"},
    )
    assert result.eligibility is Eligibility.QUALIFIED
    assert result.route_permission.value == "ALLOW_A4"
    assert result.expected_holding_sessions == {"min": 2, "max": 6}
    assert result.time_stop_sessions == 4
    assert result.strategy_facts["holding_contract_source"] == "candidate"


def test_explicit_behavior_route_conflict_is_blocked() -> None:
    result = _common(
        {
            "symbol": "600105.SH",
            "market_role": "TREND_CORE",
            "stock_behavior_type": "EMOTION",
        },
        a2={"market_role": "TREND_CORE"},
    )
    assert result.stock_behavior_type.value == "EMOTION"
    assert result.strategy_profile is StrategyProfile.NO_NEXT_DAY_PLAN
    assert result.route_permission.value == "BLOCKED"
    assert result.plan_mode is None
    assert "BEHAVIOR_ROUTE_CONFLICT" in result.reason_codes
    assert result.strategy_facts["routed_profile_before_behavior_gate"] == "TREND_MA5"


def test_explicit_unresolved_behavior_cannot_publish_a_plan() -> None:
    result = _common(
        {
            "symbol": "600106.SH",
            "market_role": "TREND_CORE",
            "stock_behavior_type": "UNRESOLVED",
        },
        a2={"market_role": "TREND_CORE"},
    )
    assert result.stock_behavior_type.value == "UNRESOLVED"
    assert result.route_permission.value == "BLOCKED"
    assert result.plan_mode is None
    assert result.strategy_profile is StrategyProfile.NO_NEXT_DAY_PLAN
    assert "STOCK_BEHAVIOR_UNRESOLVED" in result.reason_codes


def test_pattern_aliases_are_auditable_and_do_not_create_composite_scores() -> None:
    result = _common(
        {"symbol": "600107.SH", "market_role": "TREND_CORE"},
        kline={"labels": ["W底", "箱体突破"]},
    )
    assert result.setup_pattern == "W_BOTTOM"
    assert result.strategy_facts["kline_labels"][:2] == ["BOX_BREAKOUT", "W_BOTTOM"]
    assert "composite_score" not in result.strategy_facts


def test_reversal_pattern_label_does_not_authorize_left_side_entry() -> None:
    result = _common(
        {"symbol": "600109.SH", "market_role": "FOLLOWER"},
        factor=_factor(close=9.5, ma5=10.0, ma10=10.2, ma20=10.4, ma60=10.8),
        kline={"labels": ["W_BOTTOM"]},
    )
    assert result.setup_pattern == "W_BOTTOM"
    assert result.strategy_profile is StrategyProfile.NO_NEXT_DAY_PLAN
    assert result.route_permission.value == "BLOCKED"


def test_missing_market_role_and_no_route_remain_unresolved_and_blocked() -> None:
    result = _common(
        {"symbol": "600108.SH"},
        factor=_factor(
            close=8.5,
            ma5=8.0,
            ma10=9.0,
            ma20=10.0,
            ma60=11.0,
        ),
        kline={"labels": []},
    )
    assert result.stock_behavior_type.value == "UNRESOLVED"
    assert result.route_permission.value == "BLOCKED"
    assert result.strategy_profile is StrategyProfile.NO_NEXT_DAY_PLAN


def test_emotion_top_fact_blocks_only_the_emotion_route_with_auditable_signal() -> None:
    result = _common(
        {
            "symbol": "600110.SH",
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 2,
            "ladder_intact": True,
        },
        a2={
            "market_role": "EMOTION_LEADER",
            "theme_stage": "CONFIRMATION",
            "ladder_height": 2,
            "ladder_intact": True,
        },
        kline={"labels": ["炸板"]},
    )

    assert result.stock_behavior_type.value == "EMOTION"
    assert result.route_permission.value == "BLOCKED"
    assert "EMOTION_TOP_RISK_CONFIRMED" in result.veto_conditions
    assert result.behavior_risk["emotion_top"]["signals"] == ["FAILED_SEAL"]
    assert result.behavior_risk["trend_top"]["confirmed"] is False


def test_confirmed_trend_top_pattern_blocks_trend_route_without_composite_score() -> None:
    result = _common(
        {"symbol": "600111.SH", "market_role": "TREND_CORE"},
        a2={"market_role": "TREND_CORE"},
        kline={"labels": ["头肩顶"]},
    )

    assert result.stock_behavior_type.value == "TREND"
    assert result.route_permission.value == "BLOCKED"
    assert "TREND_TOP_RISK_CONFIRMED" in result.veto_conditions
    assert result.behavior_risk["trend_top"]["signals"] == ["HEAD_SHOULDERS_TOP"]
    assert result.behavior_risk["scoring_used"] is False


def test_model_side_free_form_top_label_does_not_become_server_owned_top_fact() -> None:
    result = _common(
        {
            "symbol": "600112.SH",
            "market_role": "TREND_CORE",
            "labels": ["头肩顶"],
        },
        a2={"market_role": "TREND_CORE"},
        kline={"labels": []},
    )

    assert result.eligibility is Eligibility.QUALIFIED
    assert result.route_permission.value == "ALLOW_A4"
    assert result.behavior_risk["trend_top"]["confirmed"] is False
