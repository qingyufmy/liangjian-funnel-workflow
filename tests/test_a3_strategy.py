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
        factor=_factor(ma5=10.2, ma10=10.3, ma20=10.0, ma60=9.5, close=10.15, low=9.95, ma_event="PULLBACK_HOLD_MA20"),
        kline={"labels": []},
    )

    assert leader.strategy_profile is StrategyProfile.LEADER_INTRADAY
    assert trend.strategy_profile is StrategyProfile.TREND_MA5
    assert ma520.strategy_profile is StrategyProfile.MA520_SWING
    assert all(item.eligibility is Eligibility.QUALIFIED for item in (leader, trend, ma520))
    assert leader.strategy_profile != trend.strategy_profile != ma520.strategy_profile
    assert not any(key in trend.model_dump() for key in ("score", "weight", "composite_score"))


def test_leader_has_priority_over_trend_and_520() -> None:
    result = _common(
        {"symbol": "600004.SH", "market_role": "LEADER", "theme_stage": "IGNITION", "ladder_height": 2, "ladder_intact": True},
        a2={"market_role": "LEADER", "theme_stage": "IGNITION", "ladder_height": 2, "ladder_intact": True},
    )
    assert result.strategy_profile == StrategyProfile.LEADER_INTRADAY
    assert result.strategy_profile != StrategyProfile.TREND_MA5
    assert result.strategy_profile != StrategyProfile.MA520_SWING


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
        _factor(ma5=10.2, ma10=10.3, ma20=10.0, ma60=9.5, close=10.15, low=9.95, ma_event="PULLBACK_HOLD_MA20"),
        _prices(),
        {"tradable": True},
        {"labels": []},
    )
    assert isinstance(result, dict)
    assert result["strategy_profile"] == "MA520_SWING"
    assert result["eligibility"] == "QUALIFIED"
