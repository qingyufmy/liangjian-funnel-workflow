from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.a3_strategy import evaluate_a3_strategy
from liangjian_funnel.runtime.strategies import (
    A4Action,
    StrategyProfile,
    StrategyEvaluation,
    aggregate_closed_bars,
    evaluate_a4_plan,
    evaluate_strategy,
)


TZ = ZoneInfo("Asia/Shanghai")


def _live_market_context(*, decision: str = "ALLOW", as_of: str = "2026-08-31T10:00:00+08:00") -> dict[str, object]:
    return {
        "live_market_state": {
            "status": "READY",
            "decision": decision,
            "as_of": as_of,
            "trade_date": "2026-08-31",
        }
    }


def _bar(at: datetime, *, close: float, open_: float | None = None, low: float | None = None, high: float | None = None, volume: float = 100.0, closed: bool = True) -> dict[str, object]:
    open_price = close - 0.2 if open_ is None else open_
    return {
        "symbol": "600001.SH",
        "interval": "1m",
        "bar_end": at,
        "open": open_price,
        "high": close + 0.2 if high is None else high,
        "low": close - 0.2 if low is None else low,
        "close": close,
        "volume": volume,
        "amount": close * volume,
        "closed": closed,
    }


def _bars(*, count: int = 30, start: datetime = datetime(2026, 8, 31, 9, 31, tzinfo=TZ), close: float = 10.5) -> list[dict[str, object]]:
    return [_bar(start + timedelta(minutes=index), close=close + index * 0.01) for index in range(count)]


def _base(profile: str, **extra: object) -> dict[str, object]:
    return {
        "symbol": "600001.SH",
        "strategy_profile": profile,
        "trade_date": "2026-08-31",
        "trigger_zone": {"low": 10.0, "high": 12.0},
        "invalidation_level": 8.0,
        "market_context": _live_market_context(),
        **extra,
    }


def _leader_bars() -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    start = datetime(2026, 8, 31, 9, 31, tzinfo=TZ)
    for index in range(15):
        result.append(_bar(start + timedelta(minutes=index), close=10.0, open_=9.8, low=9.7, high=10.2))
    for index in range(15, 30):
        close = 11.0 + (index - 15) * 0.02
        result.append(_bar(start + timedelta(minutes=index), close=close, open_=close - 0.15, low=close - 0.2, high=close + 0.2))
    return result


def test_each_strategy_has_a_deterministic_valid_entry() -> None:
    leader = evaluate_a4_plan(
        _base(
            StrategyProfile.LEADER_INTRADAY.value,
            leader_context={"valid": True, "theme_stage": "IGNITION", "ladder_intact": True, "market_role": "LEADER", "board_count": 2},
        ),
        _leader_bars(),
    )
    swing = evaluate_a4_plan(
        _base(
            StrategyProfile.MA520_SWING.value,
            daily_indicators={"ma5": 11.0, "ma20": 10.0, "close": 11.2},
            strategy_facts={"ma520_setup": {"second_wave_restart": True}},
        ),
        _bars(),
    )
    trend = evaluate_a4_plan(
        _base(
            StrategyProfile.TREND_MA5.value,
            daily_indicators={"ma5": 11.0, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
    )
    assert leader["action"] == A4Action.BUY_SIGNAL.value
    assert swing["action"] == A4Action.BUY_SIGNAL.value
    assert trend["action"] == A4Action.BUY_SIGNAL.value
    for result in (leader, swing, trend):
        assert result["closed_5m_end"] == "2026-08-31T10:00:00+08:00"
        assert result["closed_15m_end"] == "2026-08-31T10:00:00+08:00"
        assert result["veto_conditions"] == []


def test_leader_requires_context_and_520_requires_daily_snapshot() -> None:
    leader = evaluate_a4_plan(_base(StrategyProfile.LEADER_INTRADAY.value), _leader_bars())
    swing = evaluate_a4_plan(_base(StrategyProfile.MA520_SWING.value), _bars())
    assert leader["action"] == A4Action.START_CONFIRMATION.value
    assert "LEADER_CONTEXT_MISSING" in leader["reason_codes"]
    assert swing["action"] == A4Action.DATA_BLOCK.value
    assert "MA520_DAILY_SNAPSHOT_MISSING" in swing["reason_codes"]
    assert "A3_RIGHT_SIDE_CONFIRMATION_MISSING" in swing["reason_codes"]


def test_trend_trusts_a3_daily_route_and_520_requires_two_5m_confirmations() -> None:
    trend = evaluate_a4_plan(
        _base(StrategyProfile.TREND_MA5.value, daily_indicators={"ma5": 10.0, "ma10": 10.5, "ma20": 10.2, "ma60": 9.5, "close": 10.8}),
        _bars(),
    )
    weak_520_bars = _bars(count=15)
    for index in range(10, 15):
        at = weak_520_bars[index]["bar_end"]
        weak_520_bars[index] = _bar(at, close=9.5, open_=10.0, low=9.3, high=10.1)
    swing = evaluate_a4_plan(
        _base(
            StrategyProfile.MA520_SWING.value,
            market_context=_live_market_context(as_of="2026-08-31T09:45:00+08:00"),
            daily_indicators={"ma5": 11.0, "ma20": 10.0, "close": 11.2},
        ),
        weak_520_bars,
    )
    assert trend["action"] == A4Action.BUY_SIGNAL.value
    assert "A3_TREND_ROUTE_APPROVED" in trend["met_conditions"]
    assert "TREND_DAILY_NOT_MAIN_UPTREND" not in trend["reason_codes"]
    assert swing["action"] != A4Action.BUY_SIGNAL.value
    assert "MA520_TWO_CLOSED_5M_CONFIRMATIONS" in swing["unmet_conditions"]


def test_ma520_without_frozen_right_side_evidence_is_data_blocked() -> None:
    result = evaluate_a4_plan(
        _base(
            StrategyProfile.MA520_SWING.value,
            daily_indicators={"ma5": 11.0, "ma20": 10.0, "close": 11.2},
        ),
        _bars(),
    )
    assert result["action"] == A4Action.DATA_BLOCK.value
    assert result["state"] == "DATA_BLOCKED"
    assert "A3_RIGHT_SIDE_CONFIRMATION_MISSING" in result["reason_codes"]
    assert "A3_RIGHT_SIDE_CONFIRMATION" in result["unmet_conditions"]
    assert "A3_RIGHT_SIDE_CONFIRMATION_MISSING" in result["veto_conditions"]


def test_ma520_explicit_right_side_evidence_can_continue_to_entry() -> None:
    result = evaluate_a4_plan(
        _base(
            StrategyProfile.MA520_SWING.value,
            daily_indicators={"ma5": 11.0, "ma20": 10.0, "close": 11.2},
            strategy_facts={"ma520_setup": {"second_wave_restart": True}},
        ),
        _bars(),
    )
    assert result["action"] == A4Action.BUY_SIGNAL.value
    assert "A3_RIGHT_SIDE_CONFIRMATION" in result["met_conditions"]


def test_a3_ma520_payload_preserves_right_side_evidence_into_a4() -> None:
    factor = {
        "timeframes": {
            "monthly": {"closed": True, "state": "BULL"},
            "weekly": {"closed": True, "state": "BULL"},
            "daily": {
                "closed": True,
                "state": "BULL",
                "close": 10.25,
                "low": 9.95,
                "moving_averages": {"ma5": 10.2, "ma10": 10.3, "ma20": 10.0, "ma60": 9.5},
                "ma_slopes": {"ma5": 0.0, "ma10": -0.01, "ma20": -0.02},
                "ma_event": "PULLBACK_HOLD_MA20",
            },
        }
    }
    a3 = evaluate_a3_strategy(
        {"symbol": "600001.SH", "name": "520"},
        factor=factor,
        price_levels={"trigger_zone": {"low": 10.0, "high": 12.0}, "invalidation": 8.0, "max_chase_price": 13.0},
        tradability={"tradable": True},
        kline={"labels": []},
        a2_context={},
    )
    plan = a3.model_dump(mode="json")
    plan["trade_date"] = "2026-08-31"
    plan["market_context"] = _live_market_context()

    result = evaluate_a4_plan(plan, _bars())

    assert a3.strategy_profile.value == StrategyProfile.MA520_SWING.value
    assert a3.strategy_facts["ma520_right_side"]["second_wave_restart"] is True
    assert result["action"] == A4Action.BUY_SIGNAL.value
    assert "A3_RIGHT_SIDE_CONFIRMATION" in result["met_conditions"]


def test_ma520_right_side_guard_does_not_suppress_hard_stop_exit() -> None:
    bars = _bars()
    bars[-1] = _bar(bars[-1]["bar_end"], close=10.5, low=7.5)
    result = evaluate_a4_plan(
        _base(
            StrategyProfile.MA520_SWING.value,
            position_open=True,
            stop_level=8.0,
            daily_indicators={"ma5": 11.0, "ma20": 10.0, "close": 11.2},
        ),
        bars,
    )
    assert result["action"] == A4Action.FORCED_RISK_EXIT.value
    assert result["reason_codes"] == ["HARD_STOP"]


def test_unclosed_bucket_and_future_bar_never_trigger() -> None:
    partial = evaluate_a4_plan(
        _base(StrategyProfile.TREND_MA5.value, daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3}),
        _bars(count=4),
    )
    assert partial["action"] == A4Action.DATA_BLOCK.value
    assert "NO_CLOSED_5M" in partial["reason_codes"]

    bars = _bars(count=5)
    future = evaluate_a4_plan(
        _base(StrategyProfile.TREND_MA5.value, daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3}),
        bars,
        as_of=datetime(2026, 8, 31, 9, 34, tzinfo=TZ),
    )
    assert future["action"] == A4Action.DATA_BLOCK.value
    assert future["reason_codes"] == ["FUTURE_BAR_DETECTED"]


def test_lunch_break_is_not_aggregated_across_sessions() -> None:
    bars = _bars(count=15)
    afternoon = datetime(2026, 8, 31, 13, 1, tzinfo=TZ)
    bars.extend(_bar(afternoon + timedelta(minutes=index), close=11.0 + index * 0.01) for index in range(15))
    result = aggregate_closed_bars(bars)
    assert [item["bar_end"] for item in result["5m"]] == [
        "2026-08-31T09:35:00+08:00",
        "2026-08-31T09:40:00+08:00",
        "2026-08-31T09:45:00+08:00",
        "2026-08-31T13:05:00+08:00",
        "2026-08-31T13:10:00+08:00",
        "2026-08-31T13:15:00+08:00",
    ]
    assert [item["bar_end"] for item in result["15m"]] == [
        "2026-08-31T09:45:00+08:00",
        "2026-08-31T13:15:00+08:00",
    ]


def test_hard_stop_is_1m_safety_and_locked_limit_up_cannot_buy() -> None:
    bars = _bars()
    bars[-1] = _bar(bars[-1]["bar_end"], close=10.5, low=7.5)
    stopped = evaluate_a4_plan(
        _base(StrategyProfile.TREND_MA5.value, stop_level=8.0, daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3}),
        bars,
    )
    assert stopped["action"] == A4Action.FORCED_RISK_EXIT.value
    assert stopped["reason_codes"] == ["HARD_STOP"]

    locked_bars = _bars()
    locked_bars[-1] = _bar(locked_bars[-1]["bar_end"], close=10.0, open_=10.0, low=10.0, high=10.0)
    locked = evaluate_a4_plan(
        _base(StrategyProfile.TREND_MA5.value, upper_limit=10.0, daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3}),
        locked_bars,
    )
    assert locked["action"] != A4Action.BUY_SIGNAL.value
    assert "LOCKED_LIMIT_UP" in locked["reason_codes"]


def test_unknown_profile_and_data_gap_fail_closed_per_plan() -> None:
    unknown = evaluate_a4_plan({"symbol": "600001.SH", "strategy_profile": "NOT_A_STRATEGY"}, _bars())
    assert unknown["action"] == A4Action.DATA_BLOCK.value
    assert unknown["veto_conditions"] == ["UNKNOWN_STRATEGY_PROFILE"]
    behavior_conflict = evaluate_a4_plan(
        {
            **_base(StrategyProfile.TREND_MA5.value),
            "stock_behavior_type": "EMOTION",
        },
        _bars(),
    )
    assert behavior_conflict["action"] == A4Action.DATA_BLOCK.value
    assert behavior_conflict["veto_conditions"] == ["A4_BEHAVIOR_ROUTE_CONFLICT"]
    blocked = evaluate_a4_plan(_base(StrategyProfile.TREND_MA5.value, data_gap=True), _bars())
    assert blocked["action"] == A4Action.DATA_BLOCK.value
    assert blocked["reason_codes"] == ["PLAN_DATA_GAP"]


def test_520_does_not_use_intraday_ma5_ma20_and_trend_add_cannot_average_down() -> None:
    result = evaluate_a4_plan(
        _base(
            StrategyProfile.MA520_SWING.value,
            daily_indicators={"ma5": 11.0, "ma20": 10.0, "close": 11.2},
            strategy_facts={"ma520_setup": {"second_wave_restart": True}},
            intraday={"moving_averages": {"ma5": 999, "ma20": 1}},
        ),
        _bars(),
    )
    assert "DAILY_MA5_MA20_ONLY" in result["met_conditions"]
    assert result["action"] == A4Action.BUY_SIGNAL.value

    add = evaluate_a4_plan(
        _base(
            StrategyProfile.TREND_MA5.value,
            action=A4Action.ADD_SIGNAL.value,
            position_open=True,
            entry_price=20.0,
            daily_indicators={"ma5": 11.0, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
    )
    assert add["action"] != A4Action.ADD_SIGNAL.value
    assert "TREND_ADD_REQUIRES_PROFIT" in add["reason_codes"]


def test_invalid_1m_and_missing_data_are_data_blocked() -> None:
    unclosed = evaluate_a4_plan(_base(StrategyProfile.TREND_MA5.value), [_bar(datetime(2026, 8, 31, 9, 31, tzinfo=TZ), close=10, closed=False)])
    assert unclosed["action"] == A4Action.DATA_BLOCK.value
    assert unclosed["reason_codes"] == ["UNFINISHED_1M_BAR"]
    missing = evaluate_a4_plan(_base(StrategyProfile.TREND_MA5.value), [])
    assert missing["action"] == A4Action.DATA_BLOCK.value
    assert missing["reason_codes"] == ["NO_1M_BARS"]


def test_public_entry_returns_frozen_pydantic_result_and_accepts_overlays() -> None:
    result = evaluate_strategy(
        _base(StrategyProfile.TREND_MA5.value, daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3}),
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        position={"quantity": 0},
        market_context={**_live_market_context(), "market_shock": False},
    )
    assert isinstance(result, StrategyEvaluation)
    assert result.action == A4Action.BUY_SIGNAL.value
    assert result["symbol"] == "600001.SH"
    assert result.model_dump(mode="json")["closed_5m_end"] == "2026-08-31T10:00:00+08:00"
    with pytest.raises(Exception):
        result.action = A4Action.NO_ACTION.value


def test_a3_compact_plan_fields_are_accepted_without_recomputing_daily_facts() -> None:
    plan = _base(
        StrategyProfile.TREND_MA5.value,
        daily_ma={"ma5": 11.0, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5},
        strategy_facts={"daily_close": 11.3, "daily_moving_averages": {"ma5": 1, "ma20": 1}},
        entry_reference_zone={"low": 10.0, "high": 12.0},
        daily_invalidation=8.0,
    )
    plan.pop("daily_indicators", None)
    plan.pop("trigger_zone", None)
    result = evaluate_strategy(
        plan,
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
    )
    assert result.action == A4Action.BUY_SIGNAL.value


@pytest.mark.parametrize("profile", [item.value for item in StrategyProfile])
def test_contract_always_contains_required_fields(profile: str) -> None:
    result = evaluate_a4_plan(_base(profile), _bars(count=4))
    assert set(("state", "action", "reason_codes", "met_conditions", "unmet_conditions", "veto_conditions", "closed_5m_end", "closed_15m_end")) <= set(result)
    assert result["action"] in {item.value for item in A4Action}


def test_type_specific_top_risk_cancels_entry_and_exits_matching_position() -> None:
    emotion_risk = {
        "emotion_top": {"confirmed": True, "signals": ["FAILED_SEAL"]},
        "trend_top": {"confirmed": False, "signals": []},
    }
    cancelled = evaluate_a4_plan(
        _base(
            StrategyProfile.LEADER_INTRADAY.value,
            stock_behavior_type="EMOTION",
            behavior_risk=emotion_risk,
            leader_context={
                "theme_stage": "CONFIRMATION",
                "ladder_intact": True,
                "board_count": 2,
            },
        ),
        _leader_bars(),
    )
    exited = evaluate_a4_plan(
        _base(
            StrategyProfile.LEADER_INTRADAY.value,
            stock_behavior_type="EMOTION",
            position_open=True,
            behavior_risk=emotion_risk,
            leader_context={
                "theme_stage": "CONFIRMATION",
                "ladder_intact": True,
                "board_count": 2,
            },
        ),
        _leader_bars(),
    )

    assert cancelled["state"] == "CANCELLED"
    assert cancelled["action"] == A4Action.NO_ACTION.value
    assert cancelled["reason_codes"] == ["A4_EMOTION_TOP_RISK_CONFIRMED"]
    assert exited["state"] == "EXIT_READY"
    assert exited["action"] == A4Action.SELL_SIGNAL.value


def test_stale_a3_market_prior_does_not_block_as_current_market_state() -> None:
    result = evaluate_strategy(
        _base(
            StrategyProfile.TREND_MA5.value,
            stock_behavior_type="TREND",
            market_environment="BEAR_RISK",
            market_emotion={"new_long_permission": "NO_NEW_ENTRY"},
            market_context=None,
            daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
    )

    assert result.action == A4Action.DATA_BLOCK.value
    assert result.state == "DATA_BLOCKED"
    assert result.reason_codes == ("LIVE_MARKET_STATE_MISSING",)
    assert "A4_MARKET_BEAR_NO_NEW_ENTRY" not in result.reason_codes
    assert result.live_market_state_status == "DATA_BLOCK"


def test_fresh_market_block_cancels_new_entry_only() -> None:
    result = evaluate_strategy(
        _base(
            StrategyProfile.TREND_MA5.value,
            stock_behavior_type="TREND",
            daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        market_context=_live_market_context(decision="BLOCK_NEW_ENTRY"),
    )

    assert result.action == A4Action.START_CONFIRMATION.value
    assert result.state == "CONFIRMING"
    assert "A4_LIVE_MARKET_BLOCK_WAIT" in result.reason_codes
    assert result.market_gate["decision"] == "BLOCK_NEW_ENTRY"


def test_fresh_market_allow_keeps_deterministic_entry() -> None:
    result = evaluate_strategy(
        _base(
            StrategyProfile.TREND_MA5.value,
            stock_behavior_type="TREND",
            market_environment="BEAR_RISK",
            market_emotion={"new_long_permission": "NO_NEW_ENTRY"},
            daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        market_context=_live_market_context(decision="ALLOW"),
    )

    assert result.action == A4Action.BUY_SIGNAL.value
    assert result.market_gate["status"] == "READY"
    assert result.market_gate["decision"] == "ALLOW"


def test_fresh_tencent_index_fallback_is_explicitly_usable() -> None:
    context = _live_market_context(decision="ALLOW")
    context["live_market_state"]["status"] = "READY_DEGRADED"
    context["live_market_state"]["source"] = "TENCENT_INDEX_FALLBACK"
    result = evaluate_strategy(
        _base(
            StrategyProfile.TREND_MA5.value,
            stock_behavior_type="TREND",
            daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        market_context=context,
    )

    assert result.action == A4Action.BUY_SIGNAL.value
    assert result.market_gate["status"] == "READY"
    assert result.market_gate["state_status"] == "READY_DEGRADED"


def test_fresh_market_caution_keeps_entry_and_reduces_position() -> None:
    result = evaluate_strategy(
        _base(
            StrategyProfile.TREND_MA5.value,
            stock_behavior_type="TREND",
            daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        market_context={
            "live_market_state": {
                **_live_market_context(decision="CAUTION")["live_market_state"],
                "suggested_position_cap_pct": 0.5,
            }
        },
    )

    assert result.action == A4Action.BUY_SIGNAL.value
    assert "A4_MARKET_CAUTION_REDUCED_POSITION" in result.reason_codes
    assert result.suggested_position_cap_pct == 0.5
    assert result.market_gate["decision"] == "CAUTION"


def test_degraded_tencent_fallback_can_authorize_new_entry() -> None:
    result = evaluate_strategy(
        _base(
            StrategyProfile.TREND_MA5.value,
            stock_behavior_type="TREND",
            daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        market_context={
            "live_market_state": {
                "status": "READY_DEGRADED",
                "entry_permission": "ALLOW",
                "as_of": "2026-08-31T10:00:00+08:00",
                "trade_date": "2026-08-31",
            }
        },
    )

    assert result.action == A4Action.BUY_SIGNAL.value
    assert result.market_gate["status"] == "READY"
    assert result.market_gate["state_status"] == "READY_DEGRADED"


def test_expired_live_market_state_is_data_block_not_bearish() -> None:
    result = evaluate_strategy(
        _base(
            StrategyProfile.TREND_MA5.value,
            stock_behavior_type="TREND",
            market_environment="BEAR_RISK",
            daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        _bars(),
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        market_context=_live_market_context(as_of="2026-08-31T09:50:00+08:00"),
    )

    assert result.action == A4Action.DATA_BLOCK.value
    assert result.state == "DATA_BLOCKED"
    assert result.reason_codes == ("LIVE_MARKET_STATE_STALE",)
    assert result.market_gate["status"] == "DATA_BLOCK"
    assert result.market_gate["as_of"] == "2026-08-31T09:50:00+08:00"
    assert result.market_gate["trade_date"] == "2026-08-31"
    assert result.market_gate["age_seconds"] == 600.0
    assert "A4_MARKET_BEAR_NO_NEW_ENTRY" not in result.reason_codes


def test_fresh_market_block_does_not_suppress_existing_position_exit() -> None:
    result = evaluate_strategy(
        _base(
            StrategyProfile.TREND_MA5.value,
            stock_behavior_type="TREND",
            position_open=True,
            stop_level=8.0,
            daily_indicators={"ma5": 11, "ma10": 10.7, "ma20": 10.2, "ma60": 9.5, "close": 11.3},
        ),
        [*_bars()[:-1], _bar(_bars()[-1]["bar_end"], close=10.5, low=7.5)],
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        market_context=_live_market_context(decision="BLOCK_NEW_ENTRY"),
    )

    assert result.action == A4Action.FORCED_RISK_EXIT.value
    assert result.state == "FORCED_RISK_EXIT"
    assert result.reason_codes == ("HARD_STOP",)
