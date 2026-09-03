from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.pipeline.a3_strategy import Eligibility, evaluate_a3_strategy
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.state import PlanStatus, RuntimeStore
from liangjian_funnel.runtime.strategies import A4Action, evaluate_a4_plan


TZ = ZoneInfo("Asia/Shanghai")


def _a3_factor(*, daily_state: str = "BULL") -> dict:
    return {
        "timeframes": {
            "monthly": {"closed": True, "state": "BULL"},
            "weekly": {"closed": True, "state": "BULL"},
            "daily": {
                "closed": True,
                "state": daily_state,
                "close": 10.5,
                "low": 10.0,
                "moving_averages": {
                    "ma5": 10.1,
                    "ma10": 9.8,
                    "ma20": 9.4,
                    "ma60": 8.9,
                },
                "ma_slopes": {"ma5": 0.1, "ma10": 0.08, "ma20": 0.05},
            },
        }
    }


def _a3_prices() -> dict:
    return {
        "trigger_zone": {"low": 10.0, "high": 10.2},
        "invalidation": 9.6,
        "max_chase_price": 10.6,
    }


def _bars(count: int = 30) -> tuple[MinuteBar, ...]:
    start = datetime(2026, 8, 31, 9, 31, tzinfo=TZ)
    return tuple(
        MinuteBar(
            symbol="600001",
            interval="1m",
            bar_end=start + timedelta(minutes=index),
            open=10.3 + index * 0.01,
            high=10.7 + index * 0.01,
            low=10.3 + index * 0.01,
            close=10.5 + index * 0.01,
            volume=1_000,
            amount=(10.5 + index * 0.01) * 1_000,
            source_id="MOOTDX:127.0.0.1:7709",
        )
        for index in range(count)
    )


def _a4_plan(**overrides: object) -> dict:
    plan: dict[str, object] = {
        "plan_id": "p-observability",
        "lane_id": "lane-a",
        "symbol": "600001.SH",
        "strategy_profile": "TREND_MA5",
        "entry_reference_zone": {"low": 10.0, "high": 12.0},
        "invalidation_level": 8.0,
        "stop_level": 8.0,
        "daily_indicators": {
            "ma5": 11.0,
            "ma10": 10.7,
            "ma20": 10.2,
            "ma60": 9.5,
            "close": 11.3,
        },
        "market_context": {
            "live_market_state": {
                "status": "READY",
                "entry_permission": "ALLOW",
                "as_of": "2026-08-31T10:00:00+08:00",
                "trade_date": "2026-08-31",
                "source": "TEST_FULL_MARKET",
            }
        },
    }
    plan.update(overrides)
    return plan


def test_a3_gate_projection_keeps_all_failures_and_reason_kinds() -> None:
    result = evaluate_a3_strategy(
        {"symbol": "600001.SH", "market_role": "TREND_CORE"},
        factor=_a3_factor(daily_state="BEAR_STACK"),
        price_levels={},
        tradability={"tradable": True},
        kline={},
        market_regime="RISK_OFF",
        sector_permission="NO_NEW_ENTRY",
    )

    assert result.eligibility is Eligibility.DATA_GAP
    assert result.first_blocking_gate is not None
    assert {
        "PRICE_GEOMETRY_VALID",
        "DAILY_NOT_BEARISH",
    } <= set(result.all_failed_gates)
    for detail in result.gate_results.values():
        assert set(detail) == {"met", "reason", "kind", "available"}
        assert detail["kind"] == str(detail["kind"]).upper()
        assert detail["kind"] in {"CONDITION", "VETO", "DATA", "ROUTE", "ABLATED"}
    assert result.gate_results["MARKET_RISK_CLASSIFIED"] == {
        "met": True,
        "reason": "MARKET_RISK_OFF_CONTEXT_ONLY",
        "kind": "CONDITION",
        "available": True,
    }
    assert "MARKET_RISK_OFF" not in result.gate_results
    assert "MARKET_RISK_OFF" in result.reason_codes
    assert "MARKET_RISK_OFF" not in result.all_failed_gates
    assert "MARKET_RISK_OFF" not in result.veto_conditions
    assert result.gate_results["PRICE_GEOMETRY_VALID"]["available"] is False


def test_a3_disabled_ablation_preserves_decision_contract() -> None:
    kwargs = {
        "candidate": {"symbol": "600001.SH", "market_role": "TREND_CORE"},
        "factor": _a3_factor(),
        "price_levels": _a3_prices(),
        "tradability": {"tradable": True},
        "kline": {"labels": ["PLATFORM_BREAKOUT"]},
    }
    baseline = evaluate_a3_strategy(**kwargs).model_dump(mode="json")
    disabled = evaluate_a3_strategy(
        **kwargs,
        ablation={"enabled": False, "disabled_gates": ["DAILY_CLOSED"]},
    ).model_dump(mode="json")

    for field in (
        "strategy_profile",
        "eligibility",
        "plan_mode",
        "entry_reference_zone",
        "daily_invalidation",
        "reason_codes",
        "met_conditions",
        "unmet_conditions",
        "veto_conditions",
    ):
        assert disabled[field] == baseline[field]
    assert disabled["A3_ABLATION_MODE"] is False
    assert disabled["publication_state"] is None


def test_a3_ablation_is_explicit_and_cannot_be_published() -> None:
    result = evaluate_a3_strategy(
        {"symbol": "600001.SH", "market_role": "TREND_CORE"},
        factor=_a3_factor(),
        price_levels=_a3_prices(),
        tradability={"tradable": True},
        kline={"labels": ["PLATFORM_BREAKOUT"]},
        ablation={"enabled": True, "disabled_gates": ["DAILY_CLOSED"]},
    )

    assert result.A3_ABLATION_MODE is True
    assert result.a3_ablation_mode is True
    assert result.eligibility is Eligibility.DATA_GAP
    assert result.publication_state == "BLOCKED"
    assert "eligibility_before_ablation" not in result.model_dump()
    assert result.ablation_shadow_eligibility in {
        Eligibility.QUALIFIED.value,
        Eligibility.WATCH.value,
        Eligibility.DATA_GAP.value,
    }
    if not result.all_failed_gates:
        assert result.ablation_shadow_eligibility == Eligibility.QUALIFIED.value
    assert result.plan_mode is None
    assert result.gate_results["DAILY_CLOSED"]["kind"] == "ABLATED"
    assert result.gate_results["DAILY_CLOSED"]["reason"] == "ABLATED"
    assert result.strategy_facts["A3_ABLATION_MODE"] is True
    assert (
        result.strategy_facts["ablation_shadow_eligibility"]
        == result.ablation_shadow_eligibility
    )
    assert result.strategy_facts["publication_state"] == "BLOCKED"

    blocked = evaluate_a4_plan(
        _a4_plan(A3_ABLATION_MODE=True),
        _bars(),
        as_of=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
    )
    assert blocked["action"] == A4Action.DATA_BLOCK.value
    assert blocked["reason_codes"] == ["A3_ABLATION_MODE"]


def test_a4_confirmation_projection_is_strategy_specific_and_tracks_sector_lag() -> None:
    result = evaluate_a4_plan(
        _a4_plan(sector_context={"as_of": "2026-08-31T09:55:30+08:00"}),
        _bars(),
        as_of=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
    )

    assert result["action"] == A4Action.BUY_SIGNAL.value
    assert result["sector_data_lag_s"] == 270.0
    assert "A3_TREND_ROUTE_APPROVED" in result["confirmation_results"]
    assert "MA520_TWO_CLOSED_5M_CONFIRMATIONS" not in result["confirmation_results"]
    assert result["all_failed_confirmations"] == []


def test_monitor_persists_only_safe_llm_reason_code_and_strategy_observability(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "monitor.sqlite3")
    store.create_execution_plan(
        "p-observability",
        "lane-a",
        "600001.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        payload=_a4_plan(),
    )
    store.activate_plan("p-observability")
    current = _bars()[-1].bar_end

    def veto(_context: dict) -> dict:
        return {
            "signals": [
                {
                    "plan_id": "p-observability",
                    "llm_veto": True,
                    "reason_code": "MODEL_RISK",
                    "thinking": "do not persist this text",
                }
            ]
        }

    result = MonitorEngine(store, llm_veto=veto).process_minute(
        "lane-a",
        {"600001.SH": _bars()[-1]},
        minute_snapshot_id="snapshot-1",
        now=current,
        bar_histories={"600001.SH": _bars()},
    )
    assert result.events[-1].action == "LLM_VETO"
    assert result.events[-1].llm_veto is True
    assert result.events[-1].llm_reason_code == "MODEL_RISK"
    event = store.list_monitor_events(lane_id="lane-a", effective_only=True)[-1]
    payload = json.loads(event["payload_json"])
    assert payload["llm_veto"] is True
    assert payload["llm_reason_code"] == "MODEL_RISK"
    assert payload["strategy"]["llm_veto"] is True
    assert payload["strategy"]["llm_reason_code"] == "MODEL_RISK"
    assert "thinking" not in event["payload_json"]
    assert "confirmation_results" in payload["strategy"]
