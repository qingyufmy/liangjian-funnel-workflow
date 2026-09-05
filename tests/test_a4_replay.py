from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.evaluation.a4_replay import run_a4_replay, run_a4_replay_batch


TZ = ZoneInfo("Asia/Shanghai")


def _bars(symbol: str = "600519.SH") -> tuple[MinuteBar, ...]:
    day = date(2026, 8, 28)
    times = []
    current = datetime(2026, 8, 28, 9, 31, tzinfo=TZ)
    while current.time().strftime("%H:%M") <= "11:30":
        times.append(current)
        current += timedelta(minutes=1)
    current = datetime(2026, 8, 28, 13, 1, tzinfo=TZ)
    while current.time().strftime("%H:%M") <= "15:00":
        times.append(current)
        current += timedelta(minutes=1)
    assert len(times) == 240
    return tuple(
        MinuteBar(
            symbol=symbol,
            interval="1m",
            bar_end=at,
            open=10.5,
            high=10.8,
            low=9.9,
            close=10.5,
            volume=1_000,
            amount=10_500,
            source_id="TEST_ONLY:FULL_DAY",
        )
        for at in times
    )


def _plan(symbol: str, *, source_plan_id: str, confirmation_bars: int = 2, risk_unit: str = "STANDARD") -> dict:
    return {
        "plan_id": source_plan_id,
        "symbol": symbol,
        "name": f"测试{symbol}",
        "trigger_zone": {"low": 10.0, "high": 11.0},
        "invalidation_level": 9.0,
        "risk_unit": risk_unit,
        "confirmation_bars": confirmation_bars,
        "strategy_profile": "MA520_SWING",
        "execution_route": "A4_PAPER_ONLY",
        "stock_behavior_type": "TREND",
        "daily_indicators": {"ma5": 10.4, "ma20": 10.0, "close": 10.5},
        "strategy_facts": {"ma520_setup": {"second_wave_restart": True}},
    }


def test_a4_replay_isolated_full_day_reaches_one_next_bar_fill(tmp_path) -> None:
    report = run_a4_replay(
        trade_date=date(2026, 8, 28),
        source_run_id="source-run",
        source_plan={**_plan("600519.SH", source_plan_id="source-plan-single", confirmation_bars=1, risk_unit="NO_ENTRY"), "name": "贵州茅台"},
        bars=_bars(),
        state_db_path=tmp_path / "replay.sqlite3",
        output_dir=tmp_path / "report",
        official_a3_plan_count=0,
    )
    assert report["mode"] == "TEST_ONLY_COUNTERFACTUAL"
    assert report["official_a3_plan_count"] == 0
    assert report["bar_coverage"]["count"] == 240
    assert report["model_calls"] == 1
    assert len(report["fills"]) == 1
    assert report["invariants"]["next_bar_fill_only"] is True
    assert report["invariants"]["real_trading_connected"] is False
    assert report["invariants"]["strategy_document_conformance"] is True
    conformance = report["strategy_document_conformance"]
    assert conformance["status"] == "PASS"
    assert conformance["checks"]["daily_macd_a3_confirmation_only"] is True
    assert conformance["checks"]["m15_macd_auxiliary_only"] is True
    assert conformance["checks"]["kdj_observation_only"] is True
    assert conformance["latest_indicator_observations"]["multi_indicator_vote_used"] is False
    assert (tmp_path / "report" / "a4_replay_latest.md").is_file()


def test_a4_replay_preserves_core_execution_parameters_and_signal_identity(tmp_path) -> None:
    plan = _plan("600519.SH", source_plan_id="core-plan-1", confirmation_bars=2, risk_unit="STANDARD")
    report = run_a4_replay(
        trade_date=date(2026, 8, 28),
        source_run_id="source-run-core",
        source_plan=plan,
        bars=_bars(),
        state_db_path=tmp_path / "core.sqlite3",
        output_dir=tmp_path / "core-report",
        source_pool="core_watch_pool",
        official_a3_plan_count=1,
    )
    test_plan = report["test_plan"]
    assert test_plan["source_plan_id"] == "core-plan-1"
    assert test_plan["source_risk_unit"] == "STANDARD"
    assert test_plan["test_risk_unit"] == "STANDARD"
    assert test_plan["source_confirmation_bars"] == 2
    assert test_plan["test_confirmation_bars"] == 2
    assert test_plan["source_pool"] == "core_watch_pool"
    first_known = report["effective_events"][0]["first_known_minute"]
    assert first_known >= "2026-08-28T09:32:00+08:00"
    assert report["signal_identities"][0]["source_plan_id"] == "core-plan-1"


def test_a4_replay_batch_aggregates_two_plans_in_one_isolated_ledger(tmp_path) -> None:
    plans = (
        _plan("600519.SH", source_plan_id="core-plan-1"),
        _plan("600520.SH", source_plan_id="core-plan-2"),
    )
    report = run_a4_replay_batch(
        trade_date=date(2026, 8, 28),
        source_run_id="source-run-batch",
        source_plans=plans,
        source_pools=("core_watch_pool", "core_watch_pool"),
        bars_by_symbol={"600519.SH": _bars("600519.SH"), "600520.SH": _bars("600520.SH")},
        state_db_path=tmp_path / "batch.sqlite3",
        output_dir=tmp_path / "batch-report",
    )
    assert report["status"] == "READY"
    assert report["summary"] == {
        "total_plans": 2,
        "complete_240_coverage_count": 2,
        "data_failure_count": 0,
        "replay_failure_count": 0,
        "events": sum(item["events"] for item in report["results"]),
        "effective_events": 2,
        "fills": 2,
    }
    assert report["action_counts"]["BUY"] == 2
    assert report["invariants"]["production_state_isolated"] is True
    assert report["invariants"]["source_execution_parameters_preserved"] is True
    assert report["invariants"]["strategy_document_conformance"] is True
    assert report["strategy_document_conformance"]["status"] == "PASS"
    assert (tmp_path / "batch.sqlite3").is_file()
    assert (tmp_path / "batch-report" / "a4_replay_batch_latest.md").is_file()


def test_a4_replay_batch_isolates_one_incomplete_symbol(tmp_path) -> None:
    plans = (
        _plan("600519.SH", source_plan_id="core-plan-ok"),
        _plan("600520.SH", source_plan_id="core-plan-missing"),
    )
    report = run_a4_replay_batch(
        trade_date=date(2026, 8, 28),
        source_run_id="source-run-partial",
        source_plans=plans,
        source_pools=("core_watch_pool", "secondary_watch_pool"),
        bars_by_symbol={"600519.SH": _bars("600519.SH"), "600520.SH": _bars("600520.SH")[:-1]},
        state_db_path=tmp_path / "partial.sqlite3",
        output_dir=tmp_path / "partial-report",
        data_errors={"600520.SH": "TENCENT_INSUFFICIENT_BARS"},
    )
    assert report["status"] == "DEGRADED"
    assert report["summary"]["total_plans"] == 2
    assert report["summary"]["complete_240_coverage_count"] == 1
    assert report["summary"]["data_failure_count"] == 1
    assert report["summary"]["fills"] == 1
    rows = {item["symbol"]: item for item in report["results"]}
    assert rows["600520.SH"]["status"] == "DATA_FAILURE"
    assert rows["600519.SH"]["status"] in {"READY", "NO_EFFECTIVE_SIGNAL"}
    assert report["invariants"]["per_symbol_failure_isolation"] is True
