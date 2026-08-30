from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.evaluation.a4_replay import run_a4_replay


TZ = ZoneInfo("Asia/Shanghai")


def _bars() -> tuple[MinuteBar, ...]:
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
            symbol="600519.SH",
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


def test_a4_replay_isolated_full_day_reaches_one_next_bar_fill(tmp_path) -> None:
    report = run_a4_replay(
        trade_date=date(2026, 8, 28),
        source_run_id="source-run",
        source_plan={
            "symbol": "600519.SH",
            "name": "贵州茅台",
            "trigger_zone": {"low": 10.0, "high": 11.0},
            "invalidation_level": 9.0,
            "risk_unit": "NO_ENTRY",
        },
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
    assert (tmp_path / "report" / "a4_replay_latest.md").is_file()
