#!/usr/bin/env python3
"""Offline, wall-clock A4 stress evidence for S03.

This harness uses temporary simulation ledgers and generated full-session
minute fixtures.  It performs no network, model or notification call.  It
measures the deterministic position-risk path repeatedly and one decision-core
round for 50/100/200 plans; it is not production latency evidence.
"""
from __future__ import annotations

import argparse
import json
import math
import platform
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationAction
from liangjian_funnel.runtime.state import PlanStatus, RuntimeStore


SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = datetime(2026, 9, 21, tzinfo=SHANGHAI)


def session_minutes() -> tuple[datetime, ...]:
    rows: list[datetime] = []
    for hour, start, end in ((9, 31, 59), (10, 0, 59), (11, 0, 30), (13, 1, 59), (14, 0, 59), (15, 0, 0)):
        rows.extend(
            TRADE_DATE.replace(hour=hour, minute=minute)
            for minute in range(start, end + 1)
        )
    assert len(rows) == 240
    return tuple(rows)


def minute_bar(symbol: str, at: datetime, *, price: float = 10.0) -> MinuteBar:
    return MinuteBar(
        symbol=symbol,
        interval="1m",
        bar_end=at,
        open=price,
        high=price + 0.1,
        low=price - 0.1,
        close=price,
        volume=10_000,
        amount=100_000,
        source_id="S03_OFFLINE_STRESS",
    )


def percentile(values: Iterable[float], probability: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, math.ceil(len(ordered) * probability) - 1))]


def prepare(store: RuntimeStore, count: int) -> tuple[str, ...]:
    lane_id = "lane_1"
    broker = PaperBroker(store, account_id=f"paper:{lane_id}", model="S03_STRESS")
    bought_at = TRADE_DATE - timedelta(days=3) + timedelta(hours=10)
    symbols: list[str] = []
    for index in range(count):
        symbol = f"{600000 + index:06d}.SH"
        plan_id = f"stress-{count}-{index}"
        store.create_execution_plan(
            plan_id,
            lane_id,
            symbol,
            status=PlanStatus.ACTIVE_TODAY,
            valid_from=bought_at - timedelta(minutes=1),
            expires_at=TRADE_DATE.replace(hour=15),
            payload={"stop_level": 9.0},
        )
        fill = broker.apply(
            SimulationAction(
                account_id=f"paper:{lane_id}",
                signal_id=f"fill-{count}-{index}",
                symbol=symbol,
                action="BUY",
                signal_bar_end=bought_at - timedelta(minutes=1),
                entry_reference=10.0,
                stop_level=9.0,
                requested_qty=100,
                plan_id=plan_id,
            ),
            minute_bar(symbol, bought_at),
        )
        if fill.status.value != "FILLED":
            raise RuntimeError(f"fixture fill failed: {fill.reason_code}")
        symbols.append(symbol)
    return tuple(symbols)


def run_tier(root: Path, count: int) -> dict[str, object]:
    store = RuntimeStore(root / f"a4-stress-{count}.sqlite3")
    symbols = prepare(store, count)
    minutes = session_minutes()
    sampled = tuple(minutes[index] for index in (0, 23, 47, 71, 95, 120, 143, 167, 191, 239))
    engine = MonitorEngine(store)
    risk_seconds: list[float] = []
    for sequence, at in enumerate(sampled):
        bars = {symbol: minute_bar(symbol, at) for symbol in symbols}
        started = time.perf_counter()
        batch = engine.process_position_risk(
            "lane_1",
            bars,
            minute_snapshot_id=f"stress-risk-{count}-{sequence}",
            now=at,
        )
        risk_seconds.append(time.perf_counter() - started)
        if len(batch.events) != count:
            raise RuntimeError(f"risk event count mismatch: {len(batch.events)} != {count}")

    decision_at = minutes[100]
    decision_bars = {symbol: minute_bar(symbol, decision_at) for symbol in symbols}
    started = time.perf_counter()
    decision = engine.process_minute(
        "lane_1",
        decision_bars,
        minute_snapshot_id=f"stress-decision-{count}",
        now=decision_at,
    )
    decision_seconds = time.perf_counter() - started
    return {
        "plan_count": count,
        "fixture_minutes": len(minutes),
        "sampled_risk_rounds": len(sampled),
        "risk_p50_seconds": percentile(risk_seconds, 0.50),
        "risk_p99_seconds": percentile(risk_seconds, 0.99),
        "decision_core_seconds": decision_seconds,
        "decision_event_count": len(decision.events),
        "risk_target_met": percentile(risk_seconds, 0.99) <= 2.0,
        "decision_core_target_met": decision_seconds <= 47.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--tiers", nargs="+", type=int, default=(50, 100, 200))
    args = parser.parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    tiers = [run_tier(output_dir, count) for count in args.tiers]
    passed = all(row["risk_target_met"] and row["decision_core_target_met"] for row in tiers)
    payload = {
        "schema_version": "liangjian-a4-stress/1.0.0",
        "status": "PASS" if passed else "FAIL",
        "scope": "OFFLINE_WALL_CLOCK_NOT_PRODUCTION",
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "elapsed_seconds": time.perf_counter() - started,
        "tiers": tiers,
    }
    (output_dir / "a4-stress.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
