#!/usr/bin/env python3
"""Replay one A3 watch row through A4 using a separate TEST_ONLY ledger."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter
from liangjian_funnel.evaluation.a4_replay import run_a4_replay
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.reporting import atomic_write_json, atomic_write_text
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication, _intraday_market_context


TZ = ZoneInfo("Asia/Shanghai")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True, help="YYYY-MM-DD replay date")
    parser.add_argument("--source", required=True, help="persisted primary-lane research JSON")
    parser.add_argument("--symbol", default=None, help="secondary-watch symbol; defaults to first row")
    parser.add_argument("--live-model", action="store_true", help="call the configured veto-only A4 model")
    parser.add_argument("--output-root", default="outputs/evaluation", help="evaluation output root")
    return parser


def _five_minute(history: tuple[MinuteBar, ...]) -> tuple[MinuteBar, ...]:
    result: list[MinuteBar] = []
    pending: list[MinuteBar] = []
    for bar in history:
        pending.append(bar)
        clock = bar.bar_end.time().replace(tzinfo=None)
        if bar.bar_end.minute % 5 != 0:
            continue
        group = pending[-5:]
        if len(group) != 5 or any(
            (right.bar_end - left.bar_end).total_seconds() != 60
            for left, right in zip(group, group[1:])
        ):
            continue
        result.append(
            MinuteBar(
                symbol=bar.symbol,
                interval="5m",
                bar_end=bar.bar_end,
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
                volume=sum(item.volume for item in group),
                amount=sum(item.amount for item in group),
                source_id="TEST_ONLY:TENCENT_1M_AGGREGATED_5M",
            )
        )
        if clock.strftime("%H:%M") == "11:30":
            pending.clear()
    return tuple(result)


def main() -> int:
    args = _parser().parse_args()
    trade_date = date.fromisoformat(args.trade_date)
    source_path = Path(args.source).resolve()
    source = json.loads(source_path.read_text(encoding="utf-8"))
    stage = source["stages"][2]["output"]
    secondary = stage.get("secondary_watch_pool") or []
    if args.symbol:
        candidates = [item for item in secondary if str(item.get("symbol")) == args.symbol]
    else:
        candidates = list(secondary[:1])
    if len(candidates) != 1:
        raise SystemExit("A4_REPLAY_SOURCE_PLAN_NOT_UNIQUE")
    plan = candidates[0]
    symbol = str(plan["symbol"])
    cutoff = datetime.combine(trade_date, datetime.min.time(), tzinfo=TZ).replace(hour=15)
    bars_result = TencentIntradayAdapter().fetch_bars(symbol, "1m", 240, as_of=cutoff)
    if not bars_result.complete:
        raise SystemExit(bars_result.reason_code)

    settings = Settings.from_env(root=Path.cwd())
    callback_factory = None
    model_mode = "DETERMINISTIC_ACCEPT"
    if args.live_model:
        callback_host = SimpleNamespace(
            prompts=PromptRepository(settings.prompt_dir),
            monitor_model_client=OpenAICompatibleModelClient(
                settings.model_copy(
                    update={
                        "model_timeout_seconds": 45.0,
                        "model_max_output_tokens": 2_048,
                        "model_fallback_output_tokens": 1_024,
                        "model_secondary_fallback_output_tokens": 1_024,
                    }
                ),
                max_attempts=2,
                thinking_enabled=False,
            ),
            settings=settings,
        )

        def callback_factory(current, plans, history):
            five = _five_minute(history)
            context = {
                symbol: _intraday_market_context(
                    symbol,
                    tuple(history[-21:]),
                    tuple(five[-60:]),
                    current=current,
                )
            }
            return WorkflowApplication._a4_callback(callback_host, "lane_1", plans, context, current)

        model_mode = "LIVE_DEEPSEEK_FLASH_VETO_ONLY"

    stamp = datetime.now(TZ).strftime("%Y%m%dT%H%M%S")
    run_dir = Path(args.output_root).resolve() / f"a4-replay-{trade_date}-{stamp}-{'live' if args.live_model else 'deterministic'}"
    report = run_a4_replay(
        trade_date=trade_date,
        source_run_id=str(source.get("stages", [{}])[0].get("run_id") or source_path.stem),
        source_plan=plan,
        bars=bars_result.bars,
        state_db_path=run_dir / "state.sqlite3",
        output_dir=run_dir,
        veto_factory=callback_factory,
        model_mode=model_mode,
        official_a3_plan_count=len(stage.get("core_watch_pool") or []),
    )
    latest_root = Path(args.output_root).resolve()
    atomic_write_json(latest_root / "a4_replay_latest.json", report)
    atomic_write_text(
        latest_root / "a4_replay_latest.md",
        (run_dir / "a4_replay_latest.md").read_text(encoding="utf-8"),
    )
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
