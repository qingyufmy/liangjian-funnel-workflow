#!/usr/bin/env python3
"""Replay one A3 watch row through A4 using a separate TEST_ONLY ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar, MootdxAdapter, MootdxNode
from liangjian_funnel.data.tencent_minute import ResilientIntradayAdapter, TencentIntradayAdapter
from liangjian_funnel.evaluation.a4_replay import run_a4_replay, run_a4_replay_batch
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
    parser.add_argument(
        "--batch",
        action="store_true",
        help="replay every A3 core plan; use --include-secondary to append the secondary pool",
    )
    parser.add_argument(
        "--include-secondary",
        action="store_true",
        help="with --batch, append A3 secondary_watch_pool rows",
    )
    parser.add_argument("--live-model", action="store_true", help="call the configured veto-only A4 model")
    parser.add_argument("--output-root", default="outputs/evaluation", help="evaluation output root")
    return parser


def _a3_stage(source: dict) -> dict:
    stages = source.get("stages")
    if not isinstance(stages, list):
        raise SystemExit("A4_REPLAY_SOURCE_STAGES_INVALID")
    stage = next((item for item in stages if isinstance(item, dict) and item.get("stage") == "A3"), None)
    if not isinstance(stage, dict) or not isinstance(stage.get("output"), dict):
        raise SystemExit("A4_REPLAY_A3_STAGE_NOT_FOUND")
    return stage


def _source_run_id(source: dict, source_path: Path, stages: list[dict]) -> str:
    direct = str(source.get("run_id") or source.get("source_run_id") or "").strip()
    if direct:
        return direct
    first_stage = next((item for item in stages if str(item.get("run_id") or "").strip()), {})
    return str(first_stage.get("run_id") or source_path.stem)


def _canonical_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _historical_adapter(settings: Settings) -> ResilientIntradayAdapter:
    return ResilientIntradayAdapter(
        MootdxAdapter(
            tuple(MootdxNode(host=host, port=port) for host, port in settings.mootdx_servers),
            page_size=settings.mootdx_page_size,
            max_pages=settings.mootdx_max_pages,
            timeout_seconds=settings.mootdx_timeout_seconds,
        ),
        TencentIntradayAdapter(timeout_seconds=min(12.0, settings.timeout_seconds)),
    )


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
    stages = [item for item in source.get("stages", []) if isinstance(item, dict)]
    stage_row = _a3_stage(source)
    stage = stage_row["output"]
    source_run_id = _source_run_id(source, source_path, stages)
    secondary = stage.get("secondary_watch_pool") or []
    core = stage.get("core_watch_pool") or []
    if not isinstance(core, list) or not isinstance(secondary, list):
        raise SystemExit("A4_REPLAY_A3_POOLS_INVALID")
    if args.batch:
        if args.symbol:
            requested = _canonical_symbol(args.symbol)
            core = [item for item in core if _canonical_symbol(item.get("symbol")) == requested]
            secondary = [item for item in secondary if _canonical_symbol(item.get("symbol")) == requested]
        plans = [*core]
        pools = ["core_watch_pool"] * len(core)
        if args.include_secondary:
            plans.extend(secondary)
            pools.extend(["secondary_watch_pool"] * len(secondary))
        if not plans:
            raise SystemExit("A4_REPLAY_A3_BATCH_EMPTY")
        settings = Settings.from_env(root=Path.cwd())
        adapter = _historical_adapter(settings)
        cutoff = datetime.combine(trade_date, datetime.min.time(), tzinfo=TZ).replace(hour=15)
        bars_by_symbol = {}
        data_errors = {}
        for plan in plans:
            symbol = _canonical_symbol(plan.get("symbol"))
            if not symbol or symbol in bars_by_symbol or symbol in data_errors:
                continue
            try:
                result = adapter.fetch_bars(symbol, "1m", 240, as_of=cutoff)
                if not result.complete:
                    data_errors[symbol] = str(result.reason_code or "A4_REPLAY_BARS_FETCH_FAILED")
                else:
                    bars_by_symbol[symbol] = result.bars
            except Exception as exc:
                reason = str(getattr(exc, "reason_code", "") or "").strip()
                data_errors[symbol] = reason or exc.__class__.__name__

        callback_factories = {}
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

            for plan in plans:
                symbol = _canonical_symbol(plan.get("symbol"))
                if not symbol or symbol in callback_factories:
                    continue

                def _factory(current, active_plans, history, *, _symbol=symbol):
                    five = _five_minute(history)
                    context = {
                        _symbol: _intraday_market_context(
                            _symbol,
                            tuple(history[-21:]),
                            tuple(five[-60:]),
                            current=current,
                        )
                    }
                    return WorkflowApplication._a4_callback(callback_host, f"batch_{_symbol}", active_plans, context, current)

                callback_factories[symbol] = _factory
            model_mode = "LIVE_DEEPSEEK_FLASH_VETO_ONLY"

        stamp = datetime.now(TZ).strftime("%Y%m%dT%H%M%S")
        run_dir = Path(args.output_root).resolve() / f"a4-replay-batch-{trade_date}-{stamp}-{'live' if args.live_model else 'deterministic'}"
        report = run_a4_replay_batch(
            trade_date=trade_date,
            source_run_id=source_run_id,
            source_plans=plans,
            source_pools=pools,
            bars_by_symbol=bars_by_symbol,
            data_errors=data_errors,
            state_db_path=run_dir / "state.sqlite3",
            output_dir=run_dir,
            source_hash=hashlib.sha256(source_path.read_bytes()).hexdigest(),
            veto_factories=callback_factories,
            model_mode=model_mode,
        )
        latest_root = Path(args.output_root).resolve()
        atomic_write_json(latest_root / "a4_replay_batch_latest.json", report)
        atomic_write_text(
            latest_root / "a4_replay_batch_latest.md",
            (run_dir / "a4_replay_batch_latest.md").read_text(encoding="utf-8"),
        )
        print(json.dumps(report, ensure_ascii=False))
        return 0

    if args.symbol:
        candidates = [item for item in secondary if str(item.get("symbol")) == args.symbol]
    else:
        candidates = list(secondary[:1])
    if len(candidates) != 1:
        raise SystemExit("A4_REPLAY_SOURCE_PLAN_NOT_UNIQUE")
    plan = candidates[0]
    symbol = str(plan["symbol"])
    cutoff = datetime.combine(trade_date, datetime.min.time(), tzinfo=TZ).replace(hour=15)
    settings = Settings.from_env(root=Path.cwd())
    bars_result = _historical_adapter(settings).fetch_bars(symbol, "1m", 240, as_of=cutoff)
    if not bars_result.complete:
        raise SystemExit(bars_result.reason_code)

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
        source_run_id=source_run_id,
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
