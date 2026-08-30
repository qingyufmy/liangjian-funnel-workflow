"""Isolated historical A4 control-path replay.

This module is deliberately labelled TEST_ONLY.  It can promote one persisted
A3 watch row into a counterfactual probe so the morning-review, deterministic
trigger, veto-only model boundary and paper broker can be exercised without
altering the production state database or claiming a historical recommendation.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from ..data.mootdx import MinuteBar
from ..data.tencent_minute import MarketQuote, QuoteResult
from ..reporting import atomic_write_json, atomic_write_text
from ..runtime.monitor import MonitorEngine
from ..runtime.simulation import PaperBroker, SimulationConfig
from ..runtime.state import MonitorAction, PlanStatus, RuntimeStore
from ..workflow import WorkflowApplication


SHANGHAI = ZoneInfo("Asia/Shanghai")
A4_REPLAY_SCHEMA = "liangjian-a4-replay/1.0.0"
VetoFactory = Callable[[datetime, tuple[dict[str, Any], ...], tuple[MinuteBar, ...]], Any]


def _safe_plan(raw: Mapping[str, Any], *, trade_date: date, source_run_id: str) -> dict[str, Any]:
    zone = raw.get("trigger_zone") if isinstance(raw.get("trigger_zone"), Mapping) else {}
    low = float(zone.get("low"))
    high = float(zone.get("high"))
    stop = float(raw.get("invalidation_level"))
    if not (0 < stop < low <= high):
        raise ValueError("A4_REPLAY_PLAN_PRICE_CONTRACT_INVALID")
    symbol = str(raw.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("A4_REPLAY_SYMBOL_MISSING")
    logical = hashlib.sha256(
        json.dumps(
            {"source_run_id": source_run_id, "symbol": symbol, "trade_date": trade_date.isoformat()},
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    return {
        **dict(raw),
        "plan_id": f"TEST_ONLY:A4_REPLAY:{logical}",
        "symbol": symbol,
        "trigger_low": low,
        "trigger_high": high,
        "stop_level": stop,
        "confirmation_bars": 2,
        "action": MonitorAction.BUY_SIGNAL.value,
        "risk_unit": "PROBE",
        "source_risk_unit": raw.get("risk_unit"),
        "test_only_promotion": True,
        "source_run_id": source_run_id,
    }


class _ReplayQuoteSource:
    def __init__(self, bar: MinuteBar, name: str):
        self.bar = bar
        self.name = name

    def fetch_quote(self, symbol: str, *, as_of: datetime) -> QuoteResult:
        return QuoteResult(
            symbol=symbol,
            reason_code="OK",
            complete=True,
            quote=MarketQuote(
                symbol=symbol,
                name=self.name,
                quote_time=as_of,
                price=self.bar.open,
                open=self.bar.open,
                previous_close=self.bar.open,
                volume=max(1.0, self.bar.volume),
                amount=max(self.bar.amount, self.bar.open),
                source_id="TEST_ONLY:DERIVED_AUCTION_QUOTE",
            ),
        )


def run_a4_replay(
    *,
    trade_date: date,
    source_run_id: str,
    source_plan: Mapping[str, Any],
    bars: Sequence[MinuteBar],
    state_db_path: str | Path,
    output_dir: str | Path,
    veto_factory: VetoFactory | None = None,
    model_mode: str = "DETERMINISTIC_ACCEPT",
    official_a3_plan_count: int = 0,
) -> dict[str, Any]:
    """Replay one counterfactual plan through the production A4 state machine."""

    ordered = tuple(sorted(bars, key=lambda item: item.bar_end))
    day_bars = tuple(
        bar
        for bar in ordered
        if bar.interval == "1m" and bar.bar_end.astimezone(SHANGHAI).date() == trade_date
    )
    if len(day_bars) != 240:
        raise ValueError("A4_REPLAY_REQUIRES_240_CLOSED_1M_BARS")
    if day_bars[0].bar_end.strftime("%H:%M") != "09:31" or day_bars[-1].bar_end.strftime("%H:%M") != "15:00":
        raise ValueError("A4_REPLAY_SESSION_BOUNDARY_INVALID")
    symbol = day_bars[0].symbol
    if any(bar.symbol != symbol for bar in day_bars):
        raise ValueError("A4_REPLAY_MIXED_SYMBOLS")

    output_root = Path(output_dir).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    store = RuntimeStore(Path(state_db_path))
    broker = PaperBroker(
        store,
        account_id="paper:lane_1",
        model="TEST_ONLY_A4_REPLAY",
        config=SimulationConfig(initial_cash=1_000_000),
    )
    plan = _safe_plan(source_plan, trade_date=trade_date, source_run_id=source_run_id)
    if plan["symbol"] != symbol:
        raise ValueError("A4_REPLAY_PLAN_BAR_SYMBOL_MISMATCH")
    morning = datetime.combine(trade_date, datetime.min.time(), tzinfo=SHANGHAI).replace(hour=9, minute=26)
    store.create_execution_plan(
        plan["plan_id"],
        "lane_1",
        symbol,
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=morning.replace(hour=15, minute=0),
        payload=plan,
    )
    app = SimpleNamespace(
        store=store,
        brokers={"lane_1": broker},
        market_data=_ReplayQuoteSource(day_bars[0], str(plan.get("name") or "")),
        settings=SimpleNamespace(workflow_output_dir=output_root),
        _ensure_trading_day=lambda _current: None,
    )
    morning_result = WorkflowApplication.review_pending_morning(app, now=morning)
    if morning_result.get("status") != "READY":
        raise ValueError("A4_REPLAY_MORNING_REVIEW_BLOCKED")

    simulation: list[dict[str, Any]] = []
    model_calls = 0
    for index, bar in enumerate(day_bars):
        simulation.extend(WorkflowApplication._settle_prior_signals(app, "lane_1", symbol, bar))
        active = store.list_active_plans("lane_1", at=bar.bar_end)
        callback = (
            veto_factory(bar.bar_end, active, day_bars[: index + 1])
            if veto_factory is not None
            else (lambda _context: False)
        )
        batch = MonitorEngine(store, llm_veto=callback, max_seconds=50).process_minute(
            "lane_1",
            {symbol: bar},
            minute_snapshot_id=f"TEST_ONLY:{trade_date}:{bar.bar_end.strftime('%H%M')}",
            now=bar.bar_end,
            data_ok=True,
            snapshot_contiguous=True,
        )
        model_calls += int(batch.model_called)

    event_rows = store.list_monitor_events(lane_id="lane_1")
    effective: list[dict[str, Any]] = []
    for row in event_rows:
        if not bool(row.get("effective")):
            continue
        payload = json.loads(str(row.get("payload_json") or "{}"))
        effective.append(
            {
                "minute_end": row.get("minute_end"),
                "lane_id": row.get("lane_id"),
                "plan_id": payload.get("plan_id"),
                "symbol": payload.get("symbol"),
                "name": plan.get("name"),
                "action": row.get("action"),
                "reason_code": row.get("reason_code"),
                "diagnostic_code": payload.get("diagnostic_code"),
                "llm_veto": bool(payload.get("llm_veto")),
            }
        )
    fills = [dict(item) for item in store.list_fills("paper:lane_1")]
    first_signal = next(
        (item for item in effective if item["action"] in {MonitorAction.BUY_SIGNAL.value, MonitorAction.ADD_SIGNAL.value}),
        None,
    )
    next_bar_fill = bool(
        first_signal
        and fills
        and datetime.fromisoformat(str(fills[0]["bar_end"]))
        > datetime.fromisoformat(str(first_signal["minute_end"]))
        and (
            datetime.fromisoformat(str(fills[0]["bar_end"]))
            - datetime.fromisoformat(str(first_signal["minute_end"]))
        ).total_seconds()
        == 60
    )
    action_counts = Counter(str(row.get("action") or "") for row in event_rows)
    status = "READY" if effective and (fills or effective[0]["action"] == MonitorAction.LLM_VETO.value) else "NO_EFFECTIVE_SIGNAL"
    report = {
        "schema_version": A4_REPLAY_SCHEMA,
        "status": status,
        "mode": "TEST_ONLY_COUNTERFACTUAL",
        "model_mode": model_mode,
        "trade_date": trade_date.isoformat(),
        "source_run_id": source_run_id,
        "official_a3_plan_count": int(official_a3_plan_count),
        "production_path_expected": "EMPTY_SCOPE" if official_a3_plan_count == 0 else "ACTIVE_PLAN_SCOPE",
        "test_plan": {
            "plan_id": plan["plan_id"],
            "symbol": symbol,
            "name": plan.get("name"),
            "source_pool": "secondary_watch_pool",
            "source_risk_unit": plan.get("source_risk_unit"),
            "test_risk_unit": "PROBE",
            "trigger_low": plan["trigger_low"],
            "trigger_high": plan["trigger_high"],
            "stop_level": plan["stop_level"],
            "test_only_promotion": True,
        },
        "bar_coverage": {
            "source": day_bars[0].source_id,
            "count": len(day_bars),
            "first": day_bars[0].bar_end.isoformat(),
            "last": day_bars[-1].bar_end.isoformat(),
        },
        "morning_review": morning_result,
        "model_calls": model_calls,
        "event_counts": dict(sorted(action_counts.items())),
        "effective_events": effective,
        "simulation_results": simulation,
        "fills": fills,
        "invariants": {
            "production_state_isolated": True,
            "real_trading_connected": False,
            "official_a3_zero_not_overridden": official_a3_plan_count == 0,
            "closed_1m_coverage_complete": len(day_bars) == 240,
            "llm_is_veto_only": True,
            "next_bar_fill_only": next_bar_fill if fills else True,
            "duplicate_fills": max(0, len(fills) - len({str(item["signal_id"]) for item in fills})),
        },
    }
    json_path = output_root / "a4_replay_latest.json"
    md_path = output_root / "a4_replay_latest.md"
    atomic_write_json(json_path, report)
    atomic_write_text(md_path, _markdown(report))
    return report


def _markdown(report: Mapping[str, Any]) -> str:
    plan = report["test_plan"]
    lines = [
        "# A4 盘中回放验收",
        "",
        f"> `{report['mode']}`：该结果只验证控制链路，不是历史推荐，也不连接真实交易。",
        "",
        f"- 交易日：`{report['trade_date']}`",
        f"- 来源运行：`{report['source_run_id']}`",
        f"- 测试标的：`{plan['symbol']}` {plan.get('name') or ''}",
        f"- 原始 A3 风险级别：`{plan.get('source_risk_unit')}`；测试提升：`PROBE`",
        f"- 1分钟线覆盖：`{report['bar_coverage']['count']}` 根",
        f"- 模型调用：`{report['model_calls']}` 次",
        f"- 有效事件：`{len(report['effective_events'])}` 条",
        f"- 模拟成交：`{len(report['fills'])}` 笔",
        "",
        "## 有效事件",
        "",
    ]
    if not report["effective_events"]:
        lines.append("- 无有效事件。")
    for item in report["effective_events"]:
        lines.append(
            f"- `{item['minute_end']}` | `{item['symbol']}` | `{item['action']}` | `{item['reason_code']}`"
        )
    lines.extend(["", "## 验收不变量", ""])
    for key, value in report["invariants"].items():
        lines.append(f"- `{key}`：`{value}`")
    return "\n".join(lines) + "\n"


__all__ = ["A4_REPLAY_SCHEMA", "run_a4_replay"]
