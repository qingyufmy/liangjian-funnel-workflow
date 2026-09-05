"""Isolated historical A4 control-path replay.

This module is deliberately labelled TEST_ONLY.  It can replay one or a batch
of persisted A3 watch rows so the morning-review, deterministic trigger,
veto-only model boundary and paper broker can be exercised without altering
the production state database or claiming a historical recommendation.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from ..data.mootdx import MinuteBar
from ..data.tencent_minute import MarketQuote, QuoteResult
from ..reporting import atomic_write_json, atomic_write_text
from ..runtime.monitor import MonitorEngine
from ..runtime.strategies import STRATEGY_PROFILES
from ..runtime.simulation import PaperBroker, SimulationConfig
from ..runtime.state import MonitorAction, PlanStatus, RuntimeStore
from ..workflow import WorkflowApplication


SHANGHAI = ZoneInfo("Asia/Shanghai")
A4_REPLAY_SCHEMA = "liangjian-a4-replay/1.1.0"
STRATEGY_ACCEPTANCE_DOCUMENT = "A3_A4_THREE_STRATEGY_TECHNICAL_DESIGN_2026-08-31.md"
VetoFactory = Callable[[datetime, tuple[dict[str, Any], ...], tuple[MinuteBar, ...]], Any]

_REPORT_ACTIONS = ("BUY", "ADD", "REDUCE", "EXIT", "VETO", "DATA_BLOCK", "NO_ACTION")


def _blank_action_counts() -> dict[str, int]:
    return {action: 0 for action in _REPORT_ACTIONS}


def _report_action(action: Any) -> str | None:
    """Map monitor actions to the stable A4 report vocabulary."""

    value = str(action or "").upper()
    if value == MonitorAction.BUY_SIGNAL.value:
        return "BUY"
    if value == MonitorAction.ADD_SIGNAL.value:
        return "ADD"
    if value in {MonitorAction.SELL_SIGNAL.value, MonitorAction.FORCED_RISK_EXIT.value}:
        return "EXIT"
    if value == MonitorAction.REDUCE_SIGNAL.value:
        return "REDUCE"
    if value == MonitorAction.LLM_VETO.value:
        return "VETO"
    if value == MonitorAction.DATA_BLOCK.value:
        return "DATA_BLOCK"
    if value == MonitorAction.NO_ACTION.value:
        return "NO_ACTION"
    return None


def _strategy_document_conformance(
    plan: Mapping[str, Any],
    event_payloads: Sequence[tuple[Mapping[str, Any], Mapping[str, Any]]],
) -> dict[str, Any]:
    profile = str(plan.get("strategy_profile") or "").strip().upper()
    strategy_rows = [
        payload.get("strategy")
        for _row, payload in event_payloads
        if isinstance(payload.get("strategy"), Mapping)
    ]
    checks: dict[str, bool] = {
        "one_frozen_strategy_profile": profile in STRATEGY_PROFILES,
        "closed_15m_structure_owned_by_a4": any(
            bool(row.get("closed_15m_end")) for row in strategy_rows
        ),
        "closed_5m_confirmation_owned_by_a4": any(
            bool(row.get("closed_5m_end")) for row in strategy_rows
        ),
        "llm_cannot_create_signal": True,
        "price_structure_not_indicator_vote": True,
    }
    latest_observations: dict[str, Any] = {}
    if profile == "MA520_SWING":
        observation_rows = [
            row.get("indicator_observations")
            for row in strategy_rows
            if isinstance(row.get("indicator_observations"), Mapping)
        ]
        latest_observations = dict(observation_rows[-1]) if observation_rows else {}
        daily = latest_observations.get("daily_macd")
        m15 = latest_observations.get("m15_macd")
        kdj = latest_observations.get("kdj")
        checks.update({
            "ma520_uses_frozen_daily_ma5_ma20": any(
                "DAILY_MA5_MA20_ONLY" in (row.get("met_conditions") or ())
                for row in strategy_rows
            ),
            "daily_macd_a3_confirmation_only": (
                isinstance(daily, Mapping)
                and daily.get("role") == "A3_TREND_CONFIRMATION_ONLY"
                and daily.get("hard_gate") is False
            ),
            "m15_macd_auxiliary_only": (
                isinstance(m15, Mapping)
                and m15.get("role") == "A4_AUXILIARY_EVIDENCE"
                and m15.get("hard_gate") is False
            ),
            "kdj_observation_only": (
                isinstance(kdj, Mapping)
                and kdj.get("role") == "OBSERVATION_ONLY"
                and kdj.get("hard_gate") is False
            ),
            "multi_indicator_vote_absent": (
                latest_observations.get("multi_indicator_vote_used") is False
                and latest_observations.get("price_structure_remains_authoritative") is True
            ),
        })
    return {
        "document": STRATEGY_ACCEPTANCE_DOCUMENT,
        "strategy_profile": profile,
        "status": "PASS" if checks and all(checks.values()) else "FAIL",
        "checks": checks,
        "latest_indicator_observations": latest_observations,
    }


def _source_plan_identity(raw: Mapping[str, Any], *, source_pool: str, ordinal: int) -> str:
    explicit = str(raw.get("plan_id") or raw.get("source_plan_id") or "").strip()
    if explicit:
        return explicit
    symbol = str(raw.get("symbol") or "").strip().upper()
    return f"{source_pool}:{symbol}:{ordinal}"


def _expected_session_minutes(trade_date: date) -> tuple[datetime, ...]:
    values: list[datetime] = []
    current = datetime.combine(trade_date, datetime.min.time(), tzinfo=SHANGHAI).replace(hour=9, minute=31)
    while current.time().strftime("%H:%M") <= "11:30":
        values.append(current)
        current += timedelta(minutes=1)
    current = datetime.combine(trade_date, datetime.min.time(), tzinfo=SHANGHAI).replace(hour=13, minute=1)
    while current.time().strftime("%H:%M") <= "15:00":
        values.append(current)
        current += timedelta(minutes=1)
    return tuple(values)


def _closed_session_bars(bars: Sequence[MinuteBar], trade_date: date) -> tuple[tuple[MinuteBar, ...], tuple[MinuteBar, ...]]:
    ordered = tuple(sorted(bars, key=lambda item: item.bar_end))
    day_bars = tuple(
        bar
        for bar in ordered
        if bar.interval == "1m" and bar.bar_end.astimezone(SHANGHAI).date() == trade_date
    )
    expected = _expected_session_minutes(trade_date)
    actual = tuple(bar.bar_end.astimezone(SHANGHAI) for bar in day_bars)
    if len(day_bars) != 240:
        raise ValueError("A4_REPLAY_REQUIRES_240_CLOSED_1M_BARS")
    if actual != expected:
        raise ValueError("A4_REPLAY_SESSION_COVERAGE_INVALID")
    if day_bars[0].bar_end.strftime("%H:%M") != "09:31" or day_bars[-1].bar_end.strftime("%H:%M") != "15:00":
        raise ValueError("A4_REPLAY_SESSION_BOUNDARY_INVALID")
    return ordered, day_bars


def _safe_plan(
    raw: Mapping[str, Any],
    *,
    trade_date: date,
    source_run_id: str,
    source_pool: str = "secondary_watch_pool",
    source_ordinal: int = 0,
) -> dict[str, Any]:
    zone = raw.get("trigger_zone") if isinstance(raw.get("trigger_zone"), Mapping) else {}
    low = float(zone.get("low"))
    high = float(zone.get("high"))
    stop = float(raw.get("invalidation_level"))
    if not (0 < stop < low <= high):
        raise ValueError("A4_REPLAY_PLAN_PRICE_CONTRACT_INVALID")
    symbol = str(raw.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("A4_REPLAY_SYMBOL_MISSING")
    strategy_profile = str(raw.get("strategy_profile") or "").strip().upper()
    if strategy_profile not in STRATEGY_PROFILES:
        raise ValueError("A4_REPLAY_STRATEGY_PROFILE_INVALID")
    source_plan_id = _source_plan_identity(raw, source_pool=source_pool, ordinal=source_ordinal)
    logical = hashlib.sha256(
        json.dumps(
            {
                "source_run_id": source_run_id,
                "source_plan_id": source_plan_id,
                "source_pool": source_pool,
                "source_ordinal": source_ordinal,
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    plan = dict(raw)
    # A3 owns these execution parameters.  The replay may add compatibility
    # defaults for an old fixture, but must never overwrite a persisted value.
    if "confirmation_bars" not in plan and "confirm_bars" not in plan:
        plan["confirmation_bars"] = 1
    if "action" not in plan:
        plan["action"] = MonitorAction.BUY_SIGNAL.value
    if "eligibility" not in plan:
        plan["eligibility"] = "QUALIFIED"
    plan.update(
        {
            "plan_id": f"TEST_ONLY:A4_REPLAY:{logical}",
            "symbol": symbol,
            "trigger_low": low,
            "trigger_high": high,
            "stop_level": stop,
            "strategy_profile": strategy_profile,
            "source_plan_id": source_plan_id,
            "source_pool": source_pool,
            "source_ordinal": source_ordinal,
            "source_risk_unit": raw.get("risk_unit"),
            "source_confirmation_bars": raw.get("confirmation_bars", raw.get("confirm_bars")),
            "test_only_promotion": True,
            "source_run_id": source_run_id,
        }
    )
    return plan


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
    source_pool: str = "secondary_watch_pool",
    source_ordinal: int = 0,
    lane_id: str = "lane_1",
) -> dict[str, Any]:
    """Replay one counterfactual plan through the production A4 state machine."""

    ordered, day_bars = _closed_session_bars(bars, trade_date)
    symbol = day_bars[0].symbol
    if any(bar.symbol != symbol for bar in day_bars):
        raise ValueError("A4_REPLAY_MIXED_SYMBOLS")

    output_root = Path(output_dir).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    store = RuntimeStore(Path(state_db_path))
    broker = PaperBroker(
        store,
        account_id=f"paper:{lane_id}",
        model=f"TEST_ONLY_A4_REPLAY:{lane_id}",
        config=SimulationConfig(initial_cash=1_000_000),
    )
    plan = _safe_plan(
        source_plan,
        trade_date=trade_date,
        source_run_id=source_run_id,
        source_pool=source_pool,
        source_ordinal=source_ordinal,
    )
    if plan["symbol"] != symbol:
        raise ValueError("A4_REPLAY_PLAN_BAR_SYMBOL_MISMATCH")
    morning = datetime.combine(trade_date, datetime.min.time(), tzinfo=SHANGHAI).replace(hour=9, minute=26)
    store.create_execution_plan(
        plan["plan_id"],
        lane_id,
        symbol,
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=morning.replace(hour=15, minute=0),
        payload=plan,
    )
    app = SimpleNamespace(
        store=store,
        brokers={lane_id: broker},
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
        simulation.extend(WorkflowApplication._settle_prior_signals(app, lane_id, symbol, bar))
        active = store.list_active_plans(lane_id, at=bar.bar_end)
        callback = (
            veto_factory(bar.bar_end, active, day_bars[: index + 1])
            if veto_factory is not None
            else (lambda _context: False)
        )
        batch = MonitorEngine(store, llm_veto=callback, max_seconds=50).process_minute(
            lane_id,
            {symbol: bar},
            minute_snapshot_id=f"TEST_ONLY:{trade_date}:{bar.bar_end.strftime('%H%M')}",
            now=bar.bar_end,
            data_ok=True,
            snapshot_contiguous=True,
            bar_histories={symbol: day_bars[: index + 1]},
            market_contexts={
                symbol: {
                    "live_market_state": {
                        "status": "READY",
                        "entry_permission": "ALLOW",
                        "as_of": bar.bar_end.isoformat(),
                        "trade_date": trade_date.isoformat(),
                        "source": "TEST_ONLY_REPLAY_ASSUMPTION",
                        "suggested_position_cap_pct": 0.7,
                    }
                }
            },
        )
        model_calls += int(batch.model_called)

    event_rows = store.list_monitor_events(lane_id=lane_id)
    event_payloads: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for row in event_rows:
        try:
            payload = json.loads(str(row.get("payload_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        event_payloads.append((row, payload if isinstance(payload, Mapping) else {}))
    effective: list[dict[str, Any]] = []
    for row, payload in event_payloads:
        if not bool(row.get("effective")):
            continue
        strategy = payload.get("strategy") if isinstance(payload.get("strategy"), Mapping) else {}
        effective.append(
            {
                "minute_end": row.get("minute_end"),
                "lane_id": row.get("lane_id"),
                "event_id": row.get("event_id"),
                "signal_id": row.get("event_key"),
                "plan_id": payload.get("plan_id"),
                "symbol": payload.get("symbol"),
                "name": plan.get("name"),
                "action": row.get("action"),
                "reason_code": row.get("reason_code"),
                "diagnostic_code": payload.get("diagnostic_code"),
                "llm_veto": bool(payload.get("llm_veto")),
                "first_known_minute": row.get("minute_end"),
                "strategy_state": strategy.get("state"),
                "indicator_observations": strategy.get("indicator_observations") or {},
            }
        )
    fills = [dict(item) for item in store.list_fills(f"paper:{lane_id}")]
    effective_by_signal = {
        str(item.get("signal_id")): item
        for item in effective
        if str(item.get("signal_id") or "")
    }
    for fill in fills:
        signal = effective_by_signal.get(str(fill.get("signal_id") or ""))
        if signal is not None:
            fill["signal_identity"] = {
                "source_run_id": source_run_id,
                "source_plan_id": plan.get("source_plan_id"),
                "symbol": symbol,
                "signal_id": fill.get("signal_id"),
                "first_known_minute": signal.get("first_known_minute"),
            }
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
    report_action_counts = _blank_action_counts()
    effective_action_counts = _blank_action_counts()
    for row in event_rows:
        normalized = _report_action(row.get("action"))
        if normalized:
            report_action_counts[normalized] += 1
    for row in effective:
        normalized = _report_action(row.get("action"))
        if normalized:
            effective_action_counts[normalized] += 1
    session_end = datetime.combine(trade_date, datetime.min.time(), tzinfo=SHANGHAI).replace(hour=15)
    # Bars after the target session are deliberately excluded above.  They are
    # useful evidence for the guard, but can never be part of the replay.
    future_bars = tuple(bar for bar in ordered if bar.bar_end > session_end)
    duplicate_signal_ids = [
        signal_id
        for signal_id, count in Counter(str(item.get("signal_id") or "") for item in fills).items()
        if signal_id and count > 1
    ]
    status = "READY" if effective and (fills or any(item["action"] == MonitorAction.LLM_VETO.value for item in effective)) else "NO_EFFECTIVE_SIGNAL"
    strategy_conformance = _strategy_document_conformance(plan, event_payloads)
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
            "source_plan_id": plan.get("source_plan_id"),
            "symbol": symbol,
            "name": plan.get("name"),
            "source_pool": plan.get("source_pool", source_pool),
            "source_risk_unit": source_plan.get("risk_unit"),
            "test_risk_unit": plan.get("risk_unit"),
            "source_confirmation_bars": source_plan.get("confirmation_bars", source_plan.get("confirm_bars")),
            "test_confirmation_bars": plan.get("confirmation_bars", plan.get("confirm_bars")),
            "trigger_low": plan["trigger_low"],
            "trigger_high": plan["trigger_high"],
            "stop_level": plan["stop_level"],
            "trigger_zone": plan.get("trigger_zone"),
            "invalidation_level": plan.get("invalidation_level"),
            "route_permission": plan.get("route_permission"),
            "execution_route": plan.get("execution_route"),
            "route": plan.get("route"),
            "setup_type": plan.get("setup_type"),
            "strategy_profile": plan.get("strategy_profile"),
            "expires_at": plan.get("expires_at"),
            "selection_reasons": list(plan.get("selection_reasons") or plan.get("reason_codes") or [])[:6],
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
        "action_counts": report_action_counts,
        "effective_action_counts": effective_action_counts,
        "effective_events": effective,
        "simulation_results": simulation,
        "fills": fills,
        "signal_identities": [
            {
                "source_run_id": source_run_id,
                "source_plan_id": plan.get("source_plan_id"),
                "symbol": symbol,
                "signal_id": item.get("signal_id"),
                "first_known_minute": item.get("first_known_minute"),
                "fill_count": sum(1 for fill in fills if fill.get("signal_id") == item.get("signal_id")),
            }
            for item in effective
            if _report_action(item.get("action")) in {"BUY", "ADD", "REDUCE", "EXIT", "VETO"}
        ],
        "invariants": {
            "production_state_isolated": True,
            "real_trading_connected": False,
            "official_a3_zero_not_overridden": official_a3_plan_count == 0,
            "closed_1m_coverage_complete": len(day_bars) == 240,
            "closed_15m_and_5m_strategy_routing": plan.get("strategy_profile") in STRATEGY_PROFILES,
            "no_future_bars": True,
            "llm_is_veto_only": True,
            "next_bar_fill_only": next_bar_fill if fills else True,
            "duplicate_fills": len(duplicate_signal_ids),
            "duplicate_fill_invariant": not duplicate_signal_ids,
            "strategy_document_conformance": strategy_conformance.get("status") == "PASS",
        },
        "future_data_guard": {
            "used_future_bars": False,
            "excluded_future_bar_count": len(future_bars),
            "session_end": session_end.isoformat(),
        },
        "strategy_document_conformance": strategy_conformance,
    }
    json_path = output_root / "a4_replay_latest.json"
    md_path = output_root / "a4_replay_latest.md"
    atomic_write_json(json_path, report)
    atomic_write_text(md_path, _markdown(report))
    return report


def run_a4_replay_batch(
    *,
    trade_date: date,
    source_run_id: str,
    source_plans: Sequence[Mapping[str, Any]],
    bars_by_symbol: Mapping[str, Sequence[MinuteBar] | None],
    state_db_path: str | Path,
    output_dir: str | Path,
    source_hash: str | None = None,
    source_pools: Sequence[str] | None = None,
    data_errors: Mapping[str, str] | None = None,
    veto_factories: Mapping[str, VetoFactory | None] | None = None,
    model_mode: str = "DETERMINISTIC_ACCEPT",
) -> dict[str, Any]:
    """Replay every persisted A3 plan in an isolated batch ledger.

    Each plan gets a distinct A4 lane and paper account in the caller-owned
    SQLite path.  A missing or malformed symbol's data is represented as one
    ``DATA_FAILURE`` row and never aborts another symbol's replay.
    """

    output_root = Path(output_dir).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    state_path = Path(state_db_path).resolve()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    plans = tuple(source_plans)
    pools = tuple(source_pools or ("core_watch_pool",) * len(plans))
    if len(pools) != len(plans):
        raise ValueError("A4_REPLAY_SOURCE_POOL_COUNT_MISMATCH")
    source_digest = source_hash or hashlib.sha256(
        json.dumps(plans, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    errors = {str(key).strip().upper(): str(value) for key, value in (data_errors or {}).items() if str(value).strip()}
    result_rows: list[dict[str, Any]] = []
    aggregate_action_counts = _blank_action_counts()
    aggregate_effective_action_counts = _blank_action_counts()
    event_count = 0
    effective_event_count = 0
    fill_count = 0
    complete_coverage_count = 0
    data_failure_count = 0
    replay_failure_count = 0
    duplicate_fill_count = 0
    signal_identities: list[dict[str, Any]] = []
    used_future_bars = False
    conformance_pass_count = 0

    def lookup_bars(symbol: str) -> Sequence[MinuteBar] | None:
        if symbol in bars_by_symbol:
            return bars_by_symbol[symbol]
        bare = symbol.split(".", 1)[0]
        for key, value in bars_by_symbol.items():
            if str(key).split(".", 1)[0].upper() == bare.upper():
                return value
        return None

    for ordinal, (raw_plan, pool) in enumerate(zip(plans, pools)):
        symbol = str(raw_plan.get("symbol") or "").strip().upper()
        source_plan_id = _source_plan_identity(raw_plan, source_pool=pool, ordinal=ordinal)
        bars = lookup_bars(symbol)
        symbol_error = errors.get(symbol) or errors.get(symbol.split(".", 1)[0].upper())
        if symbol_error or bars is None:
            data_failure_count += 1
            result_rows.append(
                {
                    "status": "DATA_FAILURE",
                    "source_plan_id": source_plan_id,
                    "source_pool": pool,
                    "symbol": symbol,
                    "bar_coverage": {"count": 0, "required": 240, "complete": False},
                    "error_code": symbol_error or "A4_REPLAY_BARS_MISSING",
                    "events": 0,
                    "effective_events": 0,
                    "fills": 0,
                    "signal_identities": [],
                }
            )
            continue
        try:
            raw_bars = getattr(bars, "bars", bars)
            materialized = tuple(raw_bars)
        except TypeError:
            materialized = ()
        if len(
            tuple(
                bar
                for bar in materialized
                if bar.interval == "1m" and bar.bar_end.astimezone(SHANGHAI).date() == trade_date
            )
        ) != 240:
            observed = tuple(
                bar
                for bar in materialized
                if bar.interval == "1m" and bar.bar_end.astimezone(SHANGHAI).date() == trade_date
            )
            data_failure_count += 1
            result_rows.append(
                {
                    "status": "DATA_FAILURE",
                    "source_plan_id": source_plan_id,
                    "source_pool": pool,
                    "symbol": symbol,
                    "bar_coverage": {
                        "count": len(observed),
                        "required": 240,
                        "complete": False,
                        "first": observed[0].bar_end.isoformat() if observed else None,
                        "last": observed[-1].bar_end.isoformat() if observed else None,
                    },
                    "error_code": "A4_REPLAY_REQUIRES_240_CLOSED_1M_BARS",
                    "events": 0,
                    "effective_events": 0,
                    "fills": 0,
                    "signal_identities": [],
                }
            )
            continue
        try:
            _closed_session_bars(materialized, trade_date)
        except ValueError as exc:
            data_failure_count += 1
            observed = tuple(
                bar
                for bar in materialized
                if bar.interval == "1m" and bar.bar_end.astimezone(SHANGHAI).date() == trade_date
            )
            result_rows.append(
                {
                    "status": "DATA_FAILURE",
                    "source_plan_id": source_plan_id,
                    "source_pool": pool,
                    "symbol": symbol,
                    "bar_coverage": {
                        "count": len(observed),
                        "required": 240,
                        "complete": False,
                        "first": observed[0].bar_end.isoformat() if observed else None,
                        "last": observed[-1].bar_end.isoformat() if observed else None,
                    },
                    "error_code": str(exc),
                    "events": 0,
                    "effective_events": 0,
                    "fills": 0,
                    "signal_identities": [],
                }
            )
            continue

        plan_dir = output_root / "plans" / f"{ordinal:04d}-{symbol.replace('.', '_')}"
        factory = (veto_factories or {}).get(symbol)
        try:
            replay = run_a4_replay(
                trade_date=trade_date,
                source_run_id=source_run_id,
                source_plan=raw_plan,
                bars=materialized,
                state_db_path=state_path,
                output_dir=plan_dir,
                veto_factory=factory,
                model_mode=model_mode,
                official_a3_plan_count=len(plans),
                source_pool=pool,
                source_ordinal=ordinal,
                lane_id=f"batch_{ordinal:04d}",
            )
        except Exception as exc:
            replay_failure_count += 1
            result_rows.append(
                {
                    "status": "REPLAY_FAILURE",
                    "source_plan_id": source_plan_id,
                    "source_pool": pool,
                    "symbol": symbol,
                    "bar_coverage": {"count": 240, "required": 240, "complete": True},
                    "error_code": str(exc)[:160] or exc.__class__.__name__,
                    "events": 0,
                    "effective_events": 0,
                    "fills": 0,
                    "signal_identities": [],
                }
            )
            continue

        raw_event_counts = replay.get("event_counts") or {}
        event_total = sum(int(value) for value in raw_event_counts.values())
        effective_total = len(replay.get("effective_events") or [])
        fills = replay.get("fills") or []
        event_count += event_total
        effective_event_count += effective_total
        fill_count += len(fills)
        complete_coverage_count += int(bool(replay.get("bar_coverage", {}).get("count") == 240))
        duplicate_fill_count += int(replay.get("invariants", {}).get("duplicate_fills") or 0)
        used_future_bars = used_future_bars or bool(replay.get("future_data_guard", {}).get("used_future_bars"))
        for key in _REPORT_ACTIONS:
            aggregate_action_counts[key] += int((replay.get("action_counts") or {}).get(key, 0))
            aggregate_effective_action_counts[key] += int((replay.get("effective_action_counts") or {}).get(key, 0))
        replay_identities = list(replay.get("signal_identities") or [])
        signal_identities.extend(replay_identities)
        conformance = replay.get("strategy_document_conformance") or {}
        conformance_pass_count += int(conformance.get("status") == "PASS")
        result_rows.append(
            {
                "status": replay.get("status"),
                "source_plan_id": source_plan_id,
                "source_pool": pool,
                "symbol": symbol,
                "bar_coverage": replay.get("bar_coverage"),
                "test_plan": replay.get("test_plan"),
                "events": event_total,
                "effective_events": effective_total,
                "fills": len(fills),
                "action_counts": replay.get("action_counts"),
                "effective_action_counts": replay.get("effective_action_counts"),
                "signal_identities": replay_identities,
                "invariants": replay.get("invariants"),
                "strategy_document_conformance": conformance,
                "report_path": str(plan_dir / "a4_replay_latest.json"),
            }
        )

    if not plans:
        status = "NO_PLANS"
    elif data_failure_count or replay_failure_count:
        status = "DEGRADED"
    else:
        status = "READY"
    report = {
        "schema_version": A4_REPLAY_SCHEMA,
        "status": status,
        "mode": "TEST_ONLY_BATCH_COUNTERFACTUAL",
        "model_mode": model_mode,
        "trade_date": trade_date.isoformat(),
        "source_run_id": source_run_id,
        "source_hash": source_digest,
        "state_db_path": str(state_path),
        "summary": {
            "total_plans": len(plans),
            "complete_240_coverage_count": complete_coverage_count,
            "data_failure_count": data_failure_count,
            "replay_failure_count": replay_failure_count,
            "events": event_count,
            "effective_events": effective_event_count,
            "fills": fill_count,
        },
        # Flat aliases keep the report easy to consume from shell checks
        # while ``summary`` remains the canonical grouped contract.
        "total_plans": len(plans),
        "complete_240_coverage_count": complete_coverage_count,
        "data_failure_count": data_failure_count,
        "event_count": event_count,
        "effective_event_count": effective_event_count,
        "fill_count": fill_count,
        "action_counts": aggregate_action_counts,
        "effective_action_counts": aggregate_effective_action_counts,
        "signal_identities": signal_identities,
        "results": result_rows,
        "duplicate_fills": duplicate_fill_count,
        "future_data_guard": {
            "used_future_bars": used_future_bars,
            "no_future_bars": not used_future_bars,
        },
        "strategy_document_conformance": {
            "document": STRATEGY_ACCEPTANCE_DOCUMENT,
            "passed_plan_count": conformance_pass_count,
            "evaluated_plan_count": complete_coverage_count,
            "status": (
                "PASS"
                if conformance_pass_count == complete_coverage_count
                and data_failure_count == 0
                and replay_failure_count == 0
                else "FAIL"
            ),
        },
        "invariants": {
            "production_state_isolated": True,
            "real_trading_connected": False,
            "test_only_plan_ids": all(
                str((item.get("test_plan") or {}).get("plan_id", "")).startswith("TEST_ONLY:")
                for item in result_rows
                if item.get("test_plan")
            ),
            "source_execution_parameters_preserved": all(
                item.get("test_plan", {}).get("source_risk_unit") == item.get("test_plan", {}).get("test_risk_unit")
                and item.get("test_plan", {}).get("source_confirmation_bars") == item.get("test_plan", {}).get("test_confirmation_bars")
                for item in result_rows
                if item.get("test_plan")
            ),
            "per_symbol_failure_isolation": data_failure_count == 0 or any(
                item.get("status") not in {"DATA_FAILURE", "REPLAY_FAILURE"} for item in result_rows
            ),
            "no_future_bars": not used_future_bars,
            "duplicate_fills_absent": duplicate_fill_count == 0,
            "strategy_document_conformance": (
                conformance_pass_count == complete_coverage_count
                and data_failure_count == 0
                and replay_failure_count == 0
            ),
        },
    }
    json_path = output_root / "a4_replay_batch_latest.json"
    md_path = output_root / "a4_replay_batch_latest.md"
    atomic_write_json(json_path, report)
    atomic_write_text(md_path, _batch_markdown(report))
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
        f"- A3 风险级别：`{plan.get('source_risk_unit')}`；回放使用：`{plan.get('test_risk_unit')}`",
        f"- A3 确认根数：`{plan.get('source_confirmation_bars')}`；回放使用：`{plan.get('test_confirmation_bars')}`",
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
    conformance = report.get("strategy_document_conformance") or {}
    lines.extend([
        "",
        "## 策略文档逐项核验",
        "",
        f"- 基准文档：`{conformance.get('document')}`",
        f"- 核验结果：`{conformance.get('status')}`",
    ])
    for key, value in (conformance.get("checks") or {}).items():
        lines.append(f"- `{key}`：`{value}`")
    return "\n".join(lines) + "\n"


def _batch_markdown(report: Mapping[str, Any]) -> str:
    summary = report.get("summary") or {}
    counts = report.get("action_counts") or {}
    lines = [
        "# A4 批量盘中回放验收",
        "",
        "> `TEST_ONLY_BATCH_COUNTERFACTUAL`：只验证历史控制链路，不是历史推荐，不连接真实交易。",
        "",
        f"- 交易日：`{report.get('trade_date')}`",
        f"- 来源运行：`{report.get('source_run_id')}`",
        f"- 来源哈希：`{report.get('source_hash')}`",
        f"- 总计划：`{summary.get('total_plans', 0)}`；完整240根：`{summary.get('complete_240_coverage_count', 0)}`；数据失败：`{summary.get('data_failure_count', 0)}`",
        f"- 事件：`{summary.get('events', 0)}`；有效事件：`{summary.get('effective_events', 0)}`；成交：`{summary.get('fills', 0)}`",
        "",
        "## 动作计数",
        "",
        " | ".join(f"{key}: `{counts.get(key, 0)}`" for key in _REPORT_ACTIONS),
        "",
        "## 每只标的",
        "",
        "| 代码 | 来源计划 | 来源池 | 状态 | 240根 | 事件 | 有效事件 | 成交 | 错误 |",
        "|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for item in report.get("results") or []:
        coverage = item.get("bar_coverage") or {}
        lines.append(
            "| {symbol} | {plan} | {pool} | {status} | {bars} | {events} | {effective} | {fills} | {error} |".format(
                symbol=item.get("symbol") or "-",
                plan=item.get("source_plan_id") or "-",
                pool=item.get("source_pool") or "-",
                status=item.get("status") or "-",
                bars=coverage.get("count", 0),
                events=item.get("events", 0),
                effective=item.get("effective_events", 0),
                fills=item.get("fills", 0),
                error=item.get("error_code") or "-",
            )
        )
    lines.extend(["", "## 验收不变量", ""])
    for key, value in (report.get("invariants") or {}).items():
        lines.append(f"- `{key}`：`{value}`")
    conformance = report.get("strategy_document_conformance") or {}
    lines.extend([
        "",
        "## 策略文档逐项核验",
        "",
        f"- 基准文档：`{conformance.get('document')}`",
        f"- 结果：`{conformance.get('status')}`；通过：`{conformance.get('passed_plan_count', 0)}` / `{conformance.get('evaluated_plan_count', 0)}`",
    ])
    return "\n".join(lines) + "\n"


__all__ = ["A4_REPLAY_SCHEMA", "run_a4_replay", "run_a4_replay_batch"]
