"""Deterministic A4 monitor with an injected, veto-only LLM callback."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from ..data.mootdx import MinuteBar
from ..reporting import atomic_write_text
from .strategies import STRATEGY_PROFILES, evaluate_strategy
from .state import EFFECTIVE_ACTIONS, MonitorAction, PersistenceError, RuntimeStore


class MonitorEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    lane_id: str
    plan_id: str | None = None
    symbol: str | None = None
    minute_end: datetime
    action: str
    reason_code: str
    effective: bool = False
    llm_veto: bool = False
    llm_reason_code: str | None = None


class MonitorBatchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    lane_id: str
    minute_snapshot_id: str
    events: tuple[MonitorEvent, ...] = ()
    model_called: bool = False
    blocked: bool = False


VetoCallback = Callable[[Mapping[str, Any]], Any]


def _local(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("monitor timestamp must be timezone-aware")
    return value.astimezone(ZoneInfo("Asia/Shanghai"))


class MonitorEngine:
    """Run one isolated lane per call; the callback cannot create a trigger."""

    def __init__(
        self,
        store: RuntimeStore,
        *,
        llm_veto: VetoCallback | None = None,
        llm_batch: VetoCallback | None = None,
        effective_md_path: str | Path | None = None,
        max_seconds: float = 50.0,
    ):
        if max_seconds <= 0:
            raise ValueError("max_seconds must be positive")
        self.store = store
        if llm_veto is not None and llm_batch is not None:
            raise ValueError("provide only one LLM batch callback")
        self.llm_veto = llm_veto or llm_batch
        self.effective_md_path = Path(effective_md_path) if effective_md_path is not None else None
        self.max_seconds = max_seconds
        self._confirmations: dict[tuple[str, str], tuple[datetime, int]] = {}
        self._condition_active: set[tuple[str, str]] = set()
        self._overrun_until: dict[str, datetime] = {}

    def process_minute(
        self,
        lane_id: str,
        bars: Mapping[str, MinuteBar] | tuple[MinuteBar, ...] | list[MinuteBar],
        *,
        minute_snapshot_id: str,
        now: datetime | None = None,
        data_ok: bool = True,
        data_errors: Mapping[str, str] | None = None,
        gap_detected: bool = False,
        snapshot_contiguous: bool = True,
        gap: bool | None = None,
        bar_histories: Mapping[str, tuple[MinuteBar, ...] | list[MinuteBar]] | None = None,
        market_contexts: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> MonitorBatchResult:
        """Process one frozen minute; no historical catch-up is performed."""

        if not isinstance(bars, Mapping) and not isinstance(bars, (tuple, list)):
            bars = tuple(bars)
        if now is None:
            if isinstance(bars, Mapping) and bars:
                now = next(iter(bars.values())).bar_end
            elif bars:
                now = next(iter(bars)).bar_end
            else:
                raise ValueError("minute timestamp is required when bars are empty")
        minute = _local(now)
        if minute is None:
            raise ValueError("minute timestamp is required when bars are empty")
        bars_by_symbol = self._bar_map(bars)
        symbol_data_errors = {
            str(symbol): str(reason)
            for symbol, reason in (data_errors or {}).items()
            if str(reason).strip()
        }
        if gap is not None:
            gap_detected = gap
        plans = list(self.store.list_active_plans(lane_id, at=minute))
        # A real virtual position remains in the risk lane after its one-day
        # A3 entry plan expires.  Recover the frozen source plan so the same
        # type-specific exit rules continue to apply on later T+1 sessions;
        # a stop-only synthetic row would silently discard the 520/trend/
        # leader route.  Presence of another active plan must not hide such a
        # position, so merge by symbol instead of using an all-or-nothing
        # fallback.
        scoped_symbols = {str(plan["symbol"]) for plan in plans}
        for position in self.store.list_positions(f"paper:{lane_id}"):
            symbol = str(position["symbol"])
            if symbol in scoped_symbols:
                continue
            source_plan_id = str(position.get("plan_id") or "").strip()
            source_plan = self.store.get_execution_plan(source_plan_id) if source_plan_id else None
            if source_plan is not None:
                try:
                    source_payload = json.loads(str(source_plan.get("payload_json") or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    source_payload = {}
                payload = dict(source_payload) if isinstance(source_payload, Mapping) else {}
                plan_id = source_plan_id
            else:
                payload = {}
                plan_id = f"position:{lane_id}:{symbol}"
            payload["stop_level"] = position.get("stop_level") or payload.get("stop_level")
            plans.append(
                {
                    "plan_id": plan_id,
                    "lane_id": lane_id,
                    "symbol": symbol,
                    "payload_json": json.dumps(payload, ensure_ascii=False, default=str),
                }
            )
            scoped_symbols.add(symbol)
        plans = tuple(plans)
        events: list[MonitorEvent] = []
        durable_events = self.store.list_monitor_events(lane_id=lane_id)
        if not self._in_session(minute):
            return MonitorBatchResult(lane_id=lane_id, minute_snapshot_id=minute_snapshot_id, events=(), model_called=False)

        if lane_id in self._overrun_until and minute <= self._overrun_until[lane_id]:
            event = self._emit_internal(
                lane_id,
                minute,
                minute_snapshot_id,
                MonitorAction.MONITOR_OVERRUN.value,
                "MONITOR_OVERRUN",
            )
            events.append(event)
            return MonitorBatchResult(lane_id=lane_id, minute_snapshot_id=minute_snapshot_id, events=tuple(events), blocked=True)

        if not plans:
            event = self._emit_internal(lane_id, minute, minute_snapshot_id, MonitorAction.EMPTY_SCOPE.value, "EMPTY_SCOPE")
            events.append(event)
            return MonitorBatchResult(lane_id=lane_id, minute_snapshot_id=minute_snapshot_id, events=tuple(events))

        # A missing/gapped bar is normally a symbol-level issue and is passed
        # through ``data_errors`` with ``data_ok=True``.  ``data_ok=False``
        # or a non-contiguous snapshot is reserved for a system-wide
        # persistence/integrity failure and blocks every plan in the lane.
        global_data_reason = None
        if not data_ok or not snapshot_contiguous or gap_detected:
            global_data_reason = "MINUTE_DATA_GAP" if gap_detected else "MINUTE_DATA_UNAVAILABLE"

        model_called = False
        started = time.monotonic()
        pending_veto: list[dict[str, Any]] = []
        trigger_results: list[dict[str, Any]] = []
        for plan in plans:
            plan_id = str(plan["plan_id"])
            symbol = str(plan["symbol"])
            symbol_reason = symbol_data_errors.get(symbol) or symbol_data_errors.get(symbol.split(".")[0])
            # If a map is present, it is a complete per-symbol readiness
            # projection.  Only the named plan is blocked; other symbols can
            # continue through deterministic trigger evaluation.
            # ``data_ok=False`` / a non-contiguous snapshot is a lane-level
            # persistence or snapshot-integrity failure and must fail closed
            # for every plan.  A healthy lane may still carry a per-symbol
            # ``data_errors`` map; those entries block only their own plan.
            if global_data_reason or symbol_reason:
                self._reset_confirmation(lane_id, plan_id)
                reason = symbol_reason or global_data_reason or "MINUTE_DATA_UNAVAILABLE"
                trigger_results.append(
                    {
                        "plan_id": plan_id,
                        "symbol": symbol,
                        "trigger_pass": False,
                        "eligible": False,
                        "action_candidate": MonitorAction.DATA_BLOCK.value,
                    }
                )
                events.append(
                    self._emit_effective(
                        lane_id,
                        plan,
                        minute,
                        minute_snapshot_id,
                        MonitorAction.DATA_BLOCK.value,
                        reason,
                    )
                )
                continue
            bar = bars_by_symbol.get(symbol) or bars_by_symbol.get(symbol.split(".")[0])
            if bar is None:
                self._reset_confirmation(lane_id, plan_id)
                trigger_results.append({"plan_id": plan_id, "symbol": symbol, "trigger_pass": False, "eligible": False, "action_candidate": "DATA_BLOCK"})
                events.append(
                    self._emit_effective(
                        lane_id,
                        plan,
                        minute,
                        minute_snapshot_id,
                        MonitorAction.DATA_BLOCK.value,
                        "BAR_MISSING",
                    )
                )
                continue
            if bar.interval != "1m" or bar.bar_end != minute:
                self._reset_confirmation(lane_id, plan_id)
                trigger_results.append({"plan_id": plan_id, "symbol": symbol, "trigger_pass": False, "eligible": False, "action_candidate": "DATA_BLOCK"})
                events.append(self._emit_effective(lane_id, plan, minute, minute_snapshot_id, MonitorAction.DATA_BLOCK.value, "BAR_NOT_CURRENT_1M"))
                continue
            payload = self._payload(plan)
            position = self.store.get_position(f"paper:{lane_id}", symbol)
            if (payload.get("plan_invalidated") or payload.get("invalidated")) and position is None:
                self._reset_confirmation(lane_id, plan_id)
                trigger_results.append({"plan_id": plan_id, "symbol": symbol, "trigger_pass": False, "eligible": False, "action_candidate": MonitorAction.PLAN_INVALIDATED.value})
                events.append(
                    self._emit_effective(
                        lane_id,
                        plan,
                        minute,
                        minute_snapshot_id,
                        MonitorAction.PLAN_INVALIDATED.value,
                        "PLAN_INVALIDATED",
                    )
                )
                continue
            # Hard risk exits are deterministic and cannot be vetoed by LLM.
            stop_level = payload.get("stop_level")
            if position and stop_level is not None and bar.low <= float(stop_level):
                self._reset_confirmation(lane_id, plan_id)
                trigger_results.append({"plan_id": plan_id, "symbol": symbol, "trigger_pass": True, "eligible": False, "action_candidate": MonitorAction.FORCED_RISK_EXIT.value})
                events.append(
                    self._emit_effective(
                        lane_id,
                        plan,
                        minute,
                        minute_snapshot_id,
                        MonitorAction.FORCED_RISK_EXIT.value,
                        "HARD_STOP",
                    )
                )
                continue

            strategy_profile = str(payload.get("strategy_profile") or "").strip().upper()
            if strategy_profile in STRATEGY_PROFILES:
                history_map = bar_histories or {}
                history = (
                    history_map.get(symbol)
                    or history_map.get(symbol.split(".")[0])
                    or [bar]
                )
                context_map = market_contexts or {}
                strategy_result = evaluate_strategy(
                    payload,
                    history,
                    now=minute,
                    position=position,
                    market_context=(
                        context_map.get(symbol)
                        or context_map.get(symbol.split(".")[0])
                    ),
                ).model_dump(mode="json")
                action = str(strategy_result.get("action") or MonitorAction.NO_ACTION.value)
                reason_codes = strategy_result.get("reason_codes")
                reason = (
                    str(reason_codes[0])
                    if isinstance(reason_codes, (list, tuple)) and reason_codes
                    else "STRATEGY_WAITING"
                )
                key = (lane_id, plan_id)
                trigger_record = {
                    "plan_id": plan_id,
                    "symbol": symbol,
                    "strategy_profile": strategy_profile,
                    "strategy_state": strategy_result.get("state"),
                    "action_candidate": action,
                    "reason_codes": reason_codes or [],
                    "met_conditions": strategy_result.get("met_conditions") or [],
                    "unmet_conditions": strategy_result.get("unmet_conditions") or [],
                    "veto_conditions": strategy_result.get("veto_conditions") or [],
                    "closed_5m_end": strategy_result.get("closed_5m_end"),
                    "closed_15m_end": strategy_result.get("closed_15m_end"),
                    "live_entry_price": strategy_result.get("live_entry_price"),
                    "live_stop_distance_pct": strategy_result.get("live_stop_distance_pct"),
                    "live_reward_risk": strategy_result.get("live_reward_risk"),
                    "minimum_reward_risk": strategy_result.get("minimum_reward_risk"),
                    "maximum_stop_distance_pct": strategy_result.get("maximum_stop_distance_pct"),
                    "trigger_pass": action in {
                        MonitorAction.BUY_SIGNAL.value,
                        MonitorAction.ADD_SIGNAL.value,
                        MonitorAction.SELL_SIGNAL.value,
                        MonitorAction.REDUCE_SIGNAL.value,
                        MonitorAction.FORCED_RISK_EXIT.value,
                    },
                    "eligible": action in {
                        MonitorAction.BUY_SIGNAL.value,
                        MonitorAction.ADD_SIGNAL.value,
                    },
                }
                trigger_results.append(trigger_record)
                if action == MonitorAction.DATA_BLOCK.value:
                    self._reset_confirmation(lane_id, plan_id)
                    if reason in {"NO_CLOSED_5M", "NO_CLOSED_15M"}:
                        events.append(self._emit_internal(
                            lane_id, minute, minute_snapshot_id,
                            MonitorAction.START_CONFIRMATION.value,
                            "A4_SESSION_WARMUP",
                            plan_id, symbol, strategy_result=strategy_result,
                        ))
                        continue
                    events.append(self._emit_effective(
                        lane_id, plan, minute, minute_snapshot_id,
                        MonitorAction.DATA_BLOCK.value, reason,
                        strategy_result=strategy_result,
                    ))
                    continue
                if str(strategy_result.get("state") or "") == "PLAN_INVALIDATED":
                    self._reset_confirmation(lane_id, plan_id)
                    events.append(self._emit_effective(
                        lane_id, plan, minute, minute_snapshot_id,
                        MonitorAction.PLAN_INVALIDATED.value, reason,
                        strategy_result=strategy_result,
                    ))
                    continue
                if action in {MonitorAction.NO_ACTION.value, MonitorAction.START_CONFIRMATION.value}:
                    self._reset_confirmation(lane_id, plan_id)
                    events.append(self._emit_internal(
                        lane_id, minute, minute_snapshot_id, action, reason,
                        plan_id, symbol, strategy_result=strategy_result,
                    ))
                    continue
                if self._resolved_trigger_episode(
                    durable_events,
                    plan_id=plan_id,
                    minute=minute,
                    action=action,
                ) or key in self._condition_active:
                    events.append(self._emit_internal(
                        lane_id, minute, minute_snapshot_id,
                        MonitorAction.NO_ACTION.value, "SIGNAL_ALREADY_EMITTED",
                        plan_id, symbol, strategy_result=strategy_result,
                    ))
                    continue
                if action == MonitorAction.FORCED_RISK_EXIT.value:
                    if position is None:
                        # A forced exit without a position is a strategy
                        # contract violation, never evidence that the A3 plan
                        # itself became invalid.  Persist a data block so the
                        # bad evaluator result is observable without emitting
                        # a false terminal trading event.
                        self._reset_confirmation(lane_id, plan_id)
                        events.append(self._emit_effective(
                            lane_id, plan, minute, minute_snapshot_id,
                            MonitorAction.DATA_BLOCK.value,
                            "A4_FORCED_EXIT_WITHOUT_POSITION",
                            strategy_result=strategy_result,
                        ))
                        continue
                    self._condition_active.add(key)
                    events.append(self._emit_effective(
                        lane_id, plan, minute, minute_snapshot_id,
                        action, reason, strategy_result=strategy_result,
                    ))
                    continue
                if action in {MonitorAction.SELL_SIGNAL.value, MonitorAction.REDUCE_SIGNAL.value}:
                    if position is None:
                        events.append(self._emit_internal(
                            lane_id, minute, minute_snapshot_id,
                            MonitorAction.NO_ACTION.value,
                            "EXIT_WITHOUT_POSITION",
                            plan_id, symbol, strategy_result=strategy_result,
                        ))
                        continue
                    # A same-day A-share position may have sellable_qty=0.
                    # Persist the exit decision now and let the paper broker
                    # keep it pending until T+1 quantities are released;
                    # otherwise the decision disappears when today's plan
                    # expires and can never be audited or settled tomorrow.
                    self._condition_active.add(key)
                    events.append(self._emit_effective(
                        lane_id, plan, minute, minute_snapshot_id,
                        action, reason, strategy_result=strategy_result,
                    ))
                    continue
                if action == MonitorAction.BUY_SIGNAL.value and position is not None:
                    events.append(self._emit_internal(
                        lane_id, minute, minute_snapshot_id,
                        MonitorAction.NO_ACTION.value, "POSITION_ALREADY_OPEN",
                        plan_id, symbol, strategy_result=strategy_result,
                    ))
                    continue
                if action == MonitorAction.ADD_SIGNAL.value and position is None:
                    events.append(self._emit_internal(
                        lane_id, minute, minute_snapshot_id,
                        MonitorAction.NO_ACTION.value, "ADD_WITHOUT_POSITION",
                        plan_id, symbol, strategy_result=strategy_result,
                    ))
                    continue
                if not self._buy_allowed(minute):
                    events.append(self._emit_internal(
                        lane_id, minute, minute_snapshot_id,
                        MonitorAction.NO_ACTION.value, "BUY_TIME_CUTOFF",
                        plan_id, symbol, strategy_result=strategy_result,
                    ))
                    continue
                pending_veto.append({
                    "plan": plan,
                    "plan_id": plan_id,
                    "symbol": symbol,
                    "action": action,
                    "key": key,
                    "strategy_result": strategy_result,
                })
                continue

            trigger = self._deterministic_trigger(payload, bar)
            key = (lane_id, plan_id)
            if not trigger:
                self._reset_confirmation(lane_id, plan_id)
                trigger_results.append(
                    {"plan_id": plan_id, "symbol": symbol, "trigger_pass": False, "eligible": False, "action_candidate": "NO_ACTION"}
                )
                events.append(self._emit_internal(lane_id, minute, minute_snapshot_id, MonitorAction.NO_ACTION.value, "TRIGGER_NOT_MET", plan_id, symbol))
                continue

            if self._resolved_trigger_episode(
                durable_events,
                plan_id=plan_id,
                minute=minute,
                action=str(payload.get("action", MonitorAction.BUY_SIGNAL.value)),
            ):
                trigger_results.append(
                    {"plan_id": plan_id, "symbol": symbol, "trigger_pass": True, "eligible": False, "action_candidate": "NO_ACTION"}
                )
                events.append(
                    self._emit_internal(
                        lane_id,
                        minute,
                        minute_snapshot_id,
                        MonitorAction.NO_ACTION.value,
                        "SIGNAL_ALREADY_EMITTED",
                        plan_id,
                        symbol,
                    )
                )
                continue

            required = max(1, int(payload.get("confirmation_bars", payload.get("confirm_bars", 1))))
            previous = self._confirmations.get(key)
            if previous is None:
                count = self._durable_confirmation_count(lane_id, plan_id, minute) + 1
            elif minute - previous[0] != timedelta(minutes=1):
                count = 1
            else:
                count = previous[1] + 1
            self._confirmations[key] = (minute, count)
            if count < required:
                trigger_results.append(
                    {
                        "plan_id": plan_id,
                        "symbol": symbol,
                        "trigger_pass": True,
                        "eligible": False,
                        "action_candidate": "START_CONFIRMATION",
                        "confirmation_count": count,
                    }
                )
                events.append(self._emit_internal(lane_id, minute, minute_snapshot_id, MonitorAction.START_CONFIRMATION.value, "CONFIRMING", plan_id, symbol))
                continue
            if key in self._condition_active:
                trigger_results.append(
                    {"plan_id": plan_id, "symbol": symbol, "trigger_pass": True, "eligible": False, "action_candidate": "NO_ACTION", "confirmation_count": count}
                )
                events.append(self._emit_internal(lane_id, minute, minute_snapshot_id, MonitorAction.NO_ACTION.value, "SIGNAL_ALREADY_EMITTED", plan_id, symbol))
                continue

            action = str(payload.get("action", MonitorAction.BUY_SIGNAL.value))
            deterministic_exit = action in {MonitorAction.SELL_SIGNAL.value, MonitorAction.REDUCE_SIGNAL.value}
            if action not in {
                MonitorAction.BUY_SIGNAL.value,
                MonitorAction.ADD_SIGNAL.value,
                MonitorAction.SELL_SIGNAL.value,
                MonitorAction.REDUCE_SIGNAL.value,
            }:
                action = MonitorAction.BUY_SIGNAL.value
            if deterministic_exit:
                trigger_results.append(
                    {"plan_id": plan_id, "symbol": symbol, "trigger_pass": True, "eligible": False, "action_candidate": action, "confirmation_count": count}
                )
                if position is None:
                    events.append(self._emit_internal(lane_id, minute, minute_snapshot_id, MonitorAction.NO_ACTION.value, "EXIT_WITHOUT_POSITION", plan_id, symbol))
                    continue
                self._condition_active.add(key)
                events.append(self._emit_effective(lane_id, plan, minute, minute_snapshot_id, action, "DETERMINISTIC_EXIT_TRIGGER"))
                continue
            if action in {MonitorAction.BUY_SIGNAL.value, MonitorAction.ADD_SIGNAL.value} and not self._buy_allowed(minute):
                trigger_results.append(
                    {"plan_id": plan_id, "symbol": symbol, "trigger_pass": True, "eligible": False, "action_candidate": action, "confirmation_count": count}
                )
                events.append(self._emit_internal(lane_id, minute, minute_snapshot_id, MonitorAction.NO_ACTION.value, "BUY_TIME_CUTOFF", plan_id, symbol))
                continue
            trigger_results.append(
                {
                    "plan_id": plan_id,
                    "symbol": symbol,
                    "trigger_pass": True,
                    "eligible": True,
                    "action_candidate": action,
                    "confirmation_count": count,
                }
            )
            pending_veto.append({"plan": plan, "plan_id": plan_id, "symbol": symbol, "action": action, "key": key})

        # Exactly one isolated Flash batch per non-empty, data-valid lane and
        # minute.  The callback may only return veto flags; trigger_results
        # with trigger_pass=False can never become eligible here.
        vetoes: dict[str, bool] = {}
        veto_reasons: dict[str, str] = {}
        llm_failed = False
        llm_error_code: str | None = None
        if self.llm_veto is not None and pending_veto:
            model_called = True
            context = {
                "lane_id": lane_id,
                "minute_snapshot_id": minute_snapshot_id,
                "minute_end": minute.isoformat(),
                "plans": tuple(trigger_results),
            }
            try:
                response = self.llm_veto(context)
                veto_details = self._batch_veto_details(response, pending_veto)
                vetoes = {
                    plan_id: detail[0]
                    for plan_id, detail in veto_details.items()
                }
                veto_reasons = {
                    plan_id: detail[1]
                    for plan_id, detail in veto_details.items()
                    if detail[1]
                }
            except Exception as exc:
                llm_failed = True
                candidate = str(getattr(exc, "reason_code", "") or "")[:80]
                if candidate and all(character == "_" or character.isdigit() or character.isupper() for character in candidate):
                    llm_error_code = candidate
                else:
                    llm_error_code = "LLM_CALLBACK_FAILED"

        overrun = model_called and time.monotonic() - started > self.max_seconds
        if overrun:
            self._overrun_until[lane_id] = minute + timedelta(minutes=1)
            self._reset_lane(lane_id)
        for item in pending_veto:
            plan = item["plan"]
            plan_id = item["plan_id"]
            if overrun:
                strategy_result = _with_llm_observability(
                    item.get("strategy_result"),
                    llm_veto=False,
                    llm_reason_code="MONITOR_OVERRUN",
                )
                events.append(self._emit_effective(
                    lane_id,
                    plan,
                    minute,
                    minute_snapshot_id,
                    MonitorAction.DATA_BLOCK.value,
                    "MONITOR_OVERRUN",
                    llm_reason_code="MONITOR_OVERRUN",
                    strategy_result=strategy_result,
                ))
                continue
            if llm_failed or self.llm_veto is None:
                unavailable_code = llm_error_code or "LLM_CALLBACK_MISSING"
                events.append(
                    self._emit_effective(
                        lane_id,
                        plan,
                        minute,
                        minute_snapshot_id,
                        MonitorAction.DATA_BLOCK.value,
                        "LLM_UNAVAILABLE",
                        diagnostic_code=unavailable_code,
                        llm_reason_code=unavailable_code,
                        strategy_result=_with_llm_observability(
                            item.get("strategy_result"),
                            llm_veto=False,
                            llm_reason_code=unavailable_code,
                        ),
                    )
                )
                continue
            veto = bool(vetoes.get(plan_id, False))
            action = MonitorAction.LLM_VETO.value if veto else item["action"]
            llm_reason_code = veto_reasons.get(plan_id) or ("LLM_VETO" if veto else "LLM_PASS")
            strategy_result = _with_llm_observability(
                item.get("strategy_result"),
                llm_veto=veto,
                llm_reason_code=llm_reason_code,
            )
            self._condition_active.add(item["key"])
            events.append(self._emit_effective(
                lane_id,
                plan,
                minute,
                minute_snapshot_id,
                action,
                "LLM_VETO" if veto else "DETERMINISTIC_TRIGGER_PASS",
                llm_veto=veto,
                llm_reason_code=llm_reason_code,
                strategy_result=strategy_result,
            ))
        return MonitorBatchResult(
            lane_id=lane_id,
            minute_snapshot_id=minute_snapshot_id,
            events=tuple(events),
            model_called=model_called,
            blocked=any(event.action == MonitorAction.DATA_BLOCK.value for event in events),
        )

    def _bar_map(self, bars: Mapping[str, MinuteBar] | tuple[MinuteBar, ...] | list[MinuteBar]) -> dict[str, MinuteBar]:
        if isinstance(bars, Mapping):
            return {str(key): value for key, value in bars.items()}
        return {bar.symbol: bar for bar in bars}

    @staticmethod
    def _payload(plan: Mapping[str, Any]) -> dict[str, Any]:
        value = plan.get("payload_json", "{}")
        try:
            payload = json.loads(value) if isinstance(value, str) else dict(value)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _deterministic_trigger(payload: Mapping[str, Any], bar: MinuteBar) -> bool:
        low = payload.get("trigger_low")
        high = payload.get("trigger_high")
        if low is None or high is None:
            return False
        try:
            return float(low) <= bar.close <= float(high)
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _batch_veto(response: Any, pending: list[dict[str, Any]]) -> dict[str, bool]:
        """Extract only veto flags from one batch response.

        Accepted structural forms are a single boolean, a global
        ``llm_veto`` boolean, ``{"vetoes": {plan_id: bool}}`` or a list of
        ``{"plan_id": ..., "llm_veto": ...}`` records.  Any proposed action,
        price or quantity is intentionally ignored.
        """

        return {
            plan_id: detail[0]
            for plan_id, detail in MonitorEngine._batch_veto_details(response, pending).items()
        }

    @staticmethod
    def _batch_veto_details(
        response: Any,
        pending: list[dict[str, Any]],
    ) -> dict[str, tuple[bool, str | None]]:
        """Extract veto flags plus safe reason codes from one model response.

        Only explicit boolean veto fields are authoritative.  ``thinking``,
        free-form explanations, proposed actions and prices are ignored.  The
        legacy ``_batch_veto`` method remains a bool-only compatibility
        wrapper, so adding diagnostics cannot change trigger semantics.
        """

        plan_ids = [str(item["plan_id"]) for item in pending]

        def reason_from(value: Any) -> str | None:
            if not isinstance(value, Mapping):
                return None
            for key in ("reason_code", "veto_reason_code", "reason"):
                code = _safe_reason_code(value.get(key))
                if code:
                    return code
            codes = value.get("reason_codes")
            if isinstance(codes, (list, tuple)):
                for code in codes:
                    safe = _safe_reason_code(code)
                    if safe:
                        return safe
            return None

        if isinstance(response, bool):
            default = "LLM_VETO" if response else "LLM_PASS"
            return {plan_id: (response, default) for plan_id in plan_ids}
        if not isinstance(response, Mapping):
            return {}

        global_veto = response.get("llm_veto")
        if isinstance(global_veto, bool):
            default = "LLM_VETO" if global_veto else "LLM_PASS"
            reason = reason_from(response) or default
            return {plan_id: (global_veto, reason) for plan_id in plan_ids}

        raw = response.get("vetoes")
        if isinstance(raw, Mapping):
            reasons = response.get("veto_reasons")
            result: dict[str, tuple[bool, str | None]] = {}
            for plan_id in plan_ids:
                value = raw.get(plan_id, False)
                # Preserve the legacy bool(value) interpretation for the
                # existing veto map contract; mapping values were never
                # action-bearing input and are not reinterpreted here.
                veto = bool(value)
                reason = reason_from(value)
                if reason is None and isinstance(reasons, Mapping):
                    reason = _safe_reason_code(reasons.get(plan_id))
                result[plan_id] = (veto, reason or ("LLM_VETO" if veto else "LLM_PASS"))
            return result

        records = response.get("signals")
        if isinstance(records, (list, tuple)):
            result = {}
            for record in records:
                if isinstance(record, Mapping) and record.get("plan_id") in plan_ids:
                    veto = bool(record.get("llm_veto", False) or record.get("veto", False))
                    result[str(record["plan_id"])] = (
                        veto,
                        reason_from(record) or ("LLM_VETO" if veto else "LLM_PASS"),
                    )
            return result

        return {
            plan_id: (
                bool(response.get(plan_id, False)),
                "LLM_VETO" if bool(response.get(plan_id, False)) else "LLM_PASS",
            )
            for plan_id in plan_ids
        }

    @staticmethod
    def _in_session(value: datetime) -> bool:
        current = value.time().replace(tzinfo=None)
        # 09:30 is the opening-auction print.  The first closed continuous 1m
        # bar is 09:31, so A4 cannot make a bar-based decision before then.
        return datetime_time(9, 31) <= current <= datetime_time(11, 30) or datetime_time(13, 1) <= current <= datetime_time(15, 0)

    @staticmethod
    def _buy_allowed(value: datetime) -> bool:
        current = value.time().replace(tzinfo=None)
        return datetime_time(9, 32) <= current < datetime_time(14, 45)

    def _reset_confirmation(self, lane_id: str, plan_id: str) -> None:
        self._confirmations.pop((lane_id, plan_id), None)
        self._condition_active.discard((lane_id, plan_id))

    def _reset_lane(self, lane_id: str) -> None:
        for key in tuple(self._confirmations):
            if key[0] == lane_id:
                self._confirmations.pop(key, None)
                self._condition_active.discard(key)

    def _durable_confirmation_count(self, lane_id: str, plan_id: str, minute: datetime) -> int:
        """Recover consecutive confirmation minutes after a process restart."""

        expected = minute - timedelta(minutes=1)
        count = 0
        events = self.store.list_monitor_events(lane_id=lane_id)
        for event in reversed(events):
            if event.get("action") != MonitorAction.START_CONFIRMATION.value:
                continue
            try:
                payload = json.loads(event.get("payload_json") or "{}")
                event_minute = _local(datetime.fromisoformat(str(event["minute_end"])))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if payload.get("plan_id") != plan_id:
                continue
            if event_minute != expected:
                break
            count += 1
            expected -= timedelta(minutes=1)
        return count

    @staticmethod
    def _resolved_trigger_episode(
        events: tuple[dict[str, Any], ...],
        *,
        plan_id: str,
        minute: datetime,
        action: str,
    ) -> bool:
        """Recover the in-range de-duplication state after process restart.

        A filled/accepted action is terminal for that plan.  A veto only
        suppresses the same uninterrupted trigger episode; once a persisted
        ``TRIGGER_NOT_MET`` breaks the episode, a later re-entry may be
        evaluated again.
        """

        previous_minute = minute - timedelta(minutes=1)
        latest: tuple[datetime, str, str] | None = None
        # An accepted deterministic action is terminal for this plan even
        # across lunch, restarts and scheduler gaps.
        for event in events:
            if not bool(event.get("effective")) or str(event.get("action") or "") != action:
                continue
            try:
                payload = json.loads(event.get("payload_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            if payload.get("plan_id") == plan_id:
                return True
        for event in reversed(events):
            try:
                payload = json.loads(event.get("payload_json") or "{}")
                event_minute = _local(datetime.fromisoformat(str(event["minute_end"])))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if payload.get("plan_id") != plan_id:
                continue
            event_action = str(event.get("action") or "")
            reason = str(event.get("reason_code") or "")
            if latest is None:
                latest = (event_minute, event_action, reason)
            # Rows are ordered by minute.  No older row can describe the
            # immediately preceding episode once we moved before it.
            if event_minute < previous_minute:
                break
        if latest is None or latest[0] != previous_minute:
            return False
        _, latest_action, latest_reason = latest
        return latest_action == MonitorAction.LLM_VETO.value or (
            latest_action == MonitorAction.NO_ACTION.value
            and latest_reason in {"SIGNAL_ALREADY_EMITTED", "DUPLICATE_EFFECTIVE_STATE"}
        )

    def _emit_internal(
        self,
        lane_id: str,
        minute: datetime,
        snapshot_id: str,
        action: str,
        reason: str,
        plan_id: str | None = None,
        symbol: str | None = None,
        *,
        strategy_result: Mapping[str, Any] | None = None,
    ) -> MonitorEvent:
        key = f"internal:{lane_id}:{plan_id or '-'}:{minute.isoformat()}:{action}:{reason}"
        self.store.record_monitor_event(
            event_key=key,
            lane_id=lane_id,
            minute_end=minute,
            action=action,
            reason_code=reason,
            effective=False,
            payload={
                "minute_snapshot_id": snapshot_id,
                "plan_id": plan_id,
                "symbol": symbol,
                "strategy": dict(strategy_result) if isinstance(strategy_result, Mapping) else None,
            },
        )
        return MonitorEvent(lane_id=lane_id, plan_id=plan_id, symbol=symbol, minute_end=minute, action=action, reason_code=reason)

    def _emit_effective(
        self,
        lane_id: str,
        plan: Mapping[str, Any],
        minute: datetime,
        snapshot_id: str,
        action: str,
        reason: str,
        *,
        llm_veto: bool = False,
        llm_reason_code: str | None = None,
        diagnostic_code: str | None = None,
        strategy_result: Mapping[str, Any] | None = None,
    ) -> MonitorEvent:
        plan_id = str(plan["plan_id"])
        symbol = str(plan["symbol"])
        # One effective state per plan/action is the durable restart-safe
        # de-duplication key.  A new A3 plan receives a new plan_id.
        key = f"effective:{lane_id}:{plan_id}:{action}"
        record, inserted = self.store.record_monitor_event(
            event_key=key,
            lane_id=lane_id,
            minute_end=minute,
            action=action,
            reason_code=reason,
            effective=True,
            terminal_plan_id=(plan_id if action == MonitorAction.PLAN_INVALIDATED.value else None),
            sync_a4_lifecycle=True,
            payload={
                "minute_snapshot_id": snapshot_id,
                "plan_id": plan_id,
                "symbol": symbol,
                "llm_veto": bool(llm_veto),
                "llm_reason_code": _safe_reason_code(llm_reason_code),
                "diagnostic_code": diagnostic_code,
                "strategy": dict(strategy_result) if isinstance(strategy_result, Mapping) else None,
            },
        )
        if not inserted:
            return MonitorEvent(
                lane_id=lane_id,
                plan_id=plan_id,
                symbol=symbol,
                minute_end=minute,
                action=MonitorAction.NO_ACTION.value,
                reason_code="DUPLICATE_EFFECTIVE_STATE",
                effective=False,
                llm_reason_code=_safe_reason_code(llm_reason_code),
            )
        if inserted and self.effective_md_path is not None:
            self._append_markdown(record)
        return MonitorEvent(
            lane_id=lane_id,
            plan_id=plan_id,
            symbol=symbol,
            minute_end=minute,
            action=action,
            reason_code=reason,
            effective=True,
            llm_veto=llm_veto,
            llm_reason_code=_safe_reason_code(llm_reason_code),
        )

    def _append_markdown(self, record: Mapping[str, Any]) -> None:
        del record
        path = self.effective_md_path
        if path is None:
            return
        rebuild_effective_markdown(self.store, path)

    run_minute = process_minute
    tick = process_minute
    monitor_minute = process_minute


def rebuild_effective_markdown(store: RuntimeStore, path: str | Path) -> Path:
    target = Path(path)
    lines = [
        "# Effective intraday state-changing events",
        "",
        "> Includes only executable signals, vetoes, invalidations and data/risk blocks.",
        "",
    ]
    for item in store.list_monitor_events(effective_only=True):
        if item.get("action") not in EFFECTIVE_ACTIONS:
            continue
        try:
            payload = json.loads(str(item.get("payload_json") or "{}"))
        except Exception:
            payload = {}
        lines.append(
            f"- `{item['minute_end']}` | lane `{item['lane_id']}` | "
            f"`{payload.get('symbol', '')}` | `{item['action']}` | `{item.get('reason_code') or ''}`"
        )
    atomic_write_text(target, "\n".join(lines) + "\n")
    return target


_SAFE_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_\-]{0,79}$")


def _safe_reason_code(value: Any) -> str | None:
    """Return only a bounded enum-like code; never persist model prose."""

    if not isinstance(value, str):
        return None
    candidate = value.strip().upper()
    return candidate if _SAFE_REASON_CODE.fullmatch(candidate) else None


def _with_llm_observability(
    strategy_result: Any,
    *,
    llm_veto: bool,
    llm_reason_code: str,
) -> dict[str, Any] | None:
    """Copy only safe LLM outcome fields into the nested strategy payload."""

    if not isinstance(strategy_result, Mapping):
        return None
    return {
        **dict(strategy_result),
        "llm_veto": bool(llm_veto),
        "llm_reason_code": _safe_reason_code(llm_reason_code),
    }


__all__ = ["MonitorBatchResult", "MonitorEngine", "MonitorEvent", "rebuild_effective_markdown"]
