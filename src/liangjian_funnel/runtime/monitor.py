"""Deterministic A4 monitor with an injected, veto-only LLM callback."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from ..data.mootdx import MinuteBar
from ..reporting import atomic_write_text
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
        gap_detected: bool = False,
        snapshot_contiguous: bool = True,
        gap: bool | None = None,
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
        if gap is not None:
            gap_detected = gap
        plans = self.store.list_active_plans(lane_id, at=minute)
        if not plans:
            # A real virtual position remains in the risk lane even after its
            # research plan expires.  Reconstruct a minimal deterministic
            # risk scope without reviving its buy permission.
            positions = self.store.list_positions(f"paper:{lane_id}")
            plans = tuple(
                {
                    "plan_id": f"position:{lane_id}:{position['symbol']}",
                    "lane_id": lane_id,
                    "symbol": position["symbol"],
                    "payload_json": json.dumps({"stop_level": position.get("stop_level")}),
                }
                for position in positions
            )
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

        if not data_ok or not snapshot_contiguous or gap_detected:
            self._reset_lane(lane_id)
            for plan in plans:
                events.append(
                    self._emit_effective(
                        lane_id,
                        plan,
                        minute,
                        minute_snapshot_id,
                        MonitorAction.DATA_BLOCK.value,
                        "MINUTE_DATA_GAP" if gap_detected else "MINUTE_DATA_UNAVAILABLE",
                    )
                )
            return MonitorBatchResult(lane_id=lane_id, minute_snapshot_id=minute_snapshot_id, events=tuple(events), blocked=True)

        model_called = False
        started = time.monotonic()
        pending_veto: list[dict[str, Any]] = []
        trigger_results: list[dict[str, Any]] = []
        for plan in plans:
            plan_id = str(plan["plan_id"])
            symbol = str(plan["symbol"])
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
            if payload.get("plan_invalidated") or payload.get("invalidated"):
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
            position = self.store.get_position(f"paper:{lane_id}", symbol)
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
                if position is None or int(position.get("sellable_qty", 0)) <= 0:
                    events.append(self._emit_internal(lane_id, minute, minute_snapshot_id, MonitorAction.NO_ACTION.value, "EXIT_WITHOUT_SELLABLE_POSITION", plan_id, symbol))
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
        llm_failed = False
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
                vetoes = self._batch_veto(response, pending_veto)
            except Exception:
                llm_failed = True

        overrun = model_called and time.monotonic() - started > self.max_seconds
        if overrun:
            self._overrun_until[lane_id] = minute + timedelta(minutes=1)
            self._reset_lane(lane_id)
        for item in pending_veto:
            plan = item["plan"]
            plan_id = item["plan_id"]
            if overrun:
                events.append(self._emit_effective(lane_id, plan, minute, minute_snapshot_id, MonitorAction.DATA_BLOCK.value, "MONITOR_OVERRUN"))
                continue
            if llm_failed or self.llm_veto is None:
                events.append(self._emit_effective(lane_id, plan, minute, minute_snapshot_id, MonitorAction.DATA_BLOCK.value, "LLM_UNAVAILABLE"))
                continue
            veto = bool(vetoes.get(plan_id, False))
            action = MonitorAction.LLM_VETO.value if veto else item["action"]
            self._condition_active.add(item["key"])
            events.append(self._emit_effective(lane_id, plan, minute, minute_snapshot_id, action, "LLM_VETO" if veto else "DETERMINISTIC_TRIGGER_PASS", llm_veto=veto))
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

        plan_ids = [str(item["plan_id"]) for item in pending]
        if isinstance(response, bool):
            return {plan_id: response for plan_id in plan_ids}
        if not isinstance(response, Mapping):
            return {}
        global_veto = response.get("llm_veto")
        if isinstance(global_veto, bool):
            return {plan_id: global_veto for plan_id in plan_ids}
        raw = response.get("vetoes")
        if isinstance(raw, Mapping):
            return {plan_id: bool(raw.get(plan_id, False)) for plan_id in plan_ids}
        records = response.get("signals")
        if isinstance(records, (list, tuple)):
            result: dict[str, bool] = {}
            for record in records:
                if isinstance(record, Mapping) and record.get("plan_id") in plan_ids:
                    result[str(record["plan_id"])] = bool(record.get("llm_veto", False) or record.get("veto", False))
            return result
        return {plan_id: bool(response.get(plan_id, False)) for plan_id in plan_ids}

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
    ) -> MonitorEvent:
        key = f"internal:{lane_id}:{plan_id or '-'}:{minute.isoformat()}:{action}:{reason}"
        self.store.record_monitor_event(
            event_key=key,
            lane_id=lane_id,
            minute_end=minute,
            action=action,
            reason_code=reason,
            effective=False,
            payload={"minute_snapshot_id": snapshot_id, "plan_id": plan_id, "symbol": symbol},
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
            payload={
                "minute_snapshot_id": snapshot_id,
                "plan_id": plan_id,
                "symbol": symbol,
                "llm_veto": bool(llm_veto),
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


__all__ = ["MonitorBatchResult", "MonitorEngine", "MonitorEvent", "rebuild_effective_markdown"]
