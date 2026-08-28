"""Persistent, redacted workflow progress for the local control plane."""

from __future__ import annotations

import copy
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")


class WorkflowProgress:
    """Atomically publish bounded workflow progress across process restarts.

    The progress file is presentation state, not an execution authority.  It
    deliberately contains counters and stable reason codes only; prompts,
    provider responses, API credentials and model reasoning never enter it.
    """

    def __init__(self, path: Path, *, run_id: str, job: str, now: datetime | None = None) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        started = _aware(now or datetime.now(SHANGHAI))
        self._state: dict[str, Any] = {
            "schema_version": 2,
            "run_id": str(run_id)[:200],
            "job": str(job)[:40],
            "status": "RUNNING",
            "phase": "STARTING",
            "started_at": started.isoformat(),
            "phase_started_at": started.isoformat(),
            "updated_at": started.isoformat(),
            "elapsed_seconds": 0,
            "eta_seconds": None,
            "data": {
                "processed": 0,
                "total": 0,
                "cache_hits": 0,
                "cache_misses": 0,
                "failures": 0,
            },
            "lanes": {},
            "reason_code": None,
        }
        self._write()

    def set_phase(
        self,
        phase: str,
        *,
        status: str = "RUNNING",
        eta_seconds: int | None = None,
        now: datetime | None = None,
    ) -> None:
        with self._lock:
            next_phase = _token(phase, 80)
            if next_phase != self._state.get("phase"):
                self._state["phase_started_at"] = _aware(
                    now or datetime.now(SHANGHAI)
                ).isoformat()
            self._state["phase"] = next_phase
            self._state["status"] = _token(status, 40)
            self._state["eta_seconds"] = _non_negative(eta_seconds)
            self._touch(now)

    def update_data(
        self,
        *,
        processed: int,
        total: int,
        cache_hits: int,
        cache_misses: int,
        failures: int,
        current_symbol: str | None = None,
        current_document: str | None = None,
        documents_succeeded: int | None = None,
        documents_failed: int | None = None,
        eta_seconds: int | None = None,
        now: datetime | None = None,
    ) -> None:
        with self._lock:
            payload: dict[str, Any] = {
                "processed": _non_negative(processed) or 0,
                "total": _non_negative(total) or 0,
                "cache_hits": _non_negative(cache_hits) or 0,
                "cache_misses": _non_negative(cache_misses) or 0,
                "failures": _non_negative(failures) or 0,
            }
            if current_symbol:
                payload["current_symbol"] = _token(current_symbol, 32)
            if current_document:
                payload["current_document"] = _token(current_document, 200)
            if documents_succeeded is not None:
                payload["documents_succeeded"] = _non_negative(documents_succeeded) or 0
            if documents_failed is not None:
                payload["documents_failed"] = _non_negative(documents_failed) or 0
            self._state["data"] = payload
            computed_eta = _non_negative(eta_seconds)
            if computed_eta is None and payload["processed"] > 0 and payload["total"] > payload["processed"]:
                current = _aware(now or datetime.now(SHANGHAI))
                phase_started = datetime.fromisoformat(str(self._state["phase_started_at"]))
                elapsed = max(0.0, (current - phase_started).total_seconds())
                computed_eta = int(
                    elapsed * (payload["total"] - payload["processed"]) / payload["processed"]
                )
            self._state["eta_seconds"] = computed_eta
            self._touch(now)

    def research_event(self, event: Mapping[str, Any], *, now: datetime | None = None) -> None:
        """Consume one safe batch event from ``ResearchPipeline``."""

        lane = _token(event.get("lane") or event.get("lane_id") or "unknown", 40)
        stage = _token(event.get("stage") or "unknown", 40)
        with self._lock:
            lanes = self._state.setdefault("lanes", {})
            lane_state = lanes.setdefault(lane, {"model": None, "status": "RUNNING", "stages": {}})
            if event.get("model"):
                lane_state["model"] = _token(event["model"], 120)
            lane_state["status"] = _token(event.get("lane_status") or "RUNNING", 40)
            lane_state["current_stage"] = stage
            stages = lane_state.setdefault("stages", {})
            stage_state = {
                "status": _token(event.get("status") or "RUNNING", 40),
                "completed_batches": _non_negative(
                    event.get("completed_batches", event.get("completed", 0))
                ) or 0,
                "total_batches": _non_negative(event.get("total_batches", event.get("total", 0))) or 0,
                "attempts": _non_negative(event.get("attempts", 0)) or 0,
            }
            reason_codes = _reason_codes(event.get("reason_codes"))
            if reason_codes:
                stage_state["reason_codes"] = reason_codes
            if isinstance(event.get("outcome"), str) and event["outcome"].strip():
                stage_state["outcome"] = _token(event["outcome"], 80)
            if isinstance(event.get("checkpoint_reused"), bool):
                stage_state["checkpoint_reused"] = event["checkpoint_reused"]
            if event.get("checkpoint_batch_index") is not None:
                checkpoint_batch = _non_negative(event.get("checkpoint_batch_index"))
                if checkpoint_batch is not None:
                    stage_state["checkpoint_batch_index"] = checkpoint_batch
            for target, *sources in (
                ("processed_symbols", "processed_symbols", "processed"),
                ("total_symbols", "total_symbols", "universe_total"),
                ("selected_symbols", "selected_symbols"),
                ("monitor_symbols", "monitor_symbols"),
                ("rejected_symbols", "rejected_symbols"),
            ):
                value = next((_non_negative(event.get(source)) for source in sources if event.get(source) is not None), None)
                if value is not None:
                    stage_state[target] = value
            stages[stage] = stage_state
            self._state["phase"] = f"RESEARCH_{stage}"[:80]
            self._touch(now)

    def finish(
        self,
        *,
        status: str,
        phase: str = "COMPLETED",
        reason_code: str | None = None,
        now: datetime | None = None,
    ) -> None:
        with self._lock:
            self._state["status"] = _token(status, 40)
            self._state["phase"] = _token(phase, 80)
            self._state["eta_seconds"] = 0
            self._state["reason_code"] = _token(reason_code, 120) if reason_code else None
            lane_status = "COMPLETED" if self._state["status"] in {"READY", "COMPLETED"} else self._state["status"]
            for lane in self._state.get("lanes", {}).values():
                if isinstance(lane, dict):
                    lane["status"] = lane_status
            self._touch(now)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._state)

    def _touch(self, now: datetime | None) -> None:
        current = _aware(now or datetime.now(SHANGHAI))
        started = datetime.fromisoformat(str(self._state["started_at"]))
        self._state["updated_at"] = current.isoformat()
        self._state["elapsed_seconds"] = max(0, int((current - started).total_seconds()))
        self._write()

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent_stat = self.path.parent.stat()
        atomic_write_json(
            self.path,
            self._state,
            mode=0o640,
            group_id=getattr(parent_stat, "st_gid", None),
        )


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _token(value: Any, limit: int) -> str:
    text = str(value or "UNKNOWN").strip().upper()
    retained = "".join(character for character in text if character.isalnum() or character in "._:/+_-")
    return (retained or "UNKNOWN")[:limit]


def _non_negative(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, number)


def _reason_codes(value: Any) -> list[str]:
    """Keep only bounded, stable reason codes in the progress file."""

    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = list(value)
    else:
        return []
    result: list[str] = []
    for raw in values:
        if not isinstance(raw, str):
            continue
        token = raw.strip().upper()
        if not token or len(token) > 120:
            continue
        if not all(character.isalnum() or character in "_:.-" for character in token):
            continue
        if token not in result:
            result.append(token)
        if len(result) >= 20:
            break
    return result


__all__ = ["WorkflowProgress"]
