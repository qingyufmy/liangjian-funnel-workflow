"""Asia/Shanghai business-day scheduler with SQLite leases."""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from .state import RuntimeStore


SHANGHAI = ZoneInfo("Asia/Shanghai")
_SAFE_REASON_CODE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_SYMBOL = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_SAFE_BAR_END = re.compile(r"^[0-9T:+-]{20,40}$")
_SAFE_DIFFERING_FIELDS = frozenset(
    {"symbol", "interval", "bar_end", "open", "high", "low", "close", "volume", "amount", "adjust_mode"}
)


class ScheduleKind(StrEnum):
    MORNING_0925 = "morning_0925"
    CLOSE_1510 = "close_1510"
    MONITOR = "monitor"


class DispatchStatus(StrEnum):
    DISPATCHED = "DISPATCHED"
    MISSED = "MISSED"
    LEASE_BUSY = "LEASE_BUSY"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"


class ScheduledJob(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: ScheduleKind
    due: datetime
    late: bool = False
    dispatch_key: str


class DispatchRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: ScheduleKind
    due: datetime
    status: DispatchStatus
    dispatch_key: str
    reason_code: str | None = None
    diagnostics: dict[str, Any] = Field(default_factory=dict)


TradingDayFn = Callable[[date], bool]


def _local(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("scheduler timestamp must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _at(day: date, clock: datetime_time) -> datetime:
    return datetime.combine(day, clock, tzinfo=SHANGHAI)


class Scheduler:
    """Compute only the current due task; never backfills stale monitor BUYs."""

    def __init__(
        self,
        store: RuntimeStore,
        *,
        callbacks: Mapping[str | ScheduleKind, Callable[..., Any]] | None = None,
        trading_day: TradingDayFn | None = None,
        owner: str = "liangjian-runtime",
        lease_ttl_seconds: float = 90.0,
        timezone: str = "Asia/Shanghai",
    ):
        if timezone != "Asia/Shanghai":
            raise ValueError("runtime scheduler timezone must be Asia/Shanghai")
        if lease_ttl_seconds <= 0:
            raise ValueError("lease ttl must be positive")
        self.store = store
        self.callbacks = {str(key): value for key, value in (callbacks or {}).items()}
        self.trading_day = trading_day or (lambda value: value.weekday() < 5)
        self.owner = owner
        self.lease_ttl_seconds = lease_ttl_seconds

    def next_due(self, now: datetime | None = None) -> ScheduledJob | None:
        current = _local(now or datetime.now(SHANGHAI))
        day = current.date()
        for _ in range(370):
            if self.trading_day(day):
                candidate = self._next_on_day(day, current if day == current.date() else _at(day, datetime_time(0, 0)))
                if candidate is not None:
                    return candidate
            day += timedelta(days=1)
            current = _at(day, datetime_time(0, 0))
        return None

    def next_due_at(self, now: datetime | None = None) -> datetime | None:
        job = self.next_due(now)
        return job.due if job else None

    def dispatch_once(
        self,
        now: datetime | None = None,
        *,
        kinds: tuple[ScheduleKind, ...] | None = None,
    ) -> tuple[DispatchRecord, ...]:
        current = _local(now or datetime.now(SHANGHAI))
        allowed = set(kinds or tuple(ScheduleKind))
        if not self.trading_day(current.date()):
            kind = next((item for item in ScheduleKind if item in allowed), ScheduleKind.MONITOR)
            return (DispatchRecord(kind=kind, due=current, status=DispatchStatus.SKIPPED, dispatch_key=f"nontrade:{current.date()}", reason_code="NON_TRADING_DAY"),)
        jobs = tuple(job for job in self._due_jobs(current) if job.kind in allowed)
        records: list[DispatchRecord] = []
        for job in jobs:
            if self._is_missed(job, current):
                lease = self._lease_name(job.kind)
                missed_key = f"MISSED:{job.dispatch_key}"
                existing = self.store.get_lease(lease)
                # A successful dispatch leaves its normal dispatch key in the
                # lease.  Do not reinterpret that completed job as missed
                # after its late-observation window, and do not emit the same
                # MISSED record on every scheduler tick.
                if existing is not None and existing.get("last_dispatch_key") in {job.dispatch_key, missed_key}:
                    continue
                acquired = self.store.acquire_lease(
                    lease,
                    self.owner,
                    now=current,
                    ttl_seconds=self.lease_ttl_seconds,
                    dispatch_key=missed_key,
                )
                if not acquired:
                    continue
                records.append(DispatchRecord(kind=job.kind, due=job.due, status=DispatchStatus.MISSED, dispatch_key=job.dispatch_key, reason_code="SCHEDULE_MISSED"))
                self.store.complete_lease(lease, self.owner, dispatch_key=missed_key, now=current)
                continue
            lease = self._lease_name(job.kind)
            acquired = self.store.acquire_lease(
                lease,
                self.owner,
                now=current,
                ttl_seconds=self.lease_ttl_seconds,
                dispatch_key=job.dispatch_key,
            )
            if not acquired:
                records.append(DispatchRecord(kind=job.kind, due=job.due, status=DispatchStatus.LEASE_BUSY, dispatch_key=job.dispatch_key, reason_code="LEASE_BUSY"))
                continue
            callback = self._callback(job.kind)
            if callback is None:
                records.append(DispatchRecord(kind=job.kind, due=job.due, status=DispatchStatus.SKIPPED, dispatch_key=job.dispatch_key, reason_code="CALLBACK_NOT_CONFIGURED"))
                self.store.complete_lease(lease, self.owner, dispatch_key=job.dispatch_key, now=current)
                continue
            try:
                result = self._invoke(callback, job)
                business_reason = self._callback_result_reason(result)
                if business_reason is not None:
                    self.store.release_lease(lease, self.owner)
                    records.append(
                        DispatchRecord(
                            kind=job.kind,
                            due=job.due,
                            status=DispatchStatus.FAILED,
                            dispatch_key=job.dispatch_key,
                            reason_code=business_reason,
                        )
                    )
                    continue
                self.store.complete_lease(lease, self.owner, dispatch_key=job.dispatch_key, now=current)
            except Exception as exc:
                # A failed callback must be retryable before the slot's
                # deadline.  Keep the callback's error isolated from the
                # scheduler's durable lease and never persist its message.
                try:
                    self.store.release_lease(lease, self.owner)
                except Exception:
                    # Preserve the callback failure record if persistence
                    # itself prevents releasing the lease.  The lease TTL is
                    # still the final recovery guard.
                    pass
                records.append(
                    DispatchRecord(
                        kind=job.kind,
                        due=job.due,
                        status=DispatchStatus.FAILED,
                        dispatch_key=job.dispatch_key,
                        reason_code=self._callback_reason(exc),
                        diagnostics=self._callback_diagnostics(exc),
                    )
                )
                continue
            records.append(DispatchRecord(kind=job.kind, due=job.due, status=DispatchStatus.DISPATCHED, dispatch_key=job.dispatch_key))
        return tuple(records)

    def _next_on_day(self, day: date, current: datetime) -> ScheduledJob | None:
        morning = _at(day, datetime_time(9, 26))
        if current <= morning:
            return self._job(ScheduleKind.MORNING_0925, morning, current)
        first_monitor = _at(day, datetime_time(9, 31))
        if current < first_monitor:
            return self._job(ScheduleKind.MONITOR, first_monitor, current)
        if current <= _at(day, datetime_time(11, 30)):
            return self._job(ScheduleKind.MONITOR, self._ceil_minute(current), current)
        afternoon = _at(day, datetime_time(13, 0))
        if current < afternoon:
            return self._job(ScheduleKind.MONITOR, afternoon, current)
        if current <= _at(day, datetime_time(15, 0)):
            return self._job(ScheduleKind.MONITOR, self._ceil_minute(current), current)
        close = _at(day, datetime_time(15, 10))
        if current <= close:
            return self._job(ScheduleKind.CLOSE_1510, close, current)
        return None

    def _due_jobs(self, current: datetime) -> tuple[ScheduledJob, ...]:
        day = current.date()
        jobs: list[ScheduledJob] = []
        morning = _at(day, datetime_time(9, 26))
        if current >= morning and current < _at(day, datetime_time(15, 10)):
            jobs.append(self._job(ScheduleKind.MORNING_0925, morning, current))
        if datetime_time(9, 31) <= current.time().replace(tzinfo=None) <= datetime_time(11, 30):
            jobs.append(self._job(ScheduleKind.MONITOR, self._floor_minute(current), current))
        if datetime_time(13, 1) <= current.time().replace(tzinfo=None) <= datetime_time(15, 0):
            jobs.append(self._job(ScheduleKind.MONITOR, self._floor_minute(current), current))
        close = _at(day, datetime_time(15, 10))
        if current >= close and current <= _at(day, datetime_time(20, 30)):
            jobs.append(self._job(ScheduleKind.CLOSE_1510, close, current))
        return tuple(jobs)

    @staticmethod
    def _job(kind: ScheduleKind, due: datetime, reference: datetime | None = None) -> ScheduledJob:
        due = _local(due)
        return ScheduledJob(kind=kind, due=due, late=reference is not None and due < reference, dispatch_key=f"{kind.value}:{due.isoformat()}")

    @staticmethod
    def _floor_minute(value: datetime) -> datetime:
        return value.replace(second=0, microsecond=0)

    @staticmethod
    def _ceil_minute(value: datetime) -> datetime:
        floored = Scheduler._floor_minute(value)
        return floored if value == floored else floored + timedelta(minutes=1)

    @staticmethod
    def _is_missed(job: ScheduledJob, current: datetime) -> bool:
        if job.kind is ScheduleKind.MORNING_0925:
            return current > _at(current.date(), datetime_time(9, 40))
        if job.kind is ScheduleKind.CLOSE_1510:
            return current > _at(current.date(), datetime_time(20, 30))
        return False

    @staticmethod
    def _lease_name(kind: ScheduleKind) -> str:
        return f"scheduler:{kind.value}"

    def _callback(self, kind: ScheduleKind) -> Callable[..., Any] | None:
        return self.callbacks.get(kind.value) or self.callbacks.get(kind.name) or self.callbacks.get(kind)

    @staticmethod
    def _callback_reason(exc: Exception) -> str:
        """Return only a stable, non-sensitive identifier for callback errors."""

        try:
            candidate = getattr(exc, "reason_code", None)
        except Exception:
            candidate = None
        if isinstance(candidate, str) and _SAFE_REASON_CODE.fullmatch(candidate):
            return candidate
        class_name = type(exc).__name__
        return class_name if _SAFE_REASON_CODE.fullmatch(class_name) else "CALLBACK_FAILED"

    @staticmethod
    def _callback_diagnostics(exc: Exception) -> dict[str, Any]:
        """Copy only the bounded cache-conflict identifiers safe for logs."""

        try:
            raw = getattr(exc, "diagnostics", None)
        except Exception:
            return {}
        if not isinstance(raw, Mapping) or raw.get("reason_code") != "MINUTE_CACHE_CONFLICT":
            return {}
        result: dict[str, Any] = {}
        symbol = raw.get("symbol")
        if isinstance(symbol, str) and _SAFE_SYMBOL.fullmatch(symbol):
            result["symbol"] = symbol
        interval = raw.get("interval")
        if interval in {"1m", "5m"}:
            result["interval"] = interval
        bar_end = raw.get("bar_end")
        if isinstance(bar_end, str) and _SAFE_BAR_END.fullmatch(bar_end):
            result["bar_end"] = bar_end
        differing = raw.get("differing_fields")
        if isinstance(differing, (list, tuple)):
            fields = tuple(field for field in differing if field in _SAFE_DIFFERING_FIELDS)
            if fields:
                result["differing_fields"] = fields
        return result

    @staticmethod
    def _callback_result_reason(result: Any) -> str | None:
        """Promote fail-closed business results to scheduler failures."""

        if not isinstance(result, Mapping):
            return None
        status = str(result.get("status") or "").upper()
        if status in {"BLOCKED", "FAILED", "PARTIAL"}:
            return f"WORKFLOW_{status}"
        lanes = result.get("lanes")
        if isinstance(lanes, (list, tuple)) and any(
            isinstance(item, Mapping) and item.get("blocked") is True for item in lanes
        ):
            return "WORKFLOW_MONITOR_BLOCKED"
        return None

    @staticmethod
    def _invoke(callback: Callable[..., Any], job: ScheduledJob) -> Any:
        try:
            signature = inspect.signature(callback)
            required = [
                parameter
                for parameter in signature.parameters.values()
                if parameter.kind in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
                and parameter.default is parameter.empty
            ]
        except (TypeError, ValueError):
            required = [object()]
        if required:
            return callback(job)
        return callback()


RuntimeScheduler = Scheduler


__all__ = ["DispatchRecord", "DispatchStatus", "RuntimeScheduler", "ScheduleKind", "ScheduledJob", "Scheduler"]
