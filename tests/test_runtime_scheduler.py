from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.runtime.scheduler import DispatchStatus, ScheduleKind, Scheduler
from liangjian_funnel.runtime.state import RuntimeStore


TZ = ZoneInfo("Asia/Shanghai")


def test_schedule_uses_injected_business_day_and_no_duplicate_lease_dispatch(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    seen = []
    scheduler = Scheduler(
        store,
        callbacks={"morning_0925": lambda job: seen.append(job.kind)},
        trading_day=lambda day: day == date(2026, 8, 24),
        owner="owner-a",
    )
    now = datetime(2026, 8, 24, 9, 26, tzinfo=TZ)
    first = scheduler.dispatch_once(now)
    second = scheduler.dispatch_once(now)
    assert any(record.kind is ScheduleKind.MORNING_0925 and record.status is DispatchStatus.DISPATCHED for record in first)
    assert any(record.kind is ScheduleKind.MORNING_0925 and record.status is DispatchStatus.LEASE_BUSY for record in second)
    assert seen == [ScheduleKind.MORNING_0925]


def test_morning_after_window_is_missed_and_monitor_does_not_catch_up(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    seen = []
    scheduler = Scheduler(
        store,
        callbacks={"morning_0925": lambda _job: seen.append("morning"), "monitor": lambda _job: seen.append("monitor")},
        trading_day=lambda _day: True,
    )
    records = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 45, tzinfo=TZ))
    assert any(record.kind is ScheduleKind.MORNING_0925 and record.status is DispatchStatus.MISSED for record in records)
    assert any(record.kind is ScheduleKind.MONITOR and record.status is DispatchStatus.DISPATCHED for record in records)
    assert "morning" not in seen
    assert seen == ["monitor"]


def test_lunch_and_non_trading_day_are_skipped_close_can_run_until_2030(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    seen = []
    scheduler = Scheduler(
        store,
        callbacks={"monitor": lambda _job: seen.append("monitor"), "close_1510": lambda _job: seen.append("close")},
        trading_day=lambda day: day.weekday() < 5,
    )
    lunch = scheduler.dispatch_once(datetime(2026, 8, 24, 12, 0, tzinfo=TZ))
    assert all(record.kind is not ScheduleKind.MONITOR for record in lunch)
    assert any(record.status is DispatchStatus.MISSED for record in lunch)
    weekend = scheduler.dispatch_once(datetime(2026, 8, 29, 9, 25, tzinfo=TZ))
    assert weekend[0].status is DispatchStatus.SKIPPED
    close = scheduler.dispatch_once(datetime(2026, 8, 24, 15, 10, tzinfo=TZ))
    assert any(record.kind is ScheduleKind.CLOSE_1510 and record.status is DispatchStatus.DISPATCHED for record in close)
    assert "close" in seen


def test_next_due_respects_lunch_and_close_slot(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    scheduler = Scheduler(store, trading_day=lambda _day: True)
    assert scheduler.next_due_at(datetime(2026, 8, 24, 8, 0, tzinfo=TZ)).time().strftime("%H:%M") == "08:30"
    assert scheduler.next_due_at(datetime(2026, 8, 24, 8, 31, tzinfo=TZ)).time().strftime("%H:%M") == "09:26"
    assert scheduler.next_due_at(datetime(2026, 8, 24, 12, 0, tzinfo=TZ)).time().strftime("%H:%M") == "13:00"
    assert scheduler.next_due_at(datetime(2026, 8, 24, 15, 1, tzinfo=TZ)).time().strftime("%H:%M") == "15:10"


def test_callback_failure_uses_safe_reason_releases_lease_and_retries(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    calls = []

    class ModelFailureError(RuntimeError):
        reason_code = "MODEL_CALL_FAILED"

    def callback(job):
        calls.append(job.dispatch_key)
        if len(calls) == 1:
            raise ModelFailureError("response contains secret-model-content")

    scheduler = Scheduler(store, callbacks={"morning_0925": callback}, trading_day=lambda _day: True)
    first = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 26, tzinfo=TZ))
    failed = next(record for record in first if record.kind is ScheduleKind.MORNING_0925)
    assert failed.status is DispatchStatus.FAILED
    assert failed.reason_code == "MODEL_CALL_FAILED"
    assert "secret-model-content" not in failed.model_dump_json()
    assert store.get_lease("scheduler:morning_0925") is None

    second = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 26, tzinfo=TZ))
    retried = next(record for record in second if record.kind is ScheduleKind.MORNING_0925)
    assert retried.status is DispatchStatus.DISPATCHED
    assert calls == [failed.dispatch_key, retried.dispatch_key]


def test_callback_failure_falls_back_to_exception_class_without_message(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")

    class UnsafeReasonError(RuntimeError):
        reason_code = "unsafe reason; contains secret"

    def callback(_job):
        raise UnsafeReasonError("sensitive callback body")

    scheduler = Scheduler(store, callbacks={"morning_0925": callback}, trading_day=lambda _day: True)
    records = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 26, tzinfo=TZ))
    failed = next(record for record in records if record.kind is ScheduleKind.MORNING_0925)
    assert failed.status is DispatchStatus.FAILED
    assert failed.reason_code == "UnsafeReasonError"
    encoded = failed.model_dump_json()
    assert "unsafe reason" not in encoded
    assert "sensitive callback body" not in encoded


def test_cache_conflict_diagnostics_are_whitelisted_without_market_values(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")

    class CacheFailure(RuntimeError):
        reason_code = "MINUTE_CACHE_CONFLICT"
        diagnostics = {
            "reason_code": "MINUTE_CACHE_CONFLICT",
            "symbol": "300308.SZ",
            "interval": "5m",
            "bar_end": "2026-08-25T09:50:00+08:00",
            "differing_fields": ("high", "close", "secret_field"),
            "secret": "must-not-appear",
        }

    scheduler = Scheduler(
        store,
        callbacks={"morning_0925": lambda _job: (_ for _ in ()).throw(CacheFailure("secret-price"))},
        trading_day=lambda _day: True,
    )
    records = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 26, tzinfo=TZ))
    failed = next(record for record in records if record.kind is ScheduleKind.MORNING_0925)
    assert failed.diagnostics == {
        "symbol": "300308.SZ",
        "interval": "5m",
        "bar_end": "2026-08-25T09:50:00+08:00",
        "differing_fields": ("high", "close"),
    }
    assert "must-not-appear" not in failed.model_dump_json()
    assert "secret-price" not in failed.model_dump_json()


def test_missed_is_recorded_once_and_monitor_continues(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    monitor_calls = []
    scheduler = Scheduler(
        store,
        callbacks={"monitor": lambda job: monitor_calls.append(job.dispatch_key)},
        trading_day=lambda _day: True,
    )
    first = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 45, tzinfo=TZ))
    second = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 46, tzinfo=TZ))
    assert sum(record.status is DispatchStatus.MISSED for record in first) == 1
    assert not any(record.status is DispatchStatus.MISSED for record in second)
    assert all(record.kind is not ScheduleKind.MORNING_0925 for record in second)
    assert len(monitor_calls) == 2


def test_successful_job_is_not_reclassified_as_missed_after_deadline(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    scheduler = Scheduler(
        store,
        callbacks={"morning_0925": lambda _job: None, "monitor": lambda _job: None},
        trading_day=lambda _day: True,
    )
    scheduler.dispatch_once(datetime(2026, 8, 24, 9, 26, tzinfo=TZ))
    late = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 45, tzinfo=TZ))
    assert not any(record.kind is ScheduleKind.MORNING_0925 and record.status is DispatchStatus.MISSED for record in late)


def test_expired_active_lease_can_recover_same_dispatch_after_crash(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    acquired_at = datetime(2026, 8, 24, 9, 25, tzinfo=TZ)
    assert store.acquire_lease(
        "scheduler:morning_0925",
        "dead-process",
        now=acquired_at,
        ttl_seconds=10,
        dispatch_key="morning_0925:2026-08-24T09:25:00+08:00",
    )
    assert store.acquire_lease(
        "scheduler:morning_0925",
        "replacement",
        now=acquired_at + timedelta(seconds=11),
        ttl_seconds=10,
        dispatch_key="morning_0925:2026-08-24T09:25:00+08:00",
    )
    assert store.get_lease("scheduler:morning_0925")["generation"] == 2


def test_business_block_is_scheduler_failure_and_dedicated_kind_isolated(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    calls = []
    scheduler = Scheduler(
        store,
        callbacks={
            "morning_0925": lambda _job: calls.append("morning") or {"status": "BLOCKED"},
            "monitor": lambda _job: calls.append("monitor") or {"lanes": []},
        },
        trading_day=lambda _day: True,
    )
    records = scheduler.dispatch_once(
        datetime(2026, 8, 24, 9, 26, tzinfo=TZ),
        kinds=(ScheduleKind.MORNING_0925,),
    )
    assert len(records) == 1
    assert records[0].status is DispatchStatus.FAILED
    assert records[0].reason_code == "WORKFLOW_BLOCKED"
    assert calls == ["morning"]


def test_premarket_has_independent_lease_and_deadline(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    calls = []
    scheduler = Scheduler(
        store,
        callbacks={"premarket_0830": lambda job: calls.append(job.kind)},
        trading_day=lambda _day: True,
    )
    first = scheduler.dispatch_once(datetime(2026, 8, 24, 8, 30, tzinfo=TZ))
    second = scheduler.dispatch_once(datetime(2026, 8, 24, 8, 30, tzinfo=TZ))
    assert any(record.kind is ScheduleKind.PREMARKET_0830 and record.status is DispatchStatus.DISPATCHED for record in first)
    assert any(record.kind is ScheduleKind.PREMARKET_0830 and record.status is DispatchStatus.LEASE_BUSY for record in second)
    assert calls == [ScheduleKind.PREMARKET_0830]
    assert store.get_lease("scheduler:premarket_0830") is not None


def test_premarket_is_missed_after_read_only_deadline(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    called = []
    scheduler = Scheduler(
        store,
        callbacks={"premarket_0830": lambda _job: called.append(True)},
        trading_day=lambda _day: True,
    )
    records = scheduler.dispatch_once(datetime(2026, 8, 24, 9, 21, tzinfo=TZ))
    assert any(record.kind is ScheduleKind.PREMARKET_0830 and record.status is DispatchStatus.MISSED for record in records)
    assert called == []
