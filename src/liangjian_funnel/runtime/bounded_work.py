"""Bounded daemon work gates for dependencies without cooperative cancellation.

The caller gets a terminal result at its absolute deadline.  A dependency
that ignores cancellation may continue only in a daemon thread while holding
one global slot; subsequent rounds observe backpressure instead of creating an
unbounded number of leaked workers.
"""
from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue
from threading import BoundedSemaphore, Thread
import time
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class BoundedWorkResult:
    status: str
    value: Any = None
    reason_code: str = "OK"
    elapsed_ms: float = 0.0


class BoundedWorkGate:
    def __init__(self, max_inflight: int):
        if max_inflight <= 0:
            raise ValueError("max_inflight must be positive")
        self._slots = BoundedSemaphore(max_inflight)

    def try_acquire(self) -> bool:
        return self._slots.acquire(blocking=False)

    def release(self) -> None:
        self._slots.release()

    def call(
        self,
        operation: Callable[[], Any],
        *,
        deadline: float,
        clock: Callable[[], float] = time.monotonic,
        name: str = "bounded-work",
    ) -> BoundedWorkResult:
        started = clock()
        remaining = deadline - started
        if remaining <= 0:
            return BoundedWorkResult("TIMED_OUT", reason_code="DEADLINE_EXCEEDED")
        if not self.try_acquire():
            return BoundedWorkResult("BACKPRESSURE", reason_code="BACKPRESSURE")
        queue: Queue[tuple[str, Any]] = Queue(maxsize=1)

        def invoke() -> None:
            try:
                queue.put(("READY", operation()))
            except BaseException as exc:
                queue.put(("FAILED", exc))
            finally:
                self.release()

        Thread(target=invoke, daemon=True, name=name).start()
        try:
            status, value = queue.get(timeout=max(0.001, deadline - clock()))
        except Empty:
            return BoundedWorkResult(
                "TIMED_OUT",
                reason_code="DEADLINE_EXCEEDED",
                elapsed_ms=max(0.0, (clock() - started) * 1000.0),
            )
        if status == "FAILED":
            return BoundedWorkResult(
                "FAILED",
                value=type(value).__name__,
                reason_code="DEPENDENCY_FAILED",
                elapsed_ms=max(0.0, (clock() - started) * 1000.0),
            )
        return BoundedWorkResult(
            "READY",
            value=value,
            elapsed_ms=max(0.0, (clock() - started) * 1000.0),
        )


def run_many_bounded(
    operations: Mapping[str, Callable[[], Any]],
    *,
    deadline: float,
    gate: BoundedWorkGate,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, BoundedWorkResult]:
    """Launch independent work without a wait-all executor shutdown barrier."""

    output: dict[str, BoundedWorkResult] = {}
    queue: Queue[tuple[str, BoundedWorkResult]] = Queue()
    pending = iter(operations.items())
    active = 0

    def launch(key: str, operation: Callable[[], Any]) -> bool:
        nonlocal active
        if not gate.try_acquire():
            return False
        active += 1
        started = clock()

        def invoke() -> None:
            try:
                value = operation()
                result = BoundedWorkResult("READY", value=value, elapsed_ms=max(0.0, (clock() - started) * 1000.0))
            except BaseException as exc:
                result = BoundedWorkResult(
                    "FAILED", value=type(exc).__name__, reason_code="DEPENDENCY_FAILED",
                    elapsed_ms=max(0.0, (clock() - started) * 1000.0),
                )
            finally:
                gate.release()
            queue.put((key, result))

        Thread(target=invoke, daemon=True, name=f"bounded-{key}").start()
        return True

    waiting: list[tuple[str, Callable[[], Any]]] = list(pending)
    cursor = 0
    while cursor < len(waiting) or active:
        while cursor < len(waiting) and clock() < deadline:
            key, operation = waiting[cursor]
            if not launch(key, operation):
                break
            cursor += 1
        remaining = deadline - clock()
        if remaining <= 0:
            break
        if active == 0:
            # All shared slots belong to earlier timed-out work.
            break
        try:
            key, result = queue.get(timeout=max(0.001, remaining))
        except Empty:
            break
        active -= 1
        output[key] = result

    # Drain results that won the race with the deadline check.
    while True:
        try:
            key, result = queue.get_nowait()
        except Empty:
            break
        active = max(0, active - 1)
        output[key] = result
    for key, _operation in waiting[cursor:]:
        output.setdefault(key, BoundedWorkResult("BACKPRESSURE", reason_code="BACKPRESSURE"))
    for key in operations:
        output.setdefault(key, BoundedWorkResult("TIMED_OUT", reason_code="DEADLINE_EXCEEDED"))
    return output


__all__ = ["BoundedWorkGate", "BoundedWorkResult", "run_many_bounded"]
