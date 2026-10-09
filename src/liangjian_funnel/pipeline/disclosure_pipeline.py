"""Bounded close-stage overlap, independent of announcement admission rules.

The caller supplies the existing, rate-limited disclosure query function.
Workers retain their slots and resource ownership if a dependency ignores
cancellation; a subsequent run cannot grow an unbounded worker population.
No model, RuntimeStore, source endpoint or strategy predicate lives here.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, TimeoutError
from contextlib import contextmanager
from queue import Empty, Queue
from threading import Event, Lock, Thread
import time
from typing import Any

from .feature_store import content_hash
from ..runtime.bounded_work import BoundedWorkGate


_WORKERS = BoundedWorkGate(16)


class DisclosurePipelineError(RuntimeError):
    pass


def industry_batch_order(symbols: Iterable[str], membership_rows: Iterable[Mapping[str, Any]],
                         node_order: Iterable[str]) -> tuple[str, ...]:
    """Group the existing complete node order; this function cannot add/drop.

    Use the same specific-before-broad membership preference as the existing
    A1 selector. Preserve that selector's intra-node turnover order. Unmapped
    identities remain at the end, rather than being silently excluded.
    """
    ordered = tuple(symbols)
    nodes = {str(code): index for index, code in enumerate(node_order)}
    by_symbol = {}
    for row in membership_rows:
        values = [v for v in row.get('memberships', ()) if isinstance(v, Mapping)
                  and v.get('industry_thscode') in nodes]
        values.sort(key=lambda v: (not str(v['industry_thscode']).startswith('884'),
                                  str(v['industry_thscode'])))
        if values:
            by_symbol[row.get('thscode')] = str(values[0]['industry_thscode'])
    return tuple(sorted(ordered, key=lambda s: nodes.get(by_symbol.get(s), len(nodes))))


class DisclosurePipeline:
    def __init__(self, fetch: Callable[[str], Any], *, workers: int, deadline: float,
                 close_resources: Callable[[], None] | None = None):
        if not 1 <= workers <= 16:
            raise ValueError('INVALID_DISCLOSURE_WORKER_COUNT')
        self.fetch, self.workers, self.deadline = fetch, workers, deadline
        self.close_resources = close_resources
        self._queue: Queue = Queue()
        self._stop = Event()
        self._lock = Lock()
        self._futures: dict[str, Future] = {}
        self._inputs: dict[str, str] = {}
        self._windows: dict[str, tuple[float, float]] = {}
        self._stages: dict[str, tuple[float, float]] = {}
        self._started = time.perf_counter()
        self._budget_started = time.monotonic()
        self._active = 0
        self._entered = False
        self._closed_resources = False

    def __enter__(self):
        if self._entered:
            raise DisclosurePipelineError('PIPELINE_ALREADY_ENTERED')
        self._entered = True
        try:
            for _ in range(self.workers):
                if not _WORKERS.try_acquire():
                    raise DisclosurePipelineError('DISCLOSURE_PIPELINE_BACKPRESSURE')
                with self._lock:
                    self._active += 1
                Thread(target=self._worker, daemon=True, name='close-disclosure').start()
        except BaseException:
            self.close()
            raise
        return self

    def _close_if_idle(self):
        with self._lock:
            close = self._active == 0 and not self._closed_resources
            if close:
                self._closed_resources = True
        if close and self.close_resources:
            self.close_resources()

    def _worker(self):
        try:
            while not self._stop.is_set():
                if time.monotonic() >= self.deadline:
                    return
                try:
                    symbol, future = self._queue.get(timeout=.05)
                except Empty:
                    continue
                if not future.set_running_or_notify_cancel():
                    continue
                started = time.perf_counter()
                error = None
                try:
                    if self._stop.is_set() or time.monotonic() >= self.deadline:
                        raise DisclosurePipelineError('DISCLOSURE_PIPELINE_DEADLINE')
                    value = self.fetch(symbol)
                    if time.monotonic() >= self.deadline or self._stop.is_set():
                        raise DisclosurePipelineError('DISCLOSURE_PIPELINE_LATE_RESULT')
                    if not isinstance(value, tuple) or len(value) != 5 or value[0] != symbol:
                        raise DisclosurePipelineError('DISCLOSURE_PIPELINE_SYMBOL_MISMATCH')
                except BaseException as exc:
                    error = exc
                finally:
                    with self._lock:
                        self._windows[symbol] = (started, time.perf_counter())
                if error is not None:
                    future.set_exception(error)
                else:
                    future.set_result(value)
        finally:
            _WORKERS.release()
            with self._lock:
                self._active -= 1
            self._close_if_idle()

    def submit(self, symbol: str, *, input_hash: str):
        if not self._entered or self._stop.is_set() or time.monotonic() >= self.deadline:
            raise DisclosurePipelineError('DISCLOSURE_PIPELINE_DEADLINE')
        if symbol in self._futures:
            if self._inputs[symbol] != input_hash:
                raise DisclosurePipelineError('DISCLOSURE_PIPELINE_INPUT_CHANGED')
            return
        self._inputs[symbol] = input_hash
        future: Future = Future()
        self._futures[symbol] = future
        self._queue.put((symbol, future))

    def validate_domain(self, symbols: Iterable[str], input_hashes: Mapping[str, str]):
        final = set(symbols)
        if set(self._futures) - final:
            raise DisclosurePipelineError('DISCLOSURE_PIPELINE_OUTSIDE_FINAL_DOMAIN')
        if any(input_hashes.get(s) != digest for s, digest in self._inputs.items()):
            raise DisclosurePipelineError('DISCLOSURE_PIPELINE_INPUT_CHANGED')

    def results(self, symbols: Iterable[str]) -> list[Any]:
        ordered = tuple(symbols)
        if any(symbol not in self._futures for symbol in ordered):
            raise DisclosurePipelineError('DISCLOSURE_PIPELINE_SCOPE_INCOMPLETE')
        values = []
        for symbol in ordered:
            remaining = self.deadline-time.monotonic()
            if remaining <= 0:
                raise DisclosurePipelineError('DISCLOSURE_PIPELINE_DEADLINE')
            try:
                values.append(self._futures[symbol].result(timeout=remaining))
            except TimeoutError as exc:
                raise DisclosurePipelineError('DISCLOSURE_PIPELINE_DEADLINE') from exc
        return values

    @contextmanager
    def stage(self, name: str):
        started = time.perf_counter()
        try:
            yield
        finally:
            self._stages[name] = (started, time.perf_counter())

    def receipt(self) -> dict:
        # Count interval union intersections, not the sum across parallel
        # workers (which could exceed elapsed wall time).
        with self._lock:
            windows = dict(self._windows)
        daily = self._stages.get('daily')
        intervals = sorted((max(a, daily[0]), min(b, daily[1])) for a, b in windows.values()
                           if daily and max(a, daily[0]) < min(b, daily[1]))
        merged: list[list[float]] = []
        for a, b in intervals:
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        payload = {'schema_version': 'close-disclosure-pipeline/1',
            'mode': 'CANDIDATE_DOMAIN', 'execution_authority': False,
            'elapsed_seconds': time.perf_counter()-self._started,
            'deadline_seconds_from_start': self.deadline-self._budget_started,
            'stages_seconds': {n: b-a for n, (a, b) in self._stages.items()},
            'overlap_seconds': sum(b-a for a, b in merged),
            'submitted_symbols': sorted(self._futures),
            'completed_symbols': sorted(s for s, f in self._futures.items()
                                        if f.done() and not f.cancelled() and f.exception() is None),
            'input_hashes': dict(sorted(self._inputs.items())),
            'query_windows_seconds': {s: [a-self._started, b-self._started]
                                     for s, (a, b) in sorted(windows.items())}}
        payload['receipt_hash'] = content_hash(payload)
        return payload

    def close(self):
        self._stop.set()
        for future in self._futures.values():
            future.cancel()
        self._close_if_idle()

    def __exit__(self, *_):
        # Never wait for a hung source or close its HTTP client underneath it.
        # The last worker releases the shared clients and its global slot.
        self.close()
