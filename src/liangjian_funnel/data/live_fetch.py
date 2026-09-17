"""Current-session acquisition policy; no stale bars or late signal recovery."""
from __future__ import annotations

from copy import copy
from datetime import datetime, timedelta
import time
import json
from threading import RLock

from ..reporting import atomic_write_json

from .mootdx import FetchResult, MootdxAdapter, detect_missing_bars
from .tencent_minute import TencentIntradayAdapter
from .session_windows import closed_window_ends, TZ

_NODE_LOCK = RLock()
CLOSE_FINALIZATION_GRACE_SECONDS = 20.0


def _reserve_node(source):
    with _NODE_LOCK:
        state = getattr(source, '_live_node_health', None)
        if state is None:
            state = {}
            path = getattr(source, 'live_health_path', None)
            if path is not None and path.exists():
                try:
                    if path.stat().st_size < 65536:
                        state = json.loads(path.read_text(encoding='utf-8'))
                except (OSError, ValueError):
                    pass
            if not isinstance(state, dict):
                state = {}
            source._live_node_health = state
        busy = getattr(source, '_live_busy_nodes', set())
        source._live_busy_nodes = busy
        for node in source.nodes:
            value = state.get(node.server, {})
            retry_at = value.get('retry_at', 0) if isinstance(value, dict) else 0
            if node.server not in busy and isinstance(retry_at, (int, float)) and retry_at <= time.time():
                busy.add(node.server)
                return node
        return None


def _release_node(source, node, healthy):
    with _NODE_LOCK:
        source._live_busy_nodes.discard(node.server)
        source._live_node_health[node.server] = {'retry_at': 0 if healthy else time.time()+60,
                                                 'healthy': healthy, 'checked_at': time.time()}
        path = getattr(source, 'live_health_path', None)
        if path is not None:
            try:
                atomic_write_json(path, source._live_node_health)
            except OSError:
                pass  # Observability must not prevent a valid price response.


def fetch_live_window(provider, secondary, symbol, interval, required, cutoff: datetime,
                      *, deadline: float | None = None, clock=time.monotonic,
                      wall_clock=lambda: datetime.now(TZ)):
    deadline = deadline if deadline is not None else clock() + 15.0
    expected_ends = closed_window_ends(cutoff, interval)
    if required <= 0 or not expected_ends:
        return _failure(symbol, interval, max(0, required), "WAITING_BAR_CLOSE")
    first = None
    # Retrying format/identity errors is not useful. Only a transient request
    # failure is retried; the alternate source remains independently validated.
    for source, attempts in ((provider, 2), (secondary, 1)):
        if source is None or not callable(getattr(source, "fetch_bars", None)):
            continue
        for attempt in range(attempts):
            remaining = deadline - clock()
            if remaining <= 0:
                return _failure(symbol, interval, required, "MINUTE_FETCH_BUDGET_EXHAUSTED")
            bounded = source
            reserved_node = None
            if isinstance(source, (TencentIntradayAdapter, MootdxAdapter)):
                bounded = copy(source)
                bounded.timeout_seconds = min(source.timeout_seconds, 2.5, remaining / 3)
                if isinstance(source, MootdxAdapter):
                    reserved_node = _reserve_node(source)
                    if reserved_node is None:
                        break
                    # Client factory must be bound to the bounded instance.
                    if getattr(source.client_factory, "__self__", None) is source:
                        bounded.client_factory = bounded._default_factory
                    bounded.nodes = (reserved_node,)
                    bounded.max_pages = min(source.max_pages, 2)
            requested_at = wall_clock()
            try:
                result = bounded.fetch_bars(symbol, interval, required, as_of=cutoff)
            except Exception:
                result = _failure(symbol, interval, required, "TENCENT_REQUEST_FAILED" if source is provider else "NODE_REQUEST_FAILED")
            finally:
                if reserved_node is not None:
                    _release_node(source, reserved_node, result.complete)
            received_at = wall_clock()
            result = result.model_copy(update={"request_started_at": requested_at,
                "response_received_at": received_at})
            if clock() > deadline:
                return _failure(symbol, interval, required, "MINUTE_FETCH_BUDGET_EXHAUSTED")
            if first is None:
                first = result
            bars = tuple(b for b in result.bars if b.symbol == symbol
                         and b.interval == interval and b.bar_end.date() == cutoff.date()
                         and b.bar_end <= cutoff)[-required:]
            valid = (result.complete and len(bars) == required
                     and tuple(b.bar_end for b in bars) == expected_ends[-required:]
                     and not detect_missing_bars(bars, interval, as_of=cutoff))
            # The first 15:00 observation is not final merely because it is
            # non-zero.  After the bounded publication grace, the ordinary
            # stability probes still have to observe the same complete bar
            # before it can enter the frozen decision/archive pack.
            if valid and cutoff.hour == 15 and cutoff.minute == 0:
                if (received_at.date() != cutoff.date()
                        or received_at < cutoff + timedelta(seconds=CLOSE_FINALIZATION_GRACE_SECONDS)):
                    return result.model_copy(update={"complete": False, "reason_code": "CLOSE_BAR_FINALIZATION_UNCONFIRMED"})
            if valid:
                return result.model_copy(update={"bars": bars, "returned_bars": len(bars), "reason_code": "OK", "complete": True})
            if result.complete:
                result = result.model_copy(update={"complete": False, "reason_code": "CURRENT_SESSION_WINDOW_INVALID"})
                first = result
            if result.reason_code != "TENCENT_REQUEST_FAILED" or attempt + 1 >= attempts:
                break
    return first or _failure(symbol, interval, required, "MINUTE_DATA_FETCH_FAILED")


def _failure(symbol, interval, required, reason):
    return FetchResult(symbol=symbol, interval=interval, requested_bars=required,
                       returned_bars=0, bars=(), reason_code=reason, complete=False)
