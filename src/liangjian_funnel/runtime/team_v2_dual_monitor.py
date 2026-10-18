"""Customer A4 first; owned Team execution only uses the same round's remainder.

Legacy acquisition/retries and notifications are never memoized or reordered.
The Team side consumes the last exact legacy result, including pending/failure;
only symbols/windows not requested by legacy may be acquired once by Team.
"""
from concurrent.futures import Future, TimeoutError
from copy import deepcopy
from datetime import datetime, timedelta
from threading import RLock
import time
from zoneinfo import ZoneInfo


def _market_source(settings, live, raw, *, clock, calendar, deadline):
    """Bind owned emotion evidence to the unchanged customer's cache value.

    Ignore only the cache-reader's presentation flags. A changed/degraded
    customer selection cannot borrow evidence from another market result.
    """
    import hashlib
    import json
    from pathlib import Path
    from .team_v2_emotion_baseline import observe_full_market
    from .team_v2_rule_source import _write_once
    from .team_v2_store import canonical
    selected = deepcopy(live)
    for key in ('cache_hit', 'cache_reused'):
        selected.pop(key, None)
    diagnostics = selected.get('diagnostics')
    if isinstance(diagnostics, dict):
        for key in ('cache_hit', 'cache_bucket'):
            diagnostics.pop(key, None)
    identity = hashlib.sha256(canonical(selected).encode()).hexdigest()
    root = Path(settings.root).resolve()
    target = root/'state/team_v2/shared_market'/(identity+'.json')
    if target.resolve() != target:
        raise ValueError('TEAM_SHARED_MARKET_PATH_INVALID')
    if not target.exists() and raw is not None:
        source = observe_full_market(settings, raw, observed_at=clock(), calendar=calendar, deadline=deadline)
        body = dict(market_sha256=identity, known_at=clock().isoformat(), source=source)
        body['body_sha256'] = hashlib.sha256(canonical(body).encode()).hexdigest()
        try:
            _write_once(target, body)
        except FileExistsError:
            pass
    try:
        if target.stat().st_size > 2*1024*1024:
            raise ValueError
        body = json.loads(target.read_bytes()); pin = body.pop('body_sha256')
        known = datetime.fromisoformat(body['known_at'])
        observed = datetime.fromisoformat(live['as_of']); now = clock()
        if (hashlib.sha256(canonical(body).encode()).hexdigest() != pin
                or body['market_sha256'] != identity or known > now
                or observed.date() != now.date() or not 0 <= (now-observed).total_seconds() <= 300):
            raise ValueError
        return body['source']
    except (OSError, KeyError, TypeError, ValueError):
        return dict(status='DATA_BLOCK', reason_code='TEAM_SHARED_MARKET_UNAVAILABLE')


class SharedMinuteSources:
    def __init__(self, application, *, monotonic=None, minute=None):
        self.application = application
        self.monotonic = monotonic or time.monotonic
        self._pending = {}
        self._lock = RLock()
        self.market_state = None
        self.market_snapshot = None
        self.minute = minute.replace(second=0, microsecond=0) if minute is not None else None
        self._sealed = False
        self.allow_market_collection = False
        self._market_future = None

    def capture_market_snapshot(self, snapshot):
        # Capture only: no Team computation/field injection before customer A4.
        with self._lock:
            if not self._sealed:
                self.market_snapshot = snapshot

    def seal(self, market_state):
        with self._lock:
            self.market_state = market_state
            self._sealed = True

    def team_market(self, settings, *, clock, calendar, deadline):
        if self.minute is not None and clock().replace(second=0, microsecond=0) != self.minute:
            raise ValueError('TEAM_SHARED_SOURCE_MINUTE_MOVED')
        if self.monotonic() >= deadline:
            raise ValueError('TEAM_SHARED_SOURCE_DEADLINE')
        # Empty legacy scope has never requested a full-market collection.
        # Preserve the existing G5 source-only path, once and inside remainder.
        if self.allow_market_collection:
            with self._lock:
                acquire = self._market_future is None
                if acquire:self._market_future = Future()
                future = self._market_future
            if acquire:
                from .live_market import load_or_refresh_live_market_state
                from .team_v2_emotion_baseline import observe_full_market
                cutoff=clock().replace(second=0,microsecond=0)-timedelta(minutes=1)
                try:
                    live=load_or_refresh_live_market_state(settings,self.application.market_data,as_of=clock(),
                        minimum_source_as_of=cutoff.replace(minute=cutoff.minute-cutoff.minute%5),
                        snapshot_observer=lambda result:observe_full_market(settings,result,
                            observed_at=clock(),calendar=calendar,deadline=deadline))
                except Exception:
                    future.set_exception(ValueError('TEAM_SHARED_MARKET_UNAVAILABLE'))
                else:future.set_result(live)
            try:return deepcopy(future.result(timeout=max(0,deadline-self.monotonic())))
            except TimeoutError:raise ValueError('TEAM_SHARED_SOURCE_DEADLINE') from None
        live = deepcopy(self.market_state or {'status': 'DATA_BLOCKED',
            'reason_code': 'TEAM_SHARED_MARKET_UNAVAILABLE'})
        live['team_v2_source'] = _market_source(settings, live, self.market_snapshot,
            clock=clock, calendar=calendar, deadline=deadline)
        return live

    def _key(self, kind, args):
        if kind == 'quote':
            symbol, current = args
            return kind, symbol, current.replace(second=0, microsecond=0)
        return (kind, *args)

    def _original(self, kind, args, deadline, future):
        try:
            fetch = getattr(self.application, '_fetch_live_' + kind)
            result = fetch(*args, deadline=deadline)
        except Exception:
            # The original caller keeps its exception. Team sees a finite local
            # reason, not a provider response/credential or a second request.
            future.set_exception(ValueError('TEAM_SHARED_SOURCE_UNAVAILABLE'))
            raise
        future.set_result(result)
        return result

    def _legacy(self, kind, args, deadline):
        future = Future()
        with self._lock:
            self._pending[self._key(kind, args)] = future
        return self._original(kind, args, deadline, future)

    def _team(self, kind, args, deadline):
        if self.minute is not None:
            source_at = args[-1]
            if ((kind == 'quote' and source_at.replace(second=0, microsecond=0) != self.minute)
                    or (kind == 'bars' and source_at != self.minute-timedelta(minutes=1))):
                raise ValueError('TEAM_SHARED_SOURCE_MINUTE_MOVED')
        key = self._key(kind, args)
        with self._lock:
            future = self._pending.get(key)
            acquire = future is None
            if acquire:
                future = Future()
                self._pending[key] = future
        if self.monotonic() >= deadline:
            if acquire:
                future.set_exception(ValueError('TEAM_SHARED_SOURCE_DEADLINE'))
            raise ValueError('TEAM_SHARED_SOURCE_DEADLINE')
        if acquire:
            self._original(kind, args, deadline, future)
        try:
            return deepcopy(future.result(timeout=max(0, deadline-self.monotonic())))
        except TimeoutError:
            raise ValueError('TEAM_SHARED_SOURCE_DEADLINE') from None

    def legacy_bars(self, *args, deadline):
        return self._legacy('bars', args, deadline)

    def legacy_quote(self, *args, deadline):
        return self._legacy('quote', args, deadline)

    def team_bars(self, *args, deadline):
        return self._team('bars', args, deadline)

    def team_quote(self, *args, deadline):
        return self._team('quote', args, deadline)


def _legacy_failed(result):
    if result.get('status') in {'FAILED', 'ERROR', 'BLOCKED', 'TIMED_OUT'}:
        return True
    axes = result.get('observability', {}).get('axes', {})
    if axes.get('job_status') not in (None, 'SUCCEEDED'):
        return True
    return any(row.get('status') == 'FAILED' for row in result.get('notifications', ())
        if isinstance(row, dict))


def _runtime_failure(settings, *, now, reason):
    try:
        from .team_v2_morning import ACCOUNT, _directory
        from .team_v2_store import TeamMeetingStore
        from .team_v2_first_day import POLICY, latch_record
        return latch_record(TeamMeetingStore(_directory(settings)/'team.db'), POLICY,
            account_id=ACCOUNT, now=now, reason_code=reason, first_day_only=False)
    except Exception:
        return dict(status='FAILED', record_required=True,
            reason_code=reason, persistent_record=False, internal_alert='FAILED')


def run_dual_monitor(application, *, now=None, monotonic=None):
    from ..workflow import A4_DECISION_BUDGET_SECONDS, WorkflowError, record_business_reasons
    from .team_v2_monitor import run_monitor_control
    from .team_v2_record_protection import has_owned_exposure, exposure_failure_alert
    from .team_v2_first_day import forced_record
    from .team_v2_morning import ACCOUNT
    monotonic = monotonic or time.monotonic
    started = monotonic()
    deadline = started + A4_DECISION_BUDGET_SECONDS
    current = now or datetime.now(ZoneInfo('Asia/Shanghai'))
    mode = getattr(application.settings, 'a4_trader_mode', 'record')
    if mode not in {'record', 'control'}:
        raise WorkflowError('TRADER_MODE_INVALID')
    shared = SharedMinuteSources(application, monotonic=monotonic, minute=current)
    # The caller never reruns legacy on Team failure. All customer effects and
    # the old ledger have already finished before any owned broker/model work.
    legacy = application.monitor_once(now=now, archive_original_quotes=True,
        deadline_monotonic=deadline, minute_sources=shared)
    if not isinstance(legacy, dict):
        raise WorkflowError('LEGACY_MONITOR_RECEIPT_INVALID')
    shared.seal(legacy.get('live_market_state'))
    remaining = max(0, deadline-monotonic())
    timing = dict(budget_seconds=A4_DECISION_BUDGET_SECONDS,
        remaining_at_team_start_seconds=remaining)
    def finish(team, *, effective_mode=mode, **extra):
        return {**legacy, **extra, 'mode': effective_mode, 'team': team,
            'dual_path': {**timing, 'total_ms': max(0, (monotonic()-started)*1000)}}
    if _legacy_failed(legacy):
        return finish(dict(status='SKIPPED', reason_code='LEGACY_MONITOR_FAILED'))
    if remaining <= 0:
        return finish(dict(status='SKIPPED', reason_code='TEAM_REMAINDER_EXHAUSTED'))
    try:
        latch = forced_record(application.settings, account_id=ACCOUNT, now=current)
        fallback = latch
        if mode == 'record' and fallback is None:
            try:
                owned = has_owned_exposure(application.settings)
            except Exception:
                alert = exposure_failure_alert(application.settings, now=current)
                record_business_reasons({'TEAM_OWNED_EXPOSURE_CHECK_FAILED': 1},
                    incident_key='TEAM_OWNED_EXPOSURE_CHECK_FAILED:'+current.date().isoformat())
                detail = dict(reason_code='TEAM_OWNED_EXPOSURE_CHECK_FAILED', internal_alert=alert)
                return finish(dict(status='SKIPPED', **detail), team_record_exposure=detail)
            if not owned:
                return finish(dict(status='NOT_REQUESTED', reason_code='EXPLICIT_RECORD_NO_EXPOSURE'))
            fallback = {'reason_code': 'EXPLICIT_RECORD_MODE'}
        if application.settings.broker_adapter != 'local_paper':
            reason='TEAM_BROKER_ADAPTER_NOT_ENABLED'
            failure=_runtime_failure(application.settings,now=current,reason=reason)
            return finish(dict(status='FAILED',reason_code=reason),effective_mode='record',
                team_control_fallback=failure)
        result = run_monitor_control(application, now=now, record_fallback=fallback,
            deadline_monotonic=deadline, minute_sources=shared)
        if result.get('status') in {'FAILED', 'ERROR', 'DEGRADED', 'DATA_BLOCKED', 'CONTROL_RECORD_REQUIRED'}:
            failure = _runtime_failure(application.settings, now=current, reason='TEAM_MONITOR_DEGRADED')
            return finish(result, effective_mode='record', team_control_fallback=failure)
        return finish(result, effective_mode='record' if fallback is not None or result.get('mode') == 'record' else mode,
            **({'team_control_fallback': latch} if latch is not None else {}))
    except Exception:
        failure = _runtime_failure(application.settings, now=current, reason='TEAM_MONITOR_EXCEPTION')
        return finish(dict(status='FAILED', reason_code='TEAM_MONITOR_EXCEPTION'),
            effective_mode='record', team_control_fallback=failure)
