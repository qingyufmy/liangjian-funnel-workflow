"""Close-only parent clock: data30/research59/finalize1, total <=90min.

Source clients opt into request hooks, not hostname classification or a global
HTTP monkeypatch. The object is shared with owned collection worker threads.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
import math
import threading
import time

import httpx

_active = None
_owner = threading.RLock()


class CloseStageBudgetExceeded(httpx.TimeoutException):
    pass


@dataclass
class CloseStageBudget:
    started: float
    total_seconds: float
    monotonic: object = time.monotonic
    research_started: float | None = None
    finalize_started: float | None = None
    closed: bool = False
    current_symbol: str | None = None
    source_attempts_registered: int = 0
    exhausted_stages: set = field(default_factory=set)
    on_exhausted: object = None
    notice_failures: list = field(default_factory=list)
    _lock: object = field(default_factory=threading.Lock,repr=False)

    @classmethod
    def from_parent(cls, configured, *, parent_deadline_ms=None, monotonic=time.monotonic,
                    wall_time=time.time):
        total = min(5400.0, float(configured))
        if not math.isfinite(total) or total <= 0:
            raise ValueError('CLOSE_STAGE_BUDGET_INVALID')
        now = monotonic()
        elapsed = 0.0
        if parent_deadline_ms is not None:
            deadline = float(parent_deadline_ms)/1000
            if not math.isfinite(deadline) or deadline <= 0:
                raise ValueError('INVALID_PARENT_CLOSE_DEADLINE')
            # The Node parent still owns the absolute 90 minute stop. Entering
            # Python or a continuation cannot create another fresh 90 minutes.
            elapsed = max(0.0, 5400.0-(deadline-wall_time()))
        return cls(now-elapsed, total, monotonic)

    @property
    def data_deadline(self):
        return self.started+self.total_seconds/3

    @property
    def total_deadline(self):
        return self.started+self.total_seconds

    @property
    def research_deadline(self):
        start = self.research_started if self.research_started is not None else self.monotonic()
        return min(start+self.total_seconds*59/90,
                   self.total_deadline-self.total_seconds/90)

    def enter_research(self):
        if self.research_started is None:
            self.research_started = self.monotonic()
        self.check('RESEARCH')

    def enter_finalize(self):
        if self.finalize_started is None:
            self.finalize_started = self.monotonic()
        self.check('FINALIZE')

    def remaining(self, phase):
        deadline = (self.data_deadline if phase=='DATA' else self.research_deadline
                    if phase=='RESEARCH' else min(self.total_deadline,
                        (self.finalize_started if self.finalize_started is not None else self.monotonic())+self.total_seconds/90))
        return max(0.0, deadline-self.monotonic()) if not self.closed else 0.0

    def check(self, phase):
        remaining = self.remaining(phase)
        if remaining <= 0:
            self.mark_exhausted(phase)
            raise CloseStageBudgetExceeded(phase+'_STAGE_BUDGET_EXHAUSTED')
        return remaining

    def mark_exhausted(self,phase):
        with self._lock:
            if phase in self.exhausted_stages:return
            self.exhausted_stages.add(phase)
        if self.on_exhausted is not None:
            try:self.on_exhausted(phase,self.receipt())
            except Exception as exc:self.notice_failures.append(dict(phase=phase,exception_class=type(exc).__name__))

    def register_source_attempt(self):
        remaining=self.check('DATA')
        with self._lock:self.source_attempts_registered+=1
        return remaining

    def receipt(self):
        return dict(schema_version='close-stage-budget/1', authority='CLAUDE_0937',
            total_seconds=self.total_seconds, data_seconds=self.total_seconds/3,
            research_seconds=self.total_seconds*59/90, finalize_seconds=self.total_seconds/90,
            elapsed_seconds=max(0.0,self.monotonic()-self.started),
            remaining_seconds=max(0.0,self.total_deadline-self.monotonic()),
            phase='FINALIZE' if self.finalize_started is not None else
                  'RESEARCH' if self.research_started is not None else 'DATA',
            current_symbol=self.current_symbol,
            source_attempts_registered=self.source_attempts_registered,
            immediate_notice_failures=list(self.notice_failures),
            registered_attempts_are_not_http_receipts=True,
            exhausted_stages=sorted(self.exhausted_stages), parent_clock_not_reset=True)


def current_budget():
    return _active


@contextmanager
def budget_scope(budget):
    global _active
    with _owner:
        if _active is not None and _active is not budget:
            raise ValueError('CLOSE_STAGE_BUDGET_NESTED_RESET_FORBIDDEN')
        previous = _active
        _active = budget
    try:
        yield budget
    finally:
        with _owner:
            _active = previous
            if previous is None:
                budget.closed = True


def bounded_data_sleep(sleeper):
    bound = current_budget()
    @wraps(sleeper)
    def sleep(seconds):
        nonlocal bound
        if bound is None:bound=current_budget()
        budget = bound
        if budget is None:
            return sleeper(seconds)
        # No queued rate-limit/retry sleep may outlive the data phase.
        remaining = budget.remaining('DATA')
        if remaining > 0:
            return sleeper(min(max(0.0,float(seconds)),remaining))
    return sleep


class _DataStream(httpx.SyncByteStream):
    def __init__(self, original, budget):
        self.original,self.budget=original,budget
        self.closed=False
    def __iter__(self):
        try:
            for chunk in self.original:
                self.budget.check('DATA')
                yield chunk
        except BaseException:
            self.close()
            raise
    def close(self):
        if not self.closed:
            self.closed=True;self.original.close()


def configure_source_client(client):
    """Preserve Client.send(request=...) and Client.stream; add owned hooks."""
    if not hasattr(client,'event_hooks'):
        if current_budget() is not None:raise ValueError('SOURCE_BUDGET_CLIENT_UNSUPPORTED')
        return client
    if getattr(client,'_liangjian_data_deadline_hook',False):
        return client
    bound=current_budget()
    def before(request):
        nonlocal bound
        if bound is None:bound=current_budget()
        budget=bound
        if budget is None:return
        remaining=budget.register_source_attempt()
        timeout=dict(request.extensions.get('timeout') or {})
        for key in ('connect','read','write','pool'):
            value=timeout.get(key)
            timeout[key]=remaining if value is None else min(float(value),remaining)
        request.extensions['timeout']=timeout
    def received(response):
        budget=bound or current_budget()
        if budget is None:return
        try:budget.check('DATA')
        except BaseException:
            response.close();raise
        if not response.is_stream_consumed:
            response.stream=_DataStream(response.stream,budget)
    client.event_hooks['request'].insert(0,before)
    client.event_hooks['response'].append(received)
    client._liangjian_data_deadline_hook=True
    return client


def configure_source_session(session):
    """Requests sources keep redirects/retries, bounded at every actual send."""
    if not hasattr(session,'send'):
        if current_budget() is not None:raise ValueError('SOURCE_BUDGET_SESSION_UNSUPPORTED')
        return session
    if getattr(session,'_liangjian_data_deadline_hook',False):return session
    bound=current_budget();original=session.send
    @wraps(original)
    def send(request, **kwargs):
        nonlocal bound
        if bound is None:bound=current_budget()
        budget=bound
        if budget is None:return original(request,**kwargs)
        remaining=budget.register_source_attempt();timeout=kwargs.get('timeout')
        if isinstance(timeout,tuple):timeout=tuple(remaining if v is None else min(float(v),remaining) for v in timeout)
        else:timeout=remaining if timeout is None else min(float(timeout),remaining)
        wanted_stream=kwargs.get('stream',False)
        kwargs.update(timeout=timeout,stream=True)
        response=original(request,**kwargs);iterate=response.iter_content
        @wraps(iterate)
        def content(*args,**options):
            try:
                for chunk in iterate(*args,**options):
                    budget.check('DATA');yield chunk
            except BaseException:
                response.close();raise
        response.iter_content=content
        if not wanted_stream:
            try:response.content
            except BaseException:
                response.close();raise
        return response
    session.send=send;session._liangjian_data_deadline_hook=True
    return session


def source_get(url, params=None, **kwargs):
    import requests
    if current_budget() is None:return requests.get(url,params=params,**kwargs)
    with configure_source_session(requests.Session()) as session:
        return session.get(url,params=params,**kwargs)
