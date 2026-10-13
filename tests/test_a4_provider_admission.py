"""Counterexamples for timed-out holders, queueing and duplicate live windows."""
from datetime import datetime
from threading import Event, Lock, Thread
import time
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime.bounded_work import BoundedWorkGate, run_many_bounded
from liangjian_funnel.data import live_fetch
from liangjian_funnel.data.mootdx import FetchResult, MinuteBar, MootdxAdapter
from liangjian_funnel.data.session_windows import TZ
from liangjian_funnel.data.publication import confirm_publications


def test_admission_wait_recovers_when_old_timed_out_worker_releases():
    gate=BoundedWorkGate(1);release=Event();started=Event()
    def stuck():started.set();release.wait(1)
    assert gate.call(stuck,deadline=time.monotonic()+.015).status=='TIMED_OUT'
    assert started.is_set()
    Thread(target=lambda:(time.sleep(.04),release.set()),daemon=True).start()
    result=gate.call(lambda:'fresh',deadline=time.monotonic()+.3,wait_for_slot=True)
    assert result.status=='READY' and result.value=='fresh'


def test_admission_wait_does_not_start_after_deadline_or_expand_pool():
    gate=BoundedWorkGate(1);release=Event();calls=[]
    assert gate.call(lambda:release.wait(1),deadline=time.monotonic()+.01).status=='TIMED_OUT'
    started=time.monotonic()
    try:
        result=gate.call(lambda:calls.append(1),deadline=started+.04,wait_for_slot=True)
        assert result.status=='BACKPRESSURE' and result.reason_code=='ADMISSION_DEADLINE_EXCEEDED'
        assert time.monotonic()-started<.15 and not calls
    finally:release.set()


def test_same_live_request_cannot_be_launched_while_original_still_runs():
    gate=BoundedWorkGate(2);release=Event();calls=[]
    def stuck():calls.append(1);release.wait(1)
    try:
        first=gate.call(stuck,deadline=time.monotonic()+.015,operation_key=('S','1m','t'))
        assert first.status=='TIMED_OUT'
        second=gate.call(stuck,deadline=time.monotonic()+.1,operation_key=('S','1m','t'))
        assert second.status=='BACKPRESSURE' and second.reason_code=='REQUEST_ALREADY_INFLIGHT'
        assert len(calls)==1
        assert gate.call(lambda:'other',deadline=time.monotonic()+.1,operation_key=('OTHER','1m','t')).value=='other'
    finally:release.set()


def test_live_window_waits_for_real_source_slot_without_fallback_or_new_capacity(monkeypatch):
    gate=BoundedWorkGate(1);release=Event()
    monkeypatch.setattr(live_fetch,'_PRIMARY_WORK',gate)
    assert gate.call(lambda:release.wait(1),deadline=time.monotonic()+.01).status=='TIMED_OUT'
    at=datetime(2026,10,13,9,31,tzinfo=TZ)
    row=MinuteBar(symbol='000001.SZ',interval='1m',bar_end=at,open=10,high=10,low=10,close=10,volume=100,amount=1000,source_id='TEST')
    value=FetchResult(symbol=row.symbol,interval='1m',requested_bars=1,returned_bars=1,bars=(row,),complete=True,reason_code='OK')
    Thread(target=lambda:(time.sleep(.04),release.set()),daemon=True).start()
    result=live_fetch.fetch_live_window(SimpleNamespace(fetch_bars=lambda *a,**k:value),None,row.symbol,'1m',1,at,deadline=time.monotonic()+.4)
    assert result.complete and result.source_attempts[0]['complete']


def test_live_transient_retry_never_duplicates_a_hung_network_request(monkeypatch):
    monkeypatch.setattr(live_fetch,'_PRIMARY_WORK',BoundedWorkGate(8))
    release=Event();calls=[]
    def stuck(*a,**k):calls.append(1);release.wait(1);return None
    try:
        result=live_fetch.fetch_live_window(SimpleNamespace(fetch_bars=stuck),None,'000001.SZ','1m',1,
            datetime(2026,10,13,9,31,tzinfo=TZ),deadline=time.monotonic()+.12)
        assert not result.complete and len(calls)==1
        assert any(x.get('admission_reason')=='REQUEST_ALREADY_INFLIGHT' for x in result.source_attempts)
    finally:release.set()


def test_fifty_symbol_round_recovers_without_more_than_eight_transport_workers(monkeypatch):
    monkeypatch.setattr(live_fetch,'_PRIMARY_WORK',BoundedWorkGate(8))
    at=datetime(2026,10,13,9,31,tzinfo=TZ);lock=Lock()
    calls={};active=0;peak=0
    def source(symbol,*a,**k):
        nonlocal active,peak
        with lock:
            active+=1;peak=max(peak,active);calls[symbol]=calls.get(symbol,0)+1
            slow=len(calls)<=8 and calls[symbol]==1
        try:
            time.sleep(.45 if slow else .001)
            row=MinuteBar(symbol=symbol,interval='1m',bar_end=at,open=10,high=10,low=10,close=10,
                volume=999 if slow else 100,amount=1000,source_id='TEST')
            return FetchResult(symbol=symbol,interval='1m',requested_bars=1,returned_bars=1,bars=(row,),complete=True,reason_code='OK')
        finally:
            with lock:active-=1
    provider=SimpleNamespace(fetch_bars=source);deadline=time.monotonic()+1.2
    def fetch(symbol):return symbol,{'1m':live_fetch.fetch_live_window(provider,None,symbol,'1m',1,at,deadline=deadline)}
    symbols=[f'{n:06d}.SZ' for n in range(50)]
    initial=run_many_bounded({s:lambda s=s:fetch(s) for s in symbols},deadline=deadline,gate=BoundedWorkGate(8))
    assert all(v.status=='READY' for v in initial.values())
    selected=confirm_publications({s:v.value[1] for s,v in initial.items()},fetch,at=at,deadline=deadline,closed_window=True)
    assert len(selected)==50 and peak<=8
    assert all(p['1m'].complete and not p.get('publication_error') for p in selected.values())
    assert all(p['1m'].bars[0].volume==100 for p in selected.values()),'Late 999-volume results must never reenter the round'


def test_local_refusal_does_not_cool_down_untried_secondary_node(monkeypatch):
    gate=BoundedWorkGate(1);release=Event();calls=[]
    monkeypatch.setattr(live_fetch,'_SECONDARY_WORK',gate)
    assert gate.call(lambda:release.wait(1),deadline=time.monotonic()+.01).status=='TIMED_OUT'
    source=MootdxAdapter(nodes=[('127.0.0.1',7709)],client_factory=lambda node:calls.append(node))
    try:
        value=live_fetch.fetch_live_window(None,source,'000001.SZ','1m',1,datetime(2026,10,13,9,31,tzinfo=TZ),deadline=time.monotonic()+.04)
        assert not value.complete and not calls
        assert not source._live_busy_nodes and not source._live_node_health
    finally:release.set()


def test_worker_start_failure_releases_slot_and_request_identity(monkeypatch):
    from liangjian_funnel.runtime import bounded_work
    gate=BoundedWorkGate(1);real_thread=bounded_work.Thread
    class RefusedThread:
        def __init__(self,**kw):pass
        def start(self):raise RuntimeError('fixture thread start failure')
    monkeypatch.setattr(bounded_work,'Thread',RefusedThread)
    with pytest.raises(RuntimeError):gate.call(lambda:1,deadline=time.monotonic()+.1,operation_key='x')
    monkeypatch.setattr(bounded_work,'Thread',real_thread)
    assert gate.call(lambda:2,deadline=time.monotonic()+.1,operation_key='x').value==2
