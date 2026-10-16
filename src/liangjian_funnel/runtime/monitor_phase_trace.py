"""Fail-isolated monitor timing observer, installed before research imports."""
from __future__ import annotations
from contextlib import contextmanager, nullcontext
from datetime import datetime
import faulthandler
import json
import os
from pathlib import Path
import re
import signal
import sys
import tempfile
import threading
import time
from functools import wraps

PHASES=('startup','fetch','judge','persist','notify')
_CURRENT=None


class MonitorPhaseTrace:
    def __init__(self,directory: str | Path,run_id: str):
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,200}',run_id or '') or run_id in {'.','..'}:
            raise ValueError('MONITOR_TRACE_RUN_ID_INVALID')
        self.directory=Path(directory).resolve();self.directory.mkdir(parents=True,exist_ok=True)
        self.run_id=run_id;self.started=time.monotonic();self.phase_started=self.started;self.current_phase='startup'
        self.timings={name:{'duration_ms':0.0,'calls':0} for name in PHASES}
        self.business_reason_counts={};self.business_status_observed=False
        self.active={};self.lock=threading.RLock();self.old_handler=None;self.finished=False;self.native_signal=False
        self.stack_path=self.directory/(run_id+'.stacks.log')
        self.stack_handle=self.stack_path.open('a+',encoding='utf-8')
        self._save(self.directory/(run_id+'.phase.json'),self.state())

    def mark(self,name):
        if name not in PHASES:raise ValueError('MONITOR_TRACE_PHASE_INVALID')
        with self.lock:
            at=time.monotonic()
            if self.current_phase is not None:
                self.timings[self.current_phase]['duration_ms']+=max(0,(at-self.phase_started)*1000)
                self.timings[self.current_phase]['calls']+=1
            self.current_phase=name;self.phase_started=at
        self._save(self.directory/(self.run_id+'.phase.json'),self.state())

    @contextmanager
    def measure(self,name):
        if name not in PHASES:raise ValueError('MONITOR_TRACE_PHASE_INVALID')
        at=time.monotonic();key=(threading.get_ident(),at)
        with self.lock:self.active[key]={'phase':name,'started_at_monotonic':at}
        try:yield
        finally:
            with self.lock:
                self.timings[name]['duration_ms']+=max(0,(time.monotonic()-at)*1000)
                self.timings[name]['calls']+=1;self.active.pop(key,None)

    def state(self):
        # Signal handler must not wait on another thread's lock or capture
        # locals/arguments. Copies may include an in-progress scope explicitly.
        at=time.monotonic();timings={key:dict(value) for key,value in self.timings.copy().items()}
        if self.current_phase is not None:
            timings[self.current_phase]['duration_ms']+=max(0,(at-self.phase_started)*1000)
        return {'schema_version':'monitor-phase-trace/1','run_id':self.run_id,
            'observed_at':datetime.now().astimezone().isoformat(),'current_phase':self.current_phase,
            'active_scopes':list(self.active.copy().values()),'phase_timings':timings,
            'elapsed_ms':max(0,(at-self.started)*1000),
            'business_reason_counts':dict(self.business_reason_counts),
            'business_status_observed':self.business_status_observed,
            'timing_basis':'WALL_SCOPES_MAY_OVERLAP_PERSIST_IS_NESTED_NOT_ADDITIVE_CPU_TIME'}

    def note_business_reasons(self,reasons):
        # Counts and stable codes only; no exception messages, URLs or secrets.
        safe={code:count for code,count in reasons.items()
              if isinstance(code,str) and re.fullmatch(r'[A-Z][A-Z0-9_]{1,79}',code)
              and isinstance(count,int) and not isinstance(count,bool) and 0<count<=10000}
        with self.lock:
            self.business_status_observed=True
            for code,count in safe.items():
                self.business_reason_counts[code]=max(count,self.business_reason_counts.get(code,0))
        self._save(self.directory/(self.run_id+'.phase.json'),self.state())

    def _save(self,path,payload,*,immutable=False):
        temporary=None
        try:
            if immutable and path.exists():return
            with tempfile.NamedTemporaryFile('w',encoding='utf-8',prefix='.'+self.run_id+'-',suffix='.tmp',dir=self.directory,delete=False) as f:
                temporary=Path(f.name);json.dump(payload,f,ensure_ascii=False);f.flush();os.fsync(f.fileno())
            if immutable:os.link(temporary,path)
            else:os.replace(temporary,path);temporary=None
        except OSError as exc:
            # Observability failure must never change the strategy result.
            sys.stderr.write('MONITOR_TRACE_WRITE_FAILED:'+type(exc).__name__+'\n')
        finally:
            if temporary is not None:
                try:temporary.unlink(missing_ok=True)
                except OSError:pass

    def snapshot(self,reason):
        path=self.directory/(self.run_id+'.json')
        if path.exists():return
        try:
            faulthandler.dump_traceback(file=self.stack_handle,all_threads=True)
            self.stack_handle.flush();self.stack_handle.seek(0)
            stack=self.stack_handle.read(128*1024)
            payload={**self.state(),'reason':reason,'stack':stack,'locals_recorded':False}
            self._save(path,payload,immutable=True)
        except (OSError,RuntimeError,ValueError) as exc:
            sys.stderr.write('MONITOR_TRACE_SNAPSHOT_FAILED:'+type(exc).__name__+'\n')

    def install_signal(self):
        if hasattr(signal,'SIGUSR1') and threading.current_thread() is threading.main_thread():
            self.old_handler=signal.getsignal(signal.SIGUSR1)
            signal.signal(signal.SIGUSR1,lambda signum,frame:self.snapshot('SIGUSR1'))
            # C-level stack capture does not wait for a SQLite/network C call
            # to return before Python can service its pending signal handler.
            if hasattr(faulthandler,'register'):
                try:
                    faulthandler.register(signal.SIGUSR1,file=self.stack_handle,all_threads=True,chain=True)
                    self.native_signal=True
                except (OSError,RuntimeError,ValueError):pass

    def finish(self):
        with self.lock:
            if not self.finished:
                if self.current_phase is not None:
                    self.timings[self.current_phase]['duration_ms']+=max(0,(time.monotonic()-self.phase_started)*1000)
                    self.timings[self.current_phase]['calls']+=1
                self.current_phase=None;self.finished=True
        payload={**self.state(),'trace_status':'FINALIZED','business_status_inferred':False}
        self._save(self.directory/(self.run_id+'.receipt.json'),payload)
        return payload

    def close(self, *, exiting=False):
        if self.native_signal:faulthandler.unregister(signal.SIGUSR1)
        if self.old_handler is not None:
            # Parent may send its 50s signal while the child is exiting.
            signal.signal(signal.SIGUSR1,signal.SIG_IGN if exiting else self.old_handler)
        self.stack_handle.close()


def install_monitor_trace():
    global _CURRENT
    if os.getenv('LIANGJIAN_MONITOR_TRACE_DIR') and os.getenv('LIANGJIAN_PARENT_RUN_ID'):
        try:
            _CURRENT=MonitorPhaseTrace(os.environ['LIANGJIAN_MONITOR_TRACE_DIR'],os.environ['LIANGJIAN_PARENT_RUN_ID'])
            _CURRENT.install_signal()
            # Parent sends SIGUSR1 only after this child readiness marker.
            sys.stderr.write('MONITOR_TRACE_READY\n');sys.stderr.flush()
        except (OSError,ValueError) as exc:
            _CURRENT=None;sys.stderr.write('MONITOR_TRACE_INIT_FAILED:'+type(exc).__name__+'\n')
    return _CURRENT


def mark_phase(name):
    if _CURRENT is not None:_CURRENT.mark(name)


def phase_scope(name):
    return _CURRENT.measure(name) if _CURRENT is not None else nullcontext()


def phase_timings():
    return _CURRENT.state()['phase_timings'] if _CURRENT is not None else {}


def record_business_reasons(reasons):
    if _CURRENT is not None:_CURRENT.note_business_reasons(reasons)


def timed_phase(name):
    def decorate(function):
        @wraps(function)
        def wrapped(*args,**kwargs):
            with phase_scope(name):return function(*args,**kwargs)
        return wrapped
    return decorate
