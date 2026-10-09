"""Explicit, bounded, read-only Linux samples; not connected to any workflow.

Identity is supplied, never guessed. Source labels describe parsing, not host
authentication. A sampled maximum is only a lower bound; host swap is not a
process/run attribution. No daemon, proc writes, receipts, or process control.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from pathlib import Path, PurePosixPath
import re
import sys
import time

from .resource_evidence import MetricObservation, ResourceObservation

_STATUS = {'RSS_CURRENT':'VmRSS','PROCESS_RSS_LIFETIME_PEAK':'VmHWM','PROCESS_VMSWAP':'VmSwap'}
_CACHE = {'PAGECACHE_CACHED':'Cached','PAGECACHE_DIRTY':'Dirty','PAGECACHE_WRITEBACK':'Writeback'}
_PSI = {'PSI_MEMORY_SOME_TOTAL_US':'some','PSI_MEMORY_FULL_TOTAL_US':'full'}
_CGROUP = ('CGROUP_MEMORY_CURRENT','CGROUP_MEMORY_EVENTS_OOM','CGROUP_MEMORY_EVENTS_OOM_KILL')
_MAX_SAMPLES = 10_000


def _metric(name, value, unit, source, support='AVAILABLE'):
    try:
        return MetricObservation(name,value,unit,support,source)
    except ValueError:
        return MetricObservation(name,None,unit,'ERROR',source)


def _number(lines, name, *, suffix=''):
    rows = [line.partition(':')[2].strip() for line in lines if line.partition(':')[0] == name]
    if not rows:
        return None, 'UNSUPPORTED'
    if len(rows) != 1 or re.fullmatch(r'[0-9]+'+suffix, rows[0]) is None:
        return None, 'ERROR'
    try:
        return int(rows[0].split()[0]), 'AVAILABLE'
    except ValueError:
        return None,'ERROR'


def parse_status(text, *, source='LINUX_PROC_STATUS'):
    result=[]
    for metric,field in _STATUS.items():
        value,unit,state=_value_unit(text.splitlines(),field)
        result.append(_metric(metric,value,unit,source,state))
    return tuple(result)


def _value_unit(lines, field):
    value, support = _number(lines,field,suffix=r'\s+kB')
    return value, 'KiB', support


def parse_meminfo(text, *, source='LINUX_PROC_MEMINFO'):
    lines = text.splitlines()
    result = []
    total, ts = _number(lines,'SwapTotal',suffix=r'\s+kB')
    free, fs = _number(lines,'SwapFree',suffix=r'\s+kB')
    support = 'ERROR' if 'ERROR' in (ts,fs) or (total is not None and free is not None and free > total) else (
        'UNSUPPORTED' if 'UNSUPPORTED' in (ts,fs) else 'AVAILABLE')
    result.append(_metric('SYSTEM_SWAP_USED',total-free if support=='AVAILABLE' else None,'KiB',source,support))
    for name,field in _CACHE.items():
        value, unit, state = _value_unit(lines,field)
        result.append(_metric(name,value,unit,source,state))
    return tuple(result)


def parse_pressure(text, *, source='LINUX_PSI_MEMORY'):
    result = []
    for name,kind in _PSI.items():
        rows = [line.split()[1:] for line in text.splitlines() if line.split() and line.split()[0]==kind]
        totals = [token[6:] for row in rows for token in row if token.startswith('total=')]
        state = 'UNSUPPORTED' if not rows else 'ERROR' if len(rows)!=1 or len(totals)!=1 or re.fullmatch('[0-9]+',totals[0]) is None else 'AVAILABLE'
        try:
            value=int(totals[0]) if state=='AVAILABLE' else None
        except ValueError:
            value,state=None,'ERROR'
        result.append(_metric(name,value,'us',source,state))
    return tuple(result)


@dataclass(frozen=True)
class SamplerIdentity:
    run_id: str
    invocation_id: str
    pid: int
    process_started_at: datetime
    host_id: str
    cgroup_id: str | None = None

    def __post_init__(self):
        # Reuse the contract's strict identity/time validation, without guessing.
        ResourceObservation(**self.__dict__,observed_at=self.process_started_at,metrics=())


@dataclass(frozen=True)
class CgroupV2Binding:
    root: Path
    directory: Path
    relative_path: str
    cgroup_id: str


@dataclass(frozen=True)
class SamplingBatch:
    observations: tuple[ResourceObservation, ...]
    missed_interval_count: int
    truncated: bool
    unsupported_counts: dict[str, int]
    sample_issues: tuple[tuple[str, ...], ...]
    interval_seconds: float
    requested_duration_seconds: float
    sample_capacity: int
    attempt_count: int
    late_sample_count: int
    late_sample_issues: tuple[tuple[str, ...], ...]
    read_spans: tuple[tuple[datetime, datetime], ...]
    late_read_spans: tuple[tuple[datetime, datetime], ...]
    sampled_peak_is_exact: bool = False


class LinuxResourceSampler:
    def __init__(self, identity: SamplerIdentity, *, cgroup: CgroupV2Binding | None = None,
                 fixture_proc_root: Path | None = None,
                 utc_now=lambda:datetime.now(timezone.utc), monotonic=time.monotonic, wait=time.sleep):
        if not isinstance(identity,SamplerIdentity):
            raise ValueError('EXPLICIT_SAMPLER_IDENTITY_REQUIRED')
        self.identity, self.cgroup = identity, cgroup
        self.fixture = fixture_proc_root is not None
        self.proc = Path(fixture_proc_root) if self.fixture else Path('/proc')
        self.utc_now, self.monotonic, self.wait = utc_now, monotonic, wait
        self.last_issues = ()
        self.last_read_span = None

    @staticmethod
    def _read(path):
        try:
            with path.open('r',encoding='utf-8') as handle:
                text = handle.read(262145)
            if len(text)>262144:
                return None,'ERROR'
            return text,'AVAILABLE'
        except FileNotFoundError:
            return None,'UNAVAILABLE'
        except (OSError,UnicodeError):
            return None,'ERROR'

    def _cgroup_metrics(self, source, issues):
        binding = self.cgroup
        fallback = lambda state: tuple(_metric(name,None,'bytes' if name==_CGROUP[0] else 'count',source,state) for name in _CGROUP)
        if binding is None:
            issues.append('CGROUP_BINDING_NOT_SUPPLIED')
            return fallback('UNSUPPORTED')
        try:
            root, directory = Path(binding.root).absolute(), Path(binding.directory).absolute()
            relative = PurePosixPath(binding.relative_path)
            if (binding.cgroup_id != self.identity.cgroup_id or not relative.is_absolute()
                    or '..' in relative.parts or relative.as_posix()!=binding.relative_path
                    or root.resolve()!=root or directory.resolve()!=directory
                    or not directory.is_relative_to(root)
                    or directory != root.joinpath(*relative.parts[1:])):
                raise ValueError('CGROUP_BINDING_PATH_OR_IDENTITY_INVALID')
            membership, ms = self._read(self.proc/'self'/'cgroup')
            mountinfo, fs = self._read(self.proc/'self'/'mountinfo')
            if ms!='AVAILABLE' or fs!='AVAILABLE':
                raise ValueError('CGROUP_MEMBERSHIP_OR_MOUNT_EVIDENCE_MISSING')
            if [line[3:] for line in membership.splitlines() if line.startswith('0::')] != [binding.relative_path]:
                raise ValueError('CGROUP_MEMBERSHIP_MISMATCH')
            mounted = False
            for line in mountinfo.splitlines():
                before, separator, after = line.partition(' - ')
                fields, post = before.split(), after.split()
                if separator and len(fields)>=6 and post and post[0]=='cgroup2':
                    point = re.sub(r'\\([0-7]{3})',lambda match:chr(int(match[1],8)),fields[4])
                    if Path(point).absolute()==root and fields[3]=='/':
                        mounted=True
            if not mounted:
                raise ValueError('CGROUP_V2_ROOT_MOUNT_NOT_CONFIRMED')
            paths = (root/'cgroup.controllers',directory/'memory.current',directory/'memory.events')
            if any(path.is_symlink() or path.resolve()!=path for path in paths):
                raise ValueError('CGROUP_FILE_SYMLINK_OR_PATH_INVALID')
            controllers, support = self._read(paths[0])
            if support!='AVAILABLE' or 'memory' not in controllers.split():
                raise ValueError('CGROUP_MEMORY_CONTROLLER_NOT_CONFIRMED')
            current, cs = self._read(paths[1])
            value = int(current.strip()) if cs=='AVAILABLE' and re.fullmatch('[0-9]+',current.strip()) else None
            result = [_metric(_CGROUP[0],value,'bytes',source,cs if cs!='AVAILABLE' else 'AVAILABLE' if value is not None else 'ERROR')]
            events, es = self._read(paths[2])
            lines = [line.replace(' ',':',1) for line in events.splitlines()] if es=='AVAILABLE' else []
            for name,key in zip(_CGROUP[1:],('oom','oom_kill')):
                value,state = _number(lines,key) if es=='AVAILABLE' else (None,es)
                result.append(_metric(name,value,'count',source,state))
            return tuple(result)
        except (ValueError,OSError,TypeError):
            issues.append('CGROUP_BINDING_NOT_VALIDATED')
            return fallback('UNAVAILABLE')

    def sample(self):
        begin = self.utc_now()
        issues, result = [], []
        sources = [('self/status',parse_status,'LINUX_PROC_STATUS'),
                   ('meminfo',parse_meminfo,'LINUX_PROC_MEMINFO'),
                   ('pressure/memory',parse_pressure,'LINUX_PSI_MEMORY')]
        if not self.fixture and sys.platform!='linux':
            self.last_issues=('PLATFORM_UNSUPPORTED',)
            for _,parser,source in sources:
                result.extend(_metric(item.metric,None,item.unit,source,'UNSUPPORTED') for item in parser(''))
            result.extend(_metric(name,None,'bytes' if name==_CGROUP[0] else 'count','LINUX_CGROUP_V2','UNSUPPORTED') for name in _CGROUP)
        else:
            process_identity_valid=False
            for name,parser,origin in sources:
                source = 'FIXTURE' if self.fixture else origin
                text,state = self._read(self.proc/name)
                parsed = parser(text or '',source=source)
                if state!='AVAILABLE':
                    issues.append(name+':'+state)
                    parsed = tuple(_metric(item.metric,None,item.unit,source,state) for item in parsed)
                elif name=='self/status':
                    pid, ps = _number(text.splitlines(),'Pid')
                    if ps!='AVAILABLE' or pid!=self.identity.pid:
                        issues.append('PROCESS_PID_MISMATCH')
                        parsed = tuple(_metric(item.metric,None,item.unit,source,'ERROR') for item in parsed)
                    else:
                        process_identity_valid=True
                result.extend(parsed)
            source='FIXTURE' if self.fixture else 'LINUX_CGROUP_V2'
            if process_identity_valid:
                result.extend(self._cgroup_metrics(source,issues))
            else:
                issues.append('CGROUP_PROCESS_IDENTITY_NOT_VALIDATED')
                result.extend(_metric(name,None,'bytes' if name==_CGROUP[0] else 'count',source,'UNAVAILABLE') for name in _CGROUP)
            self.last_issues=tuple(issues)
        complete = self.utc_now()
        self.last_read_span=(begin,complete)
        if complete<begin:
            raise ValueError('UTC_READ_CLOCK_REGRESSED')
        return ResourceObservation(**self.identity.__dict__,observed_at=complete,metrics=tuple(result))

    def collect(self, *, duration_seconds, interval_seconds, max_samples):
        if (type(max_samples) is not int or not 1<=max_samples<=_MAX_SAMPLES
                or type(duration_seconds) not in (int,float) or not math.isfinite(duration_seconds) or duration_seconds<0
                or type(interval_seconds) not in (int,float) or not math.isfinite(interval_seconds) or interval_seconds<=0):
            raise ValueError('BOUNDED_FINITE_SCHEDULE_REQUIRED')
        samples, diagnostics, counts, spans, late_issues, late_spans = [], [], {}, [], [], []
        previous=None
        def clock():
            nonlocal previous
            value=self.monotonic()
            if type(value) not in (int,float) or not math.isfinite(value):
                raise ValueError('FINITE_SCHEDULE_CLOCK_REQUIRED')
            if previous is not None and value<previous:
                raise ValueError('MONOTONIC_CLOCK_REGRESSED')
            previous=value
            return value
        start, slot, missed, attempts = clock(), 0, 0, 0
        end = start+duration_seconds
        if not math.isfinite(start) or not math.isfinite(end) or not math.isfinite(duration_seconds/interval_seconds):
            raise ValueError('FINITE_SCHEDULE_CLOCK_REQUIRED')
        truncated = False
        while start+slot*interval_seconds <= end:
            if attempts==max_samples:
                truncated=True
                break
            due = start+slot*interval_seconds
            now = clock()
            if now<due:
                self.wait(due-now)
                if clock()<due:
                    raise ValueError('WAIT_DID_NOT_REACH_FIXED_DEADLINE')
            if clock()>end:
                missed+=math.floor(duration_seconds/interval_seconds)+1-slot
                break
            sample = self.sample()
            attempts+=1
            finished=clock()
            if finished>end:
                late_issues.append((*self.last_issues,'LATE_SAMPLE_OUTSIDE_WINDOW'))
                late_spans.append(self.last_read_span)
            else:
                samples.append(sample)
                diagnostics.append(self.last_issues)
                spans.append(self.last_read_span)
                for item in sample.metrics:
                    if item.support!='AVAILABLE':
                        counts[item.metric]=counts.get(item.metric,0)+1
            if not self.fixture and sys.platform!='linux':
                truncated=duration_seconds>0
                break
            slot+=1
            now=clock()
            if now>start+slot*interval_seconds:
                skipped=max(0,min(math.floor(duration_seconds/interval_seconds)+1-slot,
                                  math.ceil((now-start)/interval_seconds)-slot))
                missed+=skipped
                slot+=skipped
        return SamplingBatch(tuple(samples),missed,truncated,counts,tuple(diagnostics),
                             float(interval_seconds),float(duration_seconds),max_samples,attempts,
                             len(late_issues),tuple(late_issues),tuple(spans),tuple(late_spans))


__all__=['SamplerIdentity','CgroupV2Binding','SamplingBatch','LinuxResourceSampler',
         'parse_status','parse_meminfo','parse_pressure']
