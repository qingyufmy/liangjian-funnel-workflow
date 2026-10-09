from datetime import datetime, timedelta, timezone
import importlib

import pytest

from liangjian_funnel.runtime.resource_evidence import RunResourceEvidenceBuilder, RunResourceWindow

NOW = datetime(2026,10,10,tzinfo=timezone.utc)


def module():
    return importlib.import_module('liangjian_funnel.runtime.resource_sampler')


def identity(cgroup_id=None):
    return module().SamplerIdentity(run_id='r',invocation_id='i',pid=321,
        process_started_at=NOW-timedelta(hours=1),host_id='host',cgroup_id=cgroup_id)


def fake_proc(tmp_path):
    root = tmp_path/'proc'
    (root/'self').mkdir(parents=True)
    (root/'pressure').mkdir()
    (root/'self'/'status').write_text('Pid:\t321\nVmRSS:\t10 kB\nVmHWM:\t15 kB\nVmSwap:\t2 kB\n')
    (root/'meminfo').write_text('SwapTotal: 100 kB\nSwapFree: 30 kB\nCached: 20 kB\nDirty: 3 kB\nWriteback: 1 kB\n')
    (root/'pressure'/'memory').write_text('some avg10=0.00 avg60=0.00 avg300=0.00 total=123\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=7\n')
    return root


def metrics(observation):
    return {item.metric:item for item in observation.metrics}


def test_status_meminfo_pressure_units_and_scopes_without_source_authentication(tmp_path):
    sampler = module().LinuxResourceSampler(identity(), fixture_proc_root=fake_proc(tmp_path), utc_now=lambda:NOW)
    sample = sampler.sample()
    values = metrics(sample)
    assert values['RSS_CURRENT'].normalized_value() == 10240
    assert values['PROCESS_RSS_LIFETIME_PEAK'].value == 15
    assert values['PROCESS_VMSWAP'].value == 2
    assert values['SYSTEM_SWAP_USED'].value == 70
    assert values['PSI_MEMORY_SOME_TOTAL_US'].value == 123
    assert values['CGROUP_MEMORY_CURRENT'].value is None
    assert all(item.source == 'FIXTURE' for item in sample.metrics if item.support == 'AVAILABLE')
    receipt = RunResourceEvidenceBuilder().build(RunResourceWindow(run_id='r',invocation_id='i',pid=321,
        process_started_at=identity().process_started_at,host_id='host',started_at=NOW,
        observed_until=NOW,status='SUCCEEDED',ended_at=NOW),[sample])
    assert receipt['sampled_peak_is_exact_run_peak'] is False
    assert receipt['system_swap']['attributable_to_this_run'] is False
    assert receipt['acquisition_authenticated'] is False


@pytest.mark.parametrize('text', ['VmRSS: 4 MB\n','VmRSS: -1 kB\n','VmRSS: 2 kB\nVmRSS: 3 kB\n',
    'VmRSS: 99999999999999999999999 kB\n'])
def test_bad_status_is_error_not_zero(text):
    value = metrics(type('Observation',(),{'metrics':module().parse_status(text)})())['RSS_CURRENT']
    assert value.support == 'ERROR' and value.value is None


def test_missing_and_invalid_host_fields_remain_unknown():
    mod = module()
    values = {item.metric:item for item in mod.parse_meminfo('SwapTotal: 10 kB\nSwapFree: 11 kB\n')}
    assert values['SYSTEM_SWAP_USED'].support == 'ERROR'
    assert values['SYSTEM_SWAP_USED'].value is None
    assert values['PAGECACHE_CACHED'].support == 'UNSUPPORTED'
    pressure = {item.metric:item for item in mod.parse_pressure('some total=oops\n')}
    assert pressure['PSI_MEMORY_SOME_TOTAL_US'].support == 'ERROR'
    assert pressure['PSI_MEMORY_FULL_TOTAL_US'].value is None


def test_real_non_linux_does_not_read_proc(tmp_path, monkeypatch):
    mod = module()
    monkeypatch.setattr(mod.sys,'platform','win32')
    sampler = mod.LinuxResourceSampler(identity(), utc_now=lambda:NOW)
    monkeypatch.setattr(mod.Path,'open',lambda *a, **kw: (_ for _ in ()).throw(AssertionError('no OS reads')))
    assert all(item.support == 'UNSUPPORTED' and item.value is None for item in sampler.sample().metrics)


def test_missing_files_and_pid_mismatch_do_not_become_process_evidence(tmp_path):
    root = fake_proc(tmp_path)
    (root/'self'/'status').write_text('Pid: 999\nVmRSS: 10 kB\nVmHWM: 15 kB\nVmSwap: 2 kB\n')
    (root/'pressure'/'memory').unlink()
    sampler = module().LinuxResourceSampler(identity(), fixture_proc_root=root, utc_now=lambda:NOW)
    values = metrics(sampler.sample())
    assert values['RSS_CURRENT'].support == 'ERROR' and values['RSS_CURRENT'].value is None
    assert values['SYSTEM_SWAP_USED'].support == 'AVAILABLE'
    assert values['PSI_MEMORY_FULL_TOTAL_US'].support == 'UNAVAILABLE'
    assert 'PROCESS_PID_MISMATCH' in sampler.last_issues


def cgroup(tmp_path, proc, *, membership='/job', fs='cgroup2'):
    root = tmp_path/'cgroup'
    child = root/'job'
    child.mkdir(parents=True)
    (root/'cgroup.controllers').write_text('memory cpu\n')
    (child/'memory.current').write_text('4096\n')
    (child/'memory.events').write_text('low 0\nhigh 1\nmax 2\noom 3\noom_kill 1\n')
    (proc/'self'/'cgroup').write_text(f'0::{membership}\n')
    mount = root.as_posix().replace(' ', '\\040')
    (proc/'self'/'mountinfo').write_text(f'1 0 0:1 / {mount} rw - {fs} cgroup rw\n')
    return module().CgroupV2Binding(root=root,directory=child,relative_path='/job',cgroup_id='cg')


def test_explicit_cgroup_v2_validated_before_any_member_metrics(tmp_path):
    proc = fake_proc(tmp_path)
    binding = cgroup(tmp_path,proc)
    sampler = module().LinuxResourceSampler(identity('cg'),fixture_proc_root=proc,cgroup=binding,utc_now=lambda:NOW)
    values = metrics(sampler.sample())
    assert values['CGROUP_MEMORY_CURRENT'].value == 4096
    assert values['CGROUP_MEMORY_CURRENT'].unit == 'bytes'
    assert values['CGROUP_MEMORY_EVENTS_OOM'].value == 3


@pytest.mark.parametrize('violation', ['MEMBERSHIP','V1','TRAVERSAL','DIRECTORY','IDENTITY','CONTROLLER'])
def test_cgroup_bad_binding_is_unavailable_not_guessed(tmp_path,violation):
    from dataclasses import replace
    proc = fake_proc(tmp_path)
    binding = cgroup(tmp_path,proc,membership='/other' if violation=='MEMBERSHIP' else '/job',
        fs='cgroup' if violation=='V1' else 'cgroup2')
    if violation=='TRAVERSAL': binding=replace(binding,relative_path='/../job')
    if violation=='DIRECTORY': binding=replace(binding,directory=tmp_path)
    if violation=='IDENTITY': binding=replace(binding,cgroup_id='wrong')
    if violation=='CONTROLLER': (binding.root/'cgroup.controllers').write_text('cpu\n')
    sampler=module().LinuxResourceSampler(identity('cg'),fixture_proc_root=proc,cgroup=binding,utc_now=lambda:NOW)
    values=metrics(sampler.sample())
    assert values['CGROUP_MEMORY_CURRENT'].value is None
    assert values['CGROUP_MEMORY_CURRENT'].support in {'ERROR','UNAVAILABLE','UNSUPPORTED'}
    assert sampler.last_issues


class Clock:
    def __init__(self): self.t=0
    def monotonic(self): return self.t
    def utc(self): return NOW+timedelta(seconds=self.t)
    def wait(self,seconds): self.t+=seconds


def test_fixed_interval_bounded_samples_and_no_real_sleep(tmp_path):
    clock=Clock()
    sampler=module().LinuxResourceSampler(identity(),fixture_proc_root=fake_proc(tmp_path),
        utc_now=clock.utc,monotonic=clock.monotonic,wait=clock.wait)
    batch=sampler.collect(duration_seconds=5,interval_seconds=1,max_samples=3)
    assert len(batch.observations)==3
    assert [sample.observed_at for sample in batch.observations]==[NOW,NOW+timedelta(seconds=1),NOW+timedelta(seconds=2)]
    assert batch.truncated is True and batch.missed_interval_count==0
    assert batch.unsupported_counts['CGROUP_MEMORY_CURRENT']==3
    assert batch.sampled_peak_is_exact is False


def test_slow_read_records_missed_ticks_without_backfill(tmp_path,monkeypatch):
    clock=Clock()
    sampler=module().LinuxResourceSampler(identity(),fixture_proc_root=fake_proc(tmp_path),
        utc_now=clock.utc,monotonic=clock.monotonic,wait=clock.wait)
    original=sampler.sample
    def slow():
        sample=original()
        clock.t+=2.4
        return sample
    monkeypatch.setattr(sampler,'sample',slow)
    batch=sampler.collect(duration_seconds=3,interval_seconds=1,max_samples=10)
    assert len(batch.observations)==1
    assert batch.missed_interval_count==2
    assert batch.late_sample_count==1


@pytest.mark.parametrize('kwargs',[{'duration_seconds':-1,'interval_seconds':1,'max_samples':1},
    {'duration_seconds':1,'interval_seconds':0,'max_samples':1},
    {'duration_seconds':1,'interval_seconds':float('nan'),'max_samples':1},
    {'duration_seconds':1,'interval_seconds':1,'max_samples':100001}])
def test_invalid_schedule_is_rejected_before_sampling(kwargs,tmp_path):
    sampler=module().LinuxResourceSampler(identity(),fixture_proc_root=fake_proc(tmp_path),utc_now=lambda:NOW)
    with pytest.raises(ValueError): sampler.collect(**kwargs)


def test_wrong_pid_cannot_certify_cgroup_for_the_declared_process(tmp_path):
    proc=fake_proc(tmp_path)
    binding=cgroup(tmp_path,proc)
    (proc/'self'/'status').write_text('Pid: 999\nVmRSS: 10 kB\n')
    sampler=module().LinuxResourceSampler(identity('cg'),fixture_proc_root=proc,cgroup=binding,utc_now=lambda:NOW)
    assert metrics(sampler.sample())['CGROUP_MEMORY_CURRENT'].value is None


def test_no_late_sample_after_read_or_wait_passes_deadline(tmp_path,monkeypatch):
    clock=Clock()
    sampler=module().LinuxResourceSampler(identity(),fixture_proc_root=fake_proc(tmp_path),
        utc_now=clock.utc,monotonic=clock.monotonic,wait=clock.wait)
    original=sampler.sample
    def slow():
        sample=original()
        clock.t+=2.4
        return sample
    monkeypatch.setattr(sampler,'sample',slow)
    batch=sampler.collect(duration_seconds=2,interval_seconds=1,max_samples=10)
    assert len(batch.observations)==0 and batch.missed_interval_count==2
    assert batch.late_sample_count==1


@pytest.mark.parametrize('pressure',['some total=1 total=2\n','some total=1\nsome total=2\n'])
def test_duplicate_psi_totals_are_not_counter_evidence(pressure):
    values={item.metric:item for item in module().parse_pressure(pressure)}
    assert values['PSI_MEMORY_SOME_TOTAL_US'].support=='ERROR'


def test_identity_requires_aware_start_and_positive_explicit_pid():
    with pytest.raises(ValueError):
        module().SamplerIdentity(run_id='r',invocation_id='i',pid=321,
            process_started_at=datetime(2026,10,10),host_id='h')
    with pytest.raises(ValueError):
        module().SamplerIdentity(run_id='r',invocation_id='i',pid=0,
            process_started_at=NOW,host_id='h')


def test_observation_time_is_read_completion_not_read_start(tmp_path,monkeypatch):
    clock=Clock()
    sampler=module().LinuxResourceSampler(identity(),fixture_proc_root=fake_proc(tmp_path),utc_now=clock.utc)
    read=sampler._read
    def slow(path):
        clock.t+=1
        return read(path)
    monkeypatch.setattr(sampler,'_read',slow)
    sample=sampler.sample()
    assert sample.observed_at==NOW+timedelta(seconds=3)
    assert sampler.last_read_span==(NOW,NOW+timedelta(seconds=3))


def test_late_completed_sample_is_not_counted_as_peak_observation(tmp_path,monkeypatch):
    clock=Clock()
    sampler=module().LinuxResourceSampler(identity(),fixture_proc_root=fake_proc(tmp_path),
        utc_now=clock.utc,monotonic=clock.monotonic,wait=clock.wait)
    read=sampler._read
    def slow(path):
        clock.t+=1
        return read(path)
    monkeypatch.setattr(sampler,'_read',slow)
    batch=sampler.collect(duration_seconds=2,interval_seconds=1,max_samples=3)
    assert batch.observations==()
    assert batch.late_sample_count==1
    assert 'LATE_SAMPLE_OUTSIDE_WINDOW' in batch.late_sample_issues[0]


def test_in_slot_monotonic_regression_is_rejected_even_above_start(tmp_path):
    ticks=iter([0,2,1,3,4,5,6,7,8,9])
    sampler=module().LinuxResourceSampler(identity(),fixture_proc_root=fake_proc(tmp_path),
        utc_now=lambda:NOW,monotonic=lambda:next(ticks),wait=lambda seconds:None)
    with pytest.raises(ValueError,match='MONOTONIC_CLOCK_REGRESSED'):
        sampler.collect(duration_seconds=2,interval_seconds=1,max_samples=2)
