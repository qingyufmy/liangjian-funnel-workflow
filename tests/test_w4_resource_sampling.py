"""Resource wiring fixtures; no providers/models/production process sampling."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import json
import threading
import time

import pytest

import liangjian_funnel.workflow as workflow
from liangjian_funnel.runtime.resource_sampler import LinuxResourceSampler, SamplerIdentity


def wire(function):
    return workflow._observe_close_research_resources(function)


def fixture_sampler(tmp_path, monkeypatch, *, slow=False, fail=False):
    proc = tmp_path / 'fixture-proc'
    (proc / 'self').mkdir(parents=True)
    (proc / 'pressure').mkdir()
    (proc / 'self' / 'status').write_text('Pid: 321\nVmRSS: 10 kB\nVmHWM: 15 kB\nVmSwap: 2 kB\n')
    (proc / 'meminfo').write_text('SwapTotal: 100 kB\nSwapFree: 30 kB\nCached: 20 kB\nDirty: 3 kB\nWriteback: 1 kB\n')
    (proc / 'pressure' / 'memory').write_text('some total=123\nfull total=7\n')
    entered, release = threading.Event(), threading.Event()
    def factory(invocation):
        now = datetime.now(timezone.utc)
        identity = SamplerIdentity(run_id=invocation, invocation_id=invocation, pid=321,
            process_started_at=now-timedelta(hours=1), host_id='fixture-host')
        sampler = LinuxResourceSampler(identity, fixture_proc_root=proc)
        original = sampler.sample
        def sample():
            entered.set()
            if slow: release.wait(1)
            if fail: raise OSError('fixture source failure')
            return original()
        sampler.sample = sample
        return sampler
    monkeypatch.setattr(workflow, '_new_close_resource_sampler', factory)
    monkeypatch.setattr(workflow, '_CLOSE_RESOURCE_INTERVAL_SECONDS', 0.01)
    return entered, release


def app(tmp_path):
    return SimpleNamespace(settings=SimpleNamespace(workflow_output_dir=tmp_path / 'artifacts'))


def test_disabled_sampler_has_no_constructor_thread_io_or_result_change(tmp_path,monkeypatch):
    calls=[]
    def forbidden(*args,**kwargs):
        calls.append('observer');raise AssertionError('disabled observer constructed')
    monkeypatch.setattr(workflow,'_CloseResourceObservation',forbidden)
    actual=app(tmp_path);actual.settings.close_resource_sampling_enabled=False
    original={'status':'READY','run_id':'r','plan_publication':{'created':['p']}}
    @wire
    def run(self,slot,**kwargs):return original
    assert run(actual,'close') is original
    assert calls==[] and not actual.settings.workflow_output_dir.exists()


def test_sampling_env_switch_is_explicit_and_defaults_on(tmp_path):
    from liangjian_funnel.settings import Settings
    assert Settings.from_env({},root=tmp_path).close_resource_sampling_enabled is True
    assert Settings.from_env({'LIANGJIAN_CLOSE_RESOURCE_SAMPLING_ENABLED':'false'},root=tmp_path).close_resource_sampling_enabled is False


@pytest.mark.parametrize('site',['constructor','start','stop'])
@pytest.mark.parametrize('research_raises',[False,True])
def test_observer_lifecycle_fault_preserves_result_exception_and_deadline(tmp_path,monkeypatch,site,research_raises):
    class BrokenObserver:
        def __init__(self,*args):
            if site=='constructor':raise OSError('fixture constructor')
        def start(self):
            if site=='start':raise OSError('fixture start')
        def stop(self,*args):
            if site=='stop':raise OSError('fixture stop')
            return None
    monkeypatch.setattr(workflow,'_CloseResourceObservation',BrokenObserver)
    failure=RuntimeError('original research failure')
    original={'run_id':'r','status':'READY'};deadline=object();seen=[]
    @wire
    def run(self,slot,**kwargs):
        seen.append(kwargs)
        if research_raises:raise failure
        return original
    began=time.monotonic()
    if research_raises:
        with pytest.raises(RuntimeError) as caught:run(app(tmp_path),'close',deadline=deadline)
        assert caught.value is failure
    else:assert run(app(tmp_path),'close',deadline=deadline) is original
    assert seen==[{'deadline':deadline}]
    assert time.monotonic()-began<0.3
    assert not (tmp_path/'artifacts').exists()


def receipt(path):
    deadline = time.monotonic()+2
    while time.monotonic()<deadline:
        if path.exists():
            value = json.loads(path.read_text())
            if value.get('lifecycle_status') != 'RUNNING': return value
        time.sleep(0.01)
    raise AssertionError('final fixture receipt missing')


def test_close_success_receipt_is_additive_not_prompt_or_original_result(tmp_path, monkeypatch):
    entered, _ = fixture_sampler(tmp_path, monkeypatch)
    original = {'run_id': 'real-logical-run', 'status': 'READY', 'plan_publication': {'created': ['p']}}
    seen = []
    @wire
    def run(self, slot, **kwargs):
        seen.append((slot, kwargs))
        assert entered.wait(1)
        time.sleep(0.035)
        return original
    result = run(app(tmp_path), 'close', primary_only=True)
    assert seen == [('close', {'primary_only': True})]
    assert original == {'run_id': 'real-logical-run', 'status': 'READY', 'plan_publication': {'created': ['p']}}
    assert {k: v for k, v in result.items() if k != 'resource_sampling'} == original
    value = receipt(Path(result['resource_sampling']['path']))
    assert value['canonical_research_run_id'] == original['run_id']
    assert value['evidence']['sample_count'] >= 2
    assert value['evidence']['sampled_peak_is_exact_run_peak'] is False
    assert value['evidence']['lifetime_peak_is_run_peak'] is False
    assert value['evidence']['system_swap']['attributable_to_this_run'] is False
    assert value['evidence']['eligibility_released'] is False
    assert value['lifecycle_status'] == 'SUCCEEDED'


def test_failed_research_finally_stops_and_preserves_original_exception(tmp_path, monkeypatch):
    entered, _ = fixture_sampler(tmp_path, monkeypatch)
    failure = RuntimeError('original research failure')
    @wire
    def run(self, slot, **kwargs):
        assert entered.wait(1)
        raise failure
    with pytest.raises(RuntimeError) as captured: run(app(tmp_path), 'close')
    assert captured.value is failure
    paths = list((tmp_path/'artifacts'/'resource_observations').glob('*.json'))
    assert len(paths) == 1
    value = receipt(paths[0])
    assert value['lifecycle_status'] == 'FAILED'
    assert value['evidence']['declared_status'] == 'FAILED'
    assert value['evidence']['run_completed'] is False
    assert 'original research failure' not in json.dumps(value)


@pytest.mark.parametrize('skip', [{'slot': 'morning'}, {'historical_replay': True},
                                 {'comparison_run': True}, {'auction_refresh': True}])
def test_non_target_paths_do_not_start_observer(tmp_path, monkeypatch, skip):
    monkeypatch.setattr(workflow, '_new_close_resource_sampler', lambda *_: pytest.fail('out of scope'))
    @wire
    def run(self, slot, **kwargs): return {'run_id': 'r', 'status': 'READY'}
    slot = skip.get('slot', 'close')
    result = run(app(tmp_path), slot, **{k:v for k,v in skip.items() if k!='slot'})
    assert 'resource_sampling' not in result
    assert not (tmp_path/'artifacts').exists()


@pytest.mark.parametrize('failure_site', ['identity', 'sample', 'write'])
def test_observation_failure_does_not_change_research_action(tmp_path, monkeypatch, failure_site):
    entered, _ = fixture_sampler(tmp_path, monkeypatch, fail=failure_site=='sample')
    if failure_site=='identity':
        monkeypatch.setattr(workflow, '_new_close_resource_sampler', lambda *_: (_ for _ in ()).throw(ValueError('unknown identity')))
    if failure_site=='write':
        original_write = workflow.atomic_write_json
        def write(path, value, *args, **kwargs):
            if 'resource_observations' in Path(path).parts: raise OSError('fixture write failure')
            return original_write(path,value,*args,**kwargs)
        monkeypatch.setattr(workflow, 'atomic_write_json', write)
    @wire
    def run(self, slot, **kwargs):
        if failure_site=='sample': assert entered.wait(1)
        time.sleep(0.02)
        return {'status':'READY','plan_publication':{'created':['p']},'run_id':'r'}
    result = run(app(tmp_path), 'close')
    assert result['status']=='READY' and result['plan_publication']['created']==['p']


def test_slow_proc_read_cannot_hold_research_or_certify_end(tmp_path, monkeypatch):
    entered, release = fixture_sampler(tmp_path, monkeypatch, slow=True)
    monkeypatch.setattr(workflow, '_CLOSE_RESOURCE_STOP_JOIN_SECONDS', 0.02)
    @wire
    def run(self, slot, **kwargs):
        assert entered.wait(1)
        return {'status':'READY','run_id':'r'}
    begin = time.monotonic()
    result = run(app(tmp_path), 'close')
    assert time.monotonic()-begin < 0.3
    assert result['resource_sampling']['observer_stopped'] is False
    release.set()
    value = receipt(Path(result['resource_sampling']['path']))
    assert value['evidence']['sample_count']==0
    assert value['evidence']['run_completed'] is False
    assert 'LATE_SAMPLE_OUTSIDE_RUN_WINDOW' in value['gap_codes']


def test_reentry_uses_unique_artifacts_no_run_overwrite(tmp_path, monkeypatch):
    fixture_sampler(tmp_path, monkeypatch)
    @wire
    def run(self, slot, **kwargs):
        time.sleep(0.02)
        return {'status':'READY','run_id':'same-run'}
    first, second = run(app(tmp_path),'close'), run(app(tmp_path),'close')
    assert first['resource_sampling']['path'] != second['resource_sampling']['path']
    assert receipt(Path(first['resource_sampling']['path']))['invocation_id'] != receipt(Path(second['resource_sampling']['path']))['invocation_id']


def test_observer_budget_is_separate_and_never_extends_research_deadline(tmp_path, monkeypatch):
    fixture_sampler(tmp_path, monkeypatch)
    monkeypatch.setattr(workflow, '_CLOSE_RESOURCE_MAX_SECONDS', 0.015)
    @wire
    def run(self, slot, **kwargs):
        time.sleep(0.04)
        return {'status':'READY','run_id':'r'}
    result=run(app(tmp_path),'close')
    value=receipt(Path(result['resource_sampling']['path']))
    assert 'OBSERVATION_WINDOW_BUDGET_EXCEEDED' in value['gap_codes']
    assert value['observation_budget']['max_seconds']==0.015
    assert result['status']=='READY'


@pytest.mark.parametrize('which', ['read', 'window'])
def test_completed_read_crossing_observation_budget_is_explicit(tmp_path, monkeypatch, which):
    entered, _ = fixture_sampler(tmp_path, monkeypatch)
    factory=workflow._new_close_resource_sampler
    def delayed(invocation):
        sampler=factory(invocation)
        original=sampler.sample
        def sample():
            value=original()
            time.sleep(0.25 if which=='window' else 0.025)
            # Read completion timestamp must not be backdated to begin.
            from dataclasses import replace
            return replace(value, observed_at=datetime.now(timezone.utc))
        sampler.sample=sample
        return sampler
    monkeypatch.setattr(workflow,'_new_close_resource_sampler',delayed)
    if which=='read': monkeypatch.setattr(workflow,'_CLOSE_RESOURCE_READ_BUDGET_SECONDS',0.001)
    else: monkeypatch.setattr(workflow,'_CLOSE_RESOURCE_MAX_SECONDS',0.2)
    @wire
    def run(self, slot, **kwargs):
        assert entered.wait(1)
        time.sleep(0.3 if which=='window' else 0.04)
        return {'status':'READY','run_id':'r'}
    value=receipt(Path(run(app(tmp_path),'close')['resource_sampling']['path']))
    if which=='read': assert 'OBSERVATION_READ_OR_SAMPLE_BUDGET_EXCEEDED' in value['gap_codes']
    else:
        assert 'LATE_SAMPLE_OUTSIDE_OBSERVATION_BUDGET' in value['gap_codes']
        assert value['evidence']['sample_count']==0


def test_actual_run_research_keeps_pipeline_inputs_publication_and_persisted_summary(tmp_path, monkeypatch):
    from test_workflow_orchestration_coverage import _app, _prepared, _resources, NOW, DEEPSEEK, _PrepCalendar
    from liangjian_funnel.pipeline.research import LaneResult, ResearchRunResult
    entered, _=fixture_sampler(tmp_path,monkeypatch)
    actual=_app(tmp_path)
    actual.trading_calendar=_PrepCalendar()
    prepared=_prepared(tmp_path)
    actual._load_research_resume_snapshot=lambda *args,**kwargs:prepared
    actual._publish_plans=lambda *args,**kwargs:{'atomic':True,'created':['p'],'activated':[],'blocked':[]}
    monkeypatch.setattr(workflow,'evaluate_resources',lambda root:_resources(True))
    monkeypatch.setattr(workflow,'measure_resources',lambda root:_resources(True).snapshot)
    monkeypatch.setattr(workflow,'_write_broker_gold_benchmark',lambda *args,**kwargs:None)
    monkeypatch.setattr(workflow,'write_stage_markdown_reports',lambda *args,**kwargs:())
    captured=[]
    class Pipeline:
        def __init__(self,*args,**kwargs): captured.append(kwargs)
        def run(self,snapshot,**kwargs):
            assert entered.wait(1)
            assert snapshot is prepared.snapshot
            assert 'resource_sampling' not in snapshot.data
            captured.append(kwargs)
            return ResearchRunResult(run_id=kwargs['run_id'],generated_at=NOW,snapshot_id=snapshot.snapshot_id,
                snapshot_hash=snapshot.snapshot_hash,status='READY',lanes=(LaneResult(lane='lane_1',model=DEEPSEEK,
                status='READY',stages=(),final_output={'core_watch_pool':[]}),),audit_paths=(),markdown_path=None,
                primary_lane_ids=('lane_1',))
    monkeypatch.setattr(workflow,'ResearchPipeline',Pipeline)
    result=actual.run_research('close',as_of=NOW,primary_only=True)
    assert result['plan_publication']['created']==['p']
    assert captured[-1]['generated_at']==NOW and captured[-1]['models']==(DEEPSEEK,)
    assert all('resource_sampling' not in kwargs for kwargs in captured)
    original=json.loads((actual.settings.workflow_output_dir/'runs'/f"{result['run_id']}.json").read_text())
    assert original==json.loads(json.dumps({k:v for k,v in result.items() if k!='resource_sampling'}))
    assert receipt(Path(result['resource_sampling']['path']))['canonical_research_run_id']==result['run_id']


def test_actual_run_research_boundary_has_wrapper_without_a4_or_prompt_wiring():
    assert hasattr(workflow.WorkflowApplication.run_research, '__wrapped__')
    import inspect
    original = inspect.getsource(workflow.WorkflowApplication.run_research.__wrapped__)
    assert 'resource_sampling' not in original
    assert 'LinuxResourceSampler' not in original


def test_actual_run_research_resource_gate_failure_still_finalizes_observation(tmp_path, monkeypatch):
    from test_workflow_orchestration_coverage import _app, _resources, NOW
    entered, _=fixture_sampler(tmp_path,monkeypatch)
    actual=_app(tmp_path)
    def reject(root):
        assert entered.wait(1)
        return _resources(False)
    monkeypatch.setattr(workflow,'evaluate_resources',reject)
    with pytest.raises(workflow.WorkflowError,match='RESOURCE_BUDGET_EXCEEDED'):
        actual.run_research('close',as_of=NOW)
    paths=list((actual.settings.workflow_output_dir/'resource_observations').glob('*.json'))
    assert len(paths)==1
    value=receipt(paths[0])
    assert value['lifecycle_status']=='FAILED'
    assert value['evidence']['run_completed'] is False


def test_unfinished_sampling_never_claims_normal_completion(tmp_path, monkeypatch):
    entered, release=fixture_sampler(tmp_path,monkeypatch,slow=True)
    monkeypatch.setattr(workflow,'_CLOSE_RESOURCE_STOP_JOIN_SECONDS',0.01)
    @wire
    def run(self,slot,**kwargs):
        assert entered.wait(1)
        return {'status':'READY','run_id':'r'}
    result=run(app(tmp_path),'close')
    path=Path(result['resource_sampling']['path'])
    running=json.loads(path.read_text())
    assert running['lifecycle_status']=='RUNNING'
    assert running['evidence'] is None and running['eligibility_released'] is False
    release.set()
    value=receipt(path)
    assert value['lifecycle_status']=='UNFINISHED'
    assert value['evidence']['declared_status']=='RUNNING'
    assert value['evidence']['declared_ended_at'] is None
    assert value['evidence']['run_completed'] is False
