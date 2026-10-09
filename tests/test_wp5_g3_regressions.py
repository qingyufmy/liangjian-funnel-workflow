"""Review G3 counterexamples, with no source or production access."""
from threading import Event
import time
import pytest
from liangjian_funnel.pipeline import disclosure_pipeline as module
from liangjian_funnel.runtime.bounded_work import BoundedWorkGate


def test_overfetch_is_audited_but_cannot_enter_final_results():
    with module.DisclosurePipeline(lambda s: (s, 1, 2, 3, 4), workers=1,
                                   deadline=time.monotonic()+2) as pipeline:
        pipeline.submit('A', input_hash='a')
        pipeline.submit('B', input_hash='b')
        pipeline.validate_domain(['B'], {'B': 'b'})
        assert pipeline.results(['B']) == [('B', 1, 2, 3, 4)]
        with pytest.raises(module.DisclosurePipelineError, match='OUTSIDE_FINAL_DOMAIN'):
            pipeline.results(['A'])
        assert pipeline.receipt()['domain_anomalies'] == [{
            'reason_code': 'PREFETCH_OUTSIDE_FINAL_DOMAIN', 'symbols': ['A'],
            'input_hashes': {'A': 'a'}}]
        with pytest.raises(module.DisclosurePipelineError, match='INPUT_CHANGED'):
            pipeline.validate_domain(['B'], {'B': 'revised'})


@pytest.mark.parametrize('fail_at', [1, 2])
def test_thread_start_failure_releases_only_unstarted_slot(monkeypatch, fail_at):
    gate = BoundedWorkGate(2)
    monkeypatch.setattr(module, '_WORKERS', gate)
    real_thread, calls = module.Thread, 0
    finished, closed = Event(), Event()
    close_calls = []
    def close():
        close_calls.append(1)
        closed.set()
    pipeline = module.DisclosurePipeline(lambda s: (s, 1, 2, 3, 4),
        workers=2, deadline=time.monotonic()+2, close_resources=close)
    real_worker = pipeline._worker
    def worker():
        try:
            real_worker()
        finally:
            finished.set()
    pipeline._worker = worker
    def thread(**kwargs):
        nonlocal calls
        calls += 1
        if calls == fail_at:
            class Broken:
                def start(self):
                    raise RuntimeError('thread startup failed')
            return Broken()
        return real_thread(**kwargs)
    monkeypatch.setattr(module, 'Thread', thread)
    with pytest.raises(RuntimeError, match='startup failed'):
        pipeline.__enter__()
    if fail_at == 2:
        assert finished.wait(1)
    assert closed.wait(1)
    assert pipeline._active == 0 and close_calls == [1]
    assert gate.try_acquire() and gate.try_acquire()
    assert not gate.try_acquire()
    gate.release()
    gate.release()


def test_collection_deadline_reserves_cleanup_and_preserves_elapsed_budget(monkeypatch):
    monkeypatch.setattr(module.time, 'monotonic', lambda: 1000)
    deadline, policy = module.collection_deadline(5400, elapsed_seconds=120,
                                                 parent_remaining_seconds=4800)
    assert deadline == 5500
    assert policy['cleanup_headroom_seconds'] == 300
    assert policy['effective_remaining_seconds'] == 4800
    assert module.collection_deadline(300, elapsed_seconds=20)[0] == 1250
    with pytest.raises(module.DisclosurePipelineError, match='DEADLINE'):
        module.collection_deadline(5400, elapsed_seconds=5300)
    with pytest.raises(module.DisclosurePipelineError, match='DEADLINE'):
        module.collection_deadline(5400, parent_remaining_seconds=200)
