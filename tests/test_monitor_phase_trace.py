"""Trace observers never own execution permission or a longer strategy budget."""
import json
from pathlib import Path
import pytest
from liangjian_funnel.runtime.monitor_phase_trace import MonitorPhaseTrace


def test_five_phases_and_final_receipt_are_measured(tmp_path):
    t=MonitorPhaseTrace(tmp_path,'monitor-test')
    try:
        t.mark('fetch')
        t.mark('judge')
        with t.measure('persist'):pass
        t.mark('notify')
        result=t.finish()
        assert set(result['phase_timings'])=={'startup','fetch','judge','persist','notify'}
        assert all(v['duration_ms']>=0 for v in result['phase_timings'].values())
        assert result['phase_timings']['persist']['calls']==1
        assert json.loads((tmp_path/'monitor-test.receipt.json').read_text())['run_id']=='monitor-test'
    finally:t.close()


def test_timeout_snapshot_is_immutable_and_does_not_expose_locals(tmp_path):
    t=MonitorPhaseTrace(tmp_path,'monitor-test')
    try:
        t.mark('fetch');t.snapshot('SIGUSR1');first=(tmp_path/'monitor-test.json').read_bytes()
        t.mark('notify');t.snapshot('SIGUSR1');t.finish()
        assert (tmp_path/'monitor-test.json').read_bytes()==first
        data=json.loads(first);assert data['current_phase']=='fetch'
        assert 'stack' in data and 'environment' not in data
    finally:t.close()


@pytest.mark.parametrize('identifier',['../escape','a/b','', 'a\\b'])
def test_run_id_cannot_escape_trace_directory(tmp_path,identifier):
    with pytest.raises(ValueError):MonitorPhaseTrace(tmp_path,identifier)


def test_unknown_phase_rejected_without_affecting_existing_receipt(tmp_path):
    t=MonitorPhaseTrace(tmp_path,'monitor-test')
    try:
        with pytest.raises(ValueError):t.mark('made-up')
        assert t.finish()['run_id']=='monitor-test'
    finally:t.close()
