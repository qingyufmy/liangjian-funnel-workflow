from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('wp0_baseline', ROOT/'scripts/full_test_baseline.py')
baseline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(baseline)


def evidence(tmp_path):
    logs = {}
    for name in ('pytest', 'npm_test', 'typecheck'):
        path = tmp_path / (name+'.log')
        path.write_text('verified test output', encoding='utf-8')
        logs[name] = {'exit_code': 0, 'log': path.name,
                      'log_sha256': baseline.file_hash(path)}
    proof = {'schema_version': 'full-test-baseline/1', 'head': 'a'*40,
        'tree': 'b'*40, 'head_end': 'a'*40, 'tree_end': 'b'*40,
        'clean_start': True, 'clean_end': True,
        'status': 'PASSED', 'steps': logs, 'totals': {'tests': 10, 'failed': 0}}
    path = tmp_path/'evidence.json'
    path.write_text(json.dumps(proof), encoding='utf-8')
    return path


def test_baseline_gate_accepts_matching_head_and_immutable_logs(tmp_path):
    path = evidence(tmp_path)
    baseline.verify_evidence(path, baseline.file_hash(path), 'a'*40)


@pytest.mark.parametrize('defect', ['wrong_head', 'wrong_hash', 'failed_step',
                                   'dirty_start', 'dirty_end', 'changed_log', 'missing_step',
                                   'changed_head_during_tests', 'changed_tree_during_tests'])
def test_baseline_gate_rejects_mismatched_or_failed_proof(tmp_path, defect):
    path = evidence(tmp_path)
    proof = json.loads(path.read_text())
    target, expected = 'a'*40, None
    if defect == 'wrong_head':
        target = 'c'*40
    elif defect == 'wrong_hash':
        expected = '0'*64
    elif defect == 'failed_step':
        proof['steps']['npm_test']['exit_code'] = 1
    elif defect == 'dirty_start':
        proof['clean_start'] = False
    elif defect == 'dirty_end':
        proof['clean_end'] = False
    elif defect == 'changed_log':
        (tmp_path/'pytest.log').write_text('overwritten', encoding='utf-8')
    elif defect == 'missing_step':
        del proof['steps']['typecheck']
    elif defect == 'changed_head_during_tests':
        proof['head_end'] = 'c'*40
    elif defect == 'changed_tree_during_tests':
        proof['tree_end'] = 'c'*40
    path.write_text(json.dumps(proof), encoding='utf-8')
    with pytest.raises(ValueError):
        baseline.verify_evidence(path, expected or baseline.file_hash(path), target)
