"""Offline projection audits must not masquerade as original replay proof."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.research.common import _sha256_json
from test_wp5_disclosure_prefilter import DAY, bars


def module():
    spec = importlib.util.spec_from_file_location('wp5_batch_audit',
        Path(__file__).parents[1]/'scripts/audit_disclosure_batch_offline.py')
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def inputs():
    snapshot = {'snapshot_id': 'original', 'snapshot_hash': 'a'*64,
        'as_of': '2026-10-09T16:00:00+08:00', 'data': {
            'g0_symbols': ['600000.SH'], 'g0_candidates': [{'symbol': '600000.SH'}],
            'RECENT_DAILY_BARS': {'600000.SH': bars(False)},
            'snapshot_manifest': {'as_of': '2026-10-09T16:00:00+08:00'},
            'MARKET_DATA_AS_OF': '2026-10-09T15:00:00+08:00',
            'SELECTED_BOARD_SNAPSHOT': {'available': True, 'trade_date': DAY.isoformat(), 'by_symbol': {}},
            'TIER_STRUCTURE_SNAPSHOT': {'available': True, 'as_of': '2026-10-09T15:00:00+08:00',
                'by_symbol': {'600000.SH': {'available': True, 'availability_state': 'OBSERVED_ABSENT'}}},
        }}
    output = {'active_research_pool': []}
    lane = {'stages': [{'stage': 'A1', 'status': 'VALIDATED', 'snapshot_id': 'original',
                       'output': output, 'output_hash': _sha256_json(output)}]}
    return snapshot, lane


def test_projection_and_empty_gate_never_authorize_query_scope_promotion():
    value = module().audit_projected_batch(*inputs())
    assert value['status'] == 'DATA_LIMITED'
    assert value['coverage']['status'] == 'COVERED'
    assert value['coverage']['required_count'] == 0
    assert 'EMPTY_QUANTITATIVE_DOMAIN' in value['limitations']
    assert value['changes_query_scope'] is False
    assert value['prefilter']['deferred_symbols'] == ['600000.SH']


@pytest.mark.parametrize('fault', ['snapshot_id', 'output_hash', 'duplicate_a1', 'outside_scope'])
def test_mismatched_or_modified_a1_is_not_replay_input(fault):
    snapshot, lane = inputs()
    stage = lane['stages'][0]
    if fault == 'snapshot_id':
        stage['snapshot_id'] = 'later-generation'
    elif fault == 'output_hash':
        stage['output']['active_research_pool'] = [{'symbol': '600001.SH'}]
    elif fault == 'duplicate_a1':
        lane['stages'].append(deepcopy(stage))
    else:
        stage['output']['active_research_pool'] = [{'symbol': '600001.SH'}]
        stage['output_hash'] = _sha256_json(stage['output'])
    with pytest.raises(ValueError):
        module().audit_projected_batch(snapshot, lane)


def test_cli_does_not_overwrite_input_or_existing_evidence(tmp_path):
    tool = module()
    snapshot, lane = inputs()
    first, second = tmp_path/'snapshot.json', tmp_path/'lane.json'
    first.write_text(json.dumps(snapshot)); second.write_text(json.dumps(lane))
    original = first.read_bytes(), second.read_bytes()
    with pytest.raises(ValueError, match='OUTPUT_CONFLICTS_WITH_INPUT'):
        tool.main(['--snapshot', str(first), '--research', str(second), '--output', str(first)])
    output = tmp_path/'audit.json'
    assert tool.main(['--snapshot', str(first), '--research', str(second), '--output', str(output)]) == 2
    with pytest.raises(FileExistsError):
        tool.main(['--snapshot', str(first), '--research', str(second), '--output', str(output)])
    assert (first.read_bytes(), second.read_bytes()) == original


def test_audit_receipt_keeps_decision_hash_not_repeated_company_evidence(monkeypatch):
    from types import SimpleNamespace
    tool = module()
    decision = {'symbol': '600000.SH', 'status': 'LOCAL_MONITOR',
                'local_eligible_for_review': False, 'eligible_routes': [],
                'business_exposure_facts': [{'text': 'report-body'*1000}]}
    monkeypatch.setattr(tool, 'screen_a2', lambda *a, **k: SimpleNamespace(
        decisions=(decision,), review_symbols=()))
    value = tool.audit_projected_batch(*inputs())
    row = value['quantitative_decisions'][0]
    assert 'business_exposure_facts' not in row
    assert row['decision_hash'] == _sha256_json(decision)
    assert value['quantitative_gate_hash'] == _sha256_json([decision])
