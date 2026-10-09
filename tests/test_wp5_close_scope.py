"""Counterexamples for scope lineage, not strategy admission changes."""
from liangjian_funnel.pipeline.close_scope import build_scope_ledger
from liangjian_funnel.pipeline.close_scope import seal_scope_receipt, validate_scope_receipt
from liangjian_funnel.pipeline.feature_store import content_hash
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import pytest


def ledger(a1, hot, discovery, g0):
    return build_scope_ledger(a1_symbols=a1, hot_symbols=hot,
        discovery_symbols=discovery, g0_symbols=g0)


def test_net_growth_is_not_added_symbol_count():
    result = ledger(['A','B','C'], ['D','E'], ['F'], ['A','B','D','E','F'])
    assert result['counts'] == {'a1':3,'selected':5,'added':3,'excluded_a1':1,'net_growth':2}
    assert result['excluded_a1'] == ['C']
    assert [row['symbol'] for row in result['added']] == ['D','E','F']


def test_overlap_keeps_both_sources_and_deduplicates():
    result = ledger(['A'], ['B','B'], ['B','C'], ['A','B','C'])
    assert result['added'][0]['sources'] == ['HOT100','EARLY_DISCOVERY']
    assert result['selected_symbols'] == ['A','B','C']


def test_outside_g0_is_not_added_or_silently_admitted():
    result = ledger(['A'], ['B'], ['C'], ['A'])
    assert result['selected_symbols'] == ['A']
    assert result['excluded_requested'] == ['B','C']


def test_has_no_300_cap_and_order_is_stable():
    symbols = [f'{i:06d}.SZ' for i in range(400)]
    first = ledger(symbols, [], [], symbols)
    second = ledger(reversed(symbols), [], [], reversed(symbols))
    assert len(first['selected_symbols']) == 400
    assert first == second


def test_provenance_hash_changes_with_source_not_only_union():
    hot = ledger(['A'], ['B'], [], ['A','B'])
    discovery = ledger(['A'], [], ['B'], ['A','B'])
    assert hot['selected_symbols'] == discovery['selected_symbols']
    assert hot['scope_hash'] != discovery['scope_hash']


def test_empty_inputs_cannot_be_labeled_ready():
    result = ledger([], [], [], [])
    assert result['status'] == 'EMPTY'
    assert result['execution_authority'] is False


def seal(tmp_path, **overrides):
    args = dict(root=tmp_path, run_id='2026-10-09-close',
        research_as_of=datetime(2026,10,9,15,10,tzinfo=ZoneInfo('Asia/Shanghai')),
        market_data_as_of=datetime(2026,10,9,15,10,tzinfo=ZoneInfo('Asia/Shanghai')),
        a1_reference={'generation_id':'original-a1','payload_hash':'f'*64},
        a1_symbols=['A'], hot_payload={'records':[{'symbol':'B'}]},
        discovery={'records':[{'symbol':'C','review_budget_selected':True},
                              {'symbol':'OVERFLOW','review_budget_selected':False}]},
        g0_symbols=['A','B','C','OVERFLOW'], selected_symbols=['A','B','C'])
    args.update(overrides)
    return seal_scope_receipt(**args)


def test_receipt_preserves_binding_and_only_selected_discovery_before_later_failure(tmp_path):
    receipt = seal(tmp_path)
    original = json.loads(receipt.read_text(encoding='utf-8'))
    assert original['a1_reference']['generation_id'] == 'original-a1'
    assert original['scope']['source_sets']['EARLY_DISCOVERY'] == ['C']
    assert original['discovery']['records'][1]['symbol'] == 'OVERFLOW'
    assert original['run_id'] == '2026-10-09-close'
    assert original['execution_authority'] is False
    # A later generation is a different receipt, never a relabel of the old run.
    later = seal(tmp_path,a1_reference={'generation_id':'later-a1','payload_hash':'e'*64})
    assert later != receipt
    assert json.loads(receipt.read_text()) == original


def test_receipt_refuses_mismatched_actual_selected_scope(tmp_path):
    with pytest.raises(ValueError,match='SELECTED_SCOPE_MISMATCH'):
        seal(tmp_path,selected_symbols=['A'])
    assert not list(tmp_path.glob('*.json'))


def test_unbound_a1_is_not_falsely_marked_original_run_proof(tmp_path):
    receipt = seal(tmp_path,a1_reference=None)
    assert json.loads(receipt.read_text())['binding_status'] == 'UNBOUND_A1_REFERENCE'


def test_weekend_receipt_keeps_real_source_time_separate_from_friday_market(tmp_path):
    receipt = seal(tmp_path,research_as_of=datetime(2026,10,10,12,tzinfo=ZoneInfo('Asia/Shanghai')))
    data = json.loads(receipt.read_text())
    assert data['research_as_of'].startswith('2026-10-10')
    assert data['market_data_as_of'].startswith('2026-10-09')


def test_future_market_time_is_not_accepted(tmp_path):
    with pytest.raises(ValueError,match='INVALID_SCOPE_TIME'):
        seal(tmp_path,market_data_as_of=datetime(2026,10,10,15,tzinfo=ZoneInfo('Asia/Shanghai')))


def test_identical_retry_reuses_original_receipt_and_observation_time(tmp_path):
    first = seal(tmp_path)
    original = first.read_bytes()
    assert seal(tmp_path) == first
    assert first.read_bytes() == original
    assert len(list(tmp_path.glob('close-scope-*.json'))) == 1


def test_idempotent_receipt_does_not_accept_tampered_observation(tmp_path):
    first = seal(tmp_path)
    value = json.loads(first.read_text())
    value['recorded_at'] = '2000-01-01T00:00:00+00:00'
    first.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='SCOPE_RECEIPT_OBSERVATION_HASH_MISMATCH'):
        seal(tmp_path)


def test_legacy_receipt_retains_full_document_hash_contract():
    value = {'schema_version':'close-scope-receipt/1','recorded_at':'original'}
    value['receipt_hash'] = content_hash(value)
    validate_scope_receipt(value)
    value['recorded_at'] = 'tampered'
    with pytest.raises(ValueError, match='CLOSE_SCOPE_RECEIPT_HASH_MISMATCH'):
        validate_scope_receipt(value)
