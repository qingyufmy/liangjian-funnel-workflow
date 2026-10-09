"""Counterexamples for scope lineage, not strategy admission changes."""
from liangjian_funnel.pipeline.close_scope import build_scope_ledger


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
