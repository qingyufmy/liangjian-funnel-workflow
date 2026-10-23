"""Sampling invariance without changing full scope, quota or turnover rules."""
from copy import deepcopy
from itertools import permutations
from types import SimpleNamespace

import pytest

from liangjian_funnel.data.ths_industry import select_industry_diversified_symbols
from test_ths_industry import _result


def select(records, rows, *, limit=2):
    return select_industry_diversified_symbols(
        records, _result('/controlled-membership', rows), limit=limit,
        top_n_per_node=1, node_count_target=(1, 2))


def fixture():
    records = [SimpleNamespace(symbol='603278.SH', amount=100.),
               SimpleNamespace(symbol='603728.SH', amount=90.)]
    rows = [dict(thscode='603278.SH', memberships=[
        dict(industry_thscode='887002.TI', industry_name='Fallback B'),
        dict(industry_thscode='887001.TI', industry_name='Fallback A')]),
        dict(thscode='603728.SH', memberships=[
            dict(industry_thscode='884001.TI', industry_name='Specific'),
            dict(industry_thscode='881001.TI', industry_name='Parent')])]
    return records, rows


@pytest.mark.parametrize('limit', [1, 2])
def test_row_member_and_company_order_do_not_change_sample_or_prompt_metadata(limit):
    records, rows = fixture()
    expected = select(records, rows, limit=limit)
    for candidate_order in permutations(records):
        for membership_order in permutations(rows):
            for reverse_members in (False, True):
                changed = deepcopy(list(membership_order))
                if reverse_members:
                    for row in changed:
                        row['memberships'].reverse()
                assert select(candidate_order, changed, limit=limit) == expected
    assert expected[1]['requested_limit'] == limit
    assert expected[1]['full_coverage'] is (limit == 2)


def test_standard_specific_and_parent_priority_unchanged():
    records, rows = fixture()
    rows[0]['memberships'].extend([
        dict(industry_thscode='884009.TI', industry_name='Specific9'),
        dict(industry_thscode='881009.TI', industry_name='Parent9')])
    _, metadata = select(records, rows)
    node = next(n for n in metadata['nodes'] if n['industry_thscode'] == '884009.TI')
    assert node['parent_industry_thscode'] == '881009.TI'


@pytest.mark.parametrize('reverse', [False, True])
def test_conflicting_duplicate_company_does_not_pick_last_turnover(reverse):
    records, rows = fixture()
    records.append(SimpleNamespace(symbol=records[0].symbol, amount=0.))
    if reverse:
        records.reverse()
    with pytest.raises(ValueError, match='THS_RESEARCH_DUPLICATE_SYMBOL'):
        select(records, rows)


@pytest.mark.parametrize('reverse', [False, True])
def test_conflicting_duplicate_membership_does_not_pick_last_node(reverse):
    records, rows = fixture()
    other = deepcopy(rows[0])
    other['memberships'] = [dict(industry_thscode='884999.TI', industry_name='Other')]
    rows.append(other)
    if reverse:
        rows.reverse()
    with pytest.raises(ValueError, match='THS_RESEARCH_DUPLICATE_MEMBERSHIP_SYMBOL'):
        select(records, rows)


@pytest.mark.parametrize('reverse', [False, True])
def test_conflicting_name_is_source_conflict_not_order_selected_theme_label(reverse):
    records, rows = fixture()
    for row, name in zip(rows, ('Name A', 'Name B')):
        row['memberships'] = [dict(industry_thscode='884001.TI', industry_name=name)]
    if reverse:
        rows.reverse()
    with pytest.raises(ValueError, match='THS_RESEARCH_CONFLICTING_NODE_NAME'):
        select(records, rows)


@pytest.mark.parametrize('reverse', [False, True])
def test_conflicting_name_inside_one_membership_is_not_lexical_tie_resolution(reverse):
    records, rows = fixture()
    rows[0]['memberships'] = [
        dict(industry_thscode='884001.TI', industry_name='Name A'),
        dict(industry_thscode='884001.TI', industry_name='Name B')]
    if reverse:
        rows[0]['memberships'].reverse()
    with pytest.raises(ValueError, match='THS_RESEARCH_CONFLICTING_NODE_NAME'):
        select(records, rows)
