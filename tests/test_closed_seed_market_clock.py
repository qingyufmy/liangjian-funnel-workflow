"""Regression for the actual Oct23 closed producer's 20:26/15:00 conflict."""
from datetime import datetime
import ast
import inspect
import textwrap

import pytest
from liangjian_funnel.workflow import WorkflowApplication, _advance_live_market_cutoff


def at(value):
    return datetime.fromisoformat(value)


@pytest.mark.parametrize('research,included', [
    ('2026-10-23T20:25:41.409020+08:00', '2026-10-23T20:26:25.361000+08:00'),
    ('2026-10-24T00:10:00+08:00', '2026-10-24T00:10:01+08:00'),
    ('2026-10-24T12:00:00+08:00', '2026-10-24T12:00:01+08:00'),
])
def test_explicit_closed_source_keeps_close_not_collection_time(research, included):
    market = at('2026-10-23T15:00:00+08:00')
    result = _advance_live_market_cutoff(market_as_of=market, research_as_of=at(research),
                                       included_fact_as_of=at(included), closed_session=True)
    assert result == market
    assert at(included) != result  # Collection time remains separate, never backdated.


def test_ordinary_same_session_live_request_still_advances():
    market = at('2026-10-23T10:00:00+08:00')
    included = at('2026-10-23T10:00:05+08:00')
    assert _advance_live_market_cutoff(market_as_of=market, research_as_of=market,
                                      included_fact_as_of=included) == included


@pytest.mark.parametrize('intent', ['True', 1, None])
def test_closed_intent_is_explicit_boolean(intent):
    stamp = at('2026-10-23T15:00:00+08:00')
    with pytest.raises(ValueError, match='CLOSED_SESSION_INTENT_INVALID'):
        _advance_live_market_cutoff(market_as_of=stamp, research_as_of=stamp,
                                   included_fact_as_of=stamp, closed_session=intent)


def test_real_producer_passes_closed_intent_from_validated_source_argument():
    tree = ast.parse(textwrap.dedent(inspect.getsource(WorkflowApplication._prepare_snapshot)))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == '_advance_live_market_cutoff']
    assert len(calls) == 1
    intent = next(k.value for k in calls[0].keywords if k.arg == 'closed_session')
    assert ast.unparse(intent) == 'closed_session_source_date is not None'
