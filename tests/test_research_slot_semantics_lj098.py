from enum import Enum
from types import SimpleNamespace
from datetime import datetime
import pytest
from liangjian_funnel.research_slots import research_slot,require_research_slot
from liangjian_funnel.runtime.research_source_date import runtime_trade_date


@pytest.mark.parametrize('value',['CLOSE','close',' Close '])
def test_known_close_labels_have_one_semantic_value(value):
    assert research_slot(value)=='close'


def test_enum_premarket_and_numeric_portfolio_slots_are_distinct():
    class Slot(Enum): CLOSE='CLOSE'
    assert require_research_slot(Slot.CLOSE)=='close'
    assert research_slot('PREMARKET')=='premarket'
    with pytest.raises(ValueError):require_research_slot('PREMARKET')
    assert research_slot(1) is None and research_slot('close_1510') is None


def test_cli_normalizes_supported_case_without_adding_premarket_publication():
    from liangjian_funnel.cli import build_parser
    assert build_parser().parse_args(['run-research','--slot','CLOSE']).slot=='close'
    with pytest.raises(SystemExit):build_parser().parse_args(['run-research','--slot','PREMARKET'])


@pytest.mark.parametrize('value',[None,'','invented-close','CLOSED',1,True])
def test_unknown_source_slot_never_falls_back_to_capture_date(value):
    now=datetime.fromisoformat('2026-10-28T07:27:33+08:00')
    with pytest.raises(ValueError,match='RESEARCH_SLOT_INVALID'):
        runtime_trade_date(SimpleNamespace(data={},as_of=now),generated_at=now,slot=value)
