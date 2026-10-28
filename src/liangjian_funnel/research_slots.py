"""Research slot semantics only; persisted source labels remain unchanged.

Numeric portfolio slots and scheduler job identifiers are separate contracts.
"""
from enum import Enum


def research_slot(value):
    """Recognize documented research labels, never infer an unknown label."""
    if isinstance(value, Enum):
        value = value.value
    if not isinstance(value, str):
        return None
    return {'close': 'close', 'morning': 'morning',
            'premarket': 'premarket'}.get(value.strip().casefold())


def require_research_slot(value):
    result = research_slot(value)
    # 08:30 premarket is an existing reporting/configuration identity, not
    # permission to run the morning research/publication entry point.
    if result not in {'close','morning'}:
        raise ValueError('RESEARCH_SLOT_INVALID')
    return result
