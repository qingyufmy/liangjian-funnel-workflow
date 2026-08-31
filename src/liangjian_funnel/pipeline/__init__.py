"""Frozen data, factor and model-research pipeline."""

from .data_source import HithinkClient
from .factors import FactorEngine, TechnicalFactorSnapshot
from .research import ResearchPipeline, ResearchRunResult
from .a1_registry import (
    A1Generation,
    A1IncrementalScope,
    A1Registry,
    a1_global_input_hash,
)
from .snapshot import FrozenInputSnapshot, UniverseSnapshot
from .technical_aggregates import build_kline_patterns, build_price_levels, build_technical_aggregates

__all__ = [
    "FactorEngine",
    "FrozenInputSnapshot",
    "HithinkClient",
    "ResearchPipeline",
    "ResearchRunResult",
    "A1Generation",
    "A1IncrementalScope",
    "A1Registry",
    "a1_global_input_hash",
    "TechnicalFactorSnapshot",
    "UniverseSnapshot",
    "build_kline_patterns",
    "build_price_levels",
    "build_technical_aggregates",
]
