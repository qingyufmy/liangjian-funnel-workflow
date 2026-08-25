"""Fact contracts and root-constrained persistence."""

from .contracts import (
    FactEnvelope,
    FactSnapshotManifest,
    RealtimeFactEnvelope,
    SHANGHAI,
    SourceHealth,
    SourceHealthStatus,
    SourceTier,
    canonical_json,
    canonical_json_bytes,
)
from .store import FactStore
from .hithink import collect_market_results, manifest_projection, normalize_hithink_results
from .merge import merge_fact_manifests
from .cninfo import classify_cninfo_title, normalize_cninfo_results, select_cninfo_pdf_candidates
from .gov_policy import normalize_gov_policy_result
from .open_news import (
    collect_open_news_for_workflow,
    normalize_open_news_result,
    normalize_open_news_results,
)

__all__ = [
    "FactEnvelope",
    "FactSnapshotManifest",
    "FactStore",
    "RealtimeFactEnvelope",
    "SHANGHAI",
    "SourceHealth",
    "SourceHealthStatus",
    "SourceTier",
    "canonical_json",
    "canonical_json_bytes",
    "collect_market_results",
    "manifest_projection",
    "normalize_hithink_results",
    "merge_fact_manifests",
    "normalize_cninfo_results",
    "classify_cninfo_title",
    "select_cninfo_pdf_candidates",
    "normalize_gov_policy_result",
    "collect_open_news_for_workflow",
    "normalize_open_news_result",
    "normalize_open_news_results",
]
