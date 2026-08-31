from __future__ import annotations

from pathlib import Path

import pytest

from liangjian_funnel.pipeline.a1_sources import (
    A1SourceRegistryError,
    build_a1_source_context,
    load_a1_source_registry,
)


ROOT = Path(__file__).resolve().parents[1]


def test_project_registry_covers_governed_sources_and_forbids_direct_selection() -> None:
    registry = load_a1_source_registry(ROOT / "config" / "a1_research_sources.yaml")
    assert len(registry["sources"]) == 11
    assert {row["source_id"] for row in registry["sources"]} == {
        "iwencai", "cninfo", "mybbond", "jisilu", "stockstar",
        "xingqiao", "nbs", "ifind", "eastmoney", "datayes_robo",
        "cnfinancewatch_x",
    }
    assert all(row["direct_stock_selection_allowed"] is False for row in registry["sources"])


def test_context_distinguishes_active_reviewed_authorized_and_forbidden_sources() -> None:
    registry = load_a1_source_registry(ROOT / "config" / "a1_research_sources.yaml")
    context = build_a1_source_context(
        registry,
        snapshot={
            "DISCLOSURE_EVENTS": {"available": True, "events": [{"id": "a"}]},
            "RISK_EVENTS": {"available": True, "events": [{"id": "b"}]},
            "MACRO_ECONOMIC_DATA": {"available": True, "series": [{"id": "PMI"}]},
            "INDUSTRY_ACTIVITY_DATA": {"available": True, "items": [{"id": "industry"}]},
            "BROKER_RESEARCH_CONSENSUS": {"available": True, "documents": [{"id": "em"}]},
            "INDUSTRY_NEWS_FEED": {"available": False},
            "NEWS_HEAT_SNAPSHOT": {"available": False},
        },
        research_consensus={
            "documents": [
                {
                    "document_id": "eastmoney-september",
                    "source_url": "https://finance.eastmoney.com/a/example.html",
                },
                {
                    "document_id": "xingqiao-weekly",
                    "source_url": "https://xingqiao-advisor.vercel.app/member/report",
                },
            ]
        },
    )
    by_id = {row["source_id"]: row for row in context["sources"]}
    assert by_id["cninfo"]["status"] == "ACTIVE_AUTOMATED"
    assert by_id["nbs"]["status"] == "ACTIVE_INDIRECT_TRANSPORT"
    assert by_id["eastmoney"]["status"] == "ACTIVE_AUTOMATED"
    assert by_id["xingqiao"]["status"] == "REVIEWED_EVIDENCE_AVAILABLE"
    assert by_id["xingqiao"]["usable_for_a1"] is True
    assert by_id["ifind"]["status"] == "AUTHORIZATION_REQUIRED"
    assert by_id["jisilu"]["status"] == "AUTOMATION_FORBIDDEN"
    assert by_id["datayes_robo"]["status"] == "AUTOMATION_FORBIDDEN"
    assert by_id["iwencai"]["status"] == "REFERENCE_ONLY"
    assert by_id["cnfinancewatch_x"]["status"] == "REFERENCE_ONLY"
    assert by_id["cnfinancewatch_x"]["usable_for_a1"] is False
    assert context["usable_source_count"] == 4
    assert context["governance"]["homepage_is_not_evidence"] is True


def test_paid_source_without_reviewed_export_never_becomes_available() -> None:
    registry = load_a1_source_registry(ROOT / "config" / "a1_research_sources.yaml")
    context = build_a1_source_context(registry, snapshot={}, research_consensus={})
    by_id = {row["source_id"]: row for row in context["sources"]}
    assert by_id["mybbond"]["status"] == "MANUAL_EXPORT_REQUIRED"
    assert by_id["xingqiao"]["status"] == "MANUAL_EXPORT_REQUIRED"
    assert by_id["mybbond"]["usable_for_a1"] is False


def test_registry_rejects_any_source_that_claims_direct_stock_authority(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text(
        """schema_version: a1-research-source-registry/1.0.0
sources:
  - source_id: bad
    label: Bad
    host: bad.example.com
    access_mode: PUBLIC_WEB
    ingestion_mode: REFERENCE_ONLY
    evidence_tier: T3
    roles: [MARKET_SENTIMENT]
    snapshot_contracts: []
    fact_authority: false
    viewpoint_only: true
    direct_stock_selection_allowed: true
""",
        encoding="utf-8",
    )
    with pytest.raises(A1SourceRegistryError, match="A1_SOURCE_SELECTION_AUTHORITY_INVALID"):
        load_a1_source_registry(path)
