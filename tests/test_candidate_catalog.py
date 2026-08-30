from __future__ import annotations

from liangjian_funnel.pipeline.candidate_catalog import enrich_candidate_metadata


def _snapshot() -> dict:
    return {
        "g0_candidates": [{"symbol": "600001.SH", "name": "甲公司"}],
        "THS_INDUSTRY_MEMBERSHIP": {
            "records": [{
                "thscode": "600001.SH",
                "memberships": [{"industry_thscode": "881001.TI", "industry_name": "行业甲"}],
            }],
        },
        "THS_CONCEPT_MEMBERSHIP": {
            "records": [{
                "thscode": "600001.SH",
                "memberships": [{"concept_thscode": "885001.TI", "concept_name": "概念甲"}],
            }],
        },
    }


def test_enrichment_uses_snapshot_metadata_without_overwriting_model_fields() -> None:
    result = enrich_candidate_metadata(
        {"active_research_pool": [{"symbol": "600001.SH", "core_thesis": "模型逻辑"}]},
        _snapshot(),
    )

    row = result["active_research_pool"][0]
    assert row["name"] == "甲公司"
    assert row["company_name"] == "甲公司"
    assert row["core_thesis"] == "模型逻辑"
    assert row["ths_industries"] == [{"industry_thscode": "881001.TI", "industry_name": "行业甲"}]
    assert row["ths_concepts"] == [{"concept_thscode": "885001.TI", "concept_name": "概念甲"}]
    assert result["candidate_metadata_coverage"]["name_coverage"] == 1
    assert result["candidate_metadata_coverage"]["taxonomy_coverage"] == 1


def test_missing_catalog_and_taxonomy_are_explicit() -> None:
    result = enrich_candidate_metadata(
        {
            "watch_only_pool": [
                {"symbol": "000001.SZ"},
                {"symbol": "600001.SH"},
            ],
        },
        {"g0_candidates": [{"symbol": "600001.SH", "name": "甲公司"}]},
    )

    missing, unmapped = result["watch_only_pool"]
    assert "CANDIDATE_CATALOG_MISSING" in missing["reason_codes"]
    assert "CANDIDATE_TAXONOMY_MAPPING_GAP" in unmapped["reason_codes"]
    assert result["candidate_metadata_coverage"]["catalog_missing_count"] == 1
