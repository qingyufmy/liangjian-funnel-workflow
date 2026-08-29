from __future__ import annotations

from liangjian_funnel.pipeline.a1_contract import (
    A1_CONTRACT_VERSION,
    A1_MONTHLY_DECISION_COUNT,
    canonicalize_monthly_decisions,
    merge_a1_discovery_output,
    migrate_legacy_discovery_output,
    render_runtime_contract,
    validate_discovery_output,
)


def _decisions(count: int = 20) -> list[dict]:
    return [
        {
            "rank": rank,
            "industry_thscode": f"881{rank:03d}.TI",
            "industry_name": f"行业{rank}",
            "decision": "INCLUDE" if rank <= 2 else "EXCLUDE" if rank == 3 else "DEFER",
            "reason_codes": ["TEST"],
            "supporting_source_refs": [f"derived:rotation:{rank}"],
        }
        for rank in range(1, count + 1)
    ]


def _discovery(*, mappings: list[dict] | None = None, theme_count: int = 8, node_count: int = 40) -> dict:
    themes = [{"theme_id": f"theme-{index}", "source_refs": ["policy-1"]} for index in range(theme_count)]
    nodes = [
        {
            "node_id": f"node-{index}",
            "theme_ids": [f"theme-{index % theme_count}"],
            "source_refs": ["policy-1"],
        }
        for index in range(node_count)
    ]
    return {
        "structural_themes": themes,
        "industry_chain_graph": nodes,
        "taxonomy_links": [],
        "industry_theme_mappings": mappings if mappings is not None else [],
    }


def test_runtime_contract_has_one_complete_decision_rule():
    rendered = render_runtime_contract()
    assert A1_CONTRACT_VERSION in rendered
    assert "monthly_rotation_decision_top_n=20" in rendered
    assert "industry_theme_mappings" in rendered
    assert "Top10" not in rendered
    assert "top10" not in rendered.lower()
    assert "model_must_not_return" in rendered


def test_canonical_decisions_preserve_all_twenty_and_report_gaps():
    rows, coverage = canonicalize_monthly_decisions(_decisions())
    assert len(rows) == A1_MONTHLY_DECISION_COUNT
    assert [row["rank"] for row in rows] == list(range(1, 21))
    assert coverage["status"] == "READY"
    assert coverage["requested_top_n"] == 20

    partial, partial_coverage = canonicalize_monthly_decisions(_decisions(19))
    assert len(partial) == 19
    assert partial_coverage["status"] == "INCOMPLETE"
    assert partial_coverage["missing_ranks"] == [20]


def test_new_contract_requires_mapping_for_each_include_and_reports_exact_code():
    decisions = _decisions()
    output = _discovery(
        mappings=[
            {
                "industry_thscode": decisions[0]["industry_thscode"],
                "mapped_theme_ids": ["theme-0"],
                "mapping_status": "MAPPED",
                "supporting_source_refs": ["policy-1"],
                "confidence": 0.9,
            }
        ]
    )
    validation = validate_discovery_output(output, canonical_decisions=decisions)
    assert not validation.valid
    assert "A1_INDUSTRY_THEME_MAPPING_INCOMPLETE" in validation.reason_codes
    assert validation.missing_industry_codes == (decisions[1]["industry_thscode"],)


def test_unknown_and_duplicate_mapping_cannot_replace_valid_row():
    decisions = _decisions()
    code = decisions[0]["industry_thscode"]
    output = _discovery(
        mappings=[
            {"industry_thscode": code, "mapped_theme_ids": ["theme-0"], "mapping_status": "MAPPED"},
            {"industry_thscode": code, "mapped_theme_ids": ["theme-1"], "mapping_status": "MAPPED"},
            {"industry_thscode": "999999.TI", "mapped_theme_ids": ["theme-0"], "mapping_status": "MAPPED"},
            {"industry_thscode": decisions[1]["industry_thscode"], "mapped_theme_ids": [], "mapping_status": "UNMAPPED"},
        ]
    )
    validation = validate_discovery_output(output, canonical_decisions=decisions)
    assert "A1_INDUSTRY_THEME_MAPPING_DUPLICATE" in validation.reason_codes
    assert "A1_INDUSTRY_THEME_MAPPING_UNKNOWN_INDUSTRY" in validation.reason_codes
    assert "999999.TI" in validation.unknown_industry_codes


def test_invalid_mapping_status_is_not_silently_coerced_to_unmapped():
    decisions = _decisions()
    output = _discovery(
        mappings=[
            {
                "industry_thscode": decisions[0]["industry_thscode"],
                "mapped_theme_ids": ["theme-0"],
                "mapping_status": "MAYBE",
            }
        ]
    )

    validation = validate_discovery_output(output, canonical_decisions=decisions)
    assert "A1_INDUSTRY_THEME_MAPPING_STATUS_INVALID" in validation.reason_codes


def test_merge_keeps_server_decision_and_rank_and_only_joins_mapping():
    decisions = _decisions()
    output = _discovery(
        mappings=[
            {
                "industry_thscode": decisions[0]["industry_thscode"],
                "mapped_theme_ids": ["theme-0", "unknown-theme"],
                "mapping_status": "MAPPED",
                "supporting_source_refs": ["policy-1"],
                "confidence": 0.8,
            },
            {
                "industry_thscode": decisions[1]["industry_thscode"],
                "mapped_theme_ids": [],
                "mapping_status": "UNMAPPED",
                "data_gaps": ["NO_DOMESTIC_EVIDENCE"],
            },
        ]
    )
    # A model-provided legacy array is intentionally ignored by the join.
    output["monthly_industry_decisions"] = [{"rank": 1, "industry_thscode": decisions[0]["industry_thscode"], "decision": "EXCLUDE"}]
    merged = merge_a1_discovery_output(output, decisions)
    rows = merged["monthly_industry_decisions"]
    assert len(rows) == 20
    assert rows[0]["decision"] == "INCLUDE"
    assert rows[0]["final_decision"] == "INCLUDE"
    assert rows[0]["rank"] == 1
    assert rows[0]["mapped_theme_ids"] == ["theme-0"]
    assert rows[1]["decision"] == "INCLUDE"
    assert rows[1]["mapping_status"] == "UNMAPPED"
    assert merged["a1_contract"]["contract_version"] == A1_CONTRACT_VERSION
    assert merged["monthly_rotation_coverage"]["missing_include_mapping_codes"] == [decisions[1]["industry_thscode"]]


def test_legacy_reader_is_explicit_and_does_not_claim_new_model_contract():
    legacy = {"structural_themes": [], "monthly_industry_decisions": [{"rank": 1, "decision": "INCLUDE"}]}
    migrated = migrate_legacy_discovery_output(legacy)
    assert migrated["legacy_monthly_industry_decisions"] == legacy["monthly_industry_decisions"]
    assert migrated["a1_contract"]["migration"].startswith("LEGACY_")
