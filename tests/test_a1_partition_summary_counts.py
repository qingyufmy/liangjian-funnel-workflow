"""Incremental summaries describe the final partitions, never the old seed."""

from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.a1_registry import A1_OUTPUT_PARTITIONS, merge_a1_partitions


def _row(symbol, basis="HALF_YEAR_FUNDAMENTAL"):
    return {"symbol": symbol, "selection_basis": basis}


def _base():
    return {
        "active_research_pool": [_row("A"), _row("B", "LLM_REVIEWED")],
        "monitor_pool": [_row("C")],
        "rejected_candidates": [_row("D")],
        "analysis_summary": {
            "approved_count": 1749, "monitor_count": 2156, "rejected_count": 70,
            "active_research_pool": 1749, "monitor_pool": 2156, "rejected_candidates": 70,
            "pool_counts": {"active_research_pool": 1749, "monitor_pool": 2156, "rejected_candidates": 70},
            "active_research_count": 1749, "clue_pool_count": 3905,
            "selection_basis_counts": {"HALF_YEAR_FUNDAMENTAL": 1749},
            "selection_basis_total": 1749,
            "active_research_target": {"minimum": 200, "maximum": 800},
            "reason_codes": ["KEPT_AUDIT_REASON"], "model_note": "untouched",
        },
        "envelope": {"run_id": "old", "snapshot_hash": "bound"},
    }


def _check(merged, expected):
    summary = merged["analysis_summary"]
    counts = dict(zip(A1_OUTPUT_PARTITIONS, expected))
    assert {name: len(merged[name]) for name in A1_OUTPUT_PARTITIONS} == counts
    assert {name: summary[name] for name in A1_OUTPUT_PARTITIONS} == counts
    assert summary["pool_counts"] == counts
    assert (summary["approved_count"], summary["monitor_count"], summary["rejected_count"]) == expected
    assert summary["active_research_count"] == expected[0]
    assert summary["clue_pool_count"] == expected[0] + expected[1]
    assert summary["selection_basis_total"] == sum(summary["selection_basis_counts"].values())
    assert summary["model_note"] == "untouched"
    assert summary["reason_codes"] == ["KEPT_AUDIT_REASON"]
    assert summary["active_research_target"] == {"minimum": 200, "maximum": 800}
    assert merged["envelope"] == {"run_id": "old", "snapshot_hash": "bound"}


@pytest.mark.parametrize(
    "delta,updated,removed,expected,bases",
    [
        ({"active_research_pool": [_row("E")]}, ["E"], [], (3, 1, 1), {"HALF_YEAR_FUNDAMENTAL": 2, "LLM_REVIEWED": 1}),
        ({}, [], ["A", "C"], (1, 0, 1), {"LLM_REVIEWED": 1}),
        ({"monitor_pool": [_row("A")]}, ["A"], [], (1, 2, 1), {"LLM_REVIEWED": 1}),
        ({"active_research_pool": [_row("C", "BROKER_GOLD_DIRECT")]}, [], [], (3, 0, 1), {"HALF_YEAR_FUNDAMENTAL": 1, "LLM_REVIEWED": 1, "BROKER_GOLD_DIRECT": 1}),
        ({}, [], ["A", "B", "C", "D"], (0, 0, 0), {}),
        ({}, [], [], (2, 1, 1), {"HALF_YEAR_FUNDAMENTAL": 1, "LLM_REVIEWED": 1}),
        ({"active_research_pool": [_row("A"), _row("A")]}, ["A"], [], (2, 1, 1), {"HALF_YEAR_FUNDAMENTAL": 1, "LLM_REVIEWED": 1}),
    ],
)
def test_incremental_summary_uses_final_arrays(delta, updated, removed, expected, bases):
    base = _base()
    before_base, before_delta = deepcopy(base), deepcopy(delta)
    merged = merge_a1_partitions(base, delta, updated_symbols=updated, removed_symbols=removed)
    _check(merged, expected)
    assert {key: value for key, value in merged["analysis_summary"]["selection_basis_counts"].items() if value} == bases
    assert base == before_base
    assert delta == before_delta


def test_provenance_excludes_monitor_rejected_and_unknown_labels():
    base = _base()
    delta = {"active_research_pool": [_row("A", "UNKNOWN"), {"symbol": "E"}]}
    merged = merge_a1_partitions(base, delta, updated_symbols=["A"])
    _check(merged, (3, 1, 1))
    # Same full-path semantics: no guessed label for unlabelled or unknown rows.
    assert merged["analysis_summary"]["selection_basis_total"] == 1
    assert merged["analysis_summary"]["selection_basis_counts"]["HALF_YEAR_FUNDAMENTAL"] == 0


def test_legacy_summary_does_not_gain_optional_research_annotations():
    base = {name: [] for name in A1_OUTPUT_PARTITIONS}
    merged = merge_a1_partitions(base, {}, updated_symbols=[])
    summary = merged["analysis_summary"]
    assert summary["approved_count"] == summary["monitor_count"] == summary["rejected_count"] == 0
    for name in (*A1_OUTPUT_PARTITIONS, "pool_counts", "active_research_count", "clue_pool_count", "selection_basis_counts", "selection_basis_total"):
        assert name not in summary


def test_delta_only_counts_are_not_treated_as_complete_generation():
    base = _base()
    delta = {"active_research_pool": [_row("E")], "analysis_summary": {
        "active_research_count": 1, "clue_pool_count": 1,
        "selection_basis_counts": {"HALF_YEAR_FUNDAMENTAL": 1}, "selection_basis_total": 1,
        "pool_counts": {"active_research_pool": 1},
    }}
    merged = merge_a1_partitions(base, delta, updated_symbols=["E"])
    _check(merged, (3, 1, 1))
    assert merged["analysis_summary"]["selection_basis_total"] == 3
