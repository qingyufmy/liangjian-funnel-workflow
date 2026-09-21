"""Production regression for A5 projection over immutable frozen facts.

This script performs no market, model, database, or notification calls.  It
reads each fact archive once, verifies projection reconciliation, renders the
production prompt, writes a separate acceptance receipt, and proves the input
bytes did not change.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.review.context import PROMPT_TARGET, render_a5_prompt
from liangjian_funnel.review.daily import _canonical_hash, _model_fact_projection


def _table(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    columns = list(value.get("columns") or [])
    return [dict(zip(columns, row, strict=True)) for row in value.get("rows") or []]


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _count(rows: list[Mapping[str, Any]], field: str) -> dict[str, int]:
    return dict(Counter(str(row.get(field) or "UNKNOWN") for row in rows))


def verify(path: Path, prompts: PromptRepository) -> dict[str, Any]:
    before = path.read_bytes()
    source_sha256 = _sha256(before)
    facts = json.loads(before)
    expected_input_hash = _canonical_hash(
        {key: value for key, value in facts.items() if key != "input_hash"}
    )
    assert facts.get("input_hash") == expected_input_hash, "FROZEN_INPUT_HASH_MISMATCH"

    projection = _model_fact_projection(facts)
    prompt, diagnostics = render_a5_prompt(
        prompts,
        "agent_5_daily_reviewer_v1.txt",
        projection,
    )
    assert len(prompt) <= PROMPT_TARGET, "A5_PROMPT_ABOVE_TARGET"

    original_events = list(facts.get("a4", {}).get("events") or [])
    projected_effective = list(projection.get("a4", {}).get("events") or [])
    original_effective = [row for row in original_events if row.get("effective")]
    assert [row.get("evidence_id") for row in projected_effective] == [
        row.get("evidence_id") for row in original_effective
    ], "A4_EFFECTIVE_EVENT_LOSS"
    assert projection.get("a4", {}).get("lifecycles") == facts.get("a4", {}).get("lifecycles"), (
        "A4_LIFECYCLE_CHANGED"
    )

    original_counterexamples = list(
        facts.get("independent_verification", {}).get("counterexamples") or []
    )
    projected_counterexamples = list(
        projection.get("independent_verification", {}).get("counterexamples") or []
    )
    assert [row.get("evidence_id") for row in projected_counterexamples] == [
        row.get("evidence_id") for row in original_counterexamples
    ], "COUNTEREXAMPLE_LOSS"
    for original, projected in zip(
        original_counterexamples, projected_counterexamples, strict=True
    ):
        assert projected.get("raw_evidence_sha256") == _canonical_hash(original), (
            "COUNTEREXAMPLE_HASH_MISMATCH"
        )

    original_a2 = list(facts.get("a2", {}).get("candidates") or [])
    projected_a2 = projection.get("a2", {}).get("candidates") or {}
    assert int(projected_a2.get("total_count") or 0) == len(original_a2), "A2_COUNT_MISMATCH"
    assert int(projected_a2.get("detailed_count") or 0) == sum(
        str(row.get("pool") or "").upper() in {"FOCUS", "WATCH"}
        for row in original_a2
    ), "A2_FOCUS_WATCH_COUNT_MISMATCH"
    assert projection.get("metrics", {}).get("a2_focus_count") == _count(original_a2, "pool").get("FOCUS", 0)
    assert projection.get("metrics", {}).get("a2_watch_count") == _count(original_a2, "pool").get("WATCH", 0)

    original_candidates = list(facts.get("a3", {}).get("candidates") or [])
    projected_candidates = projection.get("a3", {}).get("candidates") or {}
    original_plans = list(facts.get("a3", {}).get("plans") or [])
    projected_plans = projection.get("a3", {}).get("plans") or {}
    assert int(projected_candidates.get("row_count") or 0) == len(original_candidates), "A3_CANDIDATE_COUNT_MISMATCH"
    assert int(projected_plans.get("row_count") or 0) == len(original_plans), "A3_PLAN_COUNT_MISMATCH"
    assert projection.get("metrics", {}).get("a3_plan_count") == len(original_plans), "A3_METRIC_COUNT_MISMATCH"

    non_effective = [row for row in original_events if not row.get("effective")]
    expected_groups = Counter(
        (str(row.get("symbol") or "UNKNOWN"), str(row.get("reason_code") or "UNKNOWN"))
        for row in non_effective
    )
    group_projection = projection.get("a4", {}).get("observation_groups") or {}
    group_rows = _table(group_projection)
    actual_groups = Counter(
        {(str(row["symbol"]), str(row["reason_code"])): int(row["observation_count"]) for row in group_rows}
    )
    assert actual_groups == expected_groups, "A4_REASON_COUNT_MISMATCH"
    assert int(group_projection.get("observation_count") or 0) == len(non_effective), "A4_AGGREGATE_COUNT_MISMATCH"

    expected_strategy_reasons: Counter[str] = Counter()
    for row in non_effective:
        expected_strategy_reasons.update(set(str(value) for value in row.get("strategy_reason_codes") or []))
    assert dict(sorted(expected_strategy_reasons.items())) == group_projection.get("strategy_reason_totals"), (
        "A4_STRATEGY_REASON_COUNT_MISMATCH"
    )
    raw_anomaly_count = sum(str(row.get("action")) == "DATA_BLOCK" for row in non_effective)
    projected_anomaly_count = sum(
        int(row["observation_count"])
        for row in group_rows
        if row.get("category") == "ANOMALY"
    )
    assert projected_anomaly_count == raw_anomaly_count, "A4_ANOMALY_LOSS"
    assert projection.get("metrics") == facts.get("metrics"), "AUTHORITATIVE_METRICS_CHANGED"

    after = path.read_bytes()
    assert after == before and _sha256(after) == source_sha256, "FROZEN_INPUT_CHANGED"
    return {
        "file": str(path.resolve()),
        "source_bytes": len(before),
        "source_sha256": source_sha256,
        "input_hash": facts.get("input_hash"),
        "review_kind": facts.get("review_kind"),
        "prompt_chars": diagnostics["prompt_chars"],
        "target_chars": diagnostics["target_chars"],
        "warning_chars": diagnostics["warning_chars"],
        "hard_limit_chars": diagnostics["limit_chars"],
        "effective_event_count": len(original_effective),
        "lifecycle_count": len(facts.get("a4", {}).get("lifecycles") or []),
        "counterexample_count": len(original_counterexamples),
        "anomaly_observation_count": raw_anomaly_count,
        "a2_candidate_count": len(original_a2),
        "a3_candidate_count": len(original_candidates),
        "a3_plan_count": len(original_plans),
        "a4_original_event_count": len(original_events),
        "a4_aggregated_observation_count": len(non_effective),
        "market_refetched": False,
        "model_called": False,
        "database_written": False,
        "frozen_input_unchanged": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("facts", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    resolved_inputs = {path.resolve() for path in args.facts}
    if args.output.resolve() in resolved_inputs:
        parser.error("output must not overwrite a frozen facts file")
    prompts = PromptRepository(Path(__file__).resolve().parents[1] / "prompts")
    results = [verify(path, prompts) for path in args.facts]
    receipt = {
        "schema_version": "a5-frozen-projection-acceptance/1",
        "status": "PASSED",
        "inputs": results,
    }
    atomic_write_json(args.output, receipt)
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
