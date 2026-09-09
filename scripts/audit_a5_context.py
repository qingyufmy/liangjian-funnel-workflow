"""Audit frozen A5 input transport locally; no model, market or database calls."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.review.context import pack_evidence, render_a5_prompt
from liangjian_funnel.review.daily import _model_fact_projection
from liangjian_funnel.review.plan_scope import select_review_plans


def unpack(value):
    if isinstance(value, dict):
        if value.get("encoding") == "a5-column-table/1":
            missing = {tuple(cell) for cell in value.get("missing_cells", [])}
            return [{key: unpack(row[j]) for j, key in enumerate(value["columns"]) if (i, j) not in missing}
                    for i, row in enumerate(value["rows"])]
        return {key: unpack(item) for key, item in value.items()}
    return [unpack(item) for item in value] if isinstance(value, list) else value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("facts", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.resolve() in {path.resolve() for path in args.facts}:
        parser.error("Output must not overwrite a frozen input")
    prompts = PromptRepository(Path(__file__).resolve().parents[1] / "prompts")
    results = []
    for path in args.facts:
        original = path.read_bytes()
        facts = json.loads(original)
        projected = _model_fact_projection(facts)
        prompt, diagnostics = render_a5_prompt(prompts, "agent_5_daily_reviewer_v1.txt", projected)
        assert unpack(pack_evidence(projected)) == projected
        effective = [row["evidence_id"] for row in facts["a4"]["events"] if row.get("effective")]
        counterexamples = [row["evidence_id"] for row in facts.get("independent_verification", {}).get("counterexamples", [])]
        assert all(identity in prompt for identity in effective + counterexamples)
        observed = {row["plan_id"] for row in facts["a4"]["events"]}
        selected, carried, retired = select_review_plans(facts["a3"]["plans"],
            cutoff=datetime.fromisoformat(facts["cutoff_at"]), observed_ids=observed,
            carryover_ids={row["plan_id"] for row in facts["a4"].get("carryover_lifecycles", [])})
        assert all(row["plan_id"] not in observed for row in retired)
        assert path.read_bytes() == original
        results.append({"file": str(path), "sha256": hashlib.sha256(original).hexdigest(),
                        "input_hash": facts["input_hash"], "transport": diagnostics,
                        "lossless_roundtrip": True, "effective_events_retained": len(effective),
                        "counterexamples_retained": len(counterexamples),
                        "scope_measurement": {"original_plan_count": len(facts["a3"]["plans"]),
                            "session_plan_count": len(selected), "retired_count": len(retired),
                            "session_status_counts": dict(Counter(row["status"] for row in selected)),
                            "session_strategy_counts": dict(Counter(row["strategy_profile"] for row in selected)),
                            "carried_in_archived_input": len(carried),
                            "limitation": "Only archived rows evaluated; missing prior inventory is not reconstructed"}})
    atomic_write_json(args.output, {"scope": "LOCAL_FROZEN_INPUT_ACCEPTANCE", "model_called": False,
                                  "production_updated": False, "results": results})
    print(json.dumps(results, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
