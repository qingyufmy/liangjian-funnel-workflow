"""Replay projections against immutable research/review artifacts, no runtime writes."""
import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from liangjian_funnel.pipeline.deterministic import _matched_links, _business_theme_match, screen_a2
from liangjian_funnel.pipeline.mature_theme_registry import taxonomy_is_business_related
from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.review.daily import _a2_projection, _model_fact_projection
from liangjian_funnel.review.context import render_a5_prompt
from liangjian_funnel.review.verification import counterexample_drop_stage


def audit(research, facts, prompt_dir, frozen_snapshot=None):
    a1 = next(row["output"] for row in research["stages"] if row["stage"] == "A1")
    a3 = next(row["output"] for row in research["stages"] if row["stage"] == "A3")
    a2, errors = _a2_projection(research)
    corrected = {}
    changes = []
    for row in a1.get("active_research_pool", []):
        links = [link for link in row.get("taxonomy_matches", []) if taxonomy_is_business_related(
            str(link.get("theme_id") or ""), str(link.get("taxonomy") or ""), str(link.get("taxonomy_name") or ""))]
        index = defaultdict(list)
        for link in links:
            index[link.get("taxonomy"), link.get("taxonomy_code")].append(link)
        ranked = _matched_links(links, index)
        primary = ranked[0] if ranked else {}
        exposure = row.get("business_exposure") or {}
        exposure = exposure if isinstance(exposure, dict) else {}
        item = {"symbol": row["symbol"], "name": row.get("name") or row.get("company_name"),
                "old_theme": row.get("primary_theme"), "ranked_theme": primary.get("theme_id"),
                "ranked_sector": primary.get("taxonomy_name"), "taxonomy": primary.get("taxonomy"),
                "business_name": exposure.get("business_name"), "source_ref": exposure.get("source_ref"),
                "business_match_confirmed": _business_theme_match([exposure], ranked, primary.get("theme_id"))}
        corrected[row["symbol"]] = item
        if item["old_theme"] != item["ranked_theme"]:
            changes.append(item)
    plan_symbols = {row["symbol"] for row in a3.get("core_watch_pool", [])}
    technical = {row["symbol"]: row for pool in ("core_watch_pool", "secondary_watch_pool", "rejected_candidates") for row in a3.get(pool, [])}
    by_symbol = {row["symbol"]: row for row in a2["candidates"]}
    misses = []
    for miss in facts.get("independent_verification", {}).get("counterexamples", []):
        symbol = miss["symbol"]
        candidate = by_symbol.get(symbol, {})
        misses.append({"symbol": symbol, "old_drop_stage": miss.get("drop_stage"),
            "actual_drop_stage": counterexample_drop_stage(symbol, candidate.get("pool", "UNKNOWN"), plan_symbols, technical),
            "reasons": candidate.get("reason_codes"), "mapping": corrected.get(symbol)})
    agriculture = []
    groups = facts.get("a4", {}).get("observation_groups", [])
    for symbol in sorted(plan_symbols):
        item = corrected.get(symbol, {})
        if item.get("old_theme") != "AGRICULTURE_FOOD_SECURITY" and item.get("ranked_theme") != "AGRICULTURE_FOOD_SECURITY":
            continue
        reasons = Counter()
        for group in groups:
            if group.get("symbol") == symbol:
                reasons.update(group.get("primary_reason_counts") or {})
        agriculture.append({**item, "a4_reason_counts": dict(reasons)})
    projected_facts = dict(facts)
    projected_facts["a2"] = {**facts.get("a2", {}), **a2}
    _, context = render_a5_prompt(PromptRepository(prompt_dir), "agent_5_daily_reviewer_v1.txt", _model_fact_projection(projected_facts))
    quant_replay = None
    if frozen_snapshot:
        snapshot = {**frozen_snapshot["data"], "as_of": frozen_snapshot["as_of"]}
        gate = screen_a2(snapshot, a1, review_all_eligible=True)
        by_decision = {row["symbol"]: row for row in gate.decisions}
        upstream = {row["symbol"] for row in a1.get("active_research_pool", [])}
        quant_replay = {"scope": "Same frozen board snapshot; latest deterministic A2, no LLM and no new A3 plans",
            "summary": gate.summary, "a2_subset_a1": set(by_decision).issubset(upstream),
            "decisions": list(gate.decisions),
            "counterexample_decisions": [by_decision.get(row["symbol"], {"symbol": row["symbol"], "status": "NOT_EVALUATED"}) for row in misses]}
    return {"production_updated": False, "historical_decisions_rewritten": False,
            "quant_replay": quant_replay,
            "a2_counts": a2["counts"], "quant_count": a2.get("quant_evaluated_count"), "lineage_errors": errors,
            "mapping_audited_count": len(corrected), "changed_primary_count": len(changes), "mapping_changes": changes,
            "ranked_mapping_available_count": sum(bool(row["ranked_theme"]) for row in corrected.values()),
            "ranked_mapping_unavailable_count": sum(not row["ranked_theme"] for row in corrected.values()),
            "changed_with_available_mapping_count": sum(bool(row["ranked_theme"]) for row in changes),
            "mapping_interpretation": "排序影响不等于已确认全部映射正确；无可用映射不删除旧记录，情绪动态池不自动重写。",
            "ranked_theme_counts": dict(Counter(item["ranked_theme"] for item in corrected.values())),
            "counterexamples": misses, "agriculture_plans": agriculture, "context": context,
            "scope": "Recomputed lineage and mapping priority only; not a new A1/A2/A3 model run or full A4 strategy replay."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--research", type=Path, required=True)
    parser.add_argument("--review", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path)
    args = parser.parse_args()
    research = json.loads(args.research.read_text(encoding="utf-8"))
    review = json.loads(args.review.read_text(encoding="utf-8"))
    frozen = json.loads(args.snapshot.read_text(encoding="utf-8")) if args.snapshot else None
    result = audit(research, review.get("facts", review), Path(__file__).resolve().parents[1] / "prompts", frozen)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("a2_counts", "quant_count", "lineage_errors", "mapping_audited_count", "changed_primary_count", "context")}, ensure_ascii=True))
    if result["quant_replay"]:
        print(json.dumps({"quant_replay": result["quant_replay"]["summary"], "a2_subset_a1": result["quant_replay"]["a2_subset_a1"]}, ensure_ascii=True))
