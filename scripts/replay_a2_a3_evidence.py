"""Offline A2/A3 evidence-handoff comparison. No LLM or plan publication."""
import argparse
from collections import Counter
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path

from liangjian_funnel.pipeline.deterministic import screen_a2, screen_a3
from liangjian_funnel.pipeline.emotion_theme import bind_emotion_themes
from liangjian_funnel.pipeline.factors import FactorEngine
from liangjian_funnel.pipeline.research import _build_a3_candidate_domain
from liangjian_funnel.pipeline.technical_aggregates import build_technical_aggregates
from liangjian_funnel.workflow import _compact_factor


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def replay(directory: Path, day: str, reuse_a2: Path | None = None):
    research = read(next(directory.glob(f"research*09-{day}*lane_1.json")))
    capture = read(directory / f"capture-09{day}.json")
    frozen = read(directory / f"{capture['snapshot_id']}.json")
    snapshot = {**frozen["data"], "as_of": frozen["as_of"]}
    a1, a2 = research["stages"][0]["output"], research["stages"][1]["output"]
    original = {s: {r["symbol"]: r for r in capture["decisions"] if r["stage"] == s}
                for s in ("A2_LOCAL_ROLE", "A3_LOCAL_TECHNICAL")}
    retained = read(reuse_a2) if reuse_a2 else None
    if retained and retained["run_id"] != capture["run_id"]:
        raise ValueError("A2 evidence belongs to another run")
    baseline_a2 = None if retained else screen_a2(snapshot, a1, review_all_eligible=True)
    original_a2 = original["A2_LOCAL_ROLE"]
    a2_mismatch = retained["a2_baseline_mismatches"] if retained else [r["symbol"] for r in baseline_a2.decisions if
                   (r["status"], r["sent_to_llm"]) !=
                   (original_a2[r["symbol"]]["status"], original_a2[r["symbol"]]["sent_to_llm"])]
    overlay = {r["symbol"] for r in a1["active_research_pool"] if r.get("selection_basis") == "DAILY_EMOTION_OVERLAY"}
    bindings = bind_emotion_themes(a1, snapshot, overlay)
    changed_a1 = deepcopy(a1)
    for r in changed_a1["active_research_pool"]:
        if r["symbol"] in bindings:
            b = bindings[r["symbol"]]
            r.update(primary_theme=b["theme_id"], industry_chain_node=b["node_id"], emotion_theme_binding=b)
    changed_a2_gate = None if retained else screen_a2(snapshot, changed_a1, review_all_eligible=True) if overlay else baseline_a2
    # Rebuild using only records known at the original cutoff, no intraday bars.
    factor_snapshot, price_levels, patterns = {}, {}, {}
    for symbol, bars in capture["daily_bars"].items():
        factor = FactorEngine(symbol).compute(daily_bars=bars, as_of=datetime.fromisoformat(capture["as_of"]))
        aggregates = build_technical_aggregates(factor,
            minimum_reward_risk=float(snapshot.get("MIN_REWARD_RISK", 2.5)),
            max_stop_distance_pct=float(snapshot.get("MAX_STOP_DISTANCE", .06)))
        factor_snapshot[symbol] = _compact_factor(factor.model_dump(mode="json"))
        price_levels[symbol], patterns[symbol] = aggregates["PRICE_LEVELS"], aggregates["KLINE_PATTERNS"]
    snapshot.update(FACTOR_SNAPSHOT=factor_snapshot, PRICE_LEVELS=price_levels, KLINE_PATTERNS=patterns)
    domain, origins = _build_a3_candidate_domain(a2)
    snapshot["A3_CANDIDATE_ORIGIN"] = origins
    baseline_a3 = screen_a3(snapshot, domain)
    changed_a2 = deepcopy(a2)
    for pool in ("focus_pool", "watch_only_pool"):
        for row in changed_a2.get(pool, []):
            b = bindings.get(row.get("symbol"))
            if b:
                row.update(primary_theme=b["theme_id"], theme_id=b["theme_id"],
                           node_id=b["node_id"], industry_chain_node=b["node_id"])
    # This is a controlled handoff experiment against existing A2 theme-stage
    # evidence, NOT a regenerated A2 model result or executable plan set.
    changed_domain, _ = _build_a3_candidate_domain(changed_a2)
    changed_a3 = screen_a3(snapshot, changed_domain)
    old3 = original["A3_LOCAL_TECHNICAL"]
    baseline3 = {r["symbol"]: r for r in baseline_a3.decisions}
    if set(baseline3) != set(old3):
        raise ValueError("incomplete A3 reconstruction domain")
    differences = [{"symbol": r["symbol"], "original": old3[r["symbol"]]["status"],
                    "reconstructed": r["status"], "original_reasons": old3[r["symbol"]]["reason_codes"],
                    "reconstructed_reasons": r["reason_codes"]}
                   for r in baseline_a3.decisions
                   if (r["status"], r["strategy_profile"], r["daily_ma"]) !=
                      (old3[r["symbol"]]["status"], old3[r["symbol"]]["strategy_profile"], old3[r["symbol"]]["daily_ma"])]
    changes = [{"symbol": r["symbol"], "binding": bindings.get(r["symbol"]),
                "before": baseline3[r["symbol"]]["status"], "after": r["status"],
                "before_reasons": baseline3[r["symbol"]]["reason_codes"], "after_reasons": r["reason_codes"]}
               for r in changed_a3.decisions if r["status"] != baseline3[r["symbol"]]["status"]]
    new_a2_rows = retained["a2_new_decisions"] if retained else changed_a2_gate.decisions
    eligible_a2 = {r["symbol"] for r in new_a2_rows if r["sent_to_llm"]}
    if not set(changed_a3.review_symbols).issubset(eligible_a2):
        raise ValueError("A3 qualified set escaped corrected A2 review domain")
    if not eligible_a2.issubset({r["symbol"] for r in changed_a1["active_research_pool"]}):
        raise ValueError("A2 review set escaped A1 domain")
    return {"run_id": capture["run_id"], "production_updated": False, "llm_executed": False,
            "a2_baseline_mismatches": a2_mismatch,
            "a2_baseline": retained["a2_baseline"] if retained else baseline_a2.summary,
            "a2_new": retained["a2_new"] if retained else changed_a2_gate.summary,
            "overlay_binding_counts": dict(Counter(b["reason_code"] for b in bindings.values())),
            "overlay_bindings": bindings, "a2_new_decisions": retained["a2_new_decisions"] if retained else changed_a2_gate.decisions,
            "a3_original_statuses": dict(Counter(r["status"] for r in old3.values())),
            "a3_reconstruction_differences": differences,
            "a3_reconstructed": baseline_a3.summary, "a3_handoff_experiment": changed_a3.summary,
            "a3_handoff_changes": changes,
            "macd_ready": sum(all(r["daily_macd"].get(k) is not None for k in ("dif", "dea", "hist")) for r in baseline_a3.decisions),
            "a3_decisions": changed_a3.decisions}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--day", choices=["07", "08"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reuse-a2", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("refusing to overwrite evidence")
    result = replay(args.directory, args.day, args.reuse_a2)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False)
    print(json.dumps({k: v for k, v in result.items() if k not in
                     {"overlay_bindings", "a2_new_decisions", "a3_decisions", "a3_handoff_changes"}}, ensure_ascii=True))
