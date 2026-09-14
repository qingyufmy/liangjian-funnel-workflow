"""Recheck a failed A3 against its sealed base and validated A2; never publish plans."""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.deterministic import screen_a3
from liangjian_funnel.pipeline.research import (
    ResearchPipeline, _build_a3_candidate_domain, _with_a3_candidate_context,
    _with_a3_deterministic_context, _sha256_json,
)
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication


def recover(root: Path, source_run: str, *, quant_only: bool = False) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,180}", source_run):
        raise ValueError("INVALID_SOURCE_RUN")
    settings = Settings.from_env(root=root)
    output_dir = settings.workflow_output_dir
    summary = json.loads((output_dir / "runs" / f"{source_run}.json").read_text(encoding="utf-8"))
    if summary.get("status") != "BLOCKED" or summary.get("a1_reused") is not True:
        raise ValueError("REQUIRES_FAILED_RESEARCH_WITH_REUSED_A1")
    lane_id = settings.research_primary_lane_id
    lane = json.loads((output_dir / "research" / f"research_{source_run}_{lane_id}.json").read_text(encoding="utf-8"))
    stages = {stage["stage"]: stage for stage in lane["stages"]}
    if stages["A2"]["status"] != "VALIDATED" or stages["A3"]["status"] != "BLOCKED":
        raise ValueError("REQUIRES_VALIDATED_A2_AND_BLOCKED_A3")
    a2 = stages["A2"]["output"]
    app = WorkflowApplication(settings)
    prepared = app._load_research_snapshot_by_id(
        summary["snapshot"]["snapshot_id"], expected_date=summary["source_as_of"][:10],
    )
    snapshot = prepared.snapshot
    # Restore the already-reviewed per-stock A2 evidence, not fresh news.
    attempt_dir = output_dir / "research" / "a2_review_attempts" / source_run / lane_id
    attempts = [json.loads(path.read_text(encoding="utf-8")) for path in attempt_dir.glob("*.json")]
    approved = [a for a in attempts if not a.get("validation_reasons")
                and a.get("snapshot_id") == stages["A2"]["snapshot_id"]]
    if not approved:
        raise ValueError("VALIDATED_A2_EVIDENCE_NOT_FOUND")
    approved.sort(key=lambda a: a["semantic_attempt"])
    selected_attempt = approved[-1]
    if selected_attempt.get("record_hash") != _sha256_json({k:v for k,v in selected_attempt.items() if k != "record_hash"}):
        raise ValueError("A2_EVIDENCE_HASH_MISMATCH")
    restored_context = selected_attempt["review_context"]
    restored_hash = _sha256_json({"base_snapshot_hash": snapshot.snapshot_hash,
                                 "approved_a2_record_hash": selected_attempt["record_hash"],
                                 "A2_BOTTLENECK_CONTEXT": restored_context})
    pipeline = ResearchPipeline(
        settings, prompt_repository=app.prompts, model_client=app.model_client,
        output_dir=output_dir / "recovery", runtime_store=None,
        batch_workers=1, parallel_lanes=False,
        stage_snapshot_enricher=app._stage_snapshot_enricher,
        progress_callback=lambda event: print(json.dumps(event, ensure_ascii=True), flush=True),
    )
    snapshot = type(snapshot)(
        snapshot_id=f"{snapshot.snapshot_id}:recovery-a2:{restored_hash[:12]}", snapshot_hash=restored_hash,
        as_of=snapshot.as_of,
        data={**snapshot.data, "A2_BOTTLENECK_CONTEXT": restored_context},
    )
    upstream, origins = _build_a3_candidate_domain(a2)
    snapshot = pipeline._enrich_stage_snapshot(
        stage="A3", lane_id=lane_id, model=lane["model"],
        upstream_symbols=set(origins), snapshot=snapshot,
    )
    snapshot = _with_a3_candidate_context(snapshot, origins)
    gate = screen_a3(snapshot.data, upstream)
    snapshot = _with_a3_deterministic_context(snapshot, gate)
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    run_id = f"a3-recovery-{now.strftime('%Y%m%dT%H%M%S%f')}"
    receipt = {
        "run_id": run_id, "source_run_id": source_run,
        "source_snapshot_hash": prepared.snapshot.snapshot_hash,
        "approved_a2_record_hash": selected_attempt["record_hash"],
        "source_as_of": summary["source_as_of"], "started_at": now.isoformat(),
        "mode": "FROZEN_RESEARCH_RECOVERY", "execution_publication": "UNCHANGED",
        "plan_publication": {"created": [], "activated": []},
        "a1_reused": True, "a2_reused": True, "fresh_market_data_claimed": False,
        "input_count": len(origins), "quant_review_count": len(gate.review_symbols),
        "quant_monitor_count": len(gate.monitor_symbols),
        "quant_rejected_count": len(gate.rejected_symbols),
        "data_gap_rows": [d for d in gate.decisions if d["status"] == "DATA_GAP"],
        "quant_review_symbols": list(gate.review_symbols),
        "status": "QUANT_VERIFIED" if quant_only else "RUNNING",
    }
    path = output_dir / "recovery" / f"{run_id}.json"
    atomic_write_json(path, receipt)
    print(json.dumps({k:receipt[k] for k in ("run_id", "input_count", "quant_review_count", "quant_monitor_count", "quant_rejected_count", "status")}), flush=True)
    if not quant_only:
        bundle = app.prompts.load()
        try:
            audit = pipeline._run_v2_downstream_review(
                lane_id=lane_id, model=lane["model"], stage="A3", snapshot=snapshot,
                upstream_output=upstream, full_upstream_symbols=set(origins),
                gate=gate, bundle=bundle, run_id=run_id,
            )
        except Exception as exc:
            receipt.update(status="BLOCKED", reason_code=getattr(exc, "reason_code", type(exc).__name__),
                           finished_at=datetime.now(ZoneInfo("Asia/Shanghai")).isoformat())
            atomic_write_json(path, receipt)
            raise
        receipt.update(status=audit.status, a3=audit.as_dict(), finished_at=datetime.now(ZoneInfo("Asia/Shanghai")).isoformat())
        if not set(audit.symbols).issubset(origins):
            raise ValueError("RECOVERY_LINEAGE_VIOLATION")
        atomic_write_json(path, receipt)
    return {"path": str(path), "status": receipt["status"], "run_id": run_id}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", required=True)
    parser.add_argument("--quant-only", action="store_true")
    args = parser.parse_args()
    result = recover(Path.cwd(), args.source_run, quant_only=args.quant_only)
    print(json.dumps(result, ensure_ascii=True))
    raise SystemExit(0 if result["status"] in {"QUANT_VERIFIED", "VALIDATED", "VALIDATED_NO_SETUP"} else 2)
