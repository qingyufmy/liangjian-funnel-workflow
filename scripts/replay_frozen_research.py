#!/usr/bin/env python3
"""Hash-verify and replay A1-A3 from a persisted research snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.open_macro import OpenMacroDataCollector
from liangjian_funnel.pipeline.deterministic import screen_a3
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot,
    LaneResult,
    ResearchPipeline,
    ResearchRunResult,
    StageAudit,
    _build_a3_candidate_domain,
    _lane_status_from_stages,
    _stage_completed,
    _with_a3_candidate_context,
    enrich_candidate_metadata,
)
from liangjian_funnel.pipeline.research_reports import write_stage_markdown_reports
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _snapshot_path(settings: Settings, requested: str | None) -> Path:
    root = settings.snapshot_dir.resolve()
    if requested:
        path = Path(requested).expanduser().resolve()
    else:
        candidates = sorted(
            (path for path in root.glob("snapshot-*.json") if path.is_file()),
            key=lambda path: (path.stat().st_mtime_ns, path.name),
        )
        if not candidates:
            raise SystemExit("RESEARCH_SNAPSHOT_NOT_FOUND")
        path = candidates[-1].resolve()
    if path.parent != root or not path.is_file():
        raise SystemExit("RESEARCH_SNAPSHOT_PATH_INVALID")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", help="snapshot JSON under LIANGJIAN_SNAPSHOT_DIR; latest when omitted")
    parser.add_argument("--slot", choices=("morning", "close"), default="close")
    parser.add_argument("--run-id", help="optional stable audit run id")
    parser.add_argument(
        "--model",
        help="optional configured model override for an isolated A3 comparison",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="publish validated plans and write the normal outputs/runs summary",
    )
    parser.add_argument("--resume-audit", help="validated lane audit under outputs/research")
    parser.add_argument("--stage", choices=("A3",), help="resume A3 from a validated A1/A2 lane audit")
    parser.add_argument(
        "--enable-deterministic-v2-overlay",
        action="store_true",
        help="derive a non-publishable V2 validation snapshot from a pre-V2 frozen snapshot",
    )
    parser.add_argument(
        "--refresh-open-macro-overlay",
        action="store_true",
        help="attach same-day open macro contracts to a non-publishable validation replay",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    path = _snapshot_path(settings, args.snapshot)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit("RESEARCH_SNAPSHOT_INVALID") from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("data"), dict):
        raise SystemExit("RESEARCH_SNAPSHOT_SCHEMA_INVALID")
    base_snapshot_hash = raw.get("snapshot_hash")
    if not isinstance(base_snapshot_hash, str) or _canonical_hash(raw["data"]) != base_snapshot_hash:
        raise SystemExit("RESEARCH_SNAPSHOT_HASH_MISMATCH")
    if args.publish and (args.enable_deterministic_v2_overlay or args.refresh_open_macro_overlay):
        raise SystemExit("VALIDATION_OVERLAY_CANNOT_PUBLISH")
    snapshot_data = dict(raw["data"])
    snapshot_id = str(raw.get("snapshot_id") or "")
    expected_hash = base_snapshot_hash
    if args.enable_deterministic_v2_overlay:
        snapshot_data["DETERMINISTIC_RESEARCH_V2_ENABLED"] = True
        snapshot_data["research_pipeline_mode"] = "deterministic_v2"
        expected_hash = _canonical_hash(snapshot_data)
        snapshot_id = f"{snapshot_id}:deterministic-v2-validation"

    current = datetime.now(ZoneInfo(settings.timezone))
    raw_as_of = raw.get("as_of")
    try:
        snapshot_as_of = datetime.fromisoformat(str(raw_as_of))
    except (TypeError, ValueError) as exc:
        raise SystemExit("RESEARCH_SNAPSHOT_AS_OF_INVALID") from exc
    if snapshot_as_of.tzinfo is None or snapshot_as_of.utcoffset() is None:
        raise SystemExit("RESEARCH_SNAPSHOT_AS_OF_TIMEZONE_REQUIRED")
    if args.refresh_open_macro_overlay:
        if snapshot_as_of.astimezone(ZoneInfo(settings.timezone)).date() != current.date():
            raise SystemExit("OPEN_MACRO_OVERLAY_REQUIRES_SAME_DAY_SNAPSHOT")
        open_macro = OpenMacroDataCollector(
            cache_dir=settings.open_macro_cache_dir,
        ).collect(current)
        contract_names = (
            "MACRO_ECONOMIC_DATA",
            "ASSET_ROTATION_SNAPSHOT",
            "GLOBAL_MACRO_SNAPSHOT",
            "CROSS_MARKET_LEAD_SNAPSHOT",
            "INDUSTRY_ACTIVITY_DATA",
        )
        for contract_name in contract_names:
            contract = open_macro.get(contract_name)
            if isinstance(contract, dict):
                snapshot_data[contract_name] = contract
        manifest = dict(snapshot_data.get("snapshot_manifest") or {})
        manifest["open_macro_validation_overlay"] = {
            "schema_version": open_macro.get("schema_version"),
            "content_hash": open_macro.get("content_hash"),
            "cache_status": open_macro.get("cache_status"),
            "as_of": open_macro.get("as_of"),
            "non_publishable": True,
        }
        snapshot_data["snapshot_manifest"] = manifest
        expected_hash = _canonical_hash(snapshot_data)
        snapshot_id = f"{snapshot_id}:open-macro-validation"
    snapshot = FrozenInputSnapshot(
        snapshot_id=snapshot_id,
        snapshot_hash=expected_hash,
        as_of=snapshot_as_of,
        data=snapshot_data,
    )
    run_id = args.run_id or f"{current.date()}-{args.slot}-replay-{expected_hash[:12]}"
    application = WorkflowApplication(settings)
    pipeline = ResearchPipeline(
        settings,
        prompt_repository=settings.prompt_dir,
        model_client=application.model_client,
        output_dir=settings.workflow_output_dir / "research",
        parallel_lanes=True,
        runtime_store=RuntimeStore(settings.state_db_path),
        slot=args.slot,
        batch_workers=settings.research_batch_workers,
        checkpoint_store=application.research_checkpoints,
        stage_snapshot_enricher=application._stage_snapshot_enricher,
    )
    if bool(args.resume_audit) != bool(args.stage):
        raise SystemExit("RESUME_AUDIT_AND_STAGE_REQUIRED_TOGETHER")
    if args.resume_audit:
        return _resume_stage(
            application,
            pipeline,
            snapshot,
            settings,
            args.resume_audit,
            args.stage,
            run_id,
            slot=args.slot,
            publish=args.publish,
            generated_at=current,
            model_override=args.model,
        )

    result = pipeline.run(snapshot, run_id=run_id, generated_at=current)
    publication = None
    run_summary_path = None
    if args.publish:
        publication = application._publish_plans(
            result,
            args.slot,
            current,
            snapshot_data=snapshot.data,
        )
        summary = {
            "run_id": run_id,
            "slot": args.slot,
            "status": result.status,
            "snapshot": {
                "path": str(path),
                "snapshot_hash": snapshot.snapshot_hash,
                "snapshot_id": snapshot.snapshot_id,
                "as_of": snapshot.as_of,
            },
            "research_markdown": str(result.markdown_path) if result.markdown_path else None,
            "plan_publication": publication,
        }
        run_summary_path = settings.workflow_output_dir / "runs" / f"{run_id}.json"
        atomic_write_json(run_summary_path, summary)
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "status": result.status,
                "snapshot_id": result.snapshot_id,
                "base_snapshot_hash": base_snapshot_hash if (
                    args.enable_deterministic_v2_overlay or args.refresh_open_macro_overlay
                ) else None,
                "markdown": str(result.markdown_path) if result.markdown_path else None,
                "plan_publication": publication,
                "run_summary": str(run_summary_path) if run_summary_path else None,
                "lanes": [
                    {
                        "lane": lane.lane,
                        "model": lane.model,
                        "status": lane.status,
                        "stages": [
                            {
                                "stage": stage.stage,
                                "status": stage.status,
                                "reason_codes": list(stage.reason_codes),
                                "latency_ms": stage.latency_ms,
                                "attempts": stage.attempts,
                                "symbol_count": len(stage.symbols),
                                "pool_counts": _pool_counts(stage.output, stage.stage),
                            }
                            for stage in lane.stages
                        ],
                    }
                    for lane in result.lanes
                ],
            },
            ensure_ascii=False,
        )
    )
    return 0 if result.status == "READY" else 2


def _pool_counts(output: object, stage: str) -> dict[str, int]:
    if not isinstance(output, dict):
        return {}
    fields = {
        "A1": ("active_research_pool", "monitor_pool", "rejected_candidates"),
        "A2": ("focus_pool", "watch_only_pool", "rejected_candidates"),
        "A3": ("core_watch_pool", "secondary_watch_pool", "rejected_candidates"),
    }.get(stage, ())
    return {
        field: len(output.get(field)) if isinstance(output.get(field), list) else 0
        for field in fields
    }


def _resume_stage(
    application: WorkflowApplication,
    pipeline: ResearchPipeline,
    snapshot: FrozenInputSnapshot,
    settings: Settings,
    requested_audit: str,
    stage: str,
    run_id: str,
    *,
    slot: str,
    publish: bool,
    generated_at: datetime,
    model_override: str | None,
) -> int:
    audit_root = (settings.workflow_output_dir / "research").resolve()
    audit_path = Path(requested_audit).expanduser().resolve()
    if audit_path.parent != audit_root or not audit_path.is_file():
        raise SystemExit("RESUME_AUDIT_PATH_INVALID")
    try:
        raw = json.loads(audit_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit("RESUME_AUDIT_INVALID") from exc
    if not isinstance(raw, dict) or raw.get("model") not in settings.research_models:
        raise SystemExit("RESUME_AUDIT_SCHEMA_INVALID")
    previous_stage = "A1" if stage == "A2" else "A2"
    previous = next(
        (
            item
            for item in raw.get("stages", ())
            if isinstance(item, dict) and item.get("stage") == previous_stage
        ),
        None,
    )
    if not isinstance(previous, dict) or not _stage_completed(str(previous.get("status") or "")):
        raise SystemExit("RESUME_UPSTREAM_STAGE_NOT_VALIDATED")
    if not isinstance(previous.get("output"), dict):
        raise SystemExit("RESUME_UPSTREAM_OUTPUT_MISSING")
    if not str(previous.get("snapshot_id") or "").startswith(snapshot.snapshot_id):
        raise SystemExit("RESUME_SNAPSHOT_LINEAGE_MISMATCH")
    if stage != "A3":
        raise SystemExit("DETERMINISTIC_RESUME_SUPPORTS_A3_ONLY")

    lane_id = str(raw.get("lane") or "")
    model = str(model_override or raw["model"]).strip()
    if model not in settings.research_models:
        raise SystemExit("RESUME_MODEL_NOT_CONFIGURED")
    upstream_output, origins = _build_a3_candidate_domain(previous["output"])
    upstream_symbols = set(origins)
    if not upstream_symbols:
        raise SystemExit("RESUME_A3_CANDIDATE_DOMAIN_EMPTY")

    try:
        stage_snapshot = pipeline._enrich_stage_snapshot(
            stage="A3",
            lane_id=lane_id,
            model=model,
            upstream_symbols=upstream_symbols,
            snapshot=snapshot,
        )
    except Exception as exc:
        raise SystemExit("RESUME_A3_SNAPSHOT_ENRICHMENT_FAILED") from exc
    stage_snapshot = _with_a3_candidate_context(stage_snapshot, origins)
    gate = screen_a3(stage_snapshot.data, upstream_output)
    pipeline._emit_gate_progress(run_id, lane_id, model, gate)
    audit = pipeline._run_v2_downstream_review(
        lane_id=lane_id,
        model=model,
        stage="A3",
        snapshot=stage_snapshot,
        upstream_output=upstream_output,
        full_upstream_symbols=upstream_symbols,
        gate=gate,
        bundle=pipeline.prompts.load(),
        run_id=run_id,
    )
    if isinstance(audit.output, dict):
        enriched_output = enrich_candidate_metadata(audit.output, stage_snapshot.data)
        audit = StageAudit(
            lane=audit.lane,
            model=audit.model,
            stage=audit.stage,
            status=audit.status,
            snapshot_id=audit.snapshot_id,
            prompt_hash=audit.prompt_hash,
            input_hash=audit.input_hash,
            output_hash=_canonical_hash(enriched_output),
            latency_ms=audit.latency_ms,
            attempts=audit.attempts,
            thinking_variant=audit.thinking_variant,
            symbols=audit.symbols,
            reason_codes=audit.reason_codes,
            output=enriched_output,
            diagnostics=audit.diagnostics,
        )

    previous_stages = tuple(
        _stage_audit_from_dict(item)
        for item in raw.get("stages", ())
        if isinstance(item, dict) and item.get("stage") in {"A1", "A2"}
    )
    if tuple(item.stage for item in previous_stages) != ("A1", "A2"):
        raise SystemExit("RESUME_AUDIT_STAGE_LINEAGE_INVALID")
    stages = (*previous_stages, audit)
    lane_status = _lane_status_from_stages(stages)
    lane = LaneResult(
        lane=lane_id,
        model=model,
        status=lane_status,
        stages=stages,
        final_output=audit.output if lane_status in {"READY", "READY_DEGRADED"} else None,
    )
    lane_path = pipeline._write_lane_audit(run_id, lane, snapshot=snapshot)
    lane = LaneResult(
        lane=lane.lane,
        model=lane.model,
        status=lane.status,
        stages=lane.stages,
        final_output=lane.final_output,
        audit_path=lane_path,
    )
    result = ResearchRunResult(
        run_id=run_id,
        generated_at=generated_at,
        snapshot_id=snapshot.snapshot_id,
        snapshot_hash=snapshot.snapshot_hash,
        status=lane_status,
        lanes=(lane,),
        audit_paths=(lane_path,),
        markdown_path=None,
        primary_lane_ids=(lane_id,),
    )
    markdown_path = pipeline._write_markdown(result)
    result = ResearchRunResult(
        run_id=result.run_id,
        generated_at=result.generated_at,
        snapshot_id=result.snapshot_id,
        snapshot_hash=result.snapshot_hash,
        status=result.status,
        lanes=result.lanes,
        audit_paths=result.audit_paths,
        markdown_path=markdown_path,
        primary_lane_ids=result.primary_lane_ids,
    )
    stage_markdown = write_stage_markdown_reports(result, audit_root)
    publication = (
        application._publish_plans(
            result,
            slot,
            generated_at,
            snapshot_data=stage_snapshot.data,
        )
        if publish
        else None
    )
    run_summary_path = settings.workflow_output_dir / "runs" / f"{run_id}.json"
    atomic_write_json(
        run_summary_path,
        {
            "run_id": run_id,
            "slot": slot,
            "status": result.status,
            "run_role": "A3_RESUME",
            "models": [model],
            "primary_lane_ids": [lane_id],
            "outcome_v2": result.outcome().as_dict(),
            "snapshot": {
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_hash": snapshot.snapshot_hash,
                "as_of": snapshot.as_of.isoformat() if snapshot.as_of else None,
            },
            "research_markdown": str(markdown_path),
            "stage_markdown": stage_markdown,
            "plan_publication": publication,
            "resume_source_audit": str(audit_path),
            "a3_candidate_count": len(upstream_symbols),
            "a3_gate_summary": gate.summary,
        },
    )
    print(
        json.dumps(
            {
                "run_id": run_id,
                "lane": raw.get("lane"),
                "model": raw.get("model"),
                "stage": stage,
                "status": audit.status,
                "reason_codes": list(audit.reason_codes),
                "latency_ms": audit.latency_ms,
                "attempts": audit.attempts,
                "symbol_count": len(audit.symbols),
                "pool_counts": _pool_counts(audit.output, "A3"),
                "candidate_count": len(upstream_symbols),
                "gate_summary": gate.summary,
                "audit_path": str(lane_path),
                "run_summary": str(run_summary_path),
                "plan_publication": publication,
            },
            ensure_ascii=False,
        )
    )
    return 0 if result.status in {"READY", "READY_DEGRADED"} else 2


def _stage_audit_from_dict(raw: dict[str, object]) -> StageAudit:
    output = raw.get("output") if isinstance(raw.get("output"), dict) else None
    diagnostics = raw.get("diagnostics") if isinstance(raw.get("diagnostics"), dict) else None
    return StageAudit(
        lane=str(raw.get("lane") or ""),
        model=str(raw.get("model") or ""),
        stage=str(raw.get("stage") or ""),
        status=str(raw.get("status") or ""),
        snapshot_id=str(raw.get("snapshot_id") or ""),
        prompt_hash=str(raw.get("prompt_hash") or "") or None,
        input_hash=str(raw.get("input_hash") or "") or None,
        output_hash=str(raw.get("output_hash") or "") or None,
        latency_ms=int(raw["latency_ms"]) if isinstance(raw.get("latency_ms"), int) else None,
        attempts=int(raw.get("attempts") or 0),
        thinking_variant=str(raw.get("thinking_variant") or "unknown"),
        symbols=tuple(str(value) for value in raw.get("symbols", ()) if isinstance(value, str)),
        reason_codes=tuple(
            str(value) for value in raw.get("reason_codes", ()) if isinstance(value, str)
        ),
        output=output,
        diagnostics=diagnostics,
    )


if __name__ == "__main__":
    raise SystemExit(main())
