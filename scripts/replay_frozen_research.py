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
from liangjian_funnel.pipeline.deterministic import screen_a2, screen_a3
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot,
    LaneResult,
    ResearchPipeline,
    ResearchRunResult,
    StageAudit,
    _build_a3_candidate_domain,
    _lane_status_from_stages,
    _stage_completed,
    _with_a2_bottleneck_context,
    _with_a3_candidate_context,
    enrich_candidate_metadata,
)
from liangjian_funnel.pipeline.research_reports import write_stage_markdown_reports
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime.progress import WorkflowProgress
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
        help="optional configured model override for an isolated A2/A3 comparison",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="publish validated plans and write the normal outputs/runs summary",
    )
    parser.add_argument("--resume-audit", help="validated lane audit under outputs/research")
    parser.add_argument(
        "--stage",
        choices=("A2", "A3"),
        help="resume only A2 from validated A1, or A3 from validated A2",
    )
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
    progress = WorkflowProgress(
        settings.workflow_progress_path,
        run_id=run_id,
        job=f"replay-{str(args.stage or 'research').lower()}",
        now=current,
    )
    if args.stage:
        progress.set_phase(f"RESEARCH_{args.stage}", now=current)

    def research_progress(event: dict[str, object]) -> None:
        progress.research_event(event)

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
        progress_callback=research_progress,
        checkpoint_store=application.research_checkpoints,
        stage_snapshot_enricher=application._stage_snapshot_enricher,
    )
    if bool(args.resume_audit) != bool(args.stage):
        raise SystemExit("RESUME_AUDIT_AND_STAGE_REQUIRED_TOGETHER")
    if args.resume_audit:
        try:
            exit_code = _resume_stage(
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
        except BaseException:
            progress.finish(status="FAILED", phase="FAILED", reason_code="REPLAY_STAGE_FAILED")
            raise
        progress.finish(
            status="COMPLETED" if exit_code == 0 else "BLOCKED",
            phase="COMPLETED" if exit_code == 0 else "FAILED",
            reason_code=None if exit_code == 0 else "REPLAY_STAGE_BLOCKED",
        )
        return exit_code

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
    exit_code = 0 if result.status == "READY" else 2
    progress.finish(
        status="COMPLETED" if exit_code == 0 else "BLOCKED",
        phase="COMPLETED" if exit_code == 0 else "FAILED",
        reason_code=None if exit_code == 0 else "REPLAY_RESEARCH_BLOCKED",
    )
    return exit_code


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


def _resume_stage_rows(
    raw: dict[str, object],
    *,
    stage: str,
    audit_root: Path,
) -> tuple[dict[str, object] | None, tuple[dict[str, object], ...]]:
    """Resolve the upstream stage and complete A1/A2 lineage for a resume.

    An isolated A2 replay intentionally writes ``a2_stage`` instead of a
    normal lane ``stages`` array.  A3 must still be able to consume that
    artifact, so recover its A1 lineage from the bounded ``resume_source_audit``
    path and combine it with the newly validated A2 stage.
    """

    if stage == "A3" and str(raw.get("run_role") or "") == "A2_ISOLATED_REPLAY":
        a2_stage = raw.get("a2_stage")
        if not isinstance(a2_stage, dict):
            return None, ()
        source_value = raw.get("resume_source_audit")
        source_path = Path(str(source_value or "")).expanduser().resolve()
        if source_path.parent != audit_root or not source_path.is_file():
            raise SystemExit("RESUME_A1_LINEAGE_PATH_INVALID")
        try:
            source_raw = json.loads(source_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SystemExit("RESUME_A1_LINEAGE_INVALID") from exc
        a1_stage = next(
            (
                item
                for item in source_raw.get("stages", ())
                if isinstance(item, dict) and item.get("stage") == "A1"
            ),
            None,
        ) if isinstance(source_raw, dict) else None
        lineage = (
            (a1_stage, a2_stage)
            if isinstance(a1_stage, dict)
            else ()
        )
        return a2_stage, lineage

    previous_stage = "A1" if stage == "A2" else "A2"
    stage_rows = tuple(
        item
        for item in raw.get("stages", ())
        if isinstance(item, dict)
    )
    previous = next(
        (item for item in stage_rows if item.get("stage") == previous_stage),
        None,
    )
    lineage = tuple(
        item for item in stage_rows if item.get("stage") in {"A1", "A2"}
    )
    return previous, lineage


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
    previous, lineage_rows = _resume_stage_rows(
        raw,
        stage=stage,
        audit_root=audit_root,
    )
    if not isinstance(previous, dict) or not _stage_completed(str(previous.get("status") or "")):
        raise SystemExit("RESUME_UPSTREAM_STAGE_NOT_VALIDATED")
    if not isinstance(previous.get("output"), dict):
        raise SystemExit("RESUME_UPSTREAM_OUTPUT_MISSING")
    if not str(previous.get("snapshot_id") or "").startswith(snapshot.snapshot_id):
        raise SystemExit("RESUME_SNAPSHOT_LINEAGE_MISMATCH")
    lane_id = str(raw.get("lane") or "")
    model = str(model_override or raw["model"]).strip()
    if model not in settings.research_models:
        raise SystemExit("RESUME_MODEL_NOT_CONFIGURED")
    if stage == "A2":
        if publish:
            raise SystemExit("ISOLATED_A2_REPLAY_CANNOT_PUBLISH")
        return _resume_a2(
            pipeline=pipeline,
            snapshot=snapshot,
            settings=settings,
            previous=previous,
            audit_path=audit_path,
            lane_id=lane_id,
            model=model,
            run_id=run_id,
            slot=slot,
            generated_at=generated_at,
        )

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

    previous_stages = tuple(_stage_audit_from_dict(item) for item in lineage_rows)
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


def _resume_a2(
    *,
    pipeline: ResearchPipeline,
    snapshot: FrozenInputSnapshot,
    settings: Settings,
    previous: dict[str, object],
    audit_path: Path,
    lane_id: str,
    model: str,
    run_id: str,
    slot: str,
    generated_at: datetime,
) -> int:
    """Replay only A2 from one validated A1 artifact.

    The replay is deliberately non-publishable.  It writes an isolated audit
    instead of replacing the normal A1-A3 lane result, so the dashboard and
    next-session plans cannot mistake an A2 experiment for a complete run.
    """

    upstream_output = previous.get("output")
    if not isinstance(upstream_output, dict):
        raise SystemExit("RESUME_UPSTREAM_OUTPUT_MISSING")
    upstream_symbols = {
        str(item.get("symbol") or item.get("code") or "").strip().upper()
        for item in upstream_output.get("active_research_pool", ())
        if isinstance(item, dict)
        and str(item.get("symbol") or item.get("code") or "").strip()
    }
    if not upstream_symbols:
        upstream_symbols = {
            str(value).strip().upper()
            for value in previous.get("symbols", ())
            if isinstance(value, str) and value.strip()
        }
    if not upstream_symbols:
        raise SystemExit("RESUME_A2_CANDIDATE_DOMAIN_EMPTY")

    try:
        stage_snapshot = pipeline._enrich_stage_snapshot(
            stage="A2",
            lane_id=lane_id,
            model=model,
            upstream_symbols=upstream_symbols,
            snapshot=snapshot,
        )
    except Exception as exc:
        raise SystemExit("RESUME_A2_SNAPSHOT_ENRICHMENT_FAILED") from exc
    gate = screen_a2(
        stage_snapshot.data,
        upstream_output,
        minimum_identifiability_score=float(
            stage_snapshot.data.get("MIN_IDENTIFIABILITY_SCORE") or 60.0
        ),
        llm_top_n_per_theme=settings.a2_llm_top_n_per_theme,
        review_all_eligible=settings.a2_review_all_eligible,
    )
    stage_snapshot = _with_a2_bottleneck_context(stage_snapshot, gate)
    pipeline._emit_gate_progress(run_id, lane_id, model, gate)
    audit = pipeline._run_v2_downstream_review(
        lane_id=lane_id,
        model=model,
        stage="A2",
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

    isolated_path = (
        settings.workflow_output_dir
        / "research"
        / f"research_{run_id}_{lane_id}_A2_ONLY.json"
    )
    payload = {
        "run_id": run_id,
        "run_role": "A2_ISOLATED_REPLAY",
        "slot": slot,
        "generated_at": generated_at.isoformat(),
        "lane": lane_id,
        "model": model,
        "status": audit.status,
        "snapshot": {
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_hash": snapshot.snapshot_hash,
            "as_of": snapshot.as_of.isoformat() if snapshot.as_of else None,
        },
        "resume_source_audit": str(audit_path),
        "upstream_a1_count": len(upstream_symbols),
        "a2_gate_summary": gate.summary,
        "a2_stage": audit.as_dict(),
        "publishable": False,
    }
    atomic_write_json(isolated_path, payload)
    print(
        json.dumps(
            {
                "run_id": run_id,
                "run_role": "A2_ISOLATED_REPLAY",
                "lane": lane_id,
                "model": model,
                "stage": "A2",
                "status": audit.status,
                "reason_codes": list(audit.reason_codes),
                "latency_ms": audit.latency_ms,
                "attempts": audit.attempts,
                "symbol_count": len(audit.symbols),
                "pool_counts": _pool_counts(audit.output, "A2"),
                "candidate_count": len(upstream_symbols),
                "gate_summary": gate.summary,
                "audit_path": str(isolated_path),
                "publishable": False,
            },
            ensure_ascii=False,
        )
    )
    return 0 if _stage_completed(audit.status) else 2


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
