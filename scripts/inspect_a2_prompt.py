#!/usr/bin/env python3
"""Inspect one frozen A2 request without calling the model provider."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

from liangjian_funnel.pipeline.deterministic import screen_a2
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot,
    ResearchPipeline,
    _build_a2_theme_batches,
    _prompt_replacements,
    _with_a2_bottleneck_context,
)
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication


def _hash(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_object(path: str) -> dict[str, object]:
    value = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SystemExit("JSON_OBJECT_REQUIRED")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--resume-audit", required=True)
    parser.add_argument("--rotation-snapshot-overlay")
    parser.add_argument("--model")
    parser.add_argument("--batch-size", type=int, default=5)
    args = parser.parse_args()

    settings = Settings.from_env()
    raw = _load_object(args.snapshot)
    data = raw.get("data")
    if not isinstance(data, dict) or _hash(data) != raw.get("snapshot_hash"):
        raise SystemExit("RESEARCH_SNAPSHOT_HASH_MISMATCH")
    snapshot_data = dict(data)
    if args.rotation_snapshot_overlay:
        overlay = _load_object(args.rotation_snapshot_overlay)
        overlay_hash = overlay.get("content_hash")
        overlay_body = dict(overlay)
        overlay_body.pop("content_hash", None)
        if not isinstance(overlay_hash, str) or _hash(overlay_body) != overlay_hash:
            raise SystemExit("ROTATION_SNAPSHOT_OVERLAY_HASH_MISMATCH")
        snapshot_data["SELECTED_BOARD_SNAPSHOT"] = overlay

    audit = _load_object(args.resume_audit)
    a1 = next(
        (
            row
            for row in audit.get("stages", ())
            if isinstance(row, dict)
            and row.get("stage") == "A1"
            and isinstance(row.get("output"), dict)
        ),
        None,
    )
    if not isinstance(a1, dict):
        raise SystemExit("VALIDATED_A1_OUTPUT_REQUIRED")
    upstream_output = a1["output"]
    upstream_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in upstream_output.get("active_research_pool", ())
        if isinstance(row, dict) and str(row.get("symbol") or "").strip()
    }
    if not upstream_symbols:
        raise SystemExit("A1_ACTIVE_POOL_EMPTY")

    as_of = datetime.fromisoformat(str(raw.get("as_of")))
    snapshot = FrozenInputSnapshot(
        snapshot_id=str(raw.get("snapshot_id") or ""),
        snapshot_hash=_hash(snapshot_data),
        as_of=as_of,
        data=snapshot_data,
    )
    application = WorkflowApplication(settings)
    pipeline = ResearchPipeline(
        settings,
        prompt_repository=settings.prompt_dir,
        model_client=application.model_client,
        output_dir=settings.workflow_output_dir / "research",
        runtime_store=RuntimeStore(settings.state_db_path),
        slot="close",
        checkpoint_store=application.research_checkpoints,
        stage_snapshot_enricher=application._stage_snapshot_enricher,
    )
    model = str(args.model or audit.get("model") or settings.research_models[0])
    stage_snapshot = pipeline._enrich_stage_snapshot(
        stage="A2",
        lane_id=str(audit.get("lane") or "lane_1"),
        model=model,
        upstream_symbols=upstream_symbols,
        snapshot=snapshot,
    )
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
    review_symbols = {
        str(symbol).strip().upper()
        for symbol in gate.review_symbols
        if str(symbol).strip()
    }
    batches = _build_a2_theme_batches(
        upstream_output,
        review_symbols,
        max(1, args.batch_size),
        snapshot_data=stage_snapshot.data,
    )
    if not batches:
        raise SystemExit("A2_REVIEW_BATCH_EMPTY")
    batch = set(batches[0])
    prepared = pipeline._prepare_stage_request(
        lane_id=str(audit.get("lane") or "lane_1"),
        model=model,
        stage="A2",
        snapshot=stage_snapshot,
        upstream_output=upstream_output,
        upstream_symbols=batch,
        bundle=pipeline.prompts.load(),
        projection_symbols=batch,
    )
    replacements = _prompt_replacements(
        pipeline.prompts.load(),
        "A2",
        stage_snapshot,
        upstream_output,
        projection_symbols=batch,
    )
    nested_sizes: dict[str, dict[str, int]] = {}
    for replacement_name in ("A2_BOTTLENECK_CONTEXT", "UPSTREAM_ACTIVE_POOL"):
        value = replacements.get(replacement_name)
        if isinstance(value, dict):
            entries = value.items()
        elif isinstance(value, list):
            entries = ((str(index), item) for index, item in enumerate(value))
        else:
            continue
        nested_sizes[replacement_name] = dict(
            sorted(
                (
                    (str(key), len(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)))
                    for key, item in entries
                ),
                key=lambda item: (-item[1], item[0]),
            )[:20]
        )
    bottleneck = replacements.get("A2_BOTTLENECK_CONTEXT")
    first_bottleneck_field_sizes: dict[str, int] = {}
    if isinstance(bottleneck, dict) and batch:
        first_row = bottleneck.get(sorted(batch)[0])
        if isinstance(first_row, dict):
            first_bottleneck_field_sizes = dict(
                sorted(
                    (
                        (str(key), len(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)))
                        for key, item in first_row.items()
                    ),
                    key=lambda item: (-item[1], item[0]),
                )
            )
    print(
        json.dumps(
            {
                "a1_active_count": len(upstream_symbols),
                "a2_review_count": len(review_symbols),
                "batch_count": len(batches),
                "first_batch_symbols": sorted(batch),
                "prompt_chars": prepared.prompt_chars,
                "estimated_input_tokens": prepared.estimated_input_tokens,
                "largest_prompt_replacements": dict(
                    sorted(
                        prepared.replacement_chars.items(),
                        key=lambda item: (-item[1], item[0]),
                    )
                ),
                "largest_nested_entries": nested_sizes,
                "first_bottleneck_field_sizes": first_bottleneck_field_sizes,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
