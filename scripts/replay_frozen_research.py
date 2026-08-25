#!/usr/bin/env python3
"""Hash-verify and replay A1-A3 from a persisted research snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.research import FrozenInputSnapshot, ResearchPipeline
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.settings import Settings


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
    parser.add_argument("--resume-audit", help="validated lane audit under outputs/research")
    parser.add_argument("--stage", choices=("A2", "A3"), help="single downstream stage to replay")
    args = parser.parse_args()

    settings = Settings.from_env()
    path = _snapshot_path(settings, args.snapshot)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit("RESEARCH_SNAPSHOT_INVALID") from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("data"), dict):
        raise SystemExit("RESEARCH_SNAPSHOT_SCHEMA_INVALID")
    expected_hash = raw.get("snapshot_hash")
    if not isinstance(expected_hash, str) or _canonical_hash(raw["data"]) != expected_hash:
        raise SystemExit("RESEARCH_SNAPSHOT_HASH_MISMATCH")

    current = datetime.now(ZoneInfo(settings.timezone))
    snapshot = FrozenInputSnapshot(
        snapshot_id=str(raw.get("snapshot_id") or ""),
        snapshot_hash=expected_hash,
        as_of=raw.get("as_of"),
        data=raw["data"],
    )
    run_id = args.run_id or f"{current.date()}-{args.slot}-replay-{expected_hash[:12]}"
    pipeline = ResearchPipeline(
        settings,
        prompt_repository=settings.prompt_dir,
        output_dir=settings.workflow_output_dir / "research",
        parallel_lanes=True,
        runtime_store=RuntimeStore(settings.state_db_path),
        slot=args.slot,
    )
    if bool(args.resume_audit) != bool(args.stage):
        raise SystemExit("RESUME_AUDIT_AND_STAGE_REQUIRED_TOGETHER")
    if args.resume_audit:
        return _resume_stage(pipeline, snapshot, settings, args.resume_audit, args.stage, run_id)

    result = pipeline.run(snapshot, run_id=run_id, generated_at=current)
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "status": result.status,
                "snapshot_id": result.snapshot_id,
                "markdown": str(result.markdown_path) if result.markdown_path else None,
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


def _resume_stage(
    pipeline: ResearchPipeline,
    snapshot: FrozenInputSnapshot,
    settings: Settings,
    requested_audit: str,
    stage: str,
    run_id: str,
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
    if (
        not isinstance(previous, dict)
        or previous.get("status") != "VALIDATED"
        or not isinstance(previous.get("output"), dict)
        or not isinstance(previous.get("symbols"), list)
        or not previous["symbols"]
    ):
        raise SystemExit("RESUME_UPSTREAM_STAGE_NOT_VALIDATED")
    if previous.get("snapshot_id") != snapshot.snapshot_id:
        raise SystemExit("RESUME_SNAPSHOT_LINEAGE_MISMATCH")

    audit = pipeline._run_stage(
        lane_id=str(raw.get("lane") or ""),
        model=str(raw["model"]),
        stage=stage,
        snapshot=snapshot,
        upstream_output=previous["output"],
        upstream_symbols={str(symbol) for symbol in previous["symbols"]},
        bundle=pipeline.prompts.load(),
        run_id=run_id,
    )
    output_path = audit_root / f"research_{run_id}_{raw.get('lane')}_{stage.lower()}_replay.json"
    atomic_write_json(output_path, audit.as_dict())
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
                "audit_path": str(output_path),
            },
            ensure_ascii=False,
        )
    )
    return 0 if audit.status == "VALIDATED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
