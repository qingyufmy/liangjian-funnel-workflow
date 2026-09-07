"""Verify or replay only A1 discovery, without collection or production writes.

Default: prepare a hash-bound request, with no model call.
Use --execute explicitly for the existing bounded two-attempt DeepSeek review.
Always use a new output directory; existing evidence is never overwritten.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from liangjian_funnel.pipeline.monthly_strategy import build_monthly_strategy_context
from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.pipeline.research import ResearchPipeline, _coerce_snapshot, _sha256_json
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.settings import Settings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--expected-snapshot-hash", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    out = args.output_dir.resolve()
    if out.exists():
        parser.error("output directory already exists; do not duplicate a model run or overwrite evidence")
    raw = json.loads(args.snapshot.read_text(encoding="utf-8"))
    raw["as_of"] = datetime.fromisoformat(raw["as_of"])
    snapshot = _coerce_snapshot(raw)
    if snapshot.snapshot_hash != args.expected_snapshot_hash or _sha256_json(snapshot.data) != snapshot.snapshot_hash:
        parser.error("frozen snapshot hash mismatch")
    settings = Settings.from_env(root=root).model_copy(update={"feature_store_db_path": out / "features.sqlite3"})
    context = {"mode": "POLICY_MACRO_DISCOVERY", "monthly_strategy_context": build_monthly_strategy_context(
        snapshot.data, as_of=snapshot.as_of, prior_registry=None,
        policy_lookback_days=settings.a1_policy_lookback_days,
        policy_document_limit=settings.a1_policy_document_limit,
    )}
    bundle = PromptRepository(root / "prompts").load()
    # Reproduce Linux transport line endings without editing checked-out files.
    bundle = replace(bundle, documents={
        key: replace(value, text=value.text.replace("\r\n", "\n"))
        for key, value in bundle.documents.items()
    })
    def progress(event):
        atomic_write_json(out / "progress.json", dict(event))
        print(json.dumps({k: event.get(k) for k in ("stage", "status", "attempts", "diagnostics")}), flush=True)

    pipeline = ResearchPipeline(settings, output_dir=out, progress_callback=progress)
    stage_args = dict(lane_id="lane_1", model="deepseek-v4-pro-0813", stage="A1", snapshot=snapshot,
        upstream_output=None, upstream_symbols=set(), bundle=bundle, projection_symbols=set(),
        a1_discovery_context=context)
    request = pipeline._prepare_stage_request(**stage_args)
    code_files = sorted((root / "src/liangjian_funnel").rglob("*.py"))
    code_hash = _sha256_json({p.relative_to(root).as_posix(): p.read_text(encoding="utf-8") for p in code_files})
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    atomic_write_json(out / "request.json", {
        "snapshot_hash": snapshot.snapshot_hash, "snapshot_id": snapshot.snapshot_id,
        "prompt_hash": request.prompt_hash, "input_hash": request.input_hash,
        "code_hash": code_hash, "base_commit": commit,
        "prompt_chars": request.prompt_chars, "estimated_input_tokens": request.estimated_input_tokens,
        "messages": list(request.messages), "production_publish": False, "execute": args.execute,
    })
    if not args.execute:
        print(json.dumps({"status": "PREPARED_NO_MODEL_CALL", "prompt_hash": request.prompt_hash,
                          "input_hash": request.input_hash}))
        return 0
    audit = pipeline._run_stage(**stage_args, run_id=out.name)
    atomic_write_json(out / "result.json", audit.as_dict())
    print(json.dumps({"status": audit.status, "reason_codes": audit.reason_codes, "attempts": audit.attempts}))
    return 0 if audit.status == "VALIDATED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
