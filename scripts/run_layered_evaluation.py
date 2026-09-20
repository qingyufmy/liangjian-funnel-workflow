#!/usr/bin/env python3
"""Build an isolated S10 layered report from a read-only export manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from liangjian_funnel.evaluation.layered_evaluation import (  # noqa: E402
    EvaluationContractError,
    build_layered_evaluation,
)


PROTECTED_ROOTS = tuple((REPO_ROOT / name).resolve() for name in ("state", "storage", "outputs", "cache", "config"))


def _safe_output(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if resolved == REPO_ROOT.resolve() or any(resolved == root or root in resolved.parents for root in PROTECTED_ROOTS):
        raise PermissionError("EVALUATION_OUTPUT_PROTECTED")
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path, help="Read-only JSON evaluation export")
    parser.add_argument("--output-dir", required=True, type=Path, help="Isolated report directory")
    parser.add_argument(
        "--preregistration",
        type=Path,
        default=REPO_ROOT / "config" / "evaluation_experiments.yaml",
    )
    args = parser.parse_args()
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        prereg = yaml.safe_load(args.preregistration.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or not isinstance(prereg, dict):
            raise ValueError("mapping required")
        if manifest.get("random_seed") != prereg.get("random_seed") or manifest.get("split") != prereg.get("split"):
            raise EvaluationContractError("PREREGISTRATION_MISMATCH")
        output = _safe_output(args.output_dir)
        output.mkdir(parents=True, exist_ok=True)
        report = build_layered_evaluation(
            manifest,
            minimum_sample_size=int(prereg.get("minimum_labeled_samples", 30)),
        )
        report["preregistration"] = {
            "path": str(args.preregistration.resolve()),
            "schema_version": prereg.get("schema_version"),
        }
        (output / "layered-evaluation.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    except FileNotFoundError as exc:
        print(f"MISSING_EVIDENCE: {exc}", file=sys.stderr)
        return 3
    except PermissionError as exc:
        print(str(exc), file=sys.stderr)
        return 4
    except (EvaluationContractError, ValueError, json.JSONDecodeError, yaml.YAMLError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(output / "layered-evaluation.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
