#!/usr/bin/env python3
"""Create a hashed evidence manifest from explicitly named local read-only files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from liangjian_funnel.runtime.iteration_acceptance import (  # noqa: E402
    AcceptanceContractError,
    build_evidence_package,
)


def _file(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("--file must be CATEGORY=PATH")
    category, raw = value.split("=", 1)
    if not category.strip() or not raw.strip():
        raise argparse.ArgumentTypeError("--file must be CATEGORY=PATH")
    return category.strip(), Path(raw.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--file", action="append", default=[], type=_file)
    parser.add_argument("--code-version", required=True)
    parser.add_argument("--config-version", required=True)
    parser.add_argument("--rules-version", required=True)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--prompt-version", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--generated-by", default="READ_ONLY_EXPLICIT_EXPORT")
    parser.add_argument("--test-only", action="store_true")
    parser.add_argument("--redaction", action="append", default=[])
    args = parser.parse_args()
    try:
        manifest = build_evidence_package(
            root=args.root,
            files=dict(args.file),
            versions={
                "code": args.code_version,
                "config": args.config_version,
                "rules": args.rules_version,
                "model": args.model_version,
                "prompt": args.prompt_version,
            },
            time_range={"start": args.start, "end": args.end},
            generated_by=args.generated_by,
            test_only=args.test_only,
            redactions=args.redaction,
        )
    except AcceptanceContractError as exc:
        print(exc.reason_code, file=sys.stderr)
        return 1
    print(json.dumps({
        "manifest": str(args.root.resolve() / "manifest.json"),
        "complete": manifest["complete"],
        "missing_categories": manifest["missing_categories"],
        "test_only": manifest["test_only"],
    }, ensure_ascii=False))
    return 0 if manifest["complete"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
