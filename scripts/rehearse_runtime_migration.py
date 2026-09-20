#!/usr/bin/env python3
"""Rehearse RuntimeStore migration on an isolated SQLite backup."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from liangjian_funnel.runtime.iteration_acceptance import (  # noqa: E402
    AcceptanceContractError,
    rehearse_runtime_store_migration,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--workdir", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = rehearse_runtime_store_migration(args.source, args.workdir)
    except AcceptanceContractError as exc:
        print(exc.reason_code, file=sys.stderr)
        return 4
    output = args.workdir.resolve() / "migration-report.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(output)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
