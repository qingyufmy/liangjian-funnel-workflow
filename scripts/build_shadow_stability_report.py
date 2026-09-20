#!/usr/bin/env python3
"""Build a deterministic operations report from an explicit shadow JSON file."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from liangjian_funnel.runtime.iteration_acceptance import (  # noqa: E402
    AcceptanceContractError,
    build_shadow_stability_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        evidence = json.loads(args.input.read_text(encoding="utf-8"))
        if not isinstance(evidence, dict):
            raise AcceptanceContractError("SHADOW_EVIDENCE_INVALID")
        report = build_shadow_stability_report(evidence)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except (json.JSONDecodeError, AcceptanceContractError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(args.output.resolve())
    if report["status"] == "OPERATIONS_ACCEPTED":
        return 0
    evidence_gaps = {"MINIMUM_5_SESSIONS_NOT_MET", "TEST_ONLY_EVIDENCE", "FAULT_INJECTION_INCOMPLETE"}
    return 3 if set(report["reason_codes"]).issubset(evidence_gaps) else 1


if __name__ == "__main__":
    raise SystemExit(main())
