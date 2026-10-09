"""Explicit-file-only shadow preflight. Exit 2 for implementation partial.

No Settings/RuntimeStore/network/model/scheduler. Refuse output overwrite.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from liangjian_funnel.runtime.auction_preparation import (
    PreparationError, canonical, digest, fast_preflight, local_path,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--request", required=True, help="Relative JSON path within input-root")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.output.exists():
            raise PreparationError("REFUSE_OVERWRITE")
        request_path = local_path(args.input_root, args.request)
        request_bytes = request_path.read_bytes()
        request = json.loads(request_bytes)
        if not isinstance(request, dict):
            raise PreparationError("LOCAL_INPUT_INVALID")
        slow_path = local_path(args.input_root, request.pop("slow_bundle_path", None))
        allowed = {"expected_slow_sha256", "expected_scope", "now", "trade_calendar",
                   "plan_publication", "positions", "fast_inputs", "delta_results", "budget_seconds"}
        if set(request) != allowed:
            raise PreparationError("REQUEST_FIELDS_INVALID")
        report = fast_preflight(input_root=args.input_root, slow_bundle_path=slow_path, **request)
        report["request_file_sha256"] = digest(request_bytes)
        encoded = canonical(report) + b"\n"
        with args.output.open("xb") as stream:
            stream.write(encoded)
        print(json.dumps({"contract_status": report["contract_status"],
                          "implementation_status": report["implementation_status"],
                          "reason_code": report["reason_code"], "delta_count": report["delta_count"],
                          "elapsed_seconds": report["elapsed_seconds"], "artifact_sha256": digest(encoded)}))
        return 2
    except FileExistsError:
        reason = "REFUSE_OVERWRITE"
    except PreparationError as exc:
        reason = str(exc)
    except (OSError, ValueError, TypeError):
        reason = "LOCAL_INPUT_INVALID"
    print(json.dumps({"status": "BLOCKED", "reason_code": reason}))
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
