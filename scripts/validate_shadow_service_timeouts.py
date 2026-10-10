"""Read-only guard for effective systemd deadlines; run before unit installation."""
from __future__ import annotations

import argparse
import configparser
import json
import math
from pathlib import Path
import re
import sys


def finite_seconds(value: str) -> float:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*(s|sec|min|h|d)?", value.strip())
    if not match:
        raise ValueError("SERVICE_TIMEOUT_MUST_BE_FINITE_POSITIVE")
    seconds = float(match[1]) * {None: 1, "s": 1, "sec": 1, "min": 60,
                               "h": 3600, "d": 86400}[match[2]]
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("SERVICE_TIMEOUT_MUST_BE_FINITE_POSITIVE")
    return seconds


def validate_service(text: str) -> dict:
    # No interpolation of systemd's % specifiers / shell ${...} variables.
    # Duplicate directives are rejected rather than guessing reset semantics.
    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    parser.read_string(text)
    if "Service" not in parser:
        raise ValueError("SERVICE_SECTION_REQUIRED")
    service = parser["Service"]
    kind = service.get("Type", "simple").strip()
    if kind == "oneshot":
        if "RuntimeMaxSec" in service:
            raise ValueError("ONESHOT_RUNTIME_MAX_INEFFECTIVE")
        field = "TimeoutStartSec"
    elif kind in {"simple", "exec"}:
        field = "RuntimeMaxSec"
    else:
        raise ValueError("SERVICE_TYPE_NOT_VALIDATED")
    if field not in service:
        raise ValueError(f"SERVICE_EFFECTIVE_TIMEOUT_REQUIRED:{field}")
    return {"type": kind, "effective_field": field,
            "timeout_seconds": finite_seconds(service[field])}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("units", nargs="+", type=Path)
    args = parser.parse_args(argv)
    results = []
    for path in args.units:
        try:
            result = validate_service(path.read_text(encoding="utf-8"))
            results.append({"path": str(path), "status": "VALID", **result})
        except (ValueError, OSError, configparser.Error) as exc:
            results.append({"path": str(path), "status": "INVALID", "error": str(exc)})
    print(json.dumps({"schema_version": "shadow-service-timeout/1", "units": results},
                     ensure_ascii=False))
    return 0 if all(row["status"] == "VALID" for row in results) else 2


if __name__ == "__main__":
    sys.exit(main())
