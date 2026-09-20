"""Build an offline-only supplemental source policy and comparison report."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from liangjian_funnel.data.supplemental_sidecar import (  # noqa: E402
    compare_shadow_records,
    load_supplemental_source_policies,
)


def _rows(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("comparison input must be a JSON array of objects")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Offline supplemental-source sidecar audit")
    parser.add_argument("--policy", default=str(ROOT / "config" / "supplemental_sources.yaml"))
    parser.add_argument("--primary")
    parser.add_argument("--shadow")
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("--as-of must be timezone-aware")
    policies = load_supplemental_source_policies(args.policy)
    output = {
        "schema_version": "supplemental-source-audit/1.0.0",
        "as_of": as_of.isoformat(),
        "policies": [
            {
                "source_id": item.source_id,
                "provider": item.provider,
                "capability": item.capability,
                "enabled": item.enabled,
                "shadow_only": item.shadow_only,
                "execution_authority": item.execution_authority,
                "adapter_status": item.adapter_status,
                "live_status": item.live_status,
                "license_status": item.license_status,
                "upstream_identity": item.upstream_identity,
                "applicable_paths": list(item.applicable_paths),
            }
            for item in policies.values()
        ],
        "comparison": compare_shadow_records(
            primary=_rows(args.primary),
            shadow=_rows(args.shadow),
            as_of=as_of,
        ),
        "production_changed": False,
        "network_used": False,
    }
    target = Path(args.output).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "ADAPTER_OFFLINE_TESTED", "output": str(target)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
