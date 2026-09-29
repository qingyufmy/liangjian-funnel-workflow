"""Offline, no-provider memory probe. Never modifies the input snapshot."""
from __future__ import annotations

import argparse
import hashlib
import json
import time
import tracemalloc
from pathlib import Path

from liangjian_funnel.pipeline.research import _jsonable, _sha256_json


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--mode", choices=("legacy", "optimized"), required=True)
    args = parser.parse_args()
    with args.snapshot.open(encoding="utf-8") as stream:
        snapshot = json.load(stream)
    data = snapshot.get("data", snapshot)
    tracemalloc.start()
    started = time.perf_counter()
    if args.mode == "legacy":
        digest = hashlib.sha256(json.dumps(_jsonable(data), ensure_ascii=False,
            sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    else:
        digest = _sha256_json(data)
    elapsed = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(json.dumps({"mode": args.mode, "input_bytes": args.snapshot.stat().st_size,
        "sha256": digest, "peak_extra_python_bytes": peak, "seconds": round(elapsed, 3),
        "scope": "SERIALIZATION_ONLY_NOT_FULL_A1_RSS", "model_calls": 0}, ensure_ascii=False))


if __name__ == "__main__":
    main()
