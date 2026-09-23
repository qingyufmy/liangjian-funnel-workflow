"""Explicit local research probe; never called by production schedules.

PYTHONPATH=src python scripts/probe_free_daily_package.py --date 2026-09-22
"""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx

from liangjian_funnel.data.tdx_daily_package import decode_daily_package
from liangjian_funnel.facts.store import FactStore, _atomic_write_bytes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--symbols", default="600519.SH,000001.SZ,603186.SH")
    args = parser.parse_args()
    # Fixed isolated output location; no CLI access to runtime DB or production outputs.
    root = Path(__file__).resolve().parents[1] / "artifacts" / "source_audit"
    store = FactStore(root)
    day = datetime.strptime(args.date, "%Y-%m-%d").date()
    url = f"https://www.tdx.com.cn/products/data/data/g4day/{day:%Y%m%d}.zip"
    received = bytearray()
    with httpx.Client(timeout=20, follow_redirects=False) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            for chunk in response.iter_bytes():
                received.extend(chunk)
                if len(received) > 16 * 1024 * 1024:
                    raise ValueError("response exceeds archive size limit")
    raw = bytes(received)
    digest = hashlib.sha256(raw).hexdigest()
    archive_path = root / f"{digest}.zip"
    if not archive_path.exists():
        _atomic_write_bytes(archive_path, raw)
    result = decode_daily_package(raw, args.date, equity_symbols=args.symbols.split(","))
    result.update(source_url=url, fetched_at=datetime.now(timezone.utc).isoformat(),
                  raw_archive_path=str(archive_path), compressed_bytes=len(raw))
    output = store.write_json(root / f"tdx-{day}-{digest[:12]}.json", result)
    print(json.dumps(dict(output=str(output), **result), ensure_ascii=False))


if __name__ == "__main__":
    main()
