"""Execute this export only, using the project's owned local-test wrapper."""
from pathlib import Path
import argparse
import hashlib
import importlib.metadata
import json
import os
import sys
import tempfile

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from bootstrap import enable

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--junit", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((ROOT / "MANIFEST.json").read_text(encoding="utf-8"))
    pins = {r["export_path"]: r["sha256"] for r in manifest["files"]}
    def verify():
        for name, sha in pins.items():
            if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != sha:
                raise ValueError("DK_EXPORT_SNAPSHOT_CHANGED: " + name)
    verify()
    network_attempts = []
    def deny_network(event, values):
        if event in {"socket.connect", "socket.connect_ex", "socket.getaddrinfo", "socket.bind"}:
            network_attempts.append(event)
            raise RuntimeError("DK_EXPORT_ZERO_NETWORK: " + event)
    sys.addaudithook(deny_network)
    enable()
    import pytest
    tests = sorted(str(p) for p in (ROOT / "snapshot" / "tests").glob("test_*.py"))
    tests.append(str(ROOT / "checks" / "test_export_example.py"))
    with tempfile.TemporaryDirectory(prefix="dk-audit-") as owned_temp:
        temp = Path(owned_temp)
        command = ["-q", "--noconftest", "--import-mode=importlib", "--confcutdir=" + str(ROOT),
            "--rootdir=" + str(ROOT), "--basetemp=" + str(temp / "dk-pytest"),
            "-o", "cache_dir=" + str(temp / "dk-pytest-cache"), "--junitxml=" + str(args.junit), *tests]
        code = int(pytest.main(command))
    verify()
    result = dict(schema="dk-export-offline-validation/1", exit_code=code,
        snapshot_commit=manifest["source_commit"], snapshot_pins_verified=len(pins),
        network_attempt_count=len(network_attempts), network_attempts=network_attempts,
        production_writes=0, vm_calls=0, model_calls=0,
        evidence_class="CONTROLLED_TESTS_NOT_REAL_TRADES_OR_20_DAY_MARKET_REPLAY",
        versions={n:importlib.metadata.version(n) for n in ("pytest", "pydantic", "PyYAML", "exchange-calendars", "tzdata")})
    args.result.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return code if code else (1 if network_attempts else 0)

if __name__ == "__main__":
    raise SystemExit(main())
