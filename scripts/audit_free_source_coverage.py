"""Read-only VM snapshot export + local archive audit. No production writes."""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from liangjian_funnel.data.tdx_daily_package import decode_daily_package
from liangjian_funnel.facts.store import FactStore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if not all(c.isalnum() or c in "-_" for c in args.run_id):
        raise ValueError("invalid run id")
    remote = """
import json,pathlib,hashlib
root=pathlib.Path('/www/wwwroot/Agu/liangjian-funnel-workflow')
run=json.loads((root/'outputs/runs'/('%s.json')).read_text())
raw=pathlib.Path(run['snapshot']['path']).read_bytes()
snapshot=json.loads(raw)
data=snapshot['data']
print(json.dumps({'run_id':run['run_id'],'trade_date':run['market_trade_date'],
 'snapshot_id':snapshot['snapshot_id'],'file_sha256':hashlib.sha256(raw).hexdigest(),
 'universe':data['universe_candidates']},ensure_ascii=True))
""" % args.run_id
    result = subprocess.run(["ssh", "aurum-vm", "/www/wwwroot/Agu/liangjian-funnel-workflow/.venv/bin/python -"],
                            input=remote, text=True, capture_output=True, timeout=45, check=True)
    manifest = json.loads(result.stdout)
    root = Path(__file__).resolve().parents[1] / "artifacts/source_audit"
    store = FactStore(root)
    store.write_json(root / f"universe-{args.run_id}.json", manifest)
    symbols = [x["symbol"] for x in manifest["universe"]]
    if len(set(symbols)) != len(symbols):
        raise ValueError("snapshot universe duplicates")
    # Reuse frozen archive, never select a new package or silently refetch.
    index = list(root.glob(f"tdx-{manifest['trade_date']}-*.json"))
    if len(index) != 1:
        raise ValueError("explicit unique archive manifest required")
    saved = store.read_json(index[0])
    raw = Path(saved["raw_archive_path"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != saved["archive_sha256"]:
        raise ValueError("archive hash mismatch")
    decoded = decode_daily_package(raw, manifest["trade_date"], equity_symbols=symbols)
    report = dict(run_id=args.run_id, snapshot_sha256=manifest["file_sha256"], **decoded)
    store.write_json(root / f"coverage-{args.run_id}.json", report)
    print(json.dumps({k:v for k,v in report.items() if k != "rows"},ensure_ascii=False))


if __name__ == "__main__":
    main()
