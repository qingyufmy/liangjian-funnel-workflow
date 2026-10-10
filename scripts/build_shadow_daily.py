"""Build W3 from explicit shadow/census/outer-baseline files; no production writes."""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))

from liangjian_funnel.runtime.shadow_evidence import read_shadow_evidence
from liangjian_funnel.evaluation.ablation.shadow_day_adapter import build_shadow_day, render_shadow_day


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--mirror", type=Path)
    parser.add_argument("--census", type=Path, required=True)
    parser.add_argument("--production", type=Path, required=True)
    parser.add_argument("--prices", type=Path, help="Optional hash-bound full-validity price archive")
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    source = [p.resolve() for p in (args.ledger,args.census,args.production,
        *([args.mirror] if args.mirror else []),*([args.prices] if args.prices else []))]
    target = args.output.resolve()
    if any(target==p or p.is_relative_to(target) for p in source) or target.exists():
        parser.error("NEW_INDEPENDENT_OUTPUT_REQUIRED")
    now = datetime.fromisoformat(args.as_of)
    if now.tzinfo is None:
        parser.error("AWARE_AS_OF_REQUIRED")
    view = read_shadow_evidence(args.ledger,args.mirror)
    census = json.loads(args.census.read_bytes())
    production = json.loads(args.production.read_bytes())
    prices = json.loads(args.prices.read_bytes()) if args.prices else None
    report = build_shadow_day(view,census,production,as_of=args.as_of,price_archive=prices)
    # Unique new directory, never overwrite immutable prior daily inputs/report.
    target.mkdir(parents=True,exist_ok=False)
    files = {"report.json":json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False).encode(),
        "SHADOW_DAILY_"+report["trade_date"]+".md":render_shadow_day(report).encode(),
        "production-equivalence.json":json.dumps(report["production_equivalence"],ensure_ascii=False,indent=2).encode()}
    for filename,body in files.items():
        with (target/filename).open("xb") as handle: handle.write(body)
    manifest = {"schema":"shadow-daily-files/1","source_kind":"REALTIME_SHADOW",
        "source_ledger_view_sha256":view["snapshot_canonical_sha256"],
        "input_files":{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            (args.census,args.production,*([args.prices] if args.prices else []))},
        "output_files":{name:hashlib.sha256(body).hexdigest() for name,body in files.items()},
        "status":report["status"],"model_calls":0,"provider_calls":0,"notifications":0,
        "production_mutation":False,"account_pnl":None}
    with (target/"manifest.json").open("x",encoding="utf-8") as handle:
        json.dump(manifest,handle,ensure_ascii=False,indent=2)
    print(json.dumps({"status":report["status"],"output":str(target),"notifications":0}))
    return 0 if report["status"]=="COMPLETE" else 2


if __name__=="__main__": raise SystemExit(main())
