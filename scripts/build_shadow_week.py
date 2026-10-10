"""Explicit five-session shadow files only. No providers or notifications."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from liangjian_funnel.evaluation.ablation.strategy_accumulation import canonical_sha256
from liangjian_funnel.evaluation.ablation.shadow_week import build_shadow_week,render_shadow_week


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report",action="append",type=Path,required=True)
    parser.add_argument("--session",action="append",required=True)
    parser.add_argument("--as-of",required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args(argv)
    target=args.output.resolve()
    if target.exists() or any(p.resolve().is_relative_to(target) for p in args.report):
        parser.error("NEW_INDEPENDENT_OUTPUT_REQUIRED")
    inputs=[];source_hashes={}
    for path in args.report:
        raw=path.read_bytes();report=json.loads(raw)
        source_hashes[str(path.resolve())]=hashlib.sha256(raw).hexdigest()
        inputs.append({"report":report,"report_sha256":canonical_sha256(report)})
    report=build_shadow_week(inputs,trading_days=args.session,as_of=args.as_of)
    target.mkdir(parents=True,exist_ok=False)
    bodies={"report.json":json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False).encode(),
        "SHADOW_WEEK_"+args.session[-1]+".md":render_shadow_week(report).encode()}
    for name,body in bodies.items():
        with (target/name).open("xb") as handle:handle.write(body)
    manifest={"schema":"shadow-week-files/1","input_files":source_hashes,
        "output_files":{name:hashlib.sha256(body).hexdigest() for name,body in bodies.items()},
        "provider_calls":0,"model_calls":0,"notifications":0,"production_mutation":False,
        "status":report["status"]}
    with (target/"manifest.json").open("x",encoding="utf-8") as handle:
        json.dump(manifest,handle,ensure_ascii=False,indent=2)
    print(json.dumps({"status":report["status"],"output":str(target)}))
    return 0 if report["status"]=="COMPLETE" else 2


if __name__=="__main__":raise SystemExit(main())
