"""Compare historical+recent composites with stored complete full queries.

Read-only production cache access; no collection, model calls, notifications
or business database writes. This is an ex-post consistency comparison, not
proof that the later recent query was available at an earlier decision time.
"""
import argparse
import json
from pathlib import Path
import subprocess

from liangjian_funnel.reporting import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # Send only this audited pure helper, so acceptance can precede installing
    # the candidate module. All production SQL handles are explicitly mode=ro.
    helper = (Path(__file__).resolve().parents[1] / "src/liangjian_funnel/data/disclosure_incremental.py").read_text(encoding="utf-8")
    helper = helper.replace("from .cninfo import", "from liangjian_funnel.data.cninfo import").replace(
        "from ..pipeline.local_fact_cache import", "from liangjian_funnel.pipeline.local_fact_cache import")
    probe = '''
import sqlite3,json,pathlib,collections
from liangjian_funnel.settings import Settings
from zoneinfo import ZoneInfo
s=Settings.from_env(root=pathlib.Path.cwd())
con=sqlite3.connect(s.fact_cache_db_path.resolve().as_uri()+"?mode=ro",uri=True)
now=datetime.now(ZoneInfo("Asia/Shanghai"))
queries=collections.defaultdict(dict)
invalid_hash=0
for key,digest,raw in con.execute("SELECT cache_key,content_hash,payload_json FROM cached_results WHERE namespace='CNINFO_ANNOUNCEMENTS' ORDER BY fetched_at DESC,content_hash DESC"):
    symbol,semantic=key.split(":",1)
    if semantic not in {"RECENT_10D","ANNUAL_REPORT_450D"}:continue
    value=json.loads(raw)
    if canonical_json_hash(value)!=digest:
        invalid_hash+=1;continue
    result=CninfoFetchResult.model_validate(value)
    if not result.ok or not result.complete or result.fetched_at>now:continue
    role="recent" if semantic=="RECENT_10D" and result.end_date==DAY else "oracle" if semantic=="ANNUAL_REPORT_450D" and result.end_date==DAY else "base" if semantic=="ANNUAL_REPORT_450D" and result.end_date<DAY else None
    if role and role not in queries[symbol]:queries[symbol][role]=result
con.close()
stats=collections.Counter();differences=[]
for symbol,values in queries.items():
    if not {"base","recent","oracle"}<=values.keys():stats["no_complete_triplet"]+=1;continue
    stats["triplets"]+=1
    oracle=values["oracle"]
    composite=compose_disclosure_delta(values["base"],values["recent"],symbol=symbol,
        start=oracle.start_date,end=oracle.end_date,keyword="年度报告",now=now,max_age=timedelta(days=45),base_keyword="年度报告")
    if composite is None:stats["strictly_declined"]+=1;continue
    stats["composed"]+=1
    observed={a.announcement_id:a.content_hash for a in composite.announcements}
    expected={a.announcement_id:a.content_hash for a in oracle.announcements}
    if observed==expected:stats["exact_document_set_and_content_match"]+=1
    elif all(observed.get(k)==v for k,v in expected.items()):
        stats["complete_superset_with_unfiltered_recent"]+=1
        stats["additional_recent_documents"]+=len(set(observed)-set(expected))
    else: differences.append({"symbol":symbol,"missing":sorted(set(expected)-set(observed)),
        "extra":sorted(set(observed)-set(expected)),"revised":[k for k in observed.keys() & expected.keys() if observed[k]!=expected[k]]})
print(json.dumps({"day":DAY,"type":"EX_POST_CACHED_QUERY_COMPARISON","counts":dict(stats),"invalid_cache_hash":invalid_hash,
    "differences":differences,"no_network_collection":True,"no_business_writes":True},ensure_ascii=False))
'''
    remote = helper + "\nDAY=" + repr(args.day) + "\n" + probe
    result = subprocess.run(["ssh", "aurum-vm", "cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -"],
                            input=remote, text=True, capture_output=True, encoding="utf-8", timeout=60)
    if result.returncode:
        raise RuntimeError(f"READONLY_AUDIT_FAILED:{result.returncode}:{result.stderr[-1000:]}")
    report = json.loads(result.stdout)
    atomic_write_json(args.output, report)
    print(json.dumps({**report,"differences":report["differences"][:5],"difference_count":len(report["differences"])}, ensure_ascii=False))
    return 1 if report["differences"] or report["invalid_cache_hash"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
