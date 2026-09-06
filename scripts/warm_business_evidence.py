"""Prewarm versioned filing evidence; never publish or mutate frozen snapshots."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.cninfo import CninfoAnnouncement
from liangjian_funnel.data.cninfo_pdf import CninfoPdfClient
from liangjian_funnel.facts.cninfo import _pdf_payload, compact_cninfo_pdf_evidence
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication, _main_business_evidence
from liangjian_funnel.pipeline.deterministic import _membership_map


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", action="append", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--scope", choices=("all", "financial"), default="all")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--workers", type=int, default=4, choices=range(1, 5))
    args = parser.parse_args()
    settings = Settings.from_env(root=Path.cwd())
    app = object.__new__(WorkflowApplication)
    app.settings = settings
    app.fact_cache = LocalFactCache(settings.fact_cache_db_path)
    tasks = {}
    for filename in args.snapshot:
        source = json.loads(Path(filename).read_text(encoding="utf-8"))
        data = source["data"]
        industries = _membership_map(data.get("THS_INDUSTRY_MEMBERSHIP"), taxonomy="INDUSTRY")
        for symbol, rows in data.get("DISCLOSURE_EVENTS", {}).get("by_symbol", {}).items():
            if args.scope == "financial" and not any(
                row["taxonomy_name"] in {"银行", "保险", "证券", "多元金融"} for row in industries.get(symbol, ())
            ):
                continue
            for row in rows:
                if row.get("pdf_downloaded") is not True:
                    continue
                ann = CninfoAnnouncement(
                    announcement_id=row["announcement_id"], sec_code=symbol[:6],
                    sec_name=row["sec_name"], announcement_title=row["announcement_title"],
                    adjunct_url=row["source_url"], publish_time=row["publish_time"],
                    storage_time=row.get("storage_time"),
                )
                tasks.setdefault(ann.announcement_id, (symbol, ann, row))
    selected = list(tasks.values())[:args.limit]
    output = Path(args.output)
    report = {"schema_version": "business-evidence-warmup/1.0.0", "snapshots": args.snapshot,
              "total_documents": len(tasks), "selected_documents": len(selected), "rows": [],
              "fact_cache_path": str(settings.fact_cache_db_path),
              "production_plans_changed": False}
    def warm(task):
        symbol, ann, row = task
        cached = app.fact_cache.get_cached_result("CNINFO_PDF_EVIDENCE", ann.announcement_id,
                                                  fresh_at=datetime.now(ZoneInfo("Asia/Shanghai")))
        evidence = app._cached_cninfo_pdf_evidence_from_record(ann, cached)
        hit = evidence is not None
        if evidence is None:
            with CninfoPdfClient(settings.cninfo_pdf_cache_dir, timeout_seconds=20, max_attempts=2) as client:
                evidence = client.fetch_evidence(ann)
            # Preserve prior usable records when a refresh fails. A retryable
            # network failure must not hide the old successful evidence.
            if evidence.available:
                app._persist_cninfo_pdf_evidence(evidence)
        projected = {**row, **_pdf_payload(compact_cninfo_pdf_evidence(evidence))}
        business = _main_business_evidence({"by_symbol": {symbol: [projected]}}, [symbol])[symbol]
        return {"symbol": symbol, "announcement_id": ann.announcement_id, "cache_hit": hit,
                "available": evidence.available, "reason_code": evidence.reason_code,
                "extraction_version": evidence.extraction_version,
                "pdf_sha256": evidence.pdf_sha256, "fetched_at": evidence.fetched_at.isoformat(),
                "business_available": business["available"], "business_evidence": business["evidence"]}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        jobs = {executor.submit(warm, task): task for task in selected}
        for future in as_completed(jobs):
            try:
                row = future.result()
            except Exception as exc:
                symbol, ann, _ = jobs[future]
                row = {"symbol": symbol, "announcement_id": ann.announcement_id,
                       "available": False, "business_available": False, "reason_code": type(exc).__name__}
            report["rows"].append(row)
            report["completed"] = len(report["rows"])
            report["available"] = sum(r["available"] for r in report["rows"])
            report["business_available"] = sum(r["business_available"] for r in report["rows"])
            report["updated_at"] = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
            atomic_write_json(output, report)
            print(json.dumps({k: report[k] for k in ("completed", "selected_documents", "available", "business_available")}), flush=True)
    return 0 if report["available"] == len(selected) else 1


if __name__ == "__main__":
    raise SystemExit(main())
