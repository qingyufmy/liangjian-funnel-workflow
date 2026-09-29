"""Read two free news sources once, freeze local evidence, never call models."""
import argparse
from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.open_news import OpenNewsClient, OpenNewsFetchResult
from liangjian_funnel.facts.contracts import FactSnapshotManifest
from liangjian_funnel.data.rotation_theme import load_rotation_theme_config
from liangjian_funnel.data.news_journal import NewsJournal
from liangjian_funnel.facts.open_news import normalize_open_news_results
from liangjian_funnel.facts.hithink import manifest_projection
from liangjian_funnel.pipeline.market_aggregates import build_news_heat_snapshot
from liangjian_funnel.pipeline.a2_news_context import build_a2_news_shadow
from liangjian_funnel.pipeline.a2_news_review import enrich_news_shadow, build_review_packet


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--frozen-dir", type=Path, help="Replay prior probe without network or new timestamps")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    if args.frozen_dir:
        results = [OpenNewsFetchResult.model_validate(r) for r in json.loads(
            (args.frozen_dir / "source-results.json").read_text(encoding="utf-8"))]
        manifest = FactSnapshotManifest.model_validate_json(
            (args.frozen_dir / "fact-manifest.json").read_text(encoding="utf-8"))
        now = manifest.as_of
    else:
        results = []
        with OpenNewsClient(timeout_seconds=10, max_retries=1) as client:
            results.append(client.fetch_cls_roll(page_size=20))
            results.append(client.fetch_eastmoney_7x24(page_size=20))
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        manifest = normalize_open_news_results(results, as_of=now, ingest_time=now)
    heat = build_news_heat_snapshot(manifest_projection(manifest), (), as_of=now)
    journal = NewsJournal(args.output_dir / "news-shadow.sqlite3")
    page_receipts = [journal.record_page(r, received_at=now) for r in results]
    report = enrich_news_shadow(build_a2_news_shadow(heat, (), as_of=now,
        snapshot_hash=manifest.manifest_hash), load_rotation_theme_config())
    outputs = {"source-results.json": [r.model_dump(mode="json") for r in results],
               "fact-manifest.json": manifest.model_dump(mode="json"),
               "shadow.json": report, "review-input.json": build_review_packet(report),
               "page-coverage.json": {"pages": page_receipts, "complete": False,
                                      "reason": "LIVE_CURSOR_CONTRACT_NOT_VERIFIED"}}
    for name, payload in outputs.items():
        with (args.output_dir / name).open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
    print(json.dumps({"as_of": now.isoformat(), "sources": [{"source": r.source_id,
        "ok": r.ok, "reason": r.reason_code, "count": len(r.items)} for r in results],
        "facts": len(manifest.facts), "eligible_events": report["event_count"],
        "excluded": report["excluded_counts"],
        "theme_mentions": sum(bool(e["theme_clues"]) for e in report["events"])}, ensure_ascii=False))


if __name__ == "__main__":
    main()
