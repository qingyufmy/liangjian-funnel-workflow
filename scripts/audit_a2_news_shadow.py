"""Offline A2 news evidence audit. No network, model calls or production stores."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

from liangjian_funnel.pipeline.a2_news_context import build_a2_news_shadow
from liangjian_funnel.pipeline.a2_news_review import enrich_news_shadow, build_review_packet
from liangjian_funnel.data.rotation_theme import load_rotation_theme_config


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--decisions", type=Path, help="Optional JSON array of frozen A2 gate decisions")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--taxonomy", type=Path, help="Pinned taxonomy YAML for reproducible mapping")
    args = parser.parse_args()
    # Never overwrite frozen input or an earlier audit.
    if args.output.exists():
        parser.error("output already exists; choose a new audit path")
    with args.snapshot.open("rb") as stream:
        source_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    with args.snapshot.open(encoding="utf-8") as stream:
        frozen = json.load(stream)
    data = frozen.get("data", frozen)
    as_of = datetime.fromisoformat(frozen["as_of"].replace("Z", "+00:00"))
    decisions = json.loads(args.decisions.read_text(encoding="utf-8")) if args.decisions else []
    if not isinstance(decisions, list):
        parser.error("decisions must be a JSON array")
    report = build_a2_news_shadow(data.get("NEWS_HEAT_SNAPSHOT"), decisions,
                                as_of=as_of, snapshot_hash=source_hash)
    report = enrich_news_shadow(report, load_rotation_theme_config(args.taxonomy), data.get("MAIN_BUSINESS_EVIDENCE"))
    packet = build_review_packet(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump({"report": report, "review_input": packet}, stream, ensure_ascii=False, indent=2)
    print(json.dumps({key: report[key] for key in (
        "input_item_count", "event_count", "eligible_fact_count", "excluded_counts", "report_hash")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
