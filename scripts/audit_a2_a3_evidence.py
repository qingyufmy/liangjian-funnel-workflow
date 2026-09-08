"""Read-only production capture for an explicit research batch.

Only writes a new diagnostic JSON artifact; never runs research, calls a
model, publishes plans or opens a database for writing.
"""
import argparse
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from liangjian_funnel.pipeline.local_fact_cache import _timestamp
from liangjian_funnel.settings import Settings


def capture(root: Path, run_id: str) -> dict:
    settings = Settings.from_env(root=root)
    research_path = root / "outputs" / "research" / f"research_{run_id}_lane_1.json"
    research = json.loads(research_path.read_text(encoding="utf-8"))
    snapshot_id = research["stages"][0]["snapshot_id"]
    snapshot = json.loads((settings.snapshot_dir / f"{snapshot_id}.json").read_text(encoding="utf-8"))
    cutoff = datetime.fromisoformat(snapshot["as_of"])
    with sqlite3.connect(settings.feature_store_db_path.as_uri() + "?mode=ro", uri=True) as db:
        decisions = [json.loads(row[0]) for row in db.execute(
            "SELECT payload_json FROM deterministic_stage_decisions WHERE run_id=? AND lane_id=? ORDER BY stage,symbol",
            (run_id, "lane_1"))]
    symbols = sorted({row["symbol"] for row in decisions if row["stage"] == "A3_LOCAL_TECHNICAL"})
    daily = {}
    with sqlite3.connect(settings.fact_cache_db_path.as_uri() + "?mode=ro", uri=True) as db:
        for symbol in symbols:
            rows = db.execute(
                "WITH revisions AS (SELECT bar_timestamp,fetched_at,payload_json,"
                "ROW_NUMBER() OVER (PARTITION BY symbol,bar_timestamp,adjust ORDER BY fetched_at DESC,content_hash DESC) AS n "
                "FROM daily_bars WHERE symbol=? AND adjust='none' AND bar_timestamp<? AND fetched_at<=?) "
                "SELECT payload_json FROM revisions WHERE n=1 ORDER BY bar_timestamp DESC LIMIT 800",
                (symbol, _timestamp(cutoff + timedelta(days=1)), _timestamp(cutoff))).fetchall()
            daily[symbol] = [json.loads(row[0]) for row in reversed(rows)]
    return {"run_id": run_id, "snapshot_id": snapshot_id, "as_of": cutoff.isoformat(),
            "daily_data_scope": "BAR_AND_KNOWLEDGE_CUTOFF_NO_LATER_REVISIONS",
            "production_updated": False, "decisions": decisions, "daily_bars": daily}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # Research identifiers cannot select an unrelated path.
    if not args.run_id or any(c not in "0123456789abcdefghijklmnopqrstuvwxyz-_" for c in args.run_id):
        parser.error("invalid research run id")
    if args.output.exists():
        parser.error("refusing to overwrite existing evidence")
    result = capture(args.root.resolve(), args.run_id)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False)
    print(json.dumps({"output": str(args.output), "decisions": len(result["decisions"]),
                      "daily_symbols": len(result["daily_bars"]), "production_updated": False}))
