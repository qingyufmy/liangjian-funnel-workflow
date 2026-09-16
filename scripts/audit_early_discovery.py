"""Read-only discovery replay. Later revisions require an explicit opt-in."""
import argparse
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from liangjian_funnel.pipeline.early_discovery import discover_early_setups
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache


class ReadOnlyCache(LocalFactCache):
    def __init__(self, path):
        self.path = Path(path)

    def _connect(self):
        connection = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro")
        connection.row_factory = sqlite3.Row
        return connection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--dates", nargs="+", required=True)
    parser.add_argument("--shape-reconstruction", action="store_true",
                        help="Use later retained revisions; not proof of historical availability")
    args = parser.parse_args()
    cache = ReadOnlyCache(args.cache)
    reports = []
    for day in args.dates:
        cutoff = datetime.fromisoformat(day + "T15:00:00+08:00")
        records = cache.query_daily_bars(args.symbol, adjust="none", end=cutoff + timedelta(seconds=1),
                                        as_of=None if args.shape_reconstruction else cutoff)
        bars = [{**record["payload"], "timestamp": record["timestamp"], "adjust": record["adjust"]}
                for record in records]
        result = discover_early_setups({args.symbol: bars}, as_of=cutoff, symbols=[args.symbol])
        reports.append({"trade_date": day, "result": result,
                        "latest_source_received_at": max((row["fetched_at"] for row in records), default=None)})
    print(json.dumps({"mode": "LATER_REVISION_SHAPE_ONLY" if args.shape_reconstruction else "POINT_IN_TIME_CACHE",
                      "cache_read_only": True, "publishes_signals": False, "reports": reports}, ensure_ascii=False))


if __name__ == "__main__":
    main()
