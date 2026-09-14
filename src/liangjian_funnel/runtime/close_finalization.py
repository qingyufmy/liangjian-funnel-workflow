"""Post-close stability evidence; never creates a decision or trading signal."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time
from hashlib import sha256
import json
from pathlib import Path
from copy import copy
from zoneinfo import ZoneInfo

from ..data.mootdx import detect_missing_bars


def collect_close_finalization(provider, minute_store, plans, output_dir: Path, *, now: datetime):
    now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    if now.time() < time(15, 2):
        return {"status": "NOT_DUE", "reason_code": "WAIT_POST_CLOSE_SETTLEMENT"}
    symbols = sorted({p["symbol"] for p in plans if str(p.get("expires_at") or "")[:10] == now.date().isoformat()})
    cutoff = now.replace(hour=15, minute=0, second=0, microsecond=0)
    source = copy(provider)
    if hasattr(source, "timeout_seconds"):
        source.timeout_seconds = min(source.timeout_seconds, 2.5)

    def fetch(symbol):
        records = []
        for interval, count in (("1m", 240), ("5m", 48)):
            attempts = []
            stable = False
            previous = None
            for _ in range(3):
                captured = datetime.now(now.tzinfo)
                try:
                    result = source.fetch_bars(symbol, interval, count, as_of=cutoff)
                    bars = tuple(result.bars)
                    valid = (result.complete and len(bars) == count and bars[-1].bar_end == cutoff
                             and all(b.symbol == symbol and b.interval == interval and b.bar_end.date() == now.date() for b in bars)
                             and not detect_missing_bars(bars, interval, as_of=cutoff))
                    digest = sha256(json.dumps([b.model_dump(mode="json") for b in bars], sort_keys=True).encode()).hexdigest() if valid else None
                    attempts.append({"captured_at": captured.isoformat(), "hash": digest, "reason": result.reason_code})
                    stable = bool(valid and digest == previous)
                    if valid:
                        # Version ledger only: no snapshot_id means no decision
                        # identity is created and prior selections stay frozen.
                        minute_store.write_live(bars, as_of=cutoff)
                    if stable:
                        break
                    previous = digest
                except Exception:
                    attempts.append({"captured_at": captured.isoformat(), "hash": None, "reason": "CLOSE_COLLECTION_FAILED"})
                    previous = None
            records.append({"symbol": symbol, "interval": interval, "status": "STABLE_OBSERVED" if stable else "UNCONFIRMED",
                            "provider_official_final": False, "attempts": attempts})
        return records

    with ThreadPoolExecutor(max_workers=min(8, len(symbols) or 1)) as pool:
        rows = [r for records in pool.map(fetch, symbols) for r in records]
    receipt = {"trade_date": now.date().isoformat(), "collected_at": datetime.now(now.tzinfo).isoformat(),
               "scope": "POST_CLOSE_ARCHIVE_ONLY", "creates_signals": False,
               "status": "COMPLETE" if rows and all(r["status"] == "STABLE_OBSERVED" for r in rows) else "DATA_LIMITED" if rows else "NO_SCOPE",
               "records": rows}
    root = output_dir / "close_finalization" / now.date().isoformat()
    root.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(receipt, ensure_ascii=False, indent=2)
    path = root / (sha256(raw.encode()).hexdigest() + ".json")
    path.write_text(raw, encoding="utf-8")
    return {**receipt, "receipt_path": str(path)}
