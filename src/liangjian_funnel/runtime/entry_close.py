"""EOD missing-entry reconciliation, never matching or historical repair.

Negative proof requires every original 239 monitor capture, valid original
hashes and every other eligible closed window. Missing captures/additional bar
holes/late-only bars are UNKNOWN, not evidence of an unfilled order. The final
15:00 market bar is outside the original monitor's 14:59 execution cutoff and
cannot be declared missing by this reader. No source query or candle synthesis.
"""
from contextlib import closing
from datetime import datetime, timedelta, time
from hashlib import sha256
import json
import sqlite3
import sys
import zlib
from zoneinfo import ZoneInfo

from ..data.mootdx import MinuteBar
from ..data.session_windows import closed_window_ends
from .entry_contract import entry_bar_for

TZ = ZoneInfo("Asia/Shanghai")


def missing_window_evidence(path, *, symbol: str, eligible: datetime, now: datetime) -> dict:
    current = now.astimezone(TZ)
    base = {"trade_date": current.date().isoformat(), "symbol": symbol,
        "eligible_bar_end": eligible.isoformat(), "observed_at": current.isoformat(),
        "status": "UNRESOLVED", "basis": "ORIGINAL_MONITOR_CAPTURE_LEDGER_ONLY"}
    if current.time() < time(15, 2) or eligible.date() != current.date():
        return base
    cutoff = current.replace(hour=14, minute=59, second=0, microsecond=0)
    expected_bars = set(closed_window_ends(cutoff, "1m"))
    if eligible not in expected_bars:
        return base
    captures = [current.replace(hour=9, minute=32, second=0, microsecond=0) + timedelta(minutes=i) for i in range(119)]
    captures += [current.replace(hour=13, minute=1, second=0, microsecond=0) + timedelta(minutes=i) for i in range(120)]
    observed, manifest = set(), []
    try:
        # One stable read transaction, streaming one bounded compressed packet at
        # a time. Never instantiate a writer or repair a missing cache.
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            if connection.execute("SELECT 1 FROM minute_bar_versions WHERE symbol=? AND interval='1m' "
                    "AND bar_end=? AND observed_at<=? LIMIT 1",
                    (symbol, eligible.isoformat(), current.isoformat())).fetchone():
                return base  # A later archived bar is not absence and cannot backfill a fill.
            for clock in captures:
                identity = f"minute-{clock.strftime('%Y%m%dT%H%M%S%z')}"
                row = connection.execute("SELECT decision_as_of,captured_at,payload_sha256,payload_zlib "
                    "FROM minute_decision_snapshots WHERE snapshot_id=? AND symbol=? AND interval='1m'",
                    (identity, symbol)).fetchone()
                if (row is None or datetime.fromisoformat(row[0]) != clock
                        or not clock <= datetime.fromisoformat(row[1]) <= current):
                    return base
                # Bound both compressed and expanded data, including malformed input.
                if len(row[3]) > 8 * 1024 * 1024:
                    return base
                decoder = zlib.decompressobj()
                raw = decoder.decompress(row[3], 8 * 1024 * 1024 + 1)
                if len(raw) > 8 * 1024 * 1024 or not decoder.eof or decoder.unused_data or sha256(raw).hexdigest() != row[2]:
                    return base
                packet = json.loads(raw)
                if not isinstance(packet, list) or not 0 < len(packet) <= 5000:
                    return base
                for item in packet:
                    bar = MinuteBar.model_validate(item)
                    if (bar.symbol != symbol or bar.interval != "1m" or not bar.source_id
                            or bar.source_id.endswith(":RISK_ONLY") or bar.bar_end > clock - timedelta(minutes=1)):
                        return base
                    if bar.bar_end.date() == current.date():
                        observed.add(bar.bar_end)
                manifest.append([identity, row[0], row[1], row[2]])
            connection.rollback()
    except (OSError, sqlite3.Error, ValueError, TypeError, zlib.error):
        return base
    if eligible in observed or observed != expected_bars - {eligible}:
        return base
    return {**base, "status": "COMPLETE_CAPTURE_MISSING", "capture_count": len(manifest),
        "other_window_count": len(observed), "execution_cutoff": cutoff.isoformat(),
        "ledger_sha256": sha256(json.dumps(manifest, separators=(",", ":")).encode()).hexdigest(),
        "captures": manifest}


def finalize_pending_entries(store, minute_store, *, now: datetime) -> dict:
    current = now.astimezone(TZ)
    result = {"trade_date": current.date().isoformat(), "observed_at": current.isoformat(),
        "closed": 0, "unresolved": 0, "status": "NOT_DUE", "items": []}
    if current.time() < time(15, 2):
        return result
    for life in store.list_a4_signal_lifecycles(trade_date=current.date().isoformat(), status="SIGNALLED", limit=10000):
        event = store.get_monitor_event(str(life["entry_event_key"]))
        try:
            eligible = entry_bar_for({**json.loads(event["payload_json"]), "signal_time": event["minute_end"]}) if event else None
        except (TypeError, ValueError):
            eligible = None
        proof = (missing_window_evidence(minute_store.path, symbol=life["symbol"], eligible=eligible, now=current)
            if eligible is not None else {"status": "UNRESOLVED"})
        if proof["status"] == "COMPLETE_CAPTURE_MISSING":
            # Persistence errors deliberately propagate; no success receipt on failure.
            store.complete_a4_entry_data_missing(life["entry_event_key"], evidence=proof, at=current)
            result["closed"] += 1
            code = "ENTRY_WINDOW_DATA_MISSING"
        else:
            result["unresolved"] += 1
            code = "A4_ENTRY_WINDOW_UNRESOLVED"
        result["items"].append({"plan_id": life["plan_id"], "signal_id": life["entry_event_key"], "reason_code": code})
    result["status"] = "DATA_LIMITED" if result["unresolved"] else "COMPLETE"
    if result["unresolved"]:
        # Exact stable line is consumed by Node's internal-only alarm, including
        # when a later A5 model invocation fails. No customer notification call.
        print("A4_ENTRY_WINDOW_UNRESOLVED", file=sys.stderr, flush=True)
    return result
