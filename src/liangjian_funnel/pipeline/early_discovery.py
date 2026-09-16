"""Closed daily-bar research leads, independent of board/popularity ranking.

No provider calls, model calls, registry writes or execution permission. The
caller supplies a frozen point-in-time universe and persists the returned audit.
"""
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from copy import deepcopy
from typing import Any

from .factors import SHANGHAI, _daily_bars
from .feature_store import content_hash
from .macd_evidence import macd_evidence

VERSION = "early-discovery/1"


def scan_cached_universe(cache: Any, symbols: Sequence[str], *, as_of: datetime) -> dict[str, Any]:
    """Pre-open cache-only radar: never perform provider requests here."""
    parts = []
    for symbol in sorted(set(symbols)):
        rows = cache.query_daily_bars(symbol, adjust="none", start=as_of - timedelta(days=800),
                                     end=as_of + timedelta(seconds=1))
        parts.append(discover_early_setups({symbol: [
            {**row["payload"], "timestamp": row["timestamp"], "adjust": row["adjust"]} for row in rows
        ]}, as_of=as_of, symbols=[symbol]))
    return merge_discovery_parts(parts, as_of=as_of)


def _signals(prices: list[float]) -> tuple[list[str], dict[str, Any]]:
    ma = {n: sum(prices[-n:]) / n for n in (5, 20, 60)}
    previous = {n: sum(prices[-n-1:-1]) / n for n in (5, 20, 60)}
    macd, old_macd = macd_evidence(prices), macd_evidence(prices[:-1])
    rising = ma[5] > previous[5]
    momentum = macd["hist"] > 0 and macd["hist"] > old_macd["hist"]
    reclaim = prices[-1] > ma[20] and prices[-2] <= previous[20]
    repaired = rising and momentum and prices[-1] > ma[20]
    signals = []
    if repaired and ma[5] < ma[20]:
        signals.append("REPAIR_WATCH")
    if repaired and previous[5] <= previous[20] and ma[5] > ma[20]:
        signals.append("MA520_CROSS_PREPARATION")
    if repaired and ma[20] > previous[20] and prices[-1] > ma[60] and reclaim:
        signals.append("TREND_RESTART_WATCH")
    return signals, {"close": prices[-1], "ma": ma, "previous_ma": previous,
                     "macd": macd, "previous_macd": old_macd}


def discover_early_setups(daily_by_symbol: Mapping[str, Any], *, as_of: datetime,
                          symbols: Sequence[str], limit: int = 100) -> dict[str, Any]:
    if as_of.tzinfo is None or limit < 1:
        raise ValueError("discovery requires aware cutoff and positive budget")
    cutoff = as_of.astimezone(SHANGHAI)
    leads, gaps = [], []
    scanned = 0
    for symbol in sorted(set(symbols)):
        rows = daily_by_symbol.get(symbol, [])
        if not isinstance(rows, (list, tuple)):
            rows = []
        bars, reasons = _daily_bars(rows, symbol, cutoff)
        modes = {bar.adjust_mode for bar in bars}
        if reasons or len(bars) < 61 or len(modes) != 1 or None in modes:
            gaps.append({"symbol": symbol, "reasons": list(reasons) or [
                "DAILY_HISTORY_INSUFFICIENT" if len(bars) < 61 else "ADJUSTMENT_BASIS_UNRESOLVED"]})
            continue
        # A stale symbol is not a fresh turn. Calendar selection is upstream;
        # at an intraday cutoff today's forming bar is deliberately excluded.
        if cutoff.hour >= 15 and bars[-1].end.date() != cutoff.date():
            gaps.append({"symbol": symbol, "reasons": ["DAILY_REFERENCE_STALE"]})
            continue
        scanned += 1
        prices = [bar.close for bar in bars]
        signals, current = _signals(prices)
        setup_end = bars[-1].end
        observation_age = 0
        if not signals and prices[-1] > current["ma"][20]:
            # Maintain a discovered setup through ten closed sessions, subject
            # to today's MA20 invalidation. Daily A1 deltas are revalidated,
            # not lost merely because yesterday's crossover is no longer new.
            for age in range(1, min(10, len(prices) - 60)):
                previous_signals, previous_evidence = _signals(prices[:-age])
                if prices[-age-1] <= previous_evidence["ma"][20]:
                    break
                if previous_signals:
                    signals, setup_end, observation_age = previous_signals, bars[-age-1].end, age
                    break
        if not signals:
            continue
        evidence = {"bar_end": bars[-1].end.isoformat(), **current,
                    "setup_bar_end": setup_end.isoformat(), "observation_age_sessions": observation_age,
                    "adjust_mode": bars[-1].adjust_mode,
                    "bar_count": len(bars)}
        leads.append({"symbol": symbol, "signals": signals, "evidence": evidence,
                      "decision_id": content_hash({"version": VERSION, "symbol": symbol, "evidence": evidence}),
                      "as_of": cutoff.isoformat(), "research_state": "PREPARATION_WATCH",
                      "event_state": "NEW_SETUP" if observation_age == 0 else "CONTINUING_WATCH",
                      "execution_permission": "BLOCKED", "requires_a1_evidence": True})
    # Deterministic event ordering, not a promise of expected return. Keep the
    # overflow in the audit so review budgets never become invisible omissions.
    leads.sort(key=lambda row: ("TREND_RESTART_WATCH" not in row["signals"],
                                "MA520_CROSS_PREPARATION" not in row["signals"], row["symbol"]))
    for index, row in enumerate(leads):
        row["review_budget_selected"] = index < limit
    return {"version": VERSION, "as_of": cutoff.isoformat(), "universe_count": len(set(symbols)),
            "scanned_count": scanned, "lead_count": len(leads), "review_limit": limit,
            "records": leads, "data_gaps": gaps, "execution_authority": False}


def attach_discovery_queue(output: Mapping[str, Any], discovery: Mapping[str, Any]) -> dict[str, Any]:
    """Keep out-of-A1 leads visible without silently admitting them to A2."""
    active = {str(row.get("symbol")): row for row in output.get("active_research_pool", [])
              if isinstance(row, Mapping) and row.get("research_route") != "DAILY_EMOTION_OVERLAY"}
    queue = []
    for raw in discovery.get("records", []):
        row = dict(raw)
        row["a1_formal_member"] = row["symbol"] in active
        row["next_action"] = "A2_INDEPENDENT_REVIEW" if row["a1_formal_member"] else "A1_EVIDENCE_RECHECK"
        queue.append(row)
    return {**output, "early_discovery": {**discovery, "records": queue,
            "evidence_recheck_symbols": [row["symbol"] for row in queue
                if not row["a1_formal_member"] and row.get("review_budget_selected")]}}


def merge_discovery_parts(parts: Sequence[Mapping[str, Any]], *, as_of: datetime, limit: int = 100) -> dict[str, Any]:
    """Combine per-symbol streaming scans without retaining market-wide bars."""
    leads = [dict(row) for part in parts for row in part.get("records", [])]
    leads.sort(key=lambda row: ("TREND_RESTART_WATCH" not in row["signals"],
                                "MA520_CROSS_PREPARATION" not in row["signals"], row["symbol"]))
    for index, row in enumerate(leads):
        row["review_budget_selected"] = index < limit
    return {"version": VERSION, "as_of": as_of.isoformat(), "universe_count": sum(p["universe_count"] for p in parts),
            "scanned_count": sum(p["scanned_count"] for p in parts), "lead_count": len(leads), "review_limit": limit,
            "records": leads, "data_gaps": [row for p in parts for row in p["data_gaps"]], "execution_authority": False}


def recheck_a1_discovery(output: Mapping[str, Any], snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Daily verified delta using the existing A1 gate, not a technical bypass.

    Only dated disclosed financial and business evidence can enter this path.
    The sealed monthly generation is never modified. Unresolved requests remain
    in the daily artifact for the next evidence refresh, not fabricated facts.
    """
    from .deterministic import screen_a1, local_active_items

    result = attach_discovery_queue(output, snapshot.get("EARLY_DISCOVERY_SNAPSHOT") or {})
    queue = result["early_discovery"]
    requested = set(queue["evidence_recheck_symbols"])
    if not requested:
        return result
    try:
        cutoff = datetime.fromisoformat(str(snapshot["snapshot_manifest"]["as_of"]).replace("Z", "+00:00"))
        if cutoff.tzinfo is None:
            raise ValueError("undated snapshot")
    except (KeyError, TypeError, ValueError):
        queue["recheck_blocker"] = "SNAPSHOT_CUTOFF_UNAVAILABLE"
        return result

    scoped = {**snapshot, "g0_symbols": sorted(requested)}
    fundamentals = {s: deepcopy((snapshot.get("COMPANY_FUNDAMENTALS") or {}).get(s) or {}) for s in requested}
    business = {s: deepcopy((snapshot.get("MAIN_BUSINESS_EVIDENCE") or {}).get(s) or {}) for s in requested}
    for symbol in requested:
        fundamental = fundamentals.get(symbol) or {}
        statements = fundamental.get("statements") or {}
        # Missing disclosure date is not equivalent to known before cutoff.
        for kind, rows in list(statements.items()):
            if isinstance(rows, list):
                statements[kind] = [row for row in rows if isinstance(row, Mapping)
                    and isinstance(row.get("report_date_ms"), (int, float))
                    and 0 < row["report_date_ms"] <= cutoff.timestamp() * 1000]
        payload = business.get(symbol) or {}
        dated = []
        for row in payload.get("evidence", []):
            if not isinstance(row, Mapping):
                continue
            try:
                published = datetime.fromisoformat(str(row.get("publish_time") or "").replace("Z", "+00:00"))
                if published.tzinfo is None:
                    published = published.replace(tzinfo=SHANGHAI)
                if published <= cutoff and row.get("source_ref"):
                    dated.append(row)
            except ValueError:
                continue
        business[symbol] = {**payload, "evidence": dated, "available": bool(dated)}
    scoped["COMPANY_FUNDAMENTALS"] = fundamentals
    scoped["MAIN_BUSINESS_EVIDENCE"] = business
    gate = screen_a1(scoped, output)
    decisions = {row["symbol"]: row for row in gate.decisions}
    admitted = []
    for row in local_active_items(gate):
        half_year = row.get("half_year_support") or {}
        if (row["symbol"] not in requested or half_year.get("supported") is not True
                or not half_year.get("report_date_ms") or row.get("downstream_trade_eligible") is False):
            continue
        row["daily_verified_increment"] = {"version": VERSION, "as_of": cutoff.isoformat(),
            "basis": "EXISTING_A1_GATE_AND_DISCLOSED_FUNDAMENTALS", "monthly_generation_mutated": False,
            "evidence_hash": content_hash({"business": business.get(row["symbol"]),
                                           "financial": fundamentals.get(row["symbol"])})}
        admitted.append(row)
    admitted_symbols = {row["symbol"] for row in admitted}
    for pool in ("active_research_pool", "monitor_pool", "rejected_candidates"):
        result[pool] = [dict(row) for row in output.get(pool, []) if row.get("symbol") not in admitted_symbols]
    result["active_research_pool"].extend(admitted)
    queue["recheck_results"] = [{"symbol": symbol, "admitted": symbol in admitted_symbols,
        "reason_codes": decisions.get(symbol, {}).get("reason_codes", ["A1_DISCOVERY_EVIDENCE_PENDING"])}
        for symbol in sorted(requested)]
    queue["admitted_symbols"] = sorted(admitted_symbols)
    queue["evidence_recheck_symbols"] = sorted(requested - admitted_symbols)
    for row in queue["records"]:
        if row["symbol"] in admitted_symbols:
            row.update(a1_formal_member=True, next_action="A2_INDEPENDENT_REVIEW")
    return result
