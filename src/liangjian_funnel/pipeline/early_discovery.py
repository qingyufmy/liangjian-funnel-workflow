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
IDENTITY_VERSION = "early-discovery-json/2"


def verify_discovery_event_identity(event):
    """Verify one exact identity format, including persisted legacy v1 MA keys.

    Python integer map keys become strings in JSON. Legacy v1 constructed
    these two maps with integer keys before hashing; reconstruct only that
    documented shape. New records hash a JSON-stable evidence representation.
    Never rewrite a historical ID or try arbitrary representations to match.
    """
    try:
        evidence = deepcopy(event['evidence'])
        identity = event.get('identity_version')
        if identity is None:
            for key in ('ma', 'previous_ma'):
                values = evidence[key]
                if (not isinstance(values, Mapping) or len(values) != 3
                        or {str(k) for k in values} != {'5','20','60'}):
                    return False
                evidence[key] = {int(k):v for k,v in values.items()}
        elif identity != IDENTITY_VERSION:
            return False
        elif any(not isinstance(evidence.get(key), Mapping)
                 or set(evidence[key]) != {'5','20','60'} for key in ('ma','previous_ma')):
            return False
        return (event['decision_id'] == content_hash({'version':VERSION,'symbol':event['symbol'],'evidence':evidence})
            and event['setup_event_id'] == content_hash({'version':VERSION,'symbol':event['symbol'],
                'signals':event['signals'],'setup_bar_end':evidence['setup_bar_end'],
                'adjust_mode':evidence['adjust_mode']}))
    except (KeyError,TypeError,ValueError):
        return False


def scan_cached_universe(cache: Any, symbols: Sequence[str], *, as_of: datetime) -> dict[str, Any]:
    """Pre-open cache-only radar: never perform provider requests here."""
    parts = []
    for symbol in sorted(set(symbols)):
        rows = cache.query_daily_bars(symbol, adjust="none", start=as_of - timedelta(days=800),
                                     end=as_of + timedelta(seconds=1), as_of=as_of)
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
        # Stable before and after a persisted JSON round trip. The numerical
        # signal calculation above retains its original integer-key MA maps.
        for key in ('ma','previous_ma'):
            evidence[key] = {str(k):v for k,v in evidence[key].items()}
        leads.append({"symbol": symbol, "signals": signals, "evidence": evidence,
                      "identity_version": IDENTITY_VERSION,
                      "decision_id": content_hash({"version": VERSION, "symbol": symbol, "evidence": evidence}),
                      "setup_event_id": content_hash({"version": VERSION, "symbol": symbol,
                          "signals": signals, "setup_bar_end": setup_end.isoformat(),
                          "adjust_mode": bars[-1].adjust_mode}),
                      "source_hash": content_hash([bar.model_dump(mode="json") for bar in bars]),
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
        row["review_state"] = "PENDING_A2_QUALIFICATION" if row["a1_formal_member"] else "PENDING_A1_EVIDENCE"
        row["next_action"] = "A2_ELIGIBILITY_CHECK" if row["a1_formal_member"] else "A1_EVIDENCE_RECHECK"
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
            row.update(a1_formal_member=True, review_state="PENDING_A2_QUALIFICATION",
                       next_action="A2_ELIGIBILITY_CHECK")
    return result


def bind_a2_discovery_states(decisions: Sequence[dict[str, Any]], discovery: Mapping[str, Any],
                             *, as_of: Any) -> None:
    """Annotate existing A2 decisions; never create eligibility or a model slot.

    A discovery event is a research lead, not a completed review. In particular
    the fixed strong-trend work budget must remain visible after local ranking.
    """
    events = {str(row.get("symbol")): row for row in discovery.get("records", [])
              if isinstance(row, Mapping)}
    for item in decisions:
        event = events.get(str(item.get("symbol")))
        if not event:
            continue
        # Old artifacts can be displayed, but cannot impersonate this run's
        # causally available discovery evidence.
        try:
            cutoff = datetime.fromisoformat(str(as_of).replace("Z", "+00:00"))
            event_time = datetime.fromisoformat(str(event.get("as_of")).replace("Z", "+00:00"))
            setup = datetime.fromisoformat(str((event.get("evidence") or {}).get("setup_bar_end")).replace("Z", "+00:00"))
            verified = (cutoff.tzinfo is not None and event_time.tzinfo is not None
                        and setup.tzinfo is not None and setup <= event_time <= cutoff)
        except (TypeError, ValueError):
            verified = False
        reasons = list(item.get("reason_codes") or [])
        selected = item.get("sent_to_llm") is True and item.get("status") == "REVIEW_CANDIDATE"
        state = ("SOURCE_UNVERIFIED" if not verified else "SELECTED_PENDING_MODEL" if selected else
                 "PENDING_REVIEW_BUDGET" if "A2_STRONG_TREND_OBSERVATION_BUDGET" in reasons else
                 "PENDING_A2_QUALIFICATION")
        audit = {**deepcopy(event), "review_state": state,
                 "next_action": "A2_MODEL_REVIEW" if selected and verified else
                     "A2_REVIEW_BUDGET_WAIT" if state == "PENDING_REVIEW_BUDGET" else "A2_ELIGIBILITY_CHECK",
                 "local_status": item.get("status"), "local_reason_codes": reasons,
                 "model_scope_selected": selected, "model_review_validated": False,
                 "execution_permission": "BLOCKED", "execution_authority": False}
        item["early_discovery"] = audit
        item.setdefault("bottleneck_context", {})["early_discovery"] = deepcopy(audit)


def finalize_a2_discovery_review(snapshot: Mapping[str, Any], upstream: Mapping[str, Any], *,
                                  symbols: Sequence[str], status: str,
                                  call_ids: Sequence[str], input_hash: str) -> dict[str, Any]:
    """Pure audit at the actual StageAudit boundary, not a provider-call claim.

    Model call IDs bind the stage, not per-stock HTTP counts. Budget overflow
    and unqualified events stay pending even when another stock was reviewed.
    """
    discovery = snapshot.get("EARLY_DISCOVERY_SNAPSHOT") or {}
    queue = attach_discovery_queue(upstream, discovery)["early_discovery"]
    contexts = snapshot.get("A2_BOTTLENECK_CONTEXT") or {}
    scope = set(symbols)
    records = []
    for raw in queue.get("records", []):
        symbol = str(raw.get("symbol"))
        context = contexts.get(symbol) or {}
        row = deepcopy(context.get("early_discovery") or raw)
        in_scope = symbol in scope and row.get("review_state") == "SELECTED_PENDING_MODEL"
        row["model_review_validated"] = bool(in_scope and status == "VALIDATED")
        if in_scope:
            row.update(review_state="REVIEW_VALIDATED" if status == "VALIDATED" else "REVIEW_BLOCKED",
                       next_action="A2_REVIEW_COMPLETE_CHECK_PARTITION" if status == "VALIDATED" else "A2_REVIEW_RETRY_PENDING",
                       model_stage_status=status, model_stage_call_ids=list(call_ids),
                       model_stage_input_hash=input_hash)
        records.append(row)
    counts: dict[str, int] = {}
    for row in records:
        state = row["review_state"]
        counts[state] = counts.get(state, 0) + 1
    return {**queue, "records": records, "review_state_counts": dict(sorted(counts.items())),
            "audit_scope": "ACTUAL_STAGE_AUDIT_NOT_GLOBAL_MODEL_OR_HTTP_COMPLETION",
            "stage_review_symbols": sorted(scope), "stage_input_hash": input_hash,
            "execution_authority": False}
