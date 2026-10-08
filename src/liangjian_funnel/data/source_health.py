"""Request/window facts, not a global green light or a strategy override."""
from collections import Counter

from .session_windows import closed_window_ends
from .execution_evidence import execution_evidence
from .live_quote import validate_quote


def source_health(symbol, pack, *, at, cutoff):
    one = pack.get("1m")
    quote_result = pack.get("quote")
    expected = closed_window_ends(cutoff, "1m") if cutoff else ()
    bars = tuple(one.bars) if one is not None else ()
    actual = {b.bar_end for b in bars if b.symbol == symbol and b.interval == "1m"}
    gaps = [t.isoformat() for t in expected if t not in actual]
    complete = bool(expected and one is not None and one.complete
                    and tuple(b.bar_end for b in bars) == expected
                    and all(b.symbol == symbol and b.interval == "1m" for b in bars))
    publication_error = pack.get("publication_error")
    minute_ready = complete and not publication_error
    checked_quote = validate_quote(quote_result, symbol, as_of=at) if quote_result is not None else None
    quote_ready = bool(checked_quote and checked_quote.complete and checked_quote.quote)
    quote = checked_quote.quote if checked_quote else None
    _, evidence = execution_evidence(bars if complete else (), (), as_of=cutoff or at)
    return {
        "schema_version": "a4-source-health/1",
        "symbol": symbol, "decision_at": at.isoformat(),
        "expected_closed_eob": cutoff.isoformat() if cutoff else None,
        "state": "WAITING_BAR_CLOSE" if not expected else "REQUIRED_INPUTS_READY" if minute_ready and quote_ready else "DATA_BLOCKED",
        "required_inputs_ready": minute_ready and quote_ready,
        "one_minute": {
            "requested": len(expected), "returned": len(bars), "bar_complete": complete,
            "publication_ready": minute_ready,
            "first_eob": bars[0].bar_end.isoformat() if bars else None,
            "last_eob": bars[-1].bar_end.isoformat() if bars else None,
            "gap_count": len(gaps), "missing_ends": gaps,
            "reason_code": publication_error or pack.get("fetch_error") or (one.reason_code if one else "NOT_AVAILABLE"),
            "source_ids": sorted({b.source_id for b in bars}),
            "input_sha256": evidence["input_sha256"],
            "request_attempts": list(one.source_attempts) if one else [],
        },
        "derived_periods": {
            interval: {"expected_last_eob": ends[-1].isoformat() if ends else None,
                       "actual_last_eob": evidence["last_ends"].get(interval)}
            for interval in ("5m", "15m")
            for ends in (closed_window_ends(cutoff, interval) if cutoff else (),)
        },
        "quote": {
            "fresh": quote_ready, "source_id": quote.source_id if quote else None,
            "provider_timestamp": quote.quote_time.isoformat() if quote else None,
            "received_timestamp": checked_quote.response_received_at.isoformat() if checked_quote and checked_quote.response_received_at else None,
            "reason_code": checked_quote.reason_code if checked_quote else "NOT_AVAILABLE",
            "request_attempts": list(checked_quote.source_attempts) if checked_quote else [],
            "authority": "LIVE_PRICE_AND_HARD_RISK_ONLY_NOT_OHLC_OR_FILL",
        },
        "auxiliary": {"reason_code": pack.get("auxiliary_error"),
                      "execution_dependency": False},
        "indicator_warmup_authority": "SEPARATE_STRATEGY_GATE",
    }


def summarize_health(records):
    """Deduplicate by decision/symbol, include failures and actual missing ticks.

    Only source-health/1 records count; legacy evidence is explicitly uncounted.
    Attempt totals are requests, never reported as independent stock coverage.
    """
    unique, legacy = {}, 0
    for record in records:
        for symbol, item in record.get("symbols", {}).items():
            health = item.get("source_health")
            if not isinstance(health, dict) or health.get("schema_version") != "a4-source-health/1":
                legacy += 1
                continue
            unique[(health["decision_at"], symbol)] = health
    counts = Counter(row["state"] for row in unique.values())
    requests = Counter()
    for row in unique.values():
        for role in ("one_minute", "quote"):
            for attempt in row[role]["request_attempts"]:
                requests[(role, attempt.get("source_role"), attempt.get("reason_code"))] += 1
    return {"schema_version": "a4-source-health-daily/1", "decision_symbol_pairs": len(unique),
            "symbols": sorted({symbol for _, symbol in unique}),
            "decision_times": sorted({at for at, _ in unique}),
            "states": dict(counts), "legacy_rows_without_health": legacy,
            "request_attempt_counts": [{"role": role, "source_role": source, "reason_code": reason, "count": count}
                                       for (role, source, reason), count in sorted(requests.items(), key=lambda x: str(x[0]))],
            "acceptance_scope": "ACQUISITION_ONLY_NOT_STRATEGY_OR_PRODUCTION_SLA"}
