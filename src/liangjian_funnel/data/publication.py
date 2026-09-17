"""Bounded publication checks before freezing a current-session execution pack.

Observed stability is not exchange finality. The late probe horizon is a
versioned conservative acquisition policy, not an OHLCV tolerance or buy gate.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import hashlib
import json
import math
import time

from .execution_evidence import execution_evidence
from .session_windows import TZ

POLICY_VERSION = "minute-publication/2"
PROBE_AGES = (20.0, 25.0)
ACQUISITION_BUDGET_SECONDS = 30.0


def _digest(result):
    if result is None or not result.complete:
        return None
    rows = [b.model_dump(mode="json") for b in result.bars]
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def receipt(pack, *, at):
    one, five = pack.get("1m"), pack.get("5m")
    comparison = {"status": "UNAVAILABLE", "conflicts": []}
    if one is not None and one.complete:
        _, evidence = execution_evidence(one.bars, five.bars if five is not None and five.complete else (), as_of=at)
        comparison = evidence["native_5m_comparison"]
    return {
        "comparison": comparison,
        "sources": {period: {"complete": bool(value and value.complete),
            "reason": value.reason_code if value else "NOT_REQUIRED",
            "hash": _digest(value),
            "request_started_at": value.request_started_at.isoformat() if value and value.request_started_at else None,
            "response_received_at": value.response_received_at.isoformat() if value and value.response_received_at else None,
            "last_end": value.bars[-1].bar_end.isoformat() if value and value.bars else None,
            "source_ids": sorted({bar.source_id for bar in value.bars}) if value else [],
            "volume_units": sorted({bar.volume_unit for bar in value.bars}) if value else [],
            "normalizer_versions": sorted({bar.normalizer_version for bar in value.bars}) if value else [],
            "adjust_modes": sorted({bar.adjust_mode for bar in value.bars}) if value else [],
            "last_bar": value.bars[-1].model_dump(mode="json") if value and value.bars else None,
            "bar_count": len(value.bars) if value else 0}
            for period, value in (("1m", one), ("5m", five))},
    }


def unresolved_conflicts(pack, attempts, *, at):
    """Resolve the exact old fields, not an unrelated aggregate status label."""
    known = {(item['end'], item['field']) for attempt in attempts
             for item in attempt['comparison'].get('conflicts', [])}
    if not known:
        return []
    one, five = pack.get('1m'), pack.get('5m')
    if one is None or five is None or not one.complete or not five.complete:
        return [{'end': end, 'field': field} for end, field in sorted(known)]
    aggregated, _ = execution_evidence(one.bars, five.bars, as_of=at)
    derived = {row['bar_end']: row for row in aggregated['5m']}
    native = {bar.bar_end.isoformat(): bar for bar in five.bars}
    return [{'end': end, 'field': field} for end, field in sorted(known)
            if end not in derived or end not in native or not math.isclose(
                float(derived[end][field]), float(getattr(native[end], field)), rel_tol=0, abs_tol=1e-7)]


def confirm_publications(initial, fetch, *, at, deadline, clock=time.monotonic,
                         wall_clock=lambda: datetime.now(TZ), sleep=time.sleep, workers=8):
    """At most two additional sweeps, same deadline, no per-symbol sleeps.

    ``fetch`` returns an atomic per-symbol pair, using the passed shared
    deadline. All symbols receive the first sweep before confirmation starts.
    Only the final selected pack may be frozen for a trading decision.
    """
    selected = {symbol: dict(pack) for symbol, pack in initial.items()}
    evidence = {symbol: {"version": POLICY_VERSION, "market_cutoff": at.isoformat(),
        "state": "PENDING_PUBLICATION", "exchange_finality_proven": False,
        "probe_ages_seconds": list(PROBE_AGES), "attempts": [receipt(pack, at=at)]}
        for symbol, pack in selected.items()}
    eligible = {symbol for symbol, pack in selected.items()
                if pack.get("1m") is not None and (
                    pack["1m"].complete
                    or pack["1m"].reason_code == "CLOSE_BAR_FINALIZATION_UNCONFIRMED"
                )}
    prior_sweep_end = None
    for target_age in PROBE_AGES:
        if not eligible:
            break
        delay = max(0., target_age - (wall_clock() - at).total_seconds())
        if prior_sweep_end is not None:
            delay = max(delay, prior_sweep_end + PROBE_AGES[-1] - PROBE_AGES[0] - clock())
        # Never sleep past the common acquisition budget, nor launch work
        # that already has no budget. Real clients enforce bounded timeouts.
        if clock() + delay >= deadline:
            break
        if delay:
            sleep(delay)
        if not eligible or clock() >= deadline:
            break
        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(eligible)))) as executor:
            futures = {executor.submit(fetch, symbol): symbol for symbol in sorted(eligible)}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    _, pack = future.result()
                except Exception:
                    pack = {"fetch_error": "MINUTE_DATA_FETCH_FAILED"}
                observed = receipt(pack, at=at)
                previous = evidence[symbol]["attempts"][-1]
                evidence[symbol]["attempts"].append(observed)
                evidence[symbol]["last_observed_at"] = wall_clock().isoformat()
                # A failed or late retry cannot be replaced with a successful
                # earlier but unconfirmed version of the current candle.
                selected[symbol] = dict(pack)
                one = pack.get("1m")
                if clock() > deadline or one is None or not one.complete:
                    continue
                late_probe = (wall_clock() - at).total_seconds() >= PROBE_AGES[-1]
                stable = bool(observed["sources"]["1m"]["hash"] and
                              observed["sources"]["1m"]["hash"] == previous["sources"]["1m"]["hash"])
                unresolved = unresolved_conflicts(pack, evidence[symbol]['attempts'][:-1], at=at)
                observed['unresolved_prior_conflicts'] = unresolved
                auxiliary_conflict = observed["comparison"]["status"] == "CONFLICT" or bool(unresolved)
                evidence[symbol]["auxiliary_state"] = "DEGRADED" if auxiliary_conflict else (
                    "READY" if observed["comparison"]["status"] in {"MATCH", "DATA_LIMITED"} else "UNAVAILABLE"
                )
                if target_age == PROBE_AGES[-1] and late_probe and stable:
                    evidence[symbol]["state"] = "OBSERVED_STABLE"
        prior_sweep_end = clock()
    for symbol, pack in selected.items():
        item = evidence[symbol]
        one = pack.get("1m")
        if one is None or not one.complete:
            item["state"] = "INPUT_UNAVAILABLE"
        if item["state"] == "PENDING_PUBLICATION":
            pack["publication_error"] = "MINUTE_PUBLICATION_PENDING"
        elif item["state"] == "INPUT_UNAVAILABLE":
            pack["publication_error"] = pack.get("fetch_error") or (one.reason_code if one else "MINUTE_DATA_UNAVAILABLE")
        comparison = item.get("attempts", [{}])[-1].get("comparison", {})
        if comparison.get("status") == "CONFLICT" or item.get("auxiliary_state") == "DEGRADED":
            pack["auxiliary_error"] = "AUXILIARY_NATIVE_5M_CONFLICT"
        elif pack.get("5m") is not None and not pack["5m"].complete:
            pack["auxiliary_error"] = pack["5m"].reason_code
        pack["publication"] = item
    return selected


def reuse_publication(pack, original, *, at):
    """An existing minute's approval cannot certify a newly revised pack."""
    result = dict(pack)
    result["publication"] = dict(original)
    result["publication_error"] = original.get("decision_error")
    current = receipt(pack, at=at)["sources"]
    frozen = original["attempts"][-1]["sources"]
    if any(current[p]["hash"] != frozen[p]["hash"] for p in ("1m", "5m")):
        result["publication_error"] = "MINUTE_PUBLICATION_REPLAY_MISMATCH"
    return result


def classify_persistent_pending(pack, previous, *, at):
    """Escalate consecutive unresolved decisions, never suppress raw faults."""
    result = dict(pack)
    item = dict(result.get("publication") or {})
    prior = previous or {}
    try:
        consecutive = (at - datetime.fromisoformat(prior["market_cutoff"])).total_seconds() == 60
    except (KeyError, ValueError, TypeError):
        consecutive = False
    pending = item.get("state") == "PENDING_PUBLICATION"
    count = (int(prior.get("pending_minutes", 0)) + 1 if consecutive else 1) if pending else 0
    item["pending_minutes"] = count
    result["publication"] = item
    if pending and count >= 2:
        result["publication_error"] = "MINUTE_PUBLICATION_UNCONFIRMED"
    return result
