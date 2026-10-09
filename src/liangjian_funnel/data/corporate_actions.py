"""Isolated local qfq-ratio/1 evidence, not a provider or eligibility adapter.

No IO, credentials, database, workflow or state mutation. Evidence assertions
are caller-supplied LOCAL certificates, never proof of live source semantics.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
from decimal import Decimal, DecimalException, localcontext
import hashlib
import json
import math
import re
from typing import Any

FORMULA_VERSION = "qfq-ratio/1"
CONTRACT_VERSION = "corporate-adjustment-evidence/1"
_SYMBOL = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_PRICE_FIELDS = ("open", "high", "low", "close")
_EVENT_FIELDS = ("symbol", "ex_date", "previous_trade_date", "preclose",
                 "cash_paid", "cash_reference", "differential_distribution",
                 "bonus_total", "rights_ratio", "rights_price")
_VERIFIED_FIELDS = {"cash_reference", "differential_distribution", "bonus_total",
                    "rights_ratio", "rights_price"}


def _hash(value: Any) -> str | None:
    try:
        body = json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, OverflowError):
        return None
    return hashlib.sha256(body).hexdigest()


def _date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _number(value: Any, *, positive: bool = False) -> bool:
    try:
        return (type(value) in (int, float) and math.isfinite(value)
                and (value > 0 if positive else value >= 0))
    except OverflowError:
        return False


def _valid_rows(rows: Any, symbol: str, *, raw: bool) -> bool:
    if not isinstance(rows, list) or not 1 <= len(rows) <= 10000:
        return False
    previous = ""
    for row in rows:
        if not isinstance(row, dict) or row.get("symbol") != symbol or not _date(row.get("date")):
            return False
        if row["date"] <= previous:
            return False
        previous = row["date"]
        if raw and (not isinstance(row.get("adjust_mode"), str)
                    or row["adjust_mode"] not in {"raw", "none"}):
            return False
        if not all(_number(row.get(field), positive=True) for field in _PRICE_FIELDS):
            return False
        if not all(_number(row.get(field)) for field in ("volume", "amount")):
            return False
        if not (row["low"] <= min(row["open"], row["close"])
                <= max(row["open"], row["close"]) <= row["high"]):
            return False
    return True


def build_adjustment_evidence(*, symbol: str, raw_bars: list[dict], events: list[dict],
                              coverage: dict) -> dict:
    """Price-only historical product, anchored to the latest covered raw bar.

    ``coverage`` certifies the complete local trade calendar and event inventory
    within the raw window; empty events alone cannot prove no corporate action.
    Each event requires original source payload hash, reference hash and a
    SAMPLE_VERIFIED certificate for the exact reference cash, total bonus and
    rights fields. This module does not generate or authenticate certificates.
    """
    result = {"contract_version": CONTRACT_VERSION, "formula_version": FORMULA_VERSION,
              "symbol": symbol, "status": "ADJUSTMENT_DATA_GAP", "reasons": [],
              "live_verified": False, "evidence_scope": "CALLER_CERTIFIED_LOCAL_ONLY",
              "source_event_hash_basis": "CANONICAL_ORIGINAL_PAYLOAD_NOT_HTTP_BYTES",
              "field_semantics_scope": "PER_EVENT_SAMPLE_NOT_GLOBAL_PROVIDER",
              "execution_price_basis": "RAW_ONLY", "volume_amount_adjusted": False,
              "raw_rows": deepcopy(raw_bars), "raw_series_sha256": _hash(raw_bars),
              "technical_rows": [], "technical_series_sha256": None,
              "input_events_sha256": _hash(events),
              "original_payload_sha256s": [digest for event in events
                  if isinstance(event, dict) and isinstance(event.get("source_event"), dict)
                  and (digest := _hash(event["source_event"])) is not None]
                  if isinstance(events, list) else [],
              "normalized_events_sha256": None, "source_event_sha256s": [],
              "factor_chain": [], "factors": [], "series_version": None,
              "corporate_action_version": None, "factor_version": None,
              "coverage_sha256": _hash(coverage)}

    def gap(reason: str) -> dict:
        result["reasons"] = [reason]
        return result

    if not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol):
        return gap("SYMBOL_INVALID")
    if not _valid_rows(raw_bars, symbol, raw=True) or result["raw_series_sha256"] is None:
        return gap("RAW_SERIES_INVALID")
    dates = [row["date"] for row in raw_bars]
    if (not isinstance(coverage, dict) or coverage.get("complete") is not True
            or coverage.get("symbol") != symbol or coverage.get("from") != dates[0]
            or coverage.get("through") != dates[-1] or coverage.get("trade_dates") != dates
            or not _HASH.fullmatch(str(coverage.get("evidence_sha256", "")))
            or result["coverage_sha256"] is None):
        return gap("EVENT_OR_CALENDAR_COVERAGE_UNPROVEN")
    expected = coverage.get("expected_event_dates")
    if (not isinstance(expected, list) or not all(_date(day) and day in dates for day in expected)
            or expected != sorted(set(expected))):
        return gap("EVENT_INVENTORY_INVALID")
    if not isinstance(events, list) or len(events) > 10000 or not all(isinstance(e, dict) for e in events):
        return gap("EVENTS_INVALID")
    event_dates = [e.get("ex_date") for e in events]
    if not all(_date(day) for day in event_dates) or sorted(event_dates) != expected:
        return gap("MISSING_DUPLICATE_OR_OUT_OF_WINDOW_EVENT")
    raw_by_date = {row["date"]: row for row in raw_bars}
    chain, normalized, source_hashes = [], [], []
    for event in sorted(events, key=lambda row: row["ex_date"]):
        day = event["ex_date"]
        index = dates.index(day)
        if (event.get("symbol") != symbol or index == 0
                or event.get("previous_trade_date") != dates[index - 1]
                or not _number(event.get("preclose"), positive=True)
                or Decimal(str(event["preclose"])) != Decimal(str(raw_by_date[dates[index - 1]]["close"]))):
            return gap("EVENT_PREVIOUS_RAW_CLOSE_OR_IDENTITY_UNPROVEN")
        if (event.get("verification_scope") != "SAMPLE_VERIFIED"
                or not isinstance(event.get("verified_fields"), list)
                or not all(isinstance(field, str) for field in event["verified_fields"])
                or not _VERIFIED_FIELDS.issubset(event["verified_fields"])
                or not _HASH.fullmatch(str(event.get("reference_sha256", "")))
                or not isinstance(event.get("source_event"), dict) or not event["source_event"]
                or not _HASH.fullmatch(str(event.get("source_event_sha256", "")))
                or _hash(event["source_event"]) != event["source_event_sha256"]):
            return gap("EXACT_EVENT_FIELD_EVIDENCE_UNPROVEN")
        if (not all(_number(event.get(field)) for field in
                    ("cash_paid", "cash_reference", "bonus_total", "rights_ratio", "rights_price"))
                or type(event.get("differential_distribution")) is not bool):
            return gap("CASH_BONUS_OR_RIGHTS_INVALID_OR_MISSING")
        if (event["differential_distribution"] is False
                and Decimal(str(event["cash_paid"])) != Decimal(str(event["cash_reference"]))):
            return gap("ORDINARY_CASH_REFERENCE_CONFLICT")
        # Explicit zero rights is still a certified event field, never defaulted.
        if (event["rights_ratio"] == 0) != (event["rights_price"] == 0):
            return gap("RIGHTS_PAIR_CONFLICT")
        try:
            with localcontext() as context:
                context.prec = 40
                cprev, cash, bonus, rights, price = (Decimal(str(event[field])) for field in
                    ("preclose", "cash_reference", "bonus_total", "rights_ratio", "rights_price"))
                reference = (cprev - cash + rights * price) / (1 + bonus + rights)
                ratio = reference / cprev
        except (DecimalException, OverflowError):
            return gap("REFERENCE_PRICE_OR_RATIO_INVALID")
        if not _number(float(reference), positive=True) or not _number(float(ratio), positive=True):
            return gap("REFERENCE_PRICE_OR_RATIO_INVALID")
        item = {field: event[field] for field in _EVENT_FIELDS}
        item.update(source_event_sha256=event["source_event_sha256"],
                    reference_sha256=event["reference_sha256"],
                    verification_scope="SAMPLE_VERIFIED",
                    verified_fields=sorted(set(event["verified_fields"])))
        normalized.append(item)
        source_hashes.append(event["source_event_sha256"])
        chain.append({"ex_date": day, "previous_trade_date": event["previous_trade_date"],
                      "preclose": event["preclose"], "cash_reference": event["cash_reference"],
                      "reference_price": float(reference), "ratio": float(ratio),
                      "source_event_sha256": event["source_event_sha256"],
                      "normalized_event_sha256": _hash(item)})
    technical, factors = [], []
    for bar in raw_bars:
        try:
            with localcontext() as context:
                context.prec = 40
                factor = Decimal(1)
                for event in chain:
                    if event["ex_date"] > bar["date"]:
                        factor *= Decimal(str(event["ratio"]))
                row = deepcopy(bar)
                for field in _PRICE_FIELDS:
                    row[field] = float(Decimal(str(bar[field])) * factor)
        except (DecimalException, OverflowError):
            return gap("HISTORICAL_PRODUCT_INVALID")
        if not _number(float(factor), positive=True) or not all(_number(row[f], positive=True) for f in _PRICE_FIELDS):
            return gap("HISTORICAL_PRODUCT_INVALID")
        row["adjust_mode"] = "qfq-local"
        technical.append(row)
        factors.append(float(factor))
    technical_hash, event_hash = _hash(technical), _hash(normalized)
    corporate_version = _hash({"symbol":symbol, "events":[
        {"symbol":symbol, "ex_date":event["ex_date"], "source_event_sha256":event["source_event_sha256"]}
        for event in normalized
    ]})
    factor_version = _hash({"formula":FORMULA_VERSION, "corporate_action_version":corporate_version,
                            "normalized_events":event_hash})
    version = _hash({"contract": CONTRACT_VERSION, "formula": FORMULA_VERSION,
                     "raw": result["raw_series_sha256"], "technical": technical_hash,
                     "normalized_events": event_hash, "coverage": result["coverage_sha256"],
                     "factor_version":factor_version})
    result.update(status="LOCAL_READY", technical_rows=technical, factors=factors,
                  factor_chain=chain, source_event_sha256s=source_hashes,
                  normalized_events_sha256=event_hash,
                  technical_series_sha256=technical_hash, series_version=version,
                  corporate_action_version=corporate_version, factor_version=factor_version)
    return result


def pending_reset_evidence(previous_factor_version: str | None, current_factor_version: str | None) -> dict:
    """Compare factor versions, not changing daily series hashes; no mutation."""
    previous_version, current_version = previous_factor_version, current_factor_version
    previous_valid = bool(isinstance(previous_version, str) and _HASH.fullmatch(previous_version))
    current_valid = bool(isinstance(current_version, str) and _HASH.fullmatch(current_version))
    changed = bool(previous_valid and current_valid and previous_version != current_version)
    return {"contract_version": "corporate-pending-reset-evidence/1",
            "previous_version": previous_version if previous_valid else None,
            "current_version": current_version if current_valid else None,
            "pending_reset_required": changed, "state_mutated": False,
            "version_role":"CORPORATE_FACTOR_VERSION_NOT_RAW_SERIES",
            "status": "VERSION_CHANGED" if changed else "UNCHANGED" if previous_valid and current_valid else "UNPROVEN",
            "reason": "ADJUSTMENT_FACTOR_CHANGED" if changed else "NO_PROVEN_VERSION_CHANGE"}


def compare_forward(evidence: dict, forward_rows: list[dict], *, source_id: str,
                    raw_source_id: str) -> dict:
    """Independent forward fixture comparison; <=0.5% each OHLC, no live claim.

    Forward prices are comparison-only and never used to construct factors.
    Partial overlap, same-source data or mismatches remain a GAP without an
    event-specific explanation; this function cannot upgrade eligibility.
    """
    result = {"contract_version": "qfq-forward-comparison/1", "status": "ADJUSTMENT_DATA_GAP",
              "live_verified": False, "maximum_relative_error": None,
              "tolerance_relative": 0.005, "compared_bars": 0, "reasons": [],
              "forward_series_sha256": _hash(forward_rows), "calculation_basis": False}
    rows = evidence.get("technical_rows") if isinstance(evidence, dict) else None
    if (not isinstance(evidence, dict) or evidence.get("status") != "LOCAL_READY"
            or not isinstance(source_id, str) or not isinstance(raw_source_id, str)
            or not source_id or not raw_source_id or source_id == raw_source_id
            or _hash(rows) != evidence.get("technical_series_sha256")
            or not _valid_rows(rows, evidence.get("symbol"), raw=False)
            or not _valid_rows(forward_rows, evidence.get("symbol"), raw=False)
            or [r["date"] for r in rows] != [r["date"] for r in forward_rows]
            or result["forward_series_sha256"] is None):
        result["reasons"] = ["INDEPENDENT_FULL_FORWARD_SCOPE_UNPROVEN"]
        return result
    try:
        errors = [abs(Decimal(str(other[field])) / Decimal(str(row[field])) - 1)
                  for row, other in zip(rows, forward_rows) for field in _PRICE_FIELDS]
        maximum = max(errors)
        numeric_maximum = float(maximum)
    except (DecimalException, OverflowError):
        result["reasons"] = ["FORWARD_ERROR_NOT_FINITE"]
        return result
    if not math.isfinite(numeric_maximum):
        result["reasons"] = ["FORWARD_ERROR_NOT_FINITE"]
        return result
    result.update(maximum_relative_error=numeric_maximum, compared_bars=len(rows),
                  status="LOCAL_MATCH" if maximum <= Decimal("0.005") else "ADJUSTMENT_DATA_GAP",
                  reasons=[] if maximum <= Decimal("0.005") else ["UNEXPLAINED_FORWARD_PRICE_CONFLICT"])
    return result
