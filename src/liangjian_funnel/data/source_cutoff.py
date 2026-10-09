"""Explicit dual-cutoff audit; no timestamp repair or acquisition authority.

An explicit closed-session value declaration differs from a provider event
timestamp. The exception applies only to same-session closed values; it never
allows a future provider event or an intraday acquisition to be backdated.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, time
import re
from typing import Any
from zoneinfo import ZoneInfo

SCHEMA = "source-dual-cutoff/1"
TZ = ZoneInfo("Asia/Shanghai")


def _aware(value: datetime | str) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("SOURCE_CUTOFF_TIME_NOT_AWARE")
    return parsed.astimezone(TZ)


@dataclass(frozen=True)
class SourceCutoffs:
    requested_market_cutoff: datetime
    frozen_received_cutoff: datetime

    def __post_init__(self):
        object.__setattr__(self, "requested_market_cutoff", _aware(self.requested_market_cutoff))
        object.__setattr__(self, "frozen_received_cutoff", _aware(self.frozen_received_cutoff))
        if self.frozen_received_cutoff < self.requested_market_cutoff:
            raise ValueError("SOURCE_SEAL_BEFORE_MARKET_CUTOFF")


def inspect_source_cutoff(entry: Mapping[str, Any], *, cutoffs: SourceCutoffs,
                          expected_source_id: str, expected_trade_date: str) -> dict[str, Any]:
    """Validate explicit metadata, independently of hash/date/TTL validation.

    CUTOFF_BOUND certifies the supplied declaration only. Source byte hashes,
    actual provider semantics and ingestion capture must be bound by callers.
    This function never treats a caller-assigned request `as_of`, file mtime,
    or a paired quote date as the provider's fund-flow event timestamp.
    """
    if not isinstance(cutoffs, SourceCutoffs):
        raise ValueError("SOURCE_CUTOFFS_REQUIRED")
    market, sealed = cutoffs.requested_market_cutoff, cutoffs.frozen_received_cutoff
    output = {"schema_version": SCHEMA, "status": "DATA_LIMITED", "usable": False,
              "reason_code": "SOURCE_TIME_METADATA_INVALID", "violated_fields": [],
              "requested_market_cutoff": market.isoformat(), "frozen_received_cutoff": sealed.isoformat(),
              "declared_market_observed_at": None, "effective_market_observed_at": None,
              "ingested_at": None, "acquisition_authenticated": False}
    if not isinstance(entry, Mapping):
        return output
    if not isinstance(expected_source_id, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,128}", expected_source_id) is None:
        output["reason_code"] = "SOURCE_EXPECTED_ID_REQUIRED"
        return output
    if entry.get("source_id") != expected_source_id or entry.get("trade_date") != expected_trade_date:
        output["reason_code"] = "SOURCE_IDENTITY_MISMATCH"
        return output
    if expected_trade_date != market.date().isoformat():
        output["reason_code"] = "SOURCE_REQUEST_TRADE_DATE_MISMATCH"
        return output
    if entry.get("cutoff_contract") != SCHEMA or any(entry.get(key) is None
            for key in ("market_observed_at", "ingested_at")):
        output.update(status="LEGACY_UNVERSIONED", reason_code="LEGACY_UNVERSIONED")
        return output
    try:
        observed, received = _aware(entry["market_observed_at"]), _aware(entry["ingested_at"])
    except (ValueError, TypeError):
        return output
    output.update(declared_market_observed_at=observed.isoformat(), ingested_at=received.isoformat())
    if observed.date().isoformat() != expected_trade_date:
        output["reason_code"] = "SOURCE_OBSERVATION_TRADE_DATE_MISMATCH"
        return output
    if received > sealed:
        output.update(reason_code="SOURCE_CACHE_FROM_FUTURE", violated_fields=["ingested_at"])
        return output
    if type(entry.get("end_of_session_semantics")) is not bool:
        return output
    basis = entry.get("observation_basis")
    effective = observed
    if basis == "CLOSED_SESSION_VALUE":
        proven_closed_session = (entry["end_of_session_semantics"] is True
            and market.timetz().replace(tzinfo=None) == time(15)
            and observed.date() == market.date() and observed >= market)
        if not proven_closed_session:
            output["reason_code"] = "OBSERVED_AT_UNPROVEN"
            return output
        effective = market
    elif basis != "PROVIDER_EVENT_TIMESTAMP":
        output["reason_code"] = "OBSERVED_AT_UNPROVEN"
        return output
    output["effective_market_observed_at"] = effective.isoformat()
    if effective > market:
        output.update(reason_code="SOURCE_CACHE_FROM_FUTURE", violated_fields=["market_observed_at"])
        return output
    if received < observed:
        return output
    output.update(status="CUTOFF_BOUND", usable=True, reason_code="OK")
    return output
