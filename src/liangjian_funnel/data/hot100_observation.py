"""Pure revalidation of full frozen Hot100 evidence, never a source fetch."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
SCHEMA = "eastmoney-guba-hot100/1.0.0"
SOURCE = "EASTMONEY_GUBA_POPULARITY_TOP100"


def _aware(value):
    try:
        stamp = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        return stamp.astimezone(TZ) if stamp.tzinfo is not None and stamp.utcoffset() is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def snapshot_decision_as_of(snapshot: Mapping[str, Any]):
    manifest = snapshot.get("snapshot_manifest")
    return manifest.get("as_of") if isinstance(manifest, Mapping) else None


@dataclass(frozen=True)
class Hot100Observation:
    validation_state: str
    reason_code: str
    records: tuple[Mapping[str, Any], ...]
    trade_date: str | None
    observed_at: str | None
    content_hash: str | None
    declared_available: bool | None

    @property
    def complete(self):
        return self.validation_state == "COMPLETE"

    def membership(self, symbol: str):
        if not self.complete:
            return "UNKNOWN"
        return "VERIFIED_IN_COMPLETE_TOP100" if any(r["symbol"] == symbol for r in self.records) else "VERIFIED_NOT_IN_COMPLETE_TOP100"

    def health(self):
        return {"available": self.complete, "validation_state": self.validation_state,
                "reason_code": self.reason_code, "trade_date": self.trade_date,
                "as_of": self.observed_at, "content_hash": self.content_hash,
                "verified_record_count": len(self.records), "declared_available": self.declared_available,
                "absence_is_not_popularity_evidence": True}


def observe_hot100(value: Any, *, decision_as_of: Any) -> Hot100Observation:
    """Only full records may establish membership; projection declarations cannot.

    Collector content_hash covers canonical records, not the envelope. Hash/PIT
    validation binds local evidence; it does not authenticate the vendor.
    """
    source = value if isinstance(value, Mapping) else {}
    declared = source.get("available") if isinstance(source.get("available"), bool) else None
    day = source.get("trade_date") if isinstance(source.get("trade_date"), str) else None
    observed = _aware(source.get("as_of"))
    supplied_hash = source.get("content_hash")
    pinned_hash = supplied_hash if isinstance(supplied_hash, str) and re.fullmatch(r"[0-9a-f]{64}", supplied_hash) else None
    def result(state, reason, records=()):
        return Hot100Observation(state, reason, records, day,
            observed.isoformat() if observed else None, pinned_hash, declared)
    if declared is not True:
        reason = source.get("reason_code")
        reason = reason if isinstance(reason, str) and re.fullmatch(r"[A-Z0-9_:.-]{1,120}", reason) else "EASTMONEY_HOT100_UNAVAILABLE"
        return result("UNAVAILABLE", reason)
    cutoff = _aware(decision_as_of)
    if cutoff is None or observed is None:
        return result("DATA_LIMITED", "HOT100_PIT_CLOCK_UNPROVEN")
    if day != cutoff.date().isoformat() or observed.date() != cutoff.date():
        return result("INVALID_SOURCE", "HOT100_TRADE_DATE_MISMATCH")
    if observed > cutoff:
        return result("INVALID_SOURCE", "HOT100_OBSERVATION_AFTER_DECISION")
    if source.get("projection_scope") is not None:
        return result("INVALID_SOURCE", "HOT100_FULL_SOURCE_REQUIRED_NOT_PROMPT_PROJECTION")
    if source.get("schema_version") != SCHEMA or source.get("source_id") != SOURCE or source.get("point_in_time") is not True:
        return result("DATA_LIMITED", "HOT100_SOURCE_CONTRACT_UNPROVEN")
    rows = source.get("records")
    if type(source.get("record_count")) is not int or source["record_count"] != 100 or not isinstance(rows, (list, tuple)) or len(rows) != 100:
        return result("INVALID_SOURCE", "HOT100_FULL_100_REQUIRED")
    if any(not isinstance(r, Mapping) for r in rows):
        return result("INVALID_SOURCE", "HOT100_ROW_MALFORMED")
    if any(type(r.get("rank")) is not int for r in rows) or [r["rank"] for r in rows] != list(range(1, 101)):
        return result("INVALID_SOURCE", "HOT100_RANKS_INCOMPLETE")
    symbols = [r.get("symbol") for r in rows]
    if any(not isinstance(s, str) or re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", s) is None for s in symbols) or len(set(symbols)) != 100:
        return result("INVALID_SOURCE", "HOT100_IDENTITIES_INCOMPLETE")
    if pinned_hash is None:
        return result("DATA_LIMITED", "HOT100_RECORDS_HASH_UNPROVEN")
    try:
        canonical = json.dumps([dict(r) for r in rows], ensure_ascii=False, sort_keys=True,
                               separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError):
        return result("INVALID_SOURCE", "HOT100_RECORDS_NOT_STRICT_JSON")
    if hashlib.sha256(canonical).hexdigest() != pinned_hash:
        return result("INVALID_SOURCE", "HOT100_RECORDS_HASH_MISMATCH")
    return result("COMPLETE", "OK", tuple(MappingProxyType(r) for r in json.loads(canonical)))
