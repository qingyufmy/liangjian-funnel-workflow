"""Non-executable publication evidence from the entire existing PIT cache."""
from collections.abc import Mapping
from datetime import datetime, timedelta
import math

from ..pipeline.factors import _canonical_symbol, _daily_bars, _row_datetime
from ..pipeline.local_fact_cache import canonical_json_hash


def _aware(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("ATR_SOURCE_TIME_INVALID")
    return value


def publication_atr_evidence(*, cache, symbol, knowledge_at, last_closed_at, captured_at):
    """Wilder TR14 seeded from ALL locally available point-in-time daily rows.

    No last-N truncation, fresh data acquisition or implicit factor-store
    enablement. Completeness means the requested local PIT window, NOT an
    assertion of listing-history completeness or corporate-action coverage.
    The seed start, row hash/count and clocks remain explicit for later replay.
    """
    limited = {"schema_version":"publication-atr-evidence/1", "status":"DATA_LIMITED",
        "atr14":None, "atr_method":"WILDER_TR14_SEED_MEAN", "adjust":"none",
        "execution_authority":False, "listing_history_completeness":"NOT_CLAIMED"}
    try:
        knowledge_at, last_closed_at, captured_at = map(_aware, (knowledge_at,last_closed_at,captured_at))
        if last_closed_at > knowledge_at or knowledge_at > captured_at or _canonical_symbol(symbol) != symbol:
            raise ValueError("ATR_SOURCE_TIME_OR_IDENTITY_CONFLICT")
        if cache is None:
            raise ValueError("ATR_LOCAL_CACHE_UNAVAILABLE")
        rows = cache.query_daily_bars(symbol, adjust="none", end=last_closed_at+timedelta(microseconds=1),
            as_of=knowledge_at, limit=None, descending=False)
        if len(rows) < 15:
            raise ValueError("ATR_HISTORY_SEED_INCOMPLETE")
        payloads, clocks = [], []
        for row in rows:
            payload = row.get("payload")
            if not isinstance(payload, Mapping) or row.get("symbol") != symbol:
                raise ValueError("ATR_SOURCE_IDENTITY_CONFLICT")
            identities = [payload[key] for key in ("symbol","thscode","ticker","code") if payload.get(key) is not None]
            if not identities or any(_canonical_symbol(item) != symbol for item in identities):
                raise ValueError("ATR_SOURCE_IDENTITY_CONFLICT")
            if row.get("adjust") != "none" or payload.get("adjust", "none") != "none":
                raise ValueError("ATR_ADJUSTMENT_BASIS_CONFLICT")
            fetched, stamp = _aware(row.get("fetched_at")), _aware(row.get("timestamp"))
            end = _row_datetime(payload, daily=True)
            if (end is None or fetched > knowledge_at or end > last_closed_at or stamp.date() != end.date()
                    or payload.get("closed") is False or payload.get("is_closed") is False):
                raise ValueError("ATR_SOURCE_TIME_OR_CLOSED_CONFLICT")
            if row.get("content_hash") != canonical_json_hash(payload):
                raise ValueError("ATR_SOURCE_ROW_HASH_CONFLICT")
            payloads.append(payload)
            clocks.append(fetched)
        bars, reasons = _daily_bars(payloads, symbol, last_closed_at)
        if (reasons or len(bars) != len(rows) or bars[-1].end != last_closed_at
                or [_row_datetime(item,daily=True) for item in payloads] != [bar.end for bar in bars]):
            raise ValueError("ATR_SOURCE_ORDER_OR_LATEST_CLOSE_CONFLICT")
        ranges = [max(bar.high-bar.low, abs(bar.high-bars[i-1].close), abs(bar.low-bars[i-1].close))
                  for i,bar in enumerate(bars) if i]
        atr = sum(ranges[:14])/14
        for value in ranges[14:]:
            atr = (atr*13+value)/14
        if not math.isfinite(atr) or atr <= 0:
            raise ValueError("ATR_VALUE_INVALID")
        result = {**limited, "status":"AVAILABLE", "symbol":symbol, "atr14":atr,
            "knowledge_at":knowledge_at.isoformat(), "captured_at":captured_at.isoformat(),
            "latest_source_observed_at":max(clocks).isoformat(),
            "last_closed_daily_bar_end":last_closed_at.isoformat(),
            "seed_start":bars[0].end.isoformat(), "seed_scope":"ALL_AVAILABLE_PIT_CACHE_ROWS_NO_LIMIT",
            "source_rows_count":len(rows), "source_rows_sha256":canonical_json_hash(rows), "reason_codes":[]}
        result["evidence_sha256"] = canonical_json_hash(result)
        return result
    except Exception as exc:
        allowed = str(exc) if isinstance(exc, ValueError) and str(exc).startswith("ATR_") else "ATR_LOCAL_EVIDENCE_UNAVAILABLE"
        return {**limited, "reason_codes":[allowed]}
