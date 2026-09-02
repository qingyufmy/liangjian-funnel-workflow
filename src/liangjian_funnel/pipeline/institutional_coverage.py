"""Point-in-time institutional research inputs for A1.

The monthly broker-gold dataset remains independently measurable after a run,
but its verified point-in-time rows are also an explicit A1 research route.
Direct entry means research coverage only: it never grants A2/A3/A4 trading
permission and it never bypasses their market, technical or risk checks.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from ..evaluation.broker_gold import BrokerGoldContractError, import_broker_gold


SCHEMA_VERSION = "liangjian-institutional-coverage/1.1.0"


def unavailable_broker_gold_coverage(
    reason_code: str,
    *,
    month: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "available": False,
        "reason_code": reason_code,
        "month": month,
        "record_count": 0,
        "broker_count": 0,
        "source_count": 0,
        "symbol_count": 0,
        "symbols": {},
        "runtime_role": "P1_INSTITUTIONAL_DIRECT_RESEARCH",
        "direct_research_entry": True,
        "direct_approval_forbidden": False,
    }


def load_broker_gold_coverage(
    source_dir: str | Path,
    *,
    as_of: datetime,
) -> dict[str, Any]:
    """Load the current month's strict benchmark file as a research projection.

    ``import_broker_gold`` owns validation, de-duplication and point-in-time
    filtering.  The returned dataset object is deliberately not injected into
    the runtime snapshot; only this immutable T2 projection is exposed.
    """

    month = as_of.strftime("%Y-%m")
    root = Path(source_dir)
    source = next(
        (candidate for candidate in (root / f"{month}.json", root / f"{month}.csv") if candidate.is_file()),
        None,
    )
    if source is None:
        return unavailable_broker_gold_coverage(
            "BROKER_GOLD_COVERAGE_NOT_CONFIGURED",
            month=month,
        )

    try:
        dataset = import_broker_gold(source, as_of=as_of)
    except (BrokerGoldContractError, OSError):
        return unavailable_broker_gold_coverage(
            "BROKER_GOLD_COVERAGE_INVALID",
            month=month,
        )

    grouped: dict[str, list[Any]] = defaultdict(list)
    for record in dataset.records:
        if record.month == month:
            grouped[record.symbol].append(record)

    symbols: dict[str, dict[str, Any]] = {}
    for symbol, records in sorted(grouped.items()):
        brokers = sorted({record.broker for record in records})
        source_refs = sorted({record.source_ref for record in records})
        latest_publish_time = max(
            (record.publish_time for record in records if record.publish_time is not None),
            default=None,
        )
        symbols[symbol] = {
            "symbol": symbol,
            "name": next((record.name for record in records if record.name), None),
            "broker_count": len(brokers),
            "brokers": brokers,
            "source_refs": source_refs,
            "latest_publish_time": latest_publish_time.isoformat() if latest_publish_time else None,
            "evidence_tier": "T2",
            "direct_research_entry": True,
            "direct_approval_forbidden": False,
        }

    eligible_records = [record for record in dataset.records if record.month == month]
    brokers = sorted({record.broker for record in eligible_records})
    source_refs = sorted({record.source_ref for record in eligible_records})
    canonical = json.dumps(symbols, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "schema_version": SCHEMA_VERSION,
        "available": bool(symbols),
        "reason_code": "OK" if symbols else "BROKER_GOLD_COVERAGE_EMPTY",
        "month": month,
        "as_of": as_of.isoformat(),
        "record_count": len(eligible_records),
        "broker_count": len(brokers),
        "source_count": len(source_refs),
        "brokers": brokers,
        # This describes only the verified rows present in the local monthly
        # dataset.  It deliberately does not claim that every brokerage in the
        # market has published, or that a public aggregator exposed every row.
        "coverage_scope": "VERIFIED_INPUT_ROWS",
        "symbol_count": len(symbols),
        "excluded_future_count": len(dataset.excluded_future),
        "duplicate_count": dataset.duplicate_count,
        "content_hash": sha256(canonical.encode("utf-8")).hexdigest(),
        "symbols": symbols,
        "runtime_role": "P1_INSTITUTIONAL_DIRECT_RESEARCH",
        "direct_research_entry": True,
        "direct_approval_forbidden": False,
        "benchmark_evaluation_remains_independent": True,
    }


__all__ = [
    "SCHEMA_VERSION",
    "load_broker_gold_coverage",
    "unavailable_broker_gold_coverage",
]
