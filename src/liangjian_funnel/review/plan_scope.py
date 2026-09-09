"""Separate session plans, prior inventory references and retired versions."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Mapping, Sequence


def _stamp(value: Any, cutoff: datetime) -> datetime | None:
    try:
        stamp = datetime.fromisoformat(str(value))
        return (stamp if stamp.tzinfo else stamp.replace(tzinfo=cutoff.tzinfo)).astimezone(cutoff.tzinfo)
    except (TypeError, ValueError):
        return None


def select_review_plans(
    rows: Sequence[Mapping[str, Any]], *, cutoff: datetime,
    observed_ids: set[str], carryover_ids: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Unknown retirement times are retained; never infer retirement by symbol."""
    selected, carryover, retired = [], [], []
    day = cutoff.date()
    for row in rows:
        plan_id = str(row.get("plan_id") or "")
        raw = row.get("payload_json")
        try:
            payload = json.loads(raw) if isinstance(raw, str) else raw if isinstance(raw, Mapping) else row
        except (TypeError, ValueError):
            payload = {}
        if not isinstance(payload, Mapping):
            payload = {}
        start, end = _stamp(row.get("valid_from"), cutoff), _stamp(row.get("expires_at"), cutoff)
        updated, created = _stamp(row.get("updated_at"), cutoff), _stamp(row.get("created_at"), cutoff)
        target = str(payload.get("target_trade_date") or "")
        scheduled = target == day.isoformat() if target else bool(
            ((start and start.date() == day) or (end and end.date() == day))
            and (start is None or start.date() <= day) and (end is None or day <= end.date()))
        if created and created > cutoff and plan_id not in observed_ids:
            continue
        prior_retired = row.get("status") == "INVALIDATED" and updated is not None and updated.date() < day
        if prior_retired and plan_id not in observed_ids:
            if plan_id in carryover_ids:
                carryover.append(dict(row))
            elif scheduled:
                retired.append({"plan_id": plan_id, "source_run_id": payload.get("source_run_id"),
                                "symbol": row.get("symbol"), "retired_at": row.get("updated_at"),
                                "reason_code": "INVALIDATED_BEFORE_SESSION"})
            continue
        if scheduled:
            selected.append(dict(row))
        elif plan_id in carryover_ids:
            carryover.append(dict(row))
        elif plan_id in observed_ids:
            selected.append(dict(row))
    return selected, carryover, retired


def carryover_evidence(rows: Sequence[Mapping[str, Any]], cutoff: datetime) -> list[dict[str, Any]]:
    """Keep prior inventory references without using a later exit as a noon fact."""
    result = []
    for row in rows:
        entry, exit_at = _stamp(row.get("entry_time"), cutoff), _stamp(row.get("exit_time"), cutoff)
        if not entry or entry.date() >= cutoff.date() or (exit_at and exit_at.date() < cutoff.date()):
            continue
        value = {"evidence_id": f"A4:CARRYOVER:{row.get('lifecycle_id')}",
                 **{key: row.get(key) for key in ("lifecycle_id", "plan_id", "symbol", "status", "entry_time",
                    "entry_price", "exit_time", "exit_reason", "remaining_qty", "net_return", "updated_at")}}
        updated = _stamp(row.get("updated_at"), cutoff)
        if (updated and updated > cutoff) or (exit_at and exit_at > cutoff):
            # A mutable lifecycle row cannot prove its historical quantity or
            # exit intent. Freeze only the pre-cutoff identity/entry facts.
            value.update(status="AS_OF_STATE_UNAVAILABLE", exit_time=None, exit_reason=None,
                         remaining_qty=None, net_return=None,
                         data_limitation="LIFECYCLE_UPDATED_AFTER_CUTOFF")
        result.append(value)
    return result
