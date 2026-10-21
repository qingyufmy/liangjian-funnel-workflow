"""Deterministic sell eligibility; a technical exit is never a fill."""
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

EXIT_ACTIONS = frozenset({"SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"})


def sellable_qty(store: Any, account: str, symbol: str, as_of: datetime) -> int | None:
    """Read this account's dated lots; missing/inconsistent lineage is unknown.

    A position label or its total quantity must never release same-day shares.
    The persisted position cap may only tighten the independently dated lots.
    """
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        return None
    day = as_of.astimezone(ZoneInfo("Asia/Shanghai")).date()
    position = store.get_position(account, symbol)
    if position is None:
        return 0
    eligibility = sell_eligibility(position)
    if eligibility["state"] == "UNKNOWN":
        return None
    lots = store.list_position_lots(account, symbol)
    total = available = 0
    for lot in lots:
        qty = lot.get("remaining_qty")
        if type(qty) is not int or qty < 0:
            return None
        try:
            acquired = date.fromisoformat(lot["acquired_trade_date"])
            released = date.fromisoformat(lot["sellable_from"])
        except (KeyError, TypeError, ValueError):
            return None
        if released <= acquired or acquired > day:
            return None
        total += qty
        if acquired < day and released <= day:
            available += qty
    if total != position["total_qty"]:
        return None
    return min(available, position["sellable_qty"])


def sell_eligibility(position: Mapping[str, Any] | None) -> dict[str, Any]:
    position = position or {}
    total, sellable = position.get("total_qty"), position.get("sellable_qty")
    valid = all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in (total, sellable))
    if not valid or sellable > total:
        return {"state": "UNKNOWN", "reason": "SELLABLE_QUANTITY_UNKNOWN", "total_qty": total, "sellable_qty": None}
    reason = "NO_POSITION" if total == 0 else "BLOCKED_T1" if sellable == 0 else None
    return {"state": "BLOCKED" if reason else "PARTIAL" if sellable < total else "ELIGIBLE",
            "reason": reason, "total_qty": total, "sellable_qty": sellable,
            "locked_qty": total - sellable}


def project_exit_eligibility(result: dict[str, Any], position: Mapping[str, Any] | None) -> dict[str, Any]:
    if result.get("action") not in EXIT_ACTIONS:
        return result
    eligibility = sell_eligibility(position)
    result["execution_eligibility"] = eligibility
    result["technical_action"] = result["action"]
    result["state"] = "EXIT_READY" if eligibility["state"] in {"ELIGIBLE", "PARTIAL"} else "EXIT_PENDING"
    if eligibility["reason"]:
        for key, value in (("reason_codes", eligibility["reason"]), ("unmet_conditions", "SELLABLE_POSITION")):
            result[key] = list(dict.fromkeys([*(result.get(key) or ()), value]))
    return result
