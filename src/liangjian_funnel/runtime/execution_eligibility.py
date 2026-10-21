"""Deterministic sell eligibility; a technical exit is never a fill."""
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from liangjian_paper_broker.sellability import sellable_qty as dated_sellable_qty

EXIT_ACTIONS = frozenset({"SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"})


def sellable_qty(store: Any, account: str, symbol: str, as_of: datetime) -> int | None:
    """Read this account's dated lots; missing/inconsistent lineage is unknown.

    A position label or its total quantity must never release same-day shares.
    The position's cached sellable count may be yesterday's zero until the
    broker's day-start task runs. Dated lots, not that cache, own T+1 release.
    Broker reservations remain enforced in the existing admission transaction.
    """
    position = store.get_position(account, symbol)
    if position is None:
        return 0
    if type(position.get("total_qty")) is not int or position["total_qty"]<0:
        return None
    lots = store.list_position_lots(account, symbol)
    normalized = []
    for lot in lots:
        if not lot.get("sellable_from"):
            return None
        normalized.append({"qty": lot.get("remaining_qty"),
            "acquired_trade_date": lot.get("acquired_trade_date"),
            "sellable_from": lot["sellable_from"]})
    return dated_sellable_qty(normalized, as_of=as_of,
        total_qty=position["total_qty"])


def sell_eligibility(position: Mapping[str, Any] | None) -> dict[str, Any]:
    position = position or {}
    total, sellable = position.get("total_qty"), position.get("sellable_qty")
    valid = all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in (total, sellable))
    if not valid or sellable > total:
        return {"state": "UNKNOWN", "reason": "SELLABLE_QUANTITY_UNKNOWN", "total_qty": total, "sellable_qty": None}
    reason = "NO_POSITION" if total == 0 else "T1_LOCKED" if sellable == 0 else None
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
