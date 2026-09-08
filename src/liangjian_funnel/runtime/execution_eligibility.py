"""Deterministic sell eligibility; a technical exit is never a fill."""
from collections.abc import Mapping
from typing import Any

EXIT_ACTIONS = frozenset({"SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"})


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
