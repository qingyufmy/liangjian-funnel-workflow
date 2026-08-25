"""Deterministic risk authority for the local paper accounts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..data.mootdx import MinuteBar
from .state import RuntimeStore


@dataclass(frozen=True, slots=True)
class RiskDecision:
    allowed: bool
    reason_code: str


class RiskGovernor:
    """Validate portfolio/position invariants independently from the LLM."""

    def __init__(self, store: RuntimeStore, account_id: str):
        self.store = store
        self.account_id = account_id

    def evaluate(self, action: Any, bar: MinuteBar) -> RiskDecision:
        action_name = str(getattr(getattr(action, "action", None), "value", getattr(action, "action", "")))
        symbol = str(getattr(action, "symbol", ""))
        if str(getattr(action, "account_id", "")) != self.account_id:
            return RiskDecision(False, "ACCOUNT_LANE_MISMATCH")
        position = self.store.get_position(self.account_id, symbol)
        plan = self.store.get_position_risk_plan(self.account_id, symbol)
        if action_name in {"BUY", "ADD"}:
            if plan is not None and int(plan.get("unresolved_corporate_action", 0)):
                return RiskDecision(False, "CORPORATE_ACTION_UNRESOLVED")
            stop = getattr(action, "stop_level", None)
            if stop is None or float(stop) >= float(getattr(action, "entry_reference", None) or bar.open):
                return RiskDecision(False, "INVALID_STOP_DIRECTION")
        if action_name == "ADD":
            if position is None or plan is None or plan.get("status") != "ACTIVE":
                return RiskDecision(False, "POSITION_RISK_PLAN_MISSING")
            if int(plan.get("adds_used", 0)) >= int(plan.get("max_adds", 0)):
                return RiskDecision(False, "MAX_ADDS_REACHED")
            if bar.close <= float(position["avg_cost"]):
                return RiskDecision(False, "ADD_REQUIRES_OPEN_PROFIT")
        if action_name in {"SELL", "REDUCE", "FORCED_RISK_EXIT"} and position is None:
            return RiskDecision(False, "NO_POSITION")
        return RiskDecision(True, "OK")


__all__ = ["RiskDecision", "RiskGovernor"]
