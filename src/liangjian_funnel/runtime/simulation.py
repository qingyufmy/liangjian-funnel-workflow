"""Deterministic, internal-only paper simulation.

The broker in this module has no order client and no network dependency.  A
signal becomes a pending simulation intent only after all deterministic risk,
T+1 and bar checks pass.  The durable fill/account update is one SQLite
transaction, so a persistence failure blocks subsequent actions.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from datetime import date, datetime, time as datetime_time
from enum import StrEnum
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..data.mootdx import MinuteBar, map_symbol
from .state import PersistenceBlockedError, PersistenceError, RuntimeStore
from .risk import RiskGovernor


SHANGHAI = ZoneInfo("Asia/Shanghai")


class SimulationActionType(StrEnum):
    BUY = "BUY"
    ADD = "ADD"
    SELL = "SELL"
    REDUCE = "REDUCE"
    FORCED_RISK_EXIT = "FORCED_RISK_EXIT"
    CANCEL = "CANCEL"


class SimulationStatus(StrEnum):
    FILLED = "FILLED"
    DUPLICATE = "DUPLICATE"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


class SimulationConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    initial_cash: float = Field(default=1_000_000.0, ge=0)
    lot_size: int = Field(default=100, ge=100)
    base_risk_pct: float = Field(default=0.01, gt=0, le=1)
    max_single_position_pct: float = Field(default=0.20, gt=0, le=1)
    max_total_position_pct: float = Field(default=0.95, gt=0, le=1)
    slippage_bps: float = Field(default=10.0, ge=0, le=1_000)
    fee_bps: float = Field(default=1.0, ge=0, le=1_000)
    sell_tax_bps: float = Field(default=5.0, ge=0, le=1_000)
    minimum_fee: float = Field(default=5.0, ge=0)

    @model_validator(mode="after")
    def cap_order(self) -> "SimulationConfig":
        if self.lot_size != 100:
            raise ValueError("A-share simulation lot_size is fixed at 100")
        if self.max_total_position_pct < self.max_single_position_pct:
            raise ValueError("total position cap must not be below single position cap")
        return self


class SimulationAction(BaseModel):
    model_config = ConfigDict(frozen=True)

    account_id: str
    signal_id: str
    symbol: str
    action: SimulationActionType
    signal_bar_end: datetime
    entry_reference: float | None = None
    stop_level: float | None = None
    requested_qty: int | None = Field(default=None, ge=1)
    risk_unit: float = Field(default=1.0, gt=0, le=1)
    plan_id: str | None = None

    @field_validator("symbol", mode="before")
    @classmethod
    def canonical_symbol(cls, value: str) -> str:
        return map_symbol(value).canonical

    @field_validator("action", mode="before")
    @classmethod
    def normalize_action(cls, value: str | SimulationActionType) -> str | SimulationActionType:
        aliases = {
            "BUY_SIGNAL": "BUY",
            "ADD_SIGNAL": "ADD",
            "SELL_SIGNAL": "SELL",
            "REDUCE_SIGNAL": "REDUCE",
        }
        value = aliases.get(str(value), value)
        return value

    @field_validator("signal_bar_end")
    @classmethod
    def aware_signal_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("signal_bar_end must be timezone-aware")
        return value

    @field_validator("entry_reference", "stop_level")
    @classmethod
    def finite_price(cls, value: float | None) -> float | None:
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ValueError("price must be positive and finite")
        return value


class SimulationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: SimulationStatus
    reason_code: str
    account_id: str
    signal_id: str
    symbol: str
    action: str
    qty: int = Field(default=0, ge=0)
    price: float | None = Field(default=None, gt=0)
    fee: float = Field(default=0, ge=0)
    fill: dict[str, Any] | None = None


ACTION_PRIORITY: dict[str, int] = {
    SimulationActionType.FORCED_RISK_EXIT.value: 500,
    SimulationActionType.SELL.value: 400,
    SimulationActionType.REDUCE.value: 400,
    SimulationActionType.CANCEL.value: 300,
    SimulationActionType.ADD.value: 200,
    SimulationActionType.BUY.value: 100,
}


def resolve_action_conflicts(actions: Iterable[SimulationAction | Mapping[str, Any]]) -> tuple[SimulationAction, ...]:
    """Keep only the deterministic winner per account/stock/minute."""

    normalized = tuple(action if isinstance(action, SimulationAction) else SimulationAction.model_validate(action) for action in actions)
    winners: dict[tuple[str, str, str], SimulationAction] = {}
    for action in normalized:
        key = (action.account_id, action.symbol, action.signal_bar_end.isoformat())
        previous = winners.get(key)
        if previous is None or ACTION_PRIORITY[action.action.value] > ACTION_PRIORITY[previous.action.value]:
            winners[key] = action
    return tuple(sorted(winners.values(), key=lambda item: (item.account_id, item.symbol, item.signal_bar_end, -ACTION_PRIORITY[item.action.value])))


def _floor_lot(value: float, lot_size: int) -> int:
    if not math.isfinite(value) or value <= 0:
        return 0
    return int(value) // lot_size * lot_size


def _fee(gross: float, config: SimulationConfig, *, sell: bool) -> float:
    rate = config.fee_bps + (config.sell_tax_bps if sell else 0)
    return max(config.minimum_fee if gross > 0 else 0, gross * rate / 10_000)


class PaperBroker:
    """One isolated virtual account; all fills are local SQLite ledger rows."""

    def __init__(
        self,
        store: RuntimeStore,
        *,
        account_id: str | None = None,
        model: str | None = None,
        config: SimulationConfig | None = None,
    ):
        if not account_id and not model:
            raise ValueError("account_id or model is required")
        self.store = store
        self.config = config or SimulationConfig()
        self.account_id = account_id or f"paper:{model}"
        self.model = model or self.account_id
        self.store.ensure_virtual_account(self.account_id, self.model, self.config.initial_cash)
        self.risk_governor = RiskGovernor(self.store, self.account_id)

    @classmethod
    def for_models(
        cls,
        store: RuntimeStore,
        models: Iterable[str],
        *,
        config: SimulationConfig | None = None,
    ) -> tuple["PaperBroker", ...]:
        return tuple(cls(store, model=model, config=config) for model in models)

    def start_trading_day(self, trade_date: date | None = None) -> int:
        """Release prior-session buys for T+1 selling."""

        started = self.store.start_account_trading_day(
            self.account_id,
            trade_date or datetime.now(SHANGHAI).date(),
        )
        return 1 if started else 0

    def calculate_quantity(
        self,
        *,
        symbol: str | None = None,
        entry_reference: float,
        stop_level: float,
        risk_unit: float = 1.0,
        mark_price: float | None = None,
        requested_qty: int | None = None,
    ) -> tuple[int, str]:
        account = self.store.get_account(self.account_id)
        if account is None:
            return 0, "ACCOUNT_NOT_FOUND"
        if not all(math.isfinite(float(value)) for value in (entry_reference, stop_level)):
            return 0, "INVALID_PRICE"
        distance = abs(entry_reference - stop_level)
        if distance <= 0:
            return 0, "INVALID_STOP_DISTANCE"
        if stop_level >= entry_reference:
            return 0, "INVALID_STOP_DIRECTION"
        unit = float(risk_unit)
        if not (0 < unit <= 1):
            return 0, "INVALID_RISK_UNIT"
        fill_reference = float(mark_price or entry_reference)
        if fill_reference <= 0:
            return 0, "INVALID_PRICE"
        risk_cash = float(account["equity"]) * self.config.base_risk_pct * unit
        qty = _floor_lot(risk_cash / distance, self.config.lot_size)
        if requested_qty is not None:
            qty = min(qty, _floor_lot(float(requested_qty), self.config.lot_size))
        # Use an adverse buy estimate for all hard caps, then check exact fill
        # accounting again before committing.
        estimate = fill_reference * (1 + self.config.slippage_bps / 10_000)
        if estimate <= 0:
            return 0, "INVALID_PRICE"
        qty = min(qty, _floor_lot(float(account["cash"]) / estimate, self.config.lot_size))
        current_position = self.store.get_position(self.account_id, symbol) if symbol else None
        current_total = 0.0
        for existing in self.store.list_positions(self.account_id):
            mark = self.store.get_market_mark(self.account_id, str(existing["symbol"]))
            existing_price = float(mark["price"]) if mark is not None else float(existing["avg_cost"])
            current_total += float(existing["total_qty"]) * existing_price
        same_value = (
            float(current_position["total_qty"])
            * (
                float(self.store.get_market_mark(self.account_id, str(current_position["symbol"]))["price"])
                if self.store.get_market_mark(self.account_id, str(current_position["symbol"])) is not None
                else fill_reference
            )
            if current_position is not None
            else 0.0
        )
        total_room = max(0.0, float(account["equity"]) * self.config.max_total_position_pct - current_total)
        single_room = float(account["equity"]) * self.config.max_single_position_pct - same_value
        qty = min(qty, _floor_lot(total_room / estimate, self.config.lot_size), _floor_lot(single_room / estimate, self.config.lot_size))
        if qty <= 0:
            return 0, "POSITION_OR_CASH_CAP"
        return qty, "OK"

    def apply(self, action: SimulationAction | Mapping[str, Any], bar: MinuteBar) -> SimulationResult:
        """Attempt a full fill on the next complete 1m bar."""

        parsed = action if isinstance(action, SimulationAction) else SimulationAction.model_validate(action)
        if parsed.account_id != self.account_id:
            return self._blocked(parsed, "ACCOUNT_LANE_MISMATCH")
        try:
            self.store.assert_writable()
            decision = self.risk_governor.evaluate(parsed, bar)
        except (PersistenceError, PersistenceBlockedError):
            return self._blocked(parsed, "PERSISTENCE_FAILED")
        if not decision.allowed:
            return self._blocked(parsed, decision.reason_code)
        intent_key = f"{parsed.account_id}:{parsed.signal_id}:{parsed.action.value}"
        try:
            existing = self.store.get_fill_by_intent_key(intent_key)
        except (PersistenceError, PersistenceBlockedError):
            return self._blocked(parsed, "PERSISTENCE_FAILED")
        if existing is not None:
            return SimulationResult(
                status=SimulationStatus.DUPLICATE,
                reason_code="IDEMPOTENT_REPLAY",
                account_id=parsed.account_id,
                signal_id=parsed.signal_id,
                symbol=parsed.symbol,
                action=parsed.action.value,
                qty=int(existing["qty"]),
                price=float(existing["price"]),
                fee=float(existing["fee"]),
                fill=existing,
            )
        if parsed.action is SimulationActionType.CANCEL:
            return SimulationResult(
                status=SimulationStatus.CANCELLED,
                reason_code="CANCELLED",
                account_id=parsed.account_id,
                signal_id=parsed.signal_id,
                symbol=parsed.symbol,
                action=parsed.action.value,
            )
        if bar.interval != "1m":
            return self._blocked(parsed, "BAR_INTERVAL_INVALID")
        if bar.bar_end <= parsed.signal_bar_end:
            return self._blocked(parsed, "NEXT_COMPLETE_BAR_REQUIRED")
        if bar.volume <= 0 or bar.high <= bar.low:
            return self._blocked(parsed, "BAR_NOT_EXECUTABLE")
        self.store.upsert_market_mark(self.account_id, parsed.symbol, bar.close, bar.bar_end)
        self.store.mark_account_to_market(self.account_id)
        if parsed.action in {SimulationActionType.BUY, SimulationActionType.ADD} and (
            bar.bar_end.time().replace(tzinfo=None) >= datetime_time(14, 45)
        ):
            return self._blocked(parsed, "BUY_AFTER_CLOSE")

        account = self.store.get_account(self.account_id)
        if account is None or account["status"] != "ACTIVE":
            return self._blocked(parsed, "ACCOUNT_UNAVAILABLE")
        position = self.store.get_position(self.account_id, parsed.symbol)
        fill_price = self._adverse_price(parsed, bar)
        if fill_price is None:
            return self._blocked(parsed, "PRICE_OUTSIDE_BAR")

        if parsed.action in {SimulationActionType.BUY, SimulationActionType.ADD}:
            if parsed.stop_level is None:
                return self._blocked(parsed, "STOP_LEVEL_REQUIRED")
            qty, reason = self.calculate_quantity(
                symbol=parsed.symbol,
                entry_reference=float(parsed.entry_reference or bar.open),
                stop_level=float(parsed.stop_level),
                risk_unit=parsed.risk_unit,
                mark_price=fill_price,
                requested_qty=parsed.requested_qty,
            )
            if qty <= 0:
                return self._blocked(parsed, reason)
            if parsed.action is SimulationActionType.BUY and position is not None and int(position["total_qty"]) > 0:
                return self._blocked(parsed, "POSITION_ALREADY_OPEN")
            if parsed.action is SimulationActionType.ADD and (position is None or int(position["total_qty"]) == 0):
                return self._blocked(parsed, "ADD_WITHOUT_POSITION")
            gross = fill_price * qty
            fee = _fee(gross, self.config, sell=False)
            if gross + fee > float(account["cash"]):
                return self._blocked(parsed, "INSUFFICIENT_CASH")
            old_qty = int(position["total_qty"]) if position else 0
            old_cost = float(position["avg_cost"]) if position else 0.0
            total_qty = old_qty + qty
            avg_cost = ((old_qty * old_cost) + gross + fee) / total_qty
            cash_after = float(account["cash"]) - gross - fee
            position_payload = {
                "total_qty": total_qty,
                "sellable_qty": int(position["sellable_qty"]) if position else 0,
                "avg_cost": avg_cost,
                "stop_level": parsed.stop_level,
                "plan_id": parsed.plan_id,
            }
        else:
            if position is None or int(position["total_qty"]) <= 0:
                return self._blocked(parsed, "NO_POSITION")
            sellable = int(position["sellable_qty"])
            if sellable <= 0:
                return self._blocked(parsed, "BLOCKED_T1")
            requested = parsed.requested_qty
            if requested is None:
                requested = sellable if parsed.action in {SimulationActionType.SELL, SimulationActionType.FORCED_RISK_EXIT} else max(
                    self.config.lot_size,
                    _floor_lot(sellable / 2, self.config.lot_size),
                )
            qty = _floor_lot(float(requested), self.config.lot_size)
            if qty <= 0:
                return self._blocked(parsed, "INVALID_SELL_QTY")
            if qty > sellable:
                return self._blocked(parsed, "BLOCKED_T1")
            gross = fill_price * qty
            fee = _fee(gross, self.config, sell=True)
            cash_after = float(account["cash"]) + gross - fee
            remaining = int(position["total_qty"]) - qty
            position_payload = None if remaining == 0 else {
                "total_qty": remaining,
                "sellable_qty": sellable - qty,
                "avg_cost": float(position["avg_cost"]),
                "stop_level": position["stop_level"],
                "plan_id": position["plan_id"],
            }

        equity_after = cash_after
        for existing in self.store.list_positions(self.account_id):
            qty_existing = int(existing["total_qty"])
            if existing["symbol"] == parsed.symbol:
                qty_existing = int(position_payload["total_qty"]) if position_payload else 0
            mark = self.store.get_market_mark(self.account_id, str(existing["symbol"]))
            mark_price = bar.close if existing["symbol"] == parsed.symbol else (
                float(mark["price"]) if mark is not None else float(existing["avg_cost"])
            )
            equity_after += qty_existing * mark_price
        if position is None and position_payload is not None:
            equity_after += int(position_payload["total_qty"]) * bar.close

        try:
            fill, created = self.store.commit_fill(
                intent_id=f"intent:{intent_key}",
                intent_key=intent_key,
                account_id=parsed.account_id,
                signal_id=parsed.signal_id,
                symbol=parsed.symbol,
                action=parsed.action.value,
                qty=qty,
                price=fill_price,
                fee=fee,
                bar_end=bar.bar_end,
                cash_after=cash_after,
                equity_after=equity_after,
                position=position_payload,
                stop_level=parsed.stop_level,
                plan_id=parsed.plan_id,
            )
        except (PersistenceError, PersistenceBlockedError):
            return self._blocked(parsed, "PERSISTENCE_FAILED")
        return SimulationResult(
            status=SimulationStatus.FILLED if created else SimulationStatus.DUPLICATE,
            reason_code="FILLED" if created else "IDEMPOTENT_REPLAY",
            account_id=parsed.account_id,
            signal_id=parsed.signal_id,
            symbol=parsed.symbol,
            action=parsed.action.value,
            qty=qty if created else int(fill["qty"]),
            price=fill_price if created else float(fill["price"]),
            fee=fee if created else float(fill["fee"]),
            fill=fill,
        )

    submit = apply
    execute = apply

    def _adverse_price(self, action: SimulationAction, bar: MinuteBar) -> float | None:
        reference = float(action.entry_reference or bar.open)
        if not math.isfinite(reference) or reference <= 0:
            return None
        slippage = self.config.slippage_bps / 10_000
        if action.action in {SimulationActionType.BUY, SimulationActionType.ADD}:
            proposed = reference * (1 + slippage)
            return proposed if bar.low <= proposed <= bar.high else None
        proposed = reference * (1 - slippage)
        return proposed if bar.low <= proposed <= bar.high else None

    @staticmethod
    def _blocked(action: SimulationAction, reason: str) -> SimulationResult:
        return SimulationResult(
            status=SimulationStatus.BLOCKED,
            reason_code=reason,
            account_id=action.account_id,
            signal_id=action.signal_id,
            symbol=action.symbol,
            action=action.action.value,
        )


SimulationEngine = PaperBroker
SimulationBroker = PaperBroker
PaperSimulation = PaperBroker


__all__ = [
    "ACTION_PRIORITY",
    "PaperBroker",
    "PaperSimulation",
    "SimulationAction",
    "SimulationActionType",
    "SimulationConfig",
    "SimulationEngine",
    "SimulationBroker",
    "SimulationResult",
    "SimulationStatus",
    "resolve_action_conflicts",
]
