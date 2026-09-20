"""Deterministic, internal-only paper simulation.

The broker in this module has no order client and no network dependency.  A
signal becomes a pending simulation intent only after all deterministic risk,
T+1 and bar checks pass.  The durable fill/account update is one SQLite
transaction, so a persistence failure blocks subsequent actions.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..data.mootdx import MinuteBar, map_symbol
from .state import PersistenceBlockedError, PersistenceError, RuntimeStore, StateTransitionError
from .risk import RiskGovernor
from .stock_trading_rules import stock_trading_rules
from .execution_eligibility import sell_eligibility
from .execution_accounting import FeeSchedule, OrderFeeLedger


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
    max_portfolio_open_risk_pct: float | None = Field(default=None, gt=0, le=1)
    max_theme_position_pct: float | None = Field(default=None, gt=0, le=1)
    slippage_bps: float = Field(default=10.0, ge=0, le=1_000)
    fee_bps: float = Field(default=1.0, ge=0, le=1_000)
    sell_tax_bps: float = Field(default=5.0, ge=0, le=1_000)
    other_fee_bps: float = Field(default=0.0, ge=0, le=1_000)
    minimum_fee: float = Field(default=5.0, ge=0)
    max_volume_participation: float = Field(default=0.10, gt=0, le=1)
    fee_model_version: str = "paper-config/2"
    fill_model_version: str = "next-complete-minute-capacity/2"

    @model_validator(mode="after")
    def cap_order(self) -> "SimulationConfig":
        if self.lot_size != 100:
            raise ValueError("A-share simulation lot_size is fixed at 100")
        if self.max_total_position_pct < self.max_single_position_pct:
            raise ValueError("total position cap must not be below single position cap")
        return self

    def fee_schedule(self) -> FeeSchedule:
        return FeeSchedule(
            commission_bps=Decimal(str(self.fee_bps)),
            minimum_commission=Decimal(str(self.minimum_fee)),
            sell_tax_bps=Decimal(str(self.sell_tax_bps)),
            other_fee_bps=Decimal(str(self.other_fee_bps)),
            version=self.fee_model_version,
        )


class SimulationAction(BaseModel):
    model_config = ConfigDict(frozen=True)

    account_id: str
    signal_id: str
    replay_run_id: str | None = None
    decision_id: str | None = None
    trigger_episode_id: str | None = None
    symbol: str
    action: SimulationActionType
    signal_bar_end: datetime
    data_available_at: datetime | None = None
    deterministic_decided_at: datetime | None = None
    review_completed_at: datetime | None = None
    order_created_at: datetime | None = None
    eligible_from: datetime | None = None
    expire_at: datetime | None = None
    entry_reference: float | None = None
    stop_level: float | None = None
    stop_basis: str | None = None
    requested_qty: int | None = Field(default=None, ge=1)
    risk_unit: float = Field(default=1.0, gt=0, le=1)
    plan_id: str | None = None
    risk_reservation_id: str | None = None
    primary_theme_id: str = "UNKNOWN"
    candidate_sources: tuple[str, ...] = ()
    strategy_identity: str | None = None
    order_revision: int = Field(default=1, ge=1)
    fee_model_version: str = "legacy-paper-fee/1"
    fill_model_version: str = "legacy-next-minute/1"
    order_type: str = "LEGACY_REFERENCE"
    limit_price: float | None = None

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

    @field_validator(
        "signal_bar_end",
        "data_available_at",
        "deterministic_decided_at",
        "review_completed_at",
        "order_created_at",
        "eligible_from",
        "expire_at",
    )
    @classmethod
    def aware_signal_time(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("execution timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def valid_clock_order(self) -> "SimulationAction":
        known = [
            self.signal_bar_end,
            self.data_available_at,
            self.deterministic_decided_at,
            self.review_completed_at,
            self.order_created_at,
        ]
        latest_known = max(value for value in known if value is not None)
        if self.eligible_from is not None and self.eligible_from < latest_known:
            raise ValueError("eligible_from cannot precede required information")
        if self.expire_at is not None and self.expire_at < (self.eligible_from or latest_known):
            raise ValueError("expire_at cannot precede eligibility")
        return self

    def effective_eligible_from(self) -> datetime:
        values = [
            self.signal_bar_end,
            self.data_available_at,
            self.deterministic_decided_at,
            self.review_completed_at,
            self.order_created_at,
            self.eligible_from,
        ]
        return max(value for value in values if value is not None)

    @field_validator("entry_reference", "stop_level", "limit_price")
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
    remaining_qty: int = Field(default=0, ge=0)
    fee_components: dict[str, Any] | None = None
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


def _first_complete_bar_end(value: datetime) -> datetime | None:
    """Return the first session minute wholly after an information timestamp."""

    local = value.astimezone(SHANGHAI)
    day = local.date()
    clock = local.time().replace(tzinfo=None)
    morning_open = datetime.combine(day, datetime_time(9, 30), SHANGHAI)
    morning_close = datetime.combine(day, datetime_time(11, 30), SHANGHAI)
    afternoon_open = datetime.combine(day, datetime_time(13, 0), SHANGHAI)
    afternoon_close = datetime.combine(day, datetime_time(15, 0), SHANGHAI)
    if local < morning_open:
        return morning_open + timedelta(minutes=1)
    if local < morning_close:
        floor = local.replace(second=0, microsecond=0)
        candidate = floor + timedelta(minutes=1 if local == floor else 2)
        return candidate if candidate <= morning_close else afternoon_open + timedelta(minutes=1)
    if local < afternoon_open:
        return afternoon_open + timedelta(minutes=1)
    if local < afternoon_close:
        floor = local.replace(second=0, microsecond=0)
        candidate = floor + timedelta(minutes=1 if local == floor else 2)
        return candidate if candidate <= afternoon_close else None
    return None


def _fee(gross: float, config: SimulationConfig, *, sell: bool) -> float:
    """Compatibility projection; new ledger rows retain every component."""

    charge = OrderFeeLedger(config.fee_schedule(), side="SELL" if sell else "BUY").charge(
        Decimal(str(gross)), final=True,
    )
    return float(charge.total)


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

    def _frozen_fee_schedule(self, action: SimulationAction) -> FeeSchedule:
        configured = self.config.fee_schedule()
        version = action.fee_model_version or configured.version
        return FeeSchedule(
            commission_bps=configured.commission_bps,
            minimum_commission=configured.minimum_commission,
            sell_tax_bps=configured.sell_tax_bps,
            other_fee_bps=configured.other_fee_bps,
            version=version,
        )

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
        rules = stock_trading_rules(symbol or "600000")
        qty = rules.floor_buy(risk_cash / distance)
        if requested_qty is not None:
            qty = min(qty, rules.floor_buy(float(requested_qty)))
        # Use an adverse buy estimate for all hard caps, then check exact fill
        # accounting again before committing.
        estimate = fill_reference * (1 + self.config.slippage_bps / 10_000)
        if estimate <= 0:
            return 0, "INVALID_PRICE"
        qty = min(qty, rules.floor_buy(float(account["cash"]) / estimate))
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
        qty = min(qty, rules.floor_buy(total_room / estimate), rules.floor_buy(single_room / estimate))
        if qty <= 0:
            return 0, "POSITION_OR_CASH_CAP"
        return qty, "OK"

    def apply(self, action: SimulationAction | Mapping[str, Any], bar: MinuteBar) -> SimulationResult:
        """Attempt a full fill on the next complete 1m bar."""

        parsed = action if isinstance(action, SimulationAction) else SimulationAction.model_validate(action)
        if parsed.account_id != self.account_id:
            return self._blocked(parsed, "ACCOUNT_LANE_MISMATCH")
        if parsed.order_type not in {"LEGACY_REFERENCE", "LIMIT"}:
            return self._blocked(parsed, "ENTRY_CONTRACT_INVALID")
        if parsed.order_type == "LIMIT" and parsed.limit_price is None:
            return self._blocked(parsed, "ENTRY_CONTRACT_INVALID")
        if bar.evidence_kind != "MARKET_BAR" or str(bar.source_id).endswith(":RISK_ONLY"):
            return self._blocked(parsed, "FILL_EVIDENCE_INVALID")
        try:
            self.store.assert_writable()
            decision = self.risk_governor.evaluate(parsed, bar)
        except (PersistenceError, PersistenceBlockedError, StateTransitionError):
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
        try:
            rules = stock_trading_rules(parsed.symbol)
        except ValueError:
            return self._blocked(parsed, "UNSUPPORTED_SECURITY_RULES")
        clock = bar.bar_end.astimezone(SHANGHAI).time().replace(tzinfo=None)
        if not (datetime_time(9, 30) < clock <= datetime_time(11, 30) or datetime_time(13) < clock <= datetime_time(15)):
            return self._blocked(parsed, "OUTSIDE_TRADING_SESSION")
        if map_symbol(bar.symbol).canonical != parsed.symbol:
            return self._blocked(parsed, "BAR_SYMBOL_MISMATCH")
        eligible_bar_end = _first_complete_bar_end(parsed.effective_eligible_from())
        if eligible_bar_end is None:
            return self._blocked(parsed, "ORDER_NO_ELIGIBLE_SESSION")
        if parsed.expire_at is not None and bar.bar_end > parsed.expire_at:
            return self._blocked(parsed, "ORDER_EXPIRED")
        if bar.bar_end < eligible_bar_end:
            return self._blocked(parsed, "ORDER_NOT_YET_ELIGIBLE")
        try:
            self.start_trading_day(bar.bar_end.astimezone(SHANGHAI).date())
        except (ValueError, RuntimeError) as exc:
            return self._blocked(parsed, str(exc) if str(exc).isupper() else "TRADING_DAY_UNAVAILABLE")
        if bar.bar_end <= parsed.signal_bar_end:
            return self._blocked(parsed, "NEXT_COMPLETE_BAR_REQUIRED")
        if parsed.order_type == "LIMIT" and parsed.action in {SimulationActionType.BUY, SimulationActionType.ADD}:
            if bar.bar_end != eligible_bar_end:
                return self._blocked(parsed, "ENTRY_NEXT_BAR_MISSED")
        capacity_model = parsed.fill_model_version != "legacy-next-minute/1"
        if capacity_model and bar.volume_unit != "shares":
            return self._blocked(parsed, "FILL_VOLUME_UNIT_UNCONFIRMED")
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
        reservation_id: str | None = None
        fill_price = self._adverse_price(parsed, bar)
        if fill_price is None:
            if parsed.order_type == "LIMIT" and parsed.action in {SimulationActionType.BUY, SimulationActionType.ADD}:
                return self._blocked(parsed, "LIMIT_TOUCH_WITHOUT_FILL_EVIDENCE"
                                     if bar.low == parsed.limit_price else "LIMIT_NOT_REACHED")
            return self._blocked(parsed, "PRICE_OUTSIDE_BAR")

        if parsed.action in {SimulationActionType.BUY, SimulationActionType.ADD}:
            if parsed.stop_level is None:
                return self._blocked(parsed, "STOP_LEVEL_REQUIRED")
            if parsed.order_type == "LIMIT" and fill_price <= parsed.stop_level:
                return self._blocked(parsed, "ENTRY_AT_OR_BELOW_STOP")
            qty, reason = self.calculate_quantity(
                symbol=parsed.symbol,
                entry_reference=float(parsed.entry_reference or bar.open),
                stop_level=float(parsed.stop_level),
                risk_unit=parsed.risk_unit,
                # Quantity is frozen from the last known decision reference;
                # the later fill bar can shrink execution but never enlarge it.
                mark_price=float(parsed.entry_reference or bar.open),
                requested_qty=parsed.requested_qty,
            )
            if qty <= 0:
                return self._blocked(parsed, reason)
            if parsed.action is SimulationActionType.BUY and position is not None and int(position["total_qty"]) > 0:
                return self._blocked(parsed, "POSITION_ALREADY_OPEN")
            if parsed.action is SimulationActionType.ADD and (position is None or int(position["total_qty"]) == 0):
                return self._blocked(parsed, "ADD_WITHOUT_POSITION")
            frozen_qty = qty
            capacity = (
                rules.floor_buy(float(bar.volume) * self.config.max_volume_participation)
                if capacity_model else frozen_qty
            )
            qty = min(frozen_qty, capacity)
            if qty <= 0:
                return self._blocked(parsed, "INSUFFICIENT_WINDOW_CAPACITY")
            remaining_qty = frozen_qty - qty
            gross = fill_price * qty
            fee_charge = OrderFeeLedger(self._frozen_fee_schedule(parsed), side="BUY").charge(
                Decimal(str(gross)), final=True,
            )
            fee = float(fee_charge.total)
            if gross + fee > float(account["cash"]):
                return self._blocked(parsed, "INSUFFICIENT_CASH")
            reservation_id = parsed.risk_reservation_id or f"risk:{intent_key}:r{parsed.order_revision}"
            reserve_reference = float(parsed.entry_reference or fill_price)
            reserve_gross = reserve_reference * frozen_qty
            reserve_fee = OrderFeeLedger(self._frozen_fee_schedule(parsed), side="BUY").charge(
                Decimal(str(reserve_gross)), final=True,
            ).total
            try:
                self.store.reserve_simulation_order(
                    reservation_id=reservation_id,
                    order_identity=f"{intent_key}:r{parsed.order_revision}",
                    account_id=parsed.account_id,
                    symbol=parsed.symbol,
                    primary_theme_id=parsed.primary_theme_id,
                    requested_qty=frozen_qty,
                    reserved_cash=float(Decimal(str(reserve_gross)) + reserve_fee),
                    reserved_risk=abs(reserve_reference - float(parsed.stop_level)) * frozen_qty,
                    max_total_value=float(account["equity"]) * self.config.max_total_position_pct,
                    max_symbol_value=float(account["equity"]) * self.config.max_single_position_pct,
                    max_open_risk=(
                        float(account["equity"]) * self.config.max_portfolio_open_risk_pct
                        if self.config.max_portfolio_open_risk_pct is not None else None
                    ),
                    max_theme_value=(
                        float(account["equity"]) * self.config.max_theme_position_pct
                        if self.config.max_theme_position_pct is not None else None
                    ),
                    metadata={
                        "candidate_sources": list(parsed.candidate_sources),
                        "strategy_identity": parsed.strategy_identity,
                        "order_revision": parsed.order_revision,
                        "theoretical_stop_loss": abs(reserve_reference - float(parsed.stop_level)) * frozen_qty,
                        "stress_loss": "UNKNOWN_GAP_AND_LIQUIDITY_NOT_CAPPED",
                    },
                )
            except StateTransitionError as exc:
                return self._blocked(parsed, str(exc))
            except (PersistenceError, PersistenceBlockedError):
                return self._blocked(parsed, "PERSISTENCE_FAILED")
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
            eligibility = sell_eligibility(position)
            if eligibility["reason"]:
                return self._blocked(parsed, eligibility["reason"])
            sellable = int(eligibility["sellable_qty"])
            requested = parsed.requested_qty
            if requested is None:
                requested = sellable if parsed.action in {SimulationActionType.SELL, SimulationActionType.FORCED_RISK_EXIT} else max(
                    min(rules.minimum, sellable),
                    rules.floor_buy(sellable / 2),
                )
            qty = rules.sell_quantity(int(requested), sellable)
            if qty <= 0:
                return self._blocked(parsed, "INVALID_SELL_QTY")
            if qty > sellable:
                return self._blocked(parsed, "BLOCKED_T1")
            frozen_qty = qty
            capacity_raw = min(int(float(bar.volume) * self.config.max_volume_participation), sellable)
            capacity = rules.sell_quantity(capacity_raw, sellable) if capacity_model else frozen_qty
            qty = min(frozen_qty, capacity)
            if qty <= 0:
                return self._blocked(parsed, "INSUFFICIENT_WINDOW_CAPACITY")
            remaining_qty = frozen_qty - qty
            gross = fill_price * qty
            fee_charge = OrderFeeLedger(self._frozen_fee_schedule(parsed), side="SELL").charge(
                Decimal(str(gross)), final=True,
            )
            fee = float(fee_charge.total)
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
                fee_components=fee_charge.as_dict(),
                bar_end=bar.bar_end,
                cash_after=cash_after,
                equity_after=equity_after,
                position=position_payload,
                stop_level=parsed.stop_level,
                plan_id=parsed.plan_id,
                requested_qty=frozen_qty,
                remaining_qty=remaining_qty,
                intent_status="FILLED" if remaining_qty == 0 else "PARTIALLY_FILLED_EXPIRED",
                reserved_cash=0.0,
                order_metadata={
                    "decision_id": parsed.decision_id,
                    "replay_run_id": parsed.replay_run_id,
                    "trigger_episode_id": parsed.trigger_episode_id,
                    "plan_id": parsed.plan_id,
                    "signal_bar_end": parsed.signal_bar_end.isoformat(),
                    "data_available_at": parsed.data_available_at.isoformat() if parsed.data_available_at else None,
                    "deterministic_decided_at": parsed.deterministic_decided_at.isoformat() if parsed.deterministic_decided_at else None,
                    "review_completed_at": parsed.review_completed_at.isoformat() if parsed.review_completed_at else None,
                    "order_created_at": parsed.order_created_at.isoformat() if parsed.order_created_at else None,
                    "eligible_from": parsed.effective_eligible_from().isoformat(),
                    "expire_at": parsed.expire_at.isoformat() if parsed.expire_at else None,
                    "order_type": parsed.order_type,
                    "limit_price": parsed.limit_price,
                    "stop_basis": parsed.stop_basis,
                    "requested_qty": frozen_qty,
                    "risk_reservation_id": parsed.risk_reservation_id,
                    "fee_model_version": parsed.fee_model_version or self.config.fee_model_version,
                    "fill_model_version": parsed.fill_model_version or self.config.fill_model_version,
                    "fill_evidence_kind": bar.evidence_kind,
                    "fill_evidence_source": bar.source_id,
                    "primary_theme_id": parsed.primary_theme_id,
                    "candidate_sources": list(parsed.candidate_sources),
                    "strategy_identity": parsed.strategy_identity,
                    "order_revision": parsed.order_revision,
                },
                reservation_id=reservation_id,
                primary_theme_id=parsed.primary_theme_id,
                strategy_identity=parsed.strategy_identity,
                exit_rules={
                    "stop_basis": parsed.stop_basis,
                    "stop_level": parsed.stop_level,
                    "forced_exit_llm_veto_allowed": False,
                },
            )
        except StateTransitionError as exc:
            if reservation_id is not None:
                self.store.release_risk_reservation(reservation_id, str(exc))
            return self._blocked(parsed, str(exc))
        except (PersistenceError, PersistenceBlockedError):
            return self._blocked(parsed, "PERSISTENCE_FAILED")
        return SimulationResult(
            status=SimulationStatus.FILLED if created else SimulationStatus.DUPLICATE,
            reason_code=("PARTIALLY_FILLED" if created and remaining_qty else "FILLED") if created else "IDEMPOTENT_REPLAY",
            account_id=parsed.account_id,
            signal_id=parsed.signal_id,
            symbol=parsed.symbol,
            action=parsed.action.value,
            qty=qty if created else int(fill["qty"]),
            price=fill_price if created else float(fill["price"]),
            fee=fee if created else float(fill["fee"]),
            remaining_qty=remaining_qty if created else int(fill.get("remaining_qty") or 0),
            fee_components=fee_charge.as_dict() if created else None,
            fill=fill,
        )

    submit = apply
    execute = apply

    def _adverse_price(self, action: SimulationAction, bar: MinuteBar) -> float | None:
        if getattr(action, "order_type", "LEGACY_REFERENCE") == "LIMIT" and action.action in {SimulationActionType.BUY, SimulationActionType.ADD}:
            limit = action.limit_price
            if limit is None:
                return None
            # A limit is a maximum, not an exact required transaction price.
            # Opening price improvement is permitted. A later touch alone
            # does not prove queue priority: require strict penetration.
            if bar.open <= limit:
                return bar.open
            return limit if bar.low < limit else None
        # Protection exits execute from the currently available window; they
        # must not wait for an obsolete signal/reference price to reappear.
        reference = float(
            bar.open
            if action.action in {
                SimulationActionType.SELL,
                SimulationActionType.REDUCE,
                SimulationActionType.FORCED_RISK_EXIT,
            }
            else action.entry_reference or bar.open
        )
        if not math.isfinite(reference) or reference <= 0:
            return None
        slippage = self.config.slippage_bps / 10_000
        if action.action in {SimulationActionType.BUY, SimulationActionType.ADD}:
            proposed = stock_trading_rules(action.symbol).adverse_tick(reference * (1 + slippage), buy=True)
            return proposed if bar.low <= proposed <= bar.high else None
        proposed = stock_trading_rules(action.symbol).adverse_tick(reference * (1 - slippage), buy=False)
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
