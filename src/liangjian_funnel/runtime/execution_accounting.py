"""Versioned, decimal-safe accounting for simulated A-share orders.

The minimum commission is an order-level charge.  It is reserved on the first
positive fill, so an in-flight order can never spend cash that will later be
needed for settlement.  Later partial fills only add the incremental
commission above that reservation.  Sell tax and other fees are independent
components and are never folded into the minimum-commission comparison.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Literal


CENT = Decimal("0.01")
BPS = Decimal("10000")


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _finite_nonnegative(value: Decimal | str | int | float, *, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite non-negative decimal") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{field} must be a finite non-negative decimal")
    return parsed


@dataclass(frozen=True, slots=True)
class FeeSchedule:
    commission_bps: Decimal = Decimal("1")
    minimum_commission: Decimal = Decimal("5")
    sell_tax_bps: Decimal = Decimal("5")
    other_fee_bps: Decimal = Decimal("0")
    version: str = "paper-config/2"
    rounding: str = "CNY_CENT_HALF_UP"

    def __post_init__(self) -> None:
        for name in ("commission_bps", "minimum_commission", "sell_tax_bps", "other_fee_bps"):
            object.__setattr__(self, name, _finite_nonnegative(getattr(self, name), field=name))
        if not str(self.version).strip():
            raise ValueError("fee schedule version is required")


@dataclass(frozen=True, slots=True)
class FeeCharge:
    gross: Decimal
    commission: Decimal
    sell_tax: Decimal
    other_fee: Decimal
    total: Decimal
    cumulative_gross: Decimal
    cumulative_commission: Decimal
    fee_model_version: str
    rounding: str
    final: bool

    def as_dict(self) -> dict[str, str | bool]:
        return {
            "gross": str(self.gross),
            "commission": str(self.commission),
            "sell_tax": str(self.sell_tax),
            "other_fee": str(self.other_fee),
            "total": str(self.total),
            "cumulative_gross": str(self.cumulative_gross),
            "cumulative_commission": str(self.cumulative_commission),
            "fee_model_version": self.fee_model_version,
            "rounding": self.rounding,
            "final": self.final,
        }


class OrderFeeLedger:
    """Accumulate fee components for one immutable order identity."""

    def __init__(self, schedule: FeeSchedule, *, side: Literal["BUY", "SELL"]):
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        self.schedule = schedule
        self.side = side
        self.cumulative_gross = Decimal("0")
        self.cumulative_commission = Decimal("0")

    def charge(self, gross: Decimal | str | int | float, *, final: bool) -> FeeCharge:
        amount = _finite_nonnegative(gross, field="gross")
        if amount == 0:
            zero = Decimal("0.00")
            return FeeCharge(
                gross=zero,
                commission=zero,
                sell_tax=zero,
                other_fee=zero,
                total=zero,
                cumulative_gross=_money(self.cumulative_gross),
                cumulative_commission=_money(self.cumulative_commission),
                fee_model_version=self.schedule.version,
                rounding=self.schedule.rounding,
                final=final,
            )

        next_gross = self.cumulative_gross + amount
        target_commission = max(
            self.schedule.minimum_commission,
            _money(next_gross * self.schedule.commission_bps / BPS),
        )
        commission = _money(target_commission - self.cumulative_commission)
        sell_tax = _money(amount * self.schedule.sell_tax_bps / BPS) if self.side == "SELL" else Decimal("0.00")
        other_fee = _money(amount * self.schedule.other_fee_bps / BPS)
        total = _money(commission + sell_tax + other_fee)
        self.cumulative_gross = next_gross
        self.cumulative_commission = target_commission
        return FeeCharge(
            gross=_money(amount),
            commission=commission,
            sell_tax=sell_tax,
            other_fee=other_fee,
            total=total,
            cumulative_gross=_money(next_gross),
            cumulative_commission=_money(target_commission),
            fee_model_version=self.schedule.version,
            rounding=self.schedule.rounding,
            final=final,
        )


__all__ = ["FeeCharge", "FeeSchedule", "OrderFeeLedger"]
