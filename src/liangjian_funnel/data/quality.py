from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CrossCheckStatus(StrEnum):
    PASS = "PASS"
    BLOCKED = "BLOCKED"


class PriceCrossCheck(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: CrossCheckStatus
    reason_code: str | None = None
    difference_pct: Decimal | None = Field(default=None, ge=0)
    maximum_difference_pct: Decimal = Field(ge=0)
    timestamp_lag_seconds: float | None = Field(default=None, ge=0)

    @field_validator("difference_pct", "maximum_difference_pct")
    @classmethod
    def finite_decimals(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and not value.is_finite():
            raise ValueError("price comparison values must be finite")
        return value


def compare_prices(
    *,
    hithink_price: Decimal | int | float | str,
    mootdx_price: Decimal | int | float | str,
    hithink_time: datetime,
    mootdx_time: datetime,
    maximum_difference_pct: Decimal | int | float | str = "0.5",
    maximum_timestamp_lag_seconds: float = 90.0,
) -> PriceCrossCheck:
    limit = _decimal(maximum_difference_pct)
    if hithink_time.tzinfo is None or mootdx_time.tzinfo is None:
        return PriceCrossCheck(
            status=CrossCheckStatus.BLOCKED,
            reason_code="NAIVE_TIMESTAMP",
            maximum_difference_pct=limit,
        )
    try:
        left = _decimal(hithink_price)
        right = _decimal(mootdx_price)
    except (InvalidOperation, TypeError, ValueError):
        return PriceCrossCheck(
            status=CrossCheckStatus.BLOCKED,
            reason_code="INVALID_PRICE",
            maximum_difference_pct=limit,
        )
    if not left.is_finite() or not right.is_finite() or left <= 0 or right <= 0:
        return PriceCrossCheck(
            status=CrossCheckStatus.BLOCKED,
            reason_code="INVALID_PRICE",
            maximum_difference_pct=limit,
        )
    lag = abs((hithink_time - mootdx_time).total_seconds())
    if lag > maximum_timestamp_lag_seconds:
        return PriceCrossCheck(
            status=CrossCheckStatus.BLOCKED,
            reason_code="TIMESTAMP_MISMATCH",
            maximum_difference_pct=limit,
            timestamp_lag_seconds=lag,
        )
    difference = (abs(left - right) / left * Decimal("100")).quantize(Decimal("0.000001"))
    return PriceCrossCheck(
        status=CrossCheckStatus.PASS if difference <= limit else CrossCheckStatus.BLOCKED,
        reason_code=None if difference <= limit else "PRICE_DIVERGENCE",
        difference_pct=difference,
        maximum_difference_pct=limit,
        timestamp_lag_seconds=lag,
    )


def _decimal(value: Decimal | int | float | str) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))
