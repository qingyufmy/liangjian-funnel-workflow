"""Deterministic A-share next-minute matching. No clock, database or network I/O.

The ledger supplies remaining participation and cash after all other orders'
reservations. A match is only a proposal until that ledger atomically commits it.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
import json
from zoneinfo import ZoneInfo

ZERO = Decimal("0")
CENT = Decimal("0.01")
SHANGHAI = ZoneInfo("Asia/Shanghai")


def number(value) -> Decimal:
    if isinstance(value, bool): raise ValueError("PAPER_NUMBER_INVALID")
    try: result = Decimal(str(value))
    except Exception as exc: raise ValueError("PAPER_NUMBER_INVALID") from exc
    if not result.is_finite() or result < 0: raise ValueError("PAPER_NUMBER_INVALID")
    return result


def local_time(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("PAPER_TIMESTAMP_INVALID")
    return value.astimezone(SHANGHAI)


def money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class PaperRules:
    trade_date: str
    participation: Decimal = Decimal("0.10")
    commission_rate: Decimal = Decimal("0.00025")
    minimum_commission: Decimal = Decimal("5")
    sell_stamp_rate: Decimal = Decimal("0.0005")
    sh_transfer_rate: Decimal = Decimal("0.00001")
    trading_day: bool = True

    def __post_init__(self):
        date.fromisoformat(self.trade_date)
        if type(self.trading_day) is not bool: raise ValueError("PAPER_RULE_INVALID")
        for key in ("participation", "commission_rate", "minimum_commission", "sell_stamp_rate", "sh_transfer_rate"):
            object.__setattr__(self, key, number(getattr(self, key)))
        if not 0 < self.participation <= 1: raise ValueError("PAPER_RULE_INVALID")
        if any(getattr(self,k)>1 for k in ("commission_rate","sell_stamp_rate","sh_transfer_rate")):
            raise ValueError("PAPER_RULE_INVALID")


@dataclass(frozen=True)
class PaperOrder:
    order_id: str
    plan_id: str
    signal_id: str
    symbol: str
    side: str
    order_type: str
    qty: int
    remaining_qty: int
    submitted_at: datetime
    limit_price: Decimal | None = None
    no_chase_price: Decimal | None = None
    sellable_qty: int = 0
    filled_notional: Decimal = ZERO
    fees_paid: Decimal = ZERO

    def __post_init__(self):
        if self.side not in {"BUY", "SELL"} or self.order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("PAPER_ORDER_INVALID")
        if (any(type(getattr(self,k)) is not int for k in ("qty","remaining_qty","sellable_qty"))
                or not self.qty > 0 or not 0 <= self.remaining_qty <= self.qty or self.sellable_qty < 0
                or any(not isinstance(getattr(self,k),str) or not getattr(self,k) for k in ("order_id","plan_id","signal_id","symbol"))):
            raise ValueError("PAPER_ORDER_INVALID")
        local_time(self.submitted_at)
        for key in ("limit_price", "no_chase_price", "filled_notional", "fees_paid"):
            if getattr(self,key) is not None: object.__setattr__(self,key,number(getattr(self,key)))
        if self.order_type == "LIMIT" and (self.limit_price is None or self.limit_price <= 0):
            raise ValueError("PAPER_LIMIT_PRICE_REQUIRED")


@dataclass(frozen=True)
class MinuteBar:
    symbol: str
    minute_end: datetime
    low: Decimal
    high: Decimal
    vwap: Decimal
    volume_shares: int
    closed: bool = False
    tradability_known: bool = False
    halted: bool = False
    limit_up: bool = False
    limit_down: bool = False
    close: Decimal | None = None

    def __post_init__(self):
        end=local_time(self.minute_end)
        if end.second or end.microsecond: raise ValueError("PAPER_CLOSED_MINUTE_BOUNDARY_REQUIRED")
        if type(self.volume_shares) is not int or self.volume_shares < 0:
            raise ValueError("PAPER_VOLUME_UNIT_SHARES_REQUIRED")
        for key in ("closed", "tradability_known", "halted", "limit_up", "limit_down"):
            if type(getattr(self,key)) is not bool: raise ValueError("PAPER_TRADABILITY_INVALID")
        for key in ("low", "high", "vwap"): object.__setattr__(self,key,number(getattr(self,key)))
        if not self.low <= self.vwap <= self.high: raise ValueError("PAPER_BAR_PRICE_INCONSISTENT")
        if self.close is not None:
            object.__setattr__(self,'close',number(self.close))
            if not self.low<=self.close<=self.high:raise ValueError("PAPER_BAR_PRICE_INCONSISTENT")


@dataclass(frozen=True)
class MatchResult:
    order_id: str
    status: str
    qty: int
    price: Decimal | None
    fee: Decimal
    minute_end: str
    reason_code: str

    def canonical_bytes(self) -> bytes:
        return json.dumps(asdict(self),sort_keys=True,separators=(",",":"),default=str).encode()


def total_fees(notional: Decimal, order: PaperOrder, rules: PaperRules) -> Decimal:
    if notional <= 0: return ZERO
    commission = money(max(rules.minimum_commission, notional * rules.commission_rate))
    stamp = money(notional * rules.sell_stamp_rate) if order.side == "SELL" else ZERO
    transfer = money(notional * rules.sh_transfer_rate) if order.symbol.endswith(".SH") else ZERO
    return commission + stamp + transfer


def match_order(order: PaperOrder, bar: MinuteBar, rules: PaperRules, *, spendable_cash: Decimal,
                available_volume_shares: int | None = None) -> MatchResult:
    end = local_time(bar.minute_end); submitted = local_time(order.submitted_at)
    cash = number(spendable_cash)
    def no(code, status="UNFILLED"):
        return MatchResult(order.order_id,status,0,None,ZERO,end.isoformat(),code)
    if bar.symbol != order.symbol: return no("BAR_SYMBOL_MISMATCH","REJECTED")
    if rules.trade_date != end.date().isoformat(): return no("RULE_SNAPSHOT_DATE_MISMATCH","REJECTED")
    if not rules.trading_day: return no("NON_TRADING_DAY")
    if not bar.closed: return no("BAR_NOT_CLOSED")
    if end <= submitted: return no("NEXT_MINUTE_NOT_YET_AVAILABLE")
    if end.date() != submitted.date(): return no("ORDER_DAY_EXPIRED","CANCELLED")
    if not (time(9,30) < end.time().replace(tzinfo=None) <= time(11,30)
            or time(13) < end.time().replace(tzinfo=None) <= time(15)):
        return no("OUTSIDE_TRADING_SESSION")
    if not bar.tradability_known: return no("TRADABILITY_UNPROVEN")
    if bar.halted: return no("SUSPENDED","REJECTED")
    if order.side == "BUY" and bar.limit_up: return no("LIMIT_UP_LOCKED")
    if order.side == "SELL" and bar.limit_down: return no("LIMIT_DOWN_LOCKED")
    if order.remaining_qty <= 0: return no("ORDER_ALREADY_FILLED")
    if bar.volume_shares <= 0 or bar.vwap <= 0: return no("NO_REPORTED_TRADES")
    if order.side == "BUY" and (order.qty % 100 or order.remaining_qty % 100):
        return no("BUY_ROUND_LOT_REQUIRED","REJECTED")
    if order.side == "SELL" and order.sellable_qty <= 0: return no("T1_NOT_SELLABLE")
    if order.order_type == "LIMIT":
        if (order.side == "BUY" and bar.low > order.limit_price
                or order.side == "SELL" and bar.high < order.limit_price):
            return no("LIMIT_NOT_TOUCHED")
        price = min(order.limit_price,bar.vwap) if order.side == "BUY" else max(order.limit_price,bar.vwap)
    else: price = bar.vwap
    if order.side == "BUY":
        if order.no_chase_price is None or order.no_chase_price <= 0: return no("BUY_PRICE_CAP_REQUIRED","REJECTED")
        if price > order.no_chase_price: return no("PRICE_ABOVE_NO_CHASE")
    cap = int((Decimal(bar.volume_shares) * rules.participation).to_integral_value(rounding=ROUND_FLOOR))
    if available_volume_shares is not None:
        if type(available_volume_shares) is not int or available_volume_shares < 0:
            raise ValueError("PAPER_PARTICIPATION_REMAINDER_INVALID")
        cap = min(cap,available_volume_shares)
    qty = min(order.remaining_qty,cap,order.sellable_qty if order.side == "SELL" else order.remaining_qty)
    if order.side == "BUY":
        qty = min(qty,int((cash / price).to_integral_value(rounding=ROUND_FLOOR)))
        qty = qty // 100 * 100
    if qty <= 0: return no("PARTICIPATION_LOT_UNAVAILABLE")
    while qty > 0:
        fee = total_fees(order.filled_notional + price * qty,order,rules) - order.fees_paid
        if fee < 0: return no("ORDER_FEE_STATE_CONFLICT","REJECTED")
        if order.side == "SELL" or price * qty + fee <= cash: break
        qty -= 100
    if qty <= 0: return no("INSUFFICIENT_CASH")
    return MatchResult(order.order_id,"FILLED" if qty == order.remaining_qty else "PARTIAL",
                       qty,price,fee,end.isoformat(),"MATCHED_NEXT_CLOSED_MINUTE")
