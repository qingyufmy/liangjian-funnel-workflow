"""Cash A-share simulation constraints, separate from strategy thresholds."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR


@dataclass(frozen=True)
class StockTradingRules:
    minimum: int = 100
    increment: int = 100
    maximum: int = 1_000_000
    tick: str = "0.01"
    version: str = "cash-ashare-20260706/1"

    def floor_buy(self, quantity: float) -> int:
        count = min(int(quantity), self.maximum)
        return count // self.increment * self.increment if count >= self.minimum else 0

    def sell_quantity(self, requested: int, available: int) -> int:
        if requested <= 0 or requested > available:
            return 0
        if requested == available:
            return min(available, self.maximum)
        count = min(requested, self.maximum) // self.increment * self.increment
        return count if count >= self.minimum else 0

    def adverse_tick(self, price: float, *, buy: bool) -> float:
        tick = Decimal(self.tick)
        return float((Decimal(str(price)) / tick).to_integral_value(
            rounding=ROUND_CEILING if buy else ROUND_FLOOR) * tick)


def stock_trading_rules(symbol: str) -> StockTradingRules:
    code = str(symbol).split(".")[0]
    if len(code) != 6 or not code.isdigit():
        raise ValueError("UNSUPPORTED_SECURITY_RULES")
    if code.startswith(("688", "689")):
        return StockTradingRules(minimum=200, increment=1, maximum=100_000)
    if code.startswith(("600", "601", "603", "605", "000", "001", "002", "003", "300", "301")):
        return StockTradingRules()
    raise ValueError("UNSUPPORTED_SECURITY_RULES")
