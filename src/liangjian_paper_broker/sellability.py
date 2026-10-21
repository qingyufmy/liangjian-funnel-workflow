"""One dated-lot T+1 calculation shared by customer and Team accounts.

Callers provide only their own account/plan lots, under their existing lock.
Unknown lineage is not permission to sell. Reservations are deducted afterwards.
"""
from datetime import date, datetime
from zoneinfo import ZoneInfo


def sellable_qty(lots, *, as_of, total_qty, position_cap=None):
    if isinstance(as_of, datetime):
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            return None
        day = as_of.astimezone(ZoneInfo("Asia/Shanghai")).date()
    elif type(as_of) is date:
        day = as_of
    else:
        return None
    if type(total_qty) is not int or total_qty < 0:
        return None
    if position_cap is not None and (type(position_cap) is not int
            or not 0 <= position_cap <= total_qty):
        return None
    total = available = 0
    for lot in lots:
        qty = lot.get("qty")
        if type(qty) is not int or qty < 0:
            return None
        try:
            acquired = date.fromisoformat(lot["acquired_trade_date"])
            release = lot.get("sellable_from")
            released = date.fromisoformat(release) if release is not None else None
        except (KeyError, TypeError, ValueError):
            return None
        if acquired > day or (released is not None and released <= acquired):
            return None
        total += qty
        if acquired < day and (released is None or released <= day):
            available += qty
    if total != total_qty:
        return None
    return min(available, position_cap) if position_cap is not None else available
