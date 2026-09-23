"""Sina five-minute research probe. Never an A4 execution fallback.

Observed vendor aggregation differs from Tencent; callers must report the
differences, not merge prices/volumes or claim independent confirmation.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from .mootdx import MinuteBar, map_symbol

TZ = ZoneInfo("Asia/Shanghai")
URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
SOURCE = "SINA:money.finance.sina.com.cn:5m"


def normalize_sina_5m(rows, *, symbol, as_of):
    if as_of.tzinfo is None or not isinstance(rows, list) or not rows:
        raise ValueError("SINA_5M_EMPTY_OR_INVALID")
    canonical = map_symbol(symbol).canonical
    cutoff = as_of.astimezone(TZ)
    bars = {}
    for row in rows:
        stamp = datetime.fromisoformat(str(row["day"]))
        if stamp.tzinfo is not None:
            raise ValueError("SINA_5M_TIMESTAMP_CONTRACT_CHANGED")
        stamp = stamp.replace(tzinfo=TZ)
        clock = stamp.hour * 60 + stamp.minute
        if stamp.second or stamp.microsecond or clock % 5 or not (575 <= clock <= 690 or 785 <= clock <= 900):
            raise ValueError("SINA_5M_SESSION_INVALID")
        if stamp > cutoff:
            continue
        values = {key: float(row[key]) for key in ("open", "high", "low", "close", "volume")}
        # No turnover amount is supplied. Explicit estimate, never reported
        # money flow or an execution-price/volume guarantee.
        bar = MinuteBar(symbol=canonical, interval="5m", bar_end=stamp, **values,
                        amount=values["volume"] * sum(values[k] for k in ("open", "high", "low", "close")) / 4,
                        source_id=SOURCE, volume_unit="shares", amount_kind="ohlc_estimate",
                        normalizer_version="sina-5m-shadow/1", provider_bar_end=stamp)
        if stamp in bars and bars[stamp] != bar:
            raise ValueError("SINA_5M_DUPLICATE_CONFLICT")
        bars[stamp] = bar
    if not bars or max(bars).date() != cutoff.date():
        raise ValueError("SINA_5M_CURRENT_DAY_MISSING")
    return tuple(bars[key] for key in sorted(bars))


def fetch_sina_5m_shadow(symbol, *, count=320, fetch=None, clock=None):
    """Explicit probe only. Archive returned raw rows before comparisons."""
    import httpx
    now = clock or (lambda: datetime.now(TZ))
    if not 1 <= count <= 1023:
        raise ValueError("SINA_5M_COUNT_UNSUPPORTED")
    mapping = map_symbol(symbol)
    params = {"symbol": mapping.exchange.lower() + mapping.code, "scale": "5", "ma": "no", "datalen": str(count)}
    started = now()
    if fetch is None:
        with httpx.Client(timeout=8, follow_redirects=False) as client:
            response = client.get(URL, params=params)
            response.raise_for_status()
            rows = response.json()
    else:
        rows = fetch(URL, params)
    received = now()
    bars = normalize_sina_5m(rows, symbol=mapping.canonical, as_of=received)
    return {"source_id": SOURCE, "shadow_only": True, "execution_authority": False,
            "one_minute_supported": False, "request_started_at": started.isoformat(),
            "response_received_at": received.isoformat(), "raw_rows": rows,
            "bars": [bar.model_dump(mode="json") for bar in bars]}


def compare_five_minute_bars(left, right):
    """Exact evidence diff, deliberately no strategy-tolerance relaxation."""
    if left and right and ({b.symbol for b in left} != {b.symbol for b in right}
                          or len({b.symbol for b in left}) != 1):
        raise ValueError("CROSS_SYMBOL_COMPARISON_FORBIDDEN")
    providers_left = {bar.source_id.split(":", 1)[0] for bar in left}
    providers_right = {bar.source_id.split(":", 1)[0] for bar in right}
    independent = bool(providers_left and providers_right and not providers_left & providers_right)
    a = {b.bar_end: b for b in left}
    b = {bar.bar_end: bar for bar in right}
    common = sorted(a.keys() & b.keys())
    differences = []
    for stamp in common:
        x, y = a[stamp], b[stamp]
        fields = {k: {"left": getattr(x, k), "right": getattr(y, k)}
                  for k in ("open", "high", "low", "close", "volume")
                  if abs(getattr(x, k) - getattr(y, k)) > 1e-8}
        if fields:
            differences.append({"bar_end": stamp.isoformat(), "fields": fields})
    return {"common_count": len(common), "different_bar_count": len(differences),
            "left_only": [t.isoformat() for t in sorted(a.keys() - b.keys())],
            "right_only": [t.isoformat() for t in sorted(b.keys() - a.keys())],
            "differences": differences,
            "distinct_provider_identity": independent,
            "independent_confirmation": independent and bool(common) and not differences and a.keys() == b.keys(),
            "execution_authority": False}
