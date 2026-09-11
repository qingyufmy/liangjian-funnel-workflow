"""Refresh current observation prices independently of today's research pool.

This is a fact acquisition step, not a decision replay. Offline labeling stays
offline; the scheduled refresh command explicitly opts into provider requests.
"""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from ..pipeline.data_source import HithinkClient
from ..pipeline.data_sync import _row_time
from ..pipeline.local_fact_cache import LocalFactCache
from ..runtime.calendar import ExchangeTradingCalendar
from .outcome_labels import _observation


def refresh_current_outcome_prices(store, settings, *, now=None, client_factory=HithinkClient,
                                   max_requests=1000, quote_fetcher=None):
    current = (now or datetime.now(ZoneInfo(settings.timezone))).astimezone(ZoneInfo("Asia/Shanghai"))
    calendar = ExchangeTradingCalendar()
    report = {"status": "NOOP", "scope": "OPEN_T_PLUS_10_CURRENT_OBSERVATIONS",
              "as_of_date": current.date().isoformat(), "requests": 0, "updated_symbols": [],
              "missing_symbols": [], "deferred_symbols": [], "failures": {}, "network_used": False}
    if current.hour < 15 or not calendar.is_trading_day(current.date()):
        report["reason_code"] = "CLOSED_TRADING_DAY_REQUIRED"
        return report
    prior = []
    day = current.date() - timedelta(days=1)
    while len(prior) < 10:
        if calendar.is_trading_day(day):
            prior.append(day)
        day -= timedelta(days=1)
    earliest = min(prior)
    # Retired/observed/rejected stocks remain measurable. Never intersect this
    # set with G0, A1, turnover gates or today's selected research symbols.
    tracked = {}
    for label in store.list_outcome_labels(labeled_only=False):
        raw_date = label.get("trade_date")
        if not raw_date:
            continue
        trade_date = date.fromisoformat(str(raw_date)[:10])
        if trade_date not in prior or (label.get("labeled_at") and not (
            label.get("stage") == "A4" and label.get("signal_return_10d") is None
        )):
            continue
        symbol = str(label.get("symbol") or "").strip().upper()
        if symbol:
            tracked[symbol] = max(trade_date, tracked.get(symbol, earliest))
    cache = LocalFactCache(settings.fact_cache_db_path)
    start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    latest = cache.latest_daily_bars_before(tuple(tracked), end=end, adjust="none")
    missing = [s for s in tracked if s not in latest or
               datetime.fromisoformat(str(latest[s]["timestamp"])).astimezone(current.tzinfo).date() != current.date()]
    missing.sort(key=lambda s: (-tracked[s].toordinal(), s))
    limit = max(0, int(max_requests))
    report["tracked_symbol_count"] = len(tracked)
    report["deferred_symbols"] = missing[limit:]
    pending = missing[:limit]
    if pending:
        with client_factory(settings) as client:
            for symbol in pending:
                report["requests"] += 1
                report["network_used"] = True
                try:
                    result = client.history_1d(symbol,
                        start=int(start.replace(year=earliest.year, month=earliest.month, day=earliest.day).timestamp() * 1000),
                        end=int(end.timestamp() * 1000), adjust="none", limit=100, max_pages=1)
                    if not result.ok or not result.complete:
                        report["failures"][symbol] = result.reason_code or "INCOMPLETE_RESPONSE"
                        continue
                    rows = []
                    for item in result.items:
                        payload = item.model_dump(mode="json")
                        stamp = _row_time(payload)
                        if not earliest <= stamp.date() <= current.date():
                            continue
                        if str(payload.get("symbol") or symbol).upper() != symbol:
                            raise ValueError("SYMBOL_MISMATCH")
                        _observation({"symbol": symbol, "timestamp": stamp.isoformat(), **payload})
                        rows.append({"symbol": symbol, "timestamp": stamp, "adjust": "none",
                                     "fetched_at": result.fetch_time, "payload": payload})
                    if rows:
                        cache.upsert_daily_bars(rows)
                    if any(r["timestamp"].date() == current.date() for r in rows):
                        report["updated_symbols"].append(symbol)
                    else:
                        report["failures"][symbol] = "CURRENT_CLOSED_DAILY_BAR_MISSING"
                except Exception as exc:
                    # Provider exceptions may include credentials or URLs.
                    report["failures"][symbol] = "PRICE_REFRESH_" + type(exc).__name__.upper()
    report["missing_symbols"] = [s for s in missing if s not in report["updated_symbols"]]
    # A complete history response may legitimately have no bar on a suspended
    # day. Verify today's absence of trades independently; never synthesize a
    # daily candle, carry a price forward, or label a zero return from a quote.
    no_bar = [s for s, reason in report["failures"].items()
              if reason == "CURRENT_CLOSED_DAILY_BAR_MISSING"]
    report["no_trade_observations"] = {}
    if no_bar:
        if quote_fetcher is None:
            from ..data.rotation_theme import _default_tencent_quote_batch_fetch
            quote_fetcher = _default_tencent_quote_batch_fetch
        report["network_used"] = True
        report["quote_check_count"] = len(no_bar)
        try:
            quotes = quote_fetcher(no_bar)
        except Exception:
            quotes = {}
        quote_cutoff = current if now is not None else datetime.now(current.tzinfo)
        for symbol in no_bar:
            quote = quotes.get(symbol, {}) if isinstance(quotes, dict) else {}
            if quote_cutoff.date() == current.date() and _verified_no_trade_quote(quote, symbol, quote_cutoff):
                report["no_trade_observations"][symbol] = {
                    "reason_code": "CURRENT_DAY_NO_REPORTED_TRADES",
                    "source_id": quote["source_id"],
                    "quote_time": str(quote["quote_time"]),
                    "reference_price": quote["latest_price"],
                    "daily_bar_created": False,
                    "performance_status": "PENDING_NO_TRADE_OBSERVATION",
                }
    report["unresolved_missing_symbols"] = [s for s in report["missing_symbols"]
        if s not in report["no_trade_observations"]]
    report["status"] = ("DATA_LIMITED" if report["unresolved_missing_symbols"] else
        "COMPLETED_WITH_NO_TRADES" if report["no_trade_observations"] else "COMPLETED")
    return report


def _verified_no_trade_quote(quote, symbol, current):
    try:
        stamp = datetime.fromisoformat(str(quote["quote_time"]))
        price, previous = float(quote["latest_price"]), float(quote["previous_close"])
        return (quote.get("symbol") == symbol and quote.get("source_id") == "TENCENT:qt.gtimg.cn"
            and quote.get("price_state") == "OBSERVED_PRICE"
            and quote.get("no_reported_trades") is True and quote.get("turnover_cny") == 0
            and 0 < price == previous and price < float('inf')
            and stamp.tzinfo is not None and stamp <= current
            and stamp.astimezone(current.tzinfo).date() == current.date()
            and stamp.astimezone(current.tzinfo).hour >= 15)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
