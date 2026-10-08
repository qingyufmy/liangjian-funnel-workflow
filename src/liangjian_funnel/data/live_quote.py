"""Independent, bounded snapshot recovery. Quotes are never minute candles.

Sina is opt-in for execution and otherwise exposed only to explicit shadow
probes. Existing minute/position/fill contracts and freshness gates stay intact.
"""
from copy import copy
from datetime import datetime
import re
import time

from .mootdx import map_symbol
from .session_windows import TZ
from .tencent_minute import MarketQuote, QuoteResult
from ..runtime.bounded_work import BoundedWorkGate

_PRIMARY = BoundedWorkGate(8)
_BACKUP = BoundedWorkGate(8)
SINA_SOURCE = "SINA:hq.sinajs.cn"


def normalize_sina_quote(raw, symbol):
    mapped = map_symbol(symbol)
    provider = mapped.exchange.lower() + mapped.code
    match = re.search(r'\bhq_str_' + re.escape(provider) + r'\s*=\s*"([^"]*)"', raw)
    if not match:
        raise ValueError("SINA_QUOTE_SYMBOL_OR_SCHEMA_INVALID")
    fields = match.group(1).split(',')
    if len(fields) < 33:
        raise ValueError("SINA_QUOTE_SCHEMA_INVALID")
    stamp = datetime.strptime(fields[30] + ' ' + fields[31], '%Y-%m-%d %H:%M:%S').replace(tzinfo=TZ)
    return MarketQuote(symbol=mapped.canonical, name=fields[0], open=float(fields[1]),
                       previous_close=float(fields[2]), price=float(fields[3]),
                       volume=float(fields[8]), amount=float(fields[9]),
                       quote_time=stamp, source_id=SINA_SOURCE)


def validate_quote(result, symbol, *, as_of, max_age_seconds=90.0):
    quote = result.quote
    if not result.complete:
        return result
    if quote is None:
        return result.model_copy(update={"complete": False, "reason_code": "QUOTE_MISSING"})
    try:
        MarketQuote.model_validate(quote.model_dump())
    except ValueError:
        return result.model_copy(update={"complete": False, "reason_code": "QUOTE_INVALID"})
    if result.symbol != symbol or quote.symbol != symbol:
        return result.model_copy(update={"complete": False, "reason_code": "QUOTE_SYMBOL_MISMATCH"})
    age = (as_of - quote.quote_time).total_seconds()
    reason = ("QUOTE_TRADE_DATE_MISMATCH" if quote.quote_time.date() != as_of.date() else
              "QUOTE_NOT_CURRENT" if age > max_age_seconds or age < -59 else
              "QUOTE_ZERO_VOLUME" if quote.volume <= 0 else None)
    return result.model_copy(update={"complete": False, "reason_code": reason}) if reason else result


class SinaQuoteAdapter:
    """Provider-owned time/price/cumulative shares; no OHLC or fill authority."""
    def __init__(self, *, text_fetcher=None, timeout_seconds=2.5):
        self.text_fetcher = text_fetcher
        self.timeout_seconds = timeout_seconds

    def fetch_quote(self, symbol, *, as_of=None, max_age_seconds=90.0):
        current = as_of or datetime.now(TZ)
        if current.tzinfo is None or current.utcoffset() is None:
            return QuoteResult(symbol=symbol, reason_code="INVALID_AS_OF")
        current = current.astimezone(TZ)
        mapped = map_symbol(symbol)
        started = datetime.now(TZ)
        try:
            url = 'https://hq.sinajs.cn/list=' + mapped.exchange.lower() + mapped.code
            if self.text_fetcher:
                raw = self.text_fetcher(url, {}, self.timeout_seconds)
            else:
                import requests
                response = requests.get(url, timeout=(self.timeout_seconds, self.timeout_seconds),
                    headers={'Referer': 'https://finance.sina.com.cn/', 'User-Agent': 'Mozilla/5.0'})
                response.raise_for_status()
                response.encoding = 'gbk'
                raw = response.text
            quote = normalize_sina_quote(raw, symbol)
            result = QuoteResult(symbol=symbol, quote=quote, reason_code='OK', complete=True)
        except (ValueError, TypeError, KeyError):
            result = QuoteResult(symbol=symbol, reason_code='SINA_QUOTE_INVALID')
        except Exception as exc:
            result = QuoteResult(symbol=symbol, reason_code='SINA_QUOTE_REQUEST_FAILED',
                                 transport_error_type=type(exc).__name__)
        return validate_quote(result, symbol, as_of=current, max_age_seconds=max_age_seconds).model_copy(
            update={'request_started_at': started, 'response_received_at': datetime.now(TZ)})


def fetch_current_quote(primary, backup, symbol, *, as_of, deadline=None,
                        clock=time.monotonic, wall_clock=lambda: datetime.now(TZ)):
    deadline = deadline if deadline is not None else clock() + 5.0
    attempts = []
    selected = QuoteResult(symbol=symbol, reason_code='REALTIME_QUOTE_UNAVAILABLE')
    for source, role in ((primary, 'PRIMARY'), (backup, 'SECONDARY'), (primary, 'PRIMARY_RETRY')):
        if source is None or not callable(getattr(source, 'fetch_quote', None)):
            continue
        # Invalid identity/format/freshness is not a transient transport retry.
        if role == 'PRIMARY_RETRY' and attempts and attempts[0]['reason_code'] not in {
            'TENCENT_QUOTE_REQUEST_FAILED', 'REALTIME_QUOTE_REQUEST_FAILED',
            'REALTIME_QUOTE_DEADLINE_EXCEEDED', 'REALTIME_QUOTE_BACKPRESSURE'}:
            continue
        started = clock()
        remaining = deadline - started
        if remaining <= 0:
            selected = QuoteResult(symbol=symbol, reason_code='REALTIME_QUOTE_DEADLINE_EXCEEDED')
            break
        budget = min(2.5, remaining / 3)
        if role == 'PRIMARY_RETRY':
            budget = min(budget, 0.75)
        bounded = copy(source)
        if hasattr(bounded, 'timeout_seconds'):
            bounded.timeout_seconds = min(budget, source.timeout_seconds)
        began = wall_clock()
        def invoke(bounded=bounded):
            # Support narrow test/legacy adapters without hiding TypeError
            # inside a production parser as a second transport request.
            import inspect
            params = inspect.signature(bounded.fetch_quote).parameters
            kwargs = {'as_of': as_of}
            if 'max_age_seconds' in params or any(p.kind == p.VAR_KEYWORD for p in params.values()):
                kwargs['max_age_seconds'] = 90.0
            return bounded.fetch_quote(symbol, **kwargs)
        work = (_BACKUP if role == 'SECONDARY' else _PRIMARY).call(
            invoke, deadline=started + budget, clock=clock, name='live-quote-provider')
        received = wall_clock()
        if work.status == 'READY' and isinstance(work.value, QuoteResult) and clock() <= started + budget:
            selected = validate_quote(work.value, symbol, as_of=as_of)
        else:
            selected = QuoteResult(symbol=symbol, reason_code=(
                'REALTIME_QUOTE_BACKPRESSURE' if work.status == 'BACKPRESSURE' else
                'REALTIME_QUOTE_DEADLINE_EXCEEDED' if work.status == 'TIMED_OUT' or clock() > started + budget
                else 'REALTIME_QUOTE_REQUEST_FAILED'))
        selected = selected.model_copy(update={'request_started_at': began, 'response_received_at': received})
        attempts.append({'source_role': role, 'source_provider': type(source).__name__,
            'requested_symbol': symbol, 'returned_symbol': selected.quote.symbol if selected.quote else None,
            'source_id': selected.quote.source_id if selected.quote else None,
            'request_started_at': began.isoformat(), 'response_received_at': received.isoformat(),
            'provider_timestamp': selected.quote.quote_time.isoformat() if selected.quote else None,
            'elapsed_ms': round(max(0, (clock()-started)*1000), 3),
            'transport_error_type': selected.transport_error_type,
            'reason_code': selected.reason_code, 'complete': selected.complete})
        if selected.complete:
            break
    return selected.model_copy(update={'source_attempts': tuple(attempts)})
