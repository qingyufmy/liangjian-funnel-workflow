"""Bounded Tencent quote/minute fallback for the intraday control plane.

The public Tencent endpoint is used only when the primary MootDX minute feed
cannot satisfy a small intraday request.  It never replaces the long-history
contract owned by MootDX and it never submits orders.  Every returned row is
normalized into the same immutable :class:`MinuteBar` contract.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from .mootdx import FetchResult, MinuteBar, MootdxAdapter, NodeAttempt, map_symbol


SHANGHAI = ZoneInfo("Asia/Shanghai")
TENCENT_HOST = "ifzq.gtimg.cn"
TENCENT_SOURCE_ID = f"TENCENT:{TENCENT_HOST}"
TENCENT_MAX_BARS = 320
_QUOTE_TIMESTAMP = re.compile(r"^\d{14}$")


class TencentMarketDataError(RuntimeError):
    """Stable, payload-free error raised by the Tencent normalizer."""

    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


class MarketQuote(BaseModel):
    """One timestamped, provider-owned auction/realtime quote."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    name: str = ""
    quote_time: datetime
    price: float = Field(gt=0)
    open: float = Field(gt=0)
    previous_close: float = Field(gt=0)
    volume: float = Field(ge=0)
    amount: float = Field(ge=0)
    source_id: str = TENCENT_SOURCE_ID


class QuoteResult(BaseModel):
    """Fail-closed quote result used by the 09:26 auction review."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    quote: MarketQuote | None = None
    reason_code: str
    complete: bool = False


JsonFetcher = Callable[[str, Mapping[str, str], float], Any]
TextFetcher = Callable[[str, Mapping[str, str], float], str]


def _provider_symbol(symbol: str) -> str:
    mapped = map_symbol(symbol)
    return ("sh" if mapped.exchange == "SH" else "sz") + mapped.code


def _default_json_fetcher(url: str, params: Mapping[str, str], timeout: float) -> Any:
    import requests

    response = requests.get(
        url,
        params=dict(params),
        timeout=(min(5.0, timeout), timeout),
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; LiangjianResearch/2.0)",
            "Referer": "https://gu.qq.com/",
        },
    )
    response.raise_for_status()
    return response.json()


def _default_text_fetcher(url: str, params: Mapping[str, str], timeout: float) -> str:
    import requests

    response = requests.get(
        url,
        params=dict(params),
        timeout=(min(5.0, timeout), timeout),
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; LiangjianResearch/2.0)",
            "Referer": "https://gu.qq.com/",
        },
    )
    response.raise_for_status()
    response.encoding = "gbk"
    return response.text


class TencentIntradayAdapter:
    """Small-window 1m/5m and auction-quote adapter.

    Tencent's endpoint exposes at most 320 recent bars.  Requests above that
    bound deliberately fail so the source can never masquerade as the long
    A3 history provider.
    """

    def __init__(
        self,
        *,
        json_fetcher: JsonFetcher | None = None,
        text_fetcher: TextFetcher | None = None,
        timeout_seconds: float = 12.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.json_fetcher = json_fetcher or _default_json_fetcher
        self.text_fetcher = text_fetcher or _default_text_fetcher
        self.timeout_seconds = timeout_seconds

    def fetch_bars(
        self,
        symbol: str,
        interval: str,
        required_bars: int,
        *,
        as_of: datetime | None = None,
    ) -> FetchResult:
        requested = required_bars if isinstance(required_bars, int) and not isinstance(required_bars, bool) else -1
        if requested <= 0:
            return self._result(symbol, interval, max(0, requested), (), "INVALID_REQUIRED_BARS")
        if interval not in {"1m", "5m"}:
            return self._result(symbol, interval, requested, (), "INVALID_INTERVAL")
        if requested > TENCENT_MAX_BARS:
            return self._result(symbol, interval, requested, (), "TENCENT_REQUEST_TOO_LARGE")
        cutoff = as_of or datetime.now(SHANGHAI)
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            return self._result(symbol, interval, requested, (), "INVALID_AS_OF")
        cutoff = cutoff.astimezone(SHANGHAI)
        try:
            provider = _provider_symbol(symbol)
            payload = self.json_fetcher(
                f"https://{TENCENT_HOST}/appstock/app/kline/mkline",
                {"param": f"{provider},m{interval[:-1]},,{TENCENT_MAX_BARS}"},
                self.timeout_seconds,
            )
            rows = self._rows(payload, provider, interval)
            bars = tuple(
                bar
                for bar in (self._bar(symbol, interval, row) for row in rows)
                if bar.bar_end <= cutoff and not (interval == "1m" and bar.bar_end.time().strftime("%H%M") == "0930")
            )
            ordered = tuple(sorted({bar.bar_end: bar for bar in bars}.values(), key=lambda item: item.bar_end))
        except (TencentMarketDataError, TypeError, ValueError, KeyError):
            return self._result(symbol, interval, requested, (), "TENCENT_RESPONSE_INVALID")
        except Exception:
            return self._result(symbol, interval, requested, (), "TENCENT_REQUEST_FAILED")
        if len(ordered) < requested:
            return self._result(symbol, interval, requested, ordered, "TENCENT_INSUFFICIENT_BARS")
        return self._result(symbol, interval, requested, ordered[-requested:], "OK", complete=True)

    def fetch_quote(
        self,
        symbol: str,
        *,
        as_of: datetime | None = None,
        max_age_seconds: float = 180.0,
    ) -> QuoteResult:
        cutoff = as_of or datetime.now(SHANGHAI)
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            return QuoteResult(symbol=str(symbol), reason_code="INVALID_AS_OF")
        cutoff = cutoff.astimezone(SHANGHAI)
        try:
            provider = _provider_symbol(symbol)
            raw = self.text_fetcher(
                "https://qt.gtimg.cn/q",
                {"q": provider},
                self.timeout_seconds,
            )
            quote = self._quote(raw, symbol)
        except (TencentMarketDataError, TypeError, ValueError, KeyError):
            return QuoteResult(symbol=str(symbol), reason_code="TENCENT_QUOTE_INVALID")
        except Exception:
            return QuoteResult(symbol=str(symbol), reason_code="TENCENT_QUOTE_REQUEST_FAILED")
        # The scheduler rounds its decision time to a minute.  Permit the
        # provider timestamp to fall inside that same minute, but never accept
        # another trade date or a stale prior snapshot.
        if quote.quote_time.date() != cutoff.date():
            return QuoteResult(symbol=quote.symbol, quote=quote, reason_code="QUOTE_TRADE_DATE_MISMATCH")
        age = cutoff - quote.quote_time
        if age > timedelta(seconds=max_age_seconds) or age < -timedelta(seconds=59):
            return QuoteResult(symbol=quote.symbol, quote=quote, reason_code="QUOTE_NOT_CURRENT")
        if quote.volume <= 0:
            return QuoteResult(symbol=quote.symbol, quote=quote, reason_code="QUOTE_ZERO_VOLUME")
        return QuoteResult(symbol=quote.symbol, quote=quote, reason_code="OK", complete=True)

    @staticmethod
    def _rows(payload: Any, provider: str, interval: str) -> list[Any]:
        if not isinstance(payload, Mapping):
            raise TencentMarketDataError("TENCENT_RESPONSE_INVALID")
        data = payload.get("data")
        item = data.get(provider) if isinstance(data, Mapping) else None
        rows = item.get(f"m{interval[:-1]}") if isinstance(item, Mapping) else None
        if not isinstance(rows, list):
            raise TencentMarketDataError("TENCENT_RESPONSE_INVALID")
        return rows

    @staticmethod
    def _bar(symbol: str, interval: str, row: Any) -> MinuteBar:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            raise TencentMarketDataError("TENCENT_BAR_INVALID")
        stamp = datetime.strptime(str(row[0]), "%Y%m%d%H%M").replace(tzinfo=SHANGHAI)
        open_value = float(row[1])
        close_value = float(row[2])
        high_value = float(row[3])
        low_value = float(row[4])
        volume = float(row[5])
        # Tencent's final field is not consistently documented across
        # securities.  A same-unit OHLCV notional preserves a valid VWAP
        # without pretending that the field is authoritative turnover.
        amount = max(0.0, volume * ((open_value + high_value + low_value + close_value) / 4.0))
        return MinuteBar(
            symbol=symbol,
            interval=interval,
            bar_end=stamp,
            open=open_value,
            high=high_value,
            low=low_value,
            close=close_value,
            volume=volume,
            amount=amount,
            source_id=TENCENT_SOURCE_ID,
        )

    @staticmethod
    def _quote(raw: str, symbol: str) -> MarketQuote:
        match = re.search(r'=\s*"([^"]*)"', str(raw or ""))
        if not match:
            raise TencentMarketDataError("TENCENT_QUOTE_INVALID")
        fields = match.group(1).split("~")
        if len(fields) <= 37 or not _QUOTE_TIMESTAMP.fullmatch(fields[30].strip()):
            raise TencentMarketDataError("TENCENT_QUOTE_INVALID")
        return MarketQuote(
            symbol=map_symbol(symbol).canonical,
            name=fields[1].strip(),
            quote_time=datetime.strptime(fields[30].strip(), "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI),
            price=float(fields[3]),
            previous_close=float(fields[4]),
            open=float(fields[5]),
            volume=max(0.0, float(fields[6] or 0)),
            amount=max(0.0, float(fields[37] or 0)),
        )

    @staticmethod
    def _result(
        symbol: str,
        interval: str,
        requested: int,
        bars: tuple[MinuteBar, ...],
        reason: str,
        *,
        complete: bool = False,
    ) -> FetchResult:
        return FetchResult(
            symbol=str(symbol).strip().upper(),
            interval=interval,
            requested_bars=max(0, requested),
            returned_bars=len(bars),
            bars=bars,
            server=f"{TENCENT_HOST}:443" if bars else None,
            attempts=(
                NodeAttempt(
                    server=f"{TENCENT_HOST}:443",
                    pages=1,
                    returned_bars=len(bars),
                    reason_code=reason,
                ),
            ),
            reason_code=reason,
            complete=complete,
        )


class ResilientIntradayAdapter:
    """Tencent-first intraday reads with MootDX as the independent fallback.

    Long-history requests still go directly to MootDX.  This ordering avoids
    spending most of the one-minute A4 deadline waiting for currently
    unreachable public TDX nodes when the bounded Tencent window is healthy.
    """

    def __init__(self, primary: MootdxAdapter, fallback: TencentIntradayAdapter):
        self.primary = primary
        self.fallback = fallback

    def fetch_bars(self, symbol: str, interval: str, required_bars: int, *, as_of: datetime | None = None) -> FetchResult:
        if required_bars <= TENCENT_MAX_BARS and interval in {"1m", "5m"}:
            tencent = self.fallback.fetch_bars(symbol, interval, required_bars, as_of=as_of)
            if tencent.complete:
                return tencent
        return self.primary.fetch_bars(symbol, interval, required_bars, as_of=as_of)

    def fetch_quote(self, symbol: str, *, as_of: datetime | None = None, max_age_seconds: float = 180.0) -> QuoteResult:
        return self.fallback.fetch_quote(symbol, as_of=as_of, max_age_seconds=max_age_seconds)


__all__ = [
    "MarketQuote",
    "QuoteResult",
    "ResilientIntradayAdapter",
    "TENCENT_HOST",
    "TENCENT_MAX_BARS",
    "TENCENT_SOURCE_ID",
    "TencentIntradayAdapter",
    "TencentMarketDataError",
]
