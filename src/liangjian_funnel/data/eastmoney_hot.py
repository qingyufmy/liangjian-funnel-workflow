"""Strict point-in-time Eastmoney stock-forum popularity snapshots.

The vendor endpoint is useful as an emotion-attention source, but it is not a
documented exchange interface.  This adapter therefore validates the complete
top-100 contract before publishing a snapshot and never re-labels yesterday's
cache as today's observation.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")
EASTMONEY_HOT100_SCHEMA = "eastmoney-guba-hot100/1.0.0"
EASTMONEY_HOT100_SOURCE = "EASTMONEY_GUBA_POPULARITY_TOP100"
EASTMONEY_HOT100_URL = "https://np-tjxg-g.eastmoney.com/api/smart-tag/stock/v3/pw/search-code"
EASTMONEY_HOT100_REFERER = "https://xuangu.eastmoney.com/"


class EastmoneyHot100Error(RuntimeError):
    """Raised when the vendor response cannot prove a complete same-day top 100."""


def collect_eastmoney_hot100(
    *,
    as_of: datetime,
    cache_dir: str | Path,
    fetch: Callable[[Mapping[str, Any]], Any] | None = None,
    expected_trade_date: date | None = None,
    max_attempts: int = 3,
    retry_wait_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Return a validated top-100 snapshot or an explicit unavailable state.

    A cache hit is accepted only for ``expected_trade_date``.  This is
    deliberate: popularity changes intraday and a previous trading day's list
    is historical evidence, not a current emotion signal.
    """

    cutoff = _aware(as_of)
    trade_day = expected_trade_date or cutoff.date()
    target_dir = Path(cache_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    cache_path = target_dir / f"eastmoney-guba-hot100-{trade_day.isoformat()}.json"
    cached = _load_cache(cache_path, trade_day)
    if cached is not None:
        return {**cached, "cache_status": "HIT", "cache_path": str(cache_path)}

    last_reason = "SOURCE_UNAVAILABLE"
    fetcher = fetch or _fetch_payload
    for attempt in range(1, max(1, max_attempts) + 1):
        try:
            normalized = normalize_eastmoney_hot100(
                fetcher(_request_body()),
                as_of=cutoff,
                expected_trade_date=trade_day,
            )
        except (EastmoneyHot100Error, httpx.HTTPError, OSError, TypeError, ValueError) as exc:
            last_reason = str(exc) or "SOURCE_UNAVAILABLE"
            if attempt < max_attempts:
                sleep(retry_wait_seconds * attempt)
            continue
        atomic_write_json(cache_path, normalized)
        return {**normalized, "cache_status": "MISS", "cache_path": str(cache_path)}
    return unavailable_eastmoney_hot100(cutoff, last_reason)


def normalize_eastmoney_hot100(
    payload: Any,
    *,
    as_of: datetime,
    expected_trade_date: date,
) -> dict[str, Any]:
    root = payload if isinstance(payload, Mapping) else {}
    if int(root.get("code") or 0) != 100:
        raise EastmoneyHot100Error("EASTMONEY_HOT100_PROVIDER_REJECTED")
    data = root.get("data")
    result = data.get("result") if isinstance(data, Mapping) else None
    rows = result.get("dataList") if isinstance(result, Mapping) else None
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        raise EastmoneyHot100Error("EASTMONEY_HOT100_ROWS_MALFORMED")

    rank_key = f"GUBA_TOP_REAL_TIME{{{expected_trade_date.isoformat()}}}"
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise EastmoneyHot100Error("EASTMONEY_HOT100_ROW_MALFORMED")
        provider_rank_keys = [str(key) for key in row if str(key).startswith("GUBA_TOP_REAL_TIME")]
        if rank_key not in row:
            if provider_rank_keys:
                raise EastmoneyHot100Error("EASTMONEY_HOT100_TRADE_DATE_MISMATCH")
            raise EastmoneyHot100Error("EASTMONEY_HOT100_RANK_MISSING")
        rank = _integer(row.get(rank_key))
        code = str(row.get("SECURITY_CODE") or "").strip()
        symbol = _a_share_symbol(code, str(row.get("MARKET_SHORT_NAME") or ""))
        name = str(row.get("SECURITY_SHORT_NAME") or "").strip()
        if rank is None or symbol is None or not name:
            raise EastmoneyHot100Error("EASTMONEY_HOT100_IDENTITY_MALFORMED")
        normalized.append({
            "rank": rank,
            "symbol": symbol,
            "code": code,
            "name": name,
            "market": str(row.get("MARKET_SHORT_NAME") or "").strip(),
            "latest_price": _number(row.get("NEWEST_PRICE")),
            "change_pct": _number(row.get("CHG")),
            "turnover_rate_pct": _number(row.get("TURNOVER_RATE")),
            "volume_ratio": _number(row.get("QRR")),
            "trading_volume": _number(row.get("TRADING_VOLUMES")),
            "dynamic_pe": _number(row.get("PE_DYNAMIC")),
            "pb": _number(row.get("PB")),
            "total_market_value": _number(row.get("TOAL_MARKET_VALUE<140>")),
            "circulating_market_value": _number(row.get("CIRCULATION_MARKET_VALUE<140>")),
        })

    normalized.sort(key=lambda item: (int(item["rank"]), str(item["symbol"])))
    ranks = [int(item["rank"]) for item in normalized]
    symbols = [str(item["symbol"]) for item in normalized]
    if len(normalized) != 100 or ranks != list(range(1, 101)) or len(set(symbols)) != 100:
        raise EastmoneyHot100Error("EASTMONEY_HOT100_INCOMPLETE")
    content_hash = hashlib.sha256(
        json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": EASTMONEY_HOT100_SCHEMA,
        "source_id": EASTMONEY_HOT100_SOURCE,
        "source_url": EASTMONEY_HOT100_URL,
        "available": True,
        "reason_code": "OK",
        "as_of": _aware(as_of).isoformat(),
        "trade_date": expected_trade_date.isoformat(),
        "record_count": 100,
        "records": normalized,
        "content_hash": content_hash,
        "point_in_time": True,
        "previous_day_fallback_forbidden": True,
    }


def unavailable_eastmoney_hot100(as_of: datetime, reason_code: str) -> dict[str, Any]:
    return {
        "schema_version": EASTMONEY_HOT100_SCHEMA,
        "source_id": EASTMONEY_HOT100_SOURCE,
        "source_url": EASTMONEY_HOT100_URL,
        "available": False,
        "reason_code": _reason(reason_code),
        "as_of": _aware(as_of).isoformat(),
        "record_count": 0,
        "records": [],
        "point_in_time": True,
        "previous_day_fallback_forbidden": True,
    }


def _fetch_payload(body: Mapping[str, Any]) -> Any:
    with httpx.Client(timeout=12.0, follow_redirects=True) as client:
        response = client.post(
            EASTMONEY_HOT100_URL,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=UTF-8",
                "Origin": "https://xuangu.eastmoney.com",
                "Referer": EASTMONEY_HOT100_REFERER,
                "User-Agent": "Mozilla/5.0",
            },
            json=dict(body),
        )
        response.raise_for_status()
        return response.json()


def _request_body() -> dict[str, Any]:
    condition = {
        "params": [{"paramInfos": [{"children": [], "paramId": 233, "optionName": "前100名"}], "paramGroupId": 1}],
        "keyCode": "10317",
        "id": 10317,
        "name": "股吧人气排名前100名",
        "label": "股吧人气排名",
        "detail": "股吧人气排名前100名",
        "desc": "前100名",
    }
    return {
        "needAmbiguousSuggest": True,
        "pageSize": 100,
        "pageNo": 1,
        "fingerprint": uuid.uuid4().hex,
        "matchWord": "",
        "shareToGuba": False,
        "timestamp": f"{int(time.time() * 1000)}000",
        "requestId": uuid.uuid4().hex,
        "removedConditionIdList": [],
        "ownSelectAll": False,
        "needCorrect": True,
        "client": "WEB",
        "product": "",
        "needShowStockNum": False,
        "biz": "web_ai_select_stocks",
        "gids": [],
        "dxInfoNew": [condition],
        "keyWordNew": "股吧人气排名前100名;",
        "customDataNew": json.dumps([condition], ensure_ascii=False, separators=(",", ":")),
    }


def _load_cache(path: Path, trade_day: date) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, Mapping) or value.get("trade_date") != trade_day.isoformat():
        return None
    if value.get("available") is not True or value.get("record_count") != 100:
        return None
    records = value.get("records")
    if not isinstance(records, list) or len(records) != 100:
        return None
    if any(not isinstance(item, Mapping) for item in records):
        return None
    if [item.get("rank") for item in records] != list(range(1, 101)):
        return None
    symbols = [str(item.get("symbol") or "").strip().upper() for item in records]
    if len(set(symbols)) != 100 or any(_a_share_symbol(symbol.split(".", 1)[0], symbol.split(".", 1)[-1]) != symbol for symbol in symbols):
        return None
    expected_hash = hashlib.sha256(
        json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if value.get("content_hash") != expected_hash:
        return None
    return dict(value)


def _a_share_symbol(code: str, market: str) -> str | None:
    if len(code) != 6 or not code.isdigit():
        return None
    text = market.upper()
    if code.startswith(("4", "8", "9")) or "北" in market or "BJ" in text:
        suffix = "BJ"
    elif code.startswith(("5", "6", "9")) or "沪" in market or "SH" in text:
        suffix = "SH"
    elif code.startswith(("0", "1", "2", "3")) or "深" in market or "SZ" in text:
        suffix = "SZ"
    else:
        return None
    return f"{code}.{suffix}"


def _integer(value: Any) -> int | None:
    number = _number(value)
    if number is None or not float(number).is_integer():
        return None
    return int(number)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _reason(value: str) -> str:
    text = str(value or "SOURCE_UNAVAILABLE").strip().upper().replace(" ", "_")
    return text if text and len(text) <= 120 else "SOURCE_UNAVAILABLE"
