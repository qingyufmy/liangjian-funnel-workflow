"""Point-in-time A2 market facts that remain separate from price proxies.

The current free implementation uses Eastmoney's published all-market capital
flow ranking through its public data endpoint.  The values are vendor derived,
not exchange-reported facts.  A missing row is never replaced with turnover,
return or attention data.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")
CAPITAL_FLOW_SCHEMA = "a2-capital-flow/1.0.0"
CAPITAL_FLOW_PROVIDER = "EASTMONEY_CAPITAL_FLOW_RANK"
PROVIDER_METHOD = "VENDOR_DERIVED"
_EASTMONEY_URL = "https://push2.eastmoney.com/api/qt/clist/get"
_EASTMONEY_UNIVERSE = "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2"
_EASTMONEY_PAGE_SIZE = 5000
_EASTMONEY_FIELDS: Mapping[str, tuple[str, str, str, str, str, str, str]] = {
    "today": ("f62", "f12,f14,f62,f184,f66,f69,f72,f75", "f62", "f184", "f72", "f75", "f69"),
    "3d": ("f267", "f12,f14,f267,f268,f269,f270,f271,f272", "f267", "f268", "f271", "f272", "f270"),
    "5d": ("f164", "f12,f14,f164,f165,f166,f167,f168,f169", "f164", "f165", "f168", "f169", "f167"),
    "10d": ("f174", "f12,f14,f174,f175,f176,f177,f178,f179", "f174", "f175", "f178", "f179", "f177"),
}
WINDOWS: tuple[tuple[str, str, float], ...] = (
    ("today", "今日", 0.35),
    ("3d", "3日", 0.25),
    ("5d", "5日", 0.25),
    ("10d", "10日", 0.15),
)


class CapitalFlowError(RuntimeError):
    """Raised for a malformed provider payload, never for an empty market."""


def collect_eastmoney_capital_flow(
    *,
    as_of: datetime,
    expected_symbols: Sequence[str],
    cache_dir: str | Path,
    fetch_rank: Callable[[str], Any] | None = None,
    now: datetime | None = None,
    minimum_coverage: float = 0.90,
) -> dict[str, Any]:
    """Load one cached trade date or collect the current all-market ranking.

    The upstream ranking does not expose a caller-selected historical date.
    Therefore a historical replay may only use a previously persisted file;
    it must not silently query the current ranking and relabel it as history.
    """

    cutoff = _aware(as_of)
    current = _aware(now or datetime.now(SHANGHAI))
    root = Path(cache_dir)
    cached = load_capital_flow_snapshot(root, cutoff.date().isoformat())
    if cached is not None:
        return cached
    if cutoff.date() != current.date():
        return unavailable_capital_flow_snapshot(
            as_of=cutoff,
            reason_code="HISTORICAL_CAPITAL_FLOW_CACHE_MISSING",
            expected_symbols=expected_symbols,
        )

    fetch = fetch_rank or _eastmoney_rank_fetcher
    frames: dict[str, Any] = {}
    failures: dict[str, str] = {}
    for key, indicator, _weight in WINDOWS:
        try:
            frames[key] = fetch(indicator)
        except Exception as exc:  # provider boundary; error text is not persisted
            failures[key] = type(exc).__name__.upper()
    snapshot = build_capital_flow_snapshot(
        frames,
        as_of=cutoff,
        expected_symbols=expected_symbols,
        minimum_coverage=minimum_coverage,
        failures=failures,
        ingested_at=current,
    )
    # Persist partial and failed observations too.  The reason code and hash
    # are required to reproduce why A2 was blocked on that trade date.
    write_capital_flow_snapshot(root, snapshot)
    return snapshot


def build_capital_flow_snapshot(
    frames: Mapping[str, Any],
    *,
    as_of: datetime,
    expected_symbols: Sequence[str],
    minimum_coverage: float = 0.90,
    failures: Mapping[str, str] | None = None,
    ingested_at: datetime | None = None,
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    ingested = _aware(ingested_at or cutoff)
    expected = tuple(dict.fromkeys(_normalize_symbol(value) for value in expected_symbols if _normalize_symbol(value)))
    normalized: dict[str, dict[str, dict[str, Any]]] = {}
    window_diagnostics: dict[str, Any] = {}
    failures = dict(failures or {})
    for key, _indicator, _weight in WINDOWS:
        rows = _records(frames.get(key)) if key in frames else ()
        by_symbol: dict[str, dict[str, Any]] = {}
        invalid_rows = 0
        for row in rows:
            symbol = _normalize_symbol(_pick(row, "代码", "股票代码", "symbol", "ts_code", "thscode"))
            if not symbol:
                invalid_rows += 1
                continue
            ratio = _number(_pick_flow(row, key, "主力净流入-净占比", "主力净流入占比", "net_inflow_ratio"))
            amount = _number(_pick_flow(row, key, "主力净流入-净额", "主力净流入净额", "net_inflow_amount"))
            large_ratio = _number(_pick_flow(row, key, "大单净流入-净占比", "大单净流入占比", "large_inflow_ratio"))
            super_ratio = _number(_pick_flow(row, key, "超大单净流入-净占比", "超大单净流入占比", "super_inflow_ratio"))
            if ratio is None and amount is None:
                invalid_rows += 1
                continue
            by_symbol[symbol] = {
                "symbol": symbol,
                "name": str(_pick(row, "名称", "股票简称", "name") or ""),
                "net_inflow_amount_cny": amount,
                "net_inflow_ratio_pct": ratio,
                "large_inflow_ratio_pct": large_ratio,
                "super_inflow_ratio_pct": super_ratio,
            }
        ratios = {
            symbol: values["net_inflow_ratio_pct"]
            for symbol, values in by_symbol.items()
            if values.get("net_inflow_ratio_pct") is not None
        }
        percentiles = _percentiles(ratios)
        for symbol, score in percentiles.items():
            by_symbol[symbol]["cross_section_percentile"] = score
        normalized[key] = by_symbol
        observed_expected = len(set(expected).intersection(by_symbol))
        coverage = observed_expected / len(expected) if expected else 1.0
        window_diagnostics[key] = {
            "available": bool(by_symbol),
            "availability_state": "OBSERVED_VALUE" if by_symbol else "SOURCE_FAILED",
            "reason_code": "OK" if by_symbol else failures.get(key, "SOURCE_EMPTY"),
            "provider_record_count": len(by_symbol),
            "invalid_row_count": invalid_rows,
            "eligible_universe_count": len(expected),
            "observed_eligible_count": observed_expected,
            "coverage_ratio": round(coverage, 6),
        }

    by_symbol_output: dict[str, dict[str, Any]] = {}
    for symbol in expected:
        metrics: dict[str, Any] = {}
        weighted = 0.0
        available_weight = 0.0
        source_refs: list[str] = []
        for key, _indicator, weight in WINDOWS:
            value = normalized.get(key, {}).get(symbol)
            if value is None:
                metrics[key] = {
                    # A capital-flow ranking is not an event set.  A missing
                    # eligible symbol therefore does not prove zero flow or
                    # an observed absence; it means the supposedly complete
                    # provider cross-section cannot support this symbol.
                    "availability_state": "SOURCE_FAILED",
                    "reason_code": failures.get(key, "SYMBOL_MISSING_FROM_PROVIDER_CROSS_SECTION"),
                }
                continue
            score = _number(value.get("cross_section_percentile"))
            metrics[key] = {**value, "availability_state": "OBSERVED_VALUE", "reason_code": "OK"}
            if score is not None:
                weighted += score * weight
                available_weight += weight
                source_refs.append(f"eastmoney:capital-flow:{cutoff.date().isoformat()}:{key}")
        today = metrics.get("today", {})
        today_observed = today.get("availability_state") == "OBSERVED_VALUE"
        score = weighted / available_weight if today_observed and available_weight > 0 else None
        by_symbol_output[symbol] = {
            "symbol": symbol,
            "available": score is not None,
            "availability_state": "OBSERVED_VALUE" if score is not None else today.get("availability_state", "SOURCE_FAILED"),
            "reason_code": "OK" if score is not None else str(today.get("reason_code") or "CAPITAL_FLOW_UNAVAILABLE"),
            "capital_flow_score": round(score, 4) if score is not None else None,
            "available_weight": round(available_weight, 4),
            "metrics": metrics,
            "source_refs": source_refs,
        }

    today_coverage = float(window_diagnostics.get("today", {}).get("coverage_ratio") or 0.0)
    available = bool(normalized.get("today")) and today_coverage >= minimum_coverage
    reason = "OK" if available and not failures else "PARTIAL_WINDOWS" if available else "CAPITAL_FLOW_COVERAGE_INSUFFICIENT"
    payload: dict[str, Any] = {
        "schema_version": CAPITAL_FLOW_SCHEMA,
        "available": available,
        "reason_code": reason,
        "source_id": CAPITAL_FLOW_PROVIDER,
        "source_tier": "T2",
        "provider_method": PROVIDER_METHOD,
        "authority_class": "VENDOR_DERIVED_NOT_EXCHANGE_FACT",
        "turnover_is_capital_flow": False,
        "trade_date": cutoff.date().isoformat(),
        "as_of": cutoff.isoformat(),
        "ingested_at": ingested.isoformat(),
        "minimum_coverage": minimum_coverage,
        "coverage_by_window": window_diagnostics,
        "failures": failures,
        "by_symbol": by_symbol_output,
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def unavailable_capital_flow_snapshot(
    *,
    as_of: datetime,
    reason_code: str,
    expected_symbols: Sequence[str],
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    payload: dict[str, Any] = {
        "schema_version": CAPITAL_FLOW_SCHEMA,
        "available": False,
        "reason_code": reason_code,
        "source_id": CAPITAL_FLOW_PROVIDER,
        "source_tier": "T2",
        "provider_method": PROVIDER_METHOD,
        "authority_class": "VENDOR_DERIVED_NOT_EXCHANGE_FACT",
        "turnover_is_capital_flow": False,
        "trade_date": cutoff.date().isoformat(),
        "as_of": cutoff.isoformat(),
        "minimum_coverage": 0.90,
        "coverage_by_window": {},
        "by_symbol": {
            symbol: {
                "symbol": symbol,
                "available": False,
                "availability_state": "NOT_CONFIGURED" if reason_code == "SOURCE_NOT_CONFIGURED" else "SOURCE_FAILED",
                "reason_code": reason_code,
                "capital_flow_score": None,
                "source_refs": [],
            }
            for value in expected_symbols
            if (symbol := _normalize_symbol(value))
        },
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def write_capital_flow_snapshot(cache_dir: str | Path, snapshot: Mapping[str, Any]) -> Path:
    root = Path(cache_dir)
    trade_date = str(snapshot.get("trade_date") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", trade_date):
        raise CapitalFlowError("capital-flow snapshot requires an ISO trade_date")
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"capital-flow-{trade_date}.json"
    atomic_write_json(path, dict(snapshot))
    return path


def load_capital_flow_snapshot(cache_dir: str | Path, trade_date: str) -> dict[str, Any] | None:
    path = Path(cache_dir) / f"capital-flow-{trade_date}.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("schema_version") != CAPITAL_FLOW_SCHEMA:
        return None
    expected = str(payload.get("content_hash") or "")
    body = dict(payload)
    body.pop("content_hash", None)
    if not expected or expected != _content_hash(body):
        return None
    return payload


def _eastmoney_rank_fetcher(indicator: str) -> list[dict[str, Any]]:
    """Fetch the complete vendor cross-section in bounded, retried pages.

    Calling the vendor endpoint directly keeps the transport contract explicit
    while the persisted fact remains labelled vendor-derived rather than an
    exchange fact.
    """

    import requests  # lazy: doctor and historical replay remain network-free
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    key = {"今日": "today", "3日": "3d", "5日": "5d", "10日": "10d"}.get(indicator)
    if key is None:
        raise CapitalFlowError("unsupported capital-flow window")
    fid, fields, amount_field, ratio_field, large_amount_field, large_ratio_field, super_ratio_field = _EASTMONEY_FIELDS[key]
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; LiangjianResearch/2.0)",
        "Referer": "https://data.eastmoney.com/",
    }
    rows: list[dict[str, Any]] = []
    page = 1
    total = None
    while total is None or len(rows) < total:
        response = session.get(
            _EASTMONEY_URL,
            params={
                "fid": fid,
                "po": "1",
                # The endpoint accepts a large bounded page.  A full A-share
                # cross-section then needs about two pages per window instead
                # of roughly sixty, while the loop still follows the provider
                # total if it applies a smaller server-side cap.
                "pz": str(_EASTMONEY_PAGE_SIZE),
                "pn": str(page),
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "ut": "b2884a393a59ad64002292a3e90d46a5",
                "fs": _EASTMONEY_UNIVERSE,
                "fields": fields,
            },
            headers=headers,
            timeout=(5, 20),
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, Mapping) or not isinstance(data.get("diff"), list):
            raise CapitalFlowError("capital-flow provider envelope invalid")
        total = int(data.get("total") or 0)
        page_rows = [item for item in data["diff"] if isinstance(item, Mapping)]
        if not page_rows and len(rows) < total:
            raise CapitalFlowError("capital-flow provider page incomplete")
        for item in page_rows:
            rows.append({
                "symbol": item.get("f12"),
                "name": item.get("f14"),
                "net_inflow_amount": item.get(amount_field),
                "net_inflow_ratio": item.get(ratio_field),
                "large_inflow_amount": item.get(large_amount_field),
                "large_inflow_ratio": item.get(large_ratio_field),
                "super_inflow_ratio": item.get(super_ratio_field),
            })
        page += 1
        if page > 100:
            raise CapitalFlowError("capital-flow provider page bound exceeded")
    return rows


def _records(value: Any) -> tuple[Mapping[str, Any], ...]:
    if value is None:
        return ()
    if hasattr(value, "to_dict"):
        try:
            value = value.to_dict(orient="records")
        except TypeError:
            value = value.to_dict("records")
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(item for item in value if isinstance(item, Mapping))
    raise CapitalFlowError("capital-flow provider result must be a record collection")


def _pick(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _pick_flow(row: Mapping[str, Any], window: str, *suffixes: str) -> Any:
    prefix = {"today": "今日", "3d": "3日", "5d": "5日", "10d": "10日"}[window]
    aliases: list[str] = []
    for suffix in suffixes:
        aliases.extend((f"{prefix}{suffix}", suffix))
    return _pick(row, *aliases)


def _normalize_symbol(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", raw):
        return raw
    raw_digits = re.sub(r"\D", "", raw)
    if not raw_digits or len(raw_digits) > 6:
        return ""
    digits = raw_digits.zfill(6)
    if not re.fullmatch(r"\d{6}", digits) or digits == "000000":
        return ""
    exchange = "SH" if digits.startswith(("5", "6", "9")) else "BJ" if digits.startswith(("4", "8")) else "SZ"
    return f"{digits}.{exchange}"


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text in {"-", "--", "None", "nan", "NaN"}:
        return None
    multiplier = 1.0
    if text.endswith("亿"):
        multiplier, text = 100_000_000.0, text[:-1]
    elif text.endswith("万"):
        multiplier, text = 10_000.0, text[:-1]
    try:
        number = float(text) * multiplier
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _percentiles(values: Mapping[str, float]) -> dict[str, float]:
    ordered = sorted((float(value), symbol) for symbol, value in values.items() if math.isfinite(float(value)))
    if not ordered:
        return {}
    if len(ordered) == 1:
        return {ordered[0][1]: 50.0}
    result: dict[str, float] = {}
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][0] == ordered[index][0]:
            end += 1
        average_rank = (index + end - 1) / 2.0
        score = average_rank / (len(ordered) - 1) * 100.0
        for _value, symbol in ordered[index:end]:
            result[symbol] = round(score, 4)
        index = end
    return result


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CapitalFlowError("A2 market timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _content_hash(value: Mapping[str, Any]) -> str:
    body = dict(value)
    body.pop("content_hash", None)
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


__all__ = [
    "CAPITAL_FLOW_PROVIDER",
    "CAPITAL_FLOW_SCHEMA",
    "CapitalFlowError",
    "build_capital_flow_snapshot",
    "collect_eastmoney_capital_flow",
    "load_capital_flow_snapshot",
    "unavailable_capital_flow_snapshot",
    "write_capital_flow_snapshot",
]
