"""Optional open-data adapter for the monthly A1 macro context.

The workflow keeps the deterministic selection layer independent from any one
vendor.  This module is a deliberately small adapter around AKShare (or an
injected provider in tests) and emits the four snapshot contracts consumed by
the monthly strategy:

* ``MACRO_ECONOMIC_DATA``
* ``ASSET_ROTATION_SNAPSHOT``
* ``GLOBAL_MACRO_SNAPSHOT``
* ``CROSS_MARKET_LEAD_SNAPSHOT``
* ``INDUSTRY_ACTIVITY_DATA``

AKShare is imported only when collection starts.  Provider/API failures are
represented as ``SOURCE_UNAVAILABLE`` records; callers do not have to catch a
network exception just to keep the rest of a research run alive.  The adapter
also applies an explicit point-in-time gate: a row with a publication time
after ``as_of`` is discarded, and a row with only an observation date is
usable only when that date is not after ``as_of``.

No missing numeric value is changed to zero.  Numeric fields remain ``None``
until a source supplies a value, and every emitted snapshot contains source
and content-integrity metadata.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
SCHEMA_VERSION = "open-macro-contract/1.0.0"
SOURCE_ID = "akshare_open"
DEFAULT_ETFS: dict[str, dict[str, str]] = {
    "EQUITY": {"symbol": "510300", "name": "沪深300ETF"},
    "GOLD": {"symbol": "518880", "name": "黄金ETF"},
    "BOND": {"symbol": "511010", "name": "国债ETF"},
    "CASH": {"symbol": "511880", "name": "货币ETF"},
}
_MISSING = object()
_SPACE = re.compile(r"\s+")
_NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")


class SourceUnavailable(RuntimeError):
    """Internal marker used to separate a provider failure from bad rows."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _content_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _aware(value: datetime | date | str | None) -> datetime:
    if value is None:
        return datetime.now(SHANGHAI)
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day, 23, 59, 59, tzinfo=SHANGHAI)
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if not raw:
            raise ValueError("as_of cannot be empty")
        parsed = _parse_time(raw)
        if parsed is None:
            raise ValueError(f"invalid as_of: {value!r}")
        if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", raw.replace("/", "-")):
            parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999999)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=SHANGHAI)
    return parsed.astimezone(SHANGHAI)


def _parse_time(value: Any) -> datetime | None:
    """Parse common API date/month formats without guessing future times."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=SHANGHAI)
        return parsed.astimezone(SHANGHAI)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=SHANGHAI)
    raw = str(value).strip()
    if not raw:
        return None
    raw = raw.replace("月份", "月")
    raw = raw.replace("年", "-").replace("月", "-").replace("日", "")
    raw = raw.replace("/", "-").replace(".", "-")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    month_match = re.fullmatch(r"(\d{4})-(\d{1,2})-?", raw)
    if month_match:
        try:
            return datetime(int(month_match.group(1)), int(month_match.group(2)), 1, tzinfo=SHANGHAI)
        except ValueError:
            return None
    # A Chinese month such as 2026-08-01 is an observation date.  The
    # distinction between date and datetime is retained by the PIT gate below.
    for candidate in (raw, raw[:10]):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=SHANGHAI)
        return parsed.astimezone(SHANGHAI)
    if re.fullmatch(r"\d{6}", raw):
        try:
            return datetime(int(raw[:4]), int(raw[4:]), 1, tzinfo=SHANGHAI)
        except ValueError:
            return None
    # Unix seconds/milliseconds are occasionally returned by index adapters.
    try:
        number = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    if number >= 100_000_000_000:
        number /= 1000
    try:
        return datetime.fromtimestamp(number, tz=SHANGHAI)
    except (OverflowError, OSError, ValueError):
        return None


def _pit_allowed(observation_time: datetime | None, publication_time: datetime | None, as_of: datetime) -> bool:
    """Return whether a row can be known at ``as_of``.

    Providers often expose only a month/date for macro observations.  In that
    case the calendar date is the conservative PIT boundary.  When a source
    has a publication timestamp it takes precedence and is checked exactly.
    """

    if publication_time is not None:
        return publication_time <= as_of
    if observation_time is not None:
        return observation_time.date() <= as_of.date()
    return False


def _rows(value: Any) -> list[dict[str, Any]]:
    """Convert pandas-like or JSON-like provider output to row mappings."""

    if value is None:
        return []
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        for kwargs in ({"orient": "records"}, {}):
            try:
                converted = to_dict(**kwargs)
            except TypeError:
                continue
            except Exception:
                return []
            if converted is not value:
                return _rows(converted)
    if isinstance(value, Mapping):
        for key in ("data", "rows", "items", "result", "records"):
            nested = value.get(key)
            if isinstance(nested, (Sequence, Mapping)) and not isinstance(nested, (str, bytes, bytearray)):
                return _rows(nested)
        return [dict(value)]
    if isinstance(value, (str, bytes, bytearray)):
        return []
    if isinstance(value, Iterable):
        result: list[dict[str, Any]] = []
        for item in value:
            if isinstance(item, Mapping):
                result.append(dict(item))
        return result
    return []


def _norm_key(key: Any) -> str:
    return _SPACE.sub("", str(key)).lower().replace("_", "").replace("-", "")


def _find_value(row: Mapping[str, Any], names: Sequence[str], *, contains: Sequence[str] = ()) -> Any:
    normalized = {_norm_key(key): value for key, value in row.items()}
    for name in names:
        value = normalized.get(_norm_key(name), _MISSING)
        if value is not _MISSING:
            return value
    if contains:
        normalized_contains = tuple(_norm_key(part) for part in contains)
        for key, value in normalized.items():
            if all(part in key for part in normalized_contains):
                return value
    return None


def _row_time(row: Mapping[str, Any]) -> datetime | None:
    value = _find_value(
        row,
        (
            "observation_time", "observationTime", "统计时间", "报告期",
            "日期", "月份", "month", "date", "trade_date", "trading_date",
            # Publication-only rows still remain usable as-of their
            # publication timestamp, but never outrank an observation date.
            "publish_time", "publishTime", "发布时间", "发布日期", "发表日期",
        ),
    )
    return _parse_time(value)


def _row_publication_time(row: Mapping[str, Any]) -> datetime | None:
    value = _find_value(row, ("publish_time", "publishTime", "发布时间", "发布日期", "发表日期"))
    return _parse_time(value)


def _number(value: Any) -> float | None:
    """Parse a provider scalar while preserving nulls and invalid values."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        raw = str(value).strip().replace(",", "")
        if not raw or raw.lower() in {"nan", "none", "null", "--", "-", "n/a"}:
            return None
        match = _NUMBER_RE.search(raw)
        if match is None:
            return None
        try:
            number = float(match.group(0))
        except ValueError:
            return None
        if "%" in raw:
            # Keep percentage values in percentage points, e.g. 2.4 rather
            # than 0.024.  The contracts explicitly label the unit.
            number = float(number)
    if not math.isfinite(number):
        return None
    return number


def _round(value: float | None, digits: int = 8) -> float | None:
    return None if value is None else round(float(value), digits)


def _source_record(
    source_ref: str,
    *,
    status: str,
    records: int = 0,
    reason_code: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "source_ref": source_ref,
        "status": status,
        "records": records,
    }
    if reason_code:
        result["reason_code"] = reason_code
    if error:
        # Provider exception text is not a fact and can contain URLs/query
        # strings; only retain a short class-level diagnostic.
        result["error_type"] = error[:120]
    return result


def _empty_contract(
    contract: str,
    *,
    as_of: datetime,
    source_refs: Sequence[str] = (),
    source_manifest: Sequence[Mapping[str, Any]] = (),
    reason_code: str = "SOURCE_UNAVAILABLE",
    quality: str = "UNAVAILABLE",
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract": contract,
        "available": False,
        "reason_code": reason_code,
        "as_of": as_of.isoformat(),
        "source_refs": list(source_refs),
        "source_manifest": [dict(item) for item in source_manifest],
        "freshness": {"as_of": as_of.isoformat(), "latest_observation": None, "age_days": None},
        "quality": quality,
    }
    if extra:
        payload.update(dict(extra))
    payload["content_hash"] = _content_hash(payload)
    return payload


def _finalize_contract(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("content_hash", None)
    result["content_hash"] = _content_hash(result)
    return result


def _freshness(times: Sequence[datetime], as_of: datetime) -> dict[str, Any]:
    if not times:
        return {"as_of": as_of.isoformat(), "latest_observation": None, "age_days": None}
    latest = max(times)
    return {
        "as_of": as_of.isoformat(),
        "latest_observation": latest.isoformat(),
        "age_days": max(0, (as_of.date() - latest.date()).days),
    }


def _percentile(value: float | None, values: Sequence[float]) -> float | None:
    if value is None:
        return None
    clean = sorted(item for item in values if math.isfinite(item))
    if not clean:
        return None
    if len(clean) == 1:
        return 50.0
    rank = sum(1 for item in clean if item <= value)
    return round((rank - 1) / (len(clean) - 1) * 100.0, 4)


def _call_provider(provider: Any, names: Sequence[str], *, kwargs_variants: Sequence[Mapping[str, Any]] = ()) -> tuple[Any, str]:
    """Call the first available provider method, preserving a useful source ref."""

    found = False
    last_error: Exception | None = None
    for name in names:
        function = getattr(provider, name, None)
        if not callable(function):
            continue
        found = True
        variants = list(kwargs_variants) or [{}]
        for kwargs in variants:
            try:
                return function(**dict(kwargs)), f"{SOURCE_ID}:{name}"
            except TypeError as exc:
                # A fake provider or an AKShare version may not accept all
                # optional arguments.  Try the next conservative signature.
                last_error = exc
                continue
            except Exception as exc:  # provider/network failures are data
                last_error = exc
                break
        # An alias may be available even when an earlier AKShare endpoint was
        # removed or changed its signature, so continue to the next name.
    if not found:
        raise SourceUnavailable("METHOD_NOT_CONFIGURED")
    raise SourceUnavailable(type(last_error).__name__ if last_error else "SOURCE_UNAVAILABLE")


def _provider_for(provider: Any | None) -> Any | None:
    if provider is not None:
        return provider
    try:
        import akshare as ak  # type: ignore[import-not-found]
    except Exception:
        return None
    return ak


def _macro_value_spec(series_id: str) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    if series_id == "PMI":
        return (("制造业-指数", "制造业指数", "PMI", "制造业PMI", "指数", "当月"), ("PMI", "制造业"), "index")
    if series_id == "CPI":
        return (("全国-当月同比", "全国-同比", "当月同比", "同比", "CPI"), ("全国", "同比"), "percent")
    if series_id == "PPI":
        return (("当月同比", "全国-同比", "同比", "PPI"), ("同比",), "percent")
    if series_id == "M1_YOY":
        return (("货币(M1)-同比", "M1同比", "M1_YOY", "M1"), ("M1", "同比"), "percent")
    if series_id == "M2_YOY":
        return (("货币和准货币(M2)-同比", "M2同比", "M2_YOY", "M2"), ("M2", "同比"), "percent")
    if series_id == "SOCIAL_FINANCING":
        return (("社会融资规模增量", "社会融资规模", "社融", "SOCIAL_FINANCING"), ("社会融资",), "billion_cny")
    return (("新增人民币贷款", "新增信贷", "人民币贷款增量", "NEW_CREDIT", "当月"), ("贷款",), "billion_cny")


def _extract_series(
    series_id: str,
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime,
    source_ref: str,
) -> tuple[list[dict[str, Any]], list[datetime]]:
    names, contains, unit = _macro_value_spec(series_id)
    points: list[dict[str, Any]] = []
    times: list[datetime] = []
    for row in raw_rows:
        observation = _row_time(row)
        publication = _row_publication_time(row)
        if not _pit_allowed(observation, publication, as_of):
            continue
        value = _number(_find_value(row, names, contains=contains))
        if value is None:
            continue
        timestamp = observation or publication
        if timestamp is None:
            continue
        points.append(
            {
                "id": series_id,
                "value": _round(value),
                "unit": unit,
                "observation_time": timestamp.isoformat(),
                "publish_time": publication.isoformat() if publication else None,
                "publish_time_available": publication is not None,
                "source_ref": source_ref,
                "pit_verified": publication is not None and publication <= as_of,
                "pit_status": "STRICT_PIT" if publication is not None and publication <= as_of else "OBSERVATION_DATE_ONLY",
            }
        )
        times.append(timestamp)
    # A provider can return duplicate revisions for the same period.  Keep the
    # last source row after deterministic sorting, never mix future revisions.
    points.sort(key=lambda item: (str(item["observation_time"]), str(item.get("publish_time") or "")))
    deduped: dict[str, dict[str, Any]] = {}
    for item in points:
        deduped[str(item["observation_time"])] = item
    return list(deduped.values()), times


def build_macro_economic_data(
    datasets: Mapping[str, Any],
    *,
    as_of: datetime | date | str,
    source_manifest: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Purely transform normalized provider datasets into the macro contract."""

    cutoff = _aware(as_of)
    series: list[dict[str, Any]] = []
    times: list[datetime] = []
    for series_id in ("PMI", "CPI", "PPI", "M1_YOY", "M2_YOY", "SOCIAL_FINANCING", "NEW_CREDIT"):
        entry = datasets.get(series_id)
        if not isinstance(entry, Mapping):
            continue
        rows = entry.get("rows")
        if not isinstance(rows, Sequence):
            rows = _rows(rows)
        ref = str(entry.get("source_ref") or f"{SOURCE_ID}:{series_id}")
        extracted, extracted_times = _extract_series(series_id, rows, as_of=cutoff, source_ref=ref)
        series.extend(extracted)
        times.extend(extracted_times)

    latest: dict[str, dict[str, Any]] = {}
    history: dict[str, list[float]] = {}
    for point in series:
        latest[point["id"]] = point
        history.setdefault(point["id"], []).append(float(point["value"]))
    latest_values = {key: item.get("value") for key, item in latest.items()}
    m1 = _number(latest_values.get("M1_YOY"))
    m2 = _number(latest_values.get("M2_YOY"))
    gap = None if m1 is None or m2 is None else _round(m1 - m2)
    gap_history: list[float] = []
    m1_points = {point["observation_time"]: float(point["value"]) for point in series if point["id"] == "M1_YOY"}
    m2_points = {point["observation_time"]: float(point["value"]) for point in series if point["id"] == "M2_YOY"}
    for timestamp in sorted(set(m1_points) & set(m2_points)):
        gap_history.append(m1_points[timestamp] - m2_points[timestamp])
    credit_id = "SOCIAL_FINANCING" if "SOCIAL_FINANCING" in latest else "NEW_CREDIT"
    credit_history = history.get(credit_id, [])
    credit_change = None
    credit_change_history: list[float] = []
    if len(credit_history) >= 2:
        credit_change = _round(credit_history[-1] - credit_history[-2])
        credit_change_history = [credit_history[index] - credit_history[index - 1] for index in range(1, len(credit_history))]
    required = ("PMI", "CPI", "PPI", "M1_YOY", "M2_YOY")
    required_available = all(key in latest for key in required)
    credit_available = bool(credit_id in latest)
    available = required_available and credit_available
    reason = "OK" if available else "PARTIAL_DATA" if latest else "SOURCE_UNAVAILABLE"
    values = {
        "PMI": latest_values.get("PMI"),
        "CPI": latest_values.get("CPI"),
        "PPI": latest_values.get("PPI"),
        "m1_yoy": m1,
        "m2_yoy": m2,
        "m1_m2_gap": gap,
        "m1_m2_gap_percentile": _percentile(gap, gap_history),
        "credit_impulse": credit_change,
        "credit_impulse_percentile": _percentile(credit_change, credit_change_history),
        "credit_series": credit_id if credit_available else None,
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract": "MACRO_ECONOMIC_DATA",
        "available": available,
        "reason_code": reason,
        "as_of": cutoff.isoformat(),
        "series": series,
        "latest": latest,
        "values": values,
        "source_refs": sorted({str(point["source_ref"]) for point in series}),
        "source_manifest": [dict(item) for item in source_manifest],
        "freshness": _freshness(times, cutoff),
        "quality": (
            "T2_OPEN_AGGREGATED"
            if available and series and all(point.get("pit_verified") is True for point in series)
            else "T2_OPEN_OBSERVATION_DATE_ONLY"
            if latest
            else "UNAVAILABLE"
        ),
        "rules": {"point_in_time": True, "missing_values_are_not_zero": True, "publication_time_preferred": True},
    }
    return _finalize_contract(payload)


def _industry_rows(raw: Any) -> list[dict[str, Any]]:
    """Normalize both records-shaped and NBS index x month tables.

    ``macro_china_nbs_nation`` currently returns industries in the DataFrame
    index and months in columns (rather than one record per observation).
    Converting that layout here keeps the pure builder independent of pandas
    while preserving the index label that carries the metric suffix.
    """

    frame_index = getattr(raw, "index", None)
    frame_columns = getattr(raw, "columns", None)
    iloc = getattr(raw, "iloc", None)
    if frame_index is None or frame_columns is None or iloc is None:
        return _rows(raw)
    try:
        index_values = list(frame_index)
        column_values = list(frame_columns)
    except TypeError:
        return _rows(raw)
    if not index_values or not column_values:
        return _rows(raw)
    result: list[dict[str, Any]] = []
    for row_index, label in enumerate(index_values):
        label_text = _SPACE.sub(" ", str(label)).strip()
        if not label_text:
            continue
        if "_" in label_text:
            industry, metric = label_text.rsplit("_", 1)
        else:
            industry, metric = label_text, "同比增长"
        metric_lower = metric.lower()
        is_cumulative = "累计" in metric or "cumulative" in metric_lower
        is_yoy = "同比" in metric or "yoy" in metric_lower or not is_cumulative
        try:
            row_values = iloc[row_index]
            row_values = row_values.to_dict() if hasattr(row_values, "to_dict") else dict(row_values)
        except (AttributeError, IndexError, KeyError, TypeError, ValueError):
            continue
        for column in column_values:
            value = row_values.get(column)
            item: dict[str, Any] = {"行业名称": industry, "统计时间": column}
            if is_cumulative:
                item["累计同比"] = value
            elif is_yoy:
                item["当月同比"] = value
            result.append(item)
    return result or _rows(raw)


def build_industry_activity_data(
    raw: Any,
    *,
    as_of: datetime | date | str,
    source_ref: str = f"{SOURCE_ID}:macro_china_nbs_nation",
    source_manifest: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Normalize official NBS monthly industrial activity observations.

    This is intentionally named *activity*, not profit: it may support the
    macro/industry-cycle context but must never be interpreted as industry
    earnings.  AKShare's ``macro_china_nbs_nation`` is used only as a transport
    for the NBS directory and therefore receives the stronger official-source
    quality label when rows have a strict publication timestamp.
    """

    cutoff = _aware(as_of)
    rows = raw.get("rows") if isinstance(raw, Mapping) and "rows" in raw else raw
    items: list[dict[str, Any]] = []
    times: list[datetime] = []
    for row in _industry_rows(rows):
        observation = _row_time(row)
        publication = _row_publication_time(row)
        if not _pit_allowed(observation, publication, cutoff):
            continue
        industry = _find_value(
            row,
            ("行业", "行业名称", "工业行业", "industry", "name", "指标名称"),
            contains=("行业",),
        )
        if industry is None:
            industry = _find_value(row, ("指标", "indicator"))
        industry_name = _SPACE.sub(" ", str(industry or "")).strip()
        if not industry_name:
            continue
        yoy = _number(_find_value(row, ("当月同比", "同比增长", "同比", "yoy"), contains=("同比",)))
        cumulative = _number(_find_value(row, ("累计同比", "累计增长", "cumulative_yoy"), contains=("累计", "同比")))
        if yoy is None and cumulative is None:
            continue
        timestamp = observation or publication
        if timestamp is None:
            continue
        item: dict[str, Any] = {
            "industry": industry_name,
            "yoy": _round(yoy),
            "cumulative_yoy": _round(cumulative),
            "observation_time": timestamp.isoformat(),
            "publish_time": publication.isoformat() if publication else None,
            "publish_time_available": publication is not None,
            "pit_verified": publication is not None and publication <= cutoff,
            "pit_status": "STRICT_PIT" if publication is not None and publication <= cutoff else "OBSERVATION_DATE_ONLY",
            "source_ref": source_ref,
        }
        items.append(item)
        times.append(timestamp)
    items.sort(key=lambda item: (str(item["observation_time"]), str(item["industry"])))
    strict = bool(items) and all(item["pit_verified"] is True for item in items)
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract": "INDUSTRY_ACTIVITY_DATA",
        "available": bool(items),
        "reason_code": "OK" if items else "SOURCE_UNAVAILABLE",
        "as_of": cutoff.isoformat(),
        "metric_scope": "INDUSTRIAL_VALUE_ADDED_GROWTH_NOT_PROFIT",
        "items": items,
        "source_refs": [source_ref] if items else [],
        "source_manifest": [dict(item) for item in source_manifest],
        "freshness": _freshness(times, cutoff),
        "quality": "T1_OFFICIAL_NORMALIZED" if strict else "T2_OPEN_OBSERVATION_DATE_ONLY" if items else "UNAVAILABLE",
        "rules": {
            "point_in_time": True,
            "missing_values_are_not_zero": True,
            "not_profit_data": True,
        },
    }
    return _finalize_contract(payload)


def _bar_rows(raw: Any, *, as_of: datetime) -> list[dict[str, Any]]:
    rows = _rows(raw)
    result: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _row_time(row)
        publication = _row_publication_time(row)
        if not _pit_allowed(timestamp, publication, as_of):
            continue
        close = _number(_find_value(row, ("收盘", "收盘价", "close", "Close", "closing_price", "price"), contains=("收盘",)))
        if close is None or close <= 0 or timestamp is None:
            continue
        result.append({"time": timestamp, "close": close, "source_row": dict(row)})
    result.sort(key=lambda item: item["time"])
    deduped: dict[str, dict[str, Any]] = {}
    for item in result:
        deduped[item["time"].date().isoformat()] = item
    return list(deduped.values())


def build_asset_rotation_snapshot(
    histories: Mapping[str, Any],
    *,
    as_of: datetime | date | str,
    source_manifest: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Purely calculate 20/60 trading-day ETF momentum for the four assets."""

    cutoff = _aware(as_of)
    raw_assets: dict[str, dict[str, Any]] = {}
    return_values: dict[int, dict[str, float]] = {20: {}, 60: {}}
    source_refs: set[str] = set()
    all_times: list[datetime] = []
    for asset, definition in DEFAULT_ETFS.items():
        entry = histories.get(asset)
        entry_mapping = entry if isinstance(entry, Mapping) else {}
        source_ref = str(entry_mapping.get("source_ref") or f"{SOURCE_ID}:fund_etf_hist_em:{definition['symbol']}")
        source_refs.add(source_ref)
        rows = _bar_rows(entry_mapping.get("rows") if isinstance(entry, Mapping) else entry, as_of=cutoff)
        if rows:
            all_times.extend(item["time"] for item in rows)
        values: dict[str, Any] = {
            "symbol": definition["symbol"],
            "name": definition["name"],
            "source_ref": source_ref,
            "available": False,
            "close": None,
            "momentum_20d": None,
            "momentum_60d": None,
            "momentum_20d_percentile": None,
            "momentum_60d_percentile": None,
            "fund_flow_percentile": None,
            "bar_count": len(rows),
        }
        if rows:
            values["close"] = _round(rows[-1]["close"])
            if len(rows) >= 21:
                values["momentum_20d"] = _round(rows[-1]["close"] / rows[-21]["close"] - 1.0)
                return_values[20][asset] = float(values["momentum_20d"])
            if len(rows) >= 61:
                values["momentum_60d"] = _round(rows[-1]["close"] / rows[-61]["close"] - 1.0)
                return_values[60][asset] = float(values["momentum_60d"])
            values["available"] = values["momentum_20d"] is not None and values["momentum_60d"] is not None
            if not values["available"]:
                values["reason_code"] = "INSUFFICIENT_HISTORY"
        else:
            values["reason_code"] = "SOURCE_UNAVAILABLE"
        raw_assets[asset] = values
    for horizon in (20, 60):
        values = return_values[horizon]
        for asset, item in raw_assets.items():
            item[f"momentum_{horizon}d_percentile"] = _percentile(values.get(asset), list(values.values()))
    available_count = sum(1 for item in raw_assets.values() if item.get("available") is True)
    status = "READY" if available_count == len(DEFAULT_ETFS) else "DEGRADED" if available_count else "SOURCE_UNAVAILABLE"
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract": "ASSET_ROTATION_SNAPSHOT",
        "available": available_count > 0,
        "reason_code": "OK" if status == "READY" else "PARTIAL_DATA" if available_count else "SOURCE_UNAVAILABLE",
        "status": status,
        "as_of": cutoff.isoformat(),
        "assets": raw_assets,
        "required_assets": list(DEFAULT_ETFS),
        "required_factors": ["MOMENTUM_20D", "MOMENTUM_60D"],
        "source_refs": sorted(source_refs),
        "source_manifest": [dict(item) for item in source_manifest],
        "freshness": _freshness(all_times, cutoff),
        "quality": "T2_OPEN_AGGREGATED" if status == "READY" else "T2_OPEN_PARTIAL" if available_count else "UNAVAILABLE",
        "rules": {"point_in_time": True, "missing_values_are_not_zero": True, "fund_flow_not_inferred": True},
    }
    return _finalize_contract(payload)


def _build_observation_contract(
    contract: str,
    values: Mapping[str, Any],
    *,
    as_of: datetime,
    source_manifest: Sequence[Mapping[str, Any]],
    required: Sequence[str],
    source_refs: Sequence[str] = (),
) -> dict[str, Any]:
    available_keys = [key for key in required if values.get(key) is not None]
    available = bool(available_keys)
    status = "READY" if len(available_keys) == len(required) else "DEGRADED" if available else "SOURCE_UNAVAILABLE"
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract": contract,
        "available": available,
        "status": status,
        "reason_code": "OK" if status == "READY" else "PARTIAL_DATA" if available else "SOURCE_UNAVAILABLE",
        "as_of": as_of.isoformat(),
        "values": dict(values),
        "available_fields": available_keys,
        "missing_fields": [key for key in required if values.get(key) is None],
        "source_refs": sorted(set(source_refs)),
        "source_manifest": [dict(item) for item in source_manifest],
        "freshness": {"as_of": as_of.isoformat(), "latest_observation": None, "age_days": None},
        "quality": "T2_OPEN_AGGREGATED" if status == "READY" else "T2_OPEN_PARTIAL" if available else "UNAVAILABLE",
        "rules": {"point_in_time": True, "missing_values_are_not_zero": True},
    }
    return _finalize_contract(payload)


def build_global_macro_snapshot(
    values: Mapping[str, Any],
    *,
    as_of: datetime | date | str,
    source_manifest: Sequence[Mapping[str, Any]] = (),
    source_refs: Sequence[str] = (),
) -> dict[str, Any]:
    """Pure projection for optional USD/rates/Fed open-data fields."""

    cutoff = _aware(as_of)
    required = ("usd_momentum_percentile", "fed_easing_probability_percentile", "us_rate", "cn_rate")
    return _build_observation_contract(
        "GLOBAL_MACRO_SNAPSHOT",
        values,
        as_of=cutoff,
        source_manifest=source_manifest,
        required=required,
        source_refs=source_refs,
    )


def build_cross_market_lead_snapshot(
    markets: Mapping[str, Any],
    *,
    as_of: datetime | date | str,
    source_manifest: Sequence[Mapping[str, Any]] = (),
    source_refs: Sequence[str] = (),
) -> dict[str, Any]:
    """Pure projection for US/Korea/Taiwan/Japan index momentum leads."""

    cutoff = _aware(as_of)
    normalized: dict[str, Any] = {}
    for market in ("US", "KOREA", "TAIWAN", "JAPAN"):
        item = markets.get(market)
        normalized[market] = dict(item) if isinstance(item, Mapping) else None
    available = [key for key, item in normalized.items() if isinstance(item, Mapping) and item.get("momentum_20d") is not None]
    values = {
        key: item
        for key, item in normalized.items()
        if isinstance(item, Mapping) and item.get("momentum_20d") is not None
    }
    payload = _build_observation_contract(
        "CROSS_MARKET_LEAD_SNAPSHOT",
        values,
        as_of=cutoff,
        source_manifest=source_manifest,
        required=("US", "KOREA", "TAIWAN", "JAPAN"),
        source_refs=source_refs,
    )
    payload["markets"] = normalized
    payload["available_markets"] = available
    payload["missing_markets"] = [key for key in normalized if key not in available]
    return _finalize_contract(payload)


def _date_kwargs(cutoff: datetime, *, lookback_days: int) -> tuple[dict[str, Any], ...]:
    start = (cutoff - timedelta(days=max(90, lookback_days))).strftime("%Y%m%d")
    end = cutoff.strftime("%Y%m%d")
    return (
        {"period": "daily", "start_date": start, "end_date": end, "adjust": ""},
        {"start_date": start, "end_date": end},
        {},
    )


def _fetch_etf_fallback(provider: Any, symbol: str, cutoff: datetime) -> tuple[list[dict[str, Any]], str]:
    """Use Tencent then Sina ETF history when the primary EM endpoint fails."""

    exchange_symbol = f"sh{symbol}" if symbol.startswith("5") else f"sz{symbol}"
    date_values = _date_kwargs(cutoff, lookback_days=420)
    attempts = (
        (
            ("stock_zh_a_hist_tx",),
            tuple(
                {"symbol": exchange_symbol, **kwargs}
                for kwargs in date_values
            )
            + ({"symbol": exchange_symbol, "start_date": date_values[1]["start_date"], "end_date": date_values[1]["end_date"], "adjust": ""},),
        ),
        (("fund_etf_hist_sina",), ({"symbol": exchange_symbol},)),
    )
    last_error = "METHOD_NOT_CONFIGURED"
    for names, variants in attempts:
        if not callable(getattr(provider, names[0], None)):
            continue
        try:
            raw, source_ref = _call_provider(provider, names, kwargs_variants=variants)
        except SourceUnavailable as exc:
            last_error = str(exc)
            continue
        rows = _rows(raw)
        if rows:
            return rows, source_ref
    raise SourceUnavailable(last_error)


class OpenMacroDataCollector:
    """Collect open macro/ETF data with an injectable provider.

    ``provider`` may be an AKShare module, a compatible object, or a simple
    fake implementing only the methods needed by a test.  The default is
    resolved lazily so importing the workflow never requires AKShare.
    """

    def __init__(
        self,
        provider: Any | None = None,
        *,
        cache_dir: str | Path | None = None,
        source_id: str = SOURCE_ID,
    ) -> None:
        self._provider = provider
        self.cache_dir = Path(cache_dir) if cache_dir is not None else None
        self.source_id = source_id

    def collect(self, as_of: datetime | date | str) -> dict[str, Any]:
        cutoff = _aware(as_of)
        provider = _provider_for(self._provider)
        if provider is None:
            unavailable = _source_record(self.source_id, status="UNAVAILABLE", reason_code="SOURCE_UNAVAILABLE")
            cached = self._read_cache(cutoff)
            if cached is not None:
                return cached
            return self._assemble(cutoff, {}, {}, {}, {}, {}, self.source_id, [unavailable])
        macro_datasets, macro_manifest = self._collect_macro(provider, cutoff)
        histories, asset_manifest = self._collect_assets(provider, cutoff)
        global_values, global_manifest = self._collect_global(provider, cutoff)
        markets, market_manifest = self._collect_markets(provider, cutoff)
        industry_raw, industry_ref, industry_manifest = self._collect_industry_activity(provider, cutoff)
        manifest = macro_manifest + asset_manifest + global_manifest + market_manifest + industry_manifest
        result = self._assemble(cutoff, macro_datasets, histories, global_values, markets, industry_raw, industry_ref, manifest)
        if not self._has_available_data(result):
            cached = self._read_cache(cutoff)
            if cached is not None:
                return cached
        result["cache_status"] = "LIVE"
        if self.cache_dir is not None:
            self._write_cache(result, cutoff)
        return result

    def _collect_macro(self, provider: Any, cutoff: datetime) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        datasets: dict[str, Any] = {}
        manifest: list[dict[str, Any]] = []
        calls: tuple[str, tuple[str, ...]] = (
            ("PMI", ("macro_china_pmi",)),
            ("CPI", ("macro_china_cpi",)),
            ("PPI", ("macro_china_ppi",)),
            ("MONEY", ("macro_china_money_supply",)),
            # Social financing and new RMB credit are different series.  They
            # are deliberately collected through separate endpoints so one
            # table can never be counted as both.
            ("SOCIAL_FINANCING", ("macro_china_shrzgm",)),
            ("NEW_CREDIT", ("macro_china_new_financial_credit",)),
        )
        for dataset_id, names in calls:
            try:
                raw, source_ref = _call_provider(provider, names)
                rows = _rows(raw)
                manifest.append(_source_record(source_ref, status="AVAILABLE" if rows else "EMPTY", records=len(rows), reason_code=None if rows else "NO_ROWS"))
                if dataset_id == "PMI":
                    datasets["PMI"] = {"rows": rows, "source_ref": source_ref}
                elif dataset_id == "CPI":
                    datasets["CPI"] = {"rows": rows, "source_ref": source_ref}
                elif dataset_id == "PPI":
                    datasets["PPI"] = {"rows": rows, "source_ref": source_ref}
                elif dataset_id == "MONEY":
                    datasets["M1_YOY"] = {"rows": rows, "source_ref": source_ref}
                    datasets["M2_YOY"] = {"rows": rows, "source_ref": source_ref}
                elif dataset_id == "SOCIAL_FINANCING":
                    datasets["SOCIAL_FINANCING"] = {"rows": rows, "source_ref": source_ref}
                elif dataset_id == "NEW_CREDIT":
                    datasets["NEW_CREDIT"] = {"rows": rows, "source_ref": source_ref}
            except SourceUnavailable as exc:
                ref = f"{self.source_id}:{names[0]}"
                manifest.append(_source_record(ref, status="UNAVAILABLE", reason_code="SOURCE_UNAVAILABLE", error=str(exc)))
        return datasets, manifest

    def _collect_industry_activity(self, provider: Any, cutoff: datetime) -> tuple[Any, str, list[dict[str, Any]]]:
        source_ref = f"{self.source_id}:macro_china_nbs_nation"
        official_path = "工业>工业分大类行业增加值增长速度 (2018-至今)"
        try:
            raw, source_ref = _call_provider(
                provider,
                ("macro_china_nbs_nation",),
                kwargs_variants=(
                    {
                        "kind": "月度数据",
                        "path": "工业>工业分大类行业增加值增长速度 (2018-至今)",
                        "period": "LAST24",
                    },
                    {"kind": "月度数据", "path": "工业>工业分大类行业增加值增长速度 (2018-至今)"},
                ),
            )
            rows = _rows(raw)
            source_ref = f"{source_ref}:{official_path}"
            return raw, source_ref, [
                _source_record(source_ref, status="AVAILABLE" if rows else "EMPTY", records=len(rows), reason_code=None if rows else "NO_ROWS")
            ]
        except SourceUnavailable as exc:
            return {}, source_ref, [_source_record(source_ref, status="UNAVAILABLE", reason_code="SOURCE_UNAVAILABLE", error=str(exc))]

    def _collect_assets(self, provider: Any, cutoff: datetime) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        histories: dict[str, Any] = {}
        manifest: list[dict[str, Any]] = []
        for asset, definition in DEFAULT_ETFS.items():
            try:
                raw, source_ref = _call_provider(
                    provider,
                    ("fund_etf_hist_em", "etf_history", "fund_etf_hist"),
                    kwargs_variants=tuple(
                        {"symbol": definition["symbol"], **kwargs}
                        for kwargs in _date_kwargs(cutoff, lookback_days=420)
                    )
                    + ({"symbol": definition["symbol"]},),
                )
                rows = _rows(raw)
                # Eastmoney-backed fund_etf_hist_em is currently prone to
                # connection resets.  Tencent and Sina are open fallbacks
                # with the same daily OHLC contract.
                if not rows:
                    rows, source_ref = _fetch_etf_fallback(provider, definition["symbol"], cutoff)
                histories[asset] = {"rows": rows, "source_ref": f"{source_ref}:{definition['symbol']}"}
                manifest.append(_source_record(histories[asset]["source_ref"], status="AVAILABLE" if rows else "EMPTY", records=len(rows), reason_code=None if rows else "NO_ROWS"))
            except SourceUnavailable as exc:
                # If EM itself failed (rather than returning an empty table),
                # try the independent Tencent/Sina endpoints before declaring
                # the asset unavailable.
                try:
                    rows, source_ref = _fetch_etf_fallback(provider, definition["symbol"], cutoff)
                    histories[asset] = {"rows": rows, "source_ref": f"{source_ref}:{definition['symbol']}"}
                    manifest.append(_source_record(histories[asset]["source_ref"], status="AVAILABLE" if rows else "EMPTY", records=len(rows), reason_code=None if rows else "NO_ROWS"))
                    continue
                except SourceUnavailable:
                    manifest.append(_source_record(f"{self.source_id}:fund_etf_hist_em:{definition['symbol']}", status="UNAVAILABLE", reason_code="SOURCE_UNAVAILABLE", error=str(exc)))
        return histories, manifest

    def _collect_global(self, provider: Any, cutoff: datetime) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        # Provider-specific global macro methods are optional.  A fake/provider
        # can return either a scalar mapping or a time-series row list.
        values: dict[str, Any] = {}
        manifest: list[dict[str, Any]] = []
        # AKShare's public bond_zh_us_rate endpoint contains both domestic
        # and US Treasury yields in one table.  Read both columns from the
        # same PIT row instead of assuming separate us_rate/cn_rate methods.
        try:
            raw, source_ref = _call_provider(
                provider,
                ("bond_zh_us_rate",),
                kwargs_variants=_date_kwargs(cutoff, lookback_days=420),
            )
            rates = _extract_latest_rates(raw, as_of=cutoff)
            values.update(rates)
            manifest.append(_source_record(source_ref, status="AVAILABLE" if rates else "EMPTY", records=1 if rates else 0, reason_code=None if rates else "NO_PIT_VALUE"))
        except SourceUnavailable as exc:
            manifest.append(_source_record(f"{self.source_id}:bond_zh_us_rate", status="UNAVAILABLE", reason_code="SOURCE_UNAVAILABLE", error=str(exc)))

        # The open global-index endpoint is optional.  When present, derive
        # USD's momentum percentile from its own historical return series;
        # without enough history the value remains missing.
        try:
            raw, source_ref = _call_provider(
                provider,
                ("index_global_hist_em", "usd_index_history"),
                kwargs_variants=tuple(
                    {"symbol": "美元指数", **kwargs}
                    for kwargs in _date_kwargs(cutoff, lookback_days=420)
                )
                + ({"symbol": "美元指数"},),
            )
            bars = _bar_rows(raw, as_of=cutoff)
            usd_momentum = _momentum_percentile(bars, 20)
            if usd_momentum is not None:
                values["usd_momentum_percentile"] = usd_momentum
            manifest.append(_source_record(f"{source_ref}:美元指数", status="AVAILABLE" if usd_momentum is not None else "EMPTY", records=len(bars), reason_code=None if usd_momentum is not None else "INSUFFICIENT_HISTORY"))
        except SourceUnavailable as exc:
            manifest.append(_source_record(f"{self.source_id}:index_global_hist_em:USD", status="UNAVAILABLE", reason_code="SOURCE_UNAVAILABLE", error=str(exc)))

        requests = {
            "usd_momentum_percentile": ("usd_index", "macro_global_usd", "usd_momentum"),
            "fed_easing_probability_percentile": ("fed_easing_probability", "macro_fed_easing"),
            "us_rate": ("us_rate",),
            "cn_rate": ("cn_rate",),
        }
        for field, names in requests.items():
            # Values from the shared bond/USD endpoints are authoritative for
            # this collection.  Avoid duplicate calls and don't overwrite a
            # real value with a less-specific fallback.
            if field in values:
                continue
            try:
                raw, source_ref = _call_provider(provider, names, kwargs_variants=_date_kwargs(cutoff, lookback_days=420))
                parsed = _extract_scalar_or_latest(raw, field=field, as_of=cutoff)
                if parsed is not None:
                    values[field] = parsed
                manifest.append(_source_record(source_ref, status="AVAILABLE" if parsed is not None else "EMPTY", records=1 if parsed is not None else 0, reason_code=None if parsed is not None else "NO_PIT_VALUE"))
            except SourceUnavailable as exc:
                manifest.append(_source_record(f"{self.source_id}:{names[0]}", status="UNAVAILABLE", reason_code="SOURCE_UNAVAILABLE", error=str(exc)))
        return values, manifest

    def _collect_markets(self, provider: Any, cutoff: datetime) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        markets: dict[str, Any] = {}
        manifest: list[dict[str, Any]] = []
        symbols = {"US": "纳斯达克", "KOREA": "韩国KOSPI", "TAIWAN": "台湾加权", "JAPAN": "日经225"}
        for market, symbol in symbols.items():
            try:
                raw, source_ref = _call_provider(
                    provider,
                    ("cross_market_history", "global_index_history", "index_global_hist_em", "index_us_stock_sina"),
                    kwargs_variants=tuple(
                        {"market": market, "symbol": symbol, **kwargs}
                        for kwargs in _date_kwargs(cutoff, lookback_days=420)
                    )
                    + ({"market": market, "symbol": symbol}, {"symbol": symbol}),
                )
                bars = _bar_rows(raw, as_of=cutoff)
                item: dict[str, Any] = {"market": market, "symbol": symbol, "source_ref": source_ref, "available": False, "momentum_20d": None, "momentum_60d": None}
                if len(bars) >= 21:
                    item["momentum_20d"] = _round(bars[-1]["close"] / bars[-21]["close"] - 1.0)
                if len(bars) >= 61:
                    item["momentum_60d"] = _round(bars[-1]["close"] / bars[-61]["close"] - 1.0)
                item["available"] = item["momentum_20d"] is not None
                markets[market] = item
                manifest.append(_source_record(f"{source_ref}:{market}", status="AVAILABLE" if item["available"] else "EMPTY", records=len(bars), reason_code=None if item["available"] else "INSUFFICIENT_HISTORY"))
            except SourceUnavailable as exc:
                manifest.append(_source_record(f"{self.source_id}:cross_market:{market}", status="UNAVAILABLE", reason_code="SOURCE_UNAVAILABLE", error=str(exc)))
        return markets, manifest

    def _assemble(
        self,
        cutoff: datetime,
        macro_datasets: Mapping[str, Any],
        histories: Mapping[str, Any],
        global_values: Mapping[str, Any],
        markets: Mapping[str, Any],
        industry_raw: Any,
        industry_ref: str,
        manifest: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        macro = build_macro_economic_data(macro_datasets, as_of=cutoff, source_manifest=manifest)
        assets = build_asset_rotation_snapshot(histories, as_of=cutoff, source_manifest=manifest)
        global_macro = build_global_macro_snapshot(global_values, as_of=cutoff, source_manifest=manifest)
        cross_market = build_cross_market_lead_snapshot(markets, as_of=cutoff, source_manifest=manifest)
        industry_activity = build_industry_activity_data(industry_raw, as_of=cutoff, source_ref=industry_ref, source_manifest=manifest)
        return {
            "schema_version": SCHEMA_VERSION,
            "as_of": cutoff.isoformat(),
            "source_manifest": [dict(item) for item in manifest],
            "source_refs": sorted({str(item.get("source_ref")) for item in manifest if item.get("source_ref")}),
            "content_hash": _content_hash({
                "as_of": cutoff.isoformat(),
                "source_manifest": manifest,
                "contracts": {
                    "MACRO_ECONOMIC_DATA": macro,
                    "ASSET_ROTATION_SNAPSHOT": assets,
                    "GLOBAL_MACRO_SNAPSHOT": global_macro,
                    "CROSS_MARKET_LEAD_SNAPSHOT": cross_market,
                    "INDUSTRY_ACTIVITY_DATA": industry_activity,
                },
            }),
            "freshness": {
                "as_of": cutoff.isoformat(),
                "contracts": {
                    "MACRO_ECONOMIC_DATA": macro["freshness"],
                    "ASSET_ROTATION_SNAPSHOT": assets["freshness"],
                    "GLOBAL_MACRO_SNAPSHOT": global_macro["freshness"],
                    "CROSS_MARKET_LEAD_SNAPSHOT": cross_market["freshness"],
                    "INDUSTRY_ACTIVITY_DATA": industry_activity["freshness"],
                },
            },
            "quality": {
                "MACRO_ECONOMIC_DATA": macro["quality"],
                "ASSET_ROTATION_SNAPSHOT": assets["quality"],
                "GLOBAL_MACRO_SNAPSHOT": global_macro["quality"],
                "CROSS_MARKET_LEAD_SNAPSHOT": cross_market["quality"],
                "INDUSTRY_ACTIVITY_DATA": industry_activity["quality"],
            },
            "MACRO_ECONOMIC_DATA": macro,
            "ASSET_ROTATION_SNAPSHOT": assets,
            "GLOBAL_MACRO_SNAPSHOT": global_macro,
            "CROSS_MARKET_LEAD_SNAPSHOT": cross_market,
            "INDUSTRY_ACTIVITY_DATA": industry_activity,
        }

    @staticmethod
    def _has_available_data(result: Mapping[str, Any]) -> bool:
        contracts = (
            "MACRO_ECONOMIC_DATA",
            "ASSET_ROTATION_SNAPSHOT",
            "GLOBAL_MACRO_SNAPSHOT",
            "CROSS_MARKET_LEAD_SNAPSHOT",
            "INDUSTRY_ACTIVITY_DATA",
        )
        return any(isinstance(result.get(key), Mapping) and result[key].get("available") is True for key in contracts)

    def _read_cache(self, cutoff: datetime) -> dict[str, Any] | None:
        if self.cache_dir is None or not self.cache_dir.exists():
            return None
        candidates: list[tuple[datetime, Path, dict[str, Any]]] = []
        for path in self.cache_dir.glob("open-macro-*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            cached_as_of = _parse_time(payload.get("as_of"))
            if cached_as_of is None or cached_as_of > cutoff:
                continue
            if payload.get("cache_status") == "STALE_FALLBACK" or not payload.get("content_hash"):
                continue
            if not self._has_available_data(payload):
                continue
            candidates.append((cached_as_of, path, payload))
        if not candidates:
            return None
        cached_as_of, _path, payload = max(candidates, key=lambda item: item[0])
        original_hash = str(payload.get("content_hash"))
        payload["cache_status"] = "STALE_FALLBACK"
        payload["requested_as_of"] = cutoff.isoformat()
        payload["cache_as_of"] = cached_as_of.isoformat()
        payload["cache_age_days"] = max(0, (cutoff.date() - cached_as_of.date()).days)
        payload["cache_original_content_hash"] = original_hash
        return payload

    def _write_cache(self, result: Mapping[str, Any], cutoff: datetime) -> None:
        assert self.cache_dir is not None
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self.cache_dir / f"open-macro-{cutoff.strftime('%Y%m%d')}.json"
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(_canonical_json(result), encoding="utf-8")
        temporary.replace(path)


def _extract_scalar_or_latest(raw: Any, *, field: str, as_of: datetime) -> float | None:
    if isinstance(raw, Mapping) and not any(key in raw for key in ("data", "rows", "items", "result", "records")):
        value = raw.get(field)
        if value is None:
            value = next((item for key, item in raw.items() if field.split("_")[0] in _norm_key(key)), None)
        return _number(value)
    rows = _rows(raw)
    selected: list[tuple[datetime, float]] = []
    for row in rows:
        observation = _row_time(row)
        publication = _row_publication_time(row)
        if not _pit_allowed(observation, publication, as_of):
            continue
        value = _number(_find_value(row, (field, field.replace("_", " "), "value", "值", "收盘", "收盘价"), contains=tuple(field.split("_")[:1])))
        timestamp = observation or publication
        if value is not None and timestamp is not None:
            selected.append((timestamp, value))
    return selected[-1][1] if selected else None


def _extract_latest_rates(raw: Any, *, as_of: datetime) -> dict[str, float]:
    """Extract the two yield columns exposed by ``bond_zh_us_rate``."""

    rows = _rows(raw)
    selected: list[tuple[datetime, dict[str, float]]] = []
    for row in rows:
        observation = _row_time(row)
        publication = _row_publication_time(row)
        if not _pit_allowed(observation, publication, as_of):
            continue
        cn = _number(
            _find_value(
                row,
                ("中国国债10年", "中国国债10年期", "中国10年国债", "CN10Y", "cn_rate"),
                contains=("中国", "10年"),
            )
        )
        us = _number(
            _find_value(
                row,
                ("美国国债10年", "美国国债10年期", "美国10年国债", "US10Y", "us_rate"),
                contains=("美国", "10年"),
            )
        )
        timestamp = observation or publication
        if timestamp is None:
            continue
        item: dict[str, float] = {}
        if cn is not None:
            item["cn_rate"] = cn
        if us is not None:
            item["us_rate"] = us
        if item:
            selected.append((timestamp, item))
    if not selected:
        return {}
    selected.sort(key=lambda item: item[0])
    return {key: _round(value) for key, value in selected[-1][1].items()}


def _momentum_percentile(rows: Sequence[Mapping[str, Any]], lookback: int) -> float | None:
    """Rank the latest ETF/index return against available rolling returns."""

    if len(rows) <= lookback:
        return None
    returns: list[float] = []
    for index in range(lookback, len(rows)):
        base = _number(rows[index - lookback].get("close"))
        latest = _number(rows[index].get("close"))
        if base is None or latest is None or base <= 0 or latest <= 0:
            continue
        returns.append(latest / base - 1.0)
    return _percentile(returns[-1], returns) if returns else None


__all__ = [
    "DEFAULT_ETFS",
    "OpenMacroDataCollector",
    "SCHEMA_VERSION",
    "SOURCE_ID",
    "SourceUnavailable",
    "build_asset_rotation_snapshot",
    "build_cross_market_lead_snapshot",
    "build_global_macro_snapshot",
    "build_industry_activity_data",
    "build_macro_economic_data",
]
