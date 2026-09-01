"""Independent benchmark for monthly broker ``金股`` lists.

The benchmark is intentionally one-way: broker lists are compared with an
A1 result, but they are never used to construct the A1 input universe.  This
module has no dependency on the research pipeline so that accidental reverse
injection is difficult to introduce.

The interchange contract is a CSV or JSON sequence with these fields:

``month``
    Monthly label in ``YYYY-MM`` form.
``broker``
    Non-empty broker/research-house name.
``symbol``
    Non-empty stock identifier (the value is upper-cased for matching).
``name``
    Optional display name.
``publish_time``
    Optional publication timestamp.  ISO-8601 timestamps and date-only ISO
    values are accepted; date-only values use Asia/Shanghai midnight.
``source_ref``
    Non-empty provenance reference for the list row.

Unknown fields, missing required fields, malformed dates and empty required
values fail closed with :class:`BrokerGoldContractError`.  ``as_of`` is
applied before de-duplication, so a future revision can never hide an older
point-in-time row.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, TextIO
from zoneinfo import ZoneInfo


BROKER_GOLD_SCHEMA_VERSION = "liangjian-broker-gold/1.0.0"
BROKER_GOLD_FIELDS = (
    "month",
    "broker",
    "symbol",
    "name",
    "publish_time",
    "source_ref",
)
_REQUIRED_FIELDS = frozenset({"month", "broker", "symbol", "source_ref"})
_OPTIONAL_FIELDS = frozenset({"name", "publish_time"})
_ALLOWED_FIELDS = _REQUIRED_FIELDS | _OPTIONAL_FIELDS
_MONTH_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])$")
_SYMBOL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/+\-]{0,63}$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
SHANGHAI = ZoneInfo("Asia/Shanghai")


class BrokerGoldContractError(ValueError):
    """A fail-closed input contract error.

    ``reason_code`` is stable enough for an import UI or an offline report,
    while the exception message identifies the offending field/row without
    echoing potentially sensitive source content.
    """

    def __init__(self, reason_code: str, message: str | None = None) -> None:
        self.reason_code = reason_code
        super().__init__(message or reason_code)


@dataclass(frozen=True, slots=True)
class BrokerGoldRecord:
    """One normalized broker-list row."""

    month: str
    broker: str
    symbol: str
    source_ref: str
    name: str | None = None
    publish_time: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "month": self.month,
            "broker": self.broker,
            "symbol": self.symbol,
            "name": self.name,
            "publish_time": self.publish_time.isoformat() if self.publish_time else None,
            "source_ref": self.source_ref,
        }


@dataclass(frozen=True, slots=True)
class BrokerGoldDataset:
    """Point-in-time eligible rows and import diagnostics.

    ``benchmark_not_runtime_input`` is a constant contract marker.  It is
    deliberately present on every dataset and report returned by this module.
    """

    records: tuple[BrokerGoldRecord, ...] = ()
    as_of: datetime | None = None
    excluded_future: tuple[BrokerGoldRecord, ...] = ()
    duplicate_count: int = 0
    schema_version: str = BROKER_GOLD_SCHEMA_VERSION
    benchmark_not_runtime_input: bool = True

    def __post_init__(self) -> None:
        if not self.benchmark_not_runtime_input:
            raise BrokerGoldContractError("BROKER_GOLD_RUNTIME_INJECTION_FORBIDDEN")
        if self.as_of is not None:
            _require_aware(self.as_of, field_name="as_of")

    @property
    def months(self) -> tuple[str, ...]:
        return tuple(sorted({record.month for record in self.records}))


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise BrokerGoldContractError("BROKER_GOLD_NAIVE_TIMESTAMP", f"{field_name} must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _text(value: Any, *, field_name: str, required: bool = True, limit: int = 2048) -> str | None:
    if value is None:
        if required:
            raise BrokerGoldContractError("BROKER_GOLD_REQUIRED_FIELD", f"{field_name} is required")
        return None
    if not isinstance(value, str):
        raise BrokerGoldContractError("BROKER_GOLD_FIELD_TYPE", f"{field_name} must be a string")
    result = value.strip()
    if required and not result:
        raise BrokerGoldContractError("BROKER_GOLD_REQUIRED_FIELD", f"{field_name} is required")
    if len(result) > limit or _CONTROL_RE.search(result):
        raise BrokerGoldContractError("BROKER_GOLD_FIELD_INVALID", f"invalid {field_name}")
    return result or None


def _month(value: Any) -> str:
    result = _text(value, field_name="month", limit=7)
    if result is None or _MONTH_RE.fullmatch(result) is None:
        raise BrokerGoldContractError("BROKER_GOLD_INVALID_MONTH", "month must use YYYY-MM")
    return result


def _symbol(value: Any) -> str:
    result = _text(value, field_name="symbol", limit=64)
    if result is None or _SYMBOL_RE.fullmatch(result) is None:
        raise BrokerGoldContractError("BROKER_GOLD_INVALID_SYMBOL", "symbol contains invalid characters")
    return result.upper()


def _publish_time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _require_aware(value, field_name="publish_time")
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day, tzinfo=SHANGHAI)
    if not isinstance(value, str):
        raise BrokerGoldContractError("BROKER_GOLD_FIELD_TYPE", "publish_time must be an ISO timestamp")
    text = value.strip()
    if not text:
        return None
    if _MONTH_RE.fullmatch(text):
        year, month = (int(part) for part in text.split("-"))
        return datetime(year, month, 1, tzinfo=SHANGHAI)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            parsed_date = date.fromisoformat(text)
        except ValueError as exc:
            raise BrokerGoldContractError("BROKER_GOLD_INVALID_PUBLISH_TIME", "invalid publish_time") from exc
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day, tzinfo=SHANGHAI)
    normalized = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise BrokerGoldContractError("BROKER_GOLD_INVALID_PUBLISH_TIME", "invalid publish_time") from exc
    return _require_aware(parsed, field_name="publish_time")


def _record(row: Mapping[str, Any], *, row_number: int | None = None) -> BrokerGoldRecord:
    if not isinstance(row, Mapping):
        raise BrokerGoldContractError("BROKER_GOLD_ROW_TYPE", f"row {row_number or '?'} must be an object")
    fields = {str(key) for key in row}
    missing = _REQUIRED_FIELDS - fields
    unknown = fields - _ALLOWED_FIELDS
    if missing:
        raise BrokerGoldContractError(
            "BROKER_GOLD_MISSING_FIELDS",
            f"row {row_number or '?'} is missing required fields",
        )
    if unknown:
        raise BrokerGoldContractError(
            "BROKER_GOLD_UNKNOWN_FIELDS",
            f"row {row_number or '?'} contains unknown fields",
        )
    return BrokerGoldRecord(
        month=_month(row.get("month")),
        broker=_text(row.get("broker"), field_name="broker", limit=256) or "",
        symbol=_symbol(row.get("symbol")),
        source_ref=_text(row.get("source_ref"), field_name="source_ref", limit=4096) or "",
        name=_text(row.get("name"), field_name="name", required=False, limit=256),
        publish_time=_publish_time(row.get("publish_time")),
    )


def _month_key(value: str) -> tuple[int, int]:
    match = _MONTH_RE.fullmatch(value)
    if match is None:
        raise BrokerGoldContractError("BROKER_GOLD_INVALID_MONTH", "month must use YYYY-MM")
    return int(match.group("year")), int(match.group("month"))


def _apply_as_of(
    records: Sequence[BrokerGoldRecord],
    *,
    as_of: datetime | None,
) -> tuple[tuple[BrokerGoldRecord, ...], tuple[BrokerGoldRecord, ...]]:
    if as_of is None:
        return tuple(records), ()
    cutoff = _require_aware(as_of, field_name="as_of")
    cutoff_month = (cutoff.year, cutoff.month)
    eligible: list[BrokerGoldRecord] = []
    future: list[BrokerGoldRecord] = []
    for record in records:
        is_future_month = _month_key(record.month) > cutoff_month
        is_future_publish = record.publish_time is not None and record.publish_time > cutoff
        if is_future_month or is_future_publish:
            future.append(record)
        else:
            eligible.append(record)
    return tuple(eligible), tuple(future)


def _deduplicate(records: Sequence[BrokerGoldRecord]) -> tuple[tuple[BrokerGoldRecord, ...], int]:
    """Keep one row per month/broker/symbol using deterministic PIT-safe order."""

    grouped: dict[tuple[str, str, str], list[BrokerGoldRecord]] = defaultdict(list)
    for record in records:
        grouped[(record.month, record.broker, record.symbol)].append(record)
    selected: list[BrokerGoldRecord] = []
    duplicate_count = 0
    for key in sorted(grouped):
        rows = grouped[key]
        duplicate_count += max(0, len(rows) - 1)
        # A dated revision is more informative than an undated import.  If
        # two dates tie, source/name make the choice deterministic.
        rows = sorted(
            rows,
            key=lambda row: (
                row.publish_time is not None,
                row.publish_time or datetime.min.replace(tzinfo=timezone.utc),
                row.source_ref,
                row.name or "",
            ),
            reverse=True,
        )
        selected.append(rows[0])
    selected.sort(key=lambda row: (row.month, row.broker, row.symbol))
    return tuple(selected), duplicate_count


def normalize_broker_gold_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    as_of: datetime | None = None,
) -> BrokerGoldDataset:
    """Validate, apply point-in-time filtering and de-duplicate rows."""

    parsed = tuple(_record(row, row_number=index) for index, row in enumerate(rows, start=1))
    eligible, future = _apply_as_of(parsed, as_of=as_of)
    unique, duplicate_count = _deduplicate(eligible)
    return BrokerGoldDataset(
        records=unique,
        as_of=_require_aware(as_of, field_name="as_of") if as_of is not None else None,
        excluded_future=future,
        duplicate_count=duplicate_count,
    )


def _csv_rows(source: str | Path | TextIO) -> tuple[dict[str, Any], ...]:
    close = False
    if hasattr(source, "read"):
        handle = source  # type: ignore[assignment]
    else:
        handle = Path(source).open("r", encoding="utf-8-sig", newline="")
        close = True
    try:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames
        if fieldnames is None:
            raise BrokerGoldContractError("BROKER_GOLD_EMPTY_CSV", "CSV header is required")
        normalized_headers = [str(item).strip() for item in fieldnames]
        if len(set(normalized_headers)) != len(normalized_headers):
            raise BrokerGoldContractError("BROKER_GOLD_DUPLICATE_FIELDS", "CSV header contains duplicates")
        missing = _REQUIRED_FIELDS - set(normalized_headers)
        unknown = set(normalized_headers) - _ALLOWED_FIELDS
        if missing:
            raise BrokerGoldContractError("BROKER_GOLD_MISSING_FIELDS", "CSV header misses required fields")
        if unknown:
            raise BrokerGoldContractError("BROKER_GOLD_UNKNOWN_FIELDS", "CSV header contains unknown fields")
        rows: list[dict[str, Any]] = []
        for index, row in enumerate(reader, start=2):
            if None in row:
                raise BrokerGoldContractError("BROKER_GOLD_CSV_MALFORMED", f"CSV row {index} is malformed")
            if not any(value not in (None, "") for value in row.values()):
                continue
            rows.append({str(key).strip(): value for key, value in row.items()})
        return tuple(rows)
    finally:
        if close:
            handle.close()


def _json_rows(source: str | Path | TextIO | bytes | bytearray | Mapping[str, Any] | Sequence[Any]) -> tuple[dict[str, Any], ...]:
    if isinstance(source, Mapping) or isinstance(source, (list, tuple)):
        payload: Any = source
    else:
        if hasattr(source, "read"):
            raw = source.read()  # type: ignore[union-attr]
        elif isinstance(source, (bytes, bytearray)):
            raw = bytes(source).decode("utf-8")
        else:
            raw = Path(source).read_text(encoding="utf-8-sig")
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise BrokerGoldContractError("BROKER_GOLD_INVALID_JSON", "JSON payload is invalid") from exc
    if isinstance(payload, Mapping):
        keys = {str(key) for key in payload}
        if keys != {"records"}:
            raise BrokerGoldContractError("BROKER_GOLD_UNKNOWN_FIELDS", "JSON envelope must contain only records")
        payload = payload.get("records")
    if not isinstance(payload, list):
        raise BrokerGoldContractError("BROKER_GOLD_JSON_SHAPE", "JSON payload must be an array of records")
    return tuple(payload)  # validated by normalize_broker_gold_rows


def load_broker_gold_csv(source: str | Path | TextIO, *, as_of: datetime | None = None) -> BrokerGoldDataset:
    """Load and validate a strict broker-gold CSV."""

    return normalize_broker_gold_rows(_csv_rows(source), as_of=as_of)


def load_broker_gold_json(
    source: str | Path | TextIO | bytes | bytearray | Mapping[str, Any] | Sequence[Any],
    *,
    as_of: datetime | None = None,
) -> BrokerGoldDataset:
    """Load and validate a strict broker-gold JSON array or ``records`` envelope."""

    return normalize_broker_gold_rows(_json_rows(source), as_of=as_of)


def import_broker_gold(
    source: str | Path | TextIO | bytes | bytearray | Mapping[str, Any] | Sequence[Any],
    *,
    as_of: datetime | None = None,
    format: str | None = None,
) -> BrokerGoldDataset:
    """Import CSV/JSON by explicit format or file suffix.

    A caller must pass ``format`` for a file-like object or in-memory value.
    This avoids guessing a contract from arbitrary text and gives callers a
    stable failure mode when the extension is absent.
    """

    selected = format.lower().lstrip(".") if isinstance(format, str) else None
    if selected is None and isinstance(source, (str, Path)):
        selected = Path(source).suffix.lower().lstrip(".")
    if selected == "csv":
        return load_broker_gold_csv(source, as_of=as_of)  # type: ignore[arg-type]
    if selected == "json":
        return load_broker_gold_json(source, as_of=as_of)
    raise BrokerGoldContractError("BROKER_GOLD_FORMAT_REQUIRED", "format must be csv or json")


def _normalise_a1_status(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "_").replace(" ", "_")


_ACTIVE_STATUSES = frozenset({
    "ACTIVE",
    "ACTIVE_RESEARCH_POOL",
    "RESEARCH_ACTIVE",
    "APPROVED",
    "LLM_APPROVED",
})
_MONITOR_STATUSES = frozenset({
    "MONITOR",
    "MONITOR_POOL",
    "LOCAL_MONITOR",
    "WATCH",
    "WATCH_ONLY",
    "OBSERVE",
})


def _as_a1_rows(a1_rows: Any) -> tuple[dict[str, Any], ...]:
    """Flatten common A1 output shapes without importing the runtime module."""

    if isinstance(a1_rows, Mapping):
        flattened: list[dict[str, Any]] = []
        for key, status in (("active_research_pool", "ACTIVE"), ("active_pool", "ACTIVE"), ("monitor_pool", "MONITOR"), ("watch_pool", "MONITOR")):
            values = a1_rows.get(key)
            if values is None:
                continue
            if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
                raise BrokerGoldContractError("BROKER_GOLD_A1_SHAPE", f"{key} must be an array")
            for item in values:
                if not isinstance(item, Mapping):
                    raise BrokerGoldContractError("BROKER_GOLD_A1_SHAPE", "A1 row must be an object")
                row = dict(item)
                # The named pool is authoritative.  This prevents a stale
                # status field embedded in a row from changing its stratum.
                row["status"] = status
                flattened.append(row)
        decisions = a1_rows.get("decisions")
        if decisions is not None:
            if not isinstance(decisions, Sequence) or isinstance(decisions, (str, bytes, bytearray)):
                raise BrokerGoldContractError("BROKER_GOLD_A1_SHAPE", "decisions must be an array")
            flattened.extend(dict(item) for item in decisions if isinstance(item, Mapping))
        return tuple(flattened)
    if not isinstance(a1_rows, Sequence) or isinstance(a1_rows, (str, bytes, bytearray)):
        raise BrokerGoldContractError("BROKER_GOLD_A1_SHAPE", "A1 rows must be an array or object")
    result: list[dict[str, Any]] = []
    for item in a1_rows:
        if not isinstance(item, Mapping):
            raise BrokerGoldContractError("BROKER_GOLD_A1_SHAPE", "A1 row must be an object")
        result.append(dict(item))
    return tuple(result)


def _row_reason_codes(row: Mapping[str, Any]) -> list[str]:
    values: list[Any] = []
    for key in ("reason_codes", "reasons"):
        value = row.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            values.extend(value)
        elif value:
            values.append(value)
    for key in ("reason_code", "reason"):
        if row.get(key):
            values.append(row[key])
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in result:
            result.append(text)
    return result


def _row_rank_value(row: Mapping[str, Any]) -> float | None:
    for key in ("rank", "a1_rank", "global_rank"):
        value = row.get(key)
        if isinstance(value, bool):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number) and number > 0:
            return number
    for key in ("score", "final_score", "composite_score", "structural_score"):
        value = row.get(key)
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _rank_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Assign a stable 0-100 percentile to the A1 covered rows."""

    if not rows:
        return {}
    explicit_rank = any(_row_rank_value(row) is not None for row in rows)
    indexed = list(enumerate(rows))
    if explicit_rank:
        # Explicit ranks are ascending; score-like values are descending.
        rank_keys = []
        for index, row in indexed:
            value = _row_rank_value(row)
            has_rank = any(row.get(key) not in (None, "") for key in ("rank", "a1_rank", "global_rank"))
            rank_keys.append((index, row, value, has_rank))
        rank_keys.sort(
            key=lambda item: (
                0 if item[3] else 1,
                item[2] if item[3] and item[2] is not None else -(item[2] or 0),
                str(item[1].get("symbol") or ""),
            )
        )
    else:
        rank_keys = [(index, row, None, False) for index, row in indexed]
    if not explicit_rank:
        rank_keys.sort(key=lambda item: str(item[1].get("symbol") or ""))
    total = len(rank_keys)
    result: dict[str, dict[str, Any]] = {}
    for position, (_index, row, value, _has_rank) in enumerate(rank_keys, start=1):
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        percentile = 100.0 if total == 1 else round((total - position) / (total - 1) * 100.0, 4)
        result[symbol] = {
            "rank": position,
            "rank_percentile": percentile,
            "raw_rank_or_score": value,
        }
    return result


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _coverage(symbols: set[str], gold_symbols: set[str]) -> dict[str, Any]:
    covered = sorted(symbols & gold_symbols)
    missing = sorted(gold_symbols - symbols)
    return {
        "gold_count": len(gold_symbols),
        "covered_count": len(covered),
        "coverage": _ratio(len(covered), len(gold_symbols)),
        "covered_symbols": covered,
        "missing_symbols": missing,
    }


def _select_month(dataset: BrokerGoldDataset, *, month: str | None) -> str | None:
    if month is not None:
        return _month(month)
    return max(dataset.months, key=_month_key, default=None)


def evaluate_broker_gold(
    dataset_or_records: BrokerGoldDataset | Iterable[Mapping[str, Any]],
    a1_rows: Any,
    *,
    as_of: datetime | None = None,
    month: str | None = None,
) -> dict[str, Any]:
    """Compare an A1 active/monitor result against a monthly gold benchmark.

    The returned object is JSON-serializable and contains no candidate-pool
    mutation hook.  ``benchmark_not_runtime_input`` must remain ``True``;
    callers should persist this report separately from runtime stage output.
    """

    if isinstance(dataset_or_records, BrokerGoldDataset):
        dataset = dataset_or_records
        if as_of is not None:
            # Re-apply a caller cutoff rather than trusting a dataset created
            # without one.  Operate on normalized records directly so a
            # dataset can safely be passed back into this function.
            eligible, future = _apply_as_of(dataset.records, as_of=as_of)
            unique, duplicate_count = _deduplicate(eligible)
            dataset = BrokerGoldDataset(
                records=unique,
                as_of=_require_aware(as_of, field_name="as_of"),
                excluded_future=tuple((*dataset.excluded_future, *future)),
                duplicate_count=dataset.duplicate_count + duplicate_count,
            )
    else:
        dataset = normalize_broker_gold_rows(dataset_or_records, as_of=as_of)
    cutoff = dataset.as_of or (_require_aware(as_of, field_name="as_of") if as_of is not None else None)
    selected_month = _select_month(dataset, month=month)
    records = tuple(record for record in dataset.records if selected_month is None or record.month == selected_month)
    gold_by_symbol: dict[str, list[BrokerGoldRecord]] = defaultdict(list)
    for record in records:
        gold_by_symbol[record.symbol].append(record)
    gold_symbols = set(gold_by_symbol)

    institutional_projection = (
        a1_rows.get("institutional_coverage_pool")
        if isinstance(a1_rows, Mapping)
        else None
    )
    institutional_projection = (
        institutional_projection
        if isinstance(institutional_projection, Sequence)
        and not isinstance(institutional_projection, (str, bytes, bytearray))
        else ()
    )
    autonomous_selected_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in institutional_projection
        if isinstance(row, Mapping)
        and str(row.get("autonomous_partition") or "").strip().upper()
        in {"LOCAL_ACTIVE_CANDIDATE", "REVIEW_CANDIDATE"}
        and str(row.get("symbol") or "").strip()
    }
    runtime_seed_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in institutional_projection
        if isinstance(row, Mapping)
        and str(row.get("coverage_origin") or "").strip().upper() == "BROKER_GOLD_T2"
        and str(row.get("symbol") or "").strip()
    }

    all_rows = _as_a1_rows(a1_rows)
    all_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            all_by_symbol[symbol].append(row)
    active_rows: list[dict[str, Any]] = []
    monitor_rows: list[dict[str, Any]] = []
    outside_rows: list[dict[str, Any]] = []
    for row in all_rows:
        status = _normalise_a1_status(row.get("status"))
        if status in _ACTIVE_STATUSES:
            active_rows.append(row)
        elif status in _MONITOR_STATUSES:
            monitor_rows.append(row)
        else:
            outside_rows.append(row)

    def _unique_rows(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            if symbol not in result:
                result[symbol] = dict(row)
            else:
                # Preserve explainability fields/reasons from both strata,
                # while an active row remains the preferred primary row.
                current = result[symbol]
                for key in ("theme_id", "node_id"):
                    if not current.get(key) and row.get(key):
                        current[key] = row[key]
                reasons = _row_reason_codes(current)
                for reason in _row_reason_codes(row):
                    if reason not in reasons:
                        reasons.append(reason)
                if reasons:
                    current["reason_codes"] = reasons
        return result

    active_by_symbol = _unique_rows(active_rows)
    monitor_by_symbol = _unique_rows(monitor_rows)
    covered_by_symbol = dict(monitor_by_symbol)
    covered_by_symbol.update(active_by_symbol)
    # A symbol can occur in both pools during a transition.  Keep the active
    # row as the primary stratum, but retain monitor-side explainability and
    # reasons so the benchmark never loses audit context.
    for symbol in sorted(set(active_by_symbol) & set(monitor_by_symbol)):
        primary = covered_by_symbol[symbol]
        secondary = monitor_by_symbol[symbol]
        for key in ("theme_id", "node_id"):
            if not primary.get(key) and secondary.get(key):
                primary[key] = secondary[key]
        reasons = _row_reason_codes(primary)
        for reason in _row_reason_codes(secondary):
            if reason not in reasons:
                reasons.append(reason)
        if reasons:
            primary["reason_codes"] = reasons
    active_symbols = set(active_by_symbol)
    monitor_symbols = set(monitor_by_symbol)
    covered_symbols = set(covered_by_symbol)

    by_broker: dict[str, dict[str, Any]] = {}
    for broker in sorted({record.broker for record in records}):
        broker_symbols = {record.symbol for record in records if record.broker == broker}
        by_broker[broker] = {
            "gold_count": len(broker_symbols),
            "symbol_coverage": _coverage(covered_symbols, broker_symbols),
            "active_coverage": _coverage(active_symbols, broker_symbols),
            "monitor_coverage": _coverage(monitor_symbols, broker_symbols),
        }

    consensus = {
        symbol: {
            "broker_count": len({record.broker for record in rows}),
            "brokers": sorted({record.broker for record in rows}),
            "covered": symbol in covered_symbols,
            "active": symbol in active_symbols,
            "monitor": symbol in monitor_symbols,
        }
        for symbol, rows in sorted(gold_by_symbol.items())
    }
    consensus_counts = defaultdict(int)
    for item in consensus.values():
        consensus_counts["all"] += 1
        if item["broker_count"] >= 2:
            consensus_counts["multi_broker"] += 1
            if item["covered"]:
                consensus_counts["multi_broker_covered"] += 1

    theme_explained = {
        symbol
        for symbol, row in covered_by_symbol.items()
        if str(row.get("theme_id") or "").strip()
    }
    node_explained = {
        symbol
        for symbol, row in covered_by_symbol.items()
        if str(row.get("node_id") or "").strip()
    }
    theme_by_gold = theme_explained & gold_symbols
    node_by_gold = node_explained & gold_symbols

    rank = _rank_rows(tuple(covered_by_symbol.values()))
    rank_by_gold = {symbol: rank[symbol] for symbol in sorted(gold_symbols & set(rank))}
    missing: list[dict[str, Any]] = []
    missing_active: list[dict[str, Any]] = []
    for symbol in sorted(gold_symbols):
        gold_rows = gold_by_symbol[symbol]
        base = {
            "symbol": symbol,
            "name": next((row.name for row in gold_rows if row.name), None),
            "brokers": sorted({row.broker for row in gold_rows}),
        }
        if symbol not in covered_symbols:
            prior = all_by_symbol.get(symbol, [])
            reasons = ["NOT_IN_A1_ACTIVE_OR_MONITOR"] if not prior else [
                "PRESENT_OUTSIDE_ACTIVE_MONITOR",
                *(_row_reason_codes(prior[0])),
            ]
            missing.append({**base, "reasons": list(dict.fromkeys(reasons))})
        if symbol not in active_symbols:
            prior = monitor_by_symbol.get(symbol) or (all_by_symbol.get(symbol) or [None])[0]
            reasons = ["NOT_IN_A1_ACTIVE_POOL"]
            if prior is not None:
                reasons.extend(_row_reason_codes(prior))
            missing_active.append({**base, "reasons": list(dict.fromkeys(reasons))})

    empty = not records
    symbol_coverage = _coverage(covered_symbols, gold_symbols)
    active_coverage = _coverage(active_symbols, gold_symbols)
    monitor_coverage = _coverage(monitor_symbols, gold_symbols)
    explainability = {
        "theme": {
            "gold_count": len(gold_symbols),
            "covered_count": len(theme_by_gold),
            "coverage": _ratio(len(theme_by_gold), len(gold_symbols)),
            "covered_a1_symbols": sorted(theme_explained & covered_symbols),
            "missing_symbols": sorted(gold_symbols - theme_by_gold),
        },
        "node": {
            "gold_count": len(gold_symbols),
            "covered_count": len(node_by_gold),
            "coverage": _ratio(len(node_by_gold), len(gold_symbols)),
            "covered_a1_symbols": sorted(node_explained & covered_symbols),
            "missing_symbols": sorted(gold_symbols - node_by_gold),
        },
    }
    rank_percentiles = {
        symbol: item["rank_percentile"] for symbol, item in rank_by_gold.items()
    }
    rank_values = list(rank_percentiles.values())
    return {
        "schema_version": "liangjian-broker-gold-evaluation/1.0.0",
        "benchmark_not_runtime_input": True,
        "as_of": cutoff.isoformat() if cutoff is not None else None,
        "month": selected_month,
        "status": "EMPTY_BENCHMARK" if empty else "EVALUATED",
        "dataset": {
            "schema_version": dataset.schema_version,
            "eligible_record_count": len(records),
            "eligible_symbol_count": len(gold_symbols),
            "broker_count": len(by_broker),
            "excluded_future_count": len(dataset.excluded_future),
            "duplicate_count": dataset.duplicate_count,
        },
        "counts": {
            "gold_symbols": len(gold_symbols),
            "a1_active_symbols": len(active_symbols),
            "a1_monitor_symbols": len(monitor_symbols),
            "a1_active_plus_monitor_symbols": len(covered_symbols),
            "a1_active_gold_symbols": len(active_symbols & gold_symbols),
            "a1_monitor_gold_symbols": len(monitor_symbols & gold_symbols),
        },
        "symbol_coverage": symbol_coverage,
        "active_coverage": active_coverage,
        "monitor_coverage": monitor_coverage,
        "autonomous_research_coverage": {
            **_coverage(autonomous_selected_symbols, gold_symbols),
            "definition": "DETERMINISTIC_A1_SELECTED_WITHOUT_INSTITUTIONAL_SEED_OVERRIDE",
        },
        "institutional_runtime_traceability": {
            **_coverage(runtime_seed_symbols, gold_symbols),
            "definition": "T2_COVERAGE_SEED_TRACED_THROUGH_A1",
            "not_a_blind_hit_rate": True,
        },
        "by_broker": by_broker,
        "broker_consensus": consensus,
        "consensus_summary": {
            "gold_symbol_count": consensus_counts["all"],
            "multi_broker_symbol_count": consensus_counts["multi_broker"],
            "multi_broker_covered_count": consensus_counts["multi_broker_covered"],
        },
        "explainability": explainability,
        "rank_percentile": {
            "by_symbol": rank_percentiles,
            "gold_symbols_ranked": len(rank_percentiles),
            "mean": round(sum(rank_values) / len(rank_values), 4) if rank_values else None,
            "min": min(rank_values) if rank_values else None,
            "max": max(rank_values) if rank_values else None,
        },
        "missing_symbols": missing,
        "missing_active_symbols": missing_active,
        "a1_statuses_outside_active_monitor": sorted({
            _normalise_a1_status(row.get("status")) for row in outside_rows if row.get("status")
        }),
    }


__all__ = [
    "BROKER_GOLD_SCHEMA_VERSION",
    "BROKER_GOLD_FIELDS",
    "BrokerGoldContractError",
    "BrokerGoldDataset",
    "BrokerGoldRecord",
    "evaluate_broker_gold",
    "import_broker_gold",
    "load_broker_gold_csv",
    "load_broker_gold_json",
    "normalize_broker_gold_rows",
]
