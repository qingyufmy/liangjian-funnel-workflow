"""Read-only daily facts for the server-owned entry-confirmation overlay.

The entry-pattern evaluator is deliberately pure, but its B pattern needs a
small amount of daily evidence.  This module is the only adapter that may
project that evidence into ``market_context``.  It never reads a plan's
self-reported daily fields and never writes a store, cache, or publication.

The production runtime path is :func:`read_entry_confirmation_daily_facts_small_batch`,
which reads only the versioned facts object embedded in an existing
publication-projection manifest.  The source-scanning readers remain for one
offline publication-time projection and bounded tests; they are never used by
the minute runtime wrapper.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import date, datetime, time
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")
HASH_RE = re.compile(r"^[0-9a-fA-F]{64}$")
MANIFEST_SCHEMA = "publication-projection-manifest/1"
ENTRY_CONFIRMATION_FACTS_SCHEMA = "entry-confirmation-daily-facts/1"
_TRADING_CALENDAR: Any | None = None


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError("non-JSON source object")


def _object_hash(value: Any) -> str:
    """Keep this source adapter lightweight while matching publication hash."""

    digest = hashlib.sha256()
    encoder = json.JSONEncoder(
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False, default=_json_default,
    )
    for chunk in encoder.iterencode(value):
        digest.update(chunk.encode("utf-8"))
    return digest.hexdigest()


def _unproven(*reason_codes: str, source_pin: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return the stable fail-closed shape without exposing source errors."""

    body: dict[str, Any] = {
        "status": "UNPROVEN",
        "reason_codes": sorted({str(code) for code in reason_codes if code}),
        "daily_closes": [],
        "previous_day_high": None,
        "daily_ma5": None,
    }
    if isinstance(source_pin, Mapping):
        body["source_pin"] = dict(source_pin)
    return body


def _ready(*, closes: list[dict[str, Any]], previous_high: dict[str, Any],
           ma5: dict[str, Any], source_pin: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": "READY",
        "reason_codes": [],
        "daily_closes": closes,
        "previous_day_high": previous_high,
        "daily_ma5": ma5,
        "source_pin": dict(source_pin),
    }


def _finite(value: Any, *, positive: bool = False) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or (positive and number <= 0):
        return None
    return number


def _hash(value: Any) -> bool:
    return isinstance(value, str) and HASH_RE.fullmatch(value) is not None


def _trade_date(value: date | datetime | str) -> date | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(SHANGHAI).date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except (TypeError, ValueError):
        return None


def _as_of(value: Any) -> datetime | None:
    """Normalize a supported daily-bar date to an aware Shanghai timestamp."""

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(SHANGHAI)
    if isinstance(value, date):
        return datetime.combine(value, time(15), tzinfo=SHANGHAI)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # The frozen data contract uses milliseconds.  Seconds are rejected
        # rather than silently turning a bad source timestamp into a date.
        number = float(value)
        if math.isfinite(number) and 100_000_000_000 <= number <= 10_000_000_000_000:
            try:
                return datetime.fromtimestamp(number / 1000, tz=SHANGHAI)
            except (OverflowError, OSError, ValueError):
                return None
        return None
    text = str(value or "").strip()
    if not text:
        return None
    # Date-only records are the normal RECENT_DAILY_BARS representation.
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return datetime.combine(date.fromisoformat(text), time(15), tzinfo=SHANGHAI)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(SHANGHAI)


def _field(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _previous_exchange_day(target: date, calendar: Any | None = None) -> date | None:
    """Resolve the prior XSHG session without a weekday approximation."""

    global _TRADING_CALENDAR
    if calendar is None:
        if _TRADING_CALENDAR is None:
            try:
                from .calendar import ExchangeTradingCalendar
                _TRADING_CALENDAR = ExchangeTradingCalendar()
            except (ImportError, TypeError, ValueError):
                return None
        calendar = _TRADING_CALENDAR
    try:
        value = calendar.previous_trading_day(target)
    except Exception:
        return None
    return value if isinstance(value, date) and not isinstance(value, datetime) else None


def _source_pin_ready(source_pin: Mapping[str, Any] | None, *, require_manifest: bool = True,
                      require_raw_plan: bool = False) -> bool:
    if not isinstance(source_pin, Mapping):
        return False
    snapshot_hash = source_pin.get("snapshot_hash") or source_pin.get("source_snapshot_hash")
    # Both object hashes and byte pins are required.  A path by itself is not
    # an authority and must never become an entry-confirmation fact source.
    base_ready = (
        isinstance(source_pin.get("source_run_id"), str)
        and bool(source_pin.get("source_run_id"))
        and isinstance(source_pin.get("plan_id"), str)
        and bool(source_pin.get("plan_id"))
        and isinstance(source_pin.get("snapshot_id"), str)
        and bool(source_pin.get("snapshot_id"))
        and _hash(snapshot_hash)
        and _hash(source_pin.get("snapshot_file_sha256"))
    )
    if not base_ready:
        return False
    if not require_manifest:
        return not require_raw_plan or _hash(source_pin.get("raw_plan_sha256"))
    manifest_hash = source_pin.get("manifest_sha256") or source_pin.get("publication_manifest_sha256")
    return _hash(manifest_hash) and _hash(source_pin.get("manifest_file_sha256"))


def _rows_for_symbol(data: Mapping[str, Any], symbol: str) -> Sequence[Any] | None:
    recent = data.get("RECENT_DAILY_BARS")
    if not isinstance(recent, Mapping):
        return None
    value = recent.get(symbol)
    if value is None:
        value = recent.get(str(symbol).upper())
    if not isinstance(value, (list, tuple)):
        return None
    return value


def _build_from_rows(rows: Sequence[Any], *, symbol: str, target: date,
                     source_pin: Mapping[str, Any], snapshot_as_of: Any = None,
                     require_manifest_pin: bool = True, require_raw_plan: bool = False,
                     calendar: Any | None = None) -> dict[str, Any]:
    if not _source_pin_ready(source_pin, require_manifest=require_manifest_pin,
                             require_raw_plan=require_raw_plan):
        return _unproven("SOURCE_PIN_INCOMPLETE", source_pin=source_pin)
    snapshot_day = _trade_date(snapshot_as_of) if snapshot_as_of is not None else None
    if snapshot_as_of is not None and snapshot_day is None:
        return _unproven("SNAPSHOT_AS_OF_INVALID", source_pin=source_pin)
    if snapshot_day is not None and snapshot_day > target:
        return _unproven("SNAPSHOT_AFTER_TARGET_DATE", source_pin=source_pin)

    parsed: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return _unproven("DAILY_BAR_INVALID", source_pin=source_pin)
        stamp = _as_of(_field(row, "as_of", "date", "trade_date", "bar_date", "date_ms"))
        if stamp is None:
            return _unproven("DAILY_BAR_DATE_INVALID", source_pin=source_pin)
        # The current and future rows are explicitly excluded.  They do not
        # participate in ordering, MA5, or the previous-day high.
        if stamp.date() >= target:
            continue
        close = _finite(_field(row, "close", "close_price", "last", "last_price", "收盘", "收盘价"), positive=True)
        if close is None:
            return _unproven("DAILY_BAR_CLOSE_INVALID", source_pin=source_pin)
        high = _finite(_field(row, "high", "high_price", "最高", "最高价"), positive=True)
        parsed.append({"date": stamp.date(), "as_of": stamp.isoformat(), "close": close, "high": high})

    parsed.sort(key=lambda item: (item["date"], item["as_of"]))
    dates = [item["date"] for item in parsed]
    if len(dates) != len(set(dates)):
        return _unproven("DAILY_BAR_DUPLICATE_DATE", source_pin=source_pin)
    if len(parsed) < 20:
        return _unproven("DAILY20_INSUFFICIENT", source_pin=source_pin)
    selected = parsed[-20:]
    if any(left["date"] >= right["date"] for left, right in zip(selected, selected[1:])):
        return _unproven("DAILY20_ORDER_INVALID", source_pin=source_pin)
    if selected[-1]["high"] is None:
        return _unproven("PREVIOUS_DAY_HIGH_MISSING", source_pin=source_pin)
    expected_previous = _previous_exchange_day(target, calendar)
    if expected_previous is None:
        return _unproven("TRADING_CALENDAR_UNAVAILABLE", source_pin=source_pin)
    if selected[-1]["date"] != expected_previous:
        return _unproven("PREVIOUS_TRADING_DAY_MISSING", source_pin=source_pin)

    closes = [{"close": item["close"], "as_of": item["as_of"]} for item in selected]
    last_as_of = selected[-1]["as_of"]
    previous_high = {"value": selected[-1]["high"], "as_of": last_as_of}
    ma5_value = sum(item["close"] for item in selected[-5:]) / 5
    ma5 = {"value": ma5_value, "as_of": last_as_of}
    return _ready(closes=closes, previous_high=previous_high, ma5=ma5, source_pin=source_pin)


def build_entry_confirmation_daily_facts(
    snapshot: Mapping[str, Any], *, symbol: str, current_trade_date: date | datetime | str,
    source_pin: Mapping[str, Any], calendar: Any | None = None,
) -> dict[str, Any]:
    """Project facts from one already verified snapshot object.

    ``snapshot`` must be the original wrapper (``data``, ``snapshot_id``,
    ``snapshot_hash``, ``as_of``), not a plan or a final execution row.  The
    wrapper hash is checked before any rows are projected.
    """

    target = _trade_date(current_trade_date)
    if target is None:
        return _unproven("TARGET_TRADE_DATE_INVALID", source_pin=source_pin)
    if not isinstance(source_pin, Mapping):
        return _unproven("SOURCE_PIN_INCOMPLETE")
    if not isinstance(snapshot, Mapping) or not isinstance(snapshot.get("data"), Mapping):
        return _unproven("SNAPSHOT_DATA_MISSING", source_pin=source_pin)
    data = snapshot["data"]
    observed_hash = snapshot.get("snapshot_hash")
    if not _hash(observed_hash) or _object_hash(data) != observed_hash:
        return _unproven("SNAPSHOT_HASH_MISMATCH", source_pin=source_pin)
    if source_pin.get("snapshot_id") != snapshot.get("snapshot_id"):
        return _unproven("SOURCE_SNAPSHOT_ID_CONFLICT", source_pin=source_pin)
    source_hash = source_pin.get("snapshot_hash") or source_pin.get("source_snapshot_hash")
    if source_hash != observed_hash:
        return _unproven("SOURCE_SNAPSHOT_HASH_CONFLICT", source_pin=source_pin)
    rows = _rows_for_symbol(data, str(symbol))
    if rows is None:
        return _unproven("DAILY_BARS_SYMBOL_MISSING", source_pin=source_pin)
    return _build_from_rows(rows, symbol=str(symbol), target=target,
                            source_pin=source_pin, snapshot_as_of=snapshot.get("as_of"),
                            calendar=calendar)


def project_entry_confirmation_daily_facts_from_rows(
    rows: Sequence[Any], *, symbol: str, current_trade_date: date | datetime | str,
    source_pin: Mapping[str, Any], source_as_of: Any = None, calendar: Any | None = None,
) -> dict[str, Any]:
    """Project one publication-time facts object from an already pinned base.

    Publication creates the small versioned object before the outer manifest
    hash exists, so its source pin binds the base snapshot and publication
    identity but intentionally does not contain a circular manifest hash.
    Runtime readers add the manifest byte/object pins after verifying the
    bound facts object.
    """

    target = _trade_date(current_trade_date)
    if target is None:
        return _unproven("TARGET_TRADE_DATE_INVALID", source_pin=source_pin)
    result = _build_from_rows(
        rows, symbol=str(symbol), target=target, source_pin=source_pin,
        snapshot_as_of=source_as_of, require_manifest_pin=False, require_raw_plan=True,
        calendar=calendar,
    )
    if result.get("status") == "READY":
        result = {
            **result,
            "schema_version": ENTRY_CONFIRMATION_FACTS_SCHEMA,
        }
    return result


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_manifest(path: Path) -> tuple[dict[str, Any], str] | None:
    try:
        if path.stat().st_size > 2_000_000:
            return None
        raw = path.read_bytes()
        body = json.loads(raw)
    except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
        return None
    if not isinstance(body, dict) or body.get("schema_version") != MANIFEST_SCHEMA:
        return None
    claimed = body.get("manifest_sha256")
    unsigned = {key: value for key, value in body.items() if key != "manifest_sha256"}
    if not _hash(claimed) or _object_hash(unsigned) != claimed:
        return None
    return body, hashlib.sha256(raw).hexdigest()


def _snapshot_scalars(path: Path, symbol: str) -> tuple[dict[str, Any], list[Any]] | None:
    """Read only scalar identity and one symbol's daily list from a snapshot."""

    try:
        import ijson  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover - dependency is in pyproject
        ijson = None
    try:
        if ijson is not None:
            scalars: dict[str, Any] = {}
            with path.open("rb") as handle:
                for key in ("snapshot_id", "snapshot_hash", "as_of"):
                    handle.seek(0)
                    value = next(ijson.items(handle, key), None)
                    scalars[key] = value
            rows: list[Any] | None = None
            with path.open("rb") as handle:
                for key, value in ijson.kvitems(handle, "data.RECENT_DAILY_BARS"):
                    if str(key).upper() == str(symbol).upper():
                        rows = value
                        break
            if rows is None:
                rows = []
            return scalars, rows
        # Small test/maintenance snapshots have a harmless fallback.  The
        # production path has ijson installed and never creates this copy.
        if path.stat().st_size > 10 * 1024 * 1024:
            return None
        body = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(body, Mapping) or not isinstance(body.get("data"), Mapping):
            return None
        return {key: body.get(key) for key in ("snapshot_id", "snapshot_hash", "as_of")}, list(
            _rows_for_symbol(body["data"], symbol) or ()
        )
    except (OSError, ValueError, TypeError, StopIteration):
        return None


def _snapshot_scalars_for_symbols(path: Path, symbols: Sequence[str]) -> tuple[dict[str, Any], dict[str, list[Any]]] | None:
    """Read snapshot identity once and retain only requested symbol rows."""

    wanted = {str(symbol).upper() for symbol in symbols}
    try:
        import ijson  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover - dependency is in pyproject
        ijson = None
    try:
        if ijson is not None:
            scalars: dict[str, Any] = {}
            with path.open("rb") as handle:
                for key in ("snapshot_id", "snapshot_hash", "as_of"):
                    handle.seek(0)
                    scalars[key] = next(ijson.items(handle, key), None)
            rows: dict[str, list[Any]] = {}
            with path.open("rb") as handle:
                for key, value in ijson.kvitems(handle, "data.RECENT_DAILY_BARS"):
                    normalized = str(key).upper()
                    if normalized in wanted and isinstance(value, list):
                        rows[normalized] = value
            return scalars, rows
        if path.stat().st_size > 10 * 1024 * 1024:
            return None
        body = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(body, Mapping) or not isinstance(body.get("data"), Mapping):
            return None
        recent = body["data"].get("RECENT_DAILY_BARS")
        rows = {
            str(key).upper(): list(value)
            for key, value in (recent.items() if isinstance(recent, Mapping) else ())
            if str(key).upper() in wanted and isinstance(value, (list, tuple))
        }
        return {key: body.get(key) for key in ("snapshot_id", "snapshot_hash", "as_of")}, rows
    except (OSError, ValueError, TypeError, StopIteration):
        return None


def _publication_entries(manifest: Mapping[str, Any], *, symbol_by_plan: Mapping[str, str] | None = None) -> list[Mapping[str, Any]]:
    plans = manifest.get("plans")
    if not isinstance(plans, list):
        return []
    result = []
    for item in plans:
        if not isinstance(item, Mapping) or not isinstance(item.get("plan_id"), str):
            continue
        payload = item.get("published_payload")
        payload = payload.get("value") if isinstance(payload, Mapping) else None
        symbol = payload.get("symbol") if isinstance(payload, Mapping) else None
        if symbol_by_plan is not None:
            symbol = symbol_by_plan.get(str(item["plan_id"]), symbol)
        result.append({**dict(item), "_symbol": str(symbol).upper() if symbol else None})
    return result


def _validated_small_facts(
    entry: Mapping[str, Any], *, symbol: str, target: date,
    manifest: Mapping[str, Any], manifest_path: Path, manifest_file_sha: str,
    calendar: Any | None = None,
) -> dict[str, Any]:
    """Validate one publication-time facts object without opening its base."""

    bound = entry.get("entry_confirmation_daily_facts")
    if not isinstance(bound, Mapping):
        return _unproven("ENTRY_CONFIRMATION_FACTS_MISSING")
    if bound.get("schema_version") != ENTRY_CONFIRMATION_FACTS_SCHEMA:
        return _unproven("ENTRY_CONFIRMATION_FACTS_SCHEMA_INVALID")
    value = bound.get("value")
    claimed_hash = bound.get("sha256")
    if not isinstance(value, Mapping) or not _hash(claimed_hash):
        return _unproven("ENTRY_CONFIRMATION_FACTS_BOUND_INVALID")
    if _object_hash(value) != claimed_hash:
        return _unproven("ENTRY_CONFIRMATION_FACTS_HASH_MISMATCH")
    if value.get("schema_version") != ENTRY_CONFIRMATION_FACTS_SCHEMA:
        return _unproven("ENTRY_CONFIRMATION_FACTS_SCHEMA_INVALID")
    source_pin = value.get("source_pin")
    if not isinstance(source_pin, Mapping):
        return _unproven("ENTRY_CONFIRMATION_FACTS_SOURCE_PIN_MISSING")
    base = manifest.get("base_snapshot")
    selected_plan_id = entry.get("plan_id")
    if not isinstance(base, Mapping) or not isinstance(selected_plan_id, str):
        return _unproven("ENTRY_CONFIRMATION_FACTS_PROVENANCE_INVALID")
    expected_base_hash = base.get("snapshot_hash")
    expected_base_id = base.get("snapshot_id")
    expected_base_file = base.get("sha256")
    if (
        source_pin.get("source_run_id") != manifest.get("run_id")
        or source_pin.get("lane_id") != manifest.get("lane_id")
        or source_pin.get("plan_id") != selected_plan_id
        or source_pin.get("snapshot_id") != expected_base_id
        or source_pin.get("snapshot_hash") != expected_base_hash
        or source_pin.get("snapshot_file_sha256") != expected_base_file
        or not _hash(source_pin.get("raw_plan_sha256"))
        or source_pin.get("target_trade_date") != target.isoformat()
        or not _hash(expected_base_hash)
        or not _hash(expected_base_file)
    ):
        return _unproven("ENTRY_CONFIRMATION_FACTS_PROVENANCE_CONFLICT")
    published_payload = entry.get("published_payload")
    published_value = published_payload.get("value") if isinstance(published_payload, Mapping) else None
    published_symbol = published_value.get("symbol") if isinstance(published_value, Mapping) else None
    if not isinstance(published_symbol, str) or published_symbol.upper() != str(symbol).upper():
        return _unproven("PUBLICATION_SYMBOL_CONFLICT")

    closes = value.get("daily_closes")
    previous_high = value.get("previous_day_high")
    daily_ma5 = value.get("daily_ma5")
    if value.get("status") != "READY" or value.get("reason_codes") not in ([], ()):
        reasons = value.get("reason_codes")
        if not isinstance(reasons, (list, tuple)):
            reasons = ["ENTRY_CONFIRMATION_FACTS_UNPROVEN"]
        return _unproven(*[str(item) for item in reasons if item], source_pin=source_pin)
    if not isinstance(closes, list) or len(closes) != 20:
        return _unproven("ENTRY_CONFIRMATION_FACTS_DAILY20_INVALID", source_pin=source_pin)
    parsed_dates: list[date] = []
    for item in closes:
        if not isinstance(item, Mapping):
            return _unproven("ENTRY_CONFIRMATION_FACTS_DAILY20_INVALID", source_pin=source_pin)
        stamp = _as_of(item.get("as_of"))
        close = _finite(item.get("close"), positive=True)
        if stamp is None or close is None or stamp.date() >= target:
            return _unproven("ENTRY_CONFIRMATION_FACTS_DAILY20_INVALID", source_pin=source_pin)
        parsed_dates.append(stamp.date())
    if parsed_dates != sorted(parsed_dates) or len(set(parsed_dates)) != len(parsed_dates):
        return _unproven("ENTRY_CONFIRMATION_FACTS_DAILY20_ORDER_INVALID", source_pin=source_pin)
    expected_previous = _previous_exchange_day(target, calendar)
    if expected_previous is None:
        return _unproven("TRADING_CALENDAR_UNAVAILABLE", source_pin=source_pin)
    if parsed_dates[-1] != expected_previous:
        return _unproven("PREVIOUS_TRADING_DAY_MISSING", source_pin=source_pin)
    if not isinstance(previous_high, Mapping) or not isinstance(daily_ma5, Mapping):
        return _unproven("ENTRY_CONFIRMATION_FACTS_LEVELS_INVALID", source_pin=source_pin)
    high_at = _as_of(previous_high.get("as_of"))
    ma_at = _as_of(daily_ma5.get("as_of"))
    if (
        high_at is None or ma_at is None
        or high_at.date() != parsed_dates[-1]
        or ma_at.date() != parsed_dates[-1]
        or _finite(previous_high.get("value"), positive=True) is None
        or _finite(daily_ma5.get("value"), positive=True) is None
    ):
        return _unproven("ENTRY_CONFIRMATION_FACTS_LEVELS_INVALID", source_pin=source_pin)
    runtime_pin = {
        **dict(source_pin),
        "publication_manifest_path": str(manifest_path),
        "manifest_sha256": manifest.get("manifest_sha256"),
        "publication_manifest_sha256": manifest.get("manifest_sha256"),
        "manifest_file_sha256": manifest_file_sha,
        "facts_sha256": claimed_hash,
    }
    return {
        "status": "READY",
        "reason_codes": [],
        "daily_closes": [
            {"close": _finite(item["close"], positive=True), "as_of": _as_of(item["as_of"]).isoformat()}
            for item in closes
        ],
        "previous_day_high": {
            "value": _finite(previous_high["value"], positive=True),
            "as_of": high_at.isoformat(),
        },
        "daily_ma5": {
            "value": _finite(daily_ma5["value"], positive=True),
            "as_of": ma_at.isoformat(),
        },
        "source_pin": runtime_pin,
    }


def read_entry_confirmation_daily_facts_small_batch(
    publication_manifest_path: str | Path, *, symbols: Sequence[str],
    current_trade_date: date | datetime | str, source_run_id: str | None = None,
    lane_id: str | None = None, plan_ids: Mapping[str, str] | None = None,
    expected_manifest_file_sha256: str | None = None,
    calendar: Any | None = None,
) -> dict[str, dict[str, Any]]:
    """Read only the small, publication-time facts objects.

    This is the runtime path.  It never follows ``base_snapshot.path`` and
    therefore cannot re-read or re-hash a full-market snapshot on each A4
    minute.  A missing object is deliberately UNPROVEN; there is no fallback
    to the legacy source scan.
    """

    normalized_symbols = tuple(dict.fromkeys(str(symbol).upper() for symbol in symbols if str(symbol).strip()))
    target = _trade_date(current_trade_date)
    if target is None:
        return {symbol: _unproven("TARGET_TRADE_DATE_INVALID") for symbol in normalized_symbols}
    path = Path(publication_manifest_path)
    try:
        path = path.resolve(strict=True)
    except (OSError, RuntimeError, TypeError):
        return {symbol: _unproven("PUBLICATION_MANIFEST_MISSING") for symbol in normalized_symbols}
    parsed = _read_manifest(path)
    if parsed is None:
        return {symbol: _unproven("PUBLICATION_MANIFEST_INVALID") for symbol in normalized_symbols}
    manifest, manifest_file_sha = parsed
    if expected_manifest_file_sha256 is not None and manifest_file_sha != expected_manifest_file_sha256:
        return {symbol: _unproven("PUBLICATION_MANIFEST_FILE_HASH_MISMATCH") for symbol in normalized_symbols}
    if source_run_id is not None and manifest.get("run_id") != source_run_id:
        return {symbol: _unproven("PUBLICATION_SOURCE_RUN_CONFLICT") for symbol in normalized_symbols}
    if lane_id is not None and manifest.get("lane_id") != lane_id:
        return {symbol: _unproven("PUBLICATION_LANE_CONFLICT") for symbol in normalized_symbols}
    entries = _publication_entries(manifest)
    output: dict[str, dict[str, Any]] = {}
    for symbol in normalized_symbols:
        requested_plan = plan_ids.get(symbol) if isinstance(plan_ids, Mapping) else None
        candidates = [entry for entry in entries if entry.get("_symbol") == symbol]
        if requested_plan is not None:
            candidates = [entry for entry in candidates if entry.get("plan_id") == requested_plan]
        elif len(entries) == 1 and not candidates:
            candidates = entries
        if len(candidates) != 1:
            output[symbol] = _unproven("PUBLICATION_PLAN_AMBIGUOUS")
            continue
        entry = candidates[0]
        if _trade_date(entry.get("target_trade_date")) != target:
            output[symbol] = _unproven("PUBLICATION_TARGET_DATE_CONFLICT")
            continue
        output[symbol] = _validated_small_facts(
            entry, symbol=symbol, target=target, manifest=manifest,
            manifest_path=path, manifest_file_sha=manifest_file_sha, calendar=calendar,
        )
    return output


def read_entry_confirmation_daily_facts_batch(
    publication_manifest_path: str | Path, *, symbols: Sequence[str],
    current_trade_date: date | datetime | str, source_run_id: str | None = None,
    lane_id: str | None = None, plan_ids: Mapping[str, str] | None = None,
    expected_manifest_file_sha256: str | None = None,
    calendar: Any | None = None,
) -> dict[str, dict[str, Any]]:
    """Read one publication source for several symbols in one bounded pass.

    Only the requested daily rows are retained.  The manifest and snapshot
    file are hashed once, which keeps a per-minute A4 caller within its
    shared budget.  ``plan_ids`` is keyed by symbol when a lane has multiple
    publication entries; symbols without an unambiguous entry are UNPROVEN.
    """

    normalized_symbols = tuple(dict.fromkeys(str(symbol).upper() for symbol in symbols if str(symbol).strip()))
    target = _trade_date(current_trade_date)
    if target is None:
        return {symbol: _unproven("TARGET_TRADE_DATE_INVALID") for symbol in normalized_symbols}
    if not normalized_symbols:
        return {}
    path = Path(publication_manifest_path)
    try:
        path = path.resolve(strict=True)
    except (OSError, RuntimeError, TypeError):
        return {symbol: _unproven("PUBLICATION_MANIFEST_MISSING") for symbol in normalized_symbols}
    parsed = _read_manifest(path)
    if parsed is None:
        return {symbol: _unproven("PUBLICATION_MANIFEST_INVALID") for symbol in normalized_symbols}
    manifest, manifest_file_sha = parsed
    if expected_manifest_file_sha256 is not None and manifest_file_sha != expected_manifest_file_sha256:
        return {symbol: _unproven("PUBLICATION_MANIFEST_FILE_HASH_MISMATCH") for symbol in normalized_symbols}
    if source_run_id is not None and manifest.get("run_id") != source_run_id:
        return {symbol: _unproven("PUBLICATION_SOURCE_RUN_CONFLICT") for symbol in normalized_symbols}
    if lane_id is not None and manifest.get("lane_id") != lane_id:
        return {symbol: _unproven("PUBLICATION_LANE_CONFLICT") for symbol in normalized_symbols}
    entries = _publication_entries(manifest)
    if not entries:
        return {symbol: _unproven("PUBLICATION_PLAN_SET_INVALID") for symbol in normalized_symbols}
    by_symbol: dict[str, Mapping[str, Any]] = {}
    for symbol in normalized_symbols:
        requested_plan = plan_ids.get(symbol) if isinstance(plan_ids, Mapping) else None
        candidates = [entry for entry in entries if requested_plan is not None and entry.get("plan_id") == requested_plan]
        if requested_plan is not None:
            # The runtime row supplies identity, but the publication manifest
            # remains authoritative for the plan-to-symbol binding. Never
            # project another symbol's bars merely because a plan id matches.
            candidates = [entry for entry in candidates if entry.get("_symbol") == symbol]
        if requested_plan is None:
            candidates = [entry for entry in entries if entry.get("_symbol") == symbol]
            if not candidates and len(entries) == 1:
                candidates = entries
        if len(candidates) == 1:
            by_symbol[symbol] = candidates[0]
    if not by_symbol:
        return {symbol: _unproven("PUBLICATION_PLAN_AMBIGUOUS") for symbol in normalized_symbols}
    if any((_trade_date(entry.get("target_trade_date")) != target) for entry in by_symbol.values()):
        return {symbol: _unproven("PUBLICATION_TARGET_DATE_CONFLICT") for symbol in normalized_symbols}

    base = manifest.get("base_snapshot")
    if not isinstance(base, Mapping):
        return {symbol: _unproven("PUBLICATION_BASE_SNAPSHOT_MISSING") for symbol in normalized_symbols}
    snapshot_path_raw = base.get("path")
    expected_file_sha = base.get("sha256")
    expected_snapshot_hash = base.get("snapshot_hash")
    expected_snapshot_id = base.get("snapshot_id")
    if (not isinstance(snapshot_path_raw, str) or not _hash(expected_file_sha)
            or not _hash(expected_snapshot_hash) or not isinstance(expected_snapshot_id, str)):
        return {symbol: _unproven("PUBLICATION_BASE_PIN_INVALID") for symbol in normalized_symbols}
    try:
        snapshot_path = Path(snapshot_path_raw).resolve(strict=True)
        actual_file_sha = _file_sha256(snapshot_path)
    except (OSError, RuntimeError, TypeError):
        return {symbol: _unproven("PUBLICATION_BASE_SNAPSHOT_READ_FAILED") for symbol in normalized_symbols}
    if actual_file_sha != expected_file_sha:
        return {symbol: _unproven("PUBLICATION_BASE_FILE_HASH_MISMATCH") for symbol in normalized_symbols}
    scalar_result = _snapshot_scalars_for_symbols(snapshot_path, normalized_symbols)
    if scalar_result is None:
        return {symbol: _unproven("PUBLICATION_BASE_SNAPSHOT_INVALID") for symbol in normalized_symbols}
    scalars, row_map = scalar_result
    if (scalars.get("snapshot_id") != expected_snapshot_id
            or scalars.get("snapshot_hash") != expected_snapshot_hash):
        return {symbol: _unproven("PUBLICATION_BASE_SNAPSHOT_ID_HASH_CONFLICT") for symbol in normalized_symbols}

    output: dict[str, dict[str, Any]] = {}
    for symbol in normalized_symbols:
        entry = by_symbol.get(symbol)
        if entry is None:
            output[symbol] = _unproven("PUBLICATION_PLAN_AMBIGUOUS")
            continue
        source_pin = {
            "source_run_id": manifest.get("run_id"), "lane_id": manifest.get("lane_id"),
            "plan_id": entry.get("plan_id"), "publication_manifest_path": str(path),
            "manifest_sha256": manifest.get("manifest_sha256"),
            "publication_manifest_sha256": manifest.get("manifest_sha256"),
            "manifest_file_sha256": manifest_file_sha, "snapshot_path": str(snapshot_path),
            "snapshot_id": expected_snapshot_id, "snapshot_hash": expected_snapshot_hash,
            "source_snapshot_id": expected_snapshot_id, "source_snapshot_hash": expected_snapshot_hash,
            "snapshot_file_sha256": actual_file_sha, "raw_plan_sha256": entry.get("raw_plan_sha256"),
            "a3_audit_sha256": entry.get("a3_audit_sha256"), "target_trade_date": target.isoformat(),
        }
        if not _source_pin_ready(source_pin):
            output[symbol] = _unproven("SOURCE_PIN_INCOMPLETE", source_pin=source_pin)
            continue
        rows = row_map.get(symbol)
        if rows is None:
            output[symbol] = _unproven("DAILY_BARS_SYMBOL_MISSING", source_pin=source_pin)
            continue
        output[symbol] = _build_from_rows(rows, symbol=symbol, target=target,
                                          source_pin=source_pin, snapshot_as_of=scalars.get("as_of"),
                                          calendar=calendar)
    return output


_RUNTIME_FACT_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}


def _stat_fingerprint(path: Path) -> tuple[int, int, int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return (int(stat.st_dev), int(stat.st_ino), int(stat.st_size), int(stat.st_mtime_ns))


def _runtime_manifest_path(settings: Any, source_run_id: str, lane_id: str) -> tuple[Path, str] | None:
    """Resolve the publication path from the run summary, never from a plan."""

    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,180}", str(source_run_id or "")):
        return None
    root = getattr(settings, "workflow_output_dir", None)
    if root is None:
        return None
    run_path = (Path(root) / "runs" / f"{source_run_id}.json").resolve()
    root_path = Path(root).resolve()
    try:
        if not run_path.is_relative_to(root_path) or not run_path.is_file() or run_path.stat().st_size > 2_000_000:
            return None
        summary = json.loads(run_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
        return None
    if not isinstance(summary, Mapping) or summary.get("run_id") not in (None, source_run_id):
        return None
    publication = summary.get("plan_publication")
    if not isinstance(publication, Mapping):
        return None
    candidates = publication.get("projection_manifests")
    if isinstance(candidates, list):
        candidates = [item for item in candidates if isinstance(item, Mapping) and item.get("lane_id") == lane_id]
    else:
        single = publication.get("projection_manifest")
        candidates = [single] if isinstance(single, Mapping) and single.get("lane_id", lane_id) == lane_id else []
    if len(candidates) != 1:
        return None
    descriptor = candidates[0]
    manifest_path = descriptor.get("path")
    manifest_sha = descriptor.get("sha256")
    if not isinstance(manifest_path, str) or not _hash(manifest_sha):
        return None
    try:
        resolved = Path(manifest_path).resolve(strict=True)
    except (OSError, RuntimeError, TypeError):
        return None
    return resolved, str(manifest_sha)


def _deadline_expired(deadline_monotonic: Any) -> bool:
    if deadline_monotonic is None:
        return False
    try:
        import time
        return time.monotonic() >= float(deadline_monotonic)
    except (TypeError, ValueError, OverflowError):
        return True


def read_runtime_daily_facts(
    settings: Any, plan_rows: Sequence[Mapping[str, Any]], *,
    current_trade_date: date | datetime | str, deadline_monotonic: float | None = None,
) -> dict[str, dict[str, Any]]:
    """Resolve daily facts from pinned small publication objects.

    ``plan_rows`` is only used for the persisted identity tuple
    ``(plan_id, symbol, lane_id, source_run_id)``.  It cannot supply prices,
    highs, closes, or a source path.  Results are keyed by the exact outer
    ``plan_id`` and are safe to pass as per-plan server context.
    """

    output: dict[str, dict[str, Any]] = {}
    groups: dict[tuple[str, str], list[dict[str, str]]] = {}
    rows_iterable = plan_rows.values() if isinstance(plan_rows, Mapping) else (plan_rows or ())
    for row in rows_iterable:
        if not isinstance(row, Mapping):
            continue
        plan_id = row.get("plan_id")
        symbol = row.get("symbol")
        lane_id = row.get("lane_id")
        source_run_id = row.get("source_run_id")
        payload = row.get("payload")
        if payload is None:
            payload = row.get("payload_json")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = None
        if not isinstance(source_run_id, str) and isinstance(payload, Mapping):
            source_run_id = payload.get("source_run_id")
        if not all(isinstance(value, str) and value for value in (plan_id, symbol, lane_id, source_run_id)):
            if isinstance(plan_id, str) and plan_id:
                output[plan_id] = _unproven("PLAN_SOURCE_IDENTITY_INCOMPLETE")
            continue
        item = {"plan_id": plan_id, "symbol": symbol.upper(), "lane_id": lane_id, "source_run_id": source_run_id}
        groups.setdefault((source_run_id, lane_id), []).append(item)

    for (source_run_id, lane_id), items in groups.items():
        if _deadline_expired(deadline_monotonic):
            for item in items:
                output[item["plan_id"]] = _unproven("DAILY_FACTS_DEADLINE_EXCEEDED")
            continue
        manifest_info = _runtime_manifest_path(settings, source_run_id, lane_id)
        if manifest_info is None:
            for item in items:
                output[item["plan_id"]] = _unproven("PUBLICATION_MANIFEST_DISCOVERY_FAILED")
            continue
        manifest_path, expected_manifest_file_sha = manifest_info
        # A symbol is not an identity key.  Two plans for the same symbol can
        # legitimately coexist in one run/lane, and a dict keyed only by
        # symbol would silently overwrite the first plan id.  Keep the fast
        # one-pass path for unambiguous symbols; duplicate-symbol plans are
        # read one at a time so the manifest selector can bind each exact id.
        by_symbol: dict[str, list[dict[str, str]]] = {}
        for item in items:
            by_symbol.setdefault(item["symbol"], []).append(item)
        ambiguous_items = [
            item for symbol_items in by_symbol.values()
            if len({candidate["plan_id"] for candidate in symbol_items}) > 1
            for item in symbol_items
        ]
        batch_items = [
            item for symbol_items in by_symbol.values()
            if len({candidate["plan_id"] for candidate in symbol_items}) == 1
            for item in symbol_items[:1]
        ]
        cache_key = (
            str(manifest_path), source_run_id, lane_id,
            _trade_date(current_trade_date), tuple(sorted((item["plan_id"], item["symbol"]) for item in items)),
        )
        manifest_stat = _stat_fingerprint(manifest_path)
        cached = _RUNTIME_FACT_CACHE.get(cache_key)
        if (cached is not None and cached.get("manifest_stat") == manifest_stat
                and cached.get("manifest_sha256") == expected_manifest_file_sha):
            facts = deepcopy(cached["facts"])
        else:
            if _deadline_expired(deadline_monotonic):
                facts = {item["symbol"]: _unproven("DAILY_FACTS_DEADLINE_EXCEEDED") for item in items}
            else:
                facts = read_entry_confirmation_daily_facts_small_batch(
                    manifest_path,
                    symbols=tuple(item["symbol"] for item in batch_items),
                    current_trade_date=current_trade_date,
                    source_run_id=source_run_id,
                    lane_id=lane_id,
                    plan_ids={item["symbol"]: item["plan_id"] for item in batch_items},
                    expected_manifest_file_sha256=expected_manifest_file_sha,
                ) if batch_items else {}
                _RUNTIME_FACT_CACHE[cache_key] = {
                    "manifest_stat": _stat_fingerprint(manifest_path),
                    "manifest_sha256": expected_manifest_file_sha,
                    "facts": deepcopy(facts),
                }
        for item in batch_items:
            output[item["plan_id"]] = deepcopy(
                facts.get(item["symbol"], _unproven("DAILY_FACTS_SYMBOL_RESULT_MISSING"))
            )

        # Do not reuse the batch result for duplicate symbols: it is keyed by
        # symbol and therefore cannot represent two different plan identities.
        # The single-item call is still bounded by the same manifest pin and
        # preserves the exact plan-to-source binding.
        for item in ambiguous_items:
            if _deadline_expired(deadline_monotonic):
                output[item["plan_id"]] = _unproven("DAILY_FACTS_DEADLINE_EXCEEDED")
                continue
            single = read_entry_confirmation_daily_facts_small_batch(
                manifest_path,
                symbols=(item["symbol"],),
                current_trade_date=current_trade_date,
                source_run_id=source_run_id,
                lane_id=lane_id,
                plan_ids={item["symbol"]: item["plan_id"]},
                expected_manifest_file_sha256=expected_manifest_file_sha,
            )
            output[item["plan_id"]] = deepcopy(
                single.get(item["symbol"], _unproven("DAILY_FACTS_SYMBOL_RESULT_MISSING"))
            )
    return output


def read_entry_confirmation_daily_facts(
    publication_manifest_path: str | Path, *, symbol: str,
    current_trade_date: date | datetime | str, source_run_id: str | None = None,
    lane_id: str | None = None, plan_id: str | None = None,
    expected_manifest_file_sha256: str | None = None,
    calendar: Any | None = None,
) -> dict[str, Any]:
    """Read a pinned publication source and return daily facts or UNPROVEN.

    The manifest is intentionally the first input.  A caller may provide an
    independently retained manifest byte pin; absent that pin, the manifest's
    canonical self-hash is still checked but no acquisition claim is made.
    """

    target = _trade_date(current_trade_date)
    if target is None:
        return _unproven("TARGET_TRADE_DATE_INVALID")
    path = Path(publication_manifest_path)
    try:
        path = path.resolve(strict=True)
    except (OSError, RuntimeError, TypeError):
        return _unproven("PUBLICATION_MANIFEST_MISSING")
    parsed = _read_manifest(path)
    if parsed is None:
        return _unproven("PUBLICATION_MANIFEST_INVALID")
    manifest, manifest_file_sha = parsed
    if expected_manifest_file_sha256 is not None and manifest_file_sha != expected_manifest_file_sha256:
        return _unproven("PUBLICATION_MANIFEST_FILE_HASH_MISMATCH")
    if source_run_id is not None and manifest.get("run_id") != source_run_id:
        return _unproven("PUBLICATION_SOURCE_RUN_CONFLICT")
    if lane_id is not None and manifest.get("lane_id") != lane_id:
        return _unproven("PUBLICATION_LANE_CONFLICT")
    plans = manifest.get("plans")
    if not isinstance(plans, list):
        return _unproven("PUBLICATION_PLAN_SET_INVALID")
    selected = [item for item in plans if isinstance(item, Mapping) and (plan_id is None or item.get("plan_id") == plan_id)]
    if plan_id is None and len(plans) == 1 and isinstance(plans[0], Mapping):
        selected = [plans[0]]
    if len(selected) != 1:
        return _unproven("PUBLICATION_PLAN_AMBIGUOUS")
    entry = selected[0]
    published_payload = entry.get("published_payload")
    published_value = published_payload.get("value") if isinstance(published_payload, Mapping) else None
    published_symbol = published_value.get("symbol") if isinstance(published_value, Mapping) else None
    if not isinstance(published_symbol, str) or published_symbol.upper() != str(symbol).upper():
        return _unproven("PUBLICATION_SYMBOL_CONFLICT")
    selected_plan_id = entry.get("plan_id")
    if not isinstance(selected_plan_id, str) or not selected_plan_id:
        return _unproven("PUBLICATION_PLAN_ID_INVALID")
    entry_date = _trade_date(entry.get("target_trade_date"))
    if entry_date != target:
        return _unproven("PUBLICATION_TARGET_DATE_CONFLICT")

    base = manifest.get("base_snapshot")
    if not isinstance(base, Mapping):
        return _unproven("PUBLICATION_BASE_SNAPSHOT_MISSING")
    snapshot_path_raw = base.get("path")
    if not isinstance(snapshot_path_raw, str) or not snapshot_path_raw:
        return _unproven("PUBLICATION_BASE_PATH_INVALID")
    try:
        snapshot_path = Path(snapshot_path_raw).resolve(strict=True)
    except (OSError, RuntimeError, TypeError):
        return _unproven("PUBLICATION_BASE_SNAPSHOT_MISSING")
    expected_file_sha = base.get("sha256")
    expected_snapshot_hash = base.get("snapshot_hash")
    expected_snapshot_id = base.get("snapshot_id")
    if not (_hash(expected_file_sha) and _hash(expected_snapshot_hash)
            and isinstance(expected_snapshot_id, str) and expected_snapshot_id):
        return _unproven("PUBLICATION_BASE_PIN_INVALID")
    try:
        actual_file_sha = _file_sha256(snapshot_path)
    except OSError:
        return _unproven("PUBLICATION_BASE_SNAPSHOT_READ_FAILED")
    if actual_file_sha != expected_file_sha:
        return _unproven("PUBLICATION_BASE_FILE_HASH_MISMATCH")
    scalar_result = _snapshot_scalars(snapshot_path, str(symbol))
    if scalar_result is None:
        return _unproven("PUBLICATION_BASE_SNAPSHOT_INVALID")
    scalars, rows = scalar_result
    if (scalars.get("snapshot_id") != expected_snapshot_id
            or scalars.get("snapshot_hash") != expected_snapshot_hash):
        return _unproven("PUBLICATION_BASE_SNAPSHOT_ID_HASH_CONFLICT")

    source_pin = {
        "source_run_id": manifest.get("run_id"),
        "lane_id": manifest.get("lane_id"),
        "plan_id": selected_plan_id,
        "publication_manifest_path": str(path),
        "manifest_sha256": manifest.get("manifest_sha256"),
        "publication_manifest_sha256": manifest.get("manifest_sha256"),
        "manifest_file_sha256": manifest_file_sha,
        "snapshot_path": str(snapshot_path),
        "snapshot_id": expected_snapshot_id,
        "snapshot_hash": expected_snapshot_hash,
        "source_snapshot_id": expected_snapshot_id,
        "source_snapshot_hash": expected_snapshot_hash,
        "snapshot_file_sha256": actual_file_sha,
        "raw_plan_sha256": entry.get("raw_plan_sha256"),
        "a3_audit_sha256": entry.get("a3_audit_sha256"),
        "target_trade_date": target.isoformat(),
    }
    if not _source_pin_ready(source_pin):
        return _unproven("SOURCE_PIN_INCOMPLETE", source_pin=source_pin)
    return _build_from_rows(rows, symbol=str(symbol), target=target,
                            source_pin=source_pin, snapshot_as_of=scalars.get("as_of"),
                            calendar=calendar)


# Descriptive aliases for the server adapter; the implementation remains one
# path so callers cannot accidentally select a legacy or plan-field fallback.
read_publication_daily_facts = read_entry_confirmation_daily_facts
read_publication_daily_facts_batch = read_entry_confirmation_daily_facts_batch
project_entry_confirmation_daily_facts = build_entry_confirmation_daily_facts


__all__ = [
    "ENTRY_CONFIRMATION_FACTS_SCHEMA",
    "build_entry_confirmation_daily_facts",
    "project_entry_confirmation_daily_facts",
    "project_entry_confirmation_daily_facts_from_rows",
    "read_entry_confirmation_daily_facts",
    "read_entry_confirmation_daily_facts_batch",
    "read_entry_confirmation_daily_facts_small_batch",
    "read_publication_daily_facts_batch",
    "read_runtime_daily_facts",
    "read_publication_daily_facts",
]
