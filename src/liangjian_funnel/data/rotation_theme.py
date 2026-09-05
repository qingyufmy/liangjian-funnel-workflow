"""Free-data rotation themes and point-in-time board aggregation.

This module is deliberately independent from the historical ``801xxx``
selected-board contract.  It owns a small, versioned Liangjian taxonomy and
turns public Eastmoney/Tencent observations into a transparent, auditable
``liangjian-rotation-theme/1.0.0`` snapshot.

The transport functions accept injected fetchers.  Tests and historical
replays therefore never need a live network, while the default fetchers keep
the public Eastmoney endpoints usable by an integration layer.  The module
never treats an unavailable page, a missing trading date, or a partial member
list as a complete observation.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import re
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")

# ``LIANGJIAN_ROTATION_THEME_V1`` is the taxonomy version, not a vendor
# version.  It is intentionally stable so a model cannot change mappings by
# changing a daily prompt or by returning another board name.
LIANGJIAN_ROTATION_THEME_V1 = "liangjian-rotation-themes/2026.09.v1"
ROTATION_THEME_CONFIG_SCHEMA = "liangjian-rotation-taxonomy/1.0.0"
ROTATION_THEME_SCHEMA = "liangjian-rotation-theme/1.0.0"
ROTATION_THEME_SOURCE_ID = "LIANGJIAN_FREE_ROTATION_V1"
MEMBERSHIP_SNAPSHOT_SCHEMA = "liangjian-rotation-membership/1.0.0"

PRIMARY = "PRIMARY"
CHILD = "CHILD"
ROTATION_THEME_TOP_N = 5
MINIMUM_FACTOR_COVERAGE = 0.5

# The score is shown as a 0..100 value.  Each input is percentile ranked over
# the active primary themes before its weight is applied.  The weights add to
# exactly 1.0 and are kept as integer percentages for audit output.
STRENGTH_WEIGHTS: dict[str, int] = {
    "relative_return": 25,
    "breadth": 20,
    "fund_flow": 25,
    "momentum_3_5d": 15,
    "leader_structure": 10,
    "rank_persistence": 5,
}

_THEME_ID = re.compile(r"^[A-Z][A-Z0-9_]{2,80}$")
_EASTMONEY_BOARD_CODE = re.compile(r"^BK[0-9]{3,10}$", re.IGNORECASE)
_SYMBOL = re.compile(r"^[0-9]{6}(?:\.(?:SH|SZ|BJ))?$", re.IGNORECASE)
_REQUIRED_ROOT_KEYS = frozenset({"schema_version", "version", "themes"})
_OPTIONAL_ROOT_KEYS = frozenset({"description"})
_REQUIRED_THEME_KEYS = frozenset(
    {
        "theme_id",
        "kind",
        "parent",
        "eastmoney_board_codes",
        "aliases",
        "effective_from",
        "effective_to",
    }
)
_OPTIONAL_THEME_KEYS = frozenset({"name", "display_name", "evidence", "strategy_theme_id"})


class RotationThemeConfigError(ValueError):
    """Stable, payload-free configuration error."""

    def __init__(self, reason_code: str):
        self.reason_code = str(reason_code)
        super().__init__(self.reason_code)


class RotationThemeDataError(ValueError):
    """Stable error raised for malformed or incomplete provider data."""

    def __init__(self, reason_code: str):
        self.reason_code = str(reason_code)
        super().__init__(self.reason_code)


@dataclass(frozen=True, slots=True)
class RotationTheme:
    """One immutable taxonomy node."""

    theme_id: str
    name: str
    kind: str
    parent: str | None
    eastmoney_board_codes: tuple[str, ...]
    aliases: tuple[str, ...]
    effective_from: date
    effective_to: date | None
    evidence: tuple[str, ...]
    strategy_theme_id: str

    @property
    def display_name(self) -> str:
        return self.name

    @property
    def is_child(self) -> bool:
        return self.kind == CHILD

    def active_on(self, trade_date: date) -> bool:
        return self.effective_from <= trade_date and (
            self.effective_to is None or trade_date <= self.effective_to
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "theme_id": self.theme_id,
            "name": self.name,
            "display_name": self.name,
            "kind": self.kind,
            "parent": self.parent,
            "eastmoney_board_codes": list(self.eastmoney_board_codes),
            "aliases": list(self.aliases),
            "effective_from": self.effective_from.isoformat(),
            "effective_to": self.effective_to.isoformat() if self.effective_to else None,
            "evidence": list(self.evidence),
            "strategy_theme_id": self.strategy_theme_id,
        }


@dataclass(frozen=True, slots=True)
class RotationThemeConfig:
    """Validated versioned taxonomy with deterministic lookups."""

    schema_version: str
    version: str
    themes: tuple[RotationTheme, ...]
    description: str = ""

    @property
    def by_id(self) -> dict[str, RotationTheme]:
        return {theme.theme_id: theme for theme in self.themes}

    @property
    def code_to_theme(self) -> dict[str, str]:
        result: dict[str, str] = {}
        for theme in self.themes:
            result.update({code: theme.theme_id for code in theme.eastmoney_board_codes})
        return result

    @property
    def alias_to_theme(self) -> dict[str, str]:
        result: dict[str, str] = {}
        for theme in self.themes:
            for alias in theme.aliases:
                result[alias] = theme.theme_id
        return result

    def get(self, theme_id: str) -> RotationTheme:
        try:
            return self.by_id[str(theme_id)]
        except KeyError as exc:
            raise RotationThemeConfigError("ROTATION_THEME_UNKNOWN_ID") from exc

    def active(self, trade_date: date) -> tuple[RotationTheme, ...]:
        return tuple(theme for theme in self.themes if theme.active_on(trade_date))

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "version": self.version,
            "description": self.description,
            "themes": [theme.as_dict() for theme in self.themes],
        }


def validate_rotation_theme_config(payload: Any) -> RotationThemeConfig:
    """Strictly validate a taxonomy mapping and return immutable objects.

    The validator intentionally rejects unknown keys, ambiguous aliases,
    duplicate Eastmoney codes, parent cycles, and a child whose parent is not
    a primary node.  Empty Eastmoney code lists are permitted only when the
    row carries non-empty evidence explaining the unresolved public mapping.
    """

    if isinstance(payload, RotationThemeConfig):
        return payload
    if not isinstance(payload, Mapping):
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_NOT_MAPPING")
    keys = set(str(key) for key in payload)
    if not _REQUIRED_ROOT_KEYS.issubset(keys):
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_ROOT_KEYS_MISSING")
    if keys - _REQUIRED_ROOT_KEYS - _OPTIONAL_ROOT_KEYS:
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_ROOT_KEYS_UNKNOWN")
    if str(payload.get("schema_version") or "") != ROTATION_THEME_CONFIG_SCHEMA:
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_SCHEMA_MISMATCH")
    if str(payload.get("version") or "") != LIANGJIAN_ROTATION_THEME_V1:
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_VERSION_MISMATCH")
    raw_themes = payload.get("themes")
    if not isinstance(raw_themes, Sequence) or isinstance(raw_themes, (str, bytes, bytearray)) or not raw_themes:
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_THEMES_MISSING")

    themes: list[RotationTheme] = []
    seen_ids: set[str] = set()
    seen_codes: dict[str, str] = {}
    seen_aliases: dict[str, str] = {}
    for raw in raw_themes:
        if not isinstance(raw, Mapping):
            raise RotationThemeConfigError("ROTATION_THEME_ROW_NOT_MAPPING")
        row_keys = set(str(key) for key in raw)
        if not _REQUIRED_THEME_KEYS.issubset(row_keys):
            raise RotationThemeConfigError("ROTATION_THEME_ROW_KEYS_MISSING")
        if row_keys - _REQUIRED_THEME_KEYS - _OPTIONAL_THEME_KEYS:
            raise RotationThemeConfigError("ROTATION_THEME_ROW_KEYS_UNKNOWN")
        theme_id = str(raw.get("theme_id") or "").strip().upper()
        if not _THEME_ID.fullmatch(theme_id) or theme_id in seen_ids:
            raise RotationThemeConfigError("ROTATION_THEME_ID_INVALID_OR_DUPLICATED")
        seen_ids.add(theme_id)
        strategy_theme_id = str(raw.get("strategy_theme_id") or theme_id).strip().upper()
        if not _THEME_ID.fullmatch(strategy_theme_id):
            raise RotationThemeConfigError("ROTATION_THEME_STRATEGY_ID_INVALID")
        name = str(raw.get("name") or raw.get("display_name") or "").strip()
        if not name or not any("\u4e00" <= char <= "\u9fff" for char in name):
            raise RotationThemeConfigError("ROTATION_THEME_NAME_INVALID")
        kind = str(raw.get("kind") or "").strip().upper()
        if kind not in {PRIMARY, CHILD}:
            raise RotationThemeConfigError("ROTATION_THEME_KIND_INVALID")
        parent_raw = raw.get("parent")
        parent = str(parent_raw).strip().upper() if parent_raw not in (None, "") else None
        if kind == PRIMARY and parent is not None:
            raise RotationThemeConfigError("ROTATION_THEME_PRIMARY_PARENT_FORBIDDEN")
        if kind == CHILD and (not parent or not _THEME_ID.fullmatch(parent)):
            raise RotationThemeConfigError("ROTATION_THEME_CHILD_PARENT_MISSING")

        raw_codes = raw.get("eastmoney_board_codes")
        if not isinstance(raw_codes, Sequence) or isinstance(raw_codes, (str, bytes, bytearray)):
            raise RotationThemeConfigError("ROTATION_THEME_BOARD_CODES_INVALID")
        codes: list[str] = []
        for value in raw_codes:
            code = str(value or "").strip().upper()
            if not _EASTMONEY_BOARD_CODE.fullmatch(code) or code in codes:
                raise RotationThemeConfigError("ROTATION_THEME_BOARD_CODE_INVALID_OR_DUPLICATED")
            if code in seen_codes:
                raise RotationThemeConfigError("ROTATION_THEME_BOARD_CODE_AMBIGUOUS")
            codes.append(code)
            seen_codes[code] = theme_id

        raw_aliases = raw.get("aliases")
        if not isinstance(raw_aliases, Sequence) or isinstance(raw_aliases, (str, bytes, bytearray)) or not raw_aliases:
            raise RotationThemeConfigError("ROTATION_THEME_ALIASES_MISSING")
        aliases: list[str] = []
        for value in raw_aliases:
            alias = str(value or "").strip()
            if not alias or alias in aliases:
                raise RotationThemeConfigError("ROTATION_THEME_ALIAS_INVALID_OR_DUPLICATED")
            if alias in seen_aliases:
                raise RotationThemeConfigError("ROTATION_THEME_ALIAS_AMBIGUOUS")
            aliases.append(alias)
            seen_aliases[alias] = theme_id

        effective_from = _parse_date(raw.get("effective_from"), "ROTATION_THEME_EFFECTIVE_FROM_INVALID")
        effective_to = (
            _parse_date(raw.get("effective_to"), "ROTATION_THEME_EFFECTIVE_TO_INVALID")
            if raw.get("effective_to") not in (None, "")
            else None
        )
        if effective_to is not None and effective_to < effective_from:
            raise RotationThemeConfigError("ROTATION_THEME_EFFECTIVE_RANGE_INVALID")
        evidence = _string_sequence(raw.get("evidence"), allow_empty=True)
        if not codes and not evidence:
            raise RotationThemeConfigError("ROTATION_THEME_UNRESOLVED_CODE_EVIDENCE_MISSING")
        themes.append(
            RotationTheme(
                theme_id=theme_id,
                name=name,
                kind=kind,
                parent=parent,
                eastmoney_board_codes=tuple(codes),
                aliases=tuple(aliases),
                effective_from=effective_from,
                effective_to=effective_to,
                evidence=tuple(evidence),
                strategy_theme_id=strategy_theme_id,
            )
        )

    by_id = {theme.theme_id: theme for theme in themes}
    for theme in themes:
        if theme.parent is None:
            continue
        parent = by_id.get(theme.parent)
        if parent is None:
            raise RotationThemeConfigError("ROTATION_THEME_PARENT_MISSING")
        if parent.kind != PRIMARY:
            raise RotationThemeConfigError("ROTATION_THEME_PARENT_NOT_PRIMARY")
        if theme.effective_from < parent.effective_from:
            raise RotationThemeConfigError("ROTATION_THEME_CHILD_EFFECTIVE_RANGE_INVALID")
        if (
            parent.effective_to is not None
            and (theme.effective_to is None or theme.effective_to > parent.effective_to)
        ):
            raise RotationThemeConfigError("ROTATION_THEME_CHILD_OUTLIVES_PARENT")
    return RotationThemeConfig(
        schema_version=ROTATION_THEME_CONFIG_SCHEMA,
        version=LIANGJIAN_ROTATION_THEME_V1,
        description=str(payload.get("description") or "").strip(),
        themes=tuple(themes),
    )


def load_rotation_theme_config(path: str | Path | None = None) -> RotationThemeConfig:
    """Load and strictly validate the repository taxonomy YAML."""

    target = Path(path) if path is not None else _default_config_path()
    try:
        import yaml

        payload = yaml.safe_load(target.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_MISSING") from exc
    except (OSError, UnicodeError) as exc:
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_READ_FAILED") from exc
    except Exception as exc:
        raise RotationThemeConfigError("ROTATION_THEME_CONFIG_YAML_INVALID") from exc
    return validate_rotation_theme_config(payload)


read_rotation_theme_config = load_rotation_theme_config


def _default_config_path() -> Path:
    # .../src/liangjian_funnel/data/rotation_theme.py -> repository root.
    return Path(__file__).resolve().parents[3] / "config" / "rotation_themes_v1.yaml"


# ---------------------------------------------------------------------------
# Eastmoney board/catalog/member collectors
# ---------------------------------------------------------------------------


EASTMONEY_BOARD_SOURCE_ID = "EASTMONEY_FREE_ROTATION_BOARD"
EASTMONEY_BOARD_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
_EASTMONEY_DEFAULT_PAGE_SIZE = 100


def collect_eastmoney_board_catalog(
    *,
    as_of: datetime,
    fetch_page: Callable[..., Any] | None = None,
    board_type: str = "all",
    page_size: int = _EASTMONEY_DEFAULT_PAGE_SIZE,
    expected_trade_date: date | None = None,
    max_pages: int = 100,
) -> dict[str, Any]:
    """Collect a complete paginated Eastmoney industry/concept directory."""

    cutoff, trade_day = _cutoff_and_trade_date(as_of, expected_trade_date)
    try:
        result = _collect_paginated(
            fetch_page or _default_eastmoney_page_fetch,
            request_kind="catalog",
            request_value=board_type,
            as_of=cutoff,
            expected_trade_date=trade_day,
            page_size=page_size,
            max_pages=max_pages,
            identity_key=_board_identity,
            normalize=_normalize_board_record,
            require_non_empty=False,
        )
    except RotationThemeDataError as exc:
        return _unavailable_source_snapshot(
            cutoff,
            trade_day,
            reason_code=exc.reason_code,
            source_id=EASTMONEY_BOARD_SOURCE_ID,
            dataset="BOARD_CATALOG",
        )
    return {
        **result,
        "dataset": "BOARD_CATALOG",
        "source_id": EASTMONEY_BOARD_SOURCE_ID,
        "source_url": EASTMONEY_BOARD_URL,
        "trade_date": trade_day.isoformat(),
        "captured_at": result["captured_at"],
    }


def collect_eastmoney_board_flow(
    *,
    as_of: datetime,
    fetch_page: Callable[..., Any] | None = None,
    board_type: str = "all",
    page_size: int = _EASTMONEY_DEFAULT_PAGE_SIZE,
    expected_trade_date: date | None = None,
    max_pages: int = 100,
) -> dict[str, Any]:
    """Collect a complete current-day Eastmoney board-flow ranking."""

    cutoff, trade_day = _cutoff_and_trade_date(as_of, expected_trade_date)
    try:
        result = _collect_paginated(
            fetch_page or _default_eastmoney_page_fetch,
            request_kind="flow",
            request_value=board_type,
            as_of=cutoff,
            expected_trade_date=trade_day,
            page_size=page_size,
            max_pages=max_pages,
            identity_key=_board_identity,
            normalize=_normalize_board_flow_record,
            require_non_empty=True,
        )
    except RotationThemeDataError as exc:
        return _unavailable_source_snapshot(
            cutoff,
            trade_day,
            reason_code=exc.reason_code,
            source_id=EASTMONEY_BOARD_SOURCE_ID,
            dataset="BOARD_FLOW",
        )
    for rank, row in enumerate(result.get("records", ()), start=1):
        if isinstance(row, dict) and row.get("provider_rank") is None:
            row["provider_rank"] = rank
    return {
        **result,
        "dataset": "BOARD_FLOW",
        "source_id": EASTMONEY_BOARD_SOURCE_ID,
        "source_url": EASTMONEY_BOARD_URL,
        "trade_date": trade_day.isoformat(),
        "captured_at": result["captured_at"],
    }


def collect_eastmoney_board_members(
    *,
    as_of: datetime,
    board_code: str,
    fetch_page: Callable[..., Any] | None = None,
    page_size: int = _EASTMONEY_DEFAULT_PAGE_SIZE,
    expected_trade_date: date | None = None,
    max_pages: int = 100,
    membership_snapshot_dir: str | Path | None = None,
    effective_from: date | None = None,
    source: str = EASTMONEY_BOARD_SOURCE_ID,
) -> dict[str, Any]:
    """Collect and validate every constituent of one Eastmoney board.

    If ``membership_snapshot_dir`` is supplied, the returned complete member
    set is also persisted as an immutable, hash-addressed version.  The
    caller can continue using the last successful version when a later update
    fails, subject to the seven/fourteen day freshness policy.
    """

    safe_code = str(board_code or "").strip().upper()
    if not _EASTMONEY_BOARD_CODE.fullmatch(safe_code):
        return _unavailable_source_snapshot(
            _aware(as_of),
            (expected_trade_date or _aware(as_of).date()),
            reason_code="EASTMONEY_BOARD_CODE_INVALID",
            source_id=EASTMONEY_BOARD_SOURCE_ID,
            dataset="BOARD_MEMBERS",
            extra={"board_code": safe_code},
        )
    cutoff, trade_day = _cutoff_and_trade_date(as_of, expected_trade_date)
    try:
        result = _collect_paginated(
            fetch_page or _default_eastmoney_page_fetch,
            request_kind="members",
            request_value=safe_code,
            as_of=cutoff,
            expected_trade_date=trade_day,
            page_size=page_size,
            max_pages=max_pages,
            identity_key=_member_identity,
            normalize=_normalize_member_record,
            require_non_empty=True,
        )
    except RotationThemeDataError as exc:
        return _unavailable_source_snapshot(
            cutoff,
            trade_day,
            reason_code=exc.reason_code,
            source_id=EASTMONEY_BOARD_SOURCE_ID,
            dataset="BOARD_MEMBERS",
            extra={"board_code": safe_code},
        )
    result = {
        **result,
        "dataset": "BOARD_MEMBERS",
        "source_id": EASTMONEY_BOARD_SOURCE_ID,
        "source_url": EASTMONEY_BOARD_URL,
        "board_code": safe_code,
        "trade_date": trade_day.isoformat(),
        "captured_at": result["captured_at"],
        "member_snapshot_complete": True,
    }
    if membership_snapshot_dir is not None:
        effective = effective_from or trade_day
        membership = build_membership_snapshot(
            theme_id=safe_code,
            members=result["records"],
            captured_at=_parse_datetime(result["captured_at"], "MEMBERSHIP_CAPTURE_TIME_INVALID"),
            effective_from=effective,
            source=source,
            pagination_evidence=result["pagination_evidence"],
            expected_trade_date=trade_day,
        )
        result["membership_snapshot"] = membership
        result["membership_snapshot_path"] = str(
            write_membership_snapshot(membership_snapshot_dir, membership)
        )
    return result


# Descriptive alias used by integration callers.
collect_eastmoney_constituents = collect_eastmoney_board_members


class EastmoneyRotationCollector:
    """Injectable façade for the three Eastmoney collection operations."""

    def __init__(self, fetch_page: Callable[..., Any] | None = None, *, page_size: int = 100, max_pages: int = 100):
        if isinstance(page_size, bool) or int(page_size) < 1:
            raise ValueError("page_size must be positive")
        if isinstance(max_pages, bool) or int(max_pages) < 1:
            raise ValueError("max_pages must be positive")
        self.fetch_page = fetch_page
        self.page_size = int(page_size)
        self.max_pages = int(max_pages)

    def collect_catalog(self, *, as_of: datetime, board_type: str = "all", expected_trade_date: date | None = None) -> dict[str, Any]:
        return collect_eastmoney_board_catalog(
            as_of=as_of,
            fetch_page=self.fetch_page,
            board_type=board_type,
            page_size=self.page_size,
            max_pages=self.max_pages,
            expected_trade_date=expected_trade_date,
        )

    def collect_flow(self, *, as_of: datetime, board_type: str = "all", expected_trade_date: date | None = None) -> dict[str, Any]:
        return collect_eastmoney_board_flow(
            as_of=as_of,
            fetch_page=self.fetch_page,
            board_type=board_type,
            page_size=self.page_size,
            max_pages=self.max_pages,
            expected_trade_date=expected_trade_date,
        )

    def collect_members(self, *, as_of: datetime, board_code: str, expected_trade_date: date | None = None, membership_snapshot_dir: str | Path | None = None, effective_from: date | None = None) -> dict[str, Any]:
        return collect_eastmoney_board_members(
            as_of=as_of,
            board_code=board_code,
            fetch_page=self.fetch_page,
            page_size=self.page_size,
            max_pages=self.max_pages,
            expected_trade_date=expected_trade_date,
            membership_snapshot_dir=membership_snapshot_dir,
            effective_from=effective_from,
        )


def _collect_paginated(
    fetcher: Callable[..., Any],
    *,
    request_kind: str,
    request_value: str,
    as_of: datetime,
    expected_trade_date: date,
    page_size: int,
    max_pages: int,
    identity_key: Callable[[Mapping[str, Any]], str | None],
    normalize: Callable[[Mapping[str, Any]], dict[str, Any]],
    require_non_empty: bool,
) -> dict[str, Any]:
    if isinstance(page_size, bool) or int(page_size) < 1:
        raise RotationThemeDataError("EASTMONEY_PAGE_SIZE_INVALID")
    if isinstance(max_pages, bool) or int(max_pages) < 1:
        raise RotationThemeDataError("EASTMONEY_PAGE_BOUND_INVALID")
    rows: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    provider_total: int | None = None
    captured: datetime | None = None
    page = 1
    while page <= int(max_pages) and (provider_total is None or len(rows) < provider_total):
        raw = _invoke_page_fetcher(fetcher, request_kind, request_value, page, int(page_size))
        records, total, provider_capture, provider_trade = _extract_page(raw)
        if total is None or total < 0:
            raise RotationThemeDataError("EASTMONEY_PAGINATION_TOTAL_MISSING")
        if provider_total is None:
            provider_total = total
        elif provider_total != total:
            raise RotationThemeDataError("EASTMONEY_PAGINATION_TOTAL_CHANGED")
        _validate_provider_time(provider_capture, provider_trade, as_of, expected_trade_date)
        if provider_capture is not None:
            if captured is not None and provider_capture != captured:
                # The provider may carry different millisecond stamps on two
                # pages, but their date/time must still be same-day.  Keep the
                # latest bounded capture for the envelope.
                if provider_capture.date() != captured.date():
                    raise RotationThemeDataError("EASTMONEY_CAPTURE_TIME_CHANGED")
            captured = max(captured, provider_capture) if captured else provider_capture
        if not records:
            if len(rows) < provider_total:
                raise RotationThemeDataError("EASTMONEY_PAGINATION_INCOMPLETE")
            break
        normalized_page: list[dict[str, Any]] = []
        for record in records:
            if not isinstance(record, Mapping):
                raise RotationThemeDataError("EASTMONEY_PAGE_ROW_INVALID")
            try:
                item = normalize(record)
            except RotationThemeDataError:
                raise
            except Exception as exc:
                raise RotationThemeDataError("EASTMONEY_PAGE_ROW_INVALID") from exc
            if identity_key(item) is None:
                raise RotationThemeDataError("EASTMONEY_PAGE_ROW_IDENTITY_INVALID")
            normalized_page.append(item)
        pages.append({"page": page, "requested": int(page_size), "returned": len(normalized_page)})
        rows.extend(normalized_page)
        page += 1
    if provider_total is None:
        raise RotationThemeDataError("EASTMONEY_PAGINATION_TOTAL_MISSING")
    if page > int(max_pages) and len(rows) < provider_total:
        raise RotationThemeDataError("EASTMONEY_PAGINATION_PAGE_BOUND_EXCEEDED")
    if len(rows) != provider_total:
        raise RotationThemeDataError("EASTMONEY_PAGINATION_INCOMPLETE")
    identities = [identity_key(item) for item in rows]
    if len(set(identities)) != len(identities):
        raise RotationThemeDataError("EASTMONEY_PAGE_ROWS_DUPLICATED")
    if require_non_empty and not rows:
        raise RotationThemeDataError("EASTMONEY_ROWS_EMPTY")
    capture = captured or as_of
    # A source timestamp is mandatory unless the injected page has a valid
    # same-day captured_at/trade_date.  ``as_of`` is not silently relabelled
    # as provider time; it is only an upper-bound for a provider timestamp.
    if captured is None:
        raise RotationThemeDataError("EASTMONEY_CAPTURE_TIME_MISSING")
    return {
        "schema_version": "eastmoney-rotation-source/1.0.0",
        "available": True,
        "reason_code": "OK",
        "records": rows,
        "provider_total": provider_total,
        "record_count": len(rows),
        "captured_at": capture.isoformat(),
        "pagination_evidence": {
            "total": provider_total,
            "page_size": int(page_size),
            "pages": pages,
            "complete": True,
        },
    }


def _invoke_page_fetcher(fetcher: Callable[..., Any], kind: str, value: str, page: int, page_size: int) -> Any:
    """Call common injected fetcher shapes without hiding provider errors."""

    # The documented shape is (kind, value, page, page_size).  Keyword-only
    # and legacy three-argument test doubles are accepted as a convenience.
    try:
        return fetcher(kind, value, page, page_size)
    except TypeError as first:
        try:
            return fetcher(kind=kind, board_code=value, page=page, page_size=page_size)
        except TypeError:
            try:
                return fetcher(kind, page, page_size)
            except TypeError:
                try:
                    return fetcher(value, page, page_size)
                except TypeError:
                    raise first


def _extract_page(payload: Any) -> tuple[list[Mapping[str, Any]], int | None, datetime | None, date | None]:
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        # A bare list cannot prove the complete total or capture timestamp and
        # is therefore rejected by the strict collector.
        return [item for item in payload if isinstance(item, Mapping)], None, None, None
    if not isinstance(payload, Mapping):
        raise RotationThemeDataError("EASTMONEY_RESPONSE_INVALID")
    root = payload
    # Public endpoints commonly wrap the same page under data/result.
    for _ in range(3):
        nested = root.get("data")
        if isinstance(nested, Mapping) and not any(key in root for key in ("rows", "records", "diff", "items", "dataList")):
            root = nested
            continue
        break
    records: Any = None
    for key in ("rows", "records", "diff", "items", "dataList"):
        if key in root:
            records = root.get(key)
            break
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
        raise RotationThemeDataError("EASTMONEY_RESPONSE_ROWS_MISSING")
    total = _integer(root.get("total"))
    if total is None:
        total = _integer(root.get("total_count"))
    if total is None:
        total = _integer(root.get("count"))
    captured = _parse_datetime_optional(
        _first(root, "captured_at", "capture_time", "provider_timestamp", "timestamp", "f124", "__liangjian_captured_at")
    )
    trade = _parse_date_optional(_first(root, "trade_date", "trading_date", "date"))
    if trade is None and captured is not None:
        trade = captured.date()
    # Some payloads carry page metadata one level above data; read it too.
    if total is None:
        total = _integer(payload.get("total"))
    if captured is None:
        captured = _parse_datetime_optional(
            _first(payload, "captured_at", "capture_time", "provider_timestamp", "timestamp", "f124", "__liangjian_captured_at")
        )
    if trade is None:
        trade = _parse_date_optional(_first(payload, "trade_date", "trading_date", "date"))
    return [item for item in records if isinstance(item, Mapping)], total, captured, trade


def _normalize_board_record(row: Mapping[str, Any]) -> dict[str, Any]:
    code = _board_identity(row)
    name = str(_first(row, "board_name", "name", "f14", "板块名称") or "").strip()
    if not code or not name:
        raise RotationThemeDataError("EASTMONEY_BOARD_ROW_INVALID")
    return {"board_code": code, "board_name": name}


def _normalize_board_flow_record(row: Mapping[str, Any]) -> dict[str, Any]:
    result = _normalize_board_record(row)
    result.update(
        {
            "relative_return_pct": _number(_first(row, "relative_return_pct", "change_pct", "涨跌幅", "f3")),
            "eastmoney_main_net_inflow_cny": _number(
                _first(row, "eastmoney_main_net_inflow_cny", "main_net_inflow_cny", "main_net", "main_net_cny", "主力净流入", "f62")
            ),
            "main_net_inflow_ratio_pct": _number(_first(row, "main_pct", "main_net_ratio", "主力净占比", "f184")),
            "leader": str(_first(row, "leader", "领涨股", "f204") or "").strip(),
            "provider_rank": _integer(_first(row, "rank", "排名")),
        }
    )
    return result


def _normalize_member_record(row: Mapping[str, Any]) -> dict[str, Any]:
    raw_symbol = _first(row, "symbol", "code", "stock_code", "SECURITY_CODE", "f12")
    symbol = _normalize_symbol(raw_symbol)
    name = str(_first(row, "name", "stock_name", "SECURITY_SHORT_NAME", "f14") or "").strip()
    if symbol is None:
        raise RotationThemeDataError("EASTMONEY_MEMBER_ROW_INVALID")
    return {
        "symbol": symbol,
        "name": name,
        "latest_price": _number(_first(row, "latest_price", "price", "NEWEST_PRICE", "f2")),
        "change_pct": _number(_first(row, "change_pct", "涨跌幅", "CHG", "f3")),
        "turnover_cny": _number(_first(row, "turnover_cny", "amount_cny", "amount", "成交额", "f6")),
        "volume": _number(_first(row, "volume", "trading_volume", "成交量", "f5")),
    }


def _board_identity(row: Mapping[str, Any]) -> str | None:
    value = _first(row, "board_code", "code", "index_code", "f12", "板块代码")
    text = str(value or "").strip().upper()
    return text if _EASTMONEY_BOARD_CODE.fullmatch(text) else None


def _member_identity(row: Mapping[str, Any]) -> str | None:
    value = row.get("symbol") if isinstance(row, Mapping) else None
    return _normalize_symbol(value)


def _default_eastmoney_page_fetch(kind: str, value: str, page: int, page_size: int) -> dict[str, Any]:
    """Small public Eastmoney page fetcher; integration may inject its own."""

    import requests

    if kind == "members":
        fs = f"b:{value}"
    elif kind == "catalog":
        fs = "m:90+t:2,m:90+t:3"
    else:
        fs = "m:90+t:2,m:90+t:3"
    response = requests.get(
        EASTMONEY_BOARD_URL,
        params={
            "pn": str(page),
            "pz": str(page_size),
            "np": "1",
            "po": "1",
            "fltt": "2",
            "invt": "2",
            "fs": fs,
            "fields": "f12,f14,f2,f3,f5,f6,f124,f62,f184",
        },
        headers={"User-Agent": "Mozilla/5.0 (compatible; LiangjianResearch/2.0)", "Referer": "https://data.eastmoney.com/"},
        timeout=(5, 20),
    )
    response.raise_for_status()
    payload = response.json()
    # Member pages commonly omit a provider timestamp.  The local HTTP
    # receive time is an honest capture boundary for this slow-changing
    # dimension; it is not used to relabel a historical page.
    if isinstance(payload, Mapping):
        wrapped = dict(payload)
        wrapped["__liangjian_captured_at"] = _aware(datetime.now(SHANGHAI)).isoformat()
        return wrapped
    return payload


# ---------------------------------------------------------------------------
# Tencent stock flow normalization and theme aggregation
# ---------------------------------------------------------------------------


TENCENT_FLOW_SOURCE_ID = "TENCENT_QQ_FINANCE_FUND_FLOW"
TENCENT_FLOW_SCHEMA = "liangjian-tencent-flow/1.0.0"


def collect_tencent_capital_flow(
    *,
    as_of: datetime,
    expected_symbols: Sequence[str],
    fetch_symbol: Callable[[str], Any] | None = None,
    capture_timestamp: datetime | str | None = None,
    fetch_capture_timestamp: Callable[[], Any] | None = None,
    quote_fetch: Callable[..., Any] | None = None,
    quotes: Mapping[str, Any] | Sequence[Any] | None = None,
    expected_trade_date: date | None = None,
    workers: int = 16,
) -> dict[str, Any]:
    """Collect same-day Tencent stock flows with explicit date proof.

    Tencent's fund-flow response often has no trade-date field.  In that
    case, a fixed capture timestamp *and* at least one same-day Tencent quote
    proof are mandatory.  The function never assigns ``as_of.date()`` to an
    undated historical response by default.
    """

    cutoff, trade_day = _cutoff_and_trade_date(as_of, expected_trade_date)
    if isinstance(workers, bool) or not 1 <= int(workers) <= 32:
        return _unavailable_source_snapshot(
            cutoff,
            trade_day,
            reason_code="TENCENT_FLOW_WORKERS_INVALID",
            source_id=TENCENT_FLOW_SOURCE_ID,
            dataset="TENCENT_FLOW",
        )
    symbols = tuple(dict.fromkeys(symbol for value in expected_symbols if (symbol := _normalize_symbol(value))))
    if not symbols:
        return _unavailable_source_snapshot(
            cutoff,
            trade_day,
            reason_code="TENCENT_FLOW_SYMBOLS_EMPTY",
            source_id=TENCENT_FLOW_SOURCE_ID,
            dataset="TENCENT_FLOW",
        )
    capture = _parse_datetime_optional(capture_timestamp)
    if capture is None and fetch_capture_timestamp is not None:
        try:
            capture = _parse_datetime_optional(fetch_capture_timestamp())
        except Exception:
            capture = None
    if capture is None:
        # An individual row timestamp is acceptable only when every row has
        # the same timestamp/date; otherwise there is no fixed capture point.
        capture = None
    capture_bound = _live_collection_upper_bound(cutoff, trade_day)
    if capture is not None and (capture.date() != trade_day or capture > capture_bound):
        return _unavailable_source_snapshot(
            cutoff,
            trade_day,
            reason_code="TENCENT_CAPTURE_TIME_INVALID",
            source_id=TENCENT_FLOW_SOURCE_ID,
            dataset="TENCENT_FLOW",
        )
    fetch = fetch_symbol or _default_tencent_flow_fetch
    rows: list[dict[str, Any]] = []
    failed: list[str] = []
    raw_by_symbol: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=int(workers), thread_name_prefix="rotation-tencent-flow") as executor:
        futures = {executor.submit(fetch, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                raw_by_symbol[symbol] = future.result()
            except Exception:
                failed.append(symbol)
    for symbol in symbols:
        if symbol not in raw_by_symbol:
            continue
        try:
            raw = raw_by_symbol[symbol]
            normalized = _normalize_tencent_flow_record(raw, symbol)
            row_capture = _parse_datetime_optional(_first(raw if isinstance(raw, Mapping) else {}, "captured_at", "capture_time", "provider_timestamp", "timestamp", "f124"))
            if row_capture is not None:
                if capture is None:
                    capture = row_capture
                elif row_capture != capture:
                    raise RotationThemeDataError("TENCENT_CAPTURE_TIME_NOT_FIXED")
            row_trade = _parse_date_optional(_first(raw if isinstance(raw, Mapping) else {}, "trade_date", "trading_date", "date"))
            if row_trade is not None and row_trade != trade_day:
                raise RotationThemeDataError("TENCENT_TRADE_DATE_MISMATCH")
            normalized["symbol"] = symbol
            rows.append(normalized)
        except RotationThemeDataError:
            failed.append(symbol)
        except Exception:
            failed.append(symbol)
    # If no top-level date was supplied, derive a fixed timestamp only from
    # the rows when every row proved the same timestamp.  The quote proof below
    # is still mandatory for undated rows.
    undated_rows = [row for row in rows if not row.get("trade_date")]
    if capture is None:
        # Individual response timestamps may have been normalized into this
        # marker by _normalize_tencent_flow_record.
        row_times = {row.get("captured_at") for row in rows if row.get("captured_at")}
        if len(row_times) == 1:
            capture = _parse_datetime_optional(next(iter(row_times)))
    if capture is None:
        return _unavailable_source_snapshot(
            cutoff,
            trade_day,
            reason_code="TENCENT_CAPTURE_TIME_MISSING",
            source_id=TENCENT_FLOW_SOURCE_ID,
            dataset="TENCENT_FLOW",
            extra={"requested_symbol_count": len(symbols), "failed_symbols": failed},
        )
    quote_validated = True
    quote_records: dict[str, dict[str, Any]] = {}
    if undated_rows:
        quote_records = _same_day_quote_records(
            quote_fetch=quote_fetch,
            quotes=quotes,
            symbols=symbols,
            expected_trade_date=trade_day,
            as_of=cutoff,
            workers=int(workers),
        )
        quote_validated = bool(quote_records)
        if not quote_validated:
            return _unavailable_source_snapshot(
                cutoff,
                trade_day,
                reason_code="TENCENT_SAME_DAY_QUOTE_PROOF_MISSING",
                source_id=TENCENT_FLOW_SOURCE_ID,
                dataset="TENCENT_FLOW",
                extra={"requested_symbol_count": len(symbols), "failed_symbols": failed},
            )
        for row in rows:
            quote = quote_records.get(row["symbol"])
            if quote:
                for key in ("latest_price", "change_pct", "turnover_cny"):
                    if row.get(key) is None and quote.get(key) is not None:
                        row[key] = quote[key]
    return {
        "schema_version": TENCENT_FLOW_SCHEMA,
        "source_id": TENCENT_FLOW_SOURCE_ID,
        "available": bool(rows),
        "reason_code": "OK" if rows else "TENCENT_FLOW_ROWS_EMPTY",
        "trade_date": trade_day.isoformat(),
        "captured_at": capture.isoformat(),
        "records": rows,
        "requested_symbol_count": len(symbols),
        "returned_symbol_count": len(rows),
        "failed_symbol_count": len(failed),
        "failed_symbols": sorted(failed),
        "coverage": len(rows) / len(symbols),
        "same_day_quote_validated": quote_validated,
        "quote_records": quote_records,
        "point_in_time": True,
        "content_hash": _content_hash({"records": rows, "trade_date": trade_day.isoformat(), "captured_at": capture.isoformat()}),
    }


def normalize_tencent_flow_records(payload: Any, *, expected_symbols: Sequence[str] | None = None) -> list[dict[str, Any]]:
    """Normalize a sequence/mapping of Tencent rows without date inference."""

    if isinstance(payload, Mapping):
        raw = payload.get("records") or payload.get("rows") or payload.get("items")
        if raw is None:
            raw = [payload]
    else:
        raw = payload
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise RotationThemeDataError("TENCENT_FLOW_ROWS_INVALID")
    allowed = {_normalize_symbol(value) for value in (expected_symbols or ())}
    result = []
    for row in raw:
        if not isinstance(row, Mapping):
            raise RotationThemeDataError("TENCENT_FLOW_ROW_INVALID")
        symbol = _normalize_symbol(_first(row, "symbol", "code", "stock_code", "stockCode"))
        if symbol is None:
            raise RotationThemeDataError("TENCENT_FLOW_SYMBOL_INVALID")
        if allowed and symbol not in allowed:
            continue
        result.append(_normalize_tencent_flow_record(row, symbol))
    if len({item["symbol"] for item in result}) != len(result):
        raise RotationThemeDataError("TENCENT_FLOW_SYMBOL_DUPLICATED")
    return result


def aggregate_tencent_theme_flows(
    flows: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    memberships: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    expected_trade_date: date | None = None,
) -> dict[str, dict[str, Any]]:
    """Aggregate Tencent flow with an auditable fund-coverage fallback.

    ``turnover_coverage`` is retained only when the normalized A-share member
    set has a complete, positive turnover denominator.  Otherwise
    ``effective_fund_coverage`` uses the ratio of members with a Tencent main
    net-flow value and marks the result as degraded with
    ``coverage_basis=member_count``.
    """

    records = normalize_tencent_flow_records(flows)
    by_symbol = {row["symbol"]: row for row in records}
    if isinstance(memberships, Mapping):
        groups = memberships.items()
    elif isinstance(memberships, Sequence) and not isinstance(memberships, (str, bytes, bytearray)):
        groups = ((str(item.get("theme_id") or ""), item) for item in memberships if isinstance(item, Mapping))
    else:
        raise RotationThemeDataError("ROTATION_THEME_MEMBERSHIPS_INVALID")
    result: dict[str, dict[str, Any]] = {}
    for theme_id, raw_group in groups:
        symbols, total_turnover, excluded_symbols = _membership_symbols_and_turnover_with_exclusions(raw_group)
        if not symbols:
            result[str(theme_id)] = {
                "tencent_main_net_inflow_cny": 0.0,
                "covered_member_count": 0,
                "member_count": 0,
                # There is no validated denominator for an empty group.  Do
                # not manufacture a turnover coverage value; the member-count
                # basis remains visible and fails the selection gate.
                "turnover_coverage": None,
                "flow_coverage": 0.0,
                "effective_fund_coverage": 0.0,
                "coverage_basis": "member_count",
                "coverage_degraded": True,
                "coverage_degraded_reason": "TENCENT_TURNOVER_DENOMINATOR_UNAVAILABLE",
                "degraded": True,
                "degraded_reason": "TENCENT_TURNOVER_DENOMINATOR_UNAVAILABLE",
                "excluded_non_a_share_count": len(excluded_symbols),
                "excluded_non_a_share_symbols": excluded_symbols,
            }
            continue
        covered = [by_symbol[symbol] for symbol in symbols if symbol in by_symbol]
        covered_turnover = sum(_turnover(row) or 0.0 for row in covered)
        total_turnover_value = total_turnover
        turnover_coverage = (
            covered_turnover / total_turnover_value
            if total_turnover_value is not None and total_turnover_value > 0
            else None
        )
        flow_coverage = len(covered) / len(symbols)
        coverage_basis = "turnover" if turnover_coverage is not None else "member_count"
        result[str(theme_id)] = {
            "tencent_main_net_inflow_cny": sum(_main_flow(row) or 0.0 for row in covered),
            "covered_member_count": len(covered),
            "member_count": len(symbols),
            "flow_coverage": flow_coverage,
            "turnover_coverage": turnover_coverage,
            # The effective gate is turnover-based only when every member has
            # a same-day, positive turnover denominator.  Otherwise it is a
            # deliberately degraded member-count gate over Tencent flow rows.
            "effective_fund_coverage": turnover_coverage if turnover_coverage is not None else flow_coverage,
            "coverage_basis": coverage_basis,
            "coverage_degraded": coverage_basis != "turnover",
            "coverage_degraded_reason": (
                "TENCENT_TURNOVER_DENOMINATOR_UNAVAILABLE"
                if coverage_basis != "turnover"
                else None
            ),
            "degraded": coverage_basis != "turnover",
            "degraded_reason": (
                "TENCENT_TURNOVER_DENOMINATOR_UNAVAILABLE"
                if coverage_basis != "turnover"
                else None
            ),
            "covered_turnover_cny": covered_turnover,
            "total_turnover_cny": total_turnover_value,
            "excluded_non_a_share_count": len(excluded_symbols),
            "excluded_non_a_share_symbols": excluded_symbols,
            "trade_date": expected_trade_date.isoformat() if expected_trade_date else None,
        }
    return result


def _normalize_tencent_flow_record(row: Any, symbol: str | None = None) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise RotationThemeDataError("TENCENT_FLOW_ROW_INVALID")
    safe_symbol = _normalize_symbol(symbol or _first(row, "symbol", "code", "stock_code", "stockCode"))
    if safe_symbol is None:
        raise RotationThemeDataError("TENCENT_FLOW_SYMBOL_INVALID")
    main = _number(_first(row, "tencent_main_net_inflow_cny", "main_net_inflow_cny", "net_inflow_amount", "main_net", "mainNetIn", "main_net_cny", "主力净流入"))
    if main is None:
        raise RotationThemeDataError("TENCENT_FLOW_MAIN_NET_MISSING")
    capture = _parse_datetime_optional(_first(row, "captured_at", "capture_time", "provider_timestamp", "timestamp", "f124"))
    trade = _parse_date_optional(_first(row, "trade_date", "trading_date", "date"))
    return {
        "symbol": safe_symbol,
        "name": str(_first(row, "name", "stock_name", "stockName") or "").strip(),
        "tencent_main_net_inflow_cny": main,
        "turnover_cny": _number(_first(row, "turnover_cny", "amount_cny", "amount", "成交额", "tradeAmount", "turnover")),
        # These are optional same-day quote fields.  They are deliberately
        # carried through the Tencent record so the daily breadth and price
        # coverage calculations never read quote values from an old member
        # snapshot.
        "latest_price": _number(_first(row, "latest_price", "price", "newest_price", "NEWEST_PRICE", "current_price")),
        "change_pct": _number(_first(row, "change_pct", "涨跌幅", "chg", "CHG", "change")),
        "main_net_inflow_ratio_pct": _number(_first(row, "main_net_inflow_ratio_pct", "net_inflow_ratio", "main_net_ratio")),
        "captured_at": capture.isoformat() if capture else None,
        "trade_date": trade.isoformat() if trade else None,
    }


def _default_tencent_flow_fetch(symbol: str) -> dict[str, Any]:
    # Keep the actual transport lazy; the a2_market collector remains the
    # richer production adapter when the workflow integrates this module.
    from .a2_market import _tencent_symbol_flow_fetcher

    return _tencent_symbol_flow_fetcher(symbol)


def _default_tencent_capture_timestamp_fetcher() -> Any:
    """Use Tencent's index quote timestamp as the fixed flow capture point."""

    from .a2_market import _tencent_trade_timestamp_fetcher

    return _tencent_trade_timestamp_fetcher()


def _default_tencent_quote_fetch(symbol: str) -> dict[str, Any]:
    """Fetch one same-day quote for coverage/breadth validation."""

    from .tencent_minute import TencentIntradayAdapter

    result = TencentIntradayAdapter().fetch_quote(symbol, as_of=datetime.now(SHANGHAI))
    quote = result.quote
    if quote is None:
        return {}
    return {
        "symbol": quote.symbol,
        "latest_price": quote.price,
        "change_pct": (quote.price / quote.previous_close - 1.0) * 100.0 if quote.previous_close else None,
        "turnover_cny": quote.amount,
        "quote_time": quote.quote_time,
        "trade_date": quote.quote_time.date(),
    }


def _validate_same_day_quotes(*, quote_fetch: Callable[..., Any] | None, quotes: Any, symbols: Sequence[str], expected_trade_date: date, as_of: datetime, workers: int = 1) -> bool:
    return bool(
        _same_day_quote_records(
            quote_fetch=quote_fetch,
            quotes=quotes,
            symbols=symbols,
            expected_trade_date=expected_trade_date,
            as_of=as_of,
            workers=workers,
        )
    )


def _same_day_quote_records(*, quote_fetch: Callable[..., Any] | None, quotes: Any, symbols: Sequence[str], expected_trade_date: date, as_of: datetime, workers: int = 1) -> dict[str, dict[str, Any]]:
    if quotes is None and quote_fetch is None:
        return {}
    quote_map: dict[str, Any] = {}
    if isinstance(quotes, Mapping):
        quote_map = {_normalize_symbol(key) or str(key): value for key, value in quotes.items()}
    elif isinstance(quotes, Sequence) and not isinstance(quotes, (str, bytes, bytearray)):
        for item in quotes:
            if isinstance(item, Mapping):
                symbol = _normalize_symbol(_first(item, "symbol", "code", "stock_code", "stockCode"))
                if symbol:
                    quote_map[symbol] = item
    # At least one quote must prove the market is on the target date.  When a
    # quote fetcher is supplied, collect every available quote so the caller
    # can calculate current-day price/breadth coverage without reading stale
    # values from a membership snapshot.
    if quote_fetch is not None:
        missing = [symbol for symbol in symbols if symbol not in quote_map]
        fetched: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=max(1, int(workers)), thread_name_prefix="rotation-tencent-quote") as executor:
            futures = {executor.submit(_invoke_quote_fetch, quote_fetch, symbol): symbol for symbol in missing}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    fetched[symbol] = future.result()
                except Exception:
                    continue
        quote_map.update(fetched)
    result: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        item = quote_map.get(symbol)
        if item is None:
            continue
        item_map = _object_to_mapping(item)
        stamp = _parse_datetime_optional(_first(item_map, "quote_time", "captured_at", "capture_time", "timestamp", "provider_timestamp", "f124"))
        trade = _parse_date_optional(_first(item_map, "trade_date", "trading_date", "date"))
        if stamp is not None and (
            stamp.date() != expected_trade_date
            or stamp > _live_collection_upper_bound(as_of, expected_trade_date)
        ):
            return {}
        if trade is not None and trade != expected_trade_date:
            return {}
        if stamp is not None or trade == expected_trade_date:
            item_map["symbol"] = symbol
            item_map["quote_time"] = stamp.isoformat() if stamp else None
            if trade is not None:
                item_map["trade_date"] = trade.isoformat()
            result[symbol] = item_map
    return result


def _invoke_quote_fetch(fetcher: Callable[..., Any], symbol: str) -> Any:
    try:
        return fetcher(symbol)
    except TypeError:
        return fetcher()


def _object_to_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        result = dict(value)
        if result.get("latest_price") is None and result.get("price") is not None:
            result["latest_price"] = result.get("price")
        if result.get("turnover_cny") is None and result.get("amount") is not None:
            result["turnover_cny"] = result.get("amount")
        if result.get("change_pct") is None and result.get("change") is not None:
            result["change_pct"] = result.get("change")
        return result
    result: dict[str, Any] = {}
    for attr, key in (
        ("symbol", "symbol"),
        ("price", "latest_price"),
        ("latest_price", "latest_price"),
        ("change_pct", "change_pct"),
        ("turnover_cny", "turnover_cny"),
        ("amount", "turnover_cny"),
        ("quote_time", "quote_time"),
        ("trade_date", "trade_date"),
    ):
        if hasattr(value, attr):
            result[key] = getattr(value, attr)
    return result


# ---------------------------------------------------------------------------
# Immutable membership snapshots and freshness policy
# ---------------------------------------------------------------------------


def build_membership_snapshot(
    *,
    theme_id: str,
    members: Sequence[Any],
    captured_at: datetime,
    effective_from: date,
    source: str,
    pagination_evidence: Mapping[str, Any],
    expected_trade_date: date | None = None,
) -> dict[str, Any]:
    """Build an immutable, hash-bound complete member snapshot."""

    safe_theme = str(theme_id or "").strip().upper()
    if not safe_theme or not _THEME_ID.fullmatch(safe_theme) and not _EASTMONEY_BOARD_CODE.fullmatch(safe_theme):
        raise RotationThemeDataError("MEMBERSHIP_THEME_ID_INVALID")
    stamp = _parse_datetime(captured_at, "MEMBERSHIP_CAPTURE_TIME_INVALID")
    effective = _parse_date(effective_from, "MEMBERSHIP_EFFECTIVE_FROM_INVALID")
    if effective > stamp.date():
        raise RotationThemeDataError("MEMBERSHIP_EFFECTIVE_FROM_FUTURE")
    if expected_trade_date is not None and stamp.date() != expected_trade_date:
        raise RotationThemeDataError("MEMBERSHIP_CAPTURE_TRADE_DATE_MISMATCH")
    if not str(source or "").strip():
        raise RotationThemeDataError("MEMBERSHIP_SOURCE_MISSING")
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes, bytearray)) or not members:
        raise RotationThemeDataError("MEMBERSHIP_MEMBERS_EMPTY")
    normalized: list[dict[str, Any]] = []
    for item in members:
        if isinstance(item, Mapping):
            row = _normalize_member_record(item)
        else:
            symbol = _normalize_symbol(item)
            if symbol is None:
                raise RotationThemeDataError("MEMBERSHIP_MEMBER_INVALID")
            row = {"symbol": symbol, "name": "", "latest_price": None, "change_pct": None, "turnover_cny": None, "volume": None}
        normalized.append(row)
    normalized.sort(key=lambda item: item["symbol"])
    symbols = [item["symbol"] for item in normalized]
    if len(set(symbols)) != len(symbols):
        raise RotationThemeDataError("MEMBERSHIP_MEMBER_DUPLICATED")
    evidence = _validate_pagination_evidence(pagination_evidence, len(normalized))
    excluded_symbols = sorted(
        symbol for symbol in symbols if _is_non_a_share_symbol(symbol)
    )
    normalized = [
        item for item in normalized
        if not _is_non_a_share_symbol(item["symbol"])
    ]
    body: dict[str, Any] = {
        "schema_version": MEMBERSHIP_SNAPSHOT_SCHEMA,
        "source_id": str(source).strip(),
        "theme_id": safe_theme,
        "available": True,
        "reason_code": "OK",
        "captured_at": stamp.isoformat(),
        "effective_from": effective.isoformat(),
        "records": normalized,
        "member_count": len(normalized),
        "excluded_non_a_share_count": len(excluded_symbols),
        "excluded_non_a_share_symbols": excluded_symbols,
        "pagination_evidence": evidence,
        "immutable": True,
        "point_in_time": True,
    }
    body["content_hash"] = _content_hash(body)
    return body


def write_membership_snapshot(snapshot_dir: str | Path, snapshot: Mapping[str, Any]) -> Path:
    """Atomically write a hash-addressed immutable membership version.

    A second write with the same content hash is a no-op.  A different hash
    gets a different filename and can never overwrite an earlier version.
    """

    if not isinstance(snapshot, Mapping) or snapshot.get("schema_version") != MEMBERSHIP_SNAPSHOT_SCHEMA:
        raise RotationThemeDataError("MEMBERSHIP_SNAPSHOT_INVALID")
    expected = str(snapshot.get("content_hash") or "")
    body = dict(snapshot)
    body.pop("content_hash", None)
    if not expected or expected != _content_hash(body):
        raise RotationThemeDataError("MEMBERSHIP_SNAPSHOT_HASH_INVALID")
    root = Path(snapshot_dir)
    root.mkdir(parents=True, exist_ok=True)
    theme = _safe_filename(str(snapshot.get("theme_id") or "theme"))
    stamp = _parse_datetime(snapshot.get("captured_at"), "MEMBERSHIP_CAPTURE_TIME_INVALID")
    path = root / f"membership-{theme}-{stamp.strftime('%Y%m%dT%H%M%S')}-{expected[:16]}.json"
    if path.is_file():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RotationThemeDataError("MEMBERSHIP_EXISTING_VERSION_INVALID") from exc
        if isinstance(existing, Mapping) and existing.get("content_hash") == expected:
            return path
        raise RotationThemeDataError("MEMBERSHIP_IMMUTABLE_OVERWRITE_FORBIDDEN")
    return atomic_write_json(path, dict(snapshot))


persist_membership_snapshot = write_membership_snapshot


def load_membership_snapshot(
    snapshot_dir: str | Path,
    theme_id: str,
    expected_trade_date: date,
    *,
    now: datetime | None = None,
    warn_after_days: int = 7,
    expire_after_days: int = 14,
    update_failed: bool = False,
) -> dict[str, Any]:
    """Load the latest successful version that existed on the target date.

    Future-captured or future-effective versions are ignored.  A seven-day
    stale warning is non-blocking; fourteen days is a hard fail-closed limit.
    ``update_failed`` records the reason for a permissible old-version
    fallback without mutating the snapshot itself.
    """

    target_date = _parse_date(expected_trade_date, "MEMBERSHIP_EXPECTED_TRADE_DATE_INVALID")
    if isinstance(warn_after_days, bool) or isinstance(expire_after_days, bool) or int(warn_after_days) < 0 or int(expire_after_days) < int(warn_after_days):
        raise RotationThemeDataError("MEMBERSHIP_FRESHNESS_POLICY_INVALID")
    safe_theme = str(theme_id or "").strip().upper()
    root = Path(snapshot_dir)
    candidates: list[tuple[date, datetime, Path, dict[str, Any]]] = []
    for path in sorted(root.glob(f"membership-{_safe_filename(safe_theme)}-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not _valid_membership_payload(payload, safe_theme):
            continue
        effective = _parse_date_optional(payload.get("effective_from"))
        stamp = _parse_datetime_optional(payload.get("captured_at"))
        if effective is None or stamp is None:
            continue
        # ``captured_at`` is a historical availability boundary: a version
        # fetched after the requested trade date did not exist then.
        if effective > target_date or stamp.date() > target_date:
            continue
        candidates.append((effective, stamp, path, dict(payload)))
    if not candidates:
        return unavailable_membership_snapshot(safe_theme, target_date, "MEMBERSHIP_SNAPSHOT_NOT_FOUND")
    candidates.sort(key=lambda item: (item[0], item[1], str(item[2])))
    effective, stamp, path, selected = candidates[-1]
    age_days = max(0, (target_date - stamp.date()).days)
    if age_days > int(expire_after_days):
        return unavailable_membership_snapshot(
            safe_theme,
            target_date,
            "MEMBERSHIP_SNAPSHOT_EXPIRED",
            extra={"path": str(path), "age_days": age_days, "effective_from": effective.isoformat()},
        )
    warning: str | None = None
    if age_days >= int(warn_after_days):
        warning = "MEMBERSHIP_SNAPSHOT_STALE_WARNING"
    if update_failed:
        warning = "MEMBERSHIP_UPDATE_FAILED_FALLBACK"
    result = {
        **selected,
        "path": str(path),
        "age_days": age_days,
        "warning": warning,
        "fallback_reused": bool(update_failed or warning),
    }
    return result


load_latest_membership_snapshot = load_membership_snapshot


def unavailable_membership_snapshot(theme_id: str, trade_date: date, reason_code: str, *, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schema_version": MEMBERSHIP_SNAPSHOT_SCHEMA,
        "source_id": None,
        "theme_id": str(theme_id or "").strip().upper(),
        "available": False,
        "reason_code": str(reason_code or "MEMBERSHIP_UNAVAILABLE"),
        "trade_date": _parse_date(trade_date, "MEMBERSHIP_EXPECTED_TRADE_DATE_INVALID").isoformat(),
        "records": [],
        "member_count": 0,
        "warning": None,
        **dict(extra or {}),
    }


def _validate_pagination_evidence(evidence: Mapping[str, Any], count: int) -> dict[str, Any]:
    if not isinstance(evidence, Mapping):
        raise RotationThemeDataError("MEMBERSHIP_PAGINATION_EVIDENCE_MISSING")
    total = _integer(evidence.get("total"))
    pages = evidence.get("pages")
    complete = evidence.get("complete")
    if total is None or total != count or complete is not True or not isinstance(pages, Sequence) or not pages:
        raise RotationThemeDataError("MEMBERSHIP_PAGINATION_EVIDENCE_INCOMPLETE")
    return {
        "total": total,
        "page_size": _integer(evidence.get("page_size")),
        "pages": [dict(item) for item in pages if isinstance(item, Mapping)],
        "complete": True,
    }


def _valid_membership_payload(payload: Any, theme_id: str) -> bool:
    if not isinstance(payload, Mapping):
        return False
    body = dict(payload)
    expected = str(body.pop("content_hash", ""))
    return bool(
        payload.get("schema_version") == MEMBERSHIP_SNAPSHOT_SCHEMA
        and str(payload.get("theme_id") or "").upper() == theme_id
        and payload.get("available") is True
        and payload.get("immutable") is True
        and isinstance(payload.get("records"), list)
        and expected
        and expected == _content_hash(body)
        and all(isinstance(item, Mapping) and _normalize_symbol(item.get("symbol")) for item in payload["records"])
    )


# ---------------------------------------------------------------------------
# Six-factor score and final rotation snapshot
# ---------------------------------------------------------------------------


def calculate_rotation_strength(
    board_rows: Sequence[Mapping[str, Any]],
    *,
    rotation_theme_count: int = ROTATION_THEME_TOP_N,
    config: RotationThemeConfig | Mapping[str, Any] | None = None,
    as_of: datetime | None = None,
    expected_trade_date: date | None = None,
    fund_coverage_minimum: float = 0.8,
    price_coverage_minimum: float = 0.9,
) -> dict[str, Any]:
    """Calculate transparent strength and hard-gated TOP-N selection.

    ``board_rows`` accepts normalized Eastmoney flow rows plus the six factor
    inputs and coverage fields.  It is kept separate from snapshot assembly
    so a shadow replay can inspect scores without writing files.
    """

    if not isinstance(board_rows, Sequence) or isinstance(board_rows, (str, bytes, bytearray)):
        raise RotationThemeDataError("ROTATION_THEME_BOARD_ROWS_INVALID")
    try:
        limit = int(rotation_theme_count)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RotationThemeDataError("ROTATION_THEME_TOP_N_INVALID") from exc
    if isinstance(rotation_theme_count, bool) or limit < 1:
        raise RotationThemeDataError("ROTATION_THEME_TOP_N_INVALID")
    fund_minimum = _validate_coverage_threshold(fund_coverage_minimum, "ROTATION_THEME_FUND_COVERAGE_THRESHOLD_INVALID")
    price_minimum = _validate_coverage_threshold(price_coverage_minimum, "ROTATION_THEME_PRICE_COVERAGE_THRESHOLD_INVALID")
    trade_day = expected_trade_date or (_aware(as_of).date() if as_of is not None else None)
    taxonomy = validate_rotation_theme_config(config) if config is not None else None
    normalized: list[dict[str, Any]] = []
    for raw in board_rows:
        if not isinstance(raw, Mapping):
            raise RotationThemeDataError("ROTATION_THEME_BOARD_ROW_INVALID")
        row = _normalize_metric_row(raw, taxonomy, trade_day)
        normalized.append(row)
    if not normalized:
        return {"boards": [], "selected_primary_boards": [], "reason_code": "ROTATION_THEME_ROWS_EMPTY"}
    primary = [row for row in normalized if row["kind"] == PRIMARY]
    if taxonomy is None:
        # A direct test/replay row may not include the repository taxonomy.  It
        # must still explicitly identify its level; unknown parents are not
        # silently promoted.
        primary = [row for row in normalized if row["kind"] == PRIMARY]
    factor_percentiles = _factor_percentiles(primary)
    for row in normalized:
        values = factor_percentiles.get(row["theme_id"], _percentile_for_child(row, factor_percentiles, primary))
        factors = {
            key: (round(float(values[key]) * 100.0, 4) if values.get(key) is not None else None)
            for key in STRENGTH_WEIGHTS
        }
        available_factors = [key for key in STRENGTH_WEIGHTS if factors[key] is not None]
        weight_total = sum(STRENGTH_WEIGHTS[key] for key in available_factors)
        score = (
            sum(float(factors[key]) * STRENGTH_WEIGHTS[key] / 100.0 for key in available_factors)
            / weight_total
            * 100.0
            if weight_total
            else None
        )
        row["strength_factors"] = factors
        row["strength"] = round(score, 4) if score is not None else None
        row["available_factors"] = available_factors
        row["missing_factors"] = [key for key in STRENGTH_WEIGHTS if key not in available_factors]
        row["factor_coverage"] = len(available_factors) / len(STRENGTH_WEIGHTS)
    by_id = {row["theme_id"]: row for row in normalized}
    eligible = []
    for row in normalized:
        row["selection_status"] = _selection_status(
            row,
            trade_day,
            fund_coverage_minimum=fund_minimum,
            price_coverage_minimum=price_minimum,
        )
        if row["kind"] == PRIMARY and row["selection_status"] == "ELIGIBLE_PRIMARY":
            eligible.append(row)
    eligible.sort(key=lambda item: (-float(item["strength"] if item["strength"] is not None else -1), -float(item.get("tencent_main_net_inflow_cny") or 0), item["theme_id"]))
    selected = eligible[:limit]
    # Child directions normally inherit a selected parent's rank so the same
    # economic chain does not consume two slots. If the parent is not
    # eligible but a child has independently positive flow, complete
    # membership and sufficient price/factor coverage, that child may fill an
    # otherwise empty TOP5 slot. A weak parent must not hide a strong child.
    selected_primary_ids = {row["theme_id"] for row in selected}
    standalone_children = [
        row
        for row in normalized
        if row["kind"] == CHILD
        and row["selection_status"] == "ELIGIBLE_PRIMARY"
        and row.get("parent_theme_id") not in selected_primary_ids
    ]
    standalone_children.sort(
        key=lambda item: (
            -float(item["strength"] if item["strength"] is not None else -1),
            -float(item.get("tencent_main_net_inflow_cny") or 0),
            item["theme_id"],
        )
    )
    selected.extend(standalone_children[: max(0, limit - len(selected))])
    selected.sort(
        key=lambda item: (
            -float(item["strength"] if item["strength"] is not None else -1),
            -float(item.get("tencent_main_net_inflow_cny") or 0),
            item["theme_id"],
        )
    )
    rank_map = {row["theme_id"]: index for index, row in enumerate(selected, start=1)}
    for row in normalized:
        if row["kind"] == CHILD:
            parent = by_id.get(row.get("parent_theme_id"))
            if parent is None:
                row["selection_status"] = "PARENT_MISSING"
                row["selected_for_rotation"] = False
            elif row["theme_id"] in rank_map:
                row["primary_rank"] = rank_map[row["theme_id"]]
                row["inherited_primary_strength"] = parent["strength"]
                row["selected_for_rotation"] = True
                row["selection_status"] = "ELIGIBLE_CHILD_STANDALONE"
            else:
                row["primary_rank"] = rank_map.get(parent["theme_id"])
                row["inherited_primary_strength"] = parent["strength"]
                row["selected_for_rotation"] = parent["theme_id"] in rank_map
                row["selection_status"] = (
                    "INHERITED_FROM_PRIMARY"
                    if row["selected_for_rotation"]
                    else "ELIGIBLE_CHILD_NOT_SELECTED"
                    if row["selection_status"] == "ELIGIBLE_PRIMARY"
                    else row["selection_status"]
                )
        else:
            row["primary_rank"] = rank_map.get(row["theme_id"])
            row["selected_for_rotation"] = row["theme_id"] in rank_map
    return {
        "boards": sorted(normalized, key=lambda item: (0 if item["kind"] == PRIMARY else 1, item.get("primary_rank") or 999, -float(item["strength"] if item["strength"] is not None else -1), item["theme_id"])),
        "selected_primary_boards": [
            {
                "board_code": row["theme_id"],
                "board_name": row["board_name"],
                "theme_id": row["theme_id"],
                "strategy_theme_id": row.get("strategy_theme_id") or row["theme_id"],
                "rank": rank_map[row["theme_id"]],
                "strength": row["strength"],
                "main_net_inflow_cny": row.get("tencent_main_net_inflow_cny"),
                "turnover_coverage": row.get("turnover_coverage"),
                "flow_coverage": row.get("flow_coverage"),
                "effective_fund_coverage": row.get("effective_fund_coverage"),
                "coverage_basis": row.get("coverage_basis"),
                "coverage_degraded": row.get("coverage_degraded", row.get("degraded", False)),
                "coverage_degraded_reason": row.get("coverage_degraded_reason", row.get("degraded_reason")),
            }
            for row in selected
        ],
        "reason_code": "OK" if selected else "NO_ELIGIBLE_PRIMARY",
        "primary_candidate_count": len(primary),
        "eligible_primary_count": len(eligible),
        "eligible_standalone_child_count": len(standalone_children),
        "selected_direction_count": len(selected),
    }


def build_rotation_theme_snapshot(
    *,
    as_of: datetime,
    board_rows: Sequence[Mapping[str, Any]] | None = None,
    board_metrics: Sequence[Mapping[str, Any]] | None = None,
    config: RotationThemeConfig | Mapping[str, Any] | None = None,
    tencent_flows: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
    memberships: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
    eastmoney_flows: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
    expected_trade_date: date | None = None,
    captured_at: datetime | None = None,
    rotation_theme_count: int = ROTATION_THEME_TOP_N,
    fund_coverage_minimum: float = 0.8,
    price_coverage_minimum: float = 0.9,
) -> dict[str, Any]:
    """Assemble the final consumer-compatible rotation snapshot."""

    cutoff, trade_day = _cutoff_and_trade_date(as_of, expected_trade_date)
    capture = _aware(captured_at or cutoff)
    if capture.date() != trade_day or capture > cutoff:
        return unavailable_rotation_theme_snapshot(cutoff, "ROTATION_THEME_CAPTURE_TIME_INVALID", expected_trade_date=trade_day)
    taxonomy: RotationThemeConfig | None
    try:
        taxonomy = validate_rotation_theme_config(config) if config is not None else None
        raw_rows = list(board_rows if board_rows is not None else board_metrics or ())
        if tencent_flows is not None and memberships is not None:
            flow_metrics = aggregate_tencent_theme_flows(tencent_flows, memberships, expected_trade_date=trade_day)
        else:
            flow_metrics = {}
        east_metrics = _theme_metrics_map(eastmoney_flows)
        merged: list[dict[str, Any]] = []
        for raw in raw_rows:
            if not isinstance(raw, Mapping):
                raise RotationThemeDataError("ROTATION_THEME_BOARD_ROW_INVALID")
            row = dict(raw)
            theme_id = str(row.get("theme_id") or row.get("board_code") or "").strip().upper()
            if theme_id in flow_metrics:
                row.update(
                    {
                        key: value
                        for key, value in flow_metrics[theme_id].items()
                        if value is not None
                        or key
                        in {
                            "turnover_coverage",
                            "flow_coverage",
                            "effective_fund_coverage",
                            "coverage_basis",
                            "coverage_degraded",
                            "coverage_degraded_reason",
                            "degraded",
                            "degraded_reason",
                        }
                    }
                )
            if theme_id in east_metrics:
                row.update({f"eastmoney_{key}": value for key, value in east_metrics[theme_id].items() if value is not None})
            member = _membership_map_get(memberships, theme_id)
            if member:
                row = _merge_membership_metrics(row, member, trade_day)
            merged.append(row)
        calculated = calculate_rotation_strength(
            merged,
            rotation_theme_count=rotation_theme_count,
            config=taxonomy,
            as_of=cutoff,
            expected_trade_date=trade_day,
            fund_coverage_minimum=fund_coverage_minimum,
            price_coverage_minimum=price_coverage_minimum,
        )
    except (RotationThemeDataError, RotationThemeConfigError) as exc:
        return unavailable_rotation_theme_snapshot(cutoff, exc.reason_code, expected_trade_date=trade_day)
    boards = calculated["boards"]
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in boards:
        for symbol in row.get("constituents", ()):
            safe_symbol = _normalize_symbol(symbol)
            if safe_symbol is None:
                continue
            by_symbol[safe_symbol].append(
                {
                    "board_code": row["theme_id"],
                    "board_name": row["board_name"],
                    "theme_id": row["theme_id"],
                    "theme_level": row["kind"],
                    "parent_theme_id": row.get("parent_theme_id"),
                    # Persist the canonical A1 strategy binding on every
                    # reverse-index row.  A2 consumes ``by_symbol`` directly;
                    # omitting this field previously made a legitimate board
                    # such as FINANCIAL_INSURANCE look unrelated to its A1
                    # direction FINANCIAL_HIGH_DIVIDEND.
                    "strategy_theme_id": row.get("strategy_theme_id") or row["theme_id"],
                    "strength": row["strength"],
                    "main_net_inflow_cny": row.get("tencent_main_net_inflow_cny"),
                    "selected_for_rotation": row.get("selected_for_rotation", False),
                    "primary_rank": row.get("primary_rank"),
                    "is_child_board": row["kind"] == CHILD,
                }
            )
    for symbol in by_symbol:
        by_symbol[symbol].sort(key=lambda item: (item["primary_rank"] or 999, item["board_code"]))
    output_boards = [_public_board_row(row) for row in boards]
    source_count = len(output_boards)
    selected_count = len(calculated["selected_primary_boards"])
    total_members = sum(int(row.get("member_count") or len(row.get("constituents", ()))) for row in boards)
    complete_members = sum(1 for row in boards if row.get("member_snapshot_complete") is True)
    excluded_non_a_share_symbols = sorted(
        {
            symbol
            for row in boards
            for value in (row.get("excluded_non_a_share_symbols") or ())
            if (symbol := _normalize_symbol(value)) and _is_non_a_share_symbol(symbol)
        }
    )
    payload: dict[str, Any] = {
        "schema_version": ROTATION_THEME_SCHEMA,
        "source_id": ROTATION_THEME_SOURCE_ID,
        "available": bool(output_boards),
        "reason_code": calculated["reason_code"],
        "trade_date": trade_day.isoformat(),
        "captured_at": capture.isoformat(),
        "boards": output_boards,
        "selected_primary_boards": calculated["selected_primary_boards"],
        "by_symbol": dict(sorted(by_symbol.items())),
        "coverage": {
            "board_count": source_count,
            "primary_count": sum(row.get("kind") == PRIMARY for row in boards),
            "selected_primary_count": selected_count,
            "member_count": total_members,
            "member_snapshot_complete_count": complete_members,
            "member_snapshot_coverage": complete_members / source_count if source_count else 0.0,
            "rotation_theme_count": int(rotation_theme_count),
            "excluded_non_a_share_count": len(excluded_non_a_share_symbols),
            "excluded_non_a_share_symbols": excluded_non_a_share_symbols,
        },
        "quality": {
            "formula": {key: value for key, value in STRENGTH_WEIGHTS.items()},
            "score_scale": "0-100",
            "tencent_positive_flow_gate": True,
            "tencent_effective_fund_coverage_gate": float(fund_coverage_minimum),
            "tencent_fund_coverage_policy": "turnover_if_valid_else_member_count",
            "price_coverage_gate": float(price_coverage_minimum),
            "minimum_factor_coverage": MINIMUM_FACTOR_COVERAGE,
            "factor_coverage": {
                row["theme_id"]: {
                    "coverage": row.get("factor_coverage", 0.0),
                    "available": row.get("available_factors", []),
                    "missing": row.get("missing_factors", []),
                }
                for row in boards
            },
            "eastmoney_conflict_policy": "OBSERVATION_ONLY",
            "pagination_required": True,
            "taxonomy_version": taxonomy.version if taxonomy else None,
        },
        "content_hash": "",
        "taxonomy_substitution_forbidden": False,
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def collect_rotation_theme_snapshot(
    *,
    as_of: datetime,
    expected_trade_date: date | None = None,
    registry_path: str | Path | None = None,
    snapshot_dir: str | Path,
    rotation_theme_count: int = ROTATION_THEME_TOP_N,
    membership_refresh_days: int = 7,
    warn_age_days: int = 7,
    max_age_days: int = 14,
    fund_coverage_minimum: float = 0.8,
    price_coverage_minimum: float = 0.9,
    workers: int = 16,
    fetchers: Mapping[str, Callable[..., Any]] | None = None,
    eastmoney_fetch_page: Callable[..., Any] | None = None,
    eastmoney_catalog_fetcher: Callable[..., Any] | None = None,
    eastmoney_flow_fetcher: Callable[..., Any] | None = None,
    eastmoney_members_fetcher: Callable[..., Any] | None = None,
    tencent_fetch_symbol: Callable[[str], Any] | None = None,
    tencent_quote_fetcher: Callable[..., Any] | None = None,
    tencent_capture_timestamp: datetime | str | None = None,
    tencent_capture_timestamp_fetcher: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Collect and persist one complete daily free-data rotation snapshot.

    Board membership is treated as a slow-changing dimension.  The latest
    successful version is loaded first; only versions at or beyond
    ``membership_refresh_days`` are refreshed.  A failed refresh may reuse a
    version up to ``max_age_days`` with a warning, after which the associated
    theme is fail-closed.  Daily flow and price inputs are never read from a
    prior day.

    All network boundaries are injectable.  The default Eastmoney/Tencent
    fetchers are intentionally lazy and are only invoked when a theme needs a
    current observation.
    """

    cutoff = _aware(as_of)
    trade_day = expected_trade_date or cutoff.date()
    trade_day = _parse_date_optional(trade_day) or cutoff.date()
    # Historical calls are read-only archive queries.  Never hit a current
    # provider endpoint and never overwrite the archive with an unavailable
    # result merely because a backfill was requested.
    if expected_trade_date is not None and trade_day < cutoff.date():
        return load_rotation_theme_snapshot(snapshot_dir, trade_day)
    try:
        trade_day = _parse_date(trade_day, "ROTATION_THEME_TRADE_DATE_INVALID")
        limit = int(rotation_theme_count)
        if isinstance(rotation_theme_count, bool) or limit < 1:
            raise RotationThemeDataError("ROTATION_THEME_TOP_N_INVALID")
        refresh_days = _validate_age_policy(membership_refresh_days, "MEMBERSHIP_REFRESH_DAYS_INVALID")
        warn_days = _validate_age_policy(warn_age_days, "MEMBERSHIP_WARN_AGE_DAYS_INVALID")
        expire_days = _validate_age_policy(max_age_days, "MEMBERSHIP_MAX_AGE_DAYS_INVALID")
        if expire_days < warn_days:
            raise RotationThemeDataError("MEMBERSHIP_FRESHNESS_POLICY_INVALID")
        _validate_coverage_threshold(fund_coverage_minimum, "ROTATION_THEME_FUND_COVERAGE_THRESHOLD_INVALID")
        _validate_coverage_threshold(price_coverage_minimum, "ROTATION_THEME_PRICE_COVERAGE_THRESHOLD_INVALID")
        if isinstance(workers, bool) or not 1 <= int(workers) <= 32:
            raise RotationThemeDataError("ROTATION_THEME_WORKERS_INVALID")
        taxonomy = load_rotation_theme_config(registry_path)
    except (RotationThemeConfigError, RotationThemeDataError) as exc:
        snapshot = unavailable_rotation_theme_snapshot(cutoff, exc.reason_code, expected_trade_date=trade_day)
        snapshot["source_health"] = {"config": "UNAVAILABLE"}
        snapshot.pop("content_hash", None)
        snapshot["content_hash"] = _content_hash(snapshot)
        return _persist_daily_rotation_snapshot(snapshot_dir, snapshot)

    provided = dict(fetchers or {})
    page_fetcher = eastmoney_fetch_page or provided.get("eastmoney_page") or provided.get("eastmoney_fetch_page")
    # Production uses lazy public fetchers when no test/integration fetcher is
    # injected.  Tests can pass all four boundaries explicitly and never
    # trigger a network call.
    catalog_fetcher = eastmoney_catalog_fetcher or provided.get("eastmoney_catalog") or page_fetcher or _default_eastmoney_page_fetch
    flow_fetcher = eastmoney_flow_fetcher or provided.get("eastmoney_flow") or page_fetcher or _default_eastmoney_page_fetch
    member_fetcher = eastmoney_members_fetcher or provided.get("eastmoney_members") or page_fetcher or _default_eastmoney_page_fetch
    tencent_fetch = tencent_fetch_symbol or provided.get("tencent_flow") or provided.get("tencent_fetch_symbol") or _default_tencent_flow_fetch
    quote_fetch = tencent_quote_fetcher or provided.get("tencent_quote") or provided.get("tencent_quote_fetcher") or _default_tencent_quote_fetch
    capture_fetch = tencent_capture_timestamp_fetcher or provided.get("tencent_capture_timestamp") or _default_tencent_capture_timestamp_fetcher
    if tencent_capture_timestamp is None and "tencent_capture_time" in provided:
        tencent_capture_timestamp = provided.get("tencent_capture_time")

    root = Path(snapshot_dir)
    membership_dir = root / "memberships"
    catalog: dict[str, Any] = {"available": False, "records": [], "reason_code": "NOT_REQUESTED"}
    if catalog_fetcher is not None:
        catalog = collect_eastmoney_board_catalog(
            as_of=cutoff,
            expected_trade_date=trade_day,
            fetch_page=catalog_fetcher,
        )

    resolved_codes: dict[str, tuple[str, ...]] = {}
    for theme in taxonomy.active(trade_day):
        configured = tuple(theme.eastmoney_board_codes)
        if configured:
            resolved_codes[theme.theme_id] = configured
            continue
        resolved_codes[theme.theme_id] = _resolve_codes_from_catalog(theme, catalog)

    membership_state: dict[str, dict[str, Any]] = {}
    membership_update_warnings: dict[str, str] = {}
    for theme in taxonomy.active(trade_day):
        loaded = load_membership_snapshot(
            membership_dir,
            theme.theme_id,
            trade_day,
            now=cutoff,
            warn_after_days=warn_days,
            expire_after_days=expire_days,
        )
        should_refresh = not loaded.get("available") or int(loaded.get("age_days") or 0) >= refresh_days
        if should_refresh and resolved_codes.get(theme.theme_id):
            # A theme may be represented by more than one public board.  The
            # records are combined into one internal theme snapshot; every
            # page's count remains in the immutable evidence envelope.
            fetched_members: list[Mapping[str, Any]] = []
            page_evidence: list[Mapping[str, Any]] = []
            failed_refresh = False
            latest_capture: datetime | None = None
            for board_code in resolved_codes[theme.theme_id]:
                result = collect_eastmoney_board_members(
                    as_of=cutoff,
                    expected_trade_date=trade_day,
                    board_code=board_code,
                    fetch_page=member_fetcher,
                    membership_snapshot_dir=None,
                    effective_from=theme.effective_from,
                )
                if not result.get("available"):
                    failed_refresh = True
                    break
                fetched_members.extend(item for item in result.get("records", ()) if isinstance(item, Mapping))
                page_evidence.append({
                    "board_code": board_code,
                    "provider_total": result.get("provider_total"),
                    "pagination_evidence": result.get("pagination_evidence"),
                })
                stamp = _parse_datetime_optional(result.get("captured_at"))
                if stamp is not None:
                    latest_capture = max(latest_capture, stamp) if latest_capture else stamp
            if not failed_refresh and fetched_members and latest_capture is not None:
                unique: dict[str, Mapping[str, Any]] = {}
                for item in fetched_members:
                    symbol = _normalize_symbol(item.get("symbol"))
                    if symbol:
                        unique.setdefault(symbol, item)
                combined_evidence = {
                    "total": len(unique),
                    "page_size": None,
                    "pages": page_evidence,
                    "complete": True,
                }
                try:
                    refreshed = build_membership_snapshot(
                        theme_id=theme.theme_id,
                        members=list(unique.values()),
                        captured_at=latest_capture,
                        effective_from=theme.effective_from,
                        source=EASTMONEY_BOARD_SOURCE_ID,
                        pagination_evidence=combined_evidence,
                        expected_trade_date=trade_day,
                    )
                    write_membership_snapshot(membership_dir, refreshed)
                    loaded = refreshed
                except RotationThemeDataError:
                    failed_refresh = True
            if failed_refresh:
                if loaded.get("available") and int(loaded.get("age_days") or 0) <= expire_days:
                    loaded = load_membership_snapshot(
                        membership_dir,
                        theme.theme_id,
                        trade_day,
                        now=cutoff,
                        warn_after_days=warn_days,
                        expire_after_days=expire_days,
                        update_failed=True,
                    )
                    membership_update_warnings[theme.theme_id] = "MEMBERSHIP_UPDATE_FAILED_FALLBACK"
                else:
                    loaded = unavailable_membership_snapshot(theme.theme_id, trade_day, "MEMBERSHIP_UPDATE_FAILED_EXPIRED")
        membership_state[theme.theme_id] = loaded

    # Daily board flow is independent from the slow member dimension.
    flow_snapshot: dict[str, Any] = {"available": False, "records": [], "reason_code": "NOT_REQUESTED"}
    if flow_fetcher is not None:
        flow_snapshot = collect_eastmoney_board_flow(
            as_of=cutoff,
            expected_trade_date=trade_day,
            fetch_page=flow_fetcher,
        )

    groups: dict[str, Any] = {}
    raw_rows: list[dict[str, Any]] = []
    active_themes = taxonomy.active(trade_day)
    for theme in active_themes:
        member = membership_state.get(theme.theme_id) or {}
        if member.get("available") is not True:
            # Keep unresolved or expired reference mappings visible in
            # source_health without allowing one optional theme to erase all
            # otherwise valid primary directions.
            continue
        records, excluded_symbols = _filter_a_share_members(member.get("records") or ())
        excluded_symbols = sorted(
            {
                *excluded_symbols,
                *(
                    symbol
                    for value in (member.get("excluded_non_a_share_symbols") or ())
                    if (symbol := _normalize_symbol(value)) and _is_non_a_share_symbol(symbol)
                ),
            }
        )
        groups[theme.theme_id] = records if member.get("available") else ()
        flow = _flow_for_theme(theme, flow_snapshot, resolved_codes.get(theme.theme_id, ()))
        raw_rows.append(
            {
                "theme_id": theme.theme_id,
                "board_name": theme.name,
                "kind": theme.kind,
                "parent": theme.parent,
                "effective_from": theme.effective_from,
                "constituents": [item.get("symbol") for item in records if isinstance(item, Mapping)],
                "member_count": len(records),
                "excluded_non_a_share_count": len(excluded_symbols),
                "excluded_non_a_share_symbols": excluded_symbols,
                "member_snapshot_complete": bool(member.get("available") and member.get("pagination_evidence", {}).get("complete") is True),
                "member_snapshot_trade_date": trade_day,
                # Member snapshots are identity-only for daily scoring.  The
                # current-day quote/flow pass below fills these fields.
                "price_coverage": 0.0,
                "breadth": None,
                "relative_return_pct": flow.get("relative_return_pct"),
                "eastmoney_main_net_inflow_cny": flow.get("eastmoney_main_net_inflow_cny"),
                "provider_rank": flow.get("provider_rank"),
                "momentum_3d_pct": flow.get("momentum_3d_pct"),
                "momentum_5d_pct": flow.get("momentum_5d_pct"),
                "leader_structure_score": flow.get("leader_structure_score"),
                "rank_persistence_score": flow.get("rank_persistence_score"),
            }
        )
    tencent_snapshot = {"available": False, "records": [], "reason_code": "NOT_REQUESTED"}
    symbols = [symbol for values in groups.values() for item in values if isinstance(item, Mapping) and (symbol := _normalize_symbol(item.get("symbol")))]
    if symbols and tencent_fetch is not None:
        tencent_snapshot = collect_tencent_capital_flow(
            as_of=cutoff,
            expected_trade_date=trade_day,
            expected_symbols=tuple(dict.fromkeys(symbols)),
            fetch_symbol=tencent_fetch,
            capture_timestamp=tencent_capture_timestamp,
            fetch_capture_timestamp=capture_fetch,
            quote_fetch=quote_fetch,
            workers=workers,
        )
    # Rebuild all daily price/breadth facts from the same-day Tencent response
    # (and its quote proof).  Never use latest_price/change_pct stored in the
    # slow membership dimension, even when that dimension is <=14 days old.
    tencent_by_symbol = {
        str(item.get("symbol")): item
        for item in tencent_snapshot.get("records", ())
        if isinstance(item, Mapping) and _normalize_symbol(item.get("symbol"))
    }
    quote_records = tencent_snapshot.get("quote_records") if isinstance(tencent_snapshot, Mapping) else {}
    if not isinstance(quote_records, Mapping):
        quote_records = {}
    if symbols and quote_fetch is not None and any(
        _number(item.get("latest_price")) is None or _number(item.get("change_pct")) is None
        for item in tencent_by_symbol.values()
        if isinstance(item, Mapping)
    ):
        # A flow response can carry a valid trade_date but no quote fields;
        # obtain same-day quotes separately for the daily coverage factors.
        supplemental_quotes = _same_day_quote_records(
            quote_fetch=quote_fetch,
            quotes=None,
            symbols=tuple(dict.fromkeys(symbols)),
            expected_trade_date=trade_day,
            as_of=cutoff,
            workers=int(workers),
        )
        if supplemental_quotes:
            quote_records = {**dict(quote_records), **supplemental_quotes}
    for flow_row in tencent_snapshot.get("records", ()):
        if not isinstance(flow_row, dict):
            continue
        quote = quote_records.get(flow_row.get("symbol"))
        if isinstance(quote, Mapping):
            for key in ("latest_price", "change_pct", "turnover_cny"):
                if flow_row.get(key) is None and quote.get(key) is not None:
                    flow_row[key] = quote.get(key)
    daily_groups: dict[str, list[dict[str, Any]]] = {}
    for row in raw_rows:
        daily: list[Mapping[str, Any]] = []
        daily_group_records: list[dict[str, Any]] = []
        for item in groups.get(row["theme_id"], ()):
            symbol = _normalize_symbol(item.get("symbol")) if isinstance(item, Mapping) else _normalize_symbol(item)
            if not symbol:
                continue
            value = dict(tencent_by_symbol.get(symbol) or {})
            quote = quote_records.get(symbol)
            if isinstance(quote, Mapping):
                for key in ("latest_price", "change_pct", "turnover_cny"):
                    if quote.get(key) is not None:
                        value[key] = quote.get(key)
            daily_group_records.append({"symbol": symbol, "turnover_cny": _turnover(value) if value else None})
            if value:
                daily.append(value)
        daily_groups[row["theme_id"]] = daily_group_records
        changes = [_number(item.get("change_pct")) for item in daily]
        valid_changes = [value for value in changes if value is not None]
        prices = [_number(item.get("latest_price")) for item in daily]
        row["breadth"] = (sum(value > 0 for value in valid_changes) / len(valid_changes)) if valid_changes else None
        row["price_coverage"] = sum(value is not None and value > 0 for value in prices) / len(groups.get(row["theme_id"], ())) if groups.get(row["theme_id"]) else 0.0
        if row.get("relative_return_pct") is None and valid_changes:
            row["relative_return_pct"] = sum(valid_changes) / len(valid_changes)
        # Leader structure is a blend of top-decile positive-return
        # contribution and limit-up breadth.  It remains unavailable when no
        # same-day quote exists rather than being silently set to zero.
        if valid_changes:
            ordered = sorted((value for value in valid_changes if value > 0), reverse=True)
            top_count = max(1, math.ceil(len(valid_changes) * 0.10))
            positive_total = sum(value for value in valid_changes if value > 0)
            top_contribution = sum(ordered[:top_count]) / positive_total if positive_total > 0 else None
            limit_ratio = sum(value >= 9.5 for value in valid_changes) / len(valid_changes)
            row["leader_structure_score"] = (0.7 * top_contribution + 0.3 * limit_ratio) if top_contribution is not None else None
    _apply_history_factors(raw_rows, root, trade_day)
    flow_metrics = aggregate_tencent_theme_flows(tencent_snapshot, daily_groups) if tencent_snapshot.get("available") else {}
    for row in raw_rows:
        metrics = flow_metrics.get(row["theme_id"], {})
        prior_excluded = {
            symbol
            for value in (row.get("excluded_non_a_share_symbols") or ())
            if (symbol := _normalize_symbol(value)) and _is_non_a_share_symbol(symbol)
        }
        row.update(metrics)
        # The daily group intentionally contains only tradable members, so
        # aggregate_tencent_theme_flows cannot see exclusions that happened
        # during membership normalization.  Preserve that audit evidence when
        # merging the independent flow metrics.
        merged_excluded = prior_excluded | {
            symbol
            for value in (metrics.get("excluded_non_a_share_symbols") or ())
            if (symbol := _normalize_symbol(value)) and _is_non_a_share_symbol(symbol)
        }
        row["excluded_non_a_share_count"] = len(merged_excluded)
        row["excluded_non_a_share_symbols"] = sorted(merged_excluded)

    snapshot = build_rotation_theme_snapshot(
        as_of=cutoff,
        expected_trade_date=trade_day,
        captured_at=cutoff,
        config=taxonomy,
        board_rows=raw_rows,
        rotation_theme_count=limit,
        fund_coverage_minimum=fund_coverage_minimum,
        price_coverage_minimum=price_coverage_minimum,
    )
    snapshot["source_health"] = {
        "eastmoney_catalog": catalog.get("reason_code") if catalog else "NOT_REQUESTED",
        "eastmoney_board_flow": flow_snapshot.get("reason_code") if flow_snapshot else "NOT_REQUESTED",
        "tencent_flow": tencent_snapshot.get("reason_code") if tencent_snapshot else "NOT_REQUESTED",
        "membership_refresh_days": refresh_days,
        "membership_warn_age_days": warn_days,
        "membership_max_age_days": expire_days,
        "membership_update_warnings": membership_update_warnings,
        "degraded_membership_count": sum(bool(value.get("warning")) for value in membership_state.values()),
        "unmapped_theme_ids": sorted(
            theme.theme_id
            for theme in active_themes
            if not resolved_codes.get(theme.theme_id)
        ),
        "unavailable_membership": {
            theme_id: str(value.get("reason_code") or "MEMBERSHIP_UNAVAILABLE")
            for theme_id, value in membership_state.items()
            if value.get("available") is not True
        },
        "excluded_non_a_share_count": sum(
            int(row.get("excluded_non_a_share_count") or 0)
            for row in raw_rows
        ),
    }
    snapshot.pop("content_hash", None)
    snapshot["content_hash"] = _content_hash(snapshot)
    return _persist_daily_rotation_snapshot(snapshot_dir, snapshot)


def _persist_daily_rotation_snapshot(snapshot_dir: str | Path, snapshot: dict[str, Any]) -> dict[str, Any]:
    try:
        path = write_rotation_theme_snapshot(snapshot_dir, snapshot)
    except RotationThemeDataError as exc:
        if exc.reason_code == "ROTATION_THEME_IMMUTABLE_OVERWRITE_FORBIDDEN":
            return {**snapshot, "available": False, "reason_code": exc.reason_code}
        return {**snapshot, "available": False, "reason_code": "ROTATION_THEME_SNAPSHOT_WRITE_FAILED"}
    except OSError:
        # The in-memory unavailable envelope is still useful to the caller;
        # do not turn a filesystem outage into a false available snapshot.
        return {**snapshot, "available": False, "reason_code": "ROTATION_THEME_SNAPSHOT_WRITE_FAILED"}
    return {**snapshot, "snapshot_path": str(path)}


def _resolve_codes_from_catalog(theme: RotationTheme, catalog: Mapping[str, Any]) -> tuple[str, ...]:
    if not catalog.get("available"):
        return ()
    aliases = {theme.name, *theme.aliases}
    matches = {
        str(item.get("board_code") or "").strip().upper()
        for item in catalog.get("records", ())
        if isinstance(item, Mapping)
        and str(item.get("board_name") or "").strip() in aliases
        and _EASTMONEY_BOARD_CODE.fullmatch(str(item.get("board_code") or "").strip().upper())
    }
    # Name-based matching is only safe if it gives exactly one code.  A
    # duplicate alias across public categories remains unresolved.
    return tuple(sorted(matches)) if len(matches) == 1 else ()


def _flow_for_theme(theme: RotationTheme, flow_snapshot: Mapping[str, Any], codes: Sequence[str]) -> dict[str, Any]:
    rows = flow_snapshot.get("records") if isinstance(flow_snapshot, Mapping) else ()
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return {}
    code_set = {str(code).strip().upper() for code in codes}
    names = {theme.name, *theme.aliases}
    matched: list[Mapping[str, Any]] = []
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        item_code = str(item.get("board_code") or item.get("code") or "").strip().upper()
        item_name = str(item.get("board_name") or item.get("name") or "").strip()
        if (item_code and item_code in code_set) or (not code_set and item_name in names):
            matched.append(item)
    if not matched:
        return {}
    returns = [value for item in matched if (value := _number(item.get("relative_return_pct"))) is not None]
    flows = [value for item in matched if (value := _number(item.get("eastmoney_main_net_inflow_cny"))) is not None]
    ranks = [value for item in matched if (value := _integer(item.get("provider_rank"))) is not None]
    return {
        "relative_return_pct": sum(returns) / len(returns) if returns else None,
        "eastmoney_main_net_inflow_cny": sum(flows) if flows else None,
        "provider_rank": min(ranks) if ranks else None,
        "component_board_codes": sorted(
            str(item.get("board_code") or "").strip().upper() for item in matched
        ),
    }


def _apply_history_factors(rows: Sequence[dict[str, Any]], snapshot_dir: Path, trade_day: date) -> None:
    """Fill 3/5-day momentum and rank persistence from prior daily files.

    Only hash-valid snapshots strictly older than ``trade_day`` are used.  A
    short history leaves these factors missing and visible in factor_coverage;
    it is never converted to a zero score.
    """

    history = _load_recent_rotation_history(snapshot_dir, trade_day, limit=5)
    for row in rows:
        theme_id = row.get("theme_id")
        previous = [item.get(theme_id) for item in history if theme_id in item]
        previous = [item for item in previous if isinstance(item, Mapping)]
        current_return = _number(row.get("relative_return_pct"))
        returns = [current_return] + [_number(item.get("relative_return_pct")) for item in previous]
        if len(returns) >= 3 and all(value is not None for value in returns[:3]):
            row["momentum_3d_pct"] = sum(float(value) for value in returns[:3])
        if len(returns) >= 5 and all(value is not None for value in returns[:5]):
            row["momentum_5d_pct"] = sum(float(value) for value in returns[:5])
        ranks = [_integer(item.get("liangjian_rank") or item.get("provider_rank")) for item in previous]
        ranks = [rank for rank in ranks if rank is not None and rank >= 1]
        if len(ranks) >= 3:
            all_ranks = [
                rank
                for group in history
                for item in group.values()
                if isinstance(item, Mapping)
                and (rank := _integer(item.get("liangjian_rank") or item.get("provider_rank"))) is not None
                and rank >= 1
            ]
            max_rank = max(all_ranks or ranks)
            row["rank_persistence_score"] = max(0.0, min(1.0, 1.0 - (sum(ranks) / len(ranks) - 1.0) / max(1.0, max_rank - 1.0)))


def _load_recent_rotation_history(snapshot_dir: Path, trade_day: date, *, limit: int) -> list[dict[str, dict[str, Any]]]:
    snapshots: list[tuple[date, dict[str, Any]]] = []
    for path in snapshot_dir.glob("rotation-theme-*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping) or payload.get("schema_version") != ROTATION_THEME_SCHEMA:
            continue
        body = dict(payload)
        expected = str(body.pop("content_hash", ""))
        if not expected or expected != _content_hash(body):
            continue
        day = _parse_date_optional(payload.get("trade_date"))
        if day is None or day >= trade_day:
            continue
        rows = payload.get("boards")
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
            continue
        normalized_rows = [
            dict(item)
            for item in rows
            if isinstance(item, Mapping)
            and str(item.get("theme_id") or item.get("board_code") or "").strip()
        ]
        primaries = sorted(
            (item for item in normalized_rows if str(item.get("theme_level") or "").upper() == PRIMARY),
            key=lambda item: (-float(_number(item.get("strength")) or -1.0), str(item.get("theme_id") or "")),
        )
        rank_by_theme = {
            str(item.get("theme_id") or item.get("board_code") or "").strip().upper(): rank
            for rank, item in enumerate(primaries, start=1)
        }
        by_theme = {}
        for item in normalized_rows:
            theme_id = str(item.get("theme_id") or item.get("board_code") or "").strip().upper()
            item["liangjian_rank"] = rank_by_theme.get(theme_id)
            by_theme[theme_id] = item
        snapshots.append((day, by_theme))
    snapshots.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in snapshots[:limit]]


def _validate_age_policy(value: Any, reason_code: str) -> int:
    if isinstance(value, bool):
        raise RotationThemeDataError(reason_code)
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RotationThemeDataError(reason_code) from exc
    if result < 0:
        raise RotationThemeDataError(reason_code)
    return result


def _validate_coverage_threshold(value: Any, reason_code: str) -> float:
    if isinstance(value, bool):
        raise RotationThemeDataError(reason_code)
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RotationThemeDataError(reason_code) from exc
    if not math.isfinite(result) or not 0.0 <= result <= 1.0:
        raise RotationThemeDataError(reason_code)
    return result


def unavailable_rotation_theme_snapshot(
    as_of: datetime,
    reason_code: str,
    *,
    expected_trade_date: date | None = None,
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    trade_day = expected_trade_date or cutoff.date()
    payload: dict[str, Any] = {
        "schema_version": ROTATION_THEME_SCHEMA,
        "source_id": ROTATION_THEME_SOURCE_ID,
        "available": False,
        "reason_code": str(reason_code or "ROTATION_THEME_UNAVAILABLE"),
        "trade_date": trade_day.isoformat(),
        "captured_at": cutoff.isoformat(),
        "boards": [],
        "selected_primary_boards": [],
        "by_symbol": {},
        "coverage": {"board_count": 0, "primary_count": 0, "selected_primary_count": 0},
        "quality": {"formula": dict(STRENGTH_WEIGHTS)},
        "content_hash": "",
        "taxonomy_substitution_forbidden": False,
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def write_rotation_theme_snapshot(snapshot_dir: str | Path, snapshot: Mapping[str, Any]) -> Path:
    """Persist an immutable hash version and atomically advance the day view."""

    if not isinstance(snapshot, Mapping) or snapshot.get("schema_version") != ROTATION_THEME_SCHEMA:
        raise RotationThemeDataError("ROTATION_THEME_SNAPSHOT_INVALID")
    expected_hash = str(snapshot.get("content_hash") or "")
    if not expected_hash or expected_hash != _content_hash(snapshot):
        raise RotationThemeDataError("ROTATION_THEME_SNAPSHOT_HASH_INVALID")
    trade_day = _parse_date(snapshot.get("trade_date"), "ROTATION_THEME_TRADE_DATE_INVALID")
    root = Path(snapshot_dir)
    root.mkdir(parents=True, exist_ok=True)
    captured = _parse_datetime(snapshot.get("captured_at"), "ROTATION_THEME_CAPTURE_TIME_INVALID")
    version_dir = root / "daily_versions"
    version_dir.mkdir(parents=True, exist_ok=True)
    version_path = version_dir / (
        f"rotation-theme-{trade_day.isoformat()}-{captured.strftime('%H%M%S')}-{expected_hash[:16]}.json"
    )
    if version_path.is_file():
        try:
            version = json.loads(version_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RotationThemeDataError("ROTATION_THEME_EXISTING_VERSION_INVALID") from exc
        if not isinstance(version, Mapping) or version.get("content_hash") != expected_hash:
            raise RotationThemeDataError("ROTATION_THEME_IMMUTABLE_OVERWRITE_FORBIDDEN")
    else:
        atomic_write_json(version_path, dict(snapshot))
    path = root / f"rotation-theme-{trade_day.isoformat()}.json"
    if path.is_file():
        try:
            current = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RotationThemeDataError("ROTATION_THEME_EXISTING_DAILY_INVALID") from exc
        if isinstance(current, Mapping) and current.get("content_hash") == expected_hash:
            return path
        current_captured = _parse_datetime(
            current.get("captured_at"),
            "ROTATION_THEME_EXISTING_DAILY_INVALID",
        )
        # Hash-addressed versions are immutable, while the date view is a
        # recoverable pointer to the newest capture for that trading day.
        # Never let an older replay replace a later point-in-time result, and
        # reject different content at the exact same capture timestamp.
        if captured <= current_captured:
            raise RotationThemeDataError("ROTATION_THEME_IMMUTABLE_OVERWRITE_FORBIDDEN")
        atomic_write_json(path, dict(snapshot))
        return path
    atomic_write_json(path, dict(snapshot))
    return path


persist_rotation_theme_snapshot = write_rotation_theme_snapshot


def load_rotation_theme_snapshot(snapshot_dir: str | Path, expected_trade_date: date) -> dict[str, Any]:
    """Read one archived daily snapshot and verify identity/hash in place."""

    trade_day = _parse_date(expected_trade_date, "ROTATION_THEME_TRADE_DATE_INVALID")
    path = Path(snapshot_dir) / f"rotation-theme-{trade_day.isoformat()}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return unavailable_rotation_theme_snapshot(
            datetime.combine(trade_day, datetime.min.time(), tzinfo=SHANGHAI),
            "ROTATION_THEME_ARCHIVE_MISSING",
            expected_trade_date=trade_day,
        ) | {"snapshot_path": str(path)}
    except (OSError, json.JSONDecodeError):
        return unavailable_rotation_theme_snapshot(
            datetime.combine(trade_day, datetime.min.time(), tzinfo=SHANGHAI),
            "ROTATION_THEME_ARCHIVE_INVALID",
            expected_trade_date=trade_day,
        ) | {"snapshot_path": str(path)}
    if not isinstance(payload, Mapping) or payload.get("schema_version") != ROTATION_THEME_SCHEMA or payload.get("trade_date") != trade_day.isoformat():
        return unavailable_rotation_theme_snapshot(
            datetime.combine(trade_day, datetime.min.time(), tzinfo=SHANGHAI),
            "ROTATION_THEME_ARCHIVE_IDENTITY_MISMATCH",
            expected_trade_date=trade_day,
        ) | {"snapshot_path": str(path)}
    expected_hash = str(payload.get("content_hash") or "")
    if not expected_hash or expected_hash != _content_hash(payload):
        return unavailable_rotation_theme_snapshot(
            datetime.combine(trade_day, datetime.min.time(), tzinfo=SHANGHAI),
            "ROTATION_THEME_ARCHIVE_HASH_MISMATCH",
            expected_trade_date=trade_day,
        ) | {"snapshot_path": str(path)}
    return {**dict(payload), "snapshot_path": str(path), "archive_read_only": True}


def _normalize_metric_row(row: Mapping[str, Any], taxonomy: RotationThemeConfig | None, trade_day: date | None) -> dict[str, Any]:
    theme_id = str(row.get("theme_id") or row.get("board_code") or "").strip().upper()
    if not theme_id:
        raise RotationThemeDataError("ROTATION_THEME_ID_MISSING")
    theme = taxonomy.by_id.get(theme_id) if taxonomy is not None else None
    kind = str(row.get("kind") or row.get("theme_level") or (theme.kind if theme else "")).strip().upper()
    if kind not in {PRIMARY, CHILD}:
        raise RotationThemeDataError("ROTATION_THEME_KIND_INVALID")
    parent = str(row.get("parent_theme_id") or row.get("parent") or (theme.parent if theme else "") or "").strip().upper() or None
    if kind == CHILD and not parent:
        raise RotationThemeDataError("ROTATION_THEME_CHILD_PARENT_MISSING")
    if kind == PRIMARY:
        parent = None
    name = str(row.get("board_name") or row.get("name") or (theme.name if theme else "")).strip()
    if not name:
        raise RotationThemeDataError("ROTATION_THEME_NAME_MISSING")
    members = row.get("constituents")
    if members is None:
        members = row.get("members") or row.get("member_symbols") or ()
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes, bytearray)):
        raise RotationThemeDataError("ROTATION_THEME_CONSTITUENTS_INVALID")
    safe_member_values: list[str] = []
    excluded_symbols = {
        symbol
        for value in (row.get("excluded_non_a_share_symbols") or ())
        if (symbol := _normalize_symbol(value)) and _is_non_a_share_symbol(symbol)
    }
    for value in members:
        raw_symbol = value.get("symbol") if isinstance(value, Mapping) else value
        symbol = _normalize_symbol(raw_symbol)
        if symbol is None:
            continue
        if _is_non_a_share_symbol(symbol):
            excluded_symbols.add(symbol)
            continue
        safe_member_values.append(symbol)
    safe_members = tuple(dict.fromkeys(safe_member_values))
    if not safe_members and not excluded_symbols:
        raise RotationThemeDataError("ROTATION_THEME_CONSTITUENTS_EMPTY")
    result = dict(row)
    result.update(
        {
            "theme_id": theme_id,
            "board_code": theme_id,
            "board_name": name,
            "kind": kind,
            "parent_theme_id": parent,
            "strategy_theme_id": (
                theme.strategy_theme_id if theme is not None else theme_id
            ),
            "constituents": safe_members,
            # The score denominator is the normalized A-share member set.  A
            # provider's raw count may include B shares and therefore cannot
            # be retained as the denominator after exclusion.
            "member_count": len(safe_members),
            "excluded_non_a_share_count": len(excluded_symbols),
            "excluded_non_a_share_symbols": sorted(excluded_symbols),
            "member_snapshot_complete": bool(row.get("member_snapshot_complete") or row.get("constituents_complete") or row.get("complete_members")),
            "price_coverage": _coverage_value(row.get("price_coverage", row.get("member_price_coverage"))),
            "tencent_main_net_inflow_cny": _number(row.get("tencent_main_net_inflow_cny", row.get("main_net_inflow_cny", row.get("net_inflow_amount")))),
            "turnover_coverage": _coverage_value(row.get("turnover_coverage", row.get("fund_coverage"))),
            "eastmoney_main_net_inflow_cny": _number(row.get("eastmoney_main_net_inflow_cny", row.get("eastmoney_main_net_cny"))),
        }
    )
    raw_coverage_basis = str(row.get("coverage_basis") or "").strip().lower()
    flow_coverage = _coverage_value(row.get("flow_coverage"))
    if flow_coverage is None:
        covered_count = _integer(row.get("covered_member_count"))
        member_count = int(result["member_count"])
        if covered_count is not None and member_count > 0:
            flow_coverage = _coverage_value(covered_count / member_count)
    # Accept an already materialized effective value when re-reading a
    # snapshot, but always derive the effective basis from the raw turnover
    # availability.  This prevents a member-count value from being mislabeled
    # as turnover coverage and preserves the no-fabrication rule.
    if flow_coverage is None and raw_coverage_basis == "member_count":
        flow_coverage = _coverage_value(row.get("effective_fund_coverage"))
    turnover_coverage = result["turnover_coverage"]
    if turnover_coverage is not None:
        effective_fund_coverage = turnover_coverage
        coverage_basis = "turnover"
        coverage_degraded = False
        coverage_degraded_reason = None
    else:
        effective_fund_coverage = flow_coverage
        coverage_basis = "member_count"
        coverage_degraded = True
        coverage_degraded_reason = str(
            row.get("coverage_degraded_reason")
            or row.get("degraded_reason")
            or "TENCENT_TURNOVER_DENOMINATOR_UNAVAILABLE"
        ).strip() or "TENCENT_TURNOVER_DENOMINATOR_UNAVAILABLE"
    result.update(
        {
            "flow_coverage": flow_coverage,
            "effective_fund_coverage": effective_fund_coverage,
            "coverage_basis": coverage_basis,
            "coverage_degraded": coverage_degraded,
            "coverage_degraded_reason": coverage_degraded_reason,
            # Generic aliases make the degraded state explicit to consumers
            # that do not know the rotation-theme field prefix.  They are
            # derived, never an independent source of truth.
            "degraded": coverage_degraded,
            "degraded_reason": coverage_degraded_reason,
        }
    )
    if trade_day is not None:
        member_date = _parse_date_optional(row.get("member_snapshot_trade_date"))
        if member_date is not None and member_date != trade_day:
            result["member_snapshot_complete"] = False
    return result


def _factor_percentiles(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, float | None]]:
    values: dict[str, dict[str, float | None]] = {
        str(row["theme_id"]): {key: _factor_raw(row, key) for key in STRENGTH_WEIGHTS}
        for row in rows
    }
    result: dict[str, dict[str, float]] = {theme_id: {} for theme_id in values}
    for key in STRENGTH_WEIGHTS:
        valid = [value for data in values.values() if (value := data.get(key)) is not None and math.isfinite(float(value))]
        for theme_id, data in values.items():
            raw = data.get(key)
            result[theme_id][key] = _percentile(float(raw), valid) if raw is not None and valid else None
    return result


def _percentile(value: float, values: Sequence[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(item) for item in values)
    if len(ordered) == 1:
        return 1.0
    positions = [index for index, item in enumerate(ordered) if item == value]
    if not positions:
        # Should not happen when value is in values, but clamp a numerical
        # edge case deterministically.
        less = sum(item < value for item in ordered)
        return min(1.0, max(0.0, less / (len(ordered) - 1)))
    return (sum(positions) / len(positions)) / (len(ordered) - 1)


def _factor_raw(row: Mapping[str, Any], factor: str) -> float | None:
    aliases: dict[str, tuple[str, ...]] = {
        "relative_return": ("relative_return_pct", "change_pct", "board_change_pct", "return_pct"),
        "breadth": ("breadth", "breadth_pct", "up_ratio", "up_breadth", "上涨广度"),
        "fund_flow": ("tencent_main_net_inflow_cny", "main_net_inflow_cny", "net_inflow_amount"),
        "momentum_3_5d": ("momentum_3_5d", "momentum_pct", "momentum_3d_pct", "momentum_5d_pct"),
        "leader_structure": ("leader_structure", "leader_structure_score", "leader_score", "limit_up_structure"),
        "rank_persistence": ("rank_persistence", "rank_persistence_score", "ranking_stability", "rank_stability"),
    }
    if factor == "momentum_3_5d":
        explicit = _number(_first(row, "momentum_3_5d", "momentum_pct"))
        if explicit is not None:
            return explicit
        three = _number(_first(row, "momentum_3d_pct", "momentum_3d"))
        five = _number(_first(row, "momentum_5d_pct", "momentum_5d"))
        if three is not None and five is not None:
            return (three + five) / 2.0
        return None
    value = _number(_first(row, *aliases[factor]))
    if factor in {"breadth", "leader_structure", "rank_persistence"} and value is not None:
        if value > 1.0:
            value /= 100.0
    return value


def _selection_status(
    row: Mapping[str, Any],
    trade_day: date | None,
    *,
    fund_coverage_minimum: float = 0.8,
    price_coverage_minimum: float = 0.9,
) -> str:
    if trade_day is not None and row.get("effective_from"):
        effective = _parse_date_optional(row.get("effective_from"))
        if effective is not None and effective > trade_day:
            return "THEME_NOT_ACTIVE"
    if not row.get("member_snapshot_complete"):
        return "OBSERVATION_ONLY_MEMBER_SNAPSHOT_INCOMPLETE"
    flow = _number(row.get("tencent_main_net_inflow_cny"))
    if flow is None or flow <= 0:
        return "OBSERVATION_ONLY_TENCENT_FLOW_NON_POSITIVE"
    coverage = row.get("effective_fund_coverage")
    if coverage is None or float(coverage) < fund_coverage_minimum:
        return "OBSERVATION_ONLY_TENCENT_COVERAGE_LOW"
    price_coverage = row.get("price_coverage")
    if price_coverage is None or float(price_coverage) < price_coverage_minimum:
        return "OBSERVATION_ONLY_PRICE_COVERAGE_LOW"
    if float(row.get("factor_coverage") or 0.0) < MINIMUM_FACTOR_COVERAGE:
        return "OBSERVATION_ONLY_FACTOR_COVERAGE_LOW"
    east_flow = _number(row.get("eastmoney_main_net_inflow_cny"))
    if east_flow is not None and east_flow < 0:
        return "OBSERVATION_ONLY_EASTMONEY_FLOW_CONFLICT"
    return "ELIGIBLE_PRIMARY"


def _percentile_for_child(row: Mapping[str, Any], factor_percentiles: Mapping[str, Mapping[str, float | None]], primaries: Sequence[Mapping[str, Any]]) -> dict[str, float | None]:
    # Children display their own six-factor score but do not alter primary
    # rank.  Percentiles are measured against primary raw factor values.
    result: dict[str, float] = {}
    for key in STRENGTH_WEIGHTS:
        raw = _factor_raw(row, key)
        values = [value for primary in primaries if (value := _factor_raw(primary, key)) is not None]
        result[key] = _percentile(float(raw), values) if raw is not None and values else None
    return result


def _public_board_row(row: Mapping[str, Any]) -> dict[str, Any]:
    fields = {
        "board_code": row["theme_id"],
        "board_name": row["board_name"],
        "theme_id": row["theme_id"],
        "theme_level": row["kind"],
        "parent_theme_id": row.get("parent_theme_id"),
        "strategy_theme_id": row.get("strategy_theme_id") or row["theme_id"],
        "strength": row.get("strength"),
        "strength_factors": dict(row.get("strength_factors") or {}),
        "relative_return_pct": row.get("relative_return_pct"),
        "breadth": row.get("breadth"),
        "momentum_3d_pct": row.get("momentum_3d_pct"),
        "momentum_5d_pct": row.get("momentum_5d_pct"),
        "leader_structure_score": row.get("leader_structure_score"),
        "rank_persistence_score": row.get("rank_persistence_score"),
        "provider_rank": row.get("provider_rank"),
        "factor_coverage": row.get("factor_coverage", 0.0),
        "available_factors": list(row.get("available_factors") or ()),
        "missing_factors": list(row.get("missing_factors") or ()),
        "tencent_main_net_inflow_cny": row.get("tencent_main_net_inflow_cny"),
        "main_net_inflow_cny": row.get("tencent_main_net_inflow_cny"),
        "eastmoney_main_net_inflow_cny": row.get("eastmoney_main_net_inflow_cny"),
        "turnover_coverage": row.get("turnover_coverage"),
        "flow_coverage": row.get("flow_coverage"),
        "effective_fund_coverage": row.get("effective_fund_coverage"),
        "coverage_basis": row.get("coverage_basis"),
        "coverage_degraded": row.get("coverage_degraded", row.get("degraded", False)),
        "coverage_degraded_reason": row.get("coverage_degraded_reason", row.get("degraded_reason")),
        "degraded": row.get("degraded", row.get("coverage_degraded", False)),
        "degraded_reason": row.get("degraded_reason", row.get("coverage_degraded_reason")),
        "price_coverage": row.get("price_coverage"),
        "member_snapshot_complete": row.get("member_snapshot_complete", False),
        "member_count": row.get("member_count", len(row.get("constituents", ()))),
        "covered_member_count": row.get("covered_member_count"),
        "excluded_non_a_share_count": row.get("excluded_non_a_share_count", 0),
        "excluded_non_a_share_symbols": list(row.get("excluded_non_a_share_symbols") or ()),
        "selection_status": row.get("selection_status"),
        "selected_for_rotation": row.get("selected_for_rotation", False),
        "primary_rank": row.get("primary_rank"),
        "is_child_board": row.get("kind") == CHILD,
        "constituents": sorted(set(row.get("constituents", ()))),
    }
    return fields


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _cutoff_and_trade_date(as_of: datetime, expected_trade_date: date | None) -> tuple[datetime, date]:
    cutoff = _aware(as_of)
    trade_day = expected_trade_date or cutoff.date()
    trade_day = _parse_date(trade_day, "ROTATION_THEME_TRADE_DATE_INVALID")
    if trade_day != cutoff.date():
        # A historical replay is allowed when the caller explicitly binds the
        # expected date, but no provider result may be captured after cutoff.
        pass
    return cutoff, trade_day


def _validate_provider_time(captured: datetime | None, trade: date | None, cutoff: datetime, expected: date) -> None:
    if captured is None and trade is None:
        raise RotationThemeDataError("EASTMONEY_TRADE_DATE_AND_CAPTURE_MISSING")
    if trade is not None and trade != expected:
        raise RotationThemeDataError("EASTMONEY_TRADE_DATE_MISMATCH")
    # Public Eastmoney pages usually expose no provider event timestamp.  The
    # default adapter therefore records the local HTTP receive time, which is
    # necessarily a few seconds after the invocation cutoff.  Permit only a
    # bounded same-day collection window; injected/provider timestamps beyond
    # that window remain future data and are rejected.
    if captured is not None and (
        captured.date() != expected
        or captured > _live_collection_upper_bound(cutoff, expected)
    ):
        raise RotationThemeDataError("EASTMONEY_CAPTURE_TIME_INVALID")


def _live_collection_upper_bound(cutoff: datetime, expected: date) -> datetime:
    """Return a strict historical cutoff or the current live fetch boundary."""

    current = datetime.now(SHANGHAI)
    lag_seconds = (current - cutoff).total_seconds()
    if (
        expected == current.date()
        and cutoff.date() == expected
        and -5 <= lag_seconds <= 30 * 60
    ):
        return current + timedelta(seconds=5)
    return cutoff


def _unavailable_source_snapshot(as_of: datetime, trade_date: date, *, reason_code: str, source_id: str, dataset: str, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schema_version": "liangjian-rotation-source/1.0.0",
        "dataset": dataset,
        "source_id": source_id,
        "available": False,
        "reason_code": str(reason_code or "SOURCE_UNAVAILABLE"),
        "trade_date": trade_date.isoformat(),
        "captured_at": _aware(as_of).isoformat(),
        "records": [],
        "provider_total": None,
        "record_count": 0,
        **dict(extra or {}),
    }


def _theme_metrics_map(payload: Any) -> dict[str, dict[str, Any]]:
    if payload is None:
        return {}
    if isinstance(payload, Mapping):
        result = {}
        for key, value in payload.items():
            if isinstance(value, Mapping):
                result[str(key).strip().upper()] = dict(value)
            else:
                result[str(key).strip().upper()] = {"main_net_inflow_cny": value}
        return result
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        result = {}
        for value in payload:
            if isinstance(value, Mapping):
                key = str(value.get("theme_id") or value.get("board_code") or "").strip().upper()
                if key:
                    result[key] = dict(value)
        return result
    return {}


def _membership_map_get(memberships: Any, theme_id: str) -> Mapping[str, Any] | None:
    if isinstance(memberships, Mapping):
        value = memberships.get(theme_id) or memberships.get(theme_id.upper())
        return value if isinstance(value, Mapping) else None
    if isinstance(memberships, Sequence) and not isinstance(memberships, (str, bytes, bytearray)):
        for value in memberships:
            if isinstance(value, Mapping) and str(value.get("theme_id") or "").strip().upper() == theme_id:
                return value
    return None


def _merge_membership_metrics(row: dict[str, Any], member: Mapping[str, Any], trade_day: date) -> dict[str, Any]:
    records = member.get("records") or member.get("constituents") or member.get("members") or ()
    row["constituents"] = [item.get("symbol") if isinstance(item, Mapping) else item for item in records]
    row["member_count"] = len(row["constituents"])
    row["member_snapshot_complete"] = bool(member.get("available") and member.get("pagination_evidence", {}).get("complete") is True)
    member_trade = _parse_date_optional(member.get("trade_date")) or _parse_date_optional(member.get("captured_at"))
    if member_trade is not None and member_trade != trade_day:
        row["member_snapshot_complete"] = False
    if "price_coverage" in member:
        row["price_coverage"] = _coverage_value(member.get("price_coverage"))
    return row


def _membership_symbols_and_turnover(group: Any) -> tuple[list[str], float | None]:
    """Return the eligible A-share symbols and their validated turnover sum."""

    symbols, total, _ = _membership_symbols_and_turnover_with_exclusions(group)
    return symbols, total


def _membership_symbols_and_turnover_with_exclusions(group: Any) -> tuple[list[str], float | None, list[str]]:
    rows = list(_iter_members(group))
    rows, excluded_symbols = _filter_a_share_members(rows)
    symbols = [symbol for item in rows if (symbol := _normalize_symbol(item.get("symbol") if isinstance(item, Mapping) else item))]
    turnover_values = [_turnover(item) for item in rows if isinstance(item, Mapping)]
    # A total with one missing member would overstate coverage.  Keep the
    # denominator unavailable until the same-day quote pass supplies every
    # member's turnover.
    if rows and (len(turnover_values) != len(rows) or any(value is None for value in turnover_values)):
        total = None
    else:
        total = sum(float(value) for value in turnover_values)
    return list(dict.fromkeys(symbols)), total or None, excluded_symbols


def _iter_members(group: Any) -> Sequence[Any]:
    if isinstance(group, Mapping):
        value = group.get("records") or group.get("constituents") or group.get("members") or group.get("symbols") or ()
        return value if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) else ()
    if isinstance(group, Sequence) and not isinstance(group, (str, bytes, bytearray)):
        return group
    return ()


def _filter_a_share_members(values: Sequence[Any]) -> tuple[list[Any], list[str]]:
    """Remove only Shanghai/Shenzhen B-share symbols from a member set.

    ``200xxx.SZ`` and ``900xxx.SH`` are B shares and are not part of the
    A-share trading candidate universe.  North-exchange ``.BJ`` symbols are
    deliberately retained because this module has no contract that excludes
    them.  The excluded symbols are returned for audit output instead of
    silently changing a coverage denominator.
    """

    included: list[Any] = []
    excluded: set[str] = set()
    for value in values:
        raw_symbol = value.get("symbol") if isinstance(value, Mapping) else value
        symbol = _normalize_symbol(raw_symbol)
        if symbol is None:
            # Invalid symbols are not tradable members and must not inflate a
            # coverage denominator.  Provider validation records the raw
            # pagination separately; this helper only builds the candidate
            # universe.
            continue
        if symbol is not None and _is_non_a_share_symbol(symbol):
            excluded.add(symbol)
            continue
        included.append(value)
    return included, sorted(excluded)


def _turnover(row: Any) -> float | None:
    return _number(_first(row, "turnover_cny", "amount_cny", "amount", "成交额", "tradeAmount", "turnover")) if isinstance(row, Mapping) else None


def _main_flow(row: Mapping[str, Any]) -> float | None:
    return _number(_first(row, "tencent_main_net_inflow_cny", "main_net_inflow_cny", "net_inflow_amount", "main_net", "mainNetIn"))


def _first(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping.get(key)
    return None


def _normalize_symbol(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if text.startswith(("SH", "SZ", "BJ")) and len(text) == 8:
        text = f"{text[2:]}.{text[:2]}"
    if "." in text:
        code, exchange = text.split(".", 1)
        return f"{code}.{exchange}" if re.fullmatch(r"\d{6}", code) and exchange in {"SH", "SZ", "BJ"} else None
    if not re.fullmatch(r"\d{6}", text):
        return None
    if text.startswith(("4", "8")):
        return f"{text}.BJ"
    return f"{text}.SH" if text.startswith(("5", "6", "9")) else f"{text}.SZ"


def _is_non_a_share_symbol(symbol: str) -> bool:
    """Return whether a normalized symbol is a Shanghai/Shenzhen B share."""

    try:
        code, exchange = str(symbol).upper().split(".", 1)
    except ValueError:
        return False
    return (exchange == "SZ" and code.startswith("200")) or (
        exchange == "SH" and code.startswith("900")
    )


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _coverage_value(value: Any) -> float | None:
    number = _number(value)
    if number is None:
        return None
    if number > 1.0:
        number /= 100.0
    return number if 0.0 <= number <= 1.0 else None


def _parse_date(value: Any, reason_code: str) -> date:
    parsed = _parse_date_optional(value)
    if parsed is None:
        raise RotationThemeConfigError(reason_code) if reason_code.startswith("ROTATION_THEME") else RotationThemeDataError(reason_code)
    return parsed


def _parse_date_optional(value: Any) -> date | None:
    if isinstance(value, datetime):
        return _aware(value).date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{8}", text):
        try:
            return datetime.strptime(text, "%Y%m%d").date()
        except ValueError:
            return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any, reason_code: str) -> datetime:
    parsed = _parse_datetime_optional(value)
    if parsed is None:
        raise RotationThemeDataError(reason_code)
    return parsed


def _parse_datetime_optional(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _aware(value)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=SHANGHAI)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        text = str(int(value))
    else:
        text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=SHANGHAI)
        except ValueError:
            pass
    try:
        return _aware(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _string_sequence(value: Any, *, allow_empty: bool = False) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        raise RotationThemeConfigError("ROTATION_THEME_EVIDENCE_INVALID")
    result = tuple(str(item).strip() for item in value if str(item).strip())
    return result if allow_empty or result else ()


def _content_hash(value: Mapping[str, Any]) -> str:
    body = dict(value)
    body.pop("content_hash", None)
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _safe_filename(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or ""))
    return text.strip("._") or "unknown"


__all__ = [
    "CHILD",
    "EASTMONEY_BOARD_SOURCE_ID",
    "EASTMONEY_BOARD_URL",
    "EastmoneyRotationCollector",
    "LIANGJIAN_ROTATION_THEME_V1",
    "MINIMUM_FACTOR_COVERAGE",
    "MEMBERSHIP_SNAPSHOT_SCHEMA",
    "PRIMARY",
    "ROTATION_THEME_CONFIG_SCHEMA",
    "ROTATION_THEME_SCHEMA",
    "ROTATION_THEME_SOURCE_ID",
    "ROTATION_THEME_TOP_N",
    "RotationTheme",
    "RotationThemeConfig",
    "RotationThemeConfigError",
    "RotationThemeDataError",
    "STRENGTH_WEIGHTS",
    "TENCENT_FLOW_SCHEMA",
    "TENCENT_FLOW_SOURCE_ID",
    "aggregate_tencent_theme_flows",
    "build_membership_snapshot",
    "build_rotation_theme_snapshot",
    "calculate_rotation_strength",
    "collect_eastmoney_board_catalog",
    "collect_eastmoney_board_flow",
    "collect_eastmoney_board_members",
    "collect_eastmoney_constituents",
    "collect_rotation_theme_snapshot",
    "collect_tencent_capital_flow",
    "load_latest_membership_snapshot",
    "load_membership_snapshot",
    "load_rotation_theme_snapshot",
    "load_rotation_theme_config",
    "normalize_tencent_flow_records",
    "persist_membership_snapshot",
    "persist_rotation_theme_snapshot",
    "read_rotation_theme_config",
    "unavailable_membership_snapshot",
    "unavailable_rotation_theme_snapshot",
    "validate_rotation_theme_config",
    "write_membership_snapshot",
    "write_rotation_theme_snapshot",
]
