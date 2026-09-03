"""Authoritative selected-board (801xxx) daily snapshot contract.

The existing industry/concept feeds use a different taxonomy and must not be
silently substituted for the app's selected-board strength table.  Until an
authenticated vendor endpoint is available, operators may place a captured
JSON export in this directory; malformed, stale or constituent-less snapshots
fail closed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
SELECTED_BOARD_SCHEMA = "ths-selected-board/1.0.0"
SELECTED_BOARD_SOURCE = "THS_SELECTED_BOARD_801"


def load_selected_board_snapshot(
    *,
    as_of: datetime,
    snapshot_dir: str | Path,
    expected_trade_date: date | None = None,
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    trade_day = expected_trade_date or cutoff.date()
    path = Path(snapshot_dir) / f"selected-board-{trade_day.isoformat()}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        result = normalize_selected_board_snapshot(payload, as_of=cutoff, expected_trade_date=trade_day)
    except FileNotFoundError:
        return _unavailable(cutoff, "SELECTED_BOARD_SNAPSHOT_MISSING", path)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return _unavailable(cutoff, "SELECTED_BOARD_SNAPSHOT_INVALID", path)
    return {**result, "cache_path": str(path)}


def normalize_selected_board_snapshot(
    payload: Any,
    *,
    as_of: datetime,
    expected_trade_date: date,
) -> dict[str, Any]:
    root = payload if isinstance(payload, Mapping) else {}
    if str(root.get("trade_date") or "") != expected_trade_date.isoformat():
        raise ValueError("SELECTED_BOARD_TRADE_DATE_MISMATCH")
    source_url = str(root.get("source_url") or "").strip()
    captured_at = _datetime(root.get("captured_at"))
    cutoff = _aware(as_of)
    if not source_url.startswith(("https://", "http://")):
        raise ValueError("SELECTED_BOARD_SOURCE_URL_MISSING")
    if (
        captured_at is None
        or captured_at > cutoff
        or captured_at.date() < expected_trade_date
    ):
        raise ValueError("SELECTED_BOARD_CAPTURE_TIME_INVALID")
    rows = root.get("boards")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)) or not rows:
        raise ValueError("SELECTED_BOARD_ROWS_MISSING")
    boards: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise ValueError("SELECTED_BOARD_ROW_INVALID")
        code = _board_code(raw.get("board_code"))
        name = str(raw.get("board_name") or "").strip()
        strength = _number(raw.get("strength"))
        net_inflow = _number(raw.get("main_net_inflow_cny"))
        members = sorted({_symbol(value) for value in raw.get("constituents", ()) if _symbol(value)})
        if not (code and name and strength is not None and net_inflow is not None):
            raise ValueError("SELECTED_BOARD_ROW_INVALID")
        parent_code = _board_code(raw.get("parent_board_code")) if raw.get("parent_board_code") else None
        boards.append({
            "board_code": code,
            "board_name": name,
            "strength": strength,
            "main_net_inflow_cny": net_inflow,
            "parent_board_code": parent_code,
            "constituents": members,
        })
    if len({row["board_code"] for row in boards}) != len(boards):
        raise ValueError("SELECTED_BOARD_CODE_DUPLICATED")

    by_code = {row["board_code"]: row for row in boards}
    positive = [row for row in boards if row["main_net_inflow_cny"] > 0]
    primary = [row for row in positive if not row["parent_board_code"]]
    primary.sort(key=lambda row: (-row["strength"], -row["main_net_inflow_cny"], row["board_code"]))
    top_primary = primary[:3]
    selected_codes = {row["board_code"] for row in top_primary}
    # A child theme, such as liquid cooling under computing power, belongs to
    # its selected parent and does not consume another primary top-three slot.
    selected_codes.update(
        row["board_code"]
        for row in positive
        if row["parent_board_code"] in selected_codes
    )
    primary_rank = {row["board_code"]: index for index, row in enumerate(top_primary, start=1)}
    for row in boards:
        parent = row["parent_board_code"]
        row["selected_for_rotation"] = row["board_code"] in selected_codes
        row["primary_rank"] = primary_rank.get(row["board_code"]) or primary_rank.get(parent)
        row["is_child_board"] = bool(parent)
        if parent and parent not in by_code:
            raise ValueError("SELECTED_BOARD_PARENT_MISSING")
        # A complete strength ranking need not duplicate constituents for
        # every non-selected board.  The three selected primaries and their
        # selected positive-flow children must, however, have a full member
        # list or the trend channel cannot be built.
        if row["selected_for_rotation"] and not row["constituents"]:
            raise ValueError("SELECTED_BOARD_CONSTITUENTS_MISSING")

    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in boards:
        for symbol in row["constituents"]:
            by_symbol.setdefault(symbol, []).append({
                key: row[key]
                for key in (
                    "board_code", "board_name", "strength", "main_net_inflow_cny",
                    "parent_board_code", "selected_for_rotation", "primary_rank", "is_child_board",
                )
            })
    canonical = sorted(boards, key=lambda row: (-row["strength"], row["board_code"]))
    content_hash = hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": SELECTED_BOARD_SCHEMA,
        "source_id": SELECTED_BOARD_SOURCE,
        "available": True,
        "reason_code": "OK",
        "as_of": cutoff.isoformat(),
        "captured_at": captured_at.isoformat(),
        "source_url": source_url,
        "trade_date": expected_trade_date.isoformat(),
        "boards": canonical,
        "selected_primary_boards": [
            {"board_code": row["board_code"], "board_name": row["board_name"], "rank": primary_rank[row["board_code"]]}
            for row in top_primary
        ],
        "by_symbol": by_symbol,
        "content_hash": content_hash,
        "ranking_board_count": len(boards),
        "selected_board_count": len(selected_codes),
        "selected_constituent_board_count": sum(
            bool(row["constituents"]) for row in boards if row["selected_for_rotation"]
        ),
        "taxonomy_substitution_forbidden": True,
    }


def _unavailable(as_of: datetime, reason: str, path: Path) -> dict[str, Any]:
    return {
        "schema_version": SELECTED_BOARD_SCHEMA,
        "source_id": SELECTED_BOARD_SOURCE,
        "available": False,
        "reason_code": reason,
        "as_of": _aware(as_of).isoformat(),
        "boards": [],
        "selected_primary_boards": [],
        "by_symbol": {},
        "cache_path": str(path),
        "taxonomy_substitution_forbidden": True,
    }


def _symbol(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if "." in text:
        code, suffix = text.split(".", 1)
        return f"{code}.{suffix}" if len(code) == 6 and code.isdigit() and suffix in {"SH", "SZ", "BJ"} else None
    if len(text) != 6 or not text.isdigit():
        return None
    if text.startswith(("4", "8")):
        return f"{text}.BJ"
    return f"{text}.SH" if text.startswith(("5", "6", "9")) else f"{text}.SZ"


def _board_code(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if text.endswith("k"):
        text = text[:-1]
    return text if len(text) == 6 and text.startswith("801") and text.isdigit() else None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)
