#!/usr/bin/env python3
"""Build a non-publishable rotation snapshot from reviewed market evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from liangjian_funnel.reporting import atomic_write_json


ROTATION_SCHEMA = "liangjian-rotation-theme/1.0.0"
ROTATION_SOURCE = "LIANGJIAN_FREE_ROTATION_V1"
OBSERVATION_SCHEMA = "liangjian-rotation-observation/1.0.0"


def _canonical_hash(value: Mapping[str, Any]) -> str:
    body = dict(value)
    body.pop("content_hash", None)
    encoded = json.dumps(
        body,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SystemExit(f"JSON_OBJECT_REQUIRED:{path}")
    return value


def _finite_number(value: Any, reason: str) -> float:
    if value is None or isinstance(value, bool):
        raise SystemExit(reason)
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SystemExit(reason) from exc
    if not math.isfinite(parsed):
        raise SystemExit(reason)
    return parsed


def build_overlay(
    evidence: Mapping[str, Any],
    membership: Mapping[str, Any],
) -> dict[str, Any]:
    if evidence.get("schema_version") != OBSERVATION_SCHEMA:
        raise SystemExit("ROTATION_OBSERVATION_SCHEMA_INVALID")
    trade_day = date.fromisoformat(str(evidence.get("trade_date") or ""))
    observed_as_of = datetime.fromisoformat(str(evidence.get("observed_as_of") or ""))
    if observed_as_of.tzinfo is None or observed_as_of.date() != trade_day:
        raise SystemExit("ROTATION_OBSERVATION_TIME_INVALID")
    if evidence.get("retrospective_validation_only") is not True:
        raise SystemExit("ROTATION_OBSERVATION_MUST_BE_NON_PUBLISHABLE")
    if membership.get("schema_version") != ROTATION_SCHEMA:
        raise SystemExit("ROTATION_MEMBERSHIP_SCHEMA_INVALID")
    expected_membership_hash = str(membership.get("content_hash") or "")
    if not expected_membership_hash or expected_membership_hash != _canonical_hash(membership):
        raise SystemExit("ROTATION_MEMBERSHIP_HASH_INVALID")
    membership_boards = {
        str(row.get("theme_id") or row.get("board_code") or "").strip().upper(): row
        for row in membership.get("boards", ())
        if isinstance(row, Mapping)
        and str(row.get("theme_id") or row.get("board_code") or "").strip()
    }
    observations = evidence.get("observations")
    if not isinstance(observations, Sequence) or isinstance(
        observations, (str, bytes, bytearray)
    ):
        raise SystemExit("ROTATION_OBSERVATIONS_INVALID")
    limit = int(evidence.get("rotation_theme_count") or 0)
    if limit < 1:
        raise SystemExit("ROTATION_THEME_COUNT_INVALID")
    rows: list[dict[str, Any]] = []
    slot_ranks: set[int] = set()
    for raw in observations:
        if not isinstance(raw, Mapping):
            raise SystemExit("ROTATION_OBSERVATION_ROW_INVALID")
        theme_id = str(raw.get("theme_id") or "").strip().upper()
        member_row = membership_boards.get(theme_id)
        if not isinstance(member_row, Mapping):
            raise SystemExit(f"ROTATION_MEMBERSHIP_THEME_MISSING:{theme_id}")
        constituents = sorted({str(value).strip().upper() for value in member_row.get("constituents", ()) if str(value).strip()})
        if not constituents:
            raise SystemExit(f"ROTATION_MEMBERSHIP_EMPTY:{theme_id}")
        rank = int(raw.get("primary_rank") or 0)
        if not 1 <= rank <= limit:
            raise SystemExit(f"ROTATION_PRIMARY_RANK_INVALID:{theme_id}")
        if raw.get("occupies_primary_slot") is True:
            if rank in slot_ranks:
                raise SystemExit(f"ROTATION_PRIMARY_RANK_DUPLICATED:{rank}")
            slot_ranks.add(rank)
        net_flow = _finite_number(raw.get("main_net_inflow_cny"), f"ROTATION_FLOW_INVALID:{theme_id}")
        if net_flow <= 0:
            raise SystemExit(f"ROTATION_SELECTED_FLOW_NON_POSITIVE:{theme_id}")
        row = dict(member_row)
        row.update({
            "board_code": theme_id,
            "theme_id": theme_id,
            "board_name": str(member_row.get("board_name") or theme_id),
            "strength": _finite_number(raw.get("strength"), f"ROTATION_STRENGTH_INVALID:{theme_id}"),
            "main_net_inflow_cny": net_flow,
            "tencent_main_net_inflow_cny": net_flow,
            "selected_for_rotation": True,
            "primary_rank": rank,
            "constituents": constituents,
            "member_count": len(constituents),
            "source_board_codes": list(raw.get("source_board_codes") or ()),
            "source_board_names": list(raw.get("source_board_names") or ()),
            "observation_mapping_note": raw.get("mapping_note"),
            "observed_strength_source": "USER_PROVIDED_SELECTED_BOARD_SCREENSHOT",
        })
        rows.append(row)
    if slot_ranks != set(range(1, limit + 1)):
        raise SystemExit("ROTATION_PRIMARY_RANKS_INCOMPLETE")

    rows.sort(key=lambda row: (int(row["primary_rank"]), -float(row["strength"]), row["theme_id"]))
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for symbol in row["constituents"]:
            by_symbol[symbol].append({
                "board_code": row["theme_id"],
                "board_name": row["board_name"],
                "theme_id": row["theme_id"],
                "theme_level": row.get("theme_level"),
                "parent_theme_id": row.get("parent_theme_id"),
                "strategy_theme_id": row.get("strategy_theme_id") or row["theme_id"],
                "strength": row["strength"],
                "main_net_inflow_cny": row["main_net_inflow_cny"],
                "selected_for_rotation": True,
                "primary_rank": row["primary_rank"],
                "is_child_board": row.get("is_child_board") is True,
            })
    for symbol in by_symbol:
        by_symbol[symbol].sort(key=lambda row: (int(row["primary_rank"]), row["board_code"]))
    # Rebuild from the evidence flag by theme rather than relying on row zip
    # identity when a child shares its parent's rank.
    occupying = {
        str(raw.get("theme_id") or "").strip().upper()
        for raw in observations
        if isinstance(raw, Mapping) and raw.get("occupies_primary_slot") is True
    }
    selected_primary = [
        {
            "board_code": row["theme_id"],
            "board_name": row["board_name"],
            "theme_id": row["theme_id"],
            "strategy_theme_id": row.get("strategy_theme_id") or row["theme_id"],
            "rank": int(row["primary_rank"]),
            "strength": row["strength"],
            "main_net_inflow_cny": row["main_net_inflow_cny"],
        }
        for row in rows
        if row["theme_id"] in occupying
    ]
    payload: dict[str, Any] = {
        "schema_version": ROTATION_SCHEMA,
        "source_id": ROTATION_SOURCE,
        "available": True,
        "reason_code": "OK_RETROSPECTIVE_USER_EVIDENCE",
        "trade_date": trade_day.isoformat(),
        "captured_at": observed_as_of.isoformat(),
        "boards": rows,
        "selected_primary_boards": selected_primary,
        "by_symbol": dict(sorted(by_symbol.items())),
        "coverage": {
            "board_count": len(rows),
            "selected_primary_count": len(selected_primary),
            "member_count": len(by_symbol),
            "rotation_theme_count": limit,
        },
        "quality": {
            "ranking_source": "USER_PROVIDED_SELECTED_BOARD_SCREENSHOT",
            "membership_contract": "VERSIONED_FULL_MARKET_SYMBOL_MEMBERSHIP",
            "membership_snapshot_hash": expected_membership_hash,
            "membership_snapshot_trade_date": membership.get("trade_date"),
            "membership_known_after_target_date": str(membership.get("trade_date")) > trade_day.isoformat(),
            "retrospective_validation_only": True,
            "production_publish_forbidden": True,
            "source_evidence": list(evidence.get("source_evidence") or ()),
            "ranking_rule": evidence.get("ranking_rule"),
            "ranking_proof": dict(evidence.get("ranking_proof") or {}),
        },
        "taxonomy_substitution_forbidden": False,
    }
    payload["content_hash"] = _canonical_hash(payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--membership-snapshot", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    evidence = _load_object(Path(args.evidence))
    membership = _load_object(Path(args.membership_snapshot))
    output = build_overlay(evidence, membership)
    atomic_write_json(Path(args.output), output)
    print(json.dumps({
        "output": str(Path(args.output)),
        "content_hash": output["content_hash"],
        "selected_primary_count": len(output["selected_primary_boards"]),
        "selected_board_count": len(output["boards"]),
        "member_count": len(output["by_symbol"]),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
