"""Tony 2026-10-14 decision 20: strength TOP8 first, then evidenced flow.

An overlay, never a rewrite of the original provider snapshot. No backfill
from rank nine when one of the first eight fails funds/quality checks.
"""
from collections.abc import Mapping
from copy import deepcopy
import math
import hashlib
import json

POLICY_ID = "TONY_20261014_TOP8_FLOW_TODAY_OR_3D"
TOP_N = 8


def authorized_rotation_gate_empty(source: Mapping) -> bool:
    """No admitted board globally, not merely no matching current candidate."""
    return bool(source.get("available") is True and not any(
        isinstance(row, Mapping) and row.get("selected_for_rotation") is True
        for key in ("boards", "selected_primary_boards")
        for row in source.get(key) or []))


def number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def positive_rotation_flow(row: Mapping) -> bool:
    today = number(row.get("main_net_inflow_cny", row.get("tencent_main_net_inflow_cny")))
    if today is not None and today > 0:
        return True
    mean = number(row.get("main_net_inflow_3d_mean_cny"))
    evidence = row.get("fund_flow_history_evidence") or {}
    return bool(today is not None and mean is not None and mean > 0 and isinstance(evidence, Mapping)
                and evidence.get("consecutive_sessions") == 3
                and evidence.get("hash_verified") is True
                and evidence.get("same_basket") is True)


def authorized_rotation_policy(source: Mapping) -> dict:
    result = deepcopy(dict(source))
    if source.get("available") is not True:
        return result
    boards = result.get("boards") or []
    parents = {str(row.get("theme_id") or row.get("board_code")): row for row in boards
               if isinstance(row, Mapping)}
    def family(row):
        return str(row.get("parent_theme_id") or row.get("parent_board_code")
                   or row.get("theme_id") or row.get("board_code"))
    ranked = sorted((row for row in boards if number(row.get("strength")) is not None),
                    key=lambda row: (-number(row["strength"]), family(row),
                                     str(row.get("board_code") or row.get("theme_id"))))
    representatives = []
    families = set()
    for row in ranked:
        fam = family(row)
        if fam in families or fam not in parents:
            continue
        families.add(fam); representatives.append(row)
    rank_by_family = {family(row): index for index, row in enumerate(representatives, 1)}
    selected_by_family = {}
    by_symbol = {}
    compact_fields = ("board_code", "board_name", "theme_id", "theme_level", "parent_theme_id",
        "parent_board_code", "strategy_theme_id", "strength", "main_net_inflow_cny",
        "main_net_inflow_3d_mean_cny", "fund_flow_history_evidence", "selected_for_rotation", "primary_rank",
        "rotation_reserve_rank", "rotation_reserve_scope", "is_child_board", "membership_source_id",
        "membership_source_board_id", "membership_reference_hash", "membership_basis", "membership_components")
    for row in boards:
        rank = rank_by_family.get(family(row))
        today = number(row.get("main_net_inflow_cny", row.get("tencent_main_net_inflow_cny")))
        east = number(row.get("eastmoney_main_net_inflow_cny"))
        sources_conflict = today is not None and east is not None and today * east < 0
        quality = (row.get("member_snapshot_complete", True) is True
                   and number(row.get("effective_fund_coverage", 1)) is not None
                   and number(row.get("effective_fund_coverage", 1)) >= .8
                   and number(row.get("price_coverage", 1)) is not None
                   and number(row.get("price_coverage", 1)) >= .9
                   and number(row.get("factor_coverage", 1)) is not None
                   and number(row.get("factor_coverage", 1)) >= .5
                   and not sources_conflict)
        row["primary_rank"] = rank
        row["selected_for_rotation"] = bool(rank and rank <= TOP_N and quality and positive_rotation_flow(row))
        row["rotation_gate_policy"] = POLICY_ID
        if row["selected_for_rotation"]:
            current = selected_by_family.get(family(row))
            if current is None or number(row.get("strength")) > number(current.get("strength")):
                selected_by_family[family(row)] = row
        for symbol in row.get("constituents") or []:
            # Do not repeat a whole membership basket for every constituent.
            by_symbol.setdefault(str(symbol), []).append({key:row.get(key) for key in compact_fields if key in row})
    selected = sorted(selected_by_family.values(), key=lambda row: row["primary_rank"])
    result.update(boards=boards, by_symbol=by_symbol, selected_primary_boards=[dict(row) for row in selected],
                  rotation_theme_count=TOP_N, rotation_gate_policy=POLICY_ID,
                  policy_source_content_hash=source.get("content_hash"))
    result.pop("content_hash", None)
    result["content_hash"] = hashlib.sha256(json.dumps(result, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return result
