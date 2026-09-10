"""Explain rotation coverage with frozen identities; never add candidates."""
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any


def rotation_coverage(snapshot: Mapping[str, Any], decisions: Sequence[Mapping[str, Any]],
                      output: Mapping[str, Any]) -> dict[str, Any]:
    source = snapshot.get("SELECTED_BOARD_SNAPSHOT") or {}
    if not isinstance(source, Mapping):
        source = {}
    membership = source.get("by_symbol") or {}
    selected = source.get("selected_primary_boards") or []
    decision_by_symbol = {str(r.get("symbol")): r for r in decisions}
    pools = {k: {str(r.get("symbol")) for r in output.get(k, []) if isinstance(r, Mapping)}
             for k in ("focus_pool", "watch_only_pool")}
    rows = []
    for board in selected:
        code = board.get("board_code") or board.get("theme_id")
        members = {symbol for symbol, bindings in membership.items()
                   if any(isinstance(b, Mapping) and (b.get("board_code") or b.get("theme_id")) == code
                          for b in bindings)}
        evaluated = members & decision_by_symbol.keys()
        local = [decision_by_symbol[s] for s in sorted(evaluated)]
        rows.append({"board_code": code, "board_name": board.get("board_name"), "rank": board.get("rank"),
            "mapped_market_member_count": len(members), "a1_input_count": len(evaluated),
            "quant_review_count": sum(r.get("status") == "REVIEW_CANDIDATE" for r in local),
            "focus_count": len(evaluated & pools["focus_pool"]),
            "watch_count": len(evaluated & pools["watch_only_pool"]),
            "behavior_counts": dict(Counter(str(r.get("stock_behavior_type") or "UNRESOLVED") for r in local)),
            "quant_status_counts": dict(Counter(str(r.get("status") or "UNKNOWN") for r in local)),
            "reason": "BOARD_MEMBERSHIP_MISSING" if not members else "A1_HAS_NO_BOARD_MEMBERS" if not evaluated else "TRACEABLE"})
    return {"schema_version": "a2-rotation-coverage/1", "source_id": source.get("source_id"),
        "trade_date": source.get("trade_date"), "source_available": source.get("available") is True,
        "requested_top_n": snapshot.get("A2_ROTATION_THEME_COUNT", 5), "selected_board_count": len(selected),
        "evaluated_a1_count": len(decision_by_symbol), "boards": rows,
        "scope": "FROZEN_BOARD_MEMBERSHIP_NOT_A_NEW_RANKING", "changes_candidate_permissions": False}
