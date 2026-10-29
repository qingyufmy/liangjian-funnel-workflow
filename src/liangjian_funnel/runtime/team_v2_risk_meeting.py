"""Pure A6/PM tightening rules; no database, model, order or notification I/O.

Evidence/session schema validation belongs at the caller edge. This boundary
rechecks domain and monotonicity even after successful model JSON validation.
The returned utilization cap must still be enforced by portfolio allocation.
"""
from collections.abc import Mapping
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any

from .risk_engine import RiskEnvelope, conservative_envelope, cap_risk_envelope

_ADJUSTMENTS = frozenset({"KEEP", "TIGHTEN_TO_PROBE", "REDUCE_BATCHES", "SUSPEND"})
_ADJUSTMENT_KEYS = frozenset({"plan_id", "adjustment", "reason_code", "supporting_refs",
                            "opposing_refs", "opposing_search_scope", "what_would_change_my_mind", "gap_review"})
_STANCE = {"HOLD": 0, "REDUCE_TOMORROW": 1, "EXIT_TOMORROW": 2}


@dataclass(frozen=True)
class RiskMeetingResult:
    status: str
    envelopes: tuple[RiskEnvelope, ...]
    utilization_cap: Decimal
    consecutive_failures: int
    reason_codes: tuple[str, ...]


def _cap(value: Any) -> Decimal:
    if isinstance(value, bool): raise ValueError("A6_CAP_INVALID")
    try:
        number = Decimal(str(value))
    except (ValueError, InvalidOperation) as exc:
        raise ValueError("A6_CAP_INVALID") from exc
    if not number.is_finite() or not 0 <= number <= 100:
        raise ValueError("A6_CAP_INVALID")
    return number


def apply_premarket_review(envelopes: tuple[RiskEnvelope, ...], output: Any, *,
                           engine_utilization_cap: Decimal, validated: bool,
                           previous_failures: int) -> RiskMeetingResult:
    """All original envelopes remain authoritative; an invalid meeting degrades.

    previous_failures is owned by the per-account/date runtime, not the model.
    No new risk budget, quantity, price or arbitrary batch count is model-owned.
    """
    if type(previous_failures) is not int or previous_failures < 0 or type(validated) is not bool:
        raise ValueError("A6_MEETING_STATE_INVALID")
    cap = _cap(engine_utilization_cap)
    envelopes = tuple(cap_risk_envelope(e) for e in envelopes)
    by_id = {e.plan_id: e for e in envelopes}
    if len(by_id) != len(envelopes) or len({e.trade_date for e in envelopes}) > 1:
        raise ValueError("A6_ENGINE_DOMAIN_INVALID")
    if previous_failures >= 2:
        return RiskMeetingResult("HALT", tuple(conservative_envelope(e, consecutive_failures=previous_failures)
            for e in envelopes), cap, previous_failures, ("A6_DEGRADED_HALT",))
    try:
        if not validated or not isinstance(output, Mapping) or output.get("session") != "PREMARKET":
            raise ValueError("A6_REVIEW_INVALID")
        value = output.get("utilization_cap")
        if not isinstance(value, Mapping): raise ValueError("A6_CAP_INVALID")
        model_cap = _cap(value.get("cap_pct"))
        if model_cap > cap: raise ValueError("A6_LOOSEN_FORBIDDEN")
        rows = output.get("envelope_adjustments")
        if not isinstance(rows, list): raise ValueError("A6_ENVELOPE_DOMAIN_INVALID")
        adjustments = {}
        for row in rows:
            if (not isinstance(row, Mapping) or not set(row).issubset(_ADJUSTMENT_KEYS)
                    or not isinstance(row.get("plan_id"), str) or row["plan_id"] not in by_id
                    or row["plan_id"] in adjustments or not isinstance(row.get("adjustment"), str)
                    or row["adjustment"] not in _ADJUSTMENTS):
                raise ValueError("A6_ENVELOPE_DOMAIN_INVALID")
            if 'gap_review' in row and (not isinstance(row['gap_review'],str) or row['gap_review'] not in {'KEEP_WITH_GAP','DOWNGRADE','REJECT'}):
                raise ValueError('A6_GAP_REVIEW_INVALID')
            adjustments[row["plan_id"]] = row["adjustment"]
        if set(adjustments) != set(by_id): raise ValueError("A6_ENVELOPE_DOMAIN_INVALID")
    except ValueError:
        count = previous_failures + 1
        blocked = tuple(conservative_envelope(e, consecutive_failures=count) for e in envelopes)
        return RiskMeetingResult("HALT" if count >= 2 else "DEGRADED", blocked, cap, count,
                                 ("A6_DEGRADED_HALT",) if count >= 2 else ("A6_REVIEW_DEGRADED",))

    result = []
    for original in envelopes:
        adjustment = adjustments[original.plan_id]
        if adjustment == "SUSPEND":
            changed = replace(original, eligible=False, risk_unit="NONE", risk_budget=Decimal(0),
                max_batches=1, max_shares_cap=0, a6_tightened=True,
                blocked_reasons=tuple(dict.fromkeys((*original.blocked_reasons, "A6_SUSPEND", *(("CAPACITY_TRIMMED",) if next(row for row in rows if row["plan_id"]==original.plan_id).get("reason_code")=="CAPACITY_TRIMMED" else ())))))
        elif adjustment == "TIGHTEN_TO_PROBE":
            changed = conservative_envelope(original, consecutive_failures=1)
        elif adjustment == "REDUCE_BATCHES":
            changed = replace(original, max_batches=1, a6_tightened=True)
        else:
            changed = original
        # Defense in depth if any future adjustment implementation changes.
        if (changed.max_shares_cap > original.max_shares_cap or changed.risk_budget > original.risk_budget
                or changed.max_batches > original.max_batches or (changed.eligible and not original.eligible)
                or not set(original.blocked_reasons).issubset(changed.blocked_reasons)):
            raise ValueError("A6_MONOTONICITY_VIOLATION")
        result.append(cap_risk_envelope(changed))
    return RiskMeetingResult("READY", tuple(result), model_cap, 0, ())


def review_overnight(pm_stances: Mapping[str, str], rows: Any) -> tuple[dict, ...]:
    """Return appendable pm_override evidence, never modify the PM source rows."""
    if (any(not isinstance(symbol, str) or stance not in tuple(_STANCE) for symbol, stance in pm_stances.items())
            or not isinstance(rows, list)):
        raise ValueError("A6_OVERNIGHT_DOMAIN_INVALID")
    by_symbol = {}
    for row in rows:
        if (not isinstance(row, Mapping) or not isinstance(row.get("symbol"), str)
                or row["symbol"] not in pm_stances or row["symbol"] in by_symbol
                or row.get("pm_stance") != pm_stances[row["symbol"]]):
            raise ValueError("A6_OVERNIGHT_DOMAIN_INVALID")
        original = pm_stances[row["symbol"]]
        action = row.get("review")
        stance = (original if action == "AGREE" else "REDUCE_TOMORROW" if action == "OVERRIDE_TO_REDUCE"
                  else "EXIT_TOMORROW" if action == "OVERRIDE_TO_EXIT" else None)
        if stance is None or _STANCE[stance] < _STANCE[original]:
            raise ValueError("A6_OVERNIGHT_LOOSENING_FORBIDDEN")
        by_symbol[row["symbol"]] = {"symbol": row["symbol"], "pm_stance": original, "stance": stance,
                                    "overridden": stance != original}
    if set(by_symbol) != set(pm_stances): raise ValueError("A6_OVERNIGHT_DOMAIN_INVALID")
    return tuple(by_symbol[symbol] for symbol in pm_stances)
