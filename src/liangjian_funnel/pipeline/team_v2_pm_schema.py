"""A3-PM authored session schemas, against server-owned frozen domains.

Validation does not repair JSON, activate plans, reserve slots, or place orders.
Allocation rows for spare eligible plans are allowed, but every slotted plan must
have one allocation. Neither a model cluster nor its copied baseline is trusted.
"""
from collections import Counter
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
import json
from typing import Any

from .team_v2_contract import TeamValidation, validate_team_output
from ..runtime.risk_engine import RiskEnvelope

_REFS = frozenset(("supporting_refs", "opposing_refs"))
_OPTIONAL = frozenset(("opposing_search_scope",))
_COMMON = frozenset(("envelope", "session", "handoff_notes", "what_would_change_my_mind"))
_PRE = _COMMON | {"deployment_target", "slot_plan", "plan_allocations", "shortfall_reason"}
_RANKING_PRE = (_PRE - {"slot_plan"}) | {"plan_ranking"}
PM_PRIMARY_CLUSTER_CAP = 2
_CLOSE = _COMMON | {"overnight_stances", "capacity_request", "unresolved_questions"}


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _number(value):
    if isinstance(value, bool): return None
    try: result = Decimal(str(value))
    except (ValueError, InvalidOperation): return None
    return result if result.is_finite() else None


def _same(left, right):
    try:
        return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(right, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError): return False


def _item(row, fields):
    return isinstance(row, Mapping) and fields.issubset(row) and set(row).issubset(fields | _OPTIONAL)


def _root(output, catalog, *, session, keys, required_envelope, recipients):
    if not isinstance(output, Mapping):
        return ["PM_RESPONSE_OBJECT_REQUIRED"], TeamValidation(False, (), (), 0)
    common = validate_team_output("A3", output, catalog, session=session)
    issues = list(common.issues)
    if set(output) != keys: issues.append("PM_ROOT_FIELDS_INVALID")
    if not _same(output.get("envelope"), required_envelope): issues.append("PM_ENVELOPE_BINDING_MISMATCH")
    notes = output.get("handoff_notes")
    if not isinstance(notes, Mapping) or set(notes) != recipients:
        issues.append("PM_HANDOFF_RECIPIENT_INVALID")
    return issues, common


def _result(issues, common):
    return TeamValidation(not issues, tuple(dict.fromkeys(issues)), common.judgments, common.sentinel_count)


def validate_pm_slots(output: Any, catalog: Any, *, required_envelope: Mapping,
                          envelopes: tuple[RiskEnvelope, ...], plan_clusters: Mapping[str, str],
                          free_slots: int, baseline_range: tuple[Decimal, Decimal],
                          utilization_cap: Decimal, high_conviction_ids: set[str]) -> TeamValidation:
    by_id = {row.plan_id: row for row in envelopes}
    low, high = map(_number, baseline_range)
    cap = _number(utilization_cap)
    if (len(by_id) != len(envelopes) or set(by_id) != set(plan_clusters)
            or any(not _text(value) for value in plan_clusters.values())
            or type(free_slots) is not int or free_slots < 0
            or low is None or high is None or cap is None or not 0 <= low <= high <= cap <= 100
            or not high_conviction_ids.issubset(by_id)):
        raise ValueError("PM_SERVER_CONTEXT_INVALID")
    issues, common = _root(output, catalog, session="PM_PREMARKET", keys=_PRE,
        required_envelope=required_envelope, recipients={"to_a4", "to_a6"})
    if not isinstance(output, Mapping): return _result(issues, common)
    target = output.get("deployment_target")
    if not _item(target, {"target_pct", "baseline_range", "basis"} | _REFS):
        issues.append("PM_DEPLOYMENT_TARGET_INVALID")
    else:
        pct = _number(target["target_pct"]); copied = target["baseline_range"]
        if (pct is None or not low <= pct <= high or not _text(target["basis"])
                or not isinstance(copied, list) or len(copied) != 2
                or [_number(value) for value in copied] != [low, high]):
            issues.append("PM_DEPLOYMENT_TARGET_INVALID")
    eligible = {key for key, row in by_id.items() if row.eligible and row.risk_unit in ("STANDARD", "PROBE")}
    slots = output.get("slot_plan"); allocations = output.get("plan_allocations")
    used: list[str] = []; primaries: list[str] = []; slot_numbers: list[int] = []; orders: list[int] = []
    if not isinstance(slots, list):
        issues.append("PM_SLOT_POOL_INVALID"); slots = []
    if len(slots) > free_slots: issues.append("PM_SLOT_CAP_EXCEEDED")
    for row in slots:
        if not _item(row, {"slot", "primary_plan_id", "backup_plan_id", "cluster", "accept_order"} | _REFS):
            issues.append("PM_SLOT_FIELDS_INVALID"); continue
        number, order = row["slot"], row["accept_order"]
        if (type(number) is not int or not 1 <= number <= free_slots
                or type(order) is not int or order < 1):
            issues.append("PM_SLOT_ORDER_INVALID")
        else:
            slot_numbers.append(number); orders.append(order)
        primary, backup = row["primary_plan_id"], row["backup_plan_id"]
        if (not isinstance(primary, str) or primary not in eligible
                or not isinstance(backup, str) or (backup and backup not in eligible)):
            issues.append("PM_SLOT_DOMAIN_INVALID"); continue
        used.append(primary); primaries.append(primary)
        if backup: used.append(backup)
        if row["cluster"] != plan_clusters[primary]: issues.append("PM_CLUSTER_BINDING_MISMATCH")
    if any(len(values) != len(set(values)) for values in (used, slot_numbers, orders)):
        issues.append("PM_DUPLICATE_SLOT_BINDING")
    if any(count > PM_PRIMARY_CLUSTER_CAP for count in Counter(plan_clusters[key] for key in primaries).values()):
        issues.append("PM_PRIMARY_CLUSTER_CAP_EXCEEDED")
    allocated: list[str] = []
    if not isinstance(allocations, list):
        issues.append("PM_ALLOCATION_POOL_INVALID"); allocations = []
    for row in allocations:
        if not _item(row, {"plan_id", "intended_risk_unit", "max_batches", "conviction", "reason_code"} | _REFS):
            issues.append("PM_ALLOCATION_FIELDS_INVALID"); continue
        key = row["plan_id"]
        if not isinstance(key, str) or key not in eligible:
            issues.append("PM_ALLOCATION_DOMAIN_INVALID"); continue
        allocated.append(key); envelope = by_id[key]
        if (row["intended_risk_unit"] not in ("STANDARD", "PROBE")
                or (envelope.risk_unit == "PROBE" and row["intended_risk_unit"] != "PROBE")
                or type(row["max_batches"]) is not int or not 1 <= row["max_batches"] <= envelope.max_batches
                or row["conviction"] not in ("NORMAL", "HIGH_CONVICTION") or not _text(row["reason_code"])):
            issues.append("PM_ALLOCATION_ABOVE_ENVELOPE")
        if row["conviction"] == "HIGH_CONVICTION" and (key not in high_conviction_ids or envelope.a6_tightened):
            issues.append("PM_HIGH_CONVICTION_NOT_ELIGIBLE")
    if len(allocated) != len(set(allocated)) or not set(used).issubset(allocated):
        issues.append("PM_ALLOCATION_COVERAGE_INVALID")
    shortfall = output.get("shortfall_reason")
    if shortfall not in ("NONE", "NO_ELIGIBLE_PLANS", "CLUSTER_CAP", "ENVELOPE_TIGHTENED", "REGIME", "EXTERNAL_RISK_OFF"):
        issues.append("PM_SHORTFALL_REASON_INVALID")
    if not eligible and shortfall == "NONE": issues.append("PM_SHORTFALL_REASON_REQUIRED")
    return _result(issues, common)


def derive_pm_slots(ranking, *, envelopes, plan_clusters, free_slots):
    """0304: ranking is a model opinion; only this engine constructs slots."""
    eligible={e.plan_id for e in envelopes if e.eligible and e.risk_unit in ('STANDARD','PROBE')}
    if not isinstance(ranking,list) or type(free_slots) is not int or free_slots<0:
        raise ValueError('PM_RANKING_INVALID')
    seen=set();ordered=[]
    for index,row in enumerate(ranking,1):
        if not _item(row,{'rank','plan_id','cluster'}|_REFS):raise ValueError('PM_RANKING_FIELDS_INVALID')
        pid=row['plan_id']
        if not isinstance(pid,str) or pid not in eligible:raise ValueError('PM_RANKING_DOMAIN_INVALID')
        if pid in seen:raise ValueError('PM_RANKING_DUPLICATE_ID')
        if type(row['rank']) is not int or row['rank']!=index:raise ValueError('PM_RANKING_ORDER_INVALID')
        if row['cluster']!=plan_clusters.get(pid):raise ValueError('PM_CLUSTER_BINDING_MISMATCH')
        seen.add(pid);ordered.append(row)
    counts=Counter();primary=[]
    for row in ordered:
        if len(primary)<free_slots and counts[row['cluster']]<PM_PRIMARY_CLUSTER_CAP:
            primary.append(row);counts[row['cluster']]+=1
    primary_ids={r['plan_id'] for r in primary}
    backup=[r for r in ordered if r['plan_id'] not in primary_ids]
    slots=[dict(slot=i,primary_plan_id=row['plan_id'],
        backup_plan_id=backup[i-1]['plan_id'] if i<=len(backup) else '',
        cluster=row['cluster'],accept_order=i,
        **{k:row[k] for k in _REFS|_OPTIONAL if k in row}) for i,row in enumerate(primary,1)]
    used=[pid for row in slots for pid in (row['primary_plan_id'],row['backup_plan_id']) if pid]
    if len(used)!=len(set(used)):raise ValueError('PM_DUPLICATE_SLOT_BINDING')
    if any(n>PM_PRIMARY_CLUSTER_CAP for n in counts.values()):raise ValueError('PM_PRIMARY_CLUSTER_CAP_EXCEEDED')
    reason=('NONE' if len(primary)==free_slots else 'NO_ELIGIBLE_PLANS' if not eligible
        else 'NO_ACCEPTED_PLANS' if not ordered else 'CLUSTER_CAP' if backup else 'RANKING_EXHAUSTED')
    return dict(slot_plan=slots,derived_shortfall=reason)


def validate_pm_premarket(output: Any, catalog: Any, **context) -> TeamValidation:
    # Reuse every original numeric/allocation assertion against the engine's
    # output. The model can no longer supply a slot_plan or derived_shortfall.
    baseline=validate_pm_slots(None,catalog,**context)  # Validate server inputs first.
    if not isinstance(output,Mapping):return baseline
    issues,common=_root(output,catalog,session='PM_PREMARKET',keys=_RANKING_PRE,
        required_envelope=context['required_envelope'],recipients={'to_a4','to_a6'})
    try:derived=derive_pm_slots(output.get('plan_ranking'),envelopes=context['envelopes'],
        plan_clusters=context['plan_clusters'],free_slots=context['free_slots'])
    except ValueError as exc:
        issues.append(str(exc));return _result(issues,common)
    projected={k:v for k,v in output.items() if k!='plan_ranking'}
    projected['slot_plan']=derived['slot_plan']
    checked=validate_pm_slots(projected,catalog,**context)
    return _result([*issues,*checked.issues],common)


def validate_pm_post_close(output: Any, catalog: Any, *, required_envelope: Mapping,
                           position_symbols: set[str], allowed_clusters: set[str],
                           max_positions: int) -> TeamValidation:
    if type(max_positions) is not int or max_positions < 0: raise ValueError("PM_SERVER_CONTEXT_INVALID")
    issues, common = _root(output, catalog, session="PM_POST_CLOSE", keys=_CLOSE,
        required_envelope=required_envelope, recipients={"to_a6", "to_a5"})
    if not isinstance(output, Mapping): return _result(issues, common)
    rows = output.get("overnight_stances"); seen: list[str] = []
    if not isinstance(rows, list): issues.append("PM_OVERNIGHT_POOL_INVALID"); rows = []
    for row in rows:
        if not _item(row, {"symbol", "stance", "reason_code", "what_would_change_my_mind"} | _REFS):
            issues.append("PM_OVERNIGHT_FIELDS_INVALID"); continue
        if not isinstance(row["symbol"], str) or row["symbol"] not in position_symbols:
            issues.append("PM_POSITION_DOMAIN_INVALID")
        else: seen.append(row["symbol"])
        falsifiers = row["what_would_change_my_mind"]
        if (row["stance"] not in ("HOLD", "REDUCE_TOMORROW", "EXIT_TOMORROW") or not _text(row["reason_code"])
                or not isinstance(falsifiers, list) or not 1 <= len(falsifiers) <= 2
                or not all(_text(value) for value in falsifiers)):
            issues.append("PM_OVERNIGHT_JUDGMENT_INVALID")
    if len(seen) != len(set(seen)) or set(seen) != position_symbols:
        issues.append("PM_POSITION_COVERAGE_INVALID")
    capacity = output.get("capacity_request")
    if not _item(capacity, {"slots", "clusters", "note"} | _REFS):
        issues.append("PM_CAPACITY_REQUEST_INVALID")
    else:
        slots, clusters = capacity["slots"], capacity["clusters"]
        if (type(slots) is not int or not 0 <= slots <= max_positions or not _text(capacity["note"])
                or not isinstance(clusters, Mapping) or not set(clusters).issubset(allowed_clusters)
                or any(type(value) is not int or value < 0 for value in clusters.values())
                or (isinstance(clusters, Mapping) and all(type(value) is int for value in clusters.values())
                    and type(slots) is int and sum(clusters.values()) > slots)):
            issues.append("PM_CAPACITY_REQUEST_INVALID")
    unresolved = output.get("unresolved_questions")
    if not isinstance(unresolved, list) or any(not _text(value) for value in unresolved):
        issues.append("PM_UNRESOLVED_QUESTIONS_INVALID")
    return _result(issues, common)
