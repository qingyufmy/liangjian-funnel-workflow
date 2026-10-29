"""A3-PM authored session schemas, against server-owned frozen domains.

Validation does not repair JSON, activate plans, reserve slots, or place orders.
Allocation rows for spare eligible plans are allowed, but every slotted plan must
have one allocation. Neither a model cluster nor its copied baseline is trusted.
"""
from collections import Counter
from collections.abc import Mapping
from copy import deepcopy
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
_COMPACT_PRE = _COMMON | {'deployment_target','plan_decisions','shortfall_reason'}
PM_COMPACT_REASONS = frozenset(('ACCEPT_STRUCTURE','ACCEPT_DIVERSIFICATION','WATCH_TRIGGER',
    'WATCH_CLUSTER','WATCH_REGIME','WATCH_RISK','WATCH_PRIORITY'))


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


def _allocation_risk_valid(unit,batches,conviction,envelope):
    return (unit in ('STANDARD','PROBE') and (envelope.risk_unit!='PROBE' or unit=='PROBE')
        and type(batches) is int and 1<=batches<=envelope.max_batches
        and conviction in ('NORMAL','HIGH_CONVICTION'))


def project_pm_notes(output):
    """0995: truncate only free-text notes; preserve raw JSON and all structure."""
    projected=deepcopy(output);changes=[]
    if not isinstance(projected,Mapping):return projected,changes
    rows=projected.get('plan_decisions')
    if not isinstance(rows,list):return projected,changes
    for index,row in enumerate(rows):
        if (isinstance(row,dict) and set(row)=={'plan_ref','decision','size_tier','reason_code','note'}
                and _text(row['note']) and len(row['note'])>40):
            original=row['note'];row['note']=original[:40]
            changes.append(dict(code='NOTE_TRUNCATED',path=f'plan_decisions.{index}.note',
                original=original,projected=row['note'],original_chars=len(original),projected_chars=40))
    return projected,changes


def project_pm_compact(output,catalog,*,plan_refs,**context):
    """0404 five-field provider judgments -> original downstream allocation.

    Source links are not fabricated supporting/opposing opinions. Global target
    keeps its original evidence contract; identity/risk assertions remain hard.
    """
    baseline=validate_pm_slots(None,catalog,**context)
    if not isinstance(output,Mapping):return None,baseline
    output,note_changes=project_pm_notes(output)
    issues,common=_root(output,catalog,session='PM_PREMARKET',keys=_COMPACT_PRE,
        required_envelope=context['required_envelope'],recipients={'to_a4','to_a6'})
    # Run the exact original global numeric/target/ref/shortfall validators with
    # no synthetic model allocation or opposing evidence.
    global_only={k:v for k,v in output.items() if k!='plan_decisions'}
    global_only.update(slot_plan=[],plan_allocations=[])
    global_check=validate_pm_slots(global_only,catalog,**context)
    issues.extend(global_check.issues)
    by_id={e.plan_id:e for e in context['envelopes']}
    if (not isinstance(plan_refs,Mapping) or len(set(plan_refs.values()))!=len(plan_refs)
            or not set(plan_refs.values()).issubset(by_id)):
        raise ValueError('PM_SHORTLIST_BINDING_INVALID')
    catalog_refs={i['id']:i for i in catalog.get('items',[]) if isinstance(i,Mapping)}
    if any(ref not in catalog_refs or catalog_refs[ref].get('category')!='PLAN'
            or catalog_refs[ref].get('value',{}).get('plan_id')!=pid for ref,pid in plan_refs.items()):
        raise ValueError('PM_SHORTLIST_BINDING_INVALID')
    rows=output.get('plan_decisions');seen=[];ranking=[];allocations=[];judgments=[]
    if not isinstance(rows,list):issues.append('PM_COMPACT_POOL_INVALID');rows=[]
    for index,row in enumerate(rows):
        if not isinstance(row,Mapping) or set(row)!={'plan_ref','decision','size_tier','reason_code','note'}:
            issues.append('PM_COMPACT_FIELDS_INVALID');continue
        ref=row['plan_ref']
        if not isinstance(ref,str) or ref not in plan_refs:
            issues.append('PM_COMPACT_REF_INVALID');continue
        seen.append(ref);pid=plan_refs[ref];e=by_id[pid]
        if (row['decision'] not in ('ACCEPT','WATCH') or row['size_tier'] not in ('STANDARD','PROBE','NONE')
                or not isinstance(row['reason_code'],str) or row['reason_code'] not in PM_COMPACT_REASONS
                or not _text(row['note']) or len(row['note'])>40
                or (row['decision']=='WATCH' and (row['size_tier']!='NONE' or not row['reason_code'].startswith('WATCH_')))
                or (row['decision']=='ACCEPT' and not row['reason_code'].startswith('ACCEPT_'))):
            issues.append('PM_COMPACT_DECISION_INVALID');continue
        judgments.append(dict(row,path=f'plan_decisions[{index}]'))
        if row['decision']=='WATCH':continue
        if (not e.eligible or not _allocation_risk_valid(row['size_tier'],1,'NORMAL',e)):
            issues.append('PM_ALLOCATION_ABOVE_ENVELOPE');continue
        ranking.append(dict(rank=len(ranking)+1,plan_id=pid,cluster=context['plan_clusters'][pid],source_link=ref))
        allocations.append(dict(plan_id=pid,intended_risk_unit=row['size_tier'],max_batches=1,
            conviction='NORMAL',reason_code=row['reason_code'],source_link=ref))
    if len(seen)!=len(set(seen)) or set(seen)!=set(plan_refs):issues.append('PM_COMPACT_COVERAGE_INVALID')
    counts=Counter();primary=[]
    from ..runtime.news_data_limits import main_position_allowed
    main_order=([r for r in ranking if main_position_allowed(by_id[r['plan_id']],1)]
        +[r for r in ranking if not main_position_allowed(by_id[r['plan_id']],1)])
    for row in main_order:
        if (len(primary)<context['free_slots'] and counts[row['cluster']]<PM_PRIMARY_CLUSTER_CAP
                and main_position_allowed(by_id[row['plan_id']],len(primary)+1)):
            primary.append(row);counts[row['cluster']]+=1
    primary_ids={r['plan_id'] for r in primary};backup=[r for r in ranking if r['plan_id'] not in primary_ids]
    slots=[dict(slot=i,primary_plan_id=r['plan_id'],backup_plan_id=backup[i-1]['plan_id'] if i<=len(backup) else '',
        cluster=r['cluster'],accept_order=i,source_link=r['source_link']) for i,r in enumerate(primary,1)]
    derived={k:v for k,v in output.items() if k!='plan_decisions'}
    derived.update(plan_ranking=ranking,plan_allocations=allocations,slot_plan=slots,
        note_normalization=note_changes,
        derived_shortfall='NONE' if len(primary)==context['free_slots'] else
            'NO_ACCEPTED_PLANS' if not ranking else 'CLUSTER_CAP' if backup else 'RANKING_EXHAUSTED')
    validation=TeamValidation(not issues,tuple(dict.fromkeys(issues)),
        (*common.judgments,*judgments),common.sentinel_count)
    return derived,validation


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
        from ..runtime.news_data_limits import main_position_allowed
        if type(number) is int and not main_position_allowed(by_id[primary],number):issues.append('PM_NEWS_GAP_TOP2_FORBIDDEN')
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
        if (not _allocation_risk_valid(row['intended_risk_unit'],row['max_batches'],row['conviction'],envelope)
                or not _text(row["reason_code"])):
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
    from ..runtime.news_data_limits import main_position_allowed
    by_id={e.plan_id:e for e in envelopes}
    main_order=([r for r in ordered if main_position_allowed(by_id[r['plan_id']],1)]
        +[r for r in ordered if not main_position_allowed(by_id[r['plan_id']],1)])
    for row in main_order:
        if (len(primary)<free_slots and counts[row['cluster']]<PM_PRIMARY_CLUSTER_CAP
                and main_position_allowed(by_id[row['plan_id']],len(primary)+1)):
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
