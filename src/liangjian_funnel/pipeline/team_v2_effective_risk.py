"""LJ-099 deterministic, reduction-only A6 projection (Claude 0937).

The original reply and its validation remain immutable. Consumers must recompute
this proof from the original reply and independently bound server facts.
"""
from collections import Counter
from copy import deepcopy
import hashlib
import json

from .team_v2_cluster_quota import checked_cluster_quota_context

VERSION = 'a6-effective-review/2'


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def nonexistent_gap_ids(raw, requirements):
    if not isinstance(raw, dict) or not isinstance(raw.get('envelope_adjustments'), list):
        return set()
    return {r['plan_id'] for r in raw['envelope_adjustments'] if isinstance(r, dict)
        and isinstance(r.get('plan_id'), str)
        and (r.get('reason_code') == 'DATA_LIMITED_REVIEWED' or 'gap_review' in r)
        and r['plan_id'] not in requirements}


def repairable_issue(issue):
    return isinstance(issue,str) and (issue == 'A6_CAPACITY_INVALID' or issue.startswith((
        'A6_CAPACITY_DETAIL:cluster_quota:', 'A6_CAPACITY_DETAIL:plans_acceptable:',
        'A6_NONEXISTENT_SOURCE_GAP_REFERENCE:')))


def project_effective_risk(raw, *, quota_context, requirements, eligible_plan_ids,
                           original_issues, validate):
    """Return a verified derived reply; no repairs outside the issue whitelist."""
    context = checked_cluster_quota_context(quota_context)
    if any(not repairable_issue(x) for x in original_issues):
        return None
    if not isinstance(raw, dict) or not isinstance(raw.get('capacity_for_tomorrow'), dict):
        return None
    rows = raw.get('envelope_adjustments')
    if (not isinstance(rows, list) or any(not isinstance(r, dict) or not isinstance(r.get('plan_id'),str) for r in rows)
            or len(rows) != len(context['plan_clusters'])
            or Counter(r.get('plan_id') for r in rows) != Counter(list(context['plan_clusters']))):
        return None
    capacity_invalid=any(x=='A6_CAPACITY_INVALID' or x.startswith('A6_CAPACITY_DETAIL:') for x in original_issues)
    false_gaps = nonexistent_gap_ids(raw, requirements)
    # The explicit NEWS contract requires actual per-stock model judgment.
    # Never repair an invented gap into an accepted reply. Keep the legacy
    # capacity-only projection for old inputs without this opt-in contract.
    if false_gaps and any(r.get('explicit_gap_treatment_required') for r in requirements.values()):
        return None
    # Claude0942: valid original eligibility and PM primary/backup are distinct
    # from simultaneous positions. Only arithmetic-invalid originals are trimmed.
    if not capacity_invalid and not false_gaps:return None
    capacity = raw['capacity_for_tomorrow']
    slots, quotas = capacity.get('plans_acceptable'), capacity.get('cluster_quota')
    if (type(slots) is not int or slots < 0 or not isinstance(quotas, dict)
            or not set(quotas) <= set(context['cluster_caps'])
            or any(type(v) is not int or v < 0 for v in quotas.values())):
        return None
    effective = deepcopy(raw)
    limits = {c: min(quotas.get(c, 0), bound)
              for c, bound in context['cluster_upper_bounds'].items()}
    remaining = min(slots, context['engine_slots'], sum(limits.values()))
    counts, kept, trimmed, corrections = Counter(), [], [], []
    known_gap_codes = {code for r in requirements.values() for code in r['reason_codes']}
    for index, row in enumerate(effective['envelope_adjustments']):
        pid = row['plan_id']
        if pid in false_gaps:
            old = deepcopy(row)
            # Remove only the exact named, nonexistent source-gap statement;
            # preserve all other original technical/model conditions.
            conditions = row.get('what_would_change_my_mind')
            if not isinstance(conditions, list) or any(not isinstance(x, str) for x in conditions):
                return None
            removed = [x for x in conditions if any(code in x for code in known_gap_codes)]
            # Claude1040: a false DATA_LIMITED label may have no source-gap
            # statement. Repair the label while preserving all conditions.
            row['what_would_change_my_mind'] = [x for x in conditions if x not in removed]
            row['reason_code'] = 'SOURCE_FACTS_REVIEWED'
            corrections.append(dict(plan_id=pid, code='NONEXISTENT_SOURCE_GAP_REFERENCE',
                                    before=old, after=deepcopy(row)))
        if row.get('adjustment') == 'SUSPEND' or pid not in eligible_plan_ids:
            continue
        cluster = context['plan_clusters'][pid]
        if not capacity_invalid or remaining and counts[cluster] < limits[cluster]:
            kept.append(pid)
            counts[cluster] += 1
            remaining -= 1
        else:
            trimmed.append(dict(plan_id=pid, original_ordinal=index, cluster=cluster,
                                global_remaining=remaining,
                                cluster_remaining=max(0, limits[cluster]-counts[cluster]),
                                original_adjustment=row['adjustment'], code='CAPACITY_TRIMMED'))
            row['adjustment'] = 'SUSPEND'
            row['reason_code'] = 'CAPACITY_TRIMMED'
    if capacity_invalid:
        effective['capacity_for_tomorrow']['plans_acceptable'] = len(kept)
        effective['capacity_for_tomorrow']['cluster_quota'] = {c: counts[c] for c in quotas}
    checked = validate(effective)
    if not checked.valid:
        return None
    proof = dict(schema_version=VERSION, authority='CLAUDE_0937_0942',
        capacity_arithmetic_invalid=capacity_invalid,
        projection_kind='CAPACITY_INVALID_TRIM' if capacity_invalid else 'LABEL_ONLY',
        ordering='LEGACY_ADJUSTMENT_ARRAY_ORDER', original_raw_sha256=digest(raw),
        context_sha256=digest(context), requirements_sha256=digest(requirements),
        eligible_input_plan_ids=sorted(eligible_plan_ids), original_issues=list(original_issues),
        eligibility_authority='ORIGINAL_ENVELOPES_AFTER_EXISTING_A6_TIGHTEN_ONLY_RULES',
        effective_raw=effective, effective_raw_sha256=digest(effective),
        candidate_count=len(rows), executable_plan_ids=kept, trimmed=trimmed,
        candidate_eligible_count=len(kept), capacity_trimmed_count=len(trimmed),
        corrections=corrections)
    return effective, proof
