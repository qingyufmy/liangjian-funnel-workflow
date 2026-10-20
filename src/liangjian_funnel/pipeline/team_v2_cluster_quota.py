"""Server-owned A6 cluster capacity; deterministic reductions only.

This pure projection never repairs model judgments, evidence, domains or risk
units. The caller retains provider raw and revalidates the independent result.
"""
from collections import Counter
from collections.abc import Mapping
from copy import deepcopy


def build_cluster_quota_context(*, engine_slots, cluster_caps, plan_clusters, plan_priority_order):
    """A3 order determines cluster priority by each cluster's first plan.

    All inputs are frozen server facts. Missing/duplicate priorities must be
    resolved by the source projection's deterministic rank/symbol/id ordering,
    never by a model-created cluster name or a quota normalization side effect.
    """
    if (type(engine_slots) is not int or engine_slots < 0 or not isinstance(cluster_caps, Mapping)
            or any(not isinstance(k, str) or not k.strip() or type(v) is not int or v < 0
                for k, v in cluster_caps.items())
            or not isinstance(plan_clusters, Mapping)
            or any(not isinstance(pid, str) or not pid.strip() or not isinstance(cluster, str)
                or cluster not in cluster_caps for pid, cluster in plan_clusters.items())
            or not isinstance(plan_priority_order, (list, tuple))
            or any(not isinstance(pid, str) for pid in plan_priority_order)
            or len(plan_priority_order) != len(set(plan_priority_order))
            or set(plan_priority_order) != set(plan_clusters)):
        raise ValueError('A6_CLUSTER_QUOTA_SERVER_CONTEXT_INVALID')
    counts = Counter(plan_clusters.values())
    plan_counts = {cluster: counts[cluster] for cluster in sorted(cluster_caps)}
    bounds = {cluster: min(cluster_caps[cluster], plan_counts[cluster]) for cluster in sorted(cluster_caps)}
    priority = list(dict.fromkeys(plan_clusters[pid] for pid in plan_priority_order))
    priority += sorted(set(cluster_caps) - set(priority))
    remaining = min(engine_slots, sum(bounds.values())); engine_quota = {}
    for cluster in priority:
        engine_quota[cluster] = min(bounds[cluster], remaining)
        remaining -= engine_quota[cluster]
    return dict(schema_version='a6-server-cluster-capacity/1', engine_slots=engine_slots,
        cluster_caps=dict(sorted(cluster_caps.items())), cluster_plan_counts=plan_counts,
        cluster_upper_bounds=bounds, plan_clusters=dict(sorted(plan_clusters.items())),
        plan_priority_order=list(plan_priority_order), cluster_priority_order=priority,
        engine_cluster_quota=engine_quota)


def checked_cluster_quota_context(context):
    if not isinstance(context, Mapping):
        raise ValueError('A6_CLUSTER_QUOTA_SERVER_CONTEXT_INVALID')
    try:
        rebuilt = build_cluster_quota_context(engine_slots=context['engine_slots'],
            cluster_caps=context['cluster_caps'], plan_clusters=context['plan_clusters'],
            plan_priority_order=context['plan_priority_order'])
    except (KeyError, TypeError) as exc:
        raise ValueError('A6_CLUSTER_QUOTA_SERVER_CONTEXT_INVALID') from exc
    if dict(context) != rebuilt:
        raise ValueError('A6_CLUSTER_QUOTA_SERVER_CONTEXT_INVALID')
    return rebuilt


def normalize_cluster_quota(raw, quota_context):
    """Return (derived raw, audit) only if finite known integer limits reduce.

    Unknown clusters, negative/bool/noninteger values remain invalid. Absent
    clusters remain absent (zero permission); no model quota or slot is raised.
    """
    context = checked_cluster_quota_context(quota_context)
    if not isinstance(raw, Mapping) or not isinstance(raw.get('capacity_for_tomorrow'), Mapping):
        return None
    capacity = raw['capacity_for_tomorrow']
    slots = capacity.get('plans_acceptable'); quota = capacity.get('cluster_quota')
    if (type(slots) is not int or slots < 0 or not isinstance(quota, Mapping)
            or not set(quota) <= set(context['cluster_caps'])
            or any(type(value) is not int or value < 0 for value in quota.values())):
        return None
    effective_slots = min(slots, context['engine_slots'], sum(context['cluster_upper_bounds'].values()))
    remaining = effective_slots; effective_quota = {}
    for cluster in context['cluster_priority_order']:
        if cluster not in quota:
            continue
        value = min(quota[cluster], context['cluster_upper_bounds'][cluster], remaining)
        effective_quota[cluster] = value; remaining -= value
    if slots == effective_slots and dict(quota) == effective_quota:
        return None
    derived = deepcopy(dict(raw))
    derived['capacity_for_tomorrow']['plans_acceptable'] = effective_slots
    derived['capacity_for_tomorrow']['cluster_quota'] = effective_quota
    return derived, dict(code='SERVER_NORMALIZED_CLUSTER_QUOTA',
        authority='CODEX_ITERATION_BRIEF_2026_10_20_P0_2',
        original_plans_acceptable=slots, effective_plans_acceptable=effective_slots,
        original_cluster_quota=deepcopy(dict(quota)), effective_cluster_quota=effective_quota,
        original_quota_sum=sum(quota.values()), effective_quota_sum=sum(effective_quota.values()),
        engine_slots=context['engine_slots'], cluster_caps=context['cluster_caps'],
        cluster_plan_counts=context['cluster_plan_counts'], cluster_upper_bounds=context['cluster_upper_bounds'],
        cluster_priority_order=context['cluster_priority_order'],
        allocation_rule='A3_HIGHEST_PRIORITY_PLAN_DETERMINES_CLUSTER_ORDER_REDUCTIONS_ONLY')
