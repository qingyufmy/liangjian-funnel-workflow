"""Bounded A6 request size; one unchanged authoritative evidence catalogue."""
from copy import deepcopy
from pathlib import Path
import math

DEFAULT_POLICY = dict(schema_version='team-a6-runtime/1', timeout_seconds=106,
    output_base_tokens=2048, output_per_plan_tokens=512, output_per_cluster_tokens=128)


def load_risk_policy(root):
    import yaml
    path = Path(root) / 'config/team_v2_a6.yaml'
    value = yaml.safe_load(path.read_text(encoding='utf-8')) if path.is_file() else dict(DEFAULT_POLICY)
    if (not isinstance(value, dict) or set(value) != set(DEFAULT_POLICY)
            or value['schema_version'] != DEFAULT_POLICY['schema_version']
            or type(value['timeout_seconds']) not in (int, float)
            or not 0 < value['timeout_seconds'] <= 106
            or any(type(value[k]) is not int or not 0 < value[k] <= 16384 for k in
                   ('output_base_tokens', 'output_per_plan_tokens', 'output_per_cluster_tokens'))):
        raise ValueError('A6_RUNTIME_POLICY_INVALID')
    return value


def risk_output_budget(policy, plan_count, cluster_count, configured_max):
    if any(type(v) is not int or v < 0 for v in (plan_count, cluster_count)):
        raise ValueError('A6_OUTPUT_DOMAIN_INVALID')
    estimate = (policy['output_base_tokens'] + plan_count * policy['output_per_plan_tokens']
                + cluster_count * policy['output_per_cluster_tokens'])
    return min(configured_max, max(4096, math.ceil(estimate / 4096) * 4096))


def project_risk_values(values):
    """Replace duplicate PLANS only; resolve each ref back to the exact value."""
    result = deepcopy(dict(values))
    plans = result.get('PLANS')
    if not isinstance(plans, list):
        raise ValueError('A6_PLAN_REF_DOMAIN_INVALID')
    items = [i for i in result['EVIDENCE_CATALOG'].get('items', []) if i.get('category') == 'PLAN']
    ids = [i['id'] for i in items]
    if len(ids) != len(set(ids)) or [i.get('value') for i in items] != plans:
        raise ValueError('A6_PLAN_REF_VALUE_MISMATCH')
    result['PLANS'] = {'source': 'EVIDENCE_CATALOG', 'plan_evidence_ids': ids}
    return result
