"""Shared Team-only sector projection and byte capacity contract.

The classifier consumes no sector quote/history arrays: retain its exact
geometry inputs, not the unrelated research payload. Legacy signed bodies
remain verifiable before an in-memory projection; never rewrite those files.
"""
from collections.abc import Mapping
from copy import deepcopy
import hashlib
from pathlib import Path

import yaml

from .team_v2_store import canonical

PROJECTION_VERSION = 'team-sector-classifier/1'
AUTHORITY = 'HISTORICAL_SECTOR_GEOMETRY_NOT_YESTERDAY_REGIME'
CYCLE_FIELDS = ('as_of', 'available')
HISTORY_FIELDS = ('top3_daily_overlap', 'persistent_mainline_candidates')


class SectorContextFailure(ValueError):
    def __init__(self, reason_code, **diagnostics):
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.diagnostics = diagnostics


def load_capacity(settings):
    path = Path(settings.root) / 'config/team_v2_sector_context.yaml'
    try:
        if path.is_symlink() or path.stat().st_size > 65536:
            raise ValueError
        raw = path.read_bytes()
        policy = yaml.safe_load(raw)
        if (not isinstance(policy, dict) or set(policy) != {
                'schema_version', 'projection_version', 'capacity_multiplier',
                'reference_projected_bytes', 'reference_legacy_bytes', 'parent_marker_max_bytes'}
                or policy['schema_version'] != 'team-sector-capacity/1'
                or policy['projection_version'] != PROJECTION_VERSION
                or type(policy['capacity_multiplier']) is not int or policy['capacity_multiplier'] != 2
                or any(type(policy[key]) is not int or not 0 < policy[key] <= 32 * 1024 * 1024
                    for key in ('reference_projected_bytes', 'reference_legacy_bytes', 'parent_marker_max_bytes'))):
            raise ValueError
    except (OSError, ValueError, TypeError, yaml.YAMLError):
        raise SectorContextFailure('TEAM_SECTOR_CAPACITY_CONFIG_INVALID') from None
    return {**policy,
        'projected_max_bytes': policy['reference_projected_bytes'] * policy['capacity_multiplier'],
        'legacy_input_max_bytes': policy['reference_legacy_bytes'] * policy['capacity_multiplier'],
        'config_sha256': hashlib.sha256(raw).hexdigest()}


def project_cycle(cycle):
    if not isinstance(cycle, Mapping):
        raise SectorContextFailure('TEAM_SECTOR_CONTEXT_PROJECTION_INVALID')
    projected = {key: deepcopy(cycle[key]) for key in CYCLE_FIELDS if key in cycle}
    metrics = cycle.get('history_metrics')
    if isinstance(metrics, Mapping):
        projected['history_metrics'] = {key: deepcopy(metrics[key]) for key in HISTORY_FIELDS if key in metrics}
    # Do not repair missing/wrong classifier inputs into valid observations.
    elif 'history_metrics' in cycle:
        projected['history_metrics'] = deepcopy(metrics)
    return projected


def projected_body(body):
    result = {key: body[key] for key in ('trade_date', 'known_at', 'source_snapshot_id', 'source_snapshot_hash', 'authority')}
    result.update(schema_version='team-sector-context/2', projection_version=PROJECTION_VERSION,
        source_cycle_sha256=hashlib.sha256(canonical(body['cycle']).encode()).hexdigest(),
        cycle=project_cycle(body['cycle']))
    return seal(result)


def seal(body):
    result = dict(body)
    result['body_sha256'] = hashlib.sha256(canonical(body).encode()).hexdigest()
    return result


def require_projected_capacity(body, policy):
    size = len(canonical(body).encode())
    if size > policy['projected_max_bytes']:
        raise SectorContextFailure('TEAM_SECTOR_CONTEXT_PROJECTED_SIZE_EXCEEDED',
            actual_bytes=size, limit_bytes=policy['projected_max_bytes'],
            capacity_config_sha256=policy['config_sha256'])
    return size


def bounded_read(path, maximum, reason_code):
    size = path.stat().st_size
    if size > maximum:
        raise SectorContextFailure(reason_code, actual_bytes=size, limit_bytes=maximum)
    with path.open('rb') as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise SectorContextFailure(reason_code, actual_bytes=len(raw), limit_bytes=maximum)
    return raw


def enqueue_capacity_alert(store, *, now, failure):
    """Durable internal-only queue; no sender, webhook lookup or HTTP here."""
    from .team_v2_internal import InternalOutbox
    details = failure.diagnostics
    identity = hashlib.sha256(canonical([failure.reason_code, details]).encode()).hexdigest()
    text = ('Team 板块上下文容量异常\n' + failure.reason_code + '\nactual_bytes=' +
        str(details.get('actual_bytes')) + ' limit_bytes=' + str(details.get('limit_bytes')) +
        '\n本轮市场状态无执行权限；原始文件保留，未重采行情。')
    return InternalOutbox(store).enqueue(account_id='local-paper', trade_date=now.date().isoformat(),
        kind='ALERT', source_id='TEAM_SECTOR_CONTEXT_CAPACITY:' + identity,
        body={'msg_type': 'text', 'content': {'text': text}}, observed_at=now, priority=0)
