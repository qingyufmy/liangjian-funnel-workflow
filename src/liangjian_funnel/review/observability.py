"""A5-only summaries of frozen decisions and finalized monitor receipts.

No provider, strategy, model, order or notification calls.  Counts describe the
same frozen minute, never an OR over different minutes or a first-reason guess.
"""
from collections import Counter
from collections.abc import Mapping
from datetime import datetime
import hashlib
import json
import math
import re


PHASES = ('startup', 'fetch', 'judge', 'persist', 'notify')
DESIGN_DISABLED = 'DISABLED_BY_DESIGN_NOT_A3_COUNT'
ROUTE_CONSTANTS = frozenset(('A3_TREND_ROUTE_APPROVED', 'TREND_ENTRY_RULE_V2',
    'PRICE_DISCOVERY_TREND_ROUTE', 'STANDARD_TREND_ROUTE', 'DAILY_MA5_MA20_ONLY'))
TREND_V2_LEAVES = frozenset(('TREND_15M_PRESSURE_EASING',
    'TREND_PULLBACK_VOLUME_CONTRACTION', 'TREND_PRIOR_5M_REVERSAL',
    'TREND_SUBSEQUENT_5M_CONFIRMATION', 'TREND_VWAP_RECLAIMED',
    'TREND_VOLUME_NOT_OVERHEATED', 'A3_PULLBACK_ZONE'))
ENTRY_CONDITIONS = {
    'TREND_MA5': TREND_V2_LEAVES | {'TREND_5M_REVERSAL_CONFIRMATION'},
    'MA520_SWING': frozenset(('DAILY_MA5_ABOVE_MA20', 'DAILY_CLOSE_ABOVE_MA20',
        'MA520_15M_STABLE', 'MA520_5M_HIGHER_LOW', 'MA520_5M_VWAP_RECLAIM',
        'MA520_TWO_CLOSED_5M_CONFIRMATIONS', 'MA520_VOLUME_NOT_OVERHEATED',
        'A3_RIGHT_SIDE_CONFIRMATION', 'A3_TRIGGER_ZONE')),
    'LEADER_INTRADAY': frozenset(('LEADER_CONTEXT', 'LEADER_CONTEXT_CURRENT',
        'LEADER_CONTEXT_STRENGTH', 'LEADER_15M_NOT_WEAK', 'LEADER_5M_DIVERGENCE_TO_STRENGTH',
        'LEADER_5M_RESEAL', 'LEADER_FIRST_15M_BREAKOUT', 'LEADER_5M_TRIGGER',
        'LEADER_BOARD_CONFIRMATION', 'LEADER_BOARD_RISK', 'A3_TRIGGER_ZONE')),
}


def mapping(value):
    if isinstance(value, Mapping):
        return dict(value)
    try:
        result = json.loads(value or '{}')
        return result if isinstance(result, dict) else {}
    except (TypeError, ValueError):
        return {}


def stamp(value):
    try:
        at = datetime.fromisoformat(str(value))
        return at if at.tzinfo is not None else None
    except (TypeError, ValueError):
        return None


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':'), default=str).encode()).hexdigest()


def _cluster(row):
    payload = mapping(row.get('payload_json'))
    for key in ('cluster_label', 'industry_name', 'industry', 'theme_name',
                'primary_theme_name', 'theme_id', 'primary_theme'):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip(), key
    return 'UNKNOWN', 'MISSING_CLUSTER_IDENTITY'


def condition_observations(plan_rows, event_rows, *, cutoff):
    """Keep unknown/unevaluated conditions unknown, including warm-up returns."""
    clusters = Counter(_cluster(row)[0] for row in plan_rows)
    by_plan = {}
    for event in event_rows:
        payload = mapping(event.get('payload_json'))
        at = stamp(event.get('minute_end'))
        if at is not None and at.astimezone(cutoff.tzinfo).date() == cutoff.date() and at <= cutoff:
            by_plan.setdefault(str(payload.get('plan_id') or ''), []).append((at, event, payload))
    results = []
    for plan in plan_rows:
        start, end = stamp(plan.get('valid_from')), stamp(plan.get('expires_at'))
        if start is None or start > cutoff:
            continue
        identity = str(plan.get('plan_id') or '')
        payload = mapping(plan.get('payload_json'))
        label, basis = _cluster(plan)
        result = {'evidence_id': 'A5O:CONDITIONS:' + identity, 'plan_id': identity,
            'symbol': plan.get('symbol'), 'strategy_profile': payload.get('strategy_profile'),
            'cluster_label': label, 'cluster_count': clusters[label], 'cluster_basis': basis,
            'max_conditions_met': None, 'max_conditions_met_at': None,
            'failing_conditions_at_max': [], 'unavailable_conditions_at_max': [],
            'unevaluated_conditions_at_max': [], 'available_conditions_at_max': [],
            'total_conditions_at_max': [], 'veto_conditions_at_max': [],
            'condition_scope': 'OBSERVED_FROZEN_CONFIRMATIONS_ONLY_NOT_ALL_STRATEGY_GATES',
            'status': 'DATA_LIMITED', 'observed_decision_count': 0,
            'missing_confirmation_count': 0,
            'note': 'First reason is not all failed conditions. Unevaluated gates are unknown; not a buy-readiness score.'}
        profile = payload.get('strategy_profile')
        expected = ENTRY_CONDITIONS.get(profile)
        universe = set(TREND_V2_LEAVES if profile == 'TREND_MA5' and payload.get('trend_entry_rule_version') == 'trend-ma5/2' else ())
        for at, event, event_payload in sorted(by_plan.get(identity, []), key=lambda item: (item[0], str(item[1].get('event_id') or ''))):
            if at < start or (end is not None and at > end):
                continue
            strategy = mapping(event_payload.get('strategy'))
            if event.get('action') == 'DATA_BLOCK' or strategy.get('state') == 'DATA_BLOCKED':
                continue
            result['observed_decision_count'] += 1
            matrix = mapping(strategy.get('confirmation_results'))
            excluded = set(ROUTE_CONSTANTS)
            # V2's reversal is the AND of the four leaf gates, not a fifth
            # independent confirmation. Retain it in the raw matrix/hash.
            if any(key in matrix for key in TREND_V2_LEAVES - {'TREND_15M_PRESSURE_EASING', 'TREND_VOLUME_NOT_OVERHEATED', 'A3_PULLBACK_ZONE'}):
                excluded.add('TREND_5M_REVERSAL_CONFIRMATION')
                universe.update(TREND_V2_LEAVES)
            conditions = {key: mapping(value) for key, value in matrix.items()
                          if key not in excluded and isinstance(value, Mapping)
                          and value.get('kind') == 'CONDITION'
                          and (expected is None or key in expected)}
            universe.update(conditions)
            if not conditions:
                result['missing_confirmation_count'] += 1
            else:
                passed = [key for key, value in conditions.items()
                          if value.get('met') is True and value.get('available') is True]
                if result['max_conditions_met'] is None or len(passed) > result['max_conditions_met']:
                    # Price/RR gates remain failures even though they are not
                    # positive technical entry leaves in the score denominator.
                    failed_map = {key: value for key, value in matrix.items()
                        if isinstance(value, Mapping) and key not in excluded
                        and value.get('kind') in ('CONDITION', 'VETO')
                        and (value.get('met') is not True or value.get('available') is not True)}
                    failed = list(failed_map)
                    vetoes = [key for key, value in matrix.items()
                              if isinstance(value, Mapping) and value.get('kind') == 'VETO'
                              and value.get('met') is not True]
                    result.update(max_conditions_met=len(passed), max_conditions_met_at=at.isoformat(),
                        total_conditions_at_max=sorted(conditions),
                        available_conditions_at_max=sorted(key for key, value in conditions.items() if value.get('available') is True),
                        failing_conditions_at_max=sorted(set(failed + vetoes)),
                        failing_condition_reasons_at_max={key: value.get('reason') for key, value in sorted(failed_map.items())},
                        unavailable_conditions_at_max=sorted(key for key, value in conditions.items() if value.get('available') is not True),
                        veto_conditions_at_max=sorted(vetoes),
                        confirmation_evidence_id='A4:EVENT:' + str(event.get('event_id') or ''),
                        confirmation_sha256=digest(matrix), status='OBSERVED')
            if event.get('effective') and event.get('action') in ('BUY_SIGNAL', 'PLAN_INVALIDATED'):
                break
        result['unevaluated_conditions_at_max'] = sorted(universe - set(result['total_conditions_at_max']))
        result['known_condition_names'] = sorted(universe)
        results.append(result)
    return {'evidence_id': 'A5O:CONDITION_SUMMARY', 'session_plan_count': len(plan_rows),
        'activated_plan_count': len(results), 'plans': results,
        'cluster_counts': dict(sorted(clusters.items())),
        'distinct_clusters': len([key for key in clusters if key != 'UNKNOWN']),
        'unknown_cluster_count': clusters.get('UNKNOWN', 0),
        'factor_ready_count': 0, 'factor_ready_count_status': DESIGN_DISABLED,
        'factor_ready_note': 'Factor lane is disabled by design; zero is not an A3 plan count or technical eligibility result.'}


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def _stats(values):
    ordered = sorted(values)
    return {'sample_count': len(values),
            'p50_ms': ordered[math.ceil(len(ordered) * .50) - 1] if ordered else None,
            'p95_ms': ordered[math.ceil(len(ordered) * .95) - 1] if ordered else None,
            'max_ms': max(ordered) if ordered else None}


def timing_summary(receipts, *, invalid_receipt_count=0):
    """Nearest-rank quantiles; wall scopes overlap and must not be summed."""
    phases = {key: _stats([row['phase_timings'][key] for row in receipts]) for key in PHASES}
    warnings = []
    if (phases['fetch']['p95_ms'] or 0) > 25000:
        warnings.append('FETCH_P95_OVER_25S')
    if any(max(row['elapsed_ms'], row.get('parent_duration_ms') or 0) > 45000 for row in receipts):
        warnings.append('MONITOR_MINUTE_OVER_45S')
    if any((row.get('parent_duration_ms') or 0) > 50000 for row in receipts):
        warnings.append('MONITOR_MINUTE_NEAR_BUDGET')
    return {'evidence_id': 'A5O:TIMINGS', 'status': 'READY' if receipts and not invalid_receipt_count else 'DATA_LIMITED',
        'minute_count': len(receipts), 'invalid_receipt_count': invalid_receipt_count,
        'parent_duration_count': sum(row.get('parent_duration_ms') is not None for row in receipts),
        'phases': phases, 'elapsed': _stats([row['elapsed_ms'] for row in receipts]),
        'budget_warnings': warnings,
        'over_30s_minutes': [row for row in receipts if max(row['elapsed_ms'], row.get('parent_duration_ms') or 0) > 30000],
        'receipt_index_sha256': digest(receipts), 'percentile_method': 'NEAREST_RANK',
        'timing_basis': 'FINALIZED_WALL_SCOPES_MAY_OVERLAP_NOT_ADDITIVE_CPU_TIME',
        'note': 'Trace finalization does not prove a business task succeeded. No client notification is sent.'}


def _parent_durations(directory, cutoff):
    parent_times = {}
    log = directory.parent / ('node-' + cutoff.date().isoformat() + '.jsonl')
    try:
        if not log.is_file():
            return parent_times, 0
        with log.open(encoding='utf-8', errors='replace') as stream:
            for line in stream:
                try:
                    value = json.loads(line)
                    at = stamp(value.get('timestamp', '').replace('Z', '+00:00'))
                    if value.get('job') != 'monitor' or value.get('stream') != 'node' or at is None or at > cutoff:
                        continue
                    message = str(value.get('message') or '')
                    match = re.search(r'\bstatus=(?:succeeded|failed|terminated)\b.*?\bduration_ms=(\d+)\b', message)
                    if match:
                        parent_times[str(value.get('runId') or '')] = int(match.group(1))
                except (ValueError, AttributeError, TypeError):
                    continue
    except OSError:
        return {}, 1
    return parent_times, 0


def timing_observations(directory, *, cutoff):
    rows = []
    parent_times, invalid = _parent_durations(directory, cutoff)
    for path in sorted(directory.glob('monitor-*.receipt.json')):
        try:
            # Natural monitor ids contain the UTC date; all A-share trading
            # minutes are on the same UTC date. Skip old natural ids without
            # repeatedly reading years of immutable receipts.
            label = path.name[len('monitor-'):len('monitor-') + 10]
            if len(label) == 10 and label[4:5] == '-' and label[7:8] == '-' and label != cutoff.date().isoformat():
                continue
            if path.stat().st_size > 1024 * 1024:
                raise ValueError('RECEIPT_TOO_LARGE')
            raw = path.read_bytes()
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError('RECEIPT_NOT_MAPPING')
            at = stamp(value.get('observed_at'))
            if at is None or at.astimezone(cutoff.tzinfo).date() != cutoff.date() or at > cutoff:
                continue
            if value.get('trace_status') != 'FINALIZED':
                continue
            if value.get('run_id') != path.name[:-len('.receipt.json')]:
                raise ValueError('RECEIPT_RUN_ID_CONFLICT')
            timings = mapping(value.get('phase_timings'))
            elapsed = value.get('elapsed_ms')
            durations = {key: mapping(timings.get(key)).get('duration_ms') for key in PHASES}
            if not _number(elapsed) or not all(_number(v) for v in durations.values()):
                raise ValueError('RECEIPT_INVALID_DURATION')
            rows.append({'run_id': value['run_id'], 'observed_at': at.isoformat(),
                'elapsed_ms': float(elapsed), 'phase_timings': {key: float(val) for key, val in durations.items()},
                'parent_duration_ms': parent_times.get(value['run_id']),
                'receipt_sha256': hashlib.sha256(raw).hexdigest()})
        except (OSError, ValueError, TypeError):
            invalid += 1
    return timing_summary(rows, invalid_receipt_count=invalid)


def compact_observability(value):
    """Keep each plan and warning; high-latency minute detail stays archived."""
    result = dict(value)
    timing = dict(mapping(value.get('timings')))
    details = timing.pop('over_30s_minutes', [])
    timing['over_30s_minute_count'] = len(details)
    timing['over_30s_archive_locator'] = 'execution_observability.timings.over_30s_minutes'
    timing['over_30s_sha256'] = digest(details)
    result['timings'] = timing
    return result


def observability_markdown(value):
    if not isinstance(value, Mapping):
        return []
    conditions, timing = mapping(value.get('conditions')), mapping(value.get('timings'))
    lines = ['## 冻结条件与运行预算（代码统计）', '',
        '- factor_ready_count：`' + str(conditions.get('factor_ready_count')) + '`；`' + str(conditions.get('factor_ready_count_status')) + '`，不代表 A3 为零。',
        '- 簇分布：`' + json.dumps(conditions.get('cluster_counts', {}), ensure_ascii=False) + '`；不同已知簇：' + str(conditions.get('distinct_clusters')) + '。',
        '- 条件只统计同一冻结分钟的明确可用证据；路由常量/派生 AND 不重复计数，未评估条件未知，不是入场资格评分。']
    for row in conditions.get('plans', []):
        lines.append('- ' + str(row.get('symbol')) + '：最多满足 ' + str(row.get('max_conditions_met')) + '，时刻 ' + str(row.get('max_conditions_met_at'))
            + '；未满足/不可用 ' + json.dumps(row.get('failing_conditions_at_max'), ensure_ascii=False)
            + '；未评估 ' + json.dumps(row.get('unevaluated_conditions_at_max'), ensure_ascii=False) + '。')
    lines.extend(['- 五段耗时 p50/p95/max（ms）：`' + json.dumps(timing.get('phases', {}), ensure_ascii=False) + '`。',
        '- 预算预警：`' + json.dumps(timing.get('budget_warnings', []), ensure_ascii=False) + '`；仅内部 A5，不发送额外客户提醒。', ''])
    return lines
