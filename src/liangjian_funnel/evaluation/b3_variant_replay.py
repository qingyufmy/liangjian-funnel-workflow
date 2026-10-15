"""Streaming, local-only B3 counterfactuals over the proven frozen V0 reader.

No model, provider, notification, order or production-store constructors. A
technical candidate is not a model approval or a simulated/executed position.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import closing
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import time

from .b3_baseline import iter_frozen_windows, project_monitor_result, _history_event
from .b3_variants import build_evaluator, rebase_plan, VariantEvidenceError, VARIANTS, CONTRACT
from .ablation.dataset import digest, stamp, session_minutes
from .ablation.exporter import _readonly, _json
from ..pipeline.factors import _daily_bars, _row_datetime
from ..pipeline.local_fact_cache import canonical_json_hash
from ..review.observability import ENTRY_CONDITIONS, ROUTE_CONSTANTS, TREND_V2_LEAVES


SESSIONS = ('2026-10-08', '2026-10-09', '2026-10-12', '2026-10-13', '2026-10-14', '2026-10-15')


def _positive(value):
    return isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def _condition_score(result, plan):
    matrix = result.get('confirmation_results') or {}
    expected = ENTRY_CONDITIONS.get(plan.get('strategy_profile'))
    excluded = set(ROUTE_CONSTANTS)
    if any(name in matrix for name in TREND_V2_LEAVES - {'A3_PULLBACK_ZONE', 'TREND_VOLUME_NOT_OVERHEATED', 'TREND_15M_PRESSURE_EASING'}):
        excluded.add('TREND_5M_REVERSAL_CONFIRMATION')
    conditions = {key: value for key, value in matrix.items() if isinstance(value, dict)
        and key not in excluded and value.get('kind') == 'CONDITION' and (expected is None or key in expected)}
    passed = [key for key, value in conditions.items() if value.get('met') is True and value.get('available') is True]
    failed = {key: value for key, value in matrix.items() if isinstance(value, dict)
        and key not in excluded and value.get('kind') in ('CONDITION', 'VETO')
        and (value.get('met') is not True or value.get('available') is not True)}
    universe = TREND_V2_LEAVES if plan.get('strategy_profile') == 'TREND_MA5' and plan.get('trend_entry_rule_version') == 'trend-ma5/2' else set(conditions)
    return {'max_conditions_met': len(passed) if conditions else None,
        'evaluated_condition_names': sorted(conditions),
        'failing_conditions': sorted(failed),
        'failing_condition_reasons': {key: value.get('reason') for key, value in sorted(failed.items())},
        'unevaluated_conditions': sorted(set(universe) - set(conditions)),
        'unavailable_conditions': sorted(key for key, value in conditions.items() if value.get('available') is not True),
        'condition_matrix_sha256': digest(matrix),
        'condition_scope': 'SAME_MINUTE_AVAILABLE_ENTRY_LEAVES_NOT_ROUTE_CONSTANTS_OR_INVALIDATION_CONFIRMATIONS'}


def evaluate_window(item, variant, *, evidence, history):
    """Do not reuse any original LLM verdict as approval of a new candidate."""
    row = {'variant': variant, 'contract': CONTRACT, 'trade_date': item['trade_date'],
        'plan_id': item.get('plan_id'), 'event_id': item['event']['event_id'],
        'at': item['event']['minute_end'],
        'recorded_action': item['event'].get('action'),
        'actual_execution_action': item['event'].get('action'),
        'actual_execution_reason': item['event'].get('reason_code'),
        'technical_action': None, 'variant_model_verdict': 'NOT_EVALUATED_NO_MODEL_CALL',
        'variant_order_or_fill': None, 'status': item['kind'],
        'source_event_sha256': (item.get('provenance') or {}).get('event_sha256')}
    if item['kind'] != 'WINDOW':
        row['reason'] = item.get('reason') or item.get('recorded_reason')
        return row
    profile = item['plan'].get('strategy_profile')
    if variant != 'V0' and profile != 'TREND_MA5' and not (variant == 'V4' and profile in ('MA520_SWING', 'LEADER_INTRADAY')):
        return {**row, 'status': 'NOT_APPLICABLE_CONTROL', 'reason': 'VARIANT_NOT_ASSIGNED_TO_STRATEGY_PROFILE',
                'symbol': item['plan'].get('symbol'), 'strategy_profile': profile}
    try:
        plan, geometry = rebase_plan(item['plan'], variant, evidence, item['decision_time'])
    except VariantEvidenceError as exc:
        return {**row, 'status': 'DATA_LIMITED', 'reason': str(exc)}
    result = build_evaluator(variant)(plan, item['bars'], now=item['observation_time'],
        decision_time=item['decision_time'], position=None,
        market_context=deepcopy(item['market_context'])).model_dump(mode='json')
    action, reason = project_monitor_result(result)
    row.update(status='EVALUATED', technical_action=action, reason=reason,
        symbol=plan.get('symbol'), strategy_profile=plan.get('strategy_profile'),
        technical_trigger=action in ('BUY_SIGNAL', 'ADD_SIGNAL'),
        kernel_state=result.get('state'), geometry_status=geometry['status'],
        geometry_sha256=digest(geometry), source_plan_sha256=(item.get('provenance') or {}).get('plan_sha256'),
        **_condition_score(result, plan))
    reference = result.get('live_entry_price')
    row['reference_price'] = float(reference) if _positive(reference) else None
    row['reference_price_basis'] = 'REPLAY_KERNEL_REFERENCE_NOT_EXECUTABLE_QUOTE' if row['reference_price'] else 'REFERENCE_UNAVAILABLE'
    if variant == 'V0':
        expected_action, expected_reason = project_monitor_result(item['payload']['strategy'])
        row['kernel_equal'] = (action, reason) == (expected_action, expected_reason)
        final_action, final_reason = action, reason
        basis = 'DIRECT_KERNEL_MONITOR_PROJECTION'
        if action in ('BUY_SIGNAL', 'ADD_SIGNAL', 'SELL_SIGNAL', 'REDUCE_SIGNAL', 'FORCED_RISK_EXIT'):
            from ..runtime.monitor import MonitorEngine
            if MonitorEngine._resolved_trigger_episode(tuple(history), plan_id=item['plan_id'], minute=item['decision_time'], action=action):
                final_action, final_reason = 'NO_ACTION', 'SIGNAL_ALREADY_EMITTED'
                basis = 'ACTUAL_MONITOR_RESOLVED_TRIGGER_EPISODE'
            elif item['payload'].get('llm_veto') is True and item['payload'].get('llm_reason_code'):
                final_action, final_reason = 'LLM_VETO', item['payload']['llm_reason_code']
                basis = 'FROZEN_EXPLICIT_LLM_VETO_NOT_REINFERRED'
            else:
                final_action, final_reason, basis = None, None, 'POST_KERNEL_FINAL_ACTION_UNVERIFIED'
        row.update(replay_final_action=final_action, replay_final_reason=final_reason,
            final_basis=basis, monitor_equal=(final_action, final_reason) ==
                (item['event'].get('action'), item['event'].get('reason_code')))
    return row


def reference_outcomes(day, reference_price, closes, *, sessions, captured_at, pit_proven=False):
    """Mature archived signal-reference returns, never fill/strategy PnL."""
    result = {'basis': 'SIGNAL_REFERENCE_RETURN_NOT_EXECUTED_TRADE_OR_PIT_PROOF', 'pit_identity_proven': pit_proven}
    capture = stamp(captured_at)
    for offset in (1, 3):
        value = {'return': None, 'target_day': None, 'close': None}
        if day not in sessions or sessions.index(day) + offset >= len(sessions):
            value['reason'] = 'TARGET_SESSION_NOT_IN_ARCHIVE'
        else:
            target = sessions[sessions.index(day) + offset]
            value['target_day'] = target
            closed_at = stamp(target + 'T15:00:00+08:00')
            evidence = closes.get(target) or {}
            if closed_at > capture:
                value['reason'] = 'TARGET_DAY_NOT_CLOSED_AS_OF_CAPTURE'
            elif not pit_proven:
                value['reason'] = 'PIT_IDENTITY_UNPROVEN'
            elif not _positive(reference_price):
                value['reason'] = 'SIGNAL_REFERENCE_PRICE_UNAVAILABLE'
            elif not evidence.get('closed') or not _positive(evidence.get('close')):
                value['reason'] = 'ARCHIVED_DAILY_CLOSE_MISSING'
            else:
                value.update(close=evidence['close'], **{'return': evidence['close'] / reference_price - 1},
                    reason='MATURE_ARCHIVED_REFERENCE_RETURN', daily_content_hash=evidence.get('content_hash'))
        result['T+' + str(offset)] = value
    return result


class _PlanMetrics:
    def __init__(self, plan, expires_at, captured_at):
        self.plan, self.expires_at, self.captured_at = plan, expires_at, captured_at
        self.rows = 0; self.counts = Counter(); self.reasons = Counter()
        self.first = None; self.invalid = None; self.original_invalid = None; self.last = None; self.best = None
        self.min_after_signal_low = None

    def add(self, row):
        self.rows += 1; self.last = row['at']; self.counts[row['status']] += 1
        if row.get('reason'):
            self.reasons[row['reason']] += 1
        if row.get('recorded_action') == 'PLAN_INVALIDATED' and self.original_invalid is None:
            self.original_invalid = row['at']
        if row.get('technical_action') == 'PLAN_INVALIDATED' and self.invalid is None:
            self.invalid = row['at']
        if row.get('technical_action') in ('BUY_SIGNAL', 'ADD_SIGNAL') and self.first is None:
            self.first = row
        score = row.get('max_conditions_met')
        if score is not None and (self.best is None or score > self.best['max_conditions_met']):
            self.best = row

    def observe_bars(self, bars):
        if not self.first:
            return
        at = stamp(self.first['at'])
        lows = [float(bar.low) for bar in bars if bar.bar_end > at and _positive(bar.low)]
        if lows:
            value = min(lows)
            self.min_after_signal_low = min(self.min_after_signal_low, value) if self.min_after_signal_low is not None else value

    def result(self):
        status = 'OBSERVED_TO_ORIGINAL_WINDOW_END'
        if not self.rows:
            status = 'NO_ORIGINAL_DECISION_WINDOWS'
        elif self.original_invalid is not None and (self.invalid is None or self.invalid > self.original_invalid):
            status = 'RIGHT_CENSORED_ORIGINAL_TERMINAL'
        elif self.invalid is not None:
            status = 'VARIANT_PERMANENTLY_INVALIDATED'
        elif self.captured_at and stamp(self.captured_at) < stamp(self.expires_at):
            status = 'RIGHT_CENSORED_SOURCE_CAPTURE'
        price = self.first.get('reference_price') if self.first else None
        mae = self.min_after_signal_low / price - 1 if _positive(price) and self.min_after_signal_low is not None else None
        return {'symbol': self.plan.get('symbol'), 'strategy_profile': self.plan.get('strategy_profile'),
            'observed_records': self.rows, 'status_counts': dict(self.counts), 'reason_counts': dict(self.reasons),
            'first_technical_signal_at': self.first['at'] if self.first else None,
            'signal_reference_price': price, 'variant_invalidation_at': self.invalid,
            'original_invalidation_at': self.original_invalid, 'last_observed_window': self.last,
            'observation_status': status, 'post_original_invalidation_signals': None if status.startswith('RIGHT_CENSORED') else 'NOT_ASSERTED',
            'max_conditions_met': self.best['max_conditions_met'] if self.best else None,
            'max_conditions_met_at': self.best['at'] if self.best else None,
            'failing_conditions_at_max': self.best.get('failing_conditions', []) if self.best else [],
            'failing_condition_reasons_at_max': self.best.get('failing_condition_reasons', {}) if self.best else {},
            'reason_at_max': self.best.get('reason') if self.best else None,
            'unevaluated_conditions_at_max': self.best.get('unevaluated_conditions', []) if self.best else [],
            'unavailable_conditions_at_max': self.best.get('unavailable_conditions', []) if self.best else [],
            'evaluated_condition_names_at_max': self.best.get('evaluated_condition_names', []) if self.best else [],
            'max_adverse_reference_excursion_observed': mae,
            'mae_scope': 'ONLY_ORIGINALLY_OBSERVED_CLOSED_BARS_AFTER_SIGNAL_NOT_FULL_HOLDING_LIFECYCLE',
            'actual_variant_execution_count': None, 'model_calls': 0}


def summarize_plan_windows(rows, *, plan, expires_at, captured_at):
    accumulator = _PlanMetrics(plan, expires_at, captured_at)
    for row in rows:
        accumulator.add(row)
    return accumulator.result()


class DailyEvidence:
    """Validate original raw row hashes before deriving PIT-cache ATR/outcomes."""
    def __init__(self, path, captured_at):
        self.rows = defaultdict(list); self.invalid_rows = Counter(); self.capture = stamp(captured_at)
        if path is None:
            return
        with closing(_readonly(path)) as connection:
            for raw in connection.execute('SELECT * FROM daily_bars'):
                row = dict(raw); symbol = row['symbol']
                try:
                    payload = _json(row['payload_json'])
                    if canonical_json_hash(payload) != row['content_hash']:
                        raise ValueError('ORIGINAL_DAILY_CONTENT_HASH_MISMATCH')
                    end = _row_datetime(payload, daily=True)
                    fetched = stamp(row['fetched_at'])
                    if end is None or row['adjust'] != 'none':
                        raise ValueError('DAILY_BASIS_OR_DATE_UNPROVEN')
                    self.rows[symbol].append((end, fetched, row, payload))
                except (ValueError, TypeError, KeyError) as exc:
                    self.invalid_rows[str(exc)] += 1

    def _versions(self, symbol, known_at):
        selected = {}
        for end, fetched, row, payload in self.rows.get(symbol, ()):
            if end <= known_at and fetched <= known_at and payload.get('closed') is not False and payload.get('is_closed') is not False:
                if end not in selected or fetched > selected[end][0]:
                    selected[end] = (fetched, row, payload)
        return [(end, *selected[end]) for end in sorted(selected)]

    def atr(self, plan, decision_time):
        try:
            known = stamp(plan.get('as_of'))
            if known > decision_time or known.date() >= decision_time.date():
                return {'atr_status': 'DATA_LIMITED', 'atr_reason': 'ORIGINAL_PLAN_DAILY_KNOWLEDGE_TIME_UNPROVEN'}
            rows = self._versions(plan['symbol'], known)[-800:]
            bars, reasons = _daily_bars([row[3] for row in rows], plan['symbol'], known)
            if reasons or len(bars) < 15 or len(bars) != len(rows):
                return {'atr_status': 'DATA_LIMITED', 'atr_reason': 'PIT_CACHED_HISTORY_INCOMPLETE'}
            ranges = [max(bar.high - bar.low, abs(bar.high - bars[i - 1].close), abs(bar.low - bars[i - 1].close))
                      for i, bar in enumerate(bars) if i]
            atr = sum(ranges[:14]) / 14
            for value in ranges[14:]:
                atr = (atr * 13 + value) / 14
            return {'atr_status': 'PIT_CACHE_DERIVED_NOT_ORIGINAL_FROZEN_INDICATOR',
                'atr14': atr, 'atr_as_of': known.isoformat(), 'atr_last_closed_day': bars[-1].end.date().isoformat(),
                'atr_method': 'WILDER_TR14_SEED_MEAN_AS_RUNTIME_SHADOW_INPUTS',
                'atr_source_rows_sha256': digest([row[2] for row in rows]), 'atr_seed_start': bars[0].end.isoformat(),
                'atr_source_row_count': len(rows), 'atr_full_original_seed_equivalence': False,
                'atr_last14_simple_mean_comparison': sum(ranges[-14:]) / 14}
        except (ValueError, KeyError, TypeError):
            return {'atr_status': 'DATA_LIMITED', 'atr_reason': 'DAILY_SOURCE_OR_PLAN_CLOCK_INVALID'}

    def closes(self, symbol):
        return {end.date().isoformat(): {'close': float(payload.get('close') or payload.get('close_price') or 0),
                    'closed': end <= self.capture, 'content_hash': row['content_hash']}
                for end, fetched, row, payload in self._versions(symbol, self.capture)}


def _auction_by_day(path):
    """Consume exported evidence classifications, never relabel post-auction."""
    if path is None:
        return {}, None
    raw = Path(path).read_bytes(); source = json.loads(raw); result = {}
    for snapshot in source.get('snapshots', []):
        metadata = snapshot.get('auction_metadata') or {}
        day = metadata.get('trade_date')
        for symbol, quote in (snapshot.get('plan_quotes') or {}).items():
            if not quote:
                continue
            result[(day, symbol)] = {'auction_status': metadata.get('observation_kind', 'UNPROVEN'),
                'auction_price': quote.get('latest_price'), 'auction_at': quote.get('quote_time'),
                'auction_received_at': metadata.get('as_of'), 'source_snapshot_raw_sha256': snapshot.get('raw_sha256'),
                'auction_final_price_claimed': metadata.get('auction_final_price_claimed') is True}
    return result, hashlib.sha256(raw).hexdigest()


def run_variants(*, state_db, market_state_root, output, captured_at, daily_db=None,
                 auction_evidence=None, minute_db=None, variants=VARIANTS, trade_dates=SESSIONS):
    """Stream exact original windows; never synthesize unobserved late minutes."""
    variants = tuple(variants)
    if not variants or len(set(variants)) != len(variants) or set(variants) - set(VARIANTS):
        raise ValueError('B3_VARIANT_LIST_INVALID')
    output = Path(output); output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic(); histories = defaultdict(list); plans = {}; accumulators = {}
    present_windows = defaultdict(set); original_terminal = {}
    daily = DailyEvidence(daily_db, captured_at); auction, auction_sha = _auction_by_day(auction_evidence)
    daily_proofs = {}; rows_by_day = defaultdict(lambda: defaultdict(Counter)); diffs = []; gaps = Counter()
    with closing(_readonly(state_db)) as state:
        for raw in state.execute('SELECT * FROM execution_plans'):
            row = dict(raw)
            if row['expires_at'][:10] in trade_dates:
                plans[row['plan_id']] = row
    with (output / 'comparisons.jsonl').open('x', encoding='utf-8') as stream:
        for item in iter_frozen_windows(state_db=state_db, minute_db=minute_db,
                market_state_root=market_state_root, trade_dates=trade_dates):
            pid, day = item['plan_id'], item['trade_date']
            if pid:
                present_windows[pid].add(item['event']['minute_end'])
                if item['event']['action'] == 'PLAN_INVALIDATED':
                    original_terminal.setdefault(pid, item['event']['minute_end'])
            for variant in variants:
                key = (pid, variant)
                if pid in plans and key not in accumulators:
                    plan_row = plans[pid]
                    accumulators[key] = _PlanMetrics(_json(plan_row['payload_json']), plan_row['expires_at'], captured_at)
                acc = accumulators.get(key)
                if item['kind'] == 'WINDOW' and variant in ('V3', 'V4', 'V5') and pid not in daily_proofs:
                    daily_proofs[pid] = daily.atr(item['plan'], item['decision_time'])
                evidence = {**daily_proofs.get(pid, {}), **auction.get((day, (item.get('plan') or {}).get('symbol')), {})}
                if variant != 'V0' and acc is not None and acc.invalid is not None:
                    row = {'variant': variant, 'trade_date': day, 'plan_id': pid, 'event_id': item['event']['event_id'],
                        'at': item['event']['minute_end'], 'status': 'VARIANT_TERMINAL',
                        'recorded_action': item['event']['action'], 'technical_action': None,
                        'reason': 'VARIANT_PERMANENT_INVALIDATION_ALREADY_RECORDED'}
                else:
                    row = evaluate_window(item, variant, evidence=evidence, history=histories[day])
                if acc is not None:
                    acc.add(row)
                    if item['kind'] == 'WINDOW':
                        acc.observe_bars(item['bars'])
                counts = rows_by_day[day][variant]
                counts['source_records'] += 1; counts[row['status']] += 1
                counts['raw_technical_trigger_minutes'] += int(row.get('technical_trigger') is True)
                if row.get('kernel_equal') is not None:
                    counts['kernel_equal'] += int(row['kernel_equal']); counts['monitor_equal'] += int(row['monitor_equal'])
                    if not row['kernel_equal'] or not row['monitor_equal']:
                        diffs.append(row)
                if row['status'] in ('GAP', 'DATA_LIMITED', 'PRE_KERNEL_BLOCK'):
                    gaps[(variant, row.get('reason') or row['status'])] += 1
                stream.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
            histories[day].append(_history_event(item['event'], pid))
    metrics = []
    for pid, plan_row in plans.items():
        plan = _json(plan_row['payload_json']); day = plan_row['expires_at'][:10]
        for variant in variants:
            acc = accumulators.get((pid, variant)) or _PlanMetrics(plan, plan_row['expires_at'], captured_at)
            value = acc.result()
            value.update(trade_date=day, plan_id=pid, variant=variant,
                activation_status='ACTIVATED' if plan_row.get('valid_from') else 'NOT_ACTIVATED',
                atr_evidence=daily_proofs.get(pid), original_plan_row_sha256=digest(plan_row))
            profile = plan.get('strategy_profile')
            applicable = variant == 'V0' or profile == 'TREND_MA5' or (variant == 'V4' and profile in ('MA520_SWING', 'LEADER_INTRADAY'))
            value['variant_applicability'] = 'APPLICABLE' if applicable else 'NOT_APPLICABLE_CONTROL'
            if not applicable:
                value['observation_status'] = 'NOT_APPLICABLE_CONTROL'
            value['reference_outcomes'] = reference_outcomes(day, value['signal_reference_price'], daily.closes(plan['symbol']),
                sessions=list(trade_dates), captured_at=captured_at, pit_proven=False)
            sensitivity = reference_outcomes(day, value['signal_reference_price'], daily.closes(plan['symbol']),
                sessions=list(trade_dates), captured_at=captured_at, pit_proven=True)
            sensitivity.update(basis='UNVERIFIED_SYMBOL_REFERENCE_SENSITIVITY', pit_identity_proven=False,
                excluded_from_strategy_winrate_gate=True,
                note='Raw symbol close/reference ratio only; no PIT identity, execution, profit/loss attribution or strategy efficacy proof.')
            value['unverified_symbol_reference_sensitivity'] = sensitivity
            metrics.append(value)
    operational_coverage = []
    for pid, plan_row in plans.items():
        if not plan_row.get('valid_from'):
            operational_coverage.append({'plan_id': pid, 'trade_date': plan_row['expires_at'][:10],
                'status': 'NOT_ACTIVATED', 'missing_count': 0})
            continue
        day = plan_row['expires_at'][:10]; start_at = stamp(plan_row['valid_from']); end_at = stamp(plan_row['expires_at'])
        if pid in original_terminal:
            end_at = min(end_at, stamp(original_terminal[pid]))
        if stamp(captured_at).date() == end_at.date():
            end_at = min(end_at, stamp(captured_at))
        expected = {at.isoformat() for at in session_minutes(day) if start_at <= at <= end_at}
        missing = sorted(expected - present_windows[pid])
        operational_coverage.append({'plan_id': pid, 'trade_date': day, 'status': 'GAPS' if missing else 'COVERED_TO_SOURCE_CUTOFF',
            'expected_records': len(expected), 'observed_records': len(present_windows[pid]),
            'missing_count': len(missing), 'missing_minutes': missing})
    with (output / 'plan_metrics.jsonl').open('x', encoding='utf-8') as stream:
        for value in metrics:
            stream.write(json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n')
    day_metrics = []
    for day in trade_dates:
        for variant in variants:
            selected = [row for row in metrics if row['trade_date'] == day and row['variant'] == variant]
            signal = [row for row in selected if row['first_technical_signal_at']]
            day_metrics.append({'trade_date': day, 'variant': variant,
                **dict(rows_by_day[day][variant]), 'published_plan_count': len(selected),
                'evidence_status': 'NO_PLAN_NOT_TESTED' if not selected else
                    'DATA_LIMITED' if rows_by_day[day][variant].get('DATA_LIMITED') else 'OBSERVED_ORIGINAL_WINDOWS_ONLY',
                'activated_plan_count': sum(row['activation_status'] == 'ACTIVATED' for row in selected),
                'applicable_plan_count': sum(row['variant_applicability'] == 'APPLICABLE' for row in selected),
                'not_applicable_control_plan_count': sum(row['variant_applicability'] == 'NOT_APPLICABLE_CONTROL' for row in selected),
                'first_technical_candidate_plan_count': len(signal),
                'first_technical_signal_at': min((row['first_technical_signal_at'] for row in signal), default=None),
                'right_censored_plan_count': sum(row['observation_status'].startswith('RIGHT_CENSORED') for row in selected),
                'T+1_mature_return_count': sum(row['reference_outcomes']['T+1']['return'] is not None for row in selected),
                'T+3_mature_return_count': sum(row['reference_outcomes']['T+3']['return'] is not None for row in selected)})
    summary = {'scope': 'OFFLINE_FROZEN_SIX_DAY_SAME_WINDOW_TECHNICAL_COUNTERFACTUAL_NO_MODEL_NO_ORDERS',
        'contract': CONTRACT, 'captured_at': captured_at, 'runtime_seconds': round(time.monotonic() - started, 3),
        'variants': list(variants), 'days': day_metrics, 'V0_diff_count': len(diffs), 'V0_diffs': diffs,
        'V0_source_gap_count': sum(values.get('GAP', 0) for day, mapping in rows_by_day.items()
                                  for variant, values in mapping.items() if variant == 'V0'),
        'V0_evaluated_window_count': sum(values.get('EVALUATED', 0) for day, mapping in rows_by_day.items()
                                  for variant, values in mapping.items() if variant == 'V0'),
        'original_operational_missing_plan_minutes': sum(row['missing_count'] for row in operational_coverage),
        'original_operational_coverage': operational_coverage,
        'gaps': [{'variant': variant, 'reason': reason, 'count': count} for (variant, reason), count in sorted(gaps.items())],
        'daily_invalid_row_counts': dict(daily.invalid_rows), 'auction_evidence_sha256': auction_sha,
        'source_mutation': False, 'model_calls': 0, 'provider_calls': 0,
        'limitations': ['Original terminal events right-censor later counterfactual windows; no future no-signal claim.',
            'V3/V4/V5 require original timestamped 09:25 evidence; POST_AUCTION_QUOTES never substitutes.',
            'ATR follows shadow_inputs Wilder seed mean over the available original cache prefix, not technical_aggregates simple ATR; full original seed equivalence unproven.',
            'T+1/T+3 require mature archived closes AND PIT identity; missing identity remains NULL.',
            'Technical candidates have no newly evaluated LLM, portfolio risk approval, orders or fills.',
            '10-15 source is partial if captured before 15:00; no post-capture observations synthesized.']}
    with (output / 'summary.json').open('x', encoding='utf-8') as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2)
    return summary
