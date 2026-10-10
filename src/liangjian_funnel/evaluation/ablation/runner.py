"""Offline experiment orchestration; only immutable files in, separate files out."""
from __future__ import annotations

from datetime import date, datetime
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import inspect
import itertools
import json
from pathlib import Path
import re
import uuid
import subprocess
from zoneinfo import ZoneInfo

from .dataset import DatasetError, digest, load_dataset, stamp, summarize_coverage


def _write(path, value):
    with path.open('x', encoding='utf-8') as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)


def file_sha256(path):
    checksum = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda:handle.read(1024*1024), b''):
            checksum.update(chunk)
    return checksum.hexdigest()


@contextmanager
def _row_stream(path):
    """Preserve the JSON array contract without retaining detailed rows."""
    with path.open('x', encoding='utf-8') as handle:
        handle.write('[')
        count = 0
        def append(row):
            nonlocal count
            if count:
                handle.write(',\n')
            json.dump(row, handle, ensure_ascii=False, separators=(',', ':'))
            count += 1
        try:
            yield append
        finally:
            handle.write(']\n')


def _projection(evaluation):
    action, reasons = evaluation.get('action'), evaluation.get('reason_codes') or []
    projected = ('START_CONFIRMATION' if action == 'DATA_BLOCK' and reasons[:1] in
                 (['NO_CLOSED_5M'], ['NO_CLOSED_15M']) else
                 'PLAN_INVALIDATED' if evaluation.get('state') == 'PLAN_INVALIDATED' else action)
    primary = ('A4_SESSION_WARMUP' if projected == 'START_CONFIRMATION' and action == 'DATA_BLOCK'
               else reasons[0] if reasons else None)
    return projected, primary


ROW_SCHEMA_VERSION = 'liangjian-ablation-row/3'
K1_SCENARIO = 'SCAN:CROSS:CURRENT:TREND_PULLBACK_LEN_K:1'
K1_VARIANT = {'zone_model':'CURRENT', 'research_sequence_model':'TREND_PULLBACK_LEN_K',
              'pullback_length':1}
_SEQUENCE_CONDITION = 'TREND_5M_REVERSAL_CONFIRMATION'
_PROTECTED_ACTIONS = {'DATA_BLOCK','FORCED_RISK_EXIT','SELL_SIGNAL','PLAN_INVALIDATED'}


def evaluation_evidence(evaluation):
    """Persist one real evaluation, preserving missing/null/empty distinctions.

    First unmet is an ordered projection, never an inferred production reason.
    A4 does not currently expose first_blocking_gate; its absence stays absent.
    """
    fields = ('action','state','reason_codes','met_conditions','unmet_conditions',
              'veto_conditions','first_blocking_gate')
    sequences = {'reason_codes','met_conditions','unmet_conditions','veto_conditions'}
    status = {}
    for name in fields:
        value = evaluation.get(name)
        status[name] = ('MISSING_IN_A4_RESULT' if name not in evaluation else 'NULL' if value is None
            else 'MALFORMED' if (name in sequences and (not isinstance(value, (list,tuple))
                or any(not isinstance(item, str) for item in value)))
                or (name not in sequences and not isinstance(value, str))
            else 'EMPTY' if not value else 'PRESENT')
    reasons = evaluation.get('reason_codes') if status['reason_codes'] in {'PRESENT','EMPTY'} else []
    unmet = evaluation.get('unmet_conditions') if status['unmet_conditions'] in {'PRESENT','EMPTY'} else []
    projected, primary = _projection({**evaluation, 'reason_codes':reasons})
    return {**{name:deepcopy(evaluation.get(name)) for name in fields},
        'row_schema_version':ROW_SCHEMA_VERSION, 'evaluation_field_status':status,
        'actual_primary_reason':reasons[0] if reasons else None,
        'projected_first_cause':unmet[0] if unmet else None,
        'projected_action':projected, 'projected_primary_reason':primary,
        'first_cause_basis':'SAME_EVALUATION_ORDERED_UNMET_CONDITIONS',
        'primary_reason_basis':'SAME_EVALUATION_REASON_CODES_0'}


def _sequence_state(evaluation):
    met, unmet = evaluation.get('met_conditions'), evaluation.get('unmet_conditions')
    if not isinstance(met, (list,tuple)) or not isinstance(unmet, (list,tuple)):
        return 'MISSING_EVIDENCE'
    passed, failed = _SEQUENCE_CONDITION in met, _SEQUENCE_CONDITION in unmet
    return 'CONFLICT' if passed and failed else 'PASS' if passed else 'FAIL' if failed else 'NOT_REACHED'


def _golden_sources():
    from .engine import evaluate_window
    from .conditions import condition_catalog
    from ...runtime.strategies import evaluate_strategy
    paths = (__file__, inspect.getfile(evaluate_window), inspect.getfile(condition_catalog),
             inspect.getfile(evaluate_strategy), inspect.getfile(load_dataset))
    root = Path(__file__).resolve().parents[4]
    def git(*args):
        return subprocess.run(['git','-C',str(root),*args], capture_output=True,
                              text=True, check=True).stdout.strip()
    return {'head':git('rev-parse','HEAD'), 'head_tree':git('rev-parse','HEAD^{tree}'),
        'working_source_sha256':{str(Path(path).resolve()):file_sha256(path) for path in paths},
        'head_is_not_a_clean_tree_claim':True}


def run_k1_golden(datasets, *, output, progress=None, entrypoint_sha256=None):
    """Explicit two-scenario, no-outcome local golden; datasets must be frozen.

    The caller loads exports through load_dataset, one day at a time. No old
    rows are accepted, no default matrix is enumerated, no output is reused.
    """
    from .engine import evaluate_window, _evaluate_with_baseline
    target = Path(output)
    target.mkdir(parents=True, exist_ok=False)
    binding = _golden_sources()
    partitions = dict(eligible=0, not_applicable=0, data_limited=0, non_trend=0, protected=0)
    reasons, distribution, scopes, audits, inputs, samples = {}, {}, {}, [], [], []
    exclusion_hash = hashlib.sha256(b'[')
    exclusion_samples, exclusion_count = [], 0
    comparison_fields = ('action','state','reason_codes','unmet_conditions','veto_conditions',
                         'actual_primary_reason','projected_first_cause','projected_action',
                         'projected_primary_reason')
    counts = {name:0 for name in (*comparison_fields, 'sequence_match')}
    differences, compared, baseline_rows, cf_rows = 0, 0, 0, 0
    checksum = hashlib.sha256(b'[')
    seen_days = set()
    with _row_stream(target/'rows.json') as append:
      for data in datasets:
        if data['trade_date'] in seen_days:
            raise DatasetError('DUPLICATE_DAY_EXPORT_SELECT_ONE_IMMUTABLE_VERSION')
        seen_days.add(data['trade_date'])
        inputs.append({'trade_date':data['trade_date'], 'source':data['source'], 'coverage':data['coverage']})
        windows = sorted(data['windows'], key=lambda window:(window['decision_time'],window['plan_id']))
        lifecycle = (data.get('export_manifest') or {}).get('lifecycle_count', 0)
        if lifecycle != 0:
            partitions['data_limited'] += len(windows)
            reasons['POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE'] = reasons.get('POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE',0)+len(windows)
            audits.append({'trade_date':data['trade_date'], 'baseline_status':'POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE',
                           'windows_requested':len(windows), 'windows_evaluated':0})
            del data, windows
            continue
        plans = {record['plan_id']:record['payload'] for record in data['plans']}
        baselines, action_mismatches, primary_mismatches, unproven = [], 0, 0, 0
        for window in windows:
            args = {'now':stamp(window['observation_time']), 'decision_time':stamp(window['decision_time']),
                    'market_context':window['market_context']}
            baseline = evaluate_window(plans[window['plan_id']], window['bars'], **args)
            baselines.append(baseline)
            observed = evaluation_evidence(baseline)
            valid = (all(observed['evaluation_field_status'][field] == 'PRESENT' for field in ('action','state'))
                     and observed['evaluation_field_status']['reason_codes'] in {'PRESENT','EMPTY'})
            if not valid or not window.get('recorded_action') or 'recorded_reason' not in window:
                unproven += 1
            else:
                action_mismatches += window['recorded_action'] != observed['projected_action']
                primary_mismatches += window['recorded_reason'] != observed['projected_primary_reason']
        day_status = ('NO_FROZEN_WINDOWS' if not windows else 'BASELINE_MISMATCH' if action_mismatches or primary_mismatches
                      else 'RECORDED_BASELINE_UNPROVEN' if unproven else 'MATCHED')
        audits.append({'trade_date':data['trade_date'], 'baseline_status':day_status,
            'coverage_status':data['coverage']['status'], 'windows_requested':len(windows),
            'windows_evaluated':len(windows), 'action_mismatch_count':action_mismatches,
            'primary_reason_mismatch_count':primary_mismatches, 'unproven_count':unproven})
        for window, baseline in zip(windows, baselines):
            plan = plans[window['plan_id']]
            identity = [data['trade_date'],plan['strategy_profile'],window['plan_id'],window['decision_time']]
            row = {'trade_date':identity[0], 'profile':identity[1], 'plan_id':identity[2], 'minute':identity[3],
                'input_sha256':window['input_sha256'], 'evidence_id':digest(identity),
                'source_binding':binding, 'removed_conditions':[], 'variant':None,
                'scenario':'BASELINE', **evaluation_evidence(baseline), 'ablation':baseline.get('ablation') or {}}
            baseline_rows += 1
            cf, partition, reason = None, None, None
            if day_status != 'MATCHED' or data['coverage']['status'] != 'COMPLETE':
                partition, reason = 'data_limited', day_status if day_status != 'MATCHED' else 'INCOMPLETE_DAY_SOURCE_EVIDENCE'
            elif plan['strategy_profile'] != 'TREND_MA5':
                partition, reason = 'non_trend', 'NON_TREND_PROFILE'
            elif plan.get('trend_entry_rule_version') != 'trend-ma5/2':
                partition, reason = 'not_applicable', 'VERSIONED_TREND_RULE_REQUIRED'
            elif baseline.get('action') in _PROTECTED_ACTIONS or baseline.get('state') == 'PLAN_INVALIDATED':
                partition, reason = 'protected', 'PROTECTED_BASELINE:'+str(baseline.get('action'))
            else:
                cf = _evaluate_with_baseline(plan, window['bars'], now=stamp(window['observation_time']),
                    decision_time=stamp(window['decision_time']), market_context=window['market_context'],
                    baseline=baseline, disabled=(), variant=deepcopy(K1_VARIANT))
                meta = cf.get('ablation') or {}
                raw_trace = meta.get('sequence_evidence')
                trace = raw_trace if isinstance(raw_trace, dict) else {}
                scope = str(trace.get('evidence_scope') or 'MISSING')
                scopes[scope] = scopes.get(scope,0)+1
                state = _sequence_state(baseline)
                matched = trace.get('matched')
                key = state+'|'+('true' if matched is True else 'false' if matched is False else 'UNKNOWN')
                distribution[key] = distribution.get(key,0)+1
                observed = evaluation_evidence(cf)
                raw_valid = (all(observed['evaluation_field_status'][field] == 'PRESENT'
                    and row['evaluation_field_status'][field] == 'PRESENT' for field in ('action','state'))
                    and all(observed['evaluation_field_status'][field] in {'PRESENT','EMPTY'}
                    and row['evaluation_field_status'][field] in {'PRESENT','EMPTY'}
                    for field in ('reason_codes','unmet_conditions','veto_conditions')))
                if meta.get('status') in {'NOT_APPLICABLE','UNSUPPORTED'}:
                    partition, reason = 'not_applicable', str(meta.get('reason') or meta['status'])
                elif meta.get('status') != 'OK':
                    partition, reason = 'data_limited', str(meta.get('reason') or meta.get('status') or 'ABLATION_STATUS_MISSING')
                elif state not in {'PASS','FAIL'}:
                    partition, reason = 'data_limited', 'BASELINE_SEQUENCE_'+state
                elif trace.get('evidence_scope') != 'ACTUAL_ISOLATED_STRATEGY_HELPER_CALL' or type(matched) is not bool:
                    partition, reason = 'data_limited', 'K1_SEQUENCE_NOT_ACTUALLY_EVALUATED_OR_BOOLEAN_MISSING'
                elif not raw_valid:
                    partition, reason = 'data_limited', 'RAW_EVALUATION_FIELDS_MISSING_OR_INVALID'
                else:
                    partition = 'eligible'
                    compared += 1
                    changed = {field:{'baseline':row[field], 'k1':observed[field]}
                               for field in comparison_fields if row[field] != observed[field]}
                    if matched != (state == 'PASS'):
                        changed['sequence_match'] = {'baseline':state,'k1_matched':matched}
                    for field in changed:
                        counts[field] += 1
                    if changed:
                        item = {'evidence_id':row['evidence_id'], 'window':identity, 'input_sha256':window['input_sha256'], 'changes':changed}
                        if differences:
                            checksum.update(b',')
                        checksum.update(json.dumps(item, sort_keys=True, ensure_ascii=False, separators=(',',':')).encode())
                        differences += 1
                        if len(samples) < 20:
                            samples.append(item)
                cf_row = {**{key:row[key] for key in ('trade_date','profile','plan_id','minute','input_sha256','evidence_id','source_binding')},
                    'scenario':K1_SCENARIO,'removed_conditions':[],'variant':deepcopy(K1_VARIANT),
                    **observed, 'ablation':meta, 'golden_partition':partition, 'golden_limitation':reason,
                    'baseline_sequence_state':state}
                cf_rows += 1
            append({**row, 'golden_partition':partition, 'golden_limitation':reason,
                    'k1_evaluated':cf is not None})
            if cf is not None:
                append(cf_row)
            partitions[partition] += 1
            if reason:
                reasons[reason] = reasons.get(reason,0)+1
                item = {'evidence_id':row['evidence_id'], 'window':identity,
                        'partition':partition, 'reason':reason}
                if exclusion_count:
                    exclusion_hash.update(b',')
                exclusion_hash.update(json.dumps(item, sort_keys=True, ensure_ascii=False, separators=(',',':')).encode())
                exclusion_count += 1
                if len(exclusion_samples) < 20:
                    exclusion_samples.append(item)
            if progress:
                progress({'trade_date':data['trade_date'],'baseline_rows':baseline_rows,'k1_rows':cf_rows})
        # Release the day's large frozen input before the generator loads another.
        del baselines, data, windows, plans
    checksum.update(b']')
    exclusion_hash.update(b']')
    unchanged = binding == _golden_sources()
    report = {'schema_version':'liangjian-k1-golden/1','row_schema_version':ROW_SCHEMA_VERSION,
        'scope':'LOCAL_K1_GOLDEN_NOT_FORMAL_15_DAY_ABLATION','source_binding':binding,
        'entrypoint_sha256':entrypoint_sha256,'source_unchanged_during_run':unchanged,
        'inputs':inputs,'day_audits':audits,'scenarios':['BASELINE',K1_SCENARIO], 'variant':deepcopy(K1_VARIANT),
        'baseline_row_count':baseline_rows,'k1_row_count':cf_rows,'partitions':partitions,
        'limitation_reason_distribution':reasons,'sequence_state_distribution':distribution,
        'sequence_evidence_scope_distribution':scopes, 'exclusion_samples':exclusion_samples,
        'evaluated_window_exclusion_count':exclusion_count,
        'evaluated_window_exclusion_sha256':exclusion_hash.hexdigest(),
        'comparable_window_count':compared,'difference_count':differences if compared else None,
        'field_difference_counts':counts if compared else {field:None for field in counts},
        'differences':samples,'complete_difference_sha256':checksum.hexdigest() if compared else None,
        'golden_pass':bool(compared and differences == 0 and unchanged),
        'pass_denominator':'ELIGIBLE_ONLY_NOT_REACHED_DATA_LIMITED_PROTECTED_NON_TREND_EXCLUDED',
        'all_requested_windows_passed':bool(compared and compared == sum(partitions.values()) and not differences and unchanged),
        'returns':None,'model_calls':0,'production_mutation':False,'configuration_action':'NO_CHANGE'}
    _write(target/'report.json', report)
    _write(target/'manifest.json', {name:file_sha256(target/name) for name in ('rows.json','report.json')})
    return report


def scan_scenarios():
    """Named, reproducible research matrix; equivalent N is not a new rule."""
    widths = ([(f'MA5_ATR:{k:g}', {'zone_model':'MA5_ATR', 'atr_multiplier':k}) for k in (.5,1.,1.5)] +
              [('REALTIME_MA5_SHIFT', {'zone_model':'REALTIME_MA5_SHIFT'})])
    sequences = ([(f'TREND_PULLBACK_LEN_K:{k}', {'research_sequence_model':'TREND_PULLBACK_LEN_K', 'pullback_length':k}) for k in (1,2,3)] +
                 [(f'TREND_CONFIRM_WITHIN_N:{n}', {'research_sequence_model':'TREND_CONFIRM_WITHIN_N', 'confirmation_window':n}) for n in (6,8)])
    return ([(f'EQUIVALENT_LATEST4:N{n}', {'sequence_window':n, 'zone_model':'CURRENT'}) for n in (4,6,8)] +
            widths + sequences +
            [(f'CROSS:{zone_name}:{sequence_name}', {**zone, **sequence})
             for zone_name, zone in [('CURRENT', {'zone_model':'CURRENT'})]+widths
             for sequence_name, sequence in sequences])


def run_experiment(datasets, *, output, max_windows=None, scans=False, progress=None):
    from .conditions import ATTRIBUTION_VERSION, atomic_failures, condition_catalog
    from .engine import evaluate_window, _evaluate_with_baseline
    from .outcomes import evaluate_outcome, opportunity_cost
    from .price_limits import resolve_price_limits
    from .statistics import summarize
    from ...runtime.strategies import evaluate_strategy

    if max_windows is not None and max_windows < 1:
        raise DatasetError('POSITIVE_SMOKE_LIMIT_REQUIRED')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    implementation_hashes = {str(Path(path).resolve()):file_sha256(path) for path in (
        __file__, inspect.getfile(evaluate_window), inspect.getfile(condition_catalog),
        inspect.getfile(evaluate_outcome), inspect.getfile(resolve_price_limits),
        inspect.getfile(summarize))}
    rows, mismatches, reason_mismatches, limitations = [], [], [], []
    day_audits, sources, coverage_inputs = [], [], []
    visited = 0
    catalogs = {}
    variants = scan_scenarios() if scans else []
    with _row_stream(output/'rows.json') as append_row:
      for data in datasets:
        sources.append(data['source'])
        coverage_inputs.append({'trade_date':data['trade_date'], 'coverage':data['coverage']})
        manifest = data.get('export_manifest')
        lifecycle = manifest.get('lifecycle_count') if manifest is not None else 0
        if lifecycle != 0:
            reason = 'POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE'
            day_audits.append({'trade_date':data['trade_date'], 'status':reason,
                               'input_window_count':len(data['windows']), 'windows_evaluated':0,
                               'lifecycle_count':lifecycle})
            limitations.append({'trade_date':data['trade_date'], 'reason':reason})
            continue
        plans = {record['plan_id']:record for record in data['plans']}
        triggers_seen = set()
        opportunities = {}
        windows = sorted(data['windows'], key=lambda value:(value['decision_time'], value['plan_id']))
        if max_windows is not None:
            windows = windows[:max(0,max_windows-visited)]
        baseline_results = []
        action_start, reason_start = len(mismatches), len(reason_mismatches)
        unproven = 0
        for index, window in enumerate(windows):
            visited += 1
            record = plans[window['plan_id']]
            plan = record['payload']
            args = {'now':stamp(window['observation_time']), 'decision_time':stamp(window['decision_time']),
                    'market_context':window['market_context']}
            baseline = evaluate_window(plan, window['bars'], **args)
            baseline_results.append(baseline)
            projected, primary_reason = _projection(baseline)
            if not window.get('recorded_action') or 'recorded_reason' not in window:
                unproven += 1
            if window.get('recorded_action') and window['recorded_action'] != projected:
                mismatches.append({'trade_date':data['trade_date'], 'plan_id':window['plan_id'],
                                   'minute':window['decision_time'], 'recorded_action':window['recorded_action'],
                                   'baseline_action':projected})
            if 'recorded_reason' in window and window['recorded_reason'] != primary_reason:
                reason_mismatches.append({'trade_date':data['trade_date'], 'plan_id':window['plan_id'],
                    'minute':window['decision_time'], 'recorded_reason':window['recorded_reason'],
                    'baseline_reason':primary_reason})
            if progress and (index % 200 == 0 or index == len(windows)-1):
                progress({'stage':'BASELINE', 'trade_date':data['trade_date'], 'completed':index+1,
                          'total':len(windows)})
        day_status = ('NO_FROZEN_WINDOWS' if not windows else
                      'BASELINE_MISMATCH' if len(mismatches)>action_start or len(reason_mismatches)>reason_start
                      else 'RECORDED_BASELINE_UNPROVEN' if unproven else 'MATCHED')
        baseline_status = day_status
        if day_status == 'MATCHED' and data['coverage']['status'] != 'COMPLETE':
            day_status = 'INCOMPLETE_DAY_SOURCE_EVIDENCE'
        scan_equivalence = {'eligible_window_count':0, 'comparison_count':0, 'difference_count':0,
                            'differences':[], 'models':{'4':'EXACT_LAST4','6':'CONTIGUOUS4_LATEST_CONFIRM',
                                                     '8':'CONTIGUOUS4_LATEST_CONFIRM'},
                            'boundary':'Four adjacent phases with latest confirmation leave only the last-four candidate.'}
        day_audits.append({'trade_date':data['trade_date'], 'status':day_status, 'baseline_status':baseline_status,
                           'input_window_count':len(data['windows']), 'windows_evaluated':len(windows),
                           'action_mismatch_count':len(mismatches)-action_start,
                           'reason_mismatch_count':len(reason_mismatches)-reason_start,
                           'recorded_baseline_unproven_count':unproven,
                           'sequence_scan_equivalence':scan_equivalence})
        if progress:
            progress({'stage':'BASELINE_DONE', 'trade_date':data['trade_date'], 'status':day_status,
                      'action_mismatch_count':len(mismatches)-action_start,
                      'reason_mismatch_count':len(reason_mismatches)-reason_start,
                      'unproven_count':unproven})
        if day_status != 'MATCHED':
            limitations.append({'trade_date':data['trade_date'], 'reason':day_status,
                                'policy':'No counterfactual return on a day with an unmatched/unproven baseline.'})
        for index, (window, baseline) in enumerate(zip(windows, baseline_results)):
            record = plans[window['plan_id']]
            plan = record['payload']
            profile = plan['strategy_profile']
            catalog = catalogs.setdefault(profile, condition_catalog(profile))
            derived = {item['id'] for item in catalog if item.get('derived')
                       and plan.get('trend_entry_rule_version') == 'trend-ma5/2'}
            supported = [item['id'] for item in catalog if item.get('supported') and item['id'] not in derived]
            at, closed = stamp(window['decision_time']), stamp(window['observation_time'])
            args = {'now':closed, 'decision_time':at, 'market_context':window['market_context']}
            action, reasons = baseline.get('action'), baseline.get('reason_codes') or []
            if day_status != 'MATCHED':
                baseline = {**baseline, 'ablation':{**baseline.get('ablation', {}),
                            'status':'DATA_LIMITED', 'reason':day_status}}
            archive = data['minute_archives'].get(plan['symbol'], {})
            future = (data.get('outcome_bars') or {}).get(plan['symbol'], archive.get('one_minute', []))
            labels = (data.get('outcome_labels') or {}).get(plan['symbol'], [])
            active_end = min(stamp(record['expires_at']), stamp(record.get('invalidated_at') or record['expires_at']))
            active_plan = {**plan, 'valid_from':record['activated_at'], 'expires_at':active_end.isoformat()}
            if window['plan_id'] not in opportunities:
                opportunities[window['plan_id']] = opportunity_cost(active_plan, archive.get('one_minute', [])) if day_status == 'MATCHED' else {
                    'status':'DATA_LIMITED', 'reason':day_status, 'value':None}
            opportunity = opportunities[window['plan_id']]
            cost = opportunity.get('value') if isinstance(opportunity, dict) else None
            # Retain unsupported/protective failures too: dropping them would
            # falsely make an ordinary technical predicate the sole killer.
            attribution = atomic_failures(baseline, versioned_trend=(profile == 'TREND_MA5'
                and plan.get('trend_entry_rule_version') == 'trend-ma5/2'))
            failed = attribution['failed_conditions']
            scenarios = [('BASELINE', (), None)]
            # Blocked input/risk windows are not opportunities to remove gates.
            # Keep the baseline evidence rather than enumerating fake variants.
            if day_status == 'MATCHED' and action not in ('DATA_BLOCK', 'FORCED_RISK_EXIT', 'SELL_SIGNAL', 'PLAN_INVALIDATED') and baseline.get('state') != 'PLAN_INVALIDATED':
                scenarios += [('SINGLE:'+name, (name,), None) for name in supported]
                scenarios += [('PAIR:'+left+'+'+right, (left,right), None)
                              for left,right in itertools.combinations(supported,2)]
                scenarios += [('SCAN:'+name, (), variant) for name, variant in variants]
            for scenario, disabled, variant in scenarios:
                evaluation = baseline if scenario == 'BASELINE' else _evaluate_with_baseline(
                    plan, window['bars'], **args, baseline=baseline, disabled=disabled, variant=variant)
                if variant and 'sequence_window' in variant and plan.get('trend_entry_rule_version') == 'trend-ma5/2' and evaluation['ablation']['status'] == 'OK':
                    scan_equivalence['eligible_window_count'] += variant['sequence_window'] == 4
                    scan_equivalence['comparison_count'] += 1
                    public = {key:value for key,value in evaluation.items() if key not in ('ablation','counterfactual')}
                    original = {key:value for key,value in baseline.items() if key not in ('ablation','counterfactual')}
                    if public != original:
                        scan_equivalence['difference_count'] += 1
                        scan_equivalence['differences'].append({'plan_id':window['plan_id'],
                            'minute':window['decision_time'], 'sequence_window':variant['sequence_window']})
                outcome = None
                key = (window['plan_id'], scenario)
                if (evaluation.get('action') == 'BUY_SIGNAL' and key not in triggers_seen
                        and (evaluation.get('ablation') or {}).get('status', 'OK') == 'OK'):
                    triggers_seen.add(key)
                    # Order knowledge cannot precede the actual decision; the
                    # scheduled minute is only the engine's market-gate clock.
                    outcome = evaluate_outcome(active_plan, stamp(window.get('evaluated_at') or window['decision_time']),
                                               future, outcome_labels=labels)
                row = {'trade_date':data['trade_date'], 'profile':profile, 'scenario':scenario,
                             'removed_conditions':list(disabled), 'variant':variant,
                             'plan_id':window['plan_id'], 'minute':window['decision_time'],
                             'action':evaluation.get('action'), 'baseline_action':action,
                             'failed_conditions':failed, 'data_block':action == 'DATA_BLOCK',
                             'attribution':attribution,
                             'unmet_conditions':evaluation.get('unmet_conditions') or [],
                             'veto_conditions':evaluation.get('veto_conditions') or [],
                             'outcome':outcome, 'opportunity_cost':cost,
                             'opportunity_cost_evidence':opportunity,
                             'input_sha256':window['input_sha256'],
                             'ablation':evaluation.get('ablation') or {}}
                row.update(evaluation_evidence(evaluation))
                append_row(row)
                rows.append({key:value for key,value in row.items() if key in {
                    'trade_date','profile','scenario','removed_conditions','plan_id','minute','action',
                    'failed_conditions','data_block','outcome','opportunity_cost','attribution','variant'}} |
                    {'ablation':{key:row['ablation'].get(key) for key in (
                        'status','sequence_scan_model','sequence_window','sequence_pullback_length',
                        'sequence_confirmation_anchor','sequence_candidate_count')} |
                        {'sequence_evidence_scope':(row['ablation'].get('sequence_evidence') or {}).get('evidence_scope')}})
            if progress and (index % 100 == 0 or index == len(windows)-1):
                progress({'stage':'ABLATION', 'trade_date':data['trade_date'], 'completed':index+1,
                          'total':len(windows)})
        if data['coverage']['status'] != 'COMPLETE':
            limitations.append({'trade_date':data['trade_date'], 'reason':'INCOMPLETE_DAY_OR_ARRIVAL_EVIDENCE'})
        if scan_equivalence['difference_count']:
            limitations.append({'trade_date':data['trade_date'], 'reason':'CONTIGUOUS_LATEST_SCAN_BASELINE_DIFFERENCE',
                                'count':scan_equivalence['difference_count']})
    strategy_file = Path(inspect.getfile(evaluate_strategy))
    receipt = {'schema_version':'liangjian-ablation-report/2',
               'row_schema_version':ROW_SCHEMA_VERSION,
               'attribution_version':ATTRIBUTION_VERSION,
               'scan_contract_version':'EXPLICIT_ATOMIC_SCANS/2',
               'mode':'OFFLINE_COUNTERFACTUAL_RESEARCH_NOT_ACCOUNT_RETURNS',
               'formal_return_policy':'COMPLETE_INPUT_DAY_AND_MATCHED_FULL_BASELINE_ONLY',
               'production_mutation':False, 'model_calls':0,
               'strategy_source_sha256':hashlib.sha256(strategy_file.read_bytes()).hexdigest(),
               'strategy_source_lf_sha256':hashlib.sha256(strategy_file.read_text(encoding='utf-8').encode()).hexdigest(),
               'implementation_source_sha256':implementation_hashes,
               'sources':sources, 'day_audits':day_audits,
               'coverage':summarize_coverage(coverage_inputs), 'windows_evaluated':visited,
               'parameter_scans_requested':scans,
               'parameter_scan_matrix':dict(variants),
               'smoke_slice':max_windows is not None, 'baseline_action_mismatches':mismatches,
               'baseline_reason_mismatches':reason_mismatches,
               'catalogs':catalogs, 'limitations':limitations,
               'statistics':summarize(rows),
               'configuration_action':'CONFIG_CHANGE_PROPOSAL_ONLY_NO_PRODUCTION_APPLY'}
    receipt['acceptance'] = {'CODE':'EXECUTED_NOT_A_FULL_REPOSITORY_TEST',
        'REPLAY':'PENDING_EVIDENCE' if not scans or max_windows is not None or mismatches or reason_mismatches or limitations or receipt['coverage']['status'] != 'COMPLETE' else 'PASSED',
        'OPERATIONS':'NOT_APPLICABLE_OFFLINE', 'STRATEGY':'PENDING_SAMPLE_SUFFICIENCY'}
    _write(output/'report.json', receipt)
    _write(output/'manifest.json', {name:file_sha256(output/name)
                                  for name in ('rows.json','report.json')})
    return receipt


def run_cli(args):
    try:
        start, end = date.fromisoformat(args.date_from), date.fromisoformat(args.date_to)
        today = datetime.now(ZoneInfo('Asia/Shanghai')).date()
        if end < start or end >= today:
            raise DatasetError('HISTORICAL_RANGE_REQUIRED_END_BEFORE_TODAY')
        run_id = args.run_id or datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:8]
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', run_id):
            raise DatasetError('SAFE_UNIQUE_RUN_ID_REQUIRED')
        datasets = [load_dataset(Path(path)) for path in args.input]
        days = [data['trade_date'] for data in datasets]
        if len(days) != len(set(days)):
            raise DatasetError('DUPLICATE_DAY_EXPORT_SELECT_ONE_IMMUTABLE_VERSION')
        if any(not start <= date.fromisoformat(day) <= end for day in days):
            raise DatasetError('DATASET_OUTSIDE_REQUESTED_RANGE')
        target = Path(args.output_root)/run_id
        receipt = run_experiment(datasets, output=target, max_windows=args.max_windows, scans=args.scans)
        print(json.dumps({'run_id':run_id, 'report':str(target/'report.json'),
                          'coverage':receipt['coverage']['status'], 'acceptance':receipt['acceptance']}, ensure_ascii=False))
        return 0
    except (DatasetError, ValueError, OSError) as exc:
        print(json.dumps({'status':'BLOCKED','reason':str(exc),'production_mutation':False}, ensure_ascii=False))
        return 2
