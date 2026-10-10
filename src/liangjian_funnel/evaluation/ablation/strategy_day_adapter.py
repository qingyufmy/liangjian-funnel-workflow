"""Read frozen WP1 files, not providers/stores/strategies or paper ledgers.

Hashes authenticate internal binding, not the truth of a provider or PIT claim.
Exact opportunity sums mean sums of source plan observations, never mean*n.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime
import hashlib
import inspect
import json
import math
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from ...runtime.simulation import _first_complete_bar_end

from .strategy_accumulation import (PROFILES, COUNT_METRICS, METRICS,
    canonical_sha256, accumulate_strategy_days, render_strategy_accumulation)

TZ = ZoneInfo('Asia/Shanghai')
ADAPTER = 'wp7-research-day-adapter/1'
OPPORTUNITY_BASIS = 'VALIDITY_WINDOW_HIGH_OVER_ENTRY_ZONE_UPPER_MINUS_ONE'
_HASH = re.compile(r'^[0-9a-f]{64}$')


def _fail(code):
    raise ValueError(code)


def _sha(value):
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        _fail('SHA256_REQUIRED')
    return value


def file_sha256(path):
    with Path(path).open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def _read(path, expected=None):
    path = Path(path)
    if path.stat().st_size > 32*1024*1024:
        _fail('METADATA_TOO_LARGE_USE_STREAMING_ROWS')
    raw = path.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    if expected is not None and sha != _sha(expected):
        _fail('FILE_HASH_MISMATCH:'+path.name)
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError):
        _fail('JSON_METADATA_INVALID')
    return value, sha


def _date(value):
    try:
        if type(value) is not str or date.fromisoformat(value).isoformat() != value:
            _fail('ISO_DATE_REQUIRED')
    except (ValueError, TypeError):
        _fail('ISO_DATE_REQUIRED')
    return value


def _stamp(value):
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            _fail('AWARE_TIMESTAMP_REQUIRED')
        return parsed.astimezone(TZ)
    except (ValueError, TypeError):
        _fail('AWARE_TIMESTAMP_REQUIRED')


def _count(value):
    if type(value) is not int or value < 0:
        _fail('NONNEGATIVE_INTEGER_REQUIRED')
    return value


def _finite(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def _legacy_digest(value):
    # Exactly the existing exporter/dataset digest, distinct from WP7 canonical.
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    default=str).encode()).hexdigest()


def _formal(day):
    return (day.get('status') == day.get('dataset_coverage') == 'COMPLETE'
        and day.get('baseline_status') == 'MATCHED'
        and all(type(day.get(k)) is int and day[k]==0 for k in
                ('baseline_action_mismatch_count','baseline_reason_mismatch_count'))
        and type(day.get('windows_evaluated')) is int and day['windows_evaluated'] > 0
        and type(day.get('input_window_count')) is int
        and day.get('windows_evaluated') == day.get('input_window_count')
        and day.get('sequence_scan_equivalence', {}).get('difference_count') == 0)


def _signature(report):
    if (report.get('schema_version') != 'liangjian-ablation-report/2'
            or report.get('mode') != 'OFFLINE_COUNTERFACTUAL_RESEARCH_NOT_ACCOUNT_RETURNS'
            or report.get('formal_return_policy') != 'COMPLETE_INPUT_DAY_AND_MATCHED_FULL_BASELINE_ONLY'
            or report.get('production_mutation') is not False
            or type(report.get('model_calls')) is not int or report['model_calls'] != 0):
        _fail('REPORT_RESEARCH_CONTRACT_INVALID')
    implementations = report.get('implementation_source_sha256')
    if not isinstance(implementations, dict) or not implementations:
        _fail('IMPLEMENTATION_SOURCE_VERSION_REQUIRED')
    normalized = {}
    for key, value in implementations.items():
        _sha(value)
        name = str(key).replace('\\','/').rsplit('/',1)[-1]
        if not name or name in normalized:
            _fail('AMBIGUOUS_IMPLEMENTATION_FILENAME')
        normalized[name] = value
    for key in ('strategy_source_sha256', 'strategy_source_lf_sha256'):
        _sha(report.get(key))
    if not report.get('attribution_version') or not report.get('scan_contract_version'):
        _fail('VERSIONED_REPORT_REQUIRED')
    signature = {k:report[k] for k in ('schema_version','strategy_source_sha256',
        'strategy_source_lf_sha256','attribution_version','scan_contract_version')}
    signature['implementation_source_sha256'] = normalized
    return signature


def _export_population(path, expected, day):
    # Optional local parser dependency. No auto install or network fallback.
    import ijson
    if file_sha256(path) != _sha(expected):
        _fail('EXPORT_HASH_MISMATCH')
    with path.open('rb') as handle:
        meta = next(ijson.items(handle, 'export_manifest', use_float=True), None)
    if not isinstance(meta, dict) or meta.get('schema_version') != 'liangjian-ablation-export-manifest/1':
        _fail('EXPORT_MANIFEST_REQUIRED')
    if (meta.get('source_mutation') is not False
            or any(type(meta.get(k)) is not int or meta[k]!=0 for k in ('model_calls','provider_calls'))):
        _fail('EXPORT_OFFLINE_SOURCE_CONTRACT_INVALID')
    if _legacy_digest({k:v for k,v in meta.items() if k!='sha256'}) != meta.get('sha256'):
        _fail('EXPORT_MANIFEST_PAYLOAD_HASH_MISMATCH')
    with path.open('rb') as handle:
        identity = {}
        def events():
            for prefix, event, value in ijson.parse(handle, use_float=True):
                if prefix in {'trade_date','schema_version'} and event=='string':
                    identity[prefix] = value
                yield prefix, event, value
        plans = list(ijson.items(events(), 'plans.item'))
    if identity!={'trade_date':day,'schema_version':'liangjian-ablation-dataset/1'}:
        _fail('EXPORT_TOP_LEVEL_IDENTITY_MISMATCH')
    ids, by_id = meta.get('activated_plan_ids'), {}
    if not isinstance(ids, list) or len(set(ids)) != len(ids):
        _fail('ACTIVATION_CENSUS_INVALID')
    for p in plans:
        pid = p.get('plan_id')
        if not isinstance(pid, str) or not pid or pid in by_id:
            _fail('DUPLICATE_OR_MISSING_PLAN_ID')
        payload = p.get('payload')
        if not isinstance(payload, dict) or _legacy_digest(payload) != p.get('sha256'):
            _fail('PLAN_PAYLOAD_HASH_MISMATCH')
        if payload.get('strategy_profile') not in PROFILES:
            _fail('PLAN_PROFILE_UNPROVEN')
        start, end = _stamp(p.get('activated_at')), _stamp(p.get('expires_at'))
        if start.date().isoformat()!=day or end.date().isoformat()!=day or end<=start:
            _fail('PLAN_VALIDITY_INVALID')
        if p.get('invalidated_at'):
            terminal = _stamp(p['invalidated_at'])
            if not start <= terminal <= end:
                _fail('PLAN_INVALIDATION_TIME_INVALID')
        if p.get('activation_basis') != 'EXECUTION_PLANS_VALID_FROM':
            _fail('ACTIVATION_BASIS_UNPROVEN')
        by_id[pid] = p
    if set(ids) != set(by_id):
        _fail('ACTIVATION_CENSUS_MISMATCH')
    return meta, by_id


def _groups(report, scenario, plans):
    groups = {}
    for group in report.get('statistics', {}).get('groups', []):
        if group.get('scenario') != scenario:
            continue
        profile = group.get('profile')
        if profile not in PROFILES or profile in groups:
            _fail('DUPLICATE_OR_AMBIGUOUS_COHORT_GROUP')
        if group.get('attribution_version') != report['attribution_version']:
            _fail('COHORT_ATTRIBUTION_VERSION_MISMATCH')
        for key in ('unsupported_window_count','data_limited_window_count','other_invalid_window_count'):
            if _count(group.get(key))!=0:
                _fail('SCENARIO_NOT_FULLY_SUPPORTED')
        for key in ('triggered_plan_count','trade_sample_count','independent_plan_count','input_plan_count','valid_window_count'):
            _count(group.get(key))
        population = sum(p['payload']['strategy_profile']==profile for p in plans.values())
        if (group['independent_plan_count'] != population or group['input_plan_count'] != population
                or group['triggered_plan_count'] > population
                or group['trade_sample_count'] > group['triggered_plan_count']):
            _fail('GROUP_POPULATION_OR_DENOMINATOR_MISMATCH')
        groups[profile] = group
    return groups


def _selected_rows(path, expected, scenario):
    """Hash every byte; parse only selected compact runner one-object lines.

    The byte prefilter may match nested text; parsed top-level scenario is checked.
    Unselected logic is not revalidated. Framing is the frozen runner's contract.
    """
    checksum = hashlib.sha256()
    token = b'"scenario":'+json.dumps(scenario, ensure_ascii=False).encode()
    with path.open('rb') as handle:
        while True:
            raw = handle.readline(64*1024*1024)
            if not raw:
                break
            if len(raw)>=64*1024*1024:
                _fail('ROW_EXCEEDS_STREAMING_LINE_BOUND')
            checksum.update(raw)
            if token not in raw:
                continue
            text = raw.decode('utf-8').strip().removeprefix('[').removesuffix(']').removesuffix(',')
            row = json.loads(text)
            if not isinstance(row,dict):
                _fail('ROW_OBJECT_FRAME_REQUIRED')
            if row.get('scenario') == scenario:
                yield row
    if checksum.hexdigest() != _sha(expected):
        _fail('ROWS_HASH_MISMATCH')


def _row_observations(path, expected, day, scenario, plans, groups):
    seen, first, costs, windows = set(), {}, {}, Counter()
    for row in _selected_rows(path, expected, scenario):
        pid, profile = row.get('plan_id'), row.get('profile')
        if pid not in plans or profile!=plans[pid]['payload']['strategy_profile']:
            _fail('ROW_PLAN_PROFILE_UNPROVEN')
        if type(row.get('data_block')) is not bool or not isinstance(row.get('action'),str) or not row['action']:
            _fail('ROW_ACTION_OR_DATA_BLOCK_UNPROVEN')
        _sha(row.get('input_sha256'))
        plan = plans[pid]
        at = _stamp(row.get('minute'))
        end = min(_stamp(plan['expires_at']), _stamp(plan.get('invalidated_at') or plan['expires_at']))
        if row.get('trade_date')!=day or at.date().isoformat()!=day or not _stamp(plan['activated_at'])<=at<=end:
            _fail('ROW_TIME_OUTSIDE_ACTIVE_PLAN_DAY')
        key = (pid, at)
        if key in seen:
            _fail('DUPLICATE_PLAN_SCENARIO_MINUTE')
        seen.add(key)
        if profile not in groups:
            # Missing report leg remains UNKNOWN; still hash all source bytes.
            continue
        if row.get('variant') != groups[profile].get('variant'):
            _fail('ROW_VARIANT_COHORT_MISMATCH')
        if row.get('ablation', {}).get('status') != 'OK':
            _fail('ROW_SCENARIO_NOT_SUPPORTED')
        windows[profile] += 1
        cost = row.get('opportunity_cost')
        evidence = row.get('opportunity_cost_evidence') or {}
        if cost is not None:
            if (not _finite(cost) or evidence.get('value')!=cost
                    or evidence.get('basis')!='VALIDITY_OBSERVED_HIGH_OVER_ENTRY_ZONE_UPPER_MINUS_ONE'
                    or evidence.get('status')!='OBSERVED_VALIDITY_BARS_ONLY'):
                _fail('OPPORTUNITY_SOURCE_CONTRACT_INVALID')
            high, upper = evidence.get('observed_high'), evidence.get('entry_zone_upper')
            if (not _finite(high) or not _finite(upper) or high<=0 or upper<=0
                    or cost != high/upper-1):
                _fail('OPPORTUNITY_SOURCE_ARITHMETIC_INVALID')
            zone = plan['payload'].get('entry_reference_zone')
            original_upper = zone.get('upper',zone.get('high')) if isinstance(zone,dict) else (
                zone[1] if isinstance(zone,list) and len(zone)==2 else None)
            if original_upper!=upper:
                _fail('OPPORTUNITY_ORIGINAL_PLAN_ZONE_MISMATCH')
        observation = {'value':cost,'evidence':evidence}
        if pid in costs and costs[pid]!=observation:
            _fail('CONFLICTING_PLAN_DAY_OPPORTUNITY')
        costs[pid] = observation
        if not row.get('data_block') and row.get('action') in {'BUY','BUY_SIGNAL','PROBE_BUY'}:
            if pid not in first or at < first[pid][0]:
                first[pid] = (at, row.get('outcome'))
    for profile, group in groups.items():
        if windows[profile]!=group['valid_window_count']:
            _fail('ROW_WINDOW_COUNT_MISMATCH')
        observed_plans = {pid for pid,_ in seen if plans[pid]['payload']['strategy_profile']==profile}
        population = {pid for pid,p in plans.items() if p['payload']['strategy_profile']==profile}
        if observed_plans != population:
            _fail('ROW_ACTIVATED_PLAN_CENSUS_MISMATCH')
        triggers = {pid:value for pid,value in first.items() if plans[pid]['payload']['strategy_profile']==profile}
        if len(triggers)!=group['triggered_plan_count']:
            _fail('ROW_FIRST_TRIGGER_COUNT_MISMATCH')
        filled = sum(isinstance(out,dict) and out.get('fill_status')=='FILLED'
                     and out.get('return_kind')=='RESEARCH_COUNTERFACTUAL' for _,out in triggers.values())
        if filled != group['trade_sample_count']:
            _fail('ROW_FILLED_COUNT_MISMATCH')
    return first, costs


def _fill_bars(path, triggers, plans):
    """Only needed when an existing outcome claims FILLED; no price collection."""
    import ijson
    targets, result = {}, {}
    for pid,(_,outcome) in triggers.items():
        if isinstance(outcome,dict) and outcome.get('fill_status')=='FILLED' and outcome.get('fill_bar_end'):
            symbol = plans[pid]['payload'].get('symbol')
            targets.setdefault(symbol,{}).setdefault(_stamp(outcome['fill_bar_end']),[]).append(pid)
    for symbol, dates in targets.items():
        with path.open('rb') as handle:
            for raw in ijson.items(handle,'outcome_bars.'+str(symbol)+'.item',use_float=True):
                try:
                    at = _stamp(raw.get('bar_end'))
                except ValueError:
                    continue
                if at in dates:
                    for pid in dates[at]:
                        if pid in result:
                            _fail('DUPLICATE_OUTCOME_CANDIDATE_BAR')
                        result[pid] = raw
    return result


def _fill_count(triggers, plans, fill_bars):
    filled = 0
    for pid, (trigger_at, outcome) in triggers.items():
        if not isinstance(outcome, dict) or outcome.get('return_kind') != 'RESEARCH_COUNTERFACTUAL':
            return None
        status = outcome.get('fill_status')
        if status == 'FILLED':
            price, bar = outcome.get('fill_price'), outcome.get('fill_bar_end')
            if price is None or not bar or outcome.get('fill_model_version') is None:
                return None
            deadline = min(_stamp(plans[pid]['expires_at']),
                           _stamp(plans[pid].get('invalidated_at') or plans[pid]['expires_at']))
            if (not _finite(price) or price<=0
                    or not trigger_at<_stamp(bar)<=deadline
                    or _stamp(bar)!=_first_complete_bar_end(trigger_at)
                    or _stamp(bar).date()!=trigger_at.date()
                    or outcome.get('fill_model_version')!='wp1-next-complete-minute-shared-paper-price/1'
                    or any(x in outcome.get('reason_codes',[]) for x in ('PRICE_LIMITS_UNKNOWN','PRICE_LIMITS_CONFLICT'))):
                _fail('FILLED_OUTCOME_CAUSAL_EVIDENCE_INVALID')
            raw = fill_bars.get(pid)
            if (not raw or raw.get('complete',raw.get('is_complete')) is not True
                    or raw.get('interval')!='1m' or raw.get('symbol')!=plans[pid]['payload'].get('symbol')
                    or raw.get('evidence_kind','MARKET_BAR')!='MARKET_BAR'
                    or not raw.get('source_id') or str(raw['source_id']).endswith(':RISK_ONLY')
                    or raw.get('volume_unit')!='shares' or not _finite(raw.get('volume')) or raw['volume']<=0
                    or raw.get('adjust_mode') not in ('none','raw','unadjusted')):
                return None
            if (not all(_finite(raw.get(k)) for k in ('open','close','low','high'))
                    or not 0<raw['low']<=raw['open']<=raw['high']
                    or not raw['low']<=raw['close']<=raw['high']
                    or not raw['low']<=price<=raw['high']):
                _fail('FILLED_OUTCOME_BAR_GEOMETRY_CONFLICT')
            filled += 1
        elif status == 'NOT_FILLED' and outcome.get('reason_codes'):
            if any(x in outcome['reason_codes'] for x in ('PRICE_LIMITS_UNKNOWN','PRICE_LIMITS_CONFLICT')):
                return None
            continue
        else:
            return None
    return filled


def _day_record(day, source_version, cohort_id, bindings, *, plans=None, groups=None,
                first=None, costs=None, fill_bars=None, read_rows=True, limited_reasons=()):
    ready = plans is not None
    summary = {'schema':'wp7-strategy-day/1','trade_date':day['trade_date'],'source_kind':'WP1_RESEARCH',
        'source_version':source_version,'cohort_id':cohort_id,'strategies':{}}
    statuses = {}
    for profile in PROFILES:
        population = {pid:p for pid,p in (plans or {}).items() if p['payload']['strategy_profile']==profile}
        group = (groups or {}).get(profile)
        known_cohort = ready and (group is not None or not population)
        metric = {key:{'count':None,'basis':key} for key in COUNT_METRICS}
        activation = len(population) if ready else None
        metric['activated_plan_days']['count'] = activation
        trigger_count = (group['triggered_plan_count'] if group and read_rows else
                         0 if ready and not population else None)
        metric['counterfactual_first_triggers']['count'] = trigger_count
        selected = {pid:value for pid,value in (first or {}).items() if pid in population}
        fills = _fill_count(selected, population, fill_bars or {}) if read_rows and known_cohort else 0 if trigger_count==0 else None
        metric['counterfactual_fill_samples']['count'] = fills
        values, eligible = [], None
        if known_cohort and trigger_count is not None:
            eligible = len(population)-(trigger_count or 0)
            if read_rows:
                for pid in population:
                    if pid in selected:
                        continue
                    value = (costs or {}).get(pid,{}).get('value')
                    if value is not None:
                        values.append(value)
        exact_complete = read_rows and known_cohort and eligible is not None and len(values)==eligible
        try:
            total = math.fsum(values) if exact_complete else None
        except OverflowError:
            _fail('OPPORTUNITY_SUM_NOT_FINITE')
        metric['opportunity_cost'] = {'sum':total,
            'sample_count':len(values) if read_rows and known_cohort else None,
            'eligible_count':eligible,'basis':OPPORTUNITY_BASIS}
        versions = Counter(p['payload'].get('strategy_version') for p in population.values()) if ready else None
        if versions is not None and (None in versions or '' in versions):
            versions = None
        projection = {'profile':profile,'activated_ids':sorted(population),'groups':group,
            'first_trigger_outcomes':{pid:out for pid,(_,out) in selected.items()},
            'fill_bar_observations':{pid:(fill_bars or {}).get(pid) for pid in selected},
            'opportunity_observations':{pid:(costs or {}).get(pid) for pid in sorted(population)},'bindings':bindings}
        projection_sha = canonical_sha256(projection)
        ref = f"WP1:{day['trade_date']}:{cohort_id}:{profile}"
        for entry in metric.values():
            if entry.get('count',entry.get('sum')) is not None:
                entry.update(source_ref=ref,evidence_sha256=projection_sha)
        statuses[profile] = {k:'COMPLETE' if v.get('count',v.get('sum')) is not None else 'UNKNOWN'
                             for k,v in metric.items()}
        statuses[profile]['strategy_versions'] = 'COMPLETE' if versions is not None else 'UNKNOWN'
        summary['strategies'][profile] = {'metrics':metric,'strategy_versions':dict(versions) if versions is not None else None,
            'version_source_ref':ref+':ORIGINAL_PLAN_PAYLOAD','version_evidence_sha256':projection_sha}
    summary_sha = canonical_sha256(summary)
    coverage = {'schema':'wp7-strategy-day-coverage/1','trade_date':day['trade_date'],
        'source_kind':'WP1_RESEARCH','source_version':source_version,'cohort_id':cohort_id,
        'summary_sha256':summary_sha,'run_id':f"WP1_FROZEN:{bindings['coverage_matrix_sha256']}:{day['trade_date']}",
        'run_status':'COMPLETE' if ready else 'DATA_LIMITED','pit_status':'MATCHED' if ready else 'UNKNOWN',
        'source_input_sha256':day['input_sha256'],'strategies':statuses}
    return {'summary':summary,'summary_sha256':summary_sha,'coverage':coverage,
        'coverage_sha256':canonical_sha256(coverage),'adapter_evidence':{'bindings':bindings,
        'source_declared_activated_plan_count':day.get('activated_plan_count'),
        'limited_reasons':list(limited_reasons),'source_declarations_authenticated':False,
        'original_transport_authenticated':False,'research_not_account_or_realtime_ledger':True,
        'exact_opportunity_source_observations':read_rows and ready}}


def adapt_strategy_days(*, run_root, export_manifest, scenario='BASELINE', as_of,
                        calendar_path=None, calendar_sha256=None, read_rows=True, progress=None):
    root, exports = Path(run_root).resolve(), Path(export_manifest).resolve()
    as_of = _date(as_of)
    if not isinstance(scenario,str) or not scenario or len(scenario)>200 or '\n' in scenario:
        _fail('EXPLICIT_COHORT_SCENARIO_REQUIRED')
    run_manifest, _ = _read(root/'manifest.json')
    matrix, matrix_sha = _read(root/'coverage-matrix.json',run_manifest.get('coverage-matrix.json'))
    if matrix.get('schema_version')!='liangjian-ablation-day-coverage/1':
        _fail('COVERAGE_MATRIX_SCHEMA_INVALID')
    exported, export_sha = _read(exports,matrix.get('export_manifest_sha256'))
    if exported.get('schema_version')!='liangjian-ablation-export-bundle/1':
        _fail('EXPORT_BUNDLE_SCHEMA_INVALID')
    days = matrix.get('days')
    if not isinstance(days,list) or not days:
        _fail('DAY_MATRIX_REQUIRED')
    named = [_date(d.get('trade_date')) for d in days]
    if len(set(named))!=len(named) or named!=sorted(named) or any(d>as_of for d in named):
        _fail('DUPLICATE_UNSORTED_OR_FUTURE_DAY')
    if named!=exported.get('trade_dates'):
        _fail('EXPORT_MATRIX_DATE_CENSUS_MISMATCH')
    prepared, signature, cohort_contracts = [], None, {}
    for day in days:
        name = day['trade_date']
        input_path = (exports.parent/(name+'.json')).resolve()
        if Path(day.get('input_path','')).resolve()!=input_path or input_path.parent!=exports.parent:
            _fail('INPUT_PATH_OUTSIDE_BOUND_EXPORT')
        expected = _sha(day.get('input_sha256'))
        if exported.get('files',{}).get(name+'.json')!=expected:
            _fail('INPUT_EXPORT_MANIFEST_HASH_MISMATCH')
        folder = (root/name).resolve()
        if folder.parent!=root:
            _fail('DAY_OUTPUT_PATH_INVALID')
        receipt, receipt_sha = _read(folder/'day-receipt.json',day.get('day_receipt_sha256'))
        for key in ('trade_date','status','dataset_coverage','baseline_status','input_sha256','activated_plan_count',
                    'input_window_count','windows_evaluated','baseline_action_mismatch_count',
                    'baseline_reason_mismatch_count','output_hashes'):
            if receipt.get(key)!=day.get(key):
                _fail('RECEIPT_MATRIX_IDENTITY_MISMATCH')
        outputs = day.get('output_hashes') or {}
        report = None
        report_sha = None
        if 'report.json' in outputs:
            report, report_sha = _read(folder/'report.json',outputs['report.json'])
            current = _signature(report)
            if signature is not None and current!=signature:
                _fail('MIXED_IMPLEMENTATION_SOURCE_VERSIONS')
            signature = current
            if _formal(day):
                for group in report.get('statistics',{}).get('groups',[]):
                    if group.get('scenario')!=scenario:
                        continue
                    profile = group.get('profile')
                    contract = {key:group.get(key) for key in ('variant','scan_contract','attribution_version')}
                    if profile in cohort_contracts and contract!=cohort_contracts[profile]:
                        _fail('MIXED_COHORT_VARIANT_OR_SCAN_CONTRACT')
                    cohort_contracts[profile] = contract
        prepared.append((day,input_path,folder,report,report_sha,receipt_sha))
    if signature is None:
        _fail('VERSIONED_RESEARCH_REPORT_REQUIRED')
    source_vector = {'adapter':ADAPTER,'adapter_source_sha256':file_sha256(Path(__file__)),
                     'clock_helper_source_sha256':file_sha256(Path(inspect.getfile(_first_complete_bar_end))),
                     'research':signature,
                     'cohort_contracts':cohort_contracts}
    source_version = ADAPTER+':'+canonical_sha256(source_vector)
    cohort_id = 'WP1:'+scenario+':'+canonical_sha256(cohort_contracts)
    records = []
    for day,input_path,folder,report,report_sha,receipt_sha in prepared:
        name = day['trade_date']
        if progress:
            progress(name)
        bindings = {'coverage_matrix_sha256':matrix_sha,'export_manifest_sha256':export_sha,
            'day_receipt_sha256':receipt_sha,'input_sha256':day['input_sha256'],
            'report_sha256':report_sha,'rows_sha256':None,'source_vector_sha256':canonical_sha256(source_vector)}
        ready = _formal(day)
        if not ready:
            if file_sha256(input_path)!=day['input_sha256']:
                _fail('EXPORT_HASH_MISMATCH')
            reasons = [day.get('reason') or 'INCOMPLETE_OR_UNMATCHED_FORMAL_DAY',
                *[gap.get('reason','UNKNOWN') for gap in day.get('source_gaps') or []]]
            records.append(_day_record(day,source_version,cohort_id,bindings,limited_reasons=reasons))
            continue
        if report is None or report.get('smoke_slice') is not False or report.get('parameter_scans_requested') is not True:
            _fail('FORMAL_REPORT_FULL_RUN_REQUIRED')
        if report.get('sources')!=[{'path':str(input_path),'sha256':day['input_sha256']}]:
            _fail('REPORT_INPUT_SOURCE_BINDING_MISMATCH')
        audit = report.get('day_audits') or []
        if (len(audit)!=1 or audit[0].get('trade_date')!=name or audit[0].get('baseline_status')!='MATCHED'
                or any(type(audit[0].get(k)) is not int or audit[0][k]!=0 for k in
                       ('action_mismatch_count','reason_mismatch_count','recorded_baseline_unproven_count'))):
            _fail('BASELINE_AUDIT_NOT_MATCHED')
        coverage = report.get('coverage',{}).get('days',[])
        if (len(coverage)!=1 or coverage[0].get('trade_date')!=name or coverage[0].get('status')!='COMPLETE'
                or coverage[0].get('all_activated_plan_census_proven') is not True
                or type(coverage[0].get('context_or_arrival_unproven_windows')) is not int
                or coverage[0]['context_or_arrival_unproven_windows']!=0 or coverage[0].get('source_evidence_gaps')):
            _fail('REPORT_PIT_CENSUS_NOT_COMPLETE')
        meta, plans = _export_population(input_path,day['input_sha256'],name)
        if meta.get('gaps') or meta.get('lifecycle_count')!=0 or len(plans)!=day.get('activated_plan_count'):
            _fail('EXPORT_ACTIVATION_OR_LIFECYCLE_NOT_COMPLETE')
        coverage_plans = coverage[0].get('plans') or []
        if (len(coverage_plans)!=len(plans) or {p.get('plan_id') for p in coverage_plans}!=set(plans)
                or any(p.get('missing_1m_count')!=0 or p.get('missing_decisions')!=0 for p in coverage_plans)):
            _fail('PLAN_BAR_OR_DECISION_COVERAGE_INCOMPLETE')
        groups = _groups(report,scenario,plans)
        first, costs, fill_bars = {}, {}, {}
        if read_rows:
            first, costs = _row_observations(folder/'rows.json',day['output_hashes'].get('rows.json'),
                name,scenario,plans,groups)
            bindings['rows_sha256'] = day['output_hashes']['rows.json']
            fill_bars = _fill_bars(input_path,first,plans)
        records.append(_day_record(day,source_version,cohort_id,bindings,plans=plans,groups=groups,
                                  first=first,costs=costs,fill_bars=fill_bars,read_rows=read_rows))
    calendar_status, calendar_evidence, accumulation = 'CALENDAR_REQUIRED', None, None
    if calendar_path is not None:
        calendar, calendar_file_sha = _read(Path(calendar_path),_sha(calendar_sha256))
        calendar_days = calendar.get('trading_days')
        if not isinstance(calendar_days,list) or calendar_days!=sorted(set(calendar_days)):
            _fail('CALENDAR_SORTED_UNIQUE_REQUIRED')
        for d in calendar_days:
            _date(d)
        if canonical_sha256(calendar_days)!=calendar.get('calendar_sha256') or not set(named)<=set(calendar_days):
            _fail('CALENDAR_PAYLOAD_OR_DATE_BINDING_MISMATCH')
        if not calendar.get('source_ref'):
            _fail('CALENDAR_SOURCE_REQUIRED')
        _sha(calendar.get('source_sha256'))
        available = len([d for d in calendar_days if d<=as_of])
        calendar_evidence = {**calendar,'file_sha256':calendar_file_sha,'missing_sessions':max(0,20-available),
                             'source_authentication':False}
        calendar_status = 'SHORT_CALENDAR' if available<20 else 'EXPLICIT_CALENDAR'
        if available>=20:
            accumulation = accumulate_strategy_days(records,trading_days=calendar_days,
                calendar_sha256=calendar['calendar_sha256'],as_of=as_of,source_kind='WP1_RESEARCH',
                source_version=source_version,cohort_id=cohort_id)
    elif calendar_sha256 is not None:
        _fail('CALENDAR_PATH_REQUIRED_WITH_HASH')
    return {'schema':'wp7-research-day-adapter-bundle/1','status':'DATA_LIMITED',
        'source_kind':'WP1_RESEARCH','source_version':source_version,'source_vector':source_vector,
        'cohort_id':cohort_id,'as_of':as_of,'days':records,'requested_trade_days':len(records),
        'complete_input_days':sum(r['coverage']['run_status']=='COMPLETE' for r in records),
        'calendar_status':calendar_status,'calendar_evidence':calendar_evidence,'accumulation':accumulation,
        'account_pnl':None,'source_mutation':False,'model_calls':0,'provider_calls':0,'notification_calls':0,
        'acceptance':{'CODE':'LOCAL_COMPONENT_TEST_REQUIRED','REPLAY':'HASH_BOUND_OFFLINE_RESEARCH_ONLY',
                      'OPERATIONS':'NOT_APPLICABLE_OFFLINE','STRATEGY':'PENDING_REAL_20_DAY_AND_SAMPLE_EVIDENCE'}}


def write_adapter_bundle(result, output):
    output = Path(output).resolve()
    if output.exists():
        _fail('REFUSE_OVERWRITE')
    output.mkdir(parents=True,exist_ok=False)
    hashes = {}
    def write(name,value):
        with (output/name).open('x',encoding='utf-8',newline='\n') as handle:
            json.dump(value,handle,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False)
            handle.write('\n')
        hashes[name] = file_sha256(output/name)
    for row in result['days']:
        write(row['summary']['trade_date']+'.json',row)
    write('adapter-report.json',{k:v for k,v in result.items() if k!='days'})
    if result['accumulation'] is not None:
        write('accumulation.json',result['accumulation'])
        with (output/'chapter.md').open('x',encoding='utf-8',newline='\n') as handle:
            handle.write(render_strategy_accumulation(result['accumulation']))
        hashes['chapter.md'] = file_sha256(output/'chapter.md')
    receipt = {'schema':'wp7-research-day-adapter-output/1','files':hashes,
        'status':result['status'],'requested_trade_days':result['requested_trade_days'],
        'complete_input_days':result['complete_input_days'],'calendar_status':result['calendar_status'],
        'source_kind':'WP1_RESEARCH','source_mutation':False}
    with (output/'manifest.json').open('x',encoding='utf-8',newline='\n') as handle:
        json.dump(receipt,handle,ensure_ascii=False,sort_keys=True,indent=2)
        handle.write('\n')
    return receipt
