"""Local frozen shadow-input audit and, when evidenced, first-trigger golden.

No provider, DB, outcome, model or production write. No historical field invention.
One day is loaded at a time; original large matrix rows are streamed only if
at least one original plan supplies the complete shadow-input evidence contract.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime
import hashlib
import inspect
import json
from pathlib import Path
import subprocess

import ijson

from liangjian_funnel.evaluation.ablation import engine
from liangjian_funnel.evaluation.ablation.dataset import digest, load_dataset, stamp
from liangjian_funnel.evaluation.ablation.runner import _projection
from liangjian_funnel.runtime.shadow_variants import (
    ShadowVariantEngine, SHADOW_INPUT_SCHEMA, SHADOW_VARIANTS_V1,
    VARIANT_SET_VERSION, prepare_shadow_plan,
)

DEFAULT_DATES = ('2026-09-10','2026-09-11','2026-09-23','2026-09-24','2026-09-29','2026-09-30')


def file_hash(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda:source.read(1024*1024), b''):
            result.update(block)
    return result.hexdigest()


def presence(value, key):
    return 'MISSING' if key not in value else 'NULL' if value[key] is None else 'EMPTY' if value[key] in ([],{}) else 'PRESENT'


def adapt_frozen_plan(record, *, trade_date, source_ref):
    """Explicit append-only offline conversion; no source/reference substitution."""
    original = record['payload']
    if digest(original) != record['sha256']:
        raise ValueError('PLAN_HASH_MISMATCH')
    daily = engine.production._daily_context(original)
    fields = ('ma5','atr14','previous_daily_closes','previous_daily_close_dates','daily_as_of','atr_source_hash')
    evidence = {'plan_id':record['plan_id'], 'profile':original.get('strategy_profile'),
        'source_ref':source_ref+'#plan:'+record['plan_id'], 'source_plan_sha256':record['sha256'],
        'field_presence':{key:presence(daily,key) for key in fields},
        'status':'DATA_LIMITED', 'reason':None, 'conversion':None}
    research = deepcopy(original)
    facts = research.get('strategy_facts')
    existing = facts.get('shadow_inputs') if isinstance(facts,dict) else None
    if existing is not None:
        evidence['conversion'] = 'EXISTING_SHADOW_INPUTS'
    elif not all(evidence['field_presence'][key] == 'PRESENT' for key in fields):
        evidence['reason'] = 'FROZEN_DAILY_SHADOW_FIELDS_INCOMPLETE'
        return None, evidence
    else:
        evidence['conversion'] = 'FROZEN_DAILY_CONTEXT_TO_SHADOW_INPUTS/1'
        values = {key:deepcopy(daily[key]) for key in fields if key != 'ma5'}
        values.update(schema_version=SHADOW_INPUT_SCHEMA, daily_ma5=daily['ma5'],
            source_ref=evidence['source_ref'], original_plan_sha256=record['sha256'],
            conversion_contract=evidence['conversion'])
        research.setdefault('strategy_facts', {})['shadow_inputs'] = values
    _, checksum, reason = prepare_shadow_plan(research,
        decision_time=datetime.fromisoformat(trade_date+'T09:31:00+08:00'))
    evidence['shadow_inputs_sha256'] = checksum
    evidence['reason'] = reason
    if reason is not None:
        return None, evidence
    evidence['status'] = 'ELIGIBLE'
    return research, evidence


def compare_first_triggers(actual, expected, *, eligible_windows):
    if not eligible_windows or expected is None:
        return {'status':'DATA_LIMITED', 'golden_pass':False, 'difference_count':None,
            'actual_first_trigger_count':len(actual) if eligible_windows else None,
            'expected_first_trigger_count':len(expected) if expected is not None and eligible_windows else None,
            'reason':'NO_ELIGIBLE_WINDOWS' if not eligible_windows else 'REFERENCE_UNAVAILABLE'}
    differences = [{'key':key, 'actual':actual.get(key), 'expected':expected.get(key)}
        for key in sorted(actual.keys() | expected.keys()) if actual.get(key) != expected.get(key)]
    return {'status':'MATCHED' if not differences else 'DIFFERENT', 'golden_pass':not differences,
        'difference_count':len(differences), 'actual_first_trigger_count':len(actual),
        'expected_first_trigger_count':len(expected), 'differences_sha256':digest(differences),
        'difference_samples':differences[:20]}


def matrix_receipt(matrix, day):
    path = matrix/day
    receipt_path, report_path = path/'day-receipt.json', path/'report.json'
    receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
    report_sha = file_hash(report_path)
    if report_sha != receipt['output_hashes']['report.json']:
        raise ValueError('MATRIX_REPORT_HASH_MISMATCH')
    if receipt.get('status') != 'COMPLETE' or receipt.get('baseline_status') != 'MATCHED':
        raise ValueError('COMPLETE_MATCHED_ORIGINAL_MATRIX_REQUIRED')
    report = json.loads(report_path.read_text(encoding='utf-8'))
    scenarios = {spec.matrix_scenario for spec in SHADOW_VARIANTS_V1}
    groups = [group for group in report['statistics']['groups'] if group.get('scenario') in scenarios]
    return {'path':str(receipt_path), 'sha256':file_hash(receipt_path),
        'report_sha256':report_sha, 'rows_expected_sha256':receipt['output_hashes']['rows.json'],
        'original_strategy_sha256':report['strategy_source_sha256'],
        'original_strategy_lf_sha256':report.get('strategy_source_lf_sha256'),
        'original_implementation_sha256':report.get('implementation_source_sha256'),
        'input_sha256':receipt['input_sha256'],
        'window_count':receipt['input_window_count'],
        'groups':[{key:group.get(key) for key in ('profile','scenario','valid_window_count',
            'data_limited_window_count','minute_trigger_count','triggered_plan_count')} for group in groups],
        'rows_read':False}


def replay_day(path, adapted, matrix, day, matrix_info):
    data = load_dataset(path)
    if data['coverage']['status'] != 'COMPLETE':
        raise ValueError('COMPLETE_DAY_REQUIRED')
    originals = {record['plan_id']:record['payload'] for record in data['plans']}
    service, actual, failures, windows = ShadowVariantEngine(), {}, [], 0
    for window in sorted(data['windows'], key=lambda value:(value['decision_time'],value['plan_id'])):
        pid = window['plan_id']
        if pid not in adapted:
            continue
        at, now = stamp(window['decision_time']), stamp(window['observation_time'])
        baseline = engine.production.evaluate_strategy(originals[pid], window['bars'],
            now=now, decision_time=at, market_context=window['market_context']).model_dump(mode='json')
        action, primary = _projection(baseline)
        if action != window.get('recorded_action') or 'recorded_reason' not in window or primary != window['recorded_reason']:
            failures.append({'plan_id':pid,'minute':at.isoformat(),'reason':'ORIGINAL_BASELINE_MISMATCH_OR_MISSING'})
            continue
        result = service.evaluate_minute([dict(plan_id=pid,plan=adapted[pid],bars=window['bars'],
            baseline=baseline,now=now,decision_time=at,market_context=window['market_context'])], minute=at)
        windows += 1
        if result['minute_summary']['status'] != 'OK':
            failures.append({'plan_id':pid,'minute':at.isoformat(),'reason':result['minute_summary']['status']})
        for signal in result['signals']:
            if signal['status'] != 'OK':
                failures.append({'plan_id':pid,'minute':at.isoformat(),'reason':signal['reason'] or signal['status']})
            if signal['event_kind'] == 'FIRST_TRIGGER':
                actual[pid+'|'+signal['variant_id']] = at.isoformat()
    expected, checksum = {}, hashlib.sha256()
    class HashingReader:
        def __init__(self, raw): self.raw = raw
        def read(self, size=-1):
            chunk = self.raw.read(size); checksum.update(chunk); return chunk
        def readinto(self, buffer):
            chunk = self.read(len(buffer)); buffer[:len(chunk)] = chunk; return len(chunk)
    mapping = {spec.matrix_scenario:spec.variant_id for spec in SHADOW_VARIANTS_V1}
    with (matrix/day/'rows.json').open('rb') as raw:
        for row in ijson.items(HashingReader(raw),'item',use_float=True):
            if row.get('plan_id') not in adapted or row.get('scenario') not in mapping:
                continue
            if (row.get('ablation') or {}).get('status') != 'OK' or row.get('action') not in ('BUY_SIGNAL','ADD_SIGNAL'):
                continue
            key = row['plan_id']+'|'+mapping[row['scenario']]
            minute = row['minute']
            if key not in expected or minute < expected[key]: expected[key] = minute
    if checksum.hexdigest() != matrix_info['rows_expected_sha256']:
        raise ValueError('MATRIX_ROWS_HASH_MISMATCH')
    matrix_info['rows_read'] = True
    compared = compare_first_triggers(actual, expected, eligible_windows=windows)
    compared.update(eligible_windows=windows, evaluation_failure_count=len(failures),
        evaluation_failures_sha256=digest(failures), evaluation_failure_samples=failures[:20])
    if failures: compared.update(status='DATA_LIMITED',golden_pass=False)
    return compared


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle',type=Path,required=True)
    parser.add_argument('--matrix',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--date',action='append',dest='dates')
    args = parser.parse_args(argv)
    try:
        if args.output.exists(): raise FileExistsError('UNIQUE_NEW_OUTPUT_REQUIRED')
        dates = args.dates or DEFAULT_DATES
        if len(dates) != len(set(dates)): raise ValueError('DUPLICATE_DAY')
        bundle_manifest = args.bundle/'manifest.json'
        expected = json.loads(bundle_manifest.read_text(encoding='utf-8'))['files']
        report = {'schema_version':'w1-shadow-offline-golden/1','cohort':'WP1_RESEARCH',
            'variant_set_version':VARIANT_SET_VERSION,'production_mutation':False,'model_calls':0,
            'scope':'FROZEN_INPUT_FIRST_TRIGGER_GOLDEN_NOT_15_DAY_RETURN_ACCEPTANCE',
            'created_at':datetime.now().astimezone().isoformat(),
            'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
            'bundle_manifest_sha256':file_hash(bundle_manifest),'source_hashes':{
                str(Path(path).resolve()):file_hash(path) for path in
                (__file__,inspect.getfile(ShadowVariantEngine),inspect.getfile(engine),inspect.getfile(engine.production))},
            'days':[]}
        for day in dates:
            path = args.bundle/(day+'.json')
            before = file_hash(path)
            if before != expected[path.name]: raise ValueError('DATASET_HASH_MISMATCH')
            info, records, adapted = matrix_receipt(args.matrix,day), [], {}
            if info['input_sha256'] != before:
                raise ValueError('MATRIX_REFERENCES_DIFFERENT_INPUT')
            with path.open('rb') as source:
                for record in ijson.items(source,'plans.item',use_float=True):
                    plan, evidence = adapt_frozen_plan(record,trade_date=day,source_ref=str(path.resolve()))
                    records.append(evidence)
                    if plan is not None and record['payload'].get('strategy_profile') in ('TREND_MA5','MA520_SWING'):
                        adapted[record['plan_id']] = plan
            comparison = replay_day(path,adapted,args.matrix,day,info) if adapted else compare_first_triggers({},None,eligible_windows=0)
            after = file_hash(path)
            if before != after: raise ValueError('SOURCE_CHANGED_DURING_AUDIT')
            report['days'].append({'trade_date':day,'source_ref':str(path.resolve()),'source_sha256':before,
                'source_sha256_after':after,'plan_count':len(records),'eligible_plan_count':len(adapted),
                'original_window_count':info['window_count'],'plans':records,'matrix':info,'comparison':comparison})
        report['golden_pass'] = bool(report['days']) and all(day['comparison']['golden_pass'] and
            day['eligible_plan_count'] == day['plan_count'] for day in report['days'])
        report['status'] = 'MATCHED' if report['golden_pass'] else 'DATA_LIMITED'
        report['requested_plan_count'] = sum(day['plan_count'] for day in report['days'])
        report['eligible_plan_count'] = sum(day['eligible_plan_count'] for day in report['days'])
        report['requested_window_count'] = sum(day['original_window_count'] for day in report['days'])
        report['native_exit_code'] = 0 if report['golden_pass'] else 2
        args.output.mkdir(parents=True)
        result_path = args.output/'report.json'
        result_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        print(json.dumps({'report':str(result_path),'sha256':file_hash(result_path),
            'status':report['status'],'golden_pass':report['golden_pass'],
            'plans':report['requested_plan_count'],'eligible':report['eligible_plan_count']},ensure_ascii=False))
        return report['native_exit_code']
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({'status':'BLOCKED','reason':str(exc),'production_mutation':False},ensure_ascii=False))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
