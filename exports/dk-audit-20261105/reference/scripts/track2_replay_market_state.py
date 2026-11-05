"""Bounded original-file projection audit; no provider, DB or runtime writes.

Run through the local test ownership wrapper, after the allowed time window.
This audits exact projections. It does not claim full-day business replay.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from liangjian_funnel.market_state.contract import (  # noqa: E402
    SOURCES, MarketStateFacts, PitProof, SourceInput, build_market_state,
    legacy_projection, load_vocabulary,
)
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar  # noqa: E402


def _json(data):
    def pairs(items):
        obj = {}
        for key, value in items:
            if key in obj:
                raise ValueError('REPLAY_DUPLICATE_JSON_KEY')
            obj[key] = value
        return obj
    def constant(_):
        raise ValueError('REPLAY_NONFINITE_JSON')
    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


def _original(case, parent, cache=None):
    path = Path(case['path'])
    if not path.is_absolute():
        path = parent / path
    if (not path.is_file() or any(p.is_symlink() for p in (path, *path.parents))
            or path.stat().st_size > 8 * 1024 * 1024):
        raise ValueError('REPLAY_ORIGINAL_FILE_UNAVAILABLE_OR_OVERSIZE')
    key = (str(path.resolve()), case['file_sha256'])
    saved = cache.get(key) if cache is not None else None
    data = saved['raw'] if saved is not None else path.read_bytes()
    if (not isinstance(case['file_sha256'], str)
            or re.fullmatch('[0-9a-f]{64}', case['file_sha256']) is None
            or hashlib.sha256(data).hexdigest() != case['file_sha256']):
        raise ValueError('REPLAY_ORIGINAL_FILE_HASH_MISMATCH')
    if saved is None and cache is not None and len(cache) < 2:
        saved = dict(raw=data, parsed=None)
        cache[key] = saved
    pointer = case.get('raw_json_pointer', '')
    encoding = case.get('input_encoding', 'EXACT_JSON_TEXT')
    if encoding not in {'EXACT_JSON_TEXT', 'CANONICAL_SELECTED_OBJECT', 'CANONICAL_DECODED_OBJECT'}:
        raise ValueError('REPLAY_INPUT_ENCODING_INVALID')
    if not isinstance(pointer, str):
        raise ValueError('REPLAY_RAW_JSON_POINTER_INVALID')
    if not pointer and encoding == 'EXACT_JSON_TEXT':
        return data
    if not pointer.startswith('/'):
        raise ValueError('REPLAY_RAW_JSON_POINTER_INVALID')
    if saved is not None:
        if saved['parsed'] is None:
            saved['parsed'] = _json(data)
        value = saved['parsed']
    else:
        value = _json(data)
    for part in pointer[1:].split('/'):
        key = part.replace('~1', '/').replace('~0', '~')
        value = value[int(key)] if isinstance(value, list) and key.isdigit() else value[key]
    if encoding == 'CANONICAL_DECODED_OBJECT':
        if not isinstance(value, str):
            raise ValueError('REPLAY_DECODED_JSON_TEXT_REQUIRED')
        value = _json(value)
        decoded_pointer = case.get('decoded_json_pointer')
        if not isinstance(decoded_pointer, str) or not decoded_pointer.startswith('/'):
            raise ValueError('REPLAY_DECODED_JSON_POINTER_INVALID')
        for part in decoded_pointer[1:].split('/'):
            key = part.replace('~1', '/').replace('~0', '~')
            value = value[int(key)] if isinstance(value, list) and key.isdigit() else value[key]
    if encoding in {'CANONICAL_SELECTED_OBJECT', 'CANONICAL_DECODED_OBJECT'}:
        if not isinstance(value, dict):
            raise ValueError('REPLAY_SELECTED_OBJECT_REQUIRED')
        # Explicitly labelled object selection: input_sha256 binds this new
        # encoding. source_file_sha256 binds the unchanged backing capture.
        # This is never claimed to be exact original provider reply bytes.
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
    if not isinstance(value, str):
        raise ValueError('REPLAY_NESTED_ORIGINAL_MUST_BE_JSON_TEXT')
    return value.encode('utf-8')


def audit_manifest(manifest_path, vocabulary_path, *, calendar):
    manifest_path = Path(manifest_path)
    manifest_bytes = manifest_path.read_bytes()
    if len(manifest_bytes) > 2 * 1024 * 1024:
        raise ValueError('REPLAY_MANIFEST_OVERSIZE')
    manifest = _json(manifest_bytes)
    if manifest['schema_version'] != 'track2-market-state-replay/1':
        raise ValueError('REPLAY_MANIFEST_VERSION')
    start, end = date.fromisoformat(manifest['start']), date.fromisoformat(manifest['end'])
    cases = manifest['cases']
    if not 0 <= (end-start).days <= 40 or not isinstance(cases, list) or len(cases) > 5000:
        raise ValueError('REPLAY_MANIFEST_SCOPE')
    days = []
    day = start
    while day <= end:
        if calendar.is_trading_day(day):
            days.append(day.isoformat())
        day += timedelta(days=1)
    if not days:
        raise ValueError('REPLAY_NO_TRADING_DAYS')
    vocabulary = load_vocabulary(vocabulary_path)
    results, covered, identities, cache = [], set(), set(), {}
    source_hashes = set()
    for case in cases:
        name, day = case['source_name'], case['trade_date']
        if name not in SOURCES or day not in days or not isinstance(case['case_id'], str) or not case['case_id']:
            raise ValueError('REPLAY_CASE_SCOPE')
        identity = (day, name, case['case_id'])
        if identity in identities:
            raise ValueError('REPLAY_CASE_DUPLICATE_IDENTITY')
        identities.add(identity)
        record = dict(trade_date=day, source_name=name, case_id=case['case_id'])
        try:
            path = Path(case['path'])
            if not path.is_absolute():
                path = manifest_path.parent / path
            source_hashes.add((str(path.resolve()), case['file_sha256']))
            original = _original(case, manifest_path.parent, cache)
            raw = _json(original)
            proof = PitProof(**case['pit'])
            if day != proof.source_trade_date:
                raise ValueError('REPLAY_SOURCE_COVERAGE_DATE_MISMATCH')
            frame = build_market_state({name: SourceInput(case['source_id'], original, proof)},
                source_trade_date=day, target_trade_date=proof.target_trade_date, vocabulary=vocabulary)
            exact = legacy_projection(frame, name) == raw
            # JSON round-trip must preserve the exact values consumers see.
            decoded = MarketStateFacts.model_validate_json(frame.model_dump_json())
            exact = exact and legacy_projection(decoded, name) == raw
            if not exact:
                raise ValueError('REPLAY_PROJECTION_DIFFERENCE')
            component = frame.components[name]
            record.update(status='PROJECTION_EQUAL', input_sha256=component.input_sha256,
                          source_file_sha256=case['file_sha256'], component_status=component.status,
                          input_encoding=case.get('input_encoding', 'EXACT_JSON_TEXT'),
                          source_pointer=case.get('raw_json_pointer', ''),
                          decoded_json_pointer=case.get('decoded_json_pointer'),
                          evidence_class=case.get('evidence_class', 'DECLARED_CAPTURE'),
                          pit=component.pit.model_dump(),
                          reasons=component.reasons,
                          tokens={key: token.model_dump() for key, token in component.tokens.items()})
            covered.add((day, name))
        except (OSError, ValueError, KeyError, TypeError, IndexError) as exc:
            # Never dump payloads, paths or third-party exception text.
            code = str(exc) if type(exc) is ValueError and re.fullmatch('[A-Z0-9_]{1,100}', str(exc)) else 'REPLAY_INPUT_REJECTED'
            record.update(status='REJECTED', exception_type=type(exc).__name__, reason_code=code)
        results.append(record)
    missing = [dict(trade_date=day, source_name=name, status='UNCOVERED_NO_ORIGINAL') for day in days for name in sorted(SOURCES)
               if (day, name) not in covered]
    rejected = sum(row['status'] == 'REJECTED' for row in results)
    changed_sources = []
    # Cache size bounds memory, never the number of originals rechecked.
    for path, expected_sha in sorted(source_hashes):
        try:
            original_path = Path(path)
            stable = (original_path.is_file() and original_path.stat().st_size <= 8*1024*1024
                      and not any(p.is_symlink() for p in (original_path, *original_path.parents))
                      and hashlib.sha256(original_path.read_bytes()).hexdigest() == expected_sha)
        except OSError:
            stable = False
        if not stable:
            changed_sources.append(dict(path=path, expected_sha256=expected_sha))
    return dict(schema_version='track2-market-state-projection-audit/1',
        status='FAILED_SOURCE_CHANGED' if changed_sources else 'PARTIAL' if missing or rejected
            else 'PROJECTION_COVERAGE_COMPLETE_NOT_BUSINESS_ACCEPTANCE',
        behavior_replay='NOT_RUN', d5_full_day_replay='NOT_RUN', d8_external_snapshots='NOT_RUN',
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(), vocabulary_sha256=vocabulary['config_sha256'],
        expected_trade_dates=days, expected_source_names=sorted(SOURCES), missing=missing,
        counts=dict(Counter(row['status'] for row in results)), cases=results, changed_sources=changed_sources,
        original_files_rechecked=len(source_hashes),
        coverage_matrix=[dict(trade_date=day, source_name=name,
            projection_objects=sum(row['trade_date'] == day and row['source_name'] == name
                and row['status'] == 'PROJECTION_EQUAL' for row in results),
            pit_complete_objects=sum(row['trade_date'] == day and row['source_name'] == name
                and row['status'] == 'PROJECTION_EQUAL' and row['component_status'] != 'DATA_BLOCK' for row in results))
            for day in days for name in sorted(SOURCES)],
        provider_calls=0, model_calls=0, production_writes=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--vocabulary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--schema-output', type=Path)
    args = parser.parse_args()
    report = audit_manifest(args.manifest, args.vocabulary, calendar=ExchangeTradingCalendar())
    report['actual_run_at'] = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    with args.output.open('x', encoding='utf-8') as output:
        json.dump(report, output, ensure_ascii=False, sort_keys=True, indent=2)
    if args.schema_output:
        with args.schema_output.open('x', encoding='utf-8') as output:
            json.dump(MarketStateFacts.model_json_schema(), output, ensure_ascii=False, sort_keys=True, indent=2)
    print(json.dumps(dict(status=report['status'], output=str(args.output), counts=report['counts'],
                         missing_count=len(report['missing'])), ensure_ascii=True))


if __name__ == '__main__':
    main()
