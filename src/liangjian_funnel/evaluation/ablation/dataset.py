"""Validate immutable exported inputs without opening a provider or database."""
from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')
SCHEMA = 'liangjian-ablation-dataset/1'
WINDOW_FIELDS = ('plan_id', 'decision_time', 'observation_time', 'bars', 'market_context')


class DatasetError(ValueError):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     default=str).encode()).hexdigest()


def stamp(value):
    try:
        result = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise DatasetError('AWARE_TIMESTAMP_REQUIRED') from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise DatasetError('AWARE_TIMESTAMP_REQUIRED')
    return result.astimezone(TZ)


def session_minutes(day):
    day = date.fromisoformat(str(day))
    result = []
    for hour, minute in ((9, 31), (13, 1)):
        start = datetime(day.year, day.month, day.day, hour, minute, tzinfo=TZ)
        result.extend(start + timedelta(minutes=index) for index in range(120))
    return result


def _validate_bars(rows, symbol, *, through=None):
    from ...data.mootdx import MinuteBar
    stamps = set()
    for row in rows:
        try:
            bar = MinuteBar.model_validate(row)
        except ValueError as exc:
            raise DatasetError('MINUTE_CONTRACT_INVALID') from exc
        if bar.symbol != symbol or bar.volume_unit != 'shares':
            raise DatasetError('BAR_SYMBOL_OR_VOLUME_UNIT_MISMATCH')
        at = bar.bar_end
        if at in stamps:
            raise DatasetError('DUPLICATE_BAR')
        if through and at > through:
            raise DatasetError('FUTURE_TRIGGER_BAR')
        stamps.add(at)
    return stamps


def load_dataset(path: Path, expected_sha256: str | None = None):
    path = Path(path).resolve()
    raw = path.read_bytes()
    checksum = hashlib.sha256(raw).hexdigest()
    if expected_sha256 and checksum != expected_sha256:
        raise DatasetError('DATASET_HASH_MISMATCH')
    try:
        data = json.loads(raw)
        # Large real day exports need only their parsed graph during validation.
        del raw
        if data.get('schema_version') != SCHEMA:
            raise DatasetError('EXPORTED_EXECUTION_DATASET_REQUIRED_NOT_A5_PROJECTION')
        day = date.fromisoformat(data['trade_date'])
        plans, windows, archives = data['plans'], data['windows'], data['minute_archives']
        manifest = data.get('export_manifest')
        source_gaps = []
        if manifest is not None:
            if digest({key:value for key,value in manifest.items() if key != 'sha256'}) != manifest.get('sha256'):
                raise DatasetError('EXPORT_MANIFEST_HASH_MISMATCH')
            if (manifest.get('schema_version') != 'liangjian-ablation-export-manifest/1'
                    or manifest.get('source_path') != 'audit_frozen_a4_decisions.py'):
                raise DatasetError('FROZEN_A4_EXPORT_SOURCE_REQUIRED')
            activated = manifest.get('activated_plan_ids') or []
            actual = [record['plan_id'] for record in plans]
            if len(activated) != len(set(activated)) or set(activated) != set(actual):
                raise DatasetError('ACTIVATED_PLAN_CENSUS_MISMATCH')
            excluded = manifest.get('not_activated_plan_ids') or []
            if (set(activated) & set(excluded) or len(excluded) != len(set(excluded))
                    or manifest.get('candidate_plan_count') != len(activated)+len(excluded)
                    or manifest.get('window_count') != len(windows)):
                raise DatasetError('EXPORT_SOURCE_CENSUS_COUNT_MISMATCH')
            source_gaps = list(manifest.get('gaps') or [])
            if manifest.get('lifecycle_count') != 0:
                source_gaps.append({'reason':'POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE'})
        else:
            source_gaps.append({'reason':'ALL_ACTIVATED_PLAN_SOURCE_CENSUS_UNPROVEN'})
        if data.get('outcome_data_sha256') is not None and digest({
            'outcome_bars':data.get('outcome_bars', {}),
            'outcome_labels':data.get('outcome_labels', {})}) != data['outcome_data_sha256']:
            raise DatasetError('OUTCOME_DATA_HASH_MISMATCH')
        ids = {}
        for record in plans:
            pid, plan = record['plan_id'], record['payload']
            if pid in ids or digest(plan) != record['sha256']:
                raise DatasetError('PLAN_DUPLICATE_OR_HASH_MISMATCH')
            start, end = stamp(record['activated_at']), stamp(record['expires_at'])
            if start.date() != day or end < start:
                raise DatasetError('PLAN_ACTIVATION_OR_VALIDITY_INVALID')
            if plan.get('as_of') and stamp(plan['as_of']) > start:
                raise DatasetError('FUTURE_PLAN_FACTS')
            if record.get('invalidated_at'):
                invalid = stamp(record['invalidated_at'])
                if invalid < start:
                    raise DatasetError('INVALIDATION_BEFORE_ACTIVATION')
            ids[pid] = record
        archive_stamps = {}
        for symbol, archive in archives.items():
            payload = {key: archive[key] for key in ('one_minute', 'five_minute')}
            if digest(payload) != archive['sha256']:
                raise DatasetError('MINUTE_ARCHIVE_HASH_MISMATCH')
            archive_stamps[symbol] = _validate_bars(payload['one_minute'], symbol)
            _validate_bars(payload['five_minute'], symbol)
        unique = set()
        missing_context = set()
        for window in windows:
            pid = window['plan_id']
            if pid not in ids or digest({key:window[key] for key in WINDOW_FIELDS}) != window['input_sha256']:
                raise DatasetError('WINDOW_PLAN_OR_HASH_MISMATCH')
            dispatch, closed = stamp(window['decision_time']), stamp(window['observation_time'])
            if dispatch.date() != day or closed.date() != day or closed > dispatch:
                raise DatasetError('INVALID_FROZEN_CLOCKS')
            evaluated = stamp(window.get('evaluated_at') or window['decision_time'])
            if evaluated.date() != day or evaluated < dispatch:
                raise DatasetError('ACTUAL_DECISION_CLOCK_INVALID')
            if window.get('frozen_at'):
                if stamp(window['frozen_at']) > evaluated:
                    raise DatasetError('INPUT_FROZEN_AFTER_DECISION')
            else:
                missing_context.add((pid, dispatch))
            key = (pid, dispatch)
            if key in unique:
                raise DatasetError('DUPLICATE_DECISION_WINDOW')
            unique.add(key)
            record = ids[pid]
            end = min(stamp(record['expires_at']), stamp(record.get('invalidated_at') or record['expires_at']))
            if not stamp(record['activated_at']) <= dispatch <= end:
                raise DatasetError('WINDOW_OUTSIDE_PLAN_VALIDITY')
            _validate_bars(window['bars'], record['payload']['symbol'], through=closed)
            if manifest is not None:
                selected = (window.get('source_snapshots') or {}).get('1m')
                if (not selected or digest(window['bars']) != selected.get('bars_sha256')
                        or stamp(selected.get('decision_as_of')) != dispatch
                        or stamp(selected.get('captured_at')) > stamp(window['frozen_at'])
                        or len(str(selected.get('payload_sha256') or '')) != 64):
                    raise DatasetError('FROZEN_SNAPSHOT_SOURCE_METADATA_MISMATCH')
            context = window['market_context']
            for rows in (context.get('closed_bars') or {}).values():
                # 15m bars use the strategy's generic format, not MinuteBar.
                for row in rows:
                    if stamp(row.get('bar_end') or row.get('end')) > closed:
                        raise DatasetError('FUTURE_CONTEXT_CLOSED_BAR')
            market = context.get('live_market_state') or {}
            if market.get('as_of') and stamp(market['as_of']) > evaluated:
                raise DatasetError('FUTURE_FROZEN_MARKET')
            if market.get('trade_date') and market['trade_date'] != str(day):
                raise DatasetError('MARKET_WRONG_TRADE_DATE')
            if not market or market.get('status') != 'READY':
                missing_context.add((pid, dispatch))
            quote = context.get('realtime_quote') or {}
            for field in ('as_of', 'received_at', 'exchange_time', 'quote_time'):
                if quote.get(field) and stamp(quote[field]) > evaluated:
                    raise DatasetError('FUTURE_REALTIME_QUOTE')
        expected = set(session_minutes(day))
        coverage = []
        for pid, record in ids.items():
            symbol = record['payload']['symbol']
            points = {at for at in archive_stamps.get(symbol, ()) if at.date() == day}
            start = stamp(record['activated_at'])
            end = min(stamp(record['expires_at']), stamp(record.get('invalidated_at') or record['expires_at']))
            applicable = {at for at in expected if start <= at <= end}
            present = {at for key, at in unique if key == pid}
            coverage.append({'plan_id':pid, 'symbol':symbol, 'expected_1m_count':240,
                             'archived_1m_count':len(points & expected),
                             'missing_1m_count':len(expected-points),
                             'expected_decisions':len(applicable), 'decision_count':len(present),
                             'missing_decisions':len(applicable-present)})
        limited = not plans or bool(source_gaps) or any(row['missing_1m_count'] or row['missing_decisions'] for row in coverage)
        data['coverage'] = {'status':'DATA_LIMITED' if limited or missing_context else 'COMPLETE',
                            'plans':coverage, 'context_or_arrival_unproven_windows':len(missing_context),
                            'all_activated_plan_census_proven':manifest is not None,
                            'source_evidence_gaps':source_gaps}
        data['source'] = {'path':str(path), 'sha256':checksum}
        return data
    except DatasetError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise DatasetError('DATASET_REQUIRED_FIELD_MISSING_OR_INVALID') from exc


def summarize_coverage(datasets, *, required_days=15):
    days = {data['trade_date'] for data in datasets}
    complete = {data['trade_date'] for data in datasets if data['coverage']['status'] == 'COMPLETE'}
    return {'covered_trade_days':len(days), 'complete_trade_days':len(complete),
            'required_trade_days':required_days,
            'status':'COMPLETE' if len(complete) >= required_days else 'INSUFFICIENT_EVIDENCE',
            'days':[{'trade_date':data['trade_date'], **data['coverage']} for data in datasets]}
