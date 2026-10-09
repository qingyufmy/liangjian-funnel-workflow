"""Bound night cache work to the original close scope; no strategy authority."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import time
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from ..reporting import atomic_write_json
from ..runtime.bounded_work import BoundedWorkGate
from .close_scope import build_scope_ledger
from .feature_store import content_hash

SHANGHAI = ZoneInfo('Asia/Shanghai')
# Shared across attempts: an uncancellable dependency keeps its slot after
# timeout. Retrying cannot spawn an unbounded collection of late writers.
_MAINTENANCE_GATE = BoundedWorkGate(1)


def _verify_hash(value: Mapping[str, Any], key: str, reason: str) -> None:
    if content_hash({k: v for k, v in value.items() if k != key}) != value.get(key):
        raise ValueError(reason)


def _aware(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError('MAINTENANCE_TIME_NOT_AWARE')
    return result.astimezone(SHANGHAI)


def build_maintenance_queue(receipt: Mapping[str, Any], prefilter: Mapping[str, Any]) -> dict:
    """Only the original frozen union can define this cache-maintenance scope.

    Keep SHADOW semantics: this queue does not narrow the formal close query.
    It exists before final A2 results, including when the research later fails.
    """
    _verify_hash(receipt, 'receipt_hash', 'CLOSE_SCOPE_RECEIPT_HASH_MISMATCH')
    _verify_hash(prefilter, 'scope_hash', 'DISCLOSURE_PREFILTER_HASH_MISMATCH')
    if receipt.get('schema_version') != 'close-scope-receipt/1' or prefilter.get('schema_version') != 'disclosure-prefilter/1':
        raise ValueError('MAINTENANCE_INPUT_SCHEMA_MISMATCH')
    reference = receipt.get('a1_reference')
    if (receipt.get('binding_status') != 'ORIGINAL_RUN_REFERENCE'
            or not isinstance(reference, Mapping) or not reference.get('generation_id')):
        raise ValueError('ORIGINAL_A1_REFERENCE_REQUIRED')
    scope = receipt['scope']
    _verify_hash(scope, 'scope_hash', 'CLOSE_SCOPE_LEDGER_HASH_MISMATCH')
    sets = scope['source_sets']
    expected = build_scope_ledger(a1_symbols=sets['A1'], hot_symbols=sets['HOT100'],
        discovery_symbols=sets['EARLY_DISCOVERY'], g0_symbols=scope['g0_symbols'])
    if expected != scope:
        raise ValueError('CLOSE_SCOPE_LEDGER_INCONSISTENT')
    market = _aware(receipt['market_data_as_of'])
    research = _aware(receipt['research_as_of'])
    if market > research or market.date().isoformat() != prefilter['trade_date']:
        raise ValueError('MAINTENANCE_MARKET_DATE_MISMATCH')
    if any(prefilter['source_sets'][key] != sets[source]
           for key, source in [('hot100', 'HOT100'), ('discovery', 'EARLY_DISCOVERY')]):
        raise ValueError('MAINTENANCE_SOURCE_SET_MISMATCH')
    selected = scope['selected_symbols']
    if any(not re.fullmatch(r'\d{6}\.(?:SH|SZ|BJ)', s) for s in selected):
        raise ValueError('MAINTENANCE_SYMBOL_INVALID')
    candidates, deferred = prefilter['candidate_symbols'], prefilter['deferred_symbols']
    if (prefilter['symbols'] != selected
            or candidates != sorted(set(candidates)) or deferred != sorted(set(deferred))
            or set(candidates) & set(deferred) or sorted(candidates + deferred) != selected):
        raise ValueError('MAINTENANCE_PARTITION_MISMATCH')
    records = prefilter['records']
    if ([r['symbol'] for r in records] != selected
            or [r['symbol'] for r in records if r['status'] == 'COLLECT_DISCLOSURE'] != candidates
            or [r['symbol'] for r in records if r['status'] == 'DEFERRED_DISCLOSURE_NOT_COLLECTED'] != deferred):
        raise ValueError('MAINTENANCE_RECORD_PARTITION_MISMATCH')
    value = {'schema_version': 'disclosure-maintenance-queue/1', 'mode': 'SHADOW',
        'execution_authority': False, 'changes_query_scope': False,
        'run_id': receipt['run_id'], 'a1_reference': dict(reference),
        'market_data_as_of': receipt['market_data_as_of'],
        'research_as_of': receipt['research_as_of'],
        'source_receipt_hash': receipt['receipt_hash'], 'prefilter_hash': prefilter['scope_hash'],
        'candidate_symbols': list(candidates), 'deferred_symbols': list(deferred),
        'source_receipt': dict(receipt), 'prefilter': dict(prefilter)}
    value['queue_hash'] = content_hash(value)
    return value


def validate_queue(value: Mapping[str, Any]) -> None:
    _verify_hash(value, 'queue_hash', 'MAINTENANCE_QUEUE_HASH_MISMATCH')
    if build_maintenance_queue(value['source_receipt'], value['prefilter']) != value:
        raise ValueError('MAINTENANCE_QUEUE_BINDING_MISMATCH')


def seal_maintenance_queue(root: Path, receipt: Mapping[str, Any], prefilter: Mapping[str, Any]) -> Path:
    queue = build_maintenance_queue(receipt, prefilter)
    path = Path(root)/f"queue-{prefilter['trade_date']}-{queue['queue_hash']}.json"
    if path.exists():
        if json.loads(path.read_text(encoding='utf-8')) != queue:
            raise ValueError('MAINTENANCE_QUEUE_FILE_CONFLICT')
    else:
        atomic_write_json(path, queue)
    return path


def run_maintenance(queue: Mapping[str, Any], *, output_dir: Path, now: datetime,
                    execute: bool = False, collect: Callable | None = None,
                    can_reuse: Callable | None = None, budget_seconds: float = 3600,
                    dependency_timeout_seconds: float = 60) -> dict:
    """Journal each task independently, then seal a new immutable report.

    Resume needs BOTH a valid success receipt and currently usable caches.
    Receipts are not themselves evidence of disclosure freshness. Query dates
    come from real observation time, not the earlier market/research cutoff.
    """
    validate_queue(queue)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('MAINTENANCE_TIME_NOT_AWARE')
    now = now.astimezone(SHANGHAI)
    if now < _aware(queue['research_as_of']) or now < _aware(queue['source_receipt']['recorded_at']):
        # Tests may freeze receipt creation explicitly; production may never
        # claim collection before the actual receipt existed.
        raise ValueError('MAINTENANCE_BEFORE_SOURCE_RECEIPT')
    if not 0 < budget_seconds <= 3600:
        raise ValueError('MAINTENANCE_BUDGET_INVALID')
    if not 0 < dependency_timeout_seconds <= 60:
        raise ValueError('MAINTENANCE_DEPENDENCY_TIMEOUT_INVALID')
    query_end = now.date().isoformat()
    value = {'schema_version': 'disclosure-maintenance-report/1',
        'queue_hash': queue['queue_hash'], 'source_receipt_hash': queue['source_receipt_hash'],
        'a1_reference': queue['a1_reference'], 'started_at': now.isoformat(),
        'query_start': (now.date()-timedelta(days=10)).isoformat(), 'query_end': query_end,
        'business_query_start': (now.date()-timedelta(days=450)).isoformat(),
        'candidate_symbols': queue['candidate_symbols'],
        'task_symbols': queue['deferred_symbols'], 'rows': [], 'resumed_symbols': [],
        'production_plans_changed': False, 'execution_authority': False,
        'maintenance_scope': 'BUSINESS_AND_PDF_ONLY', 'recent_required': False,
        'status': 'DRY_RUN'}
    if not execute:
        return value
    if collect is None or can_reuse is None:
        raise ValueError('MAINTENANCE_COLLECTOR_REQUIRED')
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    lock = output_dir/f"{queue['queue_hash']}.lock"
    try:
        handle = lock.open('x', encoding='utf-8')
    except FileExistsError as exc:
        raise ValueError('MAINTENANCE_ALREADY_RUNNING') from exc
    attempt_id = uuid4().hex
    started = time.monotonic()
    deadline = started + budget_seconds
    try:
        with handle:
            handle.write(attempt_id)
        previous: dict[str, dict] = {}
        for path in sorted(output_dir.glob(f"{queue['queue_hash']}-attempt-*.json")):
            record = json.loads(path.read_text(encoding='utf-8'))
            _verify_hash(record, 'attempt_hash', 'MAINTENANCE_ATTEMPT_HASH_MISMATCH')
            if record['queue_hash'] != queue['queue_hash'] or record['symbol'] not in queue['deferred_symbols']:
                raise ValueError('MAINTENANCE_ATTEMPT_SCOPE_MISMATCH')
            timestamp = _aware(record['observed_at'])
            if (record['query_end'] == query_end and timedelta(0) <= now-timestamp < timedelta(hours=6)
                    and record['row'].get('ok') is True
                    and (record['symbol'] not in previous or timestamp > _aware(previous[record['symbol']]['observed_at']))):
                previous[record['symbol']] = record
        for symbol in queue['deferred_symbols']:
            if time.monotonic()-started >= budget_seconds:
                row = {'symbol': symbol, 'ok': False, 'reason_code': 'MAINTENANCE_BUDGET_EXHAUSTED'}
            else:
                # Bind loop values: a late worker must not consume a later
                # symbol or mutate this attempt's journal after its deadline.
                def operation(symbol=symbol, prior=previous.get(symbol)):
                    if prior is not None and can_reuse(prior['row'], now) is True:
                        return dict(prior['row']), True
                    return dict(collect(symbol, value['query_start'], query_end,
                                        value['business_query_start'])), False
                result = _MAINTENANCE_GATE.call(operation,
                    deadline=min(deadline, time.monotonic()+dependency_timeout_seconds),
                    name='disclosure-cache-maintenance')
                if result.status != 'READY':
                    row = {'symbol': symbol, 'ok': False,
                           'reason_code': result.value if result.status == 'FAILED' else result.reason_code}
                else:
                    row, resumed = result.value
                    if row.get('symbol') != symbol:
                        row = {'symbol': symbol, 'ok': False, 'reason_code': 'COLLECTOR_SYMBOL_MISMATCH'}
                    row['ok'] = row.get('ok') is True and all(row.get(k) is True
                        for k in ('business_complete', 'pdf_complete'))
                    if not row['ok']:
                        row['reason_code'] = row.get('reason_code') or 'MAINTENANCE_INCOMPLETE'
                    elif resumed:
                        value['resumed_symbols'].append(symbol)
            value['rows'].append(row)
            record = {'queue_hash': queue['queue_hash'], 'symbol': symbol,
                'query_end': query_end,
                'observed_at': (now + timedelta(seconds=time.monotonic()-started)).isoformat(), 'row': row}
            record['attempt_hash'] = content_hash(record)
            atomic_write_json(output_dir/f"{queue['queue_hash']}-attempt-{attempt_id}-{symbol}.json", record)
        value['failed_symbols'] = [row['symbol'] for row in value['rows'] if not row['ok']]
        value['status'] = 'PARTIAL_FAILURE' if value['failed_symbols'] else 'CACHE_WARMED'
        value['elapsed_seconds'] = time.monotonic()-started
        value['report_hash'] = content_hash(value)
        atomic_write_json(output_dir/f"{queue['queue_hash']}-report-{attempt_id}.json", value)
        return value
    finally:
        lock.unlink(missing_ok=True)
