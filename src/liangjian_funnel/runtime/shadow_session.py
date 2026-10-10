"""Independent read-only monitor consumer. Never dispatch A4 or acquire data."""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid
import zlib

from ..data.mootdx import MinuteBar
from .shadow_variants import ShadowVariantEngine


def _stamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError('AWARE_CLOCK_REQUIRED')
    from zoneinfo import ZoneInfo
    return result.astimezone(ZoneInfo('Asia/Shanghai'))


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _audit_helpers():
    path = Path(__file__).resolve().parents[3] / 'scripts/audit_frozen_a4_decisions.py'
    spec = importlib.util.spec_from_file_location('_shadow_frozen_audit_helpers', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # Only pure helpers; main/REMOTE never executed.
    return module


def source_hashes():
    root = Path(__file__).resolve().parents[3]
    paths = [Path(__file__), root/'scripts/run_shadow_session.py', root/'scripts/audit_frozen_a4_decisions.py',
        root/'src/liangjian_funnel/workflow.py', root/'src/liangjian_funnel/runtime/strategies.py',
        root/'src/liangjian_funnel/runtime/shadow_variants.py', root/'src/liangjian_funnel/runtime/shadow_evidence.py']
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def validate_paths(state_db, minute_db, ledger_db, jsonl):
    paths = [Path(value).resolve() for value in (state_db, minute_db, ledger_db, jsonl)]
    for i, path in enumerate(paths):
        for other in paths[:i]:
            if path == other or path.exists() and other.exists() and os.path.samefile(path, other):
                raise ValueError('DISTINCT_SOURCE_AND_SHADOW_PATHS_REQUIRED')
    if not all(path.is_file() for path in paths[:2]):
        raise ValueError('EXPLICIT_EXISTING_SOURCE_FILES_REQUIRED')
    return paths


class ReadOnlyShadowSource:
    """Current WAL-aware SQLite read snapshots, not immutable historical replay."""
    def __init__(self, state_db, minute_db, *, lanes):
        self.state_db, self.minute_db = Path(state_db).resolve(), Path(minute_db).resolve()
        self.lanes = tuple(sorted(set(lanes)))
        if not self.lanes or not all(isinstance(v, str) and v for v in self.lanes):
            raise ValueError('EXPLICIT_LANES_REQUIRED')
        self.helpers = _audit_helpers()
        self.last_census = None

    def _connect(self, path):
        if not path.is_file():
            raise ValueError('SOURCE_NOT_LOCALLY_AVAILABLE')
        db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=.2)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        return db

    def read_minute(self, minute, *, observed_at):
        minute, observed_at = _stamp(minute), _stamp(observed_at)
        marks = ','.join('?' for _ in self.lanes)
        with closing(self._connect(self.state_db)) as db:
            plans = [dict(r) for r in db.execute(
                f'SELECT * FROM execution_plans WHERE lane_id IN ({marks}) AND status="ACTIVE_TODAY" LIMIT 1001', self.lanes)]
            events = [dict(r) for r in db.execute(
                f'SELECT * FROM monitor_events WHERE lane_id IN ({marks}) AND minute_end=? ORDER BY event_id LIMIT 1001',
                (*self.lanes, minute.isoformat()))]
            positions = db.execute(f'SELECT COUNT(*) FROM virtual_positions WHERE account_id IN ({marks}) AND total_qty>0',
                tuple('paper:'+lane for lane in self.lanes)).fetchone()[0]
            # Only the recorded terminal decision is an invalidation clock;
            # updated_at is not an historical status ledger.
            terminals = [dict(r) for r in db.execute(
                f'SELECT minute_end,payload_json FROM monitor_events WHERE lane_id IN ({marks}) AND action="PLAN_INVALIDATED" AND effective=1 AND minute_end>=? AND minute_end<=? LIMIT 1001',
                (*self.lanes, minute.replace(hour=0, minute=0).isoformat(), observed_at.isoformat()))]
        if positions:
            raise ValueError('POSITION_REPLAY_UNPROVEN')
        if max(len(plans), len(events), len(terminals)) > 1000:
            raise ValueError('READ_SCOPE_BOUND_EXCEEDED')
        by_plan, duplicates = {}, set()
        for event in events:
            payload = json.loads(event['payload_json'])
            pid = payload.get('plan_id')
            if not pid:
                continue
            if pid in by_plan:
                duplicates.add(pid)
            by_plan[pid] = event
        self.last_census = {
            'scope_basis':'CURRENT_READ_TRANSACTION_NOT_HISTORICAL_STATUS_LEDGER',
            'minute':minute.isoformat(), 'lanes':list(self.lanes),
            'active_plans':[{key:p[key] for key in ('plan_id','symbol','status','plan_version','valid_from','expires_at')}
                | {'row_sha256':_digest(p)} for p in plans],
            'expected_plan_ids':sorted(p['plan_id'] for p in plans),
            'event_plan_ids':sorted(by_plan),
            'missing_plan_ids':sorted({p['plan_id'] for p in plans}-set(by_plan)),
            'unexpected_plan_ids':sorted(set(by_plan)-{p['plan_id'] for p in plans}),
            'outer_baseline_records':[{'plan_id':pid,'event_id':event['event_id'],
                'event_sha256':_digest(event),'action':event['action'],'reason':event['reason_code'],
                'minute':event['minute_end']} for pid,event in sorted(by_plan.items())],
        }
        if duplicates:
            raise ValueError('MULTIPLE_BASELINES_FOR_PLAN_MINUTE')
        scope = {p['plan_id'] for p in plans}
        if set(by_plan) - scope:
            raise ValueError('EVENT_OUTSIDE_CURRENT_ACTIVE_SCOPE')
        if scope - set(by_plan):
            raise ValueError('ACTIVE_PLAN_BASELINE_WINDOW_MISSING')
        items = []
        from ..workflow import _intraday_market_context
        total_bytes = 0
        with closing(self._connect(self.minute_db)) as cache:
            for row in plans:
                event = by_plan[row['plan_id']]
                plan, payload = json.loads(row['payload_json']), json.loads(event['payload_json'])
                if (payload.get('symbol') != row['symbol'] or plan.get('symbol') != row['symbol']
                        or plan.get('plan_id', row['plan_id']) != row['plan_id']
                        or event['lane_id'] != row['lane_id']):
                    raise ValueError('EXECUTION_PLAN_IDENTITY_MISMATCH')
                created = _stamp(event['created_at'])
                if not minute <= created <= observed_at or _stamp(row['created_at']) > minute or _stamp(row['updated_at']) > created:
                    raise ValueError('SOURCE_TIME_OR_PLAN_REVISION_UNPROVEN')
                if event['event_id'] != str(uuid.uuid5(uuid.NAMESPACE_URL, 'liangjian-monitor:'+event['event_key'])):
                    raise ValueError('EVENT_ID_BINDING_MISMATCH')
                if event['effective'] not in (0,1):
                    raise ValueError('OUTER_BASELINE_FLAG_INVALID')
                key = (f"effective:{row['lane_id']}:{row['plan_id']}:{event['action']}" if event['effective'] else
                    f"internal:{row['lane_id']}:{row['plan_id']}:{minute.isoformat()}:{event['action']}:{event['reason_code']}")
                if event['effective'] and event['action'] in ('REDUCE_SIGNAL','ADD_SIGNAL'):
                    key += ':'+str(payload.get('trigger_episode_id') or minute.isoformat())
                if event['event_key'] != key:
                    raise ValueError('OUTER_BASELINE_EVENT_KEY_CONFLICT')
                start, end = _stamp(row['valid_from']), _stamp(row['expires_at'])
                invalid = [_stamp(t['minute_end']) for t in terminals
                    if json.loads(t['payload_json']).get('plan_id') == row['plan_id']]
                if invalid:
                    end = min(end, min(invalid))
                if (not start <= minute <= observed_at <= end or start.date() != minute.date()
                        or plan.get('invalidated') or plan.get('plan_invalidated')):
                    raise ValueError('EXECUTION_PLAN_VALIDITY_UNPROVEN')
                strategy = payload.get('strategy')
                if not isinstance(strategy, dict) or not all(key in strategy for key in ('action','state')):
                    raise ValueError('INNER_BASELINE_MISSING')
                closed, dispatch, basis = self.helpers.replay_clocks(payload, event['minute_end'])
                if basis == 'LEGACY_EVENT_CLOCK' or dispatch != minute:
                    raise ValueError('EXPLICIT_OBSERVATION_CLOCK_REQUIRED')
                snapshot = payload.get('minute_snapshot_id')
                if not isinstance(snapshot, str) or not snapshot:
                    raise ValueError('EXACT_MINUTE_SNAPSHOT_ID_REQUIRED')
                bars, hashes = {}, {}
                for interval in ('1m','5m'):
                    snap = cache.execute('SELECT * FROM minute_decision_snapshots WHERE snapshot_id=? AND symbol=? AND interval=?',
                        (snapshot, row['symbol'], interval)).fetchone()
                    if snap is None or _stamp(snap['decision_as_of']) != dispatch or not closed <= _stamp(snap['captured_at']) <= observed_at:
                        raise ValueError('MINUTE_SNAPSHOT_WINDOW_MISSING_OR_CONFLICT')
                    decoder = zlib.decompressobj()
                    raw = decoder.decompress(snap['payload_zlib'], 8*1024*1024+1)
                    if len(raw) > 8*1024*1024 or not decoder.eof or decoder.unused_data:
                        raise ValueError('MINUTE_SNAPSHOT_SIZE_OR_ENCODING_INVALID')
                    total_bytes += len(raw)
                    if total_bytes > 32*1024*1024:
                        raise ValueError('TOTAL_MINUTE_WINDOW_SIZE_EXCEEDED')
                    if hashlib.sha256(raw).hexdigest() != snap['payload_sha256']:
                        raise ValueError('MINUTE_SNAPSHOT_SHA_MISMATCH')
                    values = json.loads(raw)
                    if not isinstance(values, list) or any(v.get('closed') is False or v.get('is_complete') is False for v in values):
                        raise ValueError('UNCOMPLETED_SNAPSHOT_BAR')
                    parsed = tuple(MinuteBar.model_validate(value) for value in values)
                    if (not parsed or any(b.symbol != row['symbol'] or b.interval != interval or b.bar_end > closed
                        or b.evidence_kind != 'MARKET_BAR' for b in parsed)
                        or len({b.bar_end for b in parsed}) != len(parsed)
                        or list(parsed) != sorted(parsed, key=lambda b:b.bar_end)):
                        raise ValueError('SNAPSHOT_BAR_IDENTITY_CLOCK_OR_ORDER_INVALID')
                    bars[interval], hashes[interval] = parsed, snap['payload_sha256']
                if bars['1m'][-1].bar_end != closed:
                    raise ValueError('LATEST_CLOSED_BAR_MISSING')
                gate = strategy.get('market_gate')
                if (not isinstance(gate, dict) or _stamp(gate.get('as_of')) > dispatch
                        or _stamp(gate.get('as_of')).date() != dispatch.date()
                        or not gate.get('state_status') or not gate.get('decision')
                        or gate.get('trade_date') != dispatch.date().isoformat()):
                    raise ValueError('FROZEN_MARKET_GATE_UNPROVEN')
                context = _intraday_market_context(row['symbol'], bars['1m'], bars['5m'], current=closed)
                context = self.helpers.frozen_market_overlay(context, strategy)
                durable = {**plan, 'plan_id':row['plan_id'], 'valid_from':row['valid_from'], 'expires_at':row['expires_at']}
                if invalid:
                    durable['invalidated_at'] = min(invalid).isoformat()
                items.append({'plan_id':row['plan_id'], 'plan':durable, 'baseline':strategy,
                    'actual_outer_baseline':{'action':event['action'], 'reason':event['reason_code']},
                    'baseline_event_id':event['event_id'], 'baseline_event_sha256':_digest(event),
                    'bars':bars['1m'], 'now':closed, 'decision_time':dispatch, 'market_context':context,
                    'source_binding':{'plan_row_sha256':_digest(row),
                        'plan_payload_bytes_sha256':hashlib.sha256(row['payload_json'].encode()).hexdigest(),
                        'minute_snapshot_id':snapshot, 'minute_payload_sha256':hashes,
                        'observation_clock_basis':basis}})
        return items


class ShadowSession:
    """One poll at a time, one current minute, no historical task queue."""
    def __init__(self, source, *, engine=None, ledger, started_at, budget_seconds=5,
                 wait_seconds=5, max_lateness_seconds=15, clock=time.monotonic,
                 identity_provider=None, bar_arrival_provider=None):
        if any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) for v in (budget_seconds,wait_seconds,max_lateness_seconds)):
            raise ValueError('FINITE_BUDGET_REQUIRED')
        if not 0 < budget_seconds <= 5 or not 0 <= wait_seconds <= 5 or not 0 < max_lateness_seconds <= 30:
            raise ValueError('SHADOW_BUDGET_OR_WAIT_INVALID')
        validate_paths(source.state_db, source.minute_db, ledger.db_path, ledger.jsonl_path)
        self.source, self.engine, self.ledger = source, engine or ShadowVariantEngine(), ledger
        self.started_at = _stamp(started_at)
        self.budget_seconds, self.wait_seconds, self.max_lateness_seconds = budget_seconds, wait_seconds, max_lateness_seconds
        self.clock, self.identity_provider, self.bar_arrival_provider = clock, identity_provider, bar_arrival_provider
        self._lock, self._minute, self._finished, self._wait_start = threading.Lock(), None, False, None
        self._last_clock = None
        self.source_sha256 = source_hashes()

    def poll(self, *, observed_at):
        if not self._lock.acquire(blocking=False):
            return {'status':'SHADOW_SESSION_BUSY', 'actual_execution_authorized':False}
        started = self.clock()
        try:
            now = _stamp(observed_at)
            minute = now.replace(second=0, microsecond=0)
            receipt = {'schema_version':'shadow-session-receipt/1', 'minute':minute.isoformat(),
                'observed_at':now.isoformat(), 'status':'DATA_LIMITED', 'scope_status':'INCOMPLETE',
                'pit_status':'UNWIRED', 'arrival_status':'UNWIRED', 'gap_codes':[],
                'source_sha256':self.source_sha256, 'actual_execution_authorized':False}
            if not math.isfinite(started) or self._last_clock is not None and started < self._last_clock:
                receipt['gap_codes']=['MONOTONIC_CLOCK_REGRESSED']; return receipt
            self._last_clock = started
            if self._minute and minute < self._minute:
                receipt['gap_codes']=['CLOCK_REGRESSED']; return receipt
            if minute != self._minute:
                self._minute, self._finished, self._wait_start = minute, False, started
            if self._finished:
                receipt['status']='DUPLICATE'; return receipt
            if minute < self.started_at or (now-minute).total_seconds() > self.max_lateness_seconds:
                self._finished=True; receipt['gap_codes']=['LATE_OR_PRESTART_MINUTE_NOT_REALTIME']; return receipt
            try:
                self.source.last_census = None
                items = self.source.read_minute(minute, observed_at=now)
                if not items:
                    raise ValueError('EMPTY_ACTIVE_SCOPE_NO_STRATEGY_RECORDS')
            except Exception as exc:
                # Permit only fixed internal uppercase codes, never exception data.
                code = str(exc)
                receipt['gap_codes']=[code if code.isupper() and code.replace('_','').isalnum() and len(code)<100 else 'SHADOW_SOURCE_READ_FAILED']
                receipt['plan_window_census']=self.source.last_census
                if started-self._wait_start < self.wait_seconds:
                    receipt['status']='WAITING_COMPLETE_SCOPE'; return receipt
                self._finished=True; return receipt
            self._finished=True
            receipt.update(scope_status='COMPLETE', plan_count=len(items),
                plan_window_census=self.source.last_census,
                source_bindings={item['plan_id']:item['source_binding'] for item in items})
            receipt['gap_codes'] = ([ 'PIT_PROVIDER_UNWIRED'] if self.identity_provider is None else []) + (
                ['BAR_ARRIVAL_PROVIDER_UNWIRED'] if self.bar_arrival_provider is None else [])
            result = self.engine.evaluate_minute(items, minute=minute, budget_seconds=self.budget_seconds)
            write_started = self.clock()
            writes = []
            if self.identity_provider is not None:
                receipt['pit_status']='INJECTED_PROVIDER'
                for item in items:
                    material = self.identity_provider(item['plan'], now)
                    if material.get('status') != 'READY' or not isinstance(material.get('raw_response'),bytes):
                        receipt['pit_status']='DATA_LIMITED'
                    writes.append(self.ledger.seal_price_limit_evidence(item['plan_id'], material['evidence'],
                        observed_at=material['observed_at'], raw_response=material.get('raw_response')))
            if self.bar_arrival_provider is not None:
                receipt['arrival_status']='INJECTED_PROVIDER'
            plans = {item['plan_id']:item['plan'] for item in items}
            for signal in result['signals']:
                if signal.get('status') == 'ERROR':
                    signal = {**signal, 'reason':'SHADOW_VARIANT_EVALUATION_FAILED'}
                writes.append(self.ledger.record_signal(signal, plans[signal['plan_id']], observed_at=now))
            if self.bar_arrival_provider is not None:
                writes.append(self.ledger.advance_outcomes(self.bar_arrival_provider(now), observed_at=now))
            summary = {**result['minute_summary'], 'scope_status':'COMPLETE',
                'source_bindings':receipt['source_bindings'], 'pit_status':receipt['pit_status'],
                'arrival_status':receipt['arrival_status']}
            writes.append(self.ledger.record_minute(summary, observed_at=now))
            receipt.update(writer_elapsed_ms=max(0,(self.clock()-write_started)*1000),
                elapsed_ms=max(0,(self.clock()-started)*1000), engine_status=summary['status'],
                baseline_records=summary.get('baseline_records',[]),
                write_receipts=writes, status='SHADOW_WRITE_FAILED' if any(not r.get('ok') for r in writes) else
                    'DATA_LIMITED' if receipt['pit_status'] in ('UNWIRED','DATA_LIMITED') or self.bar_arrival_provider is None else summary['status'])
            return receipt
        except Exception:
            self._finished=True
            failed = locals().get('receipt', {})
            failed.update(status='SHADOW_WRITE_FAILED' if 'result' in locals() else 'SHADOW_ENGINE_FAILED',
                actual_execution_authorized=False, gap_codes=['SHADOW_EXCEPTION_ISOLATED'])
            return failed
        finally:
            self._lock.release()
