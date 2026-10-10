"""Independent read-only monitor consumer. Never dispatch A4 or acquire data."""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta
import ast
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
import types
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


def _checkout_root(explicit=None):
    actual=Path(__file__).resolve()
    if explicit is None:
        # Compatibility ONLY for the real src layout, never site-packages or
        # arbitrary __file__.parents guessing, cwd/env discovery or a search.
        expected=('src','liangjian_funnel','runtime','shadow_session.py')
        if tuple(actual.parts[-4:])!=expected:
            raise ValueError('SHADOW_CHECKOUT_ROOT_UNWIRED')
        root=actual.parent.parent.parent.parent
    else:
        root=Path(explicit).resolve()
    required=('pyproject.toml','src/liangjian_funnel/runtime/shadow_session.py',
        'scripts/audit_frozen_a4_decisions.py')
    if not root.is_dir() or any(not (root/name).is_file() or
            not (root/name).resolve().is_relative_to(root) for name in required):
        raise ValueError('SHADOW_CHECKOUT_ROOT_INVALID')
    return root


def _audit_helpers(*, checkout_root=None):
    root=_checkout_root(checkout_root)
    path=(root/'scripts/audit_frozen_a4_decisions.py').resolve()
    raw=path.read_bytes()
    tree=ast.parse(raw.decode('utf-8-sig'),filename=str(path))
    names={'replay_clocks':('payload','stamp'),'frozen_market_overlay':('context','strategy')}
    selected=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
    if (len(selected)!=len(names) or {node.name for node in selected}!=set(names)
            or any(node.decorator_list or tuple(arg.arg for arg in node.args.args)!=names[node.name]
                or node.args.defaults or node.args.kwonlyargs or node.args.vararg or node.args.kwarg
                for node in selected)):
        raise ValueError('SHADOW_AUDIT_HELPER_CONTRACT_INVALID')
    module=types.ModuleType('_shadow_frozen_audit_helpers')
    module.__file__=str(path)
    module.__dict__.update(datetime=datetime,__builtins__={'dict':dict,'any':any,'ValueError':ValueError})
    fragment=ast.Module(body=selected,type_ignores=[])
    exec(compile(fragment,str(path),'exec'),module.__dict__)
    checksum=hashlib.sha256(raw).hexdigest()
    if hashlib.sha256(path.read_bytes()).hexdigest()!=checksum:
        raise ValueError('SHADOW_AUDIT_HELPER_CHANGED_DURING_LOAD')
    module.source_reference={'actual_path':str(path),'sha256':checksum,
        'loader':'AST_PURE_HELPER_FUNCTIONS_ONLY','loaded_functions':sorted(names),
        'selected_ast_sha256':hashlib.sha256(ast.dump(fragment,include_attributes=False).encode()).hexdigest()}
    return module


def _source_evidence(repo_root=None):
    root=_checkout_root(repo_root)
    modules={'src/liangjian_funnel/runtime/shadow_session.py':sys.modules[__name__]}
    for name in ('workflow','runtime.strategies','runtime.decision_observability',
            'runtime.shadow_variants','runtime.shadow_evidence'):
        modules['src/liangjian_funnel/'+name.replace('.','/')+'.py']=importlib.import_module('liangjian_funnel.'+name)
    records={}
    for relative,module in modules.items():
        filename=getattr(module,'__file__',None)
        actual=Path(filename).resolve() if filename else None
        if actual is None or actual.suffix!='.py' or not actual.is_file():
            raise ValueError('SHADOW_IMPORTED_SOURCE_UNAVAILABLE')
        checkout=(root/relative).resolve()
        if not checkout.is_relative_to(root) or not checkout.is_file():
            raise ValueError('SHADOW_CHECKOUT_ROOT_INVALID')
        checksum=hashlib.sha256(actual.read_bytes()).hexdigest()
        expected=checksum if actual==checkout else hashlib.sha256(checkout.read_bytes()).hexdigest()
        if checksum!=expected:
            raise ValueError('SHADOW_IMPORTED_CHECKOUT_SOURCE_MISMATCH')
        records[relative]={'actual_path':str(actual),'sha256':checksum,
            'module_name':module.__name__,'source_kind':'ACTUAL_IMPORTED_PYTHON_SOURCE',
            'checkout_path':str(checkout),'checkout_sha256':expected,'checkout_byte_match':True}
    for relative,kind in (('scripts/run_shadow_session.py','EXPLICIT_CHECKOUT_CLI_NOT_IMPORTED'),
            ('scripts/audit_frozen_a4_decisions.py','EXPLICIT_CHECKOUT_AST_HELPER_SOURCE')):
        actual=(root/relative).resolve()
        if not actual.is_relative_to(root) or not actual.is_file():
            raise ValueError('SHADOW_CHECKOUT_ROOT_INVALID')
        records[relative]={'actual_path':str(actual),'sha256':hashlib.sha256(actual.read_bytes()).hexdigest(),
            'source_kind':kind}
    return records


def source_hashes(repo_root=None):
    """Legacy key→hash shape, but values bind actual imported Python files."""
    return {key:value['sha256'] for key,value in _source_evidence(repo_root).items()}


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
    def __init__(self, state_db, minute_db, *, lanes, monitor_latest=None, checkout_root=None):
        self.state_db, self.minute_db = Path(state_db).resolve(), Path(minute_db).resolve()
        self.lanes = tuple(sorted(set(lanes)))
        if not self.lanes or not all(isinstance(v, str) and v for v in self.lanes):
            raise ValueError('EXPLICIT_LANES_REQUIRED')
        self.checkout_root=_checkout_root(checkout_root)
        self.source_references=_source_evidence(self.checkout_root)
        self.source_sha256={key:value['sha256'] for key,value in self.source_references.items()}
        self.helpers = _audit_helpers(checkout_root=self.checkout_root)
        if self.helpers.source_reference['sha256']!=self.source_sha256['scripts/audit_frozen_a4_decisions.py']:
            raise ValueError('SHADOW_AUDIT_HELPER_CHANGED_DURING_LOAD')
        self.last_census = None
        self.monitor_latest = Path(monitor_latest).resolve() if monitor_latest is not None else None

    def read_completion(self, minute, *, observed_at, observation_clock=None):
        """Original atomic monitor/latest.json is an end-of-round fence.

        Not an artificial certificate or a file-mtime assertion. The exact
        bytes are bound; the original observability object is reconstructed
        normally to validate its existing decision hash and clock contract.
        """
        if self.monitor_latest is None:
            raise ValueError('PRODUCTION_COMPLETION_UNWIRED')
        if not self.monitor_latest.is_file():
            raise ValueError('PRODUCTION_COMPLETION_PENDING')
        with self.monitor_latest.open('rb') as source:
            raw = source.read(8*1024*1024+1)
        read_observed = _stamp(observation_clock()) if observation_clock is not None else observed_at
        if read_observed < observed_at:
            raise ValueError('PRODUCTION_COMPLETION_OBSERVATION_CLOCK_CONFLICT')
        observed_at = read_observed
        if len(raw)>8*1024*1024:
            raise ValueError('PRODUCTION_COMPLETION_SIZE_INVALID')
        payload = json.loads(raw)
        stamp = _stamp(payload['time'])
        if stamp < minute:
            raise ValueError('PRODUCTION_COMPLETION_PENDING')
        if stamp != minute:
            raise ValueError('PRODUCTION_COMPLETION_CLOCK_CONFLICT')
        from .decision_observability import (DecisionObservation, DecisionAxes, DataState,
            TradeEligibility, TimingSpan, OBSERVABILITY_SCHEMA_VERSION)
        from ..pipeline.outcomes import JobLifecycleState, OpportunityState
        original = payload['observability']
        axes = original['axes']
        observation = DecisionObservation(
            run_id=original['run_id'], decision_id=original['decision_id'], lane_id=original['lane_id'],
            scheduled_at=_stamp(original['scheduled_at']), started_at=_stamp(original['started_at']),
            deadline_at=_stamp(original['deadline_at']), snapshot_ids=tuple(original['snapshot_ids']),
            required_scope=tuple(original['required_scope']), ready_scope=tuple(original['ready_scope']),
            blocked_scope=tuple(original['blocked_scope']), no_signal_scope=tuple(original['no_signal_scope']),
            timing_spans=tuple(TimingSpan(span['name'],span['duration_ms'],
                _stamp(span['started_at']) if span['started_at'] else None,
                _stamp(span['finished_at']) if span['finished_at'] else None) for span in original['timing_spans']),
            source_attempts=tuple(original['source_attempts']),terminal_reason=original['terminal_reason'],
            versions=original['versions'], axes=DecisionAxes(JobLifecycleState(axes['job_status']),
                DataState(axes['data_state']),OpportunityState(axes['opportunity_state']),
                TradeEligibility(axes['trade_eligibility']),tuple(axes['reason_codes']),axes['critical_data']),
        )
        if (original['schema_version']!=OBSERVABILITY_SCHEMA_VERSION
                or original['decision_hash']!=observation.decision_hash
                or observation.scheduled_at!=minute or not observation.run_id or not observation.decision_id):
            raise ValueError('PRODUCTION_COMPLETION_IDENTITY_OR_HASH_CONFLICT')
        total = [span for span in observation.timing_spans if span.name=='round_total']
        if (len(total)!=1 or total[0].finished_at is None or total[0].started_at!=observation.started_at
                or not minute<=observation.started_at<=total[0].finished_at<=observed_at
                or observation.deadline_at<=observation.started_at):
            raise ValueError('PRODUCTION_COMPLETION_TIMING_CONFLICT')
        snapshot = payload['minute_snapshot_id']
        if not isinstance(snapshot,str) or not snapshot or observation.snapshot_ids!=(snapshot,):
            raise ValueError('PRODUCTION_COMPLETION_SNAPSHOT_CONFLICT')
        from ..workflow import _a4_execution_cutoff
        cutoff = _stamp(payload['execution_cutoff'])
        if cutoff!=_a4_execution_cutoff(minute):
            raise ValueError('PRODUCTION_COMPLETION_CUTOFF_CONFLICT')
        lanes = payload['lanes']
        chosen = [lane for lane in lanes if lane['lane_id'] in self.lanes]
        if len(chosen)!=len(self.lanes) or {lane['lane_id'] for lane in chosen}!=set(self.lanes):
            raise ValueError('PRODUCTION_COMPLETION_LANE_SCOPE_CONFLICT')
        if any(lane['minute_snapshot_id']!=snapshot for lane in chosen):
            raise ValueError('PRODUCTION_COMPLETION_LANE_SNAPSHOT_CONFLICT')
        return payload, {'raw_bytes_sha256':hashlib.sha256(raw).hexdigest(),
            'run_id':observation.run_id,'decision_id':observation.decision_id,
            'decision_hash':observation.decision_hash,'minute_snapshot_id':snapshot,
            'execution_cutoff':cutoff.isoformat(),'finished_at':total[0].finished_at.isoformat(),
            'observed_at':observed_at.isoformat(),
            'source_kind':'ORIGINAL_ATOMIC_MONITOR_LATEST_NOT_HISTORICAL_CERTIFICATE'}

    def read_round(self, minute, *, observed_at, observation_clock=None):
        payload, binding = self.read_completion(minute, observed_at=observed_at,observation_clock=observation_clock)
        items = self.read_minute(minute,observed_at=_stamp(binding['observed_at']))
        original_events = [event for lane in payload['lanes'] if lane['lane_id'] in self.lanes
            for event in lane['events'] if event.get('plan_id')]
        by_plan = {event['plan_id']:event for event in original_events}
        if len(by_plan)!=len(original_events) or set(by_plan)!={item['plan_id'] for item in items}:
            raise ValueError('PRODUCTION_COMPLETION_PLAN_SCOPE_CONFLICT')
        for item in items:
            event = by_plan[item['plan_id']]
            if (item['source_binding']['minute_snapshot_id']!=binding['minute_snapshot_id']
                    or item['now']!=_stamp(binding['execution_cutoff'])
                    or event['symbol']!=item['plan']['symbol'] or _stamp(event['minute_end'])!=minute
                    or event['action']!=item['actual_outer_baseline']['action']
                    or event['reason_code']!=item['actual_outer_baseline']['reason']):
                raise ValueError('PRODUCTION_COMPLETION_BASELINE_CONFLICT')
        with self.monitor_latest.open('rb') as source:
            raw = source.read(8*1024*1024+1)
        if hashlib.sha256(raw).hexdigest()!=binding['raw_bytes_sha256']:
            raise ValueError('PRODUCTION_COMPLETION_CHANGED_DURING_SOURCE_READ')
        return items,binding

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
                 identity_provider=None, bar_arrival_provider=None, wall_clock=None):
        if any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) for v in (budget_seconds,wait_seconds,max_lateness_seconds)):
            raise ValueError('FINITE_BUDGET_REQUIRED')
        if not 0 < budget_seconds <= 5 or not 0 <= wait_seconds <= 5 or not 0 < max_lateness_seconds <= 30:
            raise ValueError('SHADOW_BUDGET_OR_WAIT_INVALID')
        source_and_outputs=validate_paths(source.state_db, source.minute_db, ledger.db_path, ledger.jsonl_path)
        if source.monitor_latest is not None:
            for path in source_and_outputs:
                if source.monitor_latest==path or source.monitor_latest.exists() and path.exists() and os.path.samefile(source.monitor_latest,path):
                    raise ValueError('COMPLETION_AND_OUTPUT_PATH_ALIAS_REFUSED')
        self.source, self.engine, self.ledger = source, engine or ShadowVariantEngine(), ledger
        self.started_at = _stamp(started_at)
        self.budget_seconds, self.wait_seconds, self.max_lateness_seconds = budget_seconds, wait_seconds, max_lateness_seconds
        self.clock, self.identity_provider, self.bar_arrival_provider = clock, identity_provider, bar_arrival_provider
        self.wall_clock = wall_clock or (lambda:datetime.now().astimezone())
        self._lock, self._minute, self._finished, self._wait_start = threading.Lock(), None, False, None
        self._last_clock = None
        self._writer_uncertain = False
        self.source_sha256 = source_hashes(repo_root=source.checkout_root)
        if self.source_sha256!=source.source_sha256:
            raise ValueError('SHADOW_SOURCE_CHANGED_SINCE_SOURCE_SETUP')
        self.source_references=source.source_references

    def poll(self, *, observed_at):
        if not self._lock.acquire(blocking=False):
            return {'status':'SHADOW_SESSION_BUSY', 'actual_execution_authorized':False}
        started = self.clock()
        tentative = None
        accepted_keys = set()
        resolved = False
        signal_write_in_flight = False
        try:
            now = _stamp(observed_at)
            minute = now.replace(second=0, microsecond=0)
            receipt = {'schema_version':'shadow-session-receipt/1', 'minute':minute.isoformat(),
                'observed_at':now.isoformat(), 'status':'DATA_LIMITED', 'scope_status':'INCOMPLETE',
                'pit_status':'UNWIRED', 'arrival_status':'UNWIRED', 'gap_codes':[],
                'source_sha256':self.source_sha256,'source_references':self.source_references,
                'audit_helper_source':self.source.helpers.source_reference,
                'actual_execution_authorized':False}
            if self._writer_uncertain:
                receipt['gap_codes']=['SHADOW_WRITER_COMMIT_UNPROVEN_SESSION_PAUSED']; return receipt
            if not math.isfinite(started) or self._last_clock is not None and started < self._last_clock:
                receipt['gap_codes']=['MONOTONIC_CLOCK_REGRESSED']; return receipt
            self._last_clock = started
            if self._minute and minute < self._minute:
                receipt['gap_codes']=['CLOCK_REGRESSED']; return receipt
            if minute != self._minute:
                self._minute, self._finished, self._wait_start = minute, False, started
            if self._finished:
                receipt['status']='DUPLICATE'; return receipt
            minute_end = minute+timedelta(minutes=1)
            receipt.update(minute_fence_at=minute_end.isoformat(),evaluation_budget_seconds=self.budget_seconds,
                source_wait_policy='ORIGINAL_COMPLETION_UNTIL_SAME_MINUTE_EVALUATION_FENCE',
                legacy_wait_and_lateness_gate='DEPRECATED_NOT_APPLIED_TO_SOURCE_READINESS')
            if minute < self.started_at or now+timedelta(seconds=self.budget_seconds)>=minute_end:
                self._finished=True; receipt['gap_codes']=['LATE_OR_PRESTART_MINUTE_NOT_REALTIME']; return receipt
            try:
                self.source.last_census = None
                items, completion = self.source.read_round(minute, observed_at=now,observation_clock=self.wall_clock)
                if not items:
                    raise ValueError('EMPTY_ACTIVE_SCOPE_NO_STRATEGY_RECORDS')
            except Exception as exc:
                # Permit only fixed internal uppercase codes, never exception data.
                code = str(exc)
                receipt['gap_codes']=[code if code.isupper() and code.replace('_','').isalnum() and len(code)<100 else 'SHADOW_SOURCE_READ_FAILED']
                receipt['plan_window_census']=self.source.last_census
                if code=='PRODUCTION_COMPLETION_PENDING':
                    receipt['status']='WAITING_PRODUCTION_COMPLETION'; return receipt
                self._finished=True; return receipt
            self._finished=True
            evaluation_wall = _stamp(self.wall_clock())
            if evaluation_wall<_stamp(completion['observed_at']) or evaluation_wall+timedelta(seconds=self.budget_seconds)>=minute_end:
                receipt['gap_codes']=['SHADOW_SOURCE_READY_OUTSIDE_MINUTE_FENCE']; return receipt
            receipt.update(scope_status='COMPLETE', plan_count=len(items),
                plan_window_census=self.source.last_census,
                production_completion=completion,evaluation_started_at=evaluation_wall.isoformat(),
                source_bindings={item['plan_id']:item['source_binding'] for item in items})
            receipt['gap_codes'] = ([ 'PIT_PROVIDER_UNWIRED'] if self.identity_provider is None else []) + (
                ['BAR_ARRIVAL_PROVIDER_UNWIRED'] if self.bar_arrival_provider is None else [])
            evaluation_mono = self.clock()
            tentative = self.engine.evaluate_tentative(items, minute=minute, budget_seconds=self.budget_seconds)
            result = tentative.result
            result_ready = _stamp(self.wall_clock())
            elapsed_evaluation = self.clock()-evaluation_mono
            receipt['result_ready_at']=result_ready.isoformat()
            if (result_ready<evaluation_wall or result_ready>=minute_end or result_ready-evaluation_wall>timedelta(seconds=self.budget_seconds)
                    or not 0<=elapsed_evaluation<=self.budget_seconds):
                receipt.update(status='DATA_LIMITED',gap_codes=['SHADOW_RESULT_OUTSIDE_MINUTE_OR_BUDGET']); return receipt
            if result['minute_summary']['status']!='OK':
                receipt.update(status='DATA_LIMITED',engine_status=result['minute_summary']['status'],
                    gap_codes=['SHADOW_TENTATIVE_EVALUATION_INCOMPLETE']); return receipt
            now = result_ready  # Actual knowledge clock, never backdate to dispatch.
            write_started = self.clock()
            writes = []
            writer_failed = False
            writer_late = False
            writer_limit_reason = 'SHADOW_WRITER_MINUTE_FENCE_REACHED'
            commit_deadline_wall=min(minute_end,evaluation_wall+timedelta(seconds=self.budget_seconds))
            commit_deadline_mono=evaluation_mono+self.budget_seconds
            commit_last_wall=result_ready
            commit_last_mono=evaluation_mono+elapsed_evaluation
            receipt['shadow_total_deadline_at']=commit_deadline_wall.isoformat()
            receipt['shadow_total_budget_seconds']=self.budget_seconds
            def within_commit_deadline():
                nonlocal commit_last_wall,commit_last_mono,writer_limit_reason
                current_wall=_stamp(self.wall_clock())
                current_mono=self.clock()
                if not math.isfinite(current_mono) or current_mono<commit_last_mono or current_wall<commit_last_wall:
                    writer_limit_reason='SHADOW_WRITER_CLOCK_REGRESSED'
                    return None
                if current_wall>=minute_end:
                    writer_limit_reason='SHADOW_WRITER_MINUTE_FENCE_REACHED'
                    return None
                if current_wall>=commit_deadline_wall or current_mono>=commit_deadline_mono:
                    writer_limit_reason='SHADOW_WRITER_TOTAL_BUDGET_EXCEEDED'
                    return None
                commit_last_wall,commit_last_mono=current_wall,current_mono
                return current_wall
            if self.identity_provider is not None:
                receipt['pit_status']='INJECTED_PROVIDER'
                for item in items:
                    if within_commit_deadline() is None:
                        writer_late=True; break
                    material = self.identity_provider(item['plan'], now)
                    if within_commit_deadline() is None:
                        writer_late=True; break
                    if material.get('status') != 'READY' or not isinstance(material.get('raw_response'),bytes):
                        receipt['pit_status']='DATA_LIMITED'
                    writes.append(self.ledger.seal_price_limit_evidence(item['plan_id'], material['evidence'],
                        observed_at=material['observed_at'], raw_response=material.get('raw_response')))
            if self.bar_arrival_provider is not None:
                receipt['arrival_status']='INJECTED_PROVIDER'
            plans = {item['plan_id']:item['plan'] for item in items}
            for signal in result['signals']:
                if writer_late:
                    break
                if any(not value.get('ok') for value in writes):
                    writer_failed=True; break
                write_now = within_commit_deadline()
                if write_now is None:
                    writer_late=True; break
                if signal.get('status') == 'ERROR':
                    signal = {**signal, 'reason':'SHADOW_VARIANT_EVALUATION_FAILED'}
                signal_write_in_flight=True
                written=self.ledger.record_signal(signal, plans[signal['plan_id']], observed_at=write_now)
                signal_write_in_flight=False
                writes.append(written)
                if written.get('stored') is True:
                    accepted_keys.add((signal['plan_id'],signal['variant_id']))
                elif written.get('stored') is not False:
                    self._writer_uncertain=True
                if written.get('ok') is not True or written.get('stored') is not True or self._writer_uncertain:
                    writer_failed=True; break
                if within_commit_deadline() is None:
                    writer_late=True; break
            if self.bar_arrival_provider is not None and not (writer_failed or writer_late):
                if within_commit_deadline() is None:
                    writer_late=True
                else:
                    arrivals=self.bar_arrival_provider(now)
                    if within_commit_deadline() is None:
                        writer_late=True
                    else:
                        writes.append(self.ledger.advance_outcomes(arrivals, observed_at=now))
            summary = {**result['minute_summary'], 'scope_status':'COMPLETE',
                'production_completion':completion,'result_ready_at':result_ready.isoformat(),
                'source_bindings':receipt['source_bindings'], 'pit_status':receipt['pit_status'],
                'arrival_status':receipt['arrival_status']}
            if not (writer_failed or writer_late):
                if within_commit_deadline() is None:
                    writer_late=True
                else:
                    writes.append(self.ledger.record_minute(summary, observed_at=now))
                    if within_commit_deadline() is None:
                        writer_late=True
            writer_failed = writer_failed or any(not value.get('ok') for value in writes)
            if not (writer_failed or writer_late) and within_commit_deadline() is None:
                writer_late=True
            if not (writer_failed or writer_late):
                receipt['state_commit']=self.engine.commit_tentative(tentative)
            elif accepted_keys:
                receipt['state_commit']=self.engine.commit_tentative(tentative,accepted_keys=accepted_keys)
            else:
                receipt['state_commit']=self.engine.discard_tentative(tentative)
            resolved=receipt['state_commit'].get('ok') is True
            if not resolved:
                self._writer_uncertain=True; writer_failed=True
            if resolved and not (writer_failed or writer_late) and within_commit_deadline() is None:
                writer_late=True
            if writer_late:
                receipt['gap_codes']=[writer_limit_reason]
            if self._writer_uncertain:
                receipt['gap_codes']=['SHADOW_WRITER_COMMIT_UNPROVEN_SESSION_PAUSED']
                receipt['writer_state']='UNKNOWN_SESSION_PAUSED'
            receipt.update(writer_elapsed_ms=max(0,(self.clock()-write_started)*1000),
                elapsed_ms=max(0,(self.clock()-started)*1000), engine_status=summary['status'],
                baseline_records=summary.get('baseline_records',[]),
                write_receipts=writes, status='SHADOW_WRITE_FAILED' if writer_failed else
                    'DATA_LIMITED' if writer_late else
                    'DATA_LIMITED' if receipt['pit_status'] in ('UNWIRED','DATA_LIMITED') or self.bar_arrival_provider is None else summary['status'])
            return receipt
        except Exception:
            self._finished=True
            if signal_write_in_flight:
                self._writer_uncertain=True
            failed = locals().get('receipt', {})
            if tentative is not None and accepted_keys:
                failed['state_commit']=self.engine.commit_tentative(tentative,accepted_keys=accepted_keys)
                resolved=failed['state_commit'].get('ok') is True
                if not resolved:
                    self._writer_uncertain=True
            failed.update(status='SHADOW_WRITE_FAILED' if 'result' in locals() else 'SHADOW_ENGINE_FAILED',
                actual_execution_authorized=False, gap_codes=['SHADOW_EXCEPTION_ISOLATED'])
            if self._writer_uncertain:
                failed['writer_state']='UNKNOWN_SESSION_PAUSED'
            return failed
        finally:
            if tentative is not None and not resolved:
                cleanup=self.engine.discard_tentative(tentative)
                if tentative.token is not None and not cleanup.get('ok'):
                    self._writer_uncertain=True
                if 'receipt' in locals():
                    receipt.setdefault('state_commit',cleanup)
            self._lock.release()
