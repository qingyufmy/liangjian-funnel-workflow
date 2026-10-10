"""Explicit preopen research inputs; read-only sources, separate shadow SQLite."""
from __future__ import annotations

from contextlib import contextmanager, closing
from copy import deepcopy
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3

from ..pipeline.local_fact_cache import LocalFactCache, canonical_json_hash
from .calendar import ExchangeTradingCalendar
from .shadow_inputs import build_shadow_inputs, ALGORITHM
from .shadow_variants import prepare_shadow_plan
from .shadow_session import _stamp

SCHEMA = 'shadow-preopen-inputs/1'
ORIGIN = 'SHADOW_PREOPEN_SIDECAR'


def _limited(code):
    return {'status':'DATA_LIMITED','inputs_origin':ORIGIN,'shadow_inputs':None,
        'reason_codes':[code],'actual_execution_authorized':False}


def _binding(row):
    keys = ('plan_id','lane_id','symbol','plan_version','created_at')
    if any(row.get(k) is None or row[k]=='' for k in keys) or not isinstance(row.get('payload_json'),str):
        raise ValueError('ORIGINAL_PLAN_IDENTITY_MISSING')
    plan = json.loads(row['payload_json'])
    if plan.get('symbol') != row['symbol'] or ('plan_id' in plan and plan['plan_id'] != row['plan_id']):
        raise ValueError('ORIGINAL_PLAN_IDENTITY_CONFLICT')
    return {**{k:row[k] for k in keys},
        'payload_json_sha256':hashlib.sha256(row['payload_json'].encode('utf-8')).hexdigest()}


def _preopen_clock(value, target, *, hour):
    clock = _stamp(value)
    if clock.date() != target or clock >= clock.replace(hour=hour,minute=0,second=0,microsecond=0):
        raise ValueError('SIDECAR_NOT_GENERATED_BEFORE_0900')
    return clock


def generate_preopen_sidecar(*, plan_row, rows, target_trade_date, generated_at,
                             source_as_of, bar_cutoff, expected_close_dates):
    record = dict(schema_version=SCHEMA, inputs_origin=ORIGIN,
        target_trade_date=target_trade_date.isoformat(), generated_at=_stamp(generated_at).isoformat(),
        actual_execution_authorized=False)
    try:
        record['plan_binding'] = _binding(plan_row)
        generated = _preopen_clock(generated_at,target_trade_date,hour=9)
        if _stamp(plan_row['created_at']) > generated:
            raise ValueError('ORIGINAL_PLAN_CREATED_AFTER_GENERATION')
        source = _stamp(source_as_of)
        if source > generated:
            raise ValueError('SIDECAR_SOURCE_AFTER_GENERATION')
        if _stamp(plan_row['expires_at']).date() != target_trade_date:
            raise ValueError('SIDECAR_TARGET_PLAN_MISMATCH')
        if plan_row.get('status') not in ('ACTIVE_TODAY','PENDING_MORNING_REVIEW'):
            raise ValueError('SIDECAR_PLAN_NOT_PREOPEN_APPLICABLE')
        plan = json.loads(plan_row['payload_json'])
        facts = plan.get('strategy_facts')
        if isinstance(facts,dict) and 'shadow_inputs' in facts:
            raise ValueError('PLAN_FROZEN_INPUTS_NOT_REPLACED')
        if plan.get('strategy_profile') not in ('TREND_MA5','MA520_SWING'):
            raise ValueError('SIDECAR_PROFILE_NOT_APPLICABLE')
        inputs = build_shadow_inputs(symbol=plan_row['symbol'], rows=rows,source_as_of=source,
            bar_cutoff=_stamp(bar_cutoff),target_trade_date=target_trade_date,production_plan=plan,
            expected_close_dates=expected_close_dates)
        if inputs['status'] != 'AVAILABLE':
            record.update(_limited(inputs['gap_codes'][0])); record['builder_gaps']=inputs['gap_codes']
        else:
            record.update(status='AVAILABLE',shadow_inputs=inputs,reason_codes=[],
                builder_algorithm=ALGORITHM)
    except (ValueError,TypeError,KeyError) as exc:
        code=str(exc)
        record.update(_limited(code if code.isupper() and code.replace('_','').isalnum() else 'SIDECAR_INPUTS_UNPROVEN'))
    record['record_hash'] = canonical_json_hash(record)
    return record


def validate_preopen_record(record, row, *, target_trade_date, observed_at):
    try:
        body=deepcopy(record); pin=body.pop('record_hash')
        if pin != canonical_json_hash(body):
            raise ValueError('SIDECAR_RECORD_HASH_CONFLICT')
        if (body.get('schema_version') != SCHEMA or body.get('inputs_origin') != ORIGIN
                or body.get('target_trade_date') != target_trade_date.isoformat()
                or body.get('plan_binding') != _binding(row)):
            raise ValueError('SIDECAR_ORIGINAL_PLAN_OR_TARGET_CONFLICT')
        observed=_stamp(observed_at)
        if observed.date() != target_trade_date:
            raise ValueError('SIDECAR_OBSERVATION_DAY_CONFLICT')
        generated=_preopen_clock(body['generated_at'],target_trade_date,hour=9)
        if generated > observed:
            raise ValueError('SIDECAR_GENERATION_AFTER_OBSERVATION')
        if body.get('status') != 'AVAILABLE':
            raise ValueError('SIDECAR_SOURCE_DATA_LIMITED')
        inputs=body['shadow_inputs']
        if (inputs.get('version') != ALGORITHM or inputs.get('status') != 'AVAILABLE'
                or _stamp(inputs['daily_as_of']) > generated):
            raise ValueError('SIDECAR_SOURCE_TIME_OR_ALGORITHM_CONFLICT')
        plan=json.loads(row['payload_json']); facts=deepcopy(plan.get('strategy_facts') or {})
        if not isinstance(facts,dict) or 'shadow_inputs' in facts:
            raise ValueError('PLAN_FROZEN_INPUTS_NOT_REPLACED')
        facts['shadow_inputs']=deepcopy(inputs); plan['strategy_facts']=facts
        _,_,reason=prepare_shadow_plan(plan,decision_time=observed)
        if reason:
            raise ValueError(reason)
        return deepcopy(record)
    except (ValueError,TypeError,KeyError):
        return _limited('SIDECAR_CONSUMPTION_UNPROVEN')


class ReadOnlyLocalFactCache(LocalFactCache):
    """Reuse real cache selection/row envelopes without schema/WAL writes."""
    def __init__(self,path):
        self.path=Path(path).resolve()
        if not self.path.is_file():
            raise ValueError('LOCAL_DAILY_SOURCE_UNAVAILABLE')

    @contextmanager
    def _connect(self):
        with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
            db.row_factory=sqlite3.Row
            db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
            yield db


def generate_from_local_cache(plan_row, *, cache, target_trade_date, generated_at, calendar=None, clock=None):
    calendar=calendar or ExchangeTradingCalendar()
    if not calendar.is_trading_day(target_trade_date):
        raise ValueError('SIDECAR_TARGET_NOT_TRADING_DAY')
    day=target_trade_date; previous=[]
    for _ in range(4):
        day=calendar.previous_trading_day(day); previous.append(day)
    generated=_stamp(generated_at)
    _preopen_clock(generated,target_trade_date,hour=9)
    cutoff=generated.replace(year=previous[0].year,month=previous[0].month,day=previous[0].day,
        hour=15,minute=0,second=0,microsecond=0)
    rows=cache.query_daily_bars(plan_row['symbol'],adjust='none',as_of=generated,
        end=cutoff+timedelta(microseconds=1),limit=800,descending=True)
    finished=_stamp(clock()) if clock is not None else generated
    if finished < generated:
        raise ValueError('SIDECAR_GENERATION_CLOCK_REGRESSED')
    record=generate_preopen_sidecar(plan_row=plan_row,rows=list(reversed(rows)),
        target_trade_date=target_trade_date,generated_at=finished,source_as_of=generated,
        bar_cutoff=cutoff,expected_close_dates=list(reversed(previous)))
    record['local_cache_reference']={'path':str(cache.path),'source_kind':'LOCAL_FACT_CACHE_READ_ONLY',
        'adjust':'none','as_of':generated.isoformat(),'end_exclusive':(cutoff+timedelta(microseconds=1)).isoformat(),
        'limit':800,'source_authentication':'NOT_CLAIMED'}
    record.pop('record_hash',None); record['record_hash']=canonical_json_hash(record)
    return record


class PreopenInputsLedger:
    """Constructor is inert. Writes only explicitly separate shadow ledger."""
    def __init__(self,path):
        self.path=Path(path).resolve()

    def record(self,record):
        try:
            body=deepcopy(record); pin=body.pop('record_hash')
            if body.get('schema_version') != SCHEMA or pin != canonical_json_hash(body):
                raise ValueError('SIDECAR_RECORD_HASH_CONFLICT')
            key=record['target_trade_date']+':'+record['plan_binding']['plan_id']
            self.path.parent.mkdir(parents=True,exist_ok=True)
            with closing(sqlite3.connect(self.path,timeout=.2)) as db:
                db.execute('PRAGMA synchronous=FULL')
                db.execute('CREATE TABLE IF NOT EXISTS shadow_preopen_inputs(id TEXT PRIMARY KEY,record_json TEXT NOT NULL)')
                old=db.execute('SELECT record_json FROM shadow_preopen_inputs WHERE id=?',(key,)).fetchone()
                raw=json.dumps(record,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
                if old:
                    if old[0] != raw:
                        raise ValueError('SIDECAR_REENTRY_CONFLICT')
                    db.commit()
                    return {'ok':True,'stored':False,'duplicate':True}
                db.execute('INSERT INTO shadow_preopen_inputs VALUES(?,?)',(key,raw))
                db.commit()
            return {'ok':True,'stored':True,'duplicate':False}
        except Exception as exc:
            code=str(exc)
            reason=code if code in ('SIDECAR_REENTRY_CONFLICT','SIDECAR_RECORD_HASH_CONFLICT') else 'SIDECAR_WRITE_FAILED'
            return {'ok':False,'stored':False,'duplicate':False,'error_code':reason}

    def lookup(self,plan_row,*,target_trade_date,observed_at):
        try:
            with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
                db.execute('PRAGMA query_only=ON')
                value=db.execute('SELECT record_json FROM shadow_preopen_inputs WHERE id=?',
                    (target_trade_date.isoformat()+':'+plan_row['plan_id'],)).fetchone()
                if value is None or len(value[0].encode()) > 1024*1024:
                    raise ValueError('SIDECAR_RECORD_UNAVAILABLE_OR_TOO_LARGE')
                record=json.loads(value[0])
            return validate_preopen_record(record,plan_row,target_trade_date=target_trade_date,observed_at=observed_at)
        except Exception:
            return _limited('SIDECAR_RECORD_UNAVAILABLE')
