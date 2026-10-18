"""0277 first CONTROL day: probe-only, two entries, durable RECORD safety latch.

No environment edits, source calls, customer channel or model. Ordinary policy
rejections are not guard anomalies. The current tick never restarts its budget
by invoking a second engine; the next natural tick consumes the RECORD latch.
"""
from contextlib import closing
from dataclasses import replace
from datetime import date,time
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3

from ..data.tencent_minute import QuoteResult
from .team_v2_sources import stamp
from .team_v2_store import canonical

POLICY=dict(version='control-first-day/2',approved_by='tony-0338',activation_trade_date='2026-10-20',
    probe_only=True,max_new_entries=2,observation_start='09:30',observation_end='10:00',
    quote_freshness_max_age_seconds=90,maximum_freshness_data_block_pct=20)


def validate_first_day_policy(value):
    if not isinstance(value,dict) or value!=POLICY or any(type(value[k])!=type(v) for k,v in POLICY.items()):
        raise ValueError('CONTROL_FIRST_DAY_POLICY_INVALID')
    date.fromisoformat(value['activation_trade_date'])
    return dict(value)


def load_first_day_policy(path):
    import yaml
    body=yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    return validate_first_day_policy(body.get('control_first_day') if isinstance(body,dict) else None)


def active(policy,now):return stamp(now).date().isoformat()==policy['activation_trade_date']


def first_day_limits(limits,policy,*,now):
    return replace(limits,max_new_entries=min(limits.max_new_entries,policy['max_new_entries'])) if active(policy,now) else limits


def probe_envelope(envelope,policy,*,now,probe_multiplier):
    if not active(policy,now) or not envelope.eligible:return envelope
    factor=probe_multiplier if envelope.risk_unit=='STANDARD' else Decimal(1)
    shares=max(0,int(Decimal(envelope.max_shares_cap)*factor/100)*100)
    return replace(envelope,eligible=shares>0,risk_unit='PROBE' if shares>0 else 'NONE',
        risk_budget=envelope.risk_budget*factor,max_shares_cap=shares,max_batches=1,high_conviction=False,
        binding_limits=tuple(dict.fromkeys((*envelope.binding_limits,'CONTROL_FIRST_DAY_PROBE'))),
        blocked_reasons=envelope.blocked_reasons if shares else tuple(dict.fromkeys((*envelope.blocked_reasons,'ROUND_LOT_UNAVAILABLE'))))


def _tables(store):
    with closing(store._connect()) as db,db:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS control_first_day_observations (
                account_id TEXT NOT NULL,trade_date TEXT NOT NULL,decision_minute TEXT NOT NULL,symbol TEXT NOT NULL,
                packet_sha256 TEXT NOT NULL,quote_sha256 TEXT NOT NULL,fresh INTEGER NOT NULL,
                PRIMARY KEY(account_id,trade_date,decision_minute,symbol));
            CREATE TABLE IF NOT EXISTS control_record_latch (
                account_id TEXT NOT NULL,trade_date TEXT NOT NULL,body_json TEXT NOT NULL,body_sha256 TEXT NOT NULL,
                PRIMARY KEY(account_id,trade_date));
        ''')


def forced_record(settings,*,account_id,now):
    root=Path(settings.root).resolve();directory=root/'state/team_v2';path=directory/'team.db'
    if directory.is_symlink() or path.is_symlink() or directory.resolve()!=root/'state/team_v2':
        raise ValueError('CONTROL_RECORD_LATCH_SCOPE_INVALID')
    if not path.is_file():return None
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
        db.execute('PRAGMA query_only=ON')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='control_record_latch'").fetchone():return None
        row=db.execute('SELECT body_json,body_sha256 FROM control_record_latch WHERE account_id=? AND trade_date=?',
            (account_id,stamp(now).date().isoformat())).fetchone()
    if row is None:return None
    if hashlib.sha256(row[0].encode()).hexdigest()!=row[1]:raise ValueError('CONTROL_RECORD_LATCH_HASH_INVALID')
    body=json.loads(row[0])
    if body.get('account_id')!=account_id or body.get('trade_date')!=stamp(now).date().isoformat() or body.get('mode')!='record':
        raise ValueError('CONTROL_RECORD_LATCH_BINDING_INVALID')
    return body


def record_alert_body(body):
    general=body.get('alert_source_id')=='TEAM_MONITOR_RECORD'
    title='模拟盘｜Team CONTROL 已切换 record\n' if general else '模拟盘｜首日 CONTROL 已切换 record\n'
    return dict(msg_type='text',content={'text':title+body['reason_code']+
            ('' if general else '\n首段报价检查：'+str(body['blocked_quotes'])+'/'+str(body['checked_quotes']))+
            ('\n停止新入场；下一轮保护先撤未成交买单，再处理已有持仓硬风控与卖单撮合；客户通道不变'
             if general else '\n停止新入场并撤销未成交买单，已有持仓继续硬风控与卖单撮合；客户通道不变')})


def _notify(store,body,*,now):
    from .team_v2_internal import InternalOutbox
    # One frozen latch per day; notification retry enqueues the same identity
    # and text, never sends HTTP from the minute process.
    return InternalOutbox(store).enqueue(account_id=body['account_id'],trade_date=body['trade_date'],kind='ALERT',
        source_id=body.get('alert_source_id','CONTROL_FIRST_DAY_RECORD'),observed_at=now,priority=0,body=record_alert_body(body))


def latch_record(store,policy,*,account_id,now,reason_code,packet_sha256=None,first_day_only=True):
    now=stamp(now);day=now.date().isoformat()
    if first_day_only and not active(policy,now):return dict(status='NOT_FIRST_DAY',record_required=False)
    _tables(store)
    with closing(store._connect()) as db,db:
        db.execute('BEGIN IMMEDIATE')
        old=db.execute('SELECT body_json FROM control_record_latch WHERE account_id=? AND trade_date=?',(account_id,day)).fetchone()
        if old:body=json.loads(old[0])
        else:
            total,bad=db.execute('SELECT count(*),coalesce(sum(1-fresh),0) FROM control_first_day_observations WHERE account_id=? AND trade_date=?',
                (account_id,day)).fetchone()
            body=dict(account_id=account_id,trade_date=day,mode='record',reason_code=reason_code,
                observed_at=now.isoformat(),checked_quotes=total,blocked_quotes=bad,packet_sha256=packet_sha256,
                scope='PERSISTENT_THIS_DAY_NO_AUTOMATIC_RELEASE',current_tick='NO_SECOND_ENGINE_OR_BUDGET_RESET')
            if not first_day_only:body['alert_source_id']='TEAM_MONITOR_RECORD'
            text=canonical(body);db.execute('INSERT INTO control_record_latch VALUES (?,?,?,?)',
                (account_id,day,text,hashlib.sha256(text.encode()).hexdigest()))
    try:notification=_notify(store,body,now=now)
    except Exception:notification='PENDING_ENQUEUE_REPAIR'
    return dict(status='RECORD_REQUIRED',record_required=True,latch=body,internal_alert=notification)


def observe_first_day_packet(store,policy,*,account_id,packet,now,guard_anomaly=False):
    now=stamp(now);minute=now.replace(second=0,microsecond=0);day=now.date().isoformat()
    if not active(policy,now):return dict(status='NOT_FIRST_DAY',record_required=False)
    _tables(store)
    with closing(store._connect()) as db:
        old=db.execute('SELECT 1 FROM control_record_latch WHERE account_id=? AND trade_date=?',(account_id,day)).fetchone()
    if old:return latch_record(store,policy,account_id=account_id,now=now,reason_code='ALREADY_LATCHED')
    if guard_anomaly:return latch_record(store,policy,account_id=account_id,now=now,
        reason_code='CONTROL_FIRST_DAY_GUARD_ANOMALY',packet_sha256=packet.get('packet_sha256'))
    if not time.fromisoformat(policy['observation_start'])<=minute.time().replace(tzinfo=None)<=time.fromisoformat(policy['observation_end']):
        return dict(status='OUTSIDE_OBSERVATION_WINDOW',record_required=False)
    digest=hashlib.sha256(canonical({k:v for k,v in packet.items() if k!='packet_sha256'}).encode()).hexdigest()
    if (digest!=packet.get('packet_sha256') or stamp(packet['binding']['minute'])!=minute
            or set(packet['sources'])!=set(packet['binding']['symbols'])):
        raise ValueError('CONTROL_FIRST_DAY_PACKET_BINDING_INVALID')
    rows=[];captured=stamp(packet['captured_at'])
    for symbol,source in sorted(packet['sources'].items()):
        raw=source['quote'];typed=QuoteResult.model_validate(raw);quote=typed.quote
        fresh=bool(quote is not None and quote.symbol==typed.symbol==symbol and quote.quote_time.date()==now.date()
            and 0<=(captured-quote.quote_time).total_seconds()<=policy['quote_freshness_max_age_seconds']
            and captured.replace(second=0,microsecond=0)==minute)
        rows.append((account_id,day,minute.isoformat(),symbol,digest,hashlib.sha256(canonical(raw).encode()).hexdigest(),int(fresh)))
    with closing(store._connect()) as db,db:
        db.execute('BEGIN IMMEDIATE')
        for row in rows:
            old=db.execute('SELECT account_id,trade_date,decision_minute,symbol,packet_sha256,quote_sha256,fresh FROM '
                'control_first_day_observations WHERE account_id=? AND trade_date=? AND decision_minute=? AND symbol=?',row[:4]).fetchone()
            if old is not None and old!=row:raise ValueError('CONTROL_FIRST_DAY_OBSERVATION_CHANGED')
            if old is None:db.execute('INSERT INTO control_first_day_observations VALUES (?,?,?,?,?,?,?)',row)
        total,bad=db.execute('SELECT count(*),coalesce(sum(1-fresh),0) FROM control_first_day_observations WHERE account_id=? AND trade_date=?',
            (account_id,day)).fetchone()
    if total and bad*100>policy['maximum_freshness_data_block_pct']*total:
        return latch_record(store,policy,account_id=account_id,now=now,
            reason_code='CONTROL_FIRST_DAY_QUOTE_FRESHNESS_GT20PCT',packet_sha256=digest)
    return dict(status='OBSERVED',record_required=False,checked_quotes=total,blocked_quotes=bad,
        denominator='DISTINCT_MINUTE_SYMBOL_QUOTE_CHECKS_NOT_POLLS')
