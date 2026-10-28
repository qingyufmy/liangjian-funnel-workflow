"""0881 independent query-only count attestation, frozen before A5 model I/O."""
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import re

TRADE_ACTIONS={'BUY_SIGNAL','ADD_SIGNAL','SELL_SIGNAL','REDUCE_SIGNAL','FORCED_RISK_EXIT'}


def technical_opportunities(rows):
    """Count recorded stock/closed-bar opportunities, never minute echoes."""
    keys=set();missing=[]
    for row in rows:
        if not row.get('plan_id'):continue
        action=row.get('technical_action')
        if action is None:
            missing.append(row.get('event_id'));continue
        if action not in {'BUY_SIGNAL','ADD_SIGNAL'}:continue
        label=row.get('closed_5m_end')
        try:
            stamp=datetime.fromisoformat(label)
            minute=datetime.fromisoformat(row['minute_end'])
            if stamp.utcoffset() is None or minute.utcoffset() is None or stamp>minute:
                raise ValueError('CLOSED_BAR_CLOCK_INVALID')
        except (ValueError,TypeError,KeyError):
            missing.append(row.get('event_id'));continue
        keys.add((str(row['plan_id']),stamp.isoformat()))
    return dict(count=None if missing else len(keys),recorded_distinct_count=len(keys),
        keys=[list(key) for key in sorted(keys)],unavailable_event_ids=missing,
        scope='RECORDED_BUY_ADD_PER_PLAN_AND_ORIGINAL_CLOSED_5M_NOT_STOCK_MINUTE_ROWS')


def review_attempt_count(rows,cutoff):
    """The producer persists its decision clock in the immutable session ID."""
    if rows is None:return None
    end=datetime.fromisoformat(cutoff);count=0
    for decision_id,_valid in rows:
        match=re.fullmatch(r'team:(\d{8}T\d{4})(?::reviewed:[A-Za-z0-9_]+)?',decision_id)
        if match is None:return None
        stamp=datetime.strptime(match[1],'%Y%m%dT%H%M').replace(tzinfo=end.tzinfo)
        if stamp<=end:count+=1
    return count


def _hash(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def _read(path, sql, params):
    path=Path(path)
    if not path.is_file():return None
    if path.is_symlink():raise ValueError('A5_COUNT_LEDGER_SCOPE_INVALID')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.execute('PRAGMA query_only=ON')
        return db.execute(sql,params).fetchall()


def reconcile_counts(facts, actual, *, source_sha256, observed_at,technical_evidence=None):
    metrics=facts.get('metrics') or {}
    expected=dict(plans=metrics.get('a3_plan_count'),monitor_rows=metrics.get('a4_monitor_observation_count'),
        signals=metrics.get('a4_trade_signal_count'))
    mismatches={name:dict(fact=value,ledger=actual.get(name)) for name,value in expected.items()
        if type(value) is not int or type(actual.get(name)) is not int or value!=actual[name]}
    if len((facts.get('a3') or {}).get('plans') or [])!=actual.get('plans'):
        mismatches['plan_rows']=dict(fact=len((facts.get('a3') or {}).get('plans') or []),ledger=actual.get('plans'))
    for name in ('model_calls','fills','minute_rows','technical_opportunities'):
        field={'model_calls':'a4_team_model_attempt_count','fills':'a4_team_fill_count','minute_rows':'a4_stock_minute_count',
            'technical_opportunities':'a4_technical_opportunity_count'}[name]
        if field in metrics and metrics[field]!=actual.get(name):
            mismatches[name]=dict(fact=metrics[field],ledger=actual.get(name))
    if mismatches:
        raise ValueError('A5_KEY_COUNT_LEDGER_MISMATCH:'+json.dumps(mismatches,ensure_ascii=False,sort_keys=True))
    if technical_evidence is not None and technical_evidence.get('count')!=actual.get('technical_opportunities'):
        raise ValueError('A5_TECHNICAL_OPPORTUNITY_EVIDENCE_MISMATCH')
    unavailable=[key for key in ('model_calls','fills','technical_opportunities') if key in actual and actual.get(key) is None]
    receipt=dict(status='DATA_LIMITED' if unavailable else 'MATCH',actual=actual,
        unavailable_counts=unavailable,source_sha256=source_sha256,observed_at=observed_at,
        model_scope='A4_TEAM_REVIEW_ATTEMPTS_AT_OR_BEFORE_CUTOFF_INCLUDING_NETWORK_FAILURE_NOT_RAW_REPLY_OBJECTS',
        signal_scope='EFFECTIVE_PUBLISHED_TRADE_EVENT_ROWS_NOT_TECHNICAL_CANDIDATES',
        minute_scope='STOCK_PLAN_MINUTE_ROWS_EXCLUDES_EMPTY_SCOPE',
        snapshot_scope='INDEPENDENT_DATABASE_SNAPSHOTS_AT_OBSERVED_TIME_NOT_ATOMIC_CROSS_DATABASE')
    if technical_evidence is not None:receipt['technical_opportunity_evidence']=technical_evidence
    receipt['sha256']=_hash(receipt)
    return receipt


def freeze_key_counts(facts, *, store, root, observed_at):
    day=facts['trade_date'];cutoff=facts['cutoff_at'];lane=facts['lane_id']
    start=datetime.fromisoformat(cutoff).replace(hour=9,minute=0,second=0,microsecond=0).isoformat()
    events=_read(store.path,'SELECT event_id,minute_end,action,effective,payload_json FROM monitor_events '
        'WHERE lane_id=? AND minute_end>=? AND minute_end<=? ORDER BY minute_end,event_id',(lane,start,cutoff))
    if events is None:raise ValueError('A5_COUNT_SOURCE_MISSING')
    # Verify the exact plan IDs separately against persisted plan identity.
    plans=(facts.get('a3') or {}).get('plans') or []
    ids=sorted(str(row['plan_id']) for row in plans)
    columns=('plan_id','symbol','status','valid_from','expires_at','updated_at','created_at','payload_json')
    all_rows=_read(store.path,'SELECT '+','.join(columns)+' FROM execution_plans WHERE lane_id=?',(lane,))
    from .plan_scope import select_review_plans
    selected,_,_=select_review_plans([dict(zip(columns,row)) for row in all_rows or []],
        cutoff=datetime.fromisoformat(cutoff),observed_ids={str(json.loads(row[4]).get('plan_id') or '') for row in events},
        carryover_ids={str(row.get('plan_id') or '') for row in (facts.get('a4') or {}).get('carryover_plans') or []})
    actual_ids=sorted(str(row['plan_id']) for row in selected)
    if len(set(ids))!=len(ids) or ids!=actual_ids:
        raise ValueError('A5_COUNT_PLAN_IDENTITY_MISMATCH')
    minute_rows=sum(bool(json.loads(row[4]).get('plan_id')) for row in events)
    directory=Path(root)/'state'/'team_v2'
    def optional(path,sql,params):
        try:return _read(path,sql,params)
        except sqlite3.OperationalError as exc:
            if 'no such table' in str(exc):return None
            raise
    attempts=optional(directory/'team.db','SELECT decision_id,valid FROM trader_review_attempts '
        'WHERE account_id=? AND trade_date=? ORDER BY decision_id',('local-paper',day))
    fills=optional(directory/'paper.db','SELECT fill_id,minute_end FROM paper_fills '
        'WHERE substr(minute_end,1,10)=? AND minute_end<=? ORDER BY fill_id',(day,cutoff))
    technical_rows=[]
    for row in events:
        payload=json.loads(row[4]);strategy=payload.get('strategy') or {}
        technical_rows.append(dict(event_id=row[0],minute_end=row[1],plan_id=payload.get('plan_id'),
            technical_action=strategy.get('action'),closed_5m_end=strategy.get('closed_5m_end')))
    technical=technical_opportunities(technical_rows)
    actual=dict(plans=len(actual_ids),monitor_rows=len(events),minute_rows=minute_rows,
        signals=sum(bool(r[3]) and r[2] in TRADE_ACTIONS for r in events),
        model_calls=review_attempt_count(attempts,cutoff),
        fills=len(fills) if fills is not None else None,
        technical_opportunities=technical['count'])
    receipt=reconcile_counts(facts,actual,source_sha256=_hash(dict(plan_ids=actual_ids,plan_rows=all_rows,event_rows=events,
        review_attempts=attempts,paper_fills=fills)),observed_at=observed_at.isoformat(),technical_evidence=technical)
    return receipt
