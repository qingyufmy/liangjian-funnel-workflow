"""LJ-052: native previous-session seeds only; never fetch or fall back a day."""
from contextlib import closing
from datetime import datetime, time, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import time as timer
from zoneinfo import ZoneInfo

from ..data.cache import _from_row
from ..data.mootdx import map_symbol

TZ=ZoneInfo('Asia/Shanghai')


def session_grid(day):
    base=datetime.combine(day,time(),tzinfo=TZ)
    return tuple(base.replace(hour=h,minute=m)+timedelta(minutes=5*n)
                 for h,m in ((9,30),(13,0)) for n in range(1,25))


def validate_session_seed(symbol,bars,*,target_trade_date,calendar,as_of,actual_latest_date=None):
    symbol=map_symbol(symbol).canonical
    row=dict(schema_version='previous-session-5m-seed/1',symbol=symbol,
        target_trade_date=target_trade_date.isoformat(),status='DATA_BLOCKED',required_5m_count=48,
        available_5m_count=0,actual_latest_date=actual_latest_date,source_trade_date=None,
        reason_code='HISTORICAL_5M_PREVIOUS_SESSION_UNPROVEN',source_requests=0)
    try:
        if not isinstance(as_of,datetime) or as_of.utcoffset() is None:raise ValueError('HISTORICAL_5M_CLOCK_INVALID')
        prior=calendar.previous_trading_day(target_trade_date)
        expected=session_grid(prior);row['source_trade_date']=prior.isoformat()
        row['history_cutoff']=expected[-1].isoformat()
        values=tuple(bars)
        if values and actual_latest_date is None:
            row['actual_latest_date']=max(b.bar_end.astimezone(TZ).date() for b in values).isoformat()
        native=[b for b in values if b.symbol==symbol and b.interval=='5m'
            and b.bar_end in expected and getattr(b,'evidence_kind',None)=='MARKET_BAR'
            and not any(v in str(b.source_id).lower() for v in ('synthetic','aggregate','derived',':risk_only'))]
        row['available_5m_count']=len({b.bar_end for b in native})
        if expected[-1]>as_of:raise ValueError('HISTORICAL_5M_SESSION_NOT_CLOSED')
        if len(values)!=48 or len(native)!=48 or {b.bar_end for b in native}!=set(expected):
            raise ValueError('HISTORICAL_5M_PREVIOUS_SESSION_INCOMPLETE')
        selected=tuple(sorted(native,key=lambda b:b.bar_end))
        row.update(status='READY',reason_code='OK',native_rowset_sha256=hashlib.sha256(json.dumps(
            [b.model_dump(mode='json') for b in selected],sort_keys=True,separators=(',',':')).encode()).hexdigest())
        return selected,row
    except Exception as exc:
        if isinstance(exc,ValueError) and str(exc).startswith('HISTORICAL_5M_'):row['reason_code']=str(exc)
        return (),row


def read_previous_session_seed(path,symbol,*,target_trade_date,calendar,as_of):
    """Two bounded indexed RO queries, no cache constructor/schema/write side effects."""
    latest=None;bars=()
    try:
        prior=calendar.previous_trading_day(target_trade_date);grid=session_grid(prior)
        path=Path(path)
        if not path.is_file() or path.is_symlink():raise ValueError('HISTORICAL_5M_CACHE_UNAVAILABLE')
        symbol=map_symbol(symbol).canonical
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.15)) as db:
            db.execute('PRAGMA query_only=ON');db.execute('PRAGMA busy_timeout=150')
            deadline=timer.monotonic()+.15
            db.set_progress_handler(lambda: int(timer.monotonic()>deadline),1000)
            fields='symbol,interval,bar_end,open_value,high_value,low_value,close_value,volume_value,amount_value,source_id,adjust_mode'
            values=db.execute('SELECT '+fields+' FROM minute_bars WHERE symbol=? AND interval=? AND bar_end>=? AND bar_end<? ORDER BY bar_end LIMIT 49',
                (symbol,'5m',grid[0].replace(hour=0,minute=0).isoformat(),
                 (grid[0].replace(hour=0,minute=0)+timedelta(days=1)).isoformat())).fetchall()
            last=db.execute('SELECT bar_end FROM minute_bars WHERE symbol=? AND interval=? AND bar_end<? ORDER BY bar_end DESC LIMIT 1',
                (symbol,'5m',datetime.combine(target_trade_date,time(),tzinfo=TZ).isoformat())).fetchone()
            latest=datetime.fromisoformat(last[0]).astimezone(TZ).date().isoformat() if last else None
            bars=tuple(_from_row(row) for row in values)
    except Exception:
        pass
    return validate_session_seed(symbol,bars,target_trade_date=target_trade_date,calendar=calendar,
        as_of=as_of,actual_latest_date=latest)


def inspect_plan_seeds(plans,*,path,target_trade_date,calendar,as_of):
    memo={};rows=[]
    for plan in plans:
        symbol=plan['symbol']
        if symbol not in memo:
            _,memo[symbol]=read_previous_session_seed(path,symbol,target_trade_date=target_trade_date,calendar=calendar,as_of=as_of)
        rows.append(dict(memo[symbol],plan_id=plan['plan_id']))
    return dict(schema_version='plan-seed-preflight/1',status='READY' if all(r['status']=='READY' for r in rows) else 'DATA_LIMITED',
        checked_at=as_of.isoformat(),plans=rows,source_requests=0)
