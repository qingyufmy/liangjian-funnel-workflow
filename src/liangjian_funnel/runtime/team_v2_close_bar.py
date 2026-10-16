"""15:01 owned paper close consumer, no new intent/model/legacy constructors.

0263: original frozen 15:00 packet first, otherwise one normal acquisition
per owned symbol during 15:01--15:05. Settle may reuse, never fetch or relabel.
All capture attempts survive process restart; unknown outcomes are not retried.
"""
from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, time as wall_time
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

from ..data.mootdx import FetchResult, MinuteBar
from ..data.session_windows import closed_window_ends
from .broker.local_paper import create_broker
from .calendar import ExchangeTradingCalendar
from .team_v2_internal import InternalOutbox
from .team_v2_minute_projection import project_closed_minute
from .team_v2_morning import ACCOUNT, _directory
from .team_v2_sources import stamp, TZ
from .team_v2_store import TeamMeetingStore, canonical
from .team_v2_tradability import production_monitor_tradability


def _write_once(path, body):
    if path.is_symlink() or path.resolve()!=path:raise ValueError('CLOSE_PACKET_SCOPE_INVALID')
    path.parent.mkdir(parents=True,exist_ok=True)
    digest=hashlib.sha256(canonical(body).encode()).hexdigest()
    with os.fdopen(os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600),'w',encoding='utf-8',newline='\n') as output:
        output.write(canonical({**body,'packet_sha256':digest}));output.flush();os.fsync(output.fileno())


def _read(path):
    if path.is_symlink() or path.resolve()!=path or path.stat().st_size>32*1024*1024:
        raise ValueError('CLOSE_PACKET_SCOPE_INVALID')
    body=json.loads(path.read_text(encoding='utf-8'));digest=body.pop('packet_sha256')
    if hashlib.sha256(canonical(body).encode()).hexdigest()!=digest:raise ValueError('CLOSE_PACKET_HASH_MISMATCH')
    return body,digest


def _fetcher(settings):
    # Same existing A4 provider order, node health and limits. No application,
    # RuntimeStore, model, quote request, full-market refresh or new supplier.
    from ..data.mootdx import MootdxAdapter, MootdxNode
    from ..data.tencent_minute import TencentIntradayAdapter
    from ..data.live_fetch import fetch_live_window
    primary=TencentIntradayAdapter(timeout_seconds=min(12.0,settings.timeout_seconds))
    secondary=MootdxAdapter(tuple(MootdxNode(host=h,port=p) for h,p in settings.mootdx_servers),
        page_size=settings.mootdx_page_size,max_pages=settings.mootdx_max_pages,
        timeout_seconds=settings.mootdx_timeout_seconds)
    secondary.live_health_path=settings.minute_cache_dir/'live-node-health.json'
    return lambda symbol,cutoff,count,end:fetch_live_window(primary,secondary,symbol,'1m',count,cutoff,deadline=end)


def _plans(settings, identities):
    path=Path(settings.state_db_path)
    if not path.is_file() or path.is_symlink():return {}
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        try:
            rows=[dict(r) for r in db.execute('SELECT plan_id,symbol,payload_json FROM execution_plans WHERE lane_id=? AND plan_id IN ('+
                ','.join('?' for _ in identities)+')',(settings.research_primary_lane_id,*identities))]
        finally:db.execute('ROLLBACK')
    result={}
    for row in rows:
        try:
            raw=json.loads(row['payload_json'])
            if row['plan_id'] in result or raw.get('symbol')!=row['symbol'] or identities[row['plan_id']]!=row['symbol']:
                raise ValueError('CLOSE_OWNER_BINDING_INVALID')
            result[row['plan_id']]=row
        except (ValueError,TypeError,KeyError):raise ValueError('CLOSE_OWNER_BINDING_INVALID')
    return result


def _capture(settings, *, symbol, identities, plans, cutoff, clock, fetch, deadline):
    original=Path(settings.root)/'state/team_v2/minutes'/(cutoff.strftime('%Y%m%d')+'T1501.json')
    captured=stamp(clock());provenance='NORMAL_EXISTING_PROVIDER_ONCE'
    rows=None;reason='CLOSE_BAR_MISSING';original_sha=None
    if original.exists():
        packet,original_sha=_read(original)
        arrival=stamp(packet['captured_at'])
        if (packet['execution_cutoff']!=cutoff.isoformat() or arrival.date()!=cutoff.date()
                or not wall_time(15,1)<=arrival.time().replace(tzinfo=None)<=wall_time(15,5)
                or arrival>captured):raise ValueError('CLOSE_ORIGINAL_PACKET_TIME_INVALID')
        packed=packet.get('sources',{}).get(symbol,{})
        if packed.get('minute_complete'):
            rows=tuple(MinuteBar.model_validate(b) for b in packed.get('bars',()))
            captured=arrival;provenance='ORIGINAL_FROZEN_MONITOR_PACKET'
    if rows is None:
        try:
            if time.monotonic()>=deadline:raise ValueError('CLOSE_CAPTURE_DEADLINE')
            started=stamp(clock())
            if started.date()!=cutoff.date() or not wall_time(15,1)<=started.time().replace(tzinfo=None)<=wall_time(15,5):
                raise ValueError('CLOSE_CAPTURE_WINDOW_CLOSED')
            fetch=fetch or _fetcher(settings)
            count=len(closed_window_ends(cutoff,'1m'));result=fetch(symbol,cutoff,count,deadline)
            captured=stamp(clock())
            if (not isinstance(result,FetchResult) or not result.complete or result.symbol!=symbol
                    or result.interval!='1m' or result.requested_bars!=count):raise ValueError('CLOSE_SOURCE_UNPROVEN')
            rows=result.bars
        except Exception:rows=()
    paper=None
    if (captured.date()==cutoff.date() and wall_time(15,1)<=captured.time().replace(tzinfo=None)<=wall_time(15,5)):
        projected=project_closed_minute(rows,(),observed_at=captured,execution_cutoff=cutoff)
        bar=projected.get('paper_bar')
        if bar is not None and bar.symbol==symbol and set(plans)==set(identities):
            proofs=[production_monitor_tradability(settings,symbol=symbol,now=captured,bar=bar,quote=None,
                plan=json.loads(row['payload_json']),minute_bars=rows,max_bar_age_seconds=300) for row in plans.values()]
            if proofs and all(p['status']=='KNOWN' for p in proofs):
                # Multiple owners must not silently select a more permissive
                # flag: conflicting original limit contracts stay unproven.
                flags={(p['halted'],p['limit_up'],p['limit_down']) for p in proofs}
                if len(flags)==1:
                    halted,up,down=next(iter(flags))
                    paper=asdict(replace(bar,tradability_known=True,halted=halted,limit_up=up,limit_down=down))
                    paper['minute_end']=bar.minute_end.isoformat()
    return dict(schema_version='team-close-symbol/1',symbol=symbol,trade_date=cutoff.date().isoformat(),
        execution_cutoff=cutoff.isoformat(),captured_at=captured.isoformat(),owners=identities,original_plan_rows=plans,
        source_kind=provenance,original_packet_sha256=original_sha,
        bars=[b.model_dump(mode='json') for b in rows],paper_bar=paper,
        status='COMPLETE' if paper is not None else 'DATA_LIMITED',reason_code=None if paper is not None else reason)


def run_close_bar(settings, *, clock=None, calendar=None, fetch_bars=None, reuse_only=False, broker=None):
    clock=clock or (lambda:datetime.now(TZ));now=stamp(clock());calendar=calendar or ExchangeTradingCalendar()
    if calendar.is_trading_day(now.date()) is not True:return dict(status='NOOP',reason_code='NON_TRADING_DAY')
    if not reuse_only and not wall_time(15,1)<=now.time().replace(tzinfo=None)<=wall_time(15,5):
        return dict(status='NOOP',reason_code='CLOSE_BAR_WINDOW_CLOSED')
    if reuse_only and now.time().replace(tzinfo=None)<wall_time(15,1):
        return dict(status='NOOP',reason_code='CLOSE_BAR_WINDOW_CLOSED')
    directory=_directory(settings)
    if not (directory/'paper.db').is_file():return dict(status='NOOP',reason_code='PAPER_ACCOUNT_NOT_INITIALIZED')
    broker=broker or create_broker('local_paper',database=directory/'paper.db',paper_config=settings.root/'config/paper_broker.yaml',
        risk_config=settings.root/'config/risk_limits.yaml',clock=clock,calendar=calendar,run_id='team-close-bar:'+now.date().isoformat())
    snapshot=broker.planning_snapshot();owners={}
    for row in (*snapshot['positions'],*snapshot['open_orders']):
        old=owners.setdefault(row['plan_id'],row['symbol'])
        if old!=row['symbol']:raise ValueError('CLOSE_OWNER_BINDING_INVALID')
    cutoff=now.replace(hour=15,minute=0,second=0,microsecond=0);day=now.date().isoformat()
    fills=[];missing=[];packets={};deadline=time.monotonic()+50
    for symbol in sorted(set(owners.values())):
        identity={pid:s for pid,s in owners.items() if s==symbol}
        path=directory/'close-bars'/day/(symbol+'.json');claim=path.with_suffix('.claim.json')
        try:
            if not path.exists() and not reuse_only and not claim.exists():
                _write_once(claim,dict(symbol=symbol,owners=identity,started_at=stamp(clock()).isoformat()))
                plans=_plans(settings,identity)
                _write_once(path,_capture(settings,symbol=symbol,identities=identity,plans=plans,cutoff=cutoff,
                    clock=clock,fetch=fetch_bars,deadline=deadline))
            if not path.exists():raise ValueError('CLOSE_BAR_MISSING')
            body,digest=_read(path);packets[symbol]=digest
            if (body['symbol']!=symbol or body['trade_date']!=day or body['execution_cutoff']!=cutoff.isoformat()
                    or any(body['owners'].get(pid)!=s for pid,s in identity.items()) or body['status']!='COMPLETE'):
                raise ValueError('CLOSE_BAR_UNPROVEN')
            from liangjian_paper_broker.matching import MinuteBar as PaperBar
            raw=dict(body['paper_bar']);raw['minute_end']=stamp(raw['minute_end'])
            for key in ('low','high','vwap','close'):
                if raw[key] is not None:raw[key]=Decimal(raw[key])
            if raw['minute_end']!=cutoff:raise ValueError('CLOSE_BAR_TIMESTAMP_INVALID')
            fills.extend(broker.on_bar(PaperBar(**raw)))
        except Exception:missing.append(symbol)
    if missing:
        InternalOutbox(TeamMeetingStore(directory/'team.db')).enqueue(account_id=ACCOUNT,trade_date=day,
            kind='CLOSE_BAR_MISSING',source_id='close:'+day,observed_at=now,priority=5,
            body=dict(msg_type='text',content=dict(text='CLOSE_BAR_MISSING '+day+'; original 15:00 paper close is unproven; NAV remains DATA_LIMITED.')))
    return dict(status='DATA_LIMITED' if missing else 'COMPLETE',trade_date=day,execution_cutoff=cutoff.isoformat(),
        fills=list(fills),missing_symbols=missing,packet_sha256=packets,no_new_intents=True,source_acquisition_allowed=not reuse_only)
