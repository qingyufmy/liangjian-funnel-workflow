"""Owned morning catalogue + dated closed ladder, using existing client limits.

No production constructors/DB, rules reinterpretation, model or notification.
The bounded worker never writes; a late result cannot publish a daily baseline.
"""
from datetime import datetime,time
import hashlib
import json
import gzip
import os
from pathlib import Path
import time as timer
from zoneinfo import ZoneInfo

from ..pipeline.data_source import HithinkClient
from ..pipeline.a2_features import _symbol
from ..facts.hithink import project_closed_ladder
from .bounded_work import BoundedWorkGate
from .team_v2_emotion_source import _source_stamp,_index,emotion_pool_stats
from .team_v2_rule_source import _write_once
from .team_v2_sources import stamp
from .team_v2_store import canonical

_GATE=BoundedWorkGate(1)


def _sealed(body):
    body=dict(body);body['body_sha256']=hashlib.sha256(canonical(body).encode()).hexdigest()
    return body


def load_emotion_baseline(settings,*,now):
    now=stamp(now);root=Path(settings.root).resolve();directory=root/'state/team_v2/emotion_baseline'
    path=directory/(now.date().isoformat()+'.json')
    try:
        if directory.resolve()!=directory or path.is_symlink() or path.stat().st_size>32*1024*1024:raise ValueError
        value=json.loads(path.read_bytes());pin=value.pop('body_sha256')
        if hashlib.sha256(canonical(value).encode()).hexdigest()!=pin:raise ValueError
        known=stamp(value['known_at'])
        if (value['trade_date']!=now.date().isoformat() or known.date()!=now.date() or known>now
                or known.time().replace(tzinfo=None)>=time(9,30)):raise ValueError
        return {**value,'body_sha256':pin}
    except Exception:return dict(status='DATA_LIMITED',reason_code='EMOTION_BASELINE_MISSING_OR_UNPROVEN')


def _project(catalogue,ladder,*,now,calendar):
    day=now.date();previous=calendar.previous_trading_day(day)
    if not catalogue.ok or not catalogue.complete or not catalogue.items:
        raise ValueError('EMOTION_CATALOGUE_INCOMPLETE')
    cat_at=_source_stamp(catalogue.metadata.get('timestamp'))
    if cat_at.date()!=day or cat_at>now:raise ValueError('EMOTION_CATALOGUE_DATE_UNPROVEN')
    rows=[r.model_dump(mode='json') for r in catalogue.items];cats=_index(rows)
    closed=project_closed_ladder(ladder,market_trade_date=previous)
    if not closed.ok or not closed.complete:raise ValueError('EMOTION_PRIOR_LADDER_INCOMPLETE')
    dated=[r.model_dump(mode='json') for r in closed.items if r.model_dump(mode='json').get('date')==str(previous)]
    if len(dated)!=1:raise ValueError('EMOTION_PRIOR_LADDER_DATE_UNPROVEN')
    # Zero is justified only by absence in the complete, exactly dated ladder.
    # Pool continue_day_cnt is deliberately never used as a continuity source.
    counts={symbol:0 for symbol in cats};seen=set()
    for group in dated[0]['boards'].values():
        for stock in group:
            symbol=_symbol(stock);count=stock.get('board_num')
            if not symbol or symbol in seen or type(count) is not int or count<1:
                raise ValueError('EMOTION_PRIOR_LADDER_ROW_UNPROVEN')
            seen.add(symbol)
            if symbol in counts:counts[symbol]=count
    return dict(status='READY',trade_date=str(day),known_at=now.isoformat(),
        catalogue=dict(rows=rows,as_of=cat_at.isoformat(),complete=True,
            source=catalogue.endpoint,normalized_result_sha256=hashlib.sha256(catalogue.model_dump_json().encode()).hexdigest()),
        previous_streaks=dict(complete=True,trade_date=str(previous),known_at=now.isoformat(),counts=counts,
            source=ladder.endpoint,zero_basis='ABSENT_FROM_COMPLETE_DATED_LADDER',
            normalized_result_sha256=hashlib.sha256(ladder.model_dump_json().encode()).hexdigest()),
        source_results=dict(catalogue=catalogue.model_dump(mode='json'),ladder=ladder.model_dump(mode='json')),
        authority='EMOTION_STATISTICS_ONLY',source_authenticated=False)


def prepare_emotion_baseline(settings,*,clock,calendar,client_factory=HithinkClient,budget_seconds=10):
    now=stamp(clock());day=now.date().isoformat()
    def limited(code):return dict(status='DATA_LIMITED',reason_code=code,trade_date=day)
    if not calendar.is_trading_day(now.date()):return limited('NON_TRADING_DAY')
    if now.time().replace(tzinfo=None)>=time(9,30):return limited('EMOTION_BASELINE_WINDOW_CLOSED')
    root=Path(settings.root).resolve();directory=root/'state/team_v2/emotion_baseline'
    if directory.resolve()!=directory:raise ValueError('TEAM_EMOTION_DIRECTORY_INVALID')
    target=directory/(day+'.json');claim=directory/(day+'.claim.json')
    if target.is_symlink() or claim.is_symlink():raise ValueError('TEAM_EMOTION_FILE_SCOPE_INVALID')
    if target.exists():return load_emotion_baseline(settings,now=now)
    if not getattr(settings,'hithink_api_key',None):return limited('HITHINK_API_KEY_MISSING')
    if isinstance(budget_seconds,bool) or not 0<float(budget_seconds)<=30:raise ValueError('EMOTION_BUDGET_INVALID')
    try:_write_once(claim,dict(trade_date=day,started_at=now.isoformat()))
    except FileExistsError:return limited('EMOTION_BASELINE_ALREADY_CLAIMED')
    deadline=timer.monotonic()+budget_seconds
    def worker():
        with client_factory(settings) as client:
            catalogue=client.ticker_catalog(limit=1000,max_pages=10)
            if timer.monotonic()>=deadline:raise ValueError('EMOTION_SOURCE_DEADLINE_EXCEEDED')
            ladder=client.limit_up_ladder()
        return catalogue,ladder
    work=_GATE.call(worker,deadline=deadline,name='team-emotion-baseline')
    if work.status!='READY':body=limited('EMOTION_BASELINE_SOURCE_BUDGET_OR_REQUEST_FAILED')
    else:
        try:body=_project(*work.value,now=stamp(clock()),calendar=calendar)
        except Exception as exc:
            body=limited(str(exc) if type(exc) is ValueError and str(exc).startswith('EMOTION_') else 'EMOTION_BASELINE_SOURCE_UNPROVEN')
        if stamp(clock()).time().replace(tzinfo=None)>=time(9,30) or timer.monotonic()>=deadline:
            body=limited('EMOTION_BASELINE_COMPLETED_TOO_LATE')
    body.setdefault('known_at',stamp(clock()).isoformat());body=_sealed(body)
    _write_once(target,body)
    return body


def observe_full_market(settings,result,*,observed_at,calendar,deadline):
    """Consume the collector's exact result; never fetch or relabel old quotes."""
    now=stamp(observed_at);baseline=load_emotion_baseline(settings,now=now)
    try:source_at=_source_stamp(result.metadata.get('timestamp')).isoformat()
    except (ValueError,TypeError,OverflowError):source_at=None
    if baseline['status']!='READY':return dict(status='DATA_BLOCK',reason_code='EMOTION_BASELINE_NOT_READY',
        source_as_of=source_at,known_at=now.isoformat())
    if not result.ok or not result.complete:return dict(status='DATA_BLOCK',reason_code='EMOTION_FULL_MARKET_INCOMPLETE')
    if timer.monotonic()>=deadline:return dict(status='DATA_BLOCK',reason_code='EMOTION_COLLECTION_DEADLINE_EXCEEDED')
    stats=emotion_pool_stats(baseline['catalogue']['rows'],[r.model_dump(mode='json') for r in result.items],
        catalogue_timestamp=baseline['catalogue']['as_of'],snapshot_timestamp=result.metadata.get('timestamp'),
        observed_at=now,calendar=calendar,previous_streaks=baseline['previous_streaks'],
        catalogue_complete=True,snapshot_complete=True)
    frame=dict(schema_version='team-emotion-frame/1',known_at=now.isoformat(),
        baseline_sha256=baseline['body_sha256'],source=result.model_dump(mode='json'),emotion=stats)
    raw=canonical(frame).encode();pin=hashlib.sha256(raw).hexdigest();compressed=gzip.compress(raw,mtime=0)
    root=Path(settings.root).resolve();directory=root/'state/team_v2/emotion_live'/now.date().isoformat()
    if directory.resolve()!=directory:raise ValueError('TEAM_EMOTION_DIRECTORY_INVALID')
    if timer.monotonic()>=deadline:return dict(status='DATA_BLOCK',reason_code='EMOTION_COLLECTION_DEADLINE_EXCEEDED')
    directory.mkdir(parents=True,exist_ok=True);path=directory/(pin+'.json.gz')
    try:
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'wb') as output:output.write(compressed);output.flush();os.fsync(output.fileno())
    except FileExistsError:
        if path.is_symlink() or path.read_bytes()!=compressed:raise ValueError('EMOTION_SOURCE_FRAME_CONFLICT')
    return dict(status=stats['status'],known_at=now.isoformat(),source_as_of=stats['as_of'],
        frame_path=str(path.relative_to(root)),frame_sha256=pin,
        baseline_sha256=baseline['body_sha256'],emotion={k:v for k,v in stats.items() if k!='identities'})
