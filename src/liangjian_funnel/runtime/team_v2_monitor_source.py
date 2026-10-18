"""Bounded existing minute/quote acquisition into an immutable owned packet.

Normal runtime collectors may call providers; this module is not a read-only
production audit. It has no legacy store, model, activation, order or customer
callback. Frozen packet completeness never grants price-limit/tradability.
"""
import hashlib
import json
import os
from pathlib import Path
import time

from ..data.mootdx import FetchResult
from ..data.tencent_minute import QuoteResult
from ..data.live_quote import validate_quote
from ..data.publication import ACQUISITION_BUDGET_SECONDS,confirm_publications
from ..data.session_windows import closed_window_ends
from .bounded_work import BoundedWorkGate,run_many_bounded
from .team_v2_sources import stamp
from .team_v2_store import canonical

_WORK=BoundedWorkGate(8)


def collect_monitor_packet(settings,scope,*,clock,deadline,fetch_bars,fetch_quote,
        monotonic=time.monotonic,gate=_WORK):
    scope=json.loads(canonical(scope))
    now=stamp(clock());minute=now.replace(second=0,microsecond=0)
    from datetime import timedelta
    cutoff=minute-timedelta(minutes=1)
    if scope.get('trade_date')!=now.date().isoformat() or scope.get('lane_id')!=settings.research_primary_lane_id:
        raise ValueError('CONTROL_SCOPE_DATE_LANE_MISMATCH')
    rows=scope['plan_rows']
    if hashlib.sha256(canonical(rows).encode()).hexdigest()!=scope.get('plan_rowset_sha256'):
        raise ValueError('CONTROL_SCOPE_ROWSET_HASH_MISMATCH')
    symbols=sorted({p['symbol'] for p in scope['plans']})
    binding=dict(trade_date=scope['trade_date'],minute=minute.isoformat(),symbols=symbols,
        scope_rowset_sha256=scope['plan_rowset_sha256'],morning_rowset_sha256=scope['morning_rowset_sha256'])
    binding_hash=hashlib.sha256(canonical(binding).encode()).hexdigest()
    directory=Path(settings.root)/'state/team_v2/minutes'
    if directory.resolve()!=Path(settings.root).resolve()/'state/team_v2/minutes':
        raise ValueError('CONTROL_PACKET_DIRECTORY_INVALID')
    target=directory/(minute.strftime('%Y%m%dT%H%M')+'.json')
    def existing():
        if target.is_symlink():raise ValueError('CONTROL_PACKET_SCOPE_INVALID')
        body=json.loads(target.read_text(encoding='utf-8'));digest=body.pop('packet_sha256')
        if hashlib.sha256(canonical(body).encode()).hexdigest()!=digest:
            raise ValueError('CONTROL_PACKET_HASH_MISMATCH')
        if body['binding_sha256']!=binding_hash:raise ValueError('CONTROL_PACKET_SCOPE_CHANGED')
        return {**body,'packet_sha256':digest}
    if target.exists():return existing()
    expected=closed_window_ends(cutoff,'1m');count=len(expected)
    if not count or expected[-1]!=cutoff:
        return dict(status='NOOP',reason_code='WAITING_BAR_CLOSE',binding=binding)
    acquisition_deadline=min(deadline,monotonic()+ACQUISITION_BUDGET_SECONDS)
    def minute_fetch(symbol):
        result=fetch_bars(symbol,cutoff,count,acquisition_deadline)
        if (not isinstance(result,FetchResult) or result.symbol!=symbol or result.interval!='1m'
                or result.requested_bars!=count):raise ValueError('CONTROL_SOURCE_BAR_BINDING_INVALID')
        if (tuple(b.bar_end for b in result.bars)!=expected or any(b.symbol!=symbol or b.interval!='1m'
                or b.evidence_kind!='MARKET_BAR' or b.volume_unit!='shares'
                or b.adjust_mode not in {'none','raw'} for b in result.bars)):
            result=result.model_copy(update={'complete':False,'reason_code':'CONTROL_SOURCE_CLOSED_SCOPE_INVALID'})
        return symbol,{'1m':result,'5m':None}
    def collect(symbol):
        _,pack=minute_fetch(symbol)
        try:pack['quote']=fetch_quote(symbol,stamp(clock()),acquisition_deadline)
        except Exception:pack['quote']=QuoteResult(symbol=symbol,reason_code='CONTROL_QUOTE_UNAVAILABLE')
        return symbol,pack
    work=run_many_bounded({s:lambda s=s:collect(s) for s in symbols},deadline=acquisition_deadline,
        gate=gate,clock=monotonic,max_workers=8)
    initial={}
    for symbol,item in work.items():
        if item.status=='READY' and item.value[0]==symbol:initial[symbol]=item.value[1]
        else:initial[symbol]={'fetch_error':'CONTROL_SOURCE_'+item.status}
    confirmed=confirm_publications(initial,minute_fetch,at=cutoff,deadline=acquisition_deadline,
        clock=monotonic,wall_clock=lambda:stamp(clock()),closed_window=True)
    captured=stamp(clock());packs={};problems=list(scope.get('reason_codes',()))
    if captured.replace(second=0,microsecond=0)!=minute:
        problems.append(dict(reason_code='CONTROL_CAPTURE_MINUTE_MOVED'))
    for symbol in symbols:
        selected=confirmed[symbol];one=selected.get('1m');quote=initial[symbol].get('quote')
        if not isinstance(quote,QuoteResult):quote=QuoteResult(symbol=symbol,reason_code='CONTROL_QUOTE_UNAVAILABLE')
        checked=validate_quote(quote,symbol,as_of=captured)
        if checked.quote is not None:
            from .team_v2_quote_evidence import assert_original_market_quote
            try:assert_original_market_quote(checked.quote)
            except ValueError:
                checked=checked.model_copy(update={'complete':False,'reason_code':'CONTROL_REPLAY_QUOTE_FORBIDDEN'})
        # Existing source freshness is necessary but future provider data is
        # never known at capture, even inside the provider's timestamp tolerance.
        if checked.quote is not None and checked.quote.quote_time>captured:
            checked=checked.model_copy(update={'complete':False,'reason_code':'CONTROL_QUOTE_FUTURE'})
        minute_ok=bool(one is not None and one.complete and not selected.get('publication_error'))
        if not minute_ok:problems.append(dict(symbol=symbol,reason_code='CONTROL_CLOSED_MINUTE_UNAVAILABLE'))
        if not checked.complete:problems.append(dict(symbol=symbol,reason_code='CONTROL_CURRENT_QUOTE_UNAVAILABLE'))
        packs[symbol]=dict(bars=[b.model_dump(mode='json') for b in one.bars] if one is not None else [],
            minute_complete=minute_ok,publication=selected.get('publication'),
            quote=checked.model_dump(mode='json'),tradability_authority='UNPROVEN_RULE_ADAPTER_PENDING')
    body=dict(schema_version='team-monitor-packet/1',status='CAPTURED' if not problems else 'DATA_LIMITED',
        binding=binding,binding_sha256=binding_hash,captured_at=captured.isoformat(),scope=scope,
        producer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        execution_cutoff=cutoff.isoformat(),sources=packs,reason_codes=problems,
        no_model_or_order_authority=True)
    digest=hashlib.sha256(canonical(body).encode()).hexdigest();body['packet_sha256']=digest
    directory.mkdir(parents=True,exist_ok=True)
    # No replace/overwrite on retry; a partial interrupted packet is blocked
    # rather than refreshed into a different point-in-time input.
    try:
        fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    except FileExistsError:return existing()
    with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as output:
        output.write(canonical(body));output.flush();os.fsync(output.fileno())
    return body
