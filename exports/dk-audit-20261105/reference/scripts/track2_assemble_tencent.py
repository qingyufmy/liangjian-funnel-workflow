"""Recompute six-index facts from unchanged, explicitly listed new originals.

This is offline evidence assembly, not new provider requests or deployment.
"""
from dataclasses import asdict
from datetime import date,datetime,time
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.market_state.bars15m import TZ,compose_15m
from liangjian_funnel.market_state.calendar import market_calendar
from liangjian_funnel.market_state.config import load_market_definitions
from liangjian_funnel.market_state.dk_shadow import build_dk_shadow,json_value,canonical_minute_input
from track2_collect_tencent import plan_stock_scope
from liangjian_funnel.market_state.index_trend import market_close_facts
from liangjian_funnel.market_state.ssl import initial_ssl,ssl_append,ssl_series
from liangjian_funnel.market_state.tencent_daily import decode_payload,parse_daily


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--report',action='append',type=Path,required=True)
    parser.add_argument('--parser-resolution',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    bindings={}
    def read(path):
        path=Path(path)
        if any(p.is_symlink() for p in (path,*path.parents)):raise ValueError('ASSEMBLY_LINK_REJECTED')
        raw=path.read_bytes();bindings[str(path.resolve())]=hashlib.sha256(raw).hexdigest()
        return raw
    resolution=json.loads(read(args.parser_resolution)) if args.parser_resolution else None
    if resolution and resolution.get('status')!='VERIFIED_WITHOUT_REFETCH':raise ValueError('ASSEMBLY_RESOLUTION_INVALID')
    definitions=load_market_definitions(ROOT/'config');calendar=market_calendar()
    daily={};minutes={};minute_rows={};input_dates=set();stocks=[];scope_proof=None
    for path in args.report:
        raw_report=read(path);report=json.loads(raw_report)
        if not report['status'].startswith('COLLECTED_'):
            if (not resolution or hashlib.sha256(raw_report).hexdigest()!=resolution['source_report_sha256']
                    or report['failure']['reason']!='INDEX_UNADJUSTED_DAY_ROWS_REQUIRED'):
                raise ValueError('ASSEMBLY_FAILED_SOURCE_NOT_RESOLVED')
        day=date.fromisoformat(report['trade_date']);input_dates.add(day)
        reported_scope=report.get('plan_stock_scope')
        if isinstance(reported_scope,dict) and reported_scope.get('status')=='EXPLICIT_FROZEN_SCOPE':
            raw_scope=read(reported_scope['path'])
            if hashlib.sha256(raw_scope).hexdigest()!=reported_scope['file_sha256']:raise ValueError('ASSEMBLY_PLAN_SCOPE_CHANGED')
            selected,proof=plan_stock_scope(Path(reported_scope['path']),day,definitions)
            if scope_proof and scope_proof!=proof:raise ValueError('ASSEMBLY_CONFLICTING_PLAN_SCOPES')
            stocks,scope_proof=selected,proof
        for attempt in report['attempts']:
            key,kind=attempt['key'],attempt['kind']
            if attempt['status_code']!=200:raise ValueError('ASSEMBLY_SOURCE_HTTP_NOT_200')
            raw=read(path.parent/(key+'-'+kind.lower()+'.body'))
            if hashlib.sha256(raw).hexdigest()!=attempt['input_sha256']:raise ValueError('ASSEMBLY_RAW_HASH_MISMATCH')
            payload,_=decode_payload(raw)
            if kind=='DAILY':
                if key in daily:raise ValueError('ASSEMBLY_DUPLICATE_SERIES')
                daily[key]=parse_daily(payload,registry=definitions['registry'],key=key,trade_date=day,
                    input_sha256=attempt['input_sha256'],capture_started_at=attempt['capture_started_at'],
                    capture_completed_at=attempt['capture_completed_at'])
            elif kind=='MINUTE':
                if key in minutes:raise ValueError('ASSEMBLY_DUPLICATE_SERIES')
                minutes[key],rows=canonical_minute_input(payload,key=key,definitions=definitions,trade_date=day,
                    input_sha256=attempt['input_sha256'],capture_started_at=attempt['capture_started_at'],capture_completed_at=attempt['capture_completed_at'])
                minute_rows[key]=rows
    if len(input_dates)!=1:raise ValueError('ASSEMBLY_MIXED_DATES')
    day=next(iter(input_dates));now=datetime.now(TZ)
    market=market_close_facts(daily,definitions=definitions,calendar=calendar)
    dk=build_dk_shadow(minutes,plan_symbols=stocks,definitions=definitions,trade_date=day,observed_at=now,calendar=calendar)
    incremental={};archive={}
    for key,rows in minute_rows.items():
        symbol=definitions['registry']['indices'][key]['thscode'] if key in definitions['registry']['indices'] else key
        composed=[compose_15m([r for r in rows if r['bar_end'].date()==session],symbol=symbol,trade_date=session,
            observed_at=now,market_close_at=datetime.combine(session,time(15),tzinfo=TZ),calendar=calendar)
            for session in sorted({r['bar_end'].date() for r in rows})]
        closed=[bar for session in composed for bar in session.bars if bar.status=='CLOSED']
        incremental[key]={}
        for period in (10,20,30):
            memory=initial_ssl(period);points=[]
            for bar in closed:
                memory,point=ssl_append(memory,bar,observed_at=now,calendar=calendar);points.append(point)
            batch=ssl_series(closed,period=period,observed_at=now,calendar=calendar)
            incremental[key][str(period)]=dict(equal=tuple(points)==batch,bars=len(closed),
                last_bar=closed[-1].bar_end.isoformat())
        archive[key]=json_value(asdict(next(session for session in composed if session.trade_date==day)))
    for path,sha in bindings.items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=sha:raise ValueError('ASSEMBLY_ORIGINAL_CHANGED')
    result=dict(schema_version='track2-six-indices-real/1',authority='SHADOW_ONLY',enabled=False,source_trade_date=day.isoformat(),
        mkt2=market,dk=dk,incremental_equality=incremental,current_day_15m_archive=archive,
        original_bindings=bindings,originals_unchanged=True,
        encoding='UNCHANGED_HTTPX_APPLICATION_BODY_BYTES; transport stream may be gzip encoded and have a separate byte count',
        plan_stock_scope=scope_proof or 'UNPROVEN_WAITING_TRACK1_CURRENT_SCOPE',historical_20_trade_days='UNCOVERED_SHORT_WINDOW',
        shadow_5_trade_days='NOT_COMPLETED',provider_http_this_assembly=0,model_http=0,production_writes=0)
    with args.output.open('x',encoding='utf-8') as stream:json.dump(result,stream,ensure_ascii=False,indent=2)
    print(json.dumps(dict(daily={k:v['status'] for k,v in market['indices'].items()},
        dk={k:v['status'] for k,v in dk['series'].items()},original_files=len(bindings),output=str(args.output))))


if __name__=='__main__':main()
