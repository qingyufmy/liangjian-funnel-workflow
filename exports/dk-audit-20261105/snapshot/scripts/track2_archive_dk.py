"""Offline daily archive from the verified six-index aggregate and raw bodies.

Exclusive new output only; this neither schedules nor sends source requests.
"""
from dataclasses import fields
from datetime import date, datetime, time
from decimal import Decimal
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.market_state.bars15m import Bar15m,TZ,compose_15m
from liangjian_funnel.market_state.calendar import market_calendar
from liangjian_funnel.market_state.config import load_market_definitions
from liangjian_funnel.market_state.dk_archive import archive_evaluation,bar_identity,merge_bars
from liangjian_funnel.market_state.tencent_daily import decode_payload
from liangjian_funnel.market_state.tencent_indices import parse_index_5m


def decode_bar(row):
    value=dict(row)
    if set(value)!={f.name for f in fields(Bar15m)}:raise ValueError('DK_ARCHIVE_STORED_BAR_SHAPE')
    value['bar_end']=datetime.fromisoformat(value['bar_end'])
    for key in ('expected_5m','present_5m'):value[key]=tuple(datetime.fromisoformat(t) for t in value[key])
    value['reasons']=tuple(value['reasons'])
    for key in ('open','high','low','close','volume_shares','reported_amount_cny','estimated_amount_cny'):
        if value[key] is not None:value[key]=Decimal(value[key])
    return Bar15m(**value)


def build(evidence_path, previous_path=None):
    bindings={}
    def read(path):
        path=Path(path)
        if any(p.is_symlink() for p in (path,*path.parents)):raise ValueError('DK_ARCHIVE_LINK_REJECTED')
        raw=path.read_bytes();bindings[str(path.resolve())]=hashlib.sha256(raw).hexdigest();return raw
    evidence=json.loads(read(evidence_path));previous=None
    if (evidence.get('schema_version')!='track2-six-indices-real/1' or evidence.get('originals_unchanged') is not True
            or evidence.get('authority')!='SHADOW_ONLY'):
        raise ValueError('DK_ARCHIVE_EVIDENCE_NOT_VERIFIED')
    if previous_path:
        previous=json.loads(read(previous_path))
        if previous.get('schema_version')!='DK_DAILY_ORIGINAL_ARCHIVE/1':raise ValueError('DK_ARCHIVE_PREVIOUS_SCHEMA')
        for path,sha in previous['original_bindings'].items():
            if hashlib.sha256(read(path)).hexdigest()!=sha:raise ValueError('DK_ARCHIVE_PREVIOUS_ORIGINAL_CHANGED')
    for path,sha in evidence['original_bindings'].items():
        if hashlib.sha256(read(path)).hexdigest()!=sha:raise ValueError('DK_ARCHIVE_EVIDENCE_ORIGINAL_CHANGED')
    definitions=load_market_definitions(ROOT/'config');calendar=market_calendar();series={}
    now=datetime.fromisoformat(evidence['dk']['observed_at']);day=date.fromisoformat(evidence['source_trade_date'])
    if previous and day<=date.fromisoformat(previous['last_input_capture_day']):
        raise ValueError('DK_ARCHIVE_CAPTURE_DAY_NOT_ADVANCED')
    for key in definitions['registry']['indices']:
        dk=evidence['dk']['series'][key]
        if dk['status']!='SHADOW_RECORDED':raise ValueError('DK_ARCHIVE_SOURCE_BLOCKED')
        proof=dk['pit'];captured=datetime.fromisoformat(proof['capture_completed_at'])
        if captured.astimezone(TZ).date()!=day:raise ValueError('DK_ARCHIVE_CAPTURE_DAY_MISMATCH')
        candidates=[Path(p) for p,sha in evidence['original_bindings'].items()
            if Path(p).name==key+'-minute.body' and sha==dk['input_sha256']]
        if len(candidates)!=1:raise ValueError('DK_ARCHIVE_MINUTE_ORIGINAL_AMBIGUOUS')
        raw=read(candidates[0])
        if hashlib.sha256(raw).hexdigest()!=dk['input_sha256']:raise ValueError('DK_ARCHIVE_RAW_SHA_CONFLICT')
        payload,_=decode_payload(raw);rows=parse_index_5m(payload,registry=definitions['registry'],key=key)
        symbol=definitions['registry']['indices'][key]['thscode']
        bars=[]
        for session in sorted({r['bar_end'].date() for r in rows}):
            composed=compose_15m([r for r in rows if r['bar_end'].date()==session],symbol=symbol,trade_date=session,
                observed_at=captured,market_close_at=datetime.combine(session,time(15),tzinfo=TZ),calendar=calendar)
            bars.extend(b for b in composed.bars if b.status=='CLOSED')
        old=[] if not previous else [dict(item,bar=decode_bar(item['bar'])) for item in previous['series'][key]['items']]
        merged=merge_bars(old,bars,captured_at=captured,source_sha256=dk['input_sha256'],observed_at=now,calendar=calendar)
        series[key]=dict(items=[dict(item,bar=bar_identity(item['bar'])) for item in merged],
            evaluation=archive_evaluation(merged,observed_at=now,calendar=calendar),latest_capture_pit=proof,
            latest_input_sha256=dk['input_sha256'])
    for path,sha in bindings.items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=sha:raise ValueError('DK_ARCHIVE_ORIGINAL_CHANGED')
    return dict(schema_version='DK_DAILY_ORIGINAL_ARCHIVE/1',authority='SHADOW_ONLY',last_input_capture_day=day.isoformat(),
        series=series,original_bindings=bindings,originals_unchanged=True,
        archive_mode='OFFLINE_NO_TIMER_INSTALLED',plan_stock_scope=evidence['plan_stock_scope'],
        provider_http=0,model_http=0,production_writes=0)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True)
    p.add_argument('--previous',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if (not a.output.resolve().is_relative_to((ROOT/'artifacts/track2').resolve())
            or any(item.is_symlink() for item in (a.output,*a.output.parents))):raise ValueError('DK_ARCHIVE_OUTPUT_SCOPE')
    result=build(a.evidence,a.previous)
    with a.output.open('x',encoding='utf-8') as f:json.dump(result,f,ensure_ascii=False,indent=2)
    print(json.dumps(dict(series=len(result['series']),capture_day=result['last_input_capture_day'],provider_http=0)))
