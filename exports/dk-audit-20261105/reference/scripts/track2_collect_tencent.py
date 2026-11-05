"""M28 bounded Tencent raw archive; no workflow/production integration.

Requires same-byte zero-network preflight plus fresh T1 whole-chain terminal
readback. Calls each explicit index URL once, serially, without retries.
"""
from datetime import date, datetime, time
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import traceback
import uuid

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.market_state.bars15m import TZ
from liangjian_funnel.market_state.config import load_market_definitions
from liangjian_funnel.market_state.calendar import market_calendar
from liangjian_funnel.market_state.dk_shadow import build_dk_shadow,canonical_minute_input
from liangjian_funnel.market_state.index_trend import index_close_facts
from liangjian_funnel.market_state.tencent_daily import daily_request, decode_payload, parse_daily
from liangjian_funnel.market_state.tencent_indices import index_request
from liangjian_funnel.market_state.tencent_http import TencentTransport,live_clearance
from liangjian_funnel.market_state.tencent_archive_http import DailyArchiveLedger

FILES = ['src/liangjian_funnel/market_state/tencent_http.py','src/liangjian_funnel/market_state/tencent_daily.py',
    'src/liangjian_funnel/market_state/tencent_indices.py','src/liangjian_funnel/pipeline/model_client.py',
    'src/liangjian_funnel/market_state/research_http.py','scripts/track2_collect_tencent.py',
    'scripts/track2_check_board_gate.mjs','tests/test_track2_tencent_preflight.py',
    'src/liangjian_funnel/market_state/calendar.py','src/liangjian_funnel/market_state/index_trend.py',
    'config/market_state.yaml','config/market_indices.yaml',
    'src/liangjian_funnel/market_state/tencent_archive_http.py',
    'src/liangjian_funnel/market_state/dk_shadow.py','src/liangjian_funnel/market_state/bars15m.py',
    'src/liangjian_funnel/market_state/contract.py','src/liangjian_funnel/market_state/config.py']


def save(path,value):
    with Path(path).open('x',encoding='utf-8') as stream:
        json.dump(value,stream,ensure_ascii=False,sort_keys=True,indent=2)
        stream.flush();os.fsync(stream.fileno())


def plan_stock_scope(path,day,definitions):
    if path is None:return [],dict(status='SOURCE_NOT_SUPPLIED')
    path=Path(path)
    if any(p.is_symlink() for p in (path,*path.parents)):raise ValueError('M158_PLAN_SCOPE_LINK_INVALID')
    raw=path.read_bytes();scope=json.loads(raw);symbols=scope.get('symbols')
    indices={r['thscode'] for r in definitions['registry']['indices'].values()}
    if (scope.get('evidence_class')!='FROZEN_PLAN_SYMBOL_SCOPE' or scope.get('trade_date')!=day.isoformat()
            or not isinstance(symbols,list) or len(symbols)>18
            or any(not isinstance(s,str) or re.fullmatch(r'\d{6}\.(SH|SZ)',s) is None or s in indices for s in symbols)
            or len(set(symbols))!=len(symbols)):
        raise ValueError('M158_PLAN_SCOPE_DATE_IDENTITY_OR_CAP_INVALID')
    return symbols,dict(status='EXPLICIT_FROZEN_SCOPE',path=str(path.resolve()),file_sha256=hashlib.sha256(raw).hexdigest(),symbols=symbols)


def collection_plan(definitions,*,indices,kind,stocks,only_plan_stocks=False):
    if only_plan_stocks and (indices is not None or kind=='DAILY'):
        raise ValueError('M158_STOCK_ONLY_INDEX_OR_KIND_CONFLICT')
    keys=[] if only_plan_stocks else indices or definitions['rules']['indices']
    if len(set(keys))!=len(keys):raise ValueError('T2_DUPLICATE_INDEX_REQUEST')
    plan=[]
    for key in keys:
        if kind in {'DAILY','BOTH'}:plan.append(dict(key=key,kind='DAILY',source='MKT_TENCENT',**daily_request(definitions['registry'],key)))
        if kind in {'MINUTE','BOTH'}:plan.append(dict(key=key,kind='MINUTE',source='DK_TENCENT',**index_request(definitions['registry'],key)))
    if kind in {'MINUTE','BOTH'}:
        for symbol in stocks:
            provider=symbol[-2:].lower()+symbol[:6]
            plan.append(dict(key=symbol,kind='MINUTE',source='DK_TENCENT',
                url='https://ifzq.gtimg.cn/appstock/app/kline/mkline',params={'param':provider+',m5,,320'}))
    if len(plan)>30:raise ValueError('M158_SINGLE_DAY_SCOPE_EXCEEDS_30')
    return plan


def verify_preflight(path):
    value=json.loads(Path(path).read_bytes())
    checks={'SEND_MODES','MODEL_SUCCESS','MODEL_FAILURE','SCOPE_NEGATIVES','NO_RETRY_HTTP',
        'RESTART_PARENT_SPACING','LIVE_CLEARANCE','OPEN_STREAM_BLOCK','TRANSPORT_FAILURE','MONOTONIC_SPACING',
        'M158_SOURCE','M158_MODEL_SUCCESS','M158_MODEL_FAILURE','M158_DAILY_BUDGET','M158_PLAN_SCOPE','M158_STOCK_ONLY'}
    if (value.get('schema_version')!='track2-tencent-preflight/1' or value.get('status')!='PASSED'
            or value.get('real_source_http')!=0 or value.get('real_model_http')!=0
            or value.get('real_socket_forbidden') is not True or set(value.get('checks',[]))!=checks
            or value.get('source_hashes')!={p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in FILES}):
        raise ValueError('T2_CANONICAL_PREFLIGHT_STALE_OR_FAILED')
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--preflight',type=Path,required=True)
    parser.add_argument('--clearance',type=Path,required=True)
    parser.add_argument('--trade-date',type=date.fromisoformat,required=True)
    parser.add_argument('--index',choices=['SSE','SZSE','CYB','HS300','CSI1000','KC50'],action='append')
    parser.add_argument('--kind',choices=['DAILY','MINUTE','BOTH'],default='BOTH')
    parser.add_argument('--plan-scope',type=Path)
    parser.add_argument('--only-plan-stocks',action='store_true',help='Append only the genuine frozen plan scope; never refetch indices')
    args=parser.parse_args()
    calendar=market_calendar();now=datetime.now(TZ)
    if not calendar.is_trading_day(args.trade_date) or args.trade_date!=now.date() or now.time()<time(15,5):
        raise ValueError('T2_CANONICAL_CLOSE_COLLECTION_WINDOW_INVALID')
    preflight_sha=verify_preflight(args.preflight)
    live_clearance(args.clearance)
    definitions=load_market_definitions(ROOT/'config')
    stocks,scope_proof=plan_stock_scope(args.plan_scope,args.trade_date,definitions)
    if args.only_plan_stocks and scope_proof['status']!='EXPLICIT_FROZEN_SCOPE':
        raise ValueError('M158_STOCK_ONLY_FROZEN_SCOPE_REQUIRED')
    plan=collection_plan(definitions,indices=args.index,kind=args.kind,stocks=stocks,only_plan_stocks=args.only_plan_stocks)
    base=ROOT/'artifacts/track2/tencent-canonical';base.mkdir(parents=True,exist_ok=True)
    ledger=DailyArchiveLedger(base/'archive-ledger-m158.jsonl',parent_path=base/'cumulative-ledger.jsonl')
    directory=base/args.trade_date.isoformat()/(now.strftime('%H%M%S')+'-'+uuid.uuid4().hex[:8])
    directory.mkdir(parents=True,exist_ok=False)
    lock=base/'collection.lock'
    fd=os.open(lock,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    before=ledger.summary()
    report=dict(schema_version='track2-tencent-collection/1',authority='SHADOW_ONLY',enabled=False,status='STARTED',
        trade_date=args.trade_date.isoformat(),started_at=now.isoformat(),preflight_sha256=preflight_sha,
        source_hashes={p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in FILES},
        head=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip(),
        plan=plan,counts_before=before,attempts=[],daily={},minutes={},
        source_policy='TENCENT_ONLY_NO_EM',historical_identity='FIRST_CAPTURE_TODAY_NOT_HISTORICAL_PIT_ORIGINAL',
        twenty_trade_day_evaluation='UNCOVERED_SHORT_WINDOW',plan_stock_scope=scope_proof,
        collection_scope='ONLY_FROZEN_PLAN_STOCKS' if args.only_plan_stocks else 'EXPLICIT_INDICES_AND_PLAN_STOCKS',
        budget_policy='CLAUDE_M158_MKT30_DAILY_CLOSE_ONLY_NO_TIMER',
        model_http=0,production_writes=0,vm_access=0)
    def clearance():
        lease=live_clearance(args.clearance)
        checked=subprocess.run(['node',str(ROOT/'scripts/track2_check_board_gate.mjs'),lease['message_id']],
            capture_output=True,timeout=15,check=False)
        if checked.returncode!=0:raise ValueError('T2_CHAIN_STATE_CHANGED_RECHECK_REQUIRED')
    try:
        transport=TencentTransport(httpx.HTTPTransport(retries=0,trust_env=False),ledger,plan=plan,
            clearance=clearance,trade_date=args.trade_date.isoformat())
        with httpx.Client(transport=transport,timeout=httpx.Timeout(8,connect=3),follow_redirects=False,trust_env=False) as client:
            for item in plan:
                key,kind=item['key'],item['kind']
                stem=key+'-'+kind.lower()
                started=datetime.now(TZ).isoformat()
                save(directory/(stem+'-started.json'),dict(started_at=started,request=item))
                with client.stream('GET',item['url'],params=item['params']) as reply:
                    raw=reply.read()
                    with (directory/(stem+'.body')).open('xb') as stream:
                        stream.write(raw);stream.flush();os.fsync(stream.fileno())
                    completed=datetime.now(TZ).isoformat()
                    sha=hashlib.sha256(raw).hexdigest()
                    attempt=dict(key=key,kind=kind,status_code=reply.status_code,bytes=len(raw),input_sha256=sha,
                        capture_started_at=started,capture_completed_at=completed)
                    report['attempts'].append(attempt)
                    save(directory/(stem+'-attempt.json'),attempt)
                    reply.raise_for_status()
                payload,encoding=decode_payload(raw)
                if kind=='DAILY':
                    dataset=parse_daily(payload,registry=definitions['registry'],key=key,trade_date=args.trade_date,
                        input_sha256=sha,capture_started_at=started,capture_completed_at=completed)
                    facts=index_close_facts(key,dataset,definitions=definitions,calendar=calendar)
                    if facts['daily'] is None:raise ValueError('T2_DAILY_SOURCE_NOT_READY')
                    save(directory/(key+'-daily-normalized.json'),dataset)
                    save(directory/(key+'-daily-facts.json'),facts)
                    report['daily'][key]=dict(status=facts['status'],input_sha256=sha,encoding=encoding)
                else:
                    is_index=key in definitions['registry']['indices']
                    dataset,rows=canonical_minute_input(payload,key=key,definitions=definitions,trade_date=args.trade_date,
                        input_sha256=sha,capture_started_at=started,capture_completed_at=completed)
                    facts=build_dk_shadow({key:dataset},plan_symbols=[] if is_index else [key],definitions=definitions,
                        trade_date=args.trade_date,observed_at=datetime.now(TZ),calendar=calendar)
                    if facts['series'][key]['status']!='SHADOW_RECORDED':raise ValueError('T2_MINUTE_SOURCE_NOT_READY')
                    save(directory/(key+'-dk-shadow.json'),facts)
                    report['minutes'][key]=dict(status=facts['series'][key]['status'],input_sha256=sha,encoding=encoding)
        report['status']='COLLECTED_REQUESTED_SCOPE_NOT_ACCEPTANCE'
    except Exception as exc:
        paused=type(exc) is ValueError and str(exc) in {'T2_CHAIN_STATE_CHANGED_RECHECK_REQUIRED','T2_CHAIN_RUNNING_OR_CLEARANCE_STALE'}
        ledger.event(dict(kind='PAUSED_TRACK1_CLEARANCE' if paused else 'SOURCE_BUSINESS_FAILED',exception_type=type(exc).__name__))
        report.update(status='PAUSED_TRACK1_CLEARANCE' if paused else 'FAILED_STOPPED_NO_RETRY',failure=dict(exception_type=type(exc).__name__,
            reason=str(exc) if type(exc) is ValueError else 'HTTP_OR_TRANSPORT_FAILURE',
            frames=[dict(file=Path(f.filename).name,function=f.name,line=f.lineno) for f in traceback.extract_tb(exc.__traceback__)]))
    finally:
        report.update(counts_after=ledger.summary(),finished_at=datetime.now(TZ).isoformat())
        report['preflight_bytes_still_match']=report['source_hashes']=={p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in FILES}
        save(directory/'report.json',report)
        os.close(fd);lock.unlink()
    print(json.dumps(dict(status=report['status'],report=str(directory/'report.json'),counts_after=report['counts_after'])))
    return 0 if report['status'].startswith('COLLECTED_') else 1


if __name__=='__main__':raise SystemExit(main())
