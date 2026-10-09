"""Sealed stratified daily equivalence probe; default has zero source calls.

Only explicit --execute reads Settings and calls the existing throttled client.
All cache writes are fresh isolated outputs, never configured production caches.
Historical query cutoffs are NOT historical point-in-time knowledge receipts.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, time
import hashlib
import inspect
import json
import math
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer, _daily_value_hash, _row_time
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.settings import Settings

TZ = ZoneInfo('Asia/Shanghai')
SEED = datetime(2026,10,8,15,10,tzinfo=TZ)
TARGET = datetime(2026,10,9,15,10,tzinfo=TZ)
COUNTS = {'MAIN':80,'CHINEXT':50,'STAR':40,'BJ':20,'SPECIAL':10}
FIELDS = ('open_price','high_price','low_price','close_price','volume','turnover')
SYMBOL = re.compile(r'^[0-9]{6}\.(SH|SZ|BJ)$')


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def canonical(value: Any) -> bytes:
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def write_json(path: Path, value: Any) -> str:
    body = json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False).encode('utf-8')+b'\n'
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as stream:
        stream.write(body)
    return digest(body)


def validate_specials(rows: list[dict]) -> None:
    if len(rows)!=10 or len({r.get('symbol') for r in rows})!=10:
        raise ValueError('SPECIAL_COUNT_OR_DUPLICATE')
    for row in rows:
        if not SYMBOL.fullmatch(str(row.get('symbol',''))):
            raise ValueError('SPECIAL_SYMBOL_INVALID')
        parsed = urlparse(row.get('announcement_url',''))
        if parsed.scheme!='https' or parsed.hostname not in {'static.cninfo.com.cn','www.sse.com.cn','static.sse.com.cn'}:
            raise ValueError('SPECIAL_OFFICIAL_SOURCE_REQUIRED')
        event = date.fromisoformat(row.get('effective_date',''))
        if not '2026-09-01'<=event.isoformat()<='2026-09-30':
            raise ValueError('SPECIAL_SEPTEMBER_EVENT_REQUIRED')
        date.fromisoformat(row.get('publication_date',''))
        if row.get('event_type') not in {'IPO','SUSPENSION','SUSPENSION_RESUMPTION'} or not row.get('date_evidence'):
            raise ValueError('SPECIAL_DATE_EVIDENCE_REQUIRED')


def board(symbol: str) -> str | None:
    code,exchange = symbol.split('.')
    if exchange=='BJ': return 'BJ'
    if exchange=='SH' and code.startswith(('688','689')): return 'STAR'
    if exchange=='SZ' and code.startswith('30'): return 'CHINEXT'
    if (exchange=='SH' and code.startswith('60')) or (exchange=='SZ' and code.startswith('00')): return 'MAIN'
    return None


def build_manifest(snapshot: Path, reference: Path) -> dict:
    raw = snapshot.read_bytes()
    value = json.loads(raw)
    if not str(value.get('as_of','')).startswith('2026-10-08T'):
        raise ValueError('RAW_UNIVERSE_DATE_REQUIRED')
    reference_raw = reference.read_bytes()
    specials = json.loads(reference_raw)['records']
    validate_specials(specials)
    universe = {r['symbol']:r for r in value['universe_candidates']}
    excluded = {r['symbol'] for r in specials}
    if not excluded<=universe.keys(): raise ValueError('SPECIAL_NOT_IN_RAW_UNIVERSE')
    records = []
    for stratum,count in COUNTS.items():
        if stratum=='SPECIAL': continue
        candidates = [s for s in universe if s not in excluded and SYMBOL.fullmatch(s) and board(s)==stratum]
        candidates.sort(key=lambda s:(digest(('WP5-G3-3-200-v1:'+s).encode()),s))
        if len(candidates)<count: raise ValueError('STRATUM_INSUFFICIENT')
        records.extend({'symbol':s,'name':universe[s].get('name'),'stratum':stratum} for s in candidates[:count])
    records.extend({**r,'stratum':'SPECIAL'} for r in sorted(specials,key=lambda r:r['symbol']))
    return {'schema_version':'wp5-daily-incremental-200/1','selection_rule':'SHA256(WP5-G3-3-200-v1:symbol) ascending; ordinary excludes 10 official specials',
            'raw_universe_file':snapshot.name,'raw_universe_sha256':digest(raw),'raw_universe_as_of':value['as_of'],
            'raw_universe_count':len(universe),'special_reference_sha256':digest(reference_raw),
            'special_evidence_type':'OFFICIAL_SEPTEMBER_EVENT_REFERENCE_NOT_HISTORY_INFERENCE',
            'counts':COUNTS,'seed_as_of':SEED.isoformat(),'target_as_of':TARGET.isoformat(),'records':records}


def validate_manifest(body: bytes, expected_hash: str) -> dict:
    if not re.fullmatch('[0-9a-f]{64}',expected_hash) or digest(body)!=expected_hash:
        raise ValueError('MANIFEST_HASH_CONFLICT')
    value = json.loads(body)
    rows = value.get('records',[])
    if len({r.get('symbol') for r in rows})!=len(rows): raise ValueError('MANIFEST_DUPLICATE')
    if len(rows)!=200 or dict(Counter(r.get('stratum') for r in rows))!=COUNTS or value.get('counts')!=COUNTS:
        raise ValueError('MANIFEST_STRATA_INVALID')
    for row in rows:
        if not SYMBOL.fullmatch(str(row.get('symbol',''))): raise ValueError('MANIFEST_SYMBOL_INVALID')
        if row['stratum']!='SPECIAL' and board(row['symbol'])!=row['stratum']: raise ValueError('MANIFEST_BOARD_CONFLICT')
    validate_specials([r for r in rows if r['stratum']=='SPECIAL'])
    for key in ('raw_universe_sha256','special_reference_sha256'):
        if not re.fullmatch('[0-9a-f]{64}',str(value.get(key,''))): raise ValueError('MANIFEST_SOURCE_HASH_MISSING')
    if value.get('seed_as_of')!=SEED.isoformat() or value.get('target_as_of')!=TARGET.isoformat():
        raise ValueError('MANIFEST_QUERY_CUTOFF_CONFLICT')
    return value


def safe_bars(rows: list[dict]) -> list[dict]:
    result = []
    seen = set()
    for row in rows:
        stamp = _row_time(row).isoformat()
        if stamp in seen: raise ValueError('BAR_DUPLICATE')
        seen.add(stamp)
        if not all(isinstance(row.get(k),(int,float)) and not isinstance(row[k],bool) and math.isfinite(row[k]) for k in FIELDS):
            raise ValueError('BAR_FIELD_MISSING_OR_NONFINITE')
        result.append({'timestamp':stamp,**{k:float(row[k]) for k in FIELDS}})
    return sorted(result,key=lambda r:r['timestamp'])


def compare_bars(incremental: list[dict], full: list[dict]) -> dict:
    try:
        left,right = safe_bars(incremental),safe_bars(full)
    except (ValueError,TypeError,OverflowError):
        return {'status':'DATA_LIMITED','reason':'INVALID_BAR_EVIDENCE'}
    if not left or not right:
        return {'status':'DATA_LIMITED','reason':'EMPTY_NOT_EQUIVALENCE'}
    a,b = {r['timestamp']:r for r in left},{r['timestamp']:r for r in right}
    diffs = [{'timestamp':s,'fields':[k for k in FIELDS if a.get(s,{}).get(k)!=b.get(s,{}).get(k)]}
             for s in sorted(a.keys()|b.keys()) if s not in a or s not in b or _daily_value_hash(a[s])!=_daily_value_hash(b[s])]
    return {'status':'MATCH' if not diffs else 'CONFLICT','incremental_count':len(left),'full_count':len(right),
            'incremental_sha256':digest(canonical(left)),'full_sha256':digest(canonical(right)),'differences':diffs}


class RecordingClient:
    """Only typed allowlisted receipts, no provider metadata/error strings."""
    def __init__(self, client, *, before_request=None):
        self.client,self.receipts = client,[]
        self.before_request = before_request

    def history_1d(self,symbol,**kwargs):
        if self.before_request is not None:
            self.before_request()
        receipt = {'symbol':symbol,'start':kwargs.get('start'),'end':kwargs.get('end'),
            'adjust':kwargs.get('adjust'),'limit':kwargs.get('limit'),'max_pages':kwargs.get('max_pages'),
            'response_received':False}
        self.receipts.append(receipt)
        result = self.client.history_1d(symbol,**kwargs)
        meta = result.metadata
        receipt.update({
            'response_received':True,
            'ok':bool(result.ok),'complete':bool(result.complete),'row_count':len(result.items),
            'fetch_time':result.fetch_time.isoformat(),'http_status':result.http_status,
            'response_sha256':meta.get('response_sha256') if re.fullmatch('[0-9a-f]{64}',str(meta.get('response_sha256',''))) else None,
            'attempts':meta.get('attempts',1) if type(meta.get('attempts',1)) is int else None})
        return result


def run_probe(settings: Settings, records: list[dict], output: Path, *, client=None, observed_at=None, manifest_binding=None) -> dict:
    if output.exists(): raise ValueError('REFUSE_OVERWRITE')
    current = observed_at or datetime.now(TZ)
    current = current.astimezone(TZ)
    calendar = ExchangeTradingCalendar()
    def check_window():
        clock = observed_at or datetime.now(TZ)
        clock = clock.astimezone(TZ)
        if calendar.is_trading_day(clock.date()) and time(9)<=clock.time().replace(tzinfo=None)<=time(15,30):
            raise ValueError('REFUSE_TRADING_WINDOW')
    check_window()
    if settings.hithink_min_request_interval_seconds<0.5: raise ValueError('REFUSE_REDUCED_RATE_LIMIT')
    if client is None and settings.hithink_api_key is None: raise ValueError('HITHINK_API_KEY_MISSING')
    output.mkdir(parents=True)
    if manifest_binding is not None:
        write_json(output/'manifest-binding.json',manifest_binding)
    inc = HithinkIncrementalSynchronizer(LocalFactCache(output/'incremental.sqlite3'))
    full = HithinkIncrementalSynchronizer(LocalFactCache(output/'full.sqlite3'))
    owns = client is None
    wrapped = RecordingClient(client or HithinkClient(settings),before_request=check_window)
    summary = {'schema_version':'wp5-daily-incremental-200-probe/1','observed_at':current.isoformat(),
        'seed_as_of':SEED.isoformat(),'target_as_of':TARGET.isoformat(),'lookback_days':800,
        'adjust':'none','include_financial':False,'min_request_interval_seconds':settings.hithink_min_request_interval_seconds,
        'mode':'ISOLATED_READONLY_SOURCE_LOCAL_CACHE_WRITES_ONLY','not_historical_point_in_time_replay':True,
        'script_sha256':digest(Path(__file__).read_bytes()),
        'synchronizer_sha256':digest(inspect.getsource(HithinkIncrementalSynchronizer).encode()),
        'transport_sha256':digest(inspect.getsource(HithinkClient._get_with_retries).encode()),'records':[]}
    try:
        for reference in records:
            symbol = reference['symbol']
            symbol_start = len(wrapped.receipts)
            record = {'symbol':symbol,'stratum':reference['stratum'],'observed_at':datetime.now(TZ).isoformat()}
            results = []
            try:
                for name,sync,cutoff in (('seed_full',inc,SEED),('incremental',inc,TARGET),('independent_full',full,TARGET)):
                    start = len(wrapped.receipts)
                    result = sync.sync(wrapped,[symbol],as_of=cutoff,lookback_days=800,
                        compact_daily_bars=1000,include_financial=False,collect_early_discovery=False)
                    bars = result.daily.get(symbol,[])
                    record[name] = {'request':result.daily_requests.get(symbol),
                                    'failures':['SOURCE_OR_CORE_READINESS_FAILURE'] if result.failures.get(symbol) else [],
                                    'receipts':wrapped.receipts[start:],'bars':safe_bars(bars)}
                    results.append((result,bars))
                if any(r.failures.get(symbol) for r,b in results):
                    record['comparison'] = {'status':'DATA_LIMITED','reason':'SOURCE_OR_CORE_READINESS_FAILURE'}
                else:
                    record['comparison'] = compare_bars(results[1][1],results[2][1])
            except Exception:
                record['comparison'] = {'status':'DATA_LIMITED','reason':'PROBE_EXCEPTION_NO_SECRET_TEXT'}
            record['source_receipts'] = wrapped.receipts[symbol_start:]
            path = output/'records'/f'{symbol}.json'
            sha = write_json(path,record)
            summary['records'].append({'symbol':symbol,'stratum':reference['stratum'],
                'status':record['comparison']['status'],'artifact':str(path.relative_to(output)),
                'artifact_sha256':sha,'incremental_mode':(record.get('incremental',{}).get('request') or {}).get('mode')})
            print(json.dumps({'progress':len(summary['records']),'total':len(records),'symbol':symbol,'status':record['comparison']['status']}),flush=True)
    finally:
        if owns: wrapped.client.close()
    counts = Counter(r['status'] for r in summary['records'])
    summary.update(status='MATCH' if counts.get('MATCH')==len(records) and records else 'DATA_LIMITED',
                   matched_count=counts.get('MATCH',0),status_counts=dict(counts),source_calls=len(wrapped.receipts),
                   incremental_mode_counts=dict(Counter(r['incremental_mode'] for r in summary['records'])),
                   exit_code=0 if counts.get('MATCH')==len(records) and records else 2)
    write_json(output/'report.json',summary)
    return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-manifest',action='store_true')
    parser.add_argument('--raw-snapshot',type=Path)
    parser.add_argument('--special-reference',type=Path)
    parser.add_argument('--manifest',type=Path)
    parser.add_argument('--expected-sha256')
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--env-root',type=Path)
    parser.add_argument('--output',type=Path)
    args = parser.parse_args(argv)
    if args.build_manifest:
        if args.execute or not all((args.raw_snapshot,args.special_reference,args.output)):
            parser.error('build requires raw-snapshot, special-reference, output; no execute')
        sha = write_json(args.output,build_manifest(args.raw_snapshot,args.special_reference))
        print(json.dumps({'status':'SEALED','manifest_sha256':sha,'source_calls':0}))
        return 0
    if not args.manifest or not args.expected_sha256: parser.error('manifest and expected-sha256 required')
    value = validate_manifest(args.manifest.read_bytes(),args.expected_sha256)
    if not args.execute:
        print(json.dumps({'status':'DRY_RUN','counts':value['counts'],'source_calls':0,
                          'planned_history_calls_minimum':600,'fallback_and_transport_retries':'EXISTING_CORE_UNCHANGED',
                          'seed_as_of':SEED.isoformat(),'target_as_of':TARGET.isoformat()}))
        return 0
    if not args.output or not args.env_root: parser.error('execute requires output and explicit env-root')
    if args.output.exists(): raise ValueError('REFUSE_OVERWRITE')
    settings = Settings.from_env(root=args.env_root)
    report = run_probe(settings,value['records'],args.output,
        manifest_binding={'manifest_sha256':args.expected_sha256,'manifest':value})
    print(json.dumps({k:report[k] for k in ('status','matched_count','source_calls','exit_code')}))
    return report['exit_code']


if __name__=='__main__':
    raise SystemExit(main())
