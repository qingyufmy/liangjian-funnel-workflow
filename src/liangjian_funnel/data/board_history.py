"""Exact Eastmoney board-index history, cached separately from frozen rotation decisions."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from ..reporting import atomic_write_json
from ..runtime.calendar import ExchangeTradingCalendar


def _hash(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def fetch_board_history(code, trade_day):
    params = {'secid':'90.'+code,'klt':'101','fqt':'0','beg':(trade_day-timedelta(days=30)).strftime('%Y%m%d'),
              'end':trade_day.strftime('%Y%m%d'),'fields1':'f1,f2,f3,f4,f5,f6',
              'fields2':'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61'}
    response = httpx.get('https://push2his.eastmoney.com/api/qt/stock/kline/get',params=params,
                         timeout=8,trust_env=False)
    response.raise_for_status()
    data = response.json().get('data') or {}
    if data.get('code') != code:
        raise ValueError('BOARD_HISTORY_IDENTITY_MISMATCH')
    closes = {}
    for row in data.get('klines') or []:
        fields = row.split(',')
        stamp = datetime.strptime(fields[0],'%Y-%m-%d').date()
        close = float(fields[2])
        if stamp <= trade_day and math.isfinite(close) and close > 0:
            closes[stamp.isoformat()] = close
    return {'board_code':code,'board_name':data.get('name'),'closes':closes,
            'source':'EASTMONEY_BOARD_INDEX_DAILY_UNADJUSTED',
            'fetched_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()}


def enrich_board_momentum(rows, cache_dir: Path, trade_day, *, fetcher=fetch_board_history):
    """Six exact session closes support five returns; never mix membership proxies."""
    calendar = ExchangeTradingCalendar()
    days = [trade_day]
    for _ in range(5):
        days.append(calendar.previous_trading_day(days[-1]))
    dates = [d.isoformat() for d in reversed(days)]
    codes = sorted({str(c) for r in rows for c in r.get('component_board_codes',[]) if str(c).startswith('BK')})
    def get(code):
        path = cache_dir / f'{code}-{trade_day.isoformat()}.json'
        if path.is_file():
            try:
                cached = json.loads(path.read_text())
                digest = cached.pop('content_hash',None)
                if digest == _hash(cached) and cached.get('board_code') == code and all(d in cached.get('closes',{}) for d in dates):
                    return code,cached
            except (ValueError,OSError):
                pass
        try:
            value = fetcher(code,trade_day)
            if value.get('board_code') != code or not all(d in value.get('closes',{}) for d in dates):
                return code,{'reason_code':'BOARD_HISTORY_SESSIONS_MISSING'}
            if not all(math.isfinite(float(value['closes'][d])) and float(value['closes'][d]) > 0 for d in dates):
                return code,{'reason_code':'BOARD_HISTORY_PRICE_INVALID'}
            atomic_write_json(path,{**value,'content_hash':_hash(value)})
            return code,value
        except Exception:
            return code,{'reason_code':'BOARD_HISTORY_SOURCE_UNAVAILABLE'}
    with ThreadPoolExecutor(max_workers=4,thread_name_prefix='board-history') as pool:
        fetched = dict(pool.map(get,codes))
    for row in rows:
        components = sorted(set(row.get('component_board_codes') or []))
        valid = bool(components) and all(c in fetched and all(d in fetched[c].get('closes',{}) for d in dates) for c in components)
        evidence = {'basis':'EQUAL_WEIGHT_DAILY_REBALANCED_EXACT_BOARD_INDICES','component_codes':components,
                    'session_dates':dates,'required_closes':6,'status':'READY' if valid else 'DATA_LIMITED',
                    'missing_components':[c for c in components if not fetched.get(c,{}).get('closes')],
                    'source_records':[{k:fetched[c].get(k) for k in ('board_code','source','fetched_at')} | {'content_hash':_hash(fetched[c])} for c in components if c in fetched]}
        row['momentum_history_evidence'] = evidence
        if not valid:
            continue
        daily_returns = [sum(fetched[c]['closes'][dates[i]]/fetched[c]['closes'][dates[i-1]]-1 for c in components)/len(components) for i in range(1,6)]
        for n in (3,5):
            row[f'momentum_{n}d_pct'] = (math.prod(1+r for r in daily_returns[-n:])-1)*100
    return fetched


def enrich_reported_five_day_momentum(rows, snapshots, trade_day):
    """Reuse the vendor's explicitly dated 5d board returns, never its flow rank."""
    records = {}
    for snapshot in snapshots:
        if (snapshot.get('available') is not True or snapshot.get('period') != '5d'
                or snapshot.get('trade_date') != trade_day.isoformat()
                or snapshot.get('provider_trade_date_verified') is not True):
            continue
        for item in snapshot.get('records',[]):
            code, value = item.get('code'), item.get('change_pct')
            if code and isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value):
                records[code] = (float(value),snapshot.get('content_hash'),snapshot.get('ingested_at'))
    for row in rows:
        codes=sorted(set(row.get('component_board_codes') or []))
        if row.get('momentum_5d_pct') is not None or not codes or not all(c in records for c in codes):
            continue
        row['momentum_5d_pct']=sum(records[c][0] for c in codes)/len(codes)
        row['momentum_5d_evidence']={'basis':'MEAN_OF_EXACT_COMPONENT_PROVIDER_5D_RETURNS',
            'trade_date':trade_day.isoformat(),'period':'5d',
            'components':[{'code':c,'change_pct':records[c][0],'source_hash':records[c][1],
                           'ingested_at':records[c][2]} for c in codes],
            'not_daily_rank':True,'not_reconstructed_from_stock_members':True}
