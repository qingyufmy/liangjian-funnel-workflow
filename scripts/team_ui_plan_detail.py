"""Display projection of existing monitor facts. No business objects or new bars.

Uses caller's already bounded mode=ro/query_only SQLite connection. Missing
monitor table or facts remain explicitly missing; never computes a new signal.
"""
import hashlib
import json
import math
import re
import sqlite3
from datetime import datetime

NUMERIC_KEYS={'low','high','close','prior_close','price','volume','prior_low','midpoint','vwap','minimum','maximum','stop','target','original_stop','price_above','volume_ratio','max_attempts','ratio','breakout_volume','key','key_floor','v6','atr_period','volume_lookback','plan_upper','platform','previous_high','level','rise_high','pullback_mean','rise_mean','body','wick'}
def compact(value):
    if value is None or isinstance(value,bool):return value
    if isinstance(value,(int,float)):return value if math.isfinite(value) else None
    if isinstance(value,list):return [compact(v) for v in value[:120]]
    if isinstance(value,dict):return {k:compact(v) for k,v in value.items() if k in NUMERIC_KEYS}
    return None

def condition_source(value,event_at):
    """Keep legitimate source shapes distinct; never replace missing time with now."""
    if value=='FROZEN_PLAN':return dict(at=None,source_kind='FROZEN_PLAN',source_times=[],source_valid=True)
    if value is None:return dict(at=None,source_kind='UNRECORDED',source_times=[],source_valid=None)
    values=value if isinstance(value,list) else [value];times=[];valid=len(values)<=120
    try:
        limit=datetime.fromisoformat(event_at)
        if limit.utcoffset() is None:raise ValueError('EVENT_TIME_NAIVE')
        for item in values[:120]:
            if not isinstance(item,str) or not re.fullmatch(r'[0-9T:+.Z-]{10,40}',item):valid=False;continue
            at=datetime.fromisoformat(item);times.append(item)
            if at.utcoffset() is None or at>limit:valid=False
    except (ValueError,TypeError):valid=False
    return dict(at=times[0] if not isinstance(value,list) and times else None,source_kind='TIMESTAMP_LIST' if isinstance(value,list) else 'TIMESTAMP',source_times=times,source_valid=valid)

def project_detail(db,day,plan_id,*,root=None,connect=None):
    result={'bars':[],'conditions':[],'timeline':[],'detail_coverage':'MISSING_EXISTING_MONITOR_FACTS','rr':None,'condition_as_of':None,'price':None,'in_zone':None}
    try:
        # Bound row IDs BEFORE JSON/date evaluation. No historical full scan or
        # assumption of a non-existent JSON/plan index on the trading database.
        records=db.execute("SELECT event_id,minute_end,payload_json FROM monitor_events WHERE rowid IN (SELECT rowid FROM monitor_events ORDER BY rowid DESC LIMIT 500) AND minute_end>=? AND minute_end<? AND CASE WHEN length(payload_json)<=262144 THEN json_extract(payload_json,'$.plan_id') END=? ORDER BY minute_end DESC LIMIT 500",(day+'T09:30:00+08:00',day+'T15:01:00+08:00',plan_id))
        bars={};latest=False;snapshots=[]
        for row in records:
            body=json.loads(row['payload_json']);strategy=body.get('strategy') or {};end=strategy.get('closed_5m_end');entry=strategy.get('entry_confirmation') or {}
            execution=strategy.get('execution_data') or {}
            if len(snapshots)<8 and body.get('minute_snapshot_id') and execution.get('market_cutoff') and execution.get('input_sha256') and not any(s['snapshot_id']==body['minute_snapshot_id'] for s in snapshots):
                snapshots.append(dict(symbol=body.get('symbol'),snapshot_id=body['minute_snapshot_id'],cutoff=execution['market_cutoff'],event_at=row['minute_end'],expected_input_sha256=execution['input_sha256']))
            digest=hashlib.sha256(row['payload_json'].encode()).hexdigest()
            event_id=row['event_id'] if isinstance(row['event_id'],str) and re.fullmatch(r'[A-Za-z0-9_.:-]{1,240}',row['event_id']) else None
            codes=[c for c in strategy.get('reason_codes',[]) if isinstance(c,str) and re.fullmatch(r'[A-Z0-9_:.-]{1,120}',c)][:24]
            if len(result['timeline'])<48:result['timeline'].append(dict(at=row['minute_end'],reason_codes=codes,source_id=event_id,sha256=digest))
            if not isinstance(end,str) or not end.startswith(day+'T'):continue
            try:
                if datetime.fromisoformat(end)>datetime.fromisoformat(row['minute_end']):continue
            except (ValueError,TypeError):continue
            if not latest and entry.get('conditions'):
                latest=True;result['condition_as_of']=strategy.get('as_of') or row['minute_end']
                close=entry.get('current_entry_close')
                result['price']=close if type(close) in (int,float) and math.isfinite(close) and close>0 else None
                result['reason_codes']=codes
                for key in (entry.get('required_conditions') or [])[:24]:
                    if not isinstance(key,str) or not re.fullmatch(r'[A-Z][A-Z0-9_]{0,95}',key):continue
                    c=entry['conditions'].get(key) or {};status=c.get('status')
                    result['conditions'].append({'code':key,'status':status if status in ('MET','UNMET','UNPROVEN') else 'UNPROVEN','threshold':compact(c.get('threshold')),'value':compact(c.get('value')),**condition_source(c.get('source_as_of'),row['minute_end']),'source_id':event_id,'sha256':digest})
                rr=next((c['value'] for c in result['conditions'] if c['code']=='ENTRY_RR' and c['source_valid'] is not False),None)
                result['rr']=compact(rr) if isinstance(rr,(int,float)) and not isinstance(rr,bool) else None
            if end in bars:continue
            if end[14:16] not in ('00','05','10','15','20','25','30','35','40','45','50','55'):continue
            price=entry.get('current_entry_close');close_source='ENTRY_CONFIRMATION'
            receipt=((strategy.get('execution_data') or {}).get('receipts') or {}).get('1m') or {}
            if price is None and receipt.get('complete') is True and (receipt.get('last_bar') or {}).get('bar_end')==end:
                price=receipt['last_bar'].get('close');close_source='COMPLETE_1M_CLOSE_AT_CLOSED5_END'
            if isinstance(price,bool) or not isinstance(price,(int,float)) or not math.isfinite(price) or price<=0:continue
            volume=(strategy.get('indicator_observations') or {}).get('volume_5m') or {}
            bars[end]={'at':end,'close':price,'closed':True,'volume':compact(volume.get('latest_volume')) if volume.get('closed_bar_end')==end else None,'source_id':event_id,'sha256':digest,'close_source':close_source}
        result['bars']=[bars[key] for key in sorted(bars)]
        result['timeline'].reverse()
        if result['bars'] or result['conditions']:result['detail_coverage']='LATEST_500_EVENTS_CLOSED5M_MONITOR_FACTS_NOT_NATIVE_BARS_OR_FULL_DAY'
        if root is not None and connect is not None and snapshots:
            try:
                from team_ui_bars import project_snapshot
                minutes=connect(root,'storage/minute/minute_bars.sqlite3')
                try:
                    for snapshot in snapshots:
                        try:
                            result.update(project_snapshot(root,connect,connection=minutes,**snapshot));break
                        except (ValueError,KeyError,TypeError):continue
                    else:result['bar_missing_reason']='EXACT_EXECUTION_SNAPSHOT_UNAVAILABLE_OR_UNBOUND'
                finally:minutes.close()
            except (OSError,ValueError,KeyError,TypeError,sqlite3.Error):result['bar_missing_reason']='EXACT_EXECUTION_SNAPSHOT_UNAVAILABLE_OR_UNBOUND'
    except sqlite3.OperationalError as error:
        if 'no such table' not in str(error):raise
    return result
