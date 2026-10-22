"""Display-only closed-five projection of one exact execution snapshot.

No market fetch, native-5m claim, strategy evaluation or business import. Complete
five-minute session buckets mirror strategies.aggregate_closed_bars semantics.
"""
from datetime import datetime, timedelta, timezone, time
import hashlib
import json
import math
import zlib

TZ=timezone(timedelta(hours=8))
MAX_RAW=512*1024

def aware(value):
    value=datetime.fromisoformat(value)
    if value.utcoffset() is None:raise ValueError('BAR_TIME_NAIVE')
    return value.astimezone(TZ)

def closed_five(raw,*,symbol,cutoff,decision_as_of,expected_input_sha256=None):
    bars=json.loads(raw);at=aware(cutoff);observed=aware(decision_as_of)
    if at>observed or not isinstance(bars,list) or len(bars)>500:raise ValueError('BAR_SNAPSHOT_SCOPE')
    rows={}
    for row in bars:
        end=aware(row['bar_end'])
        if (row.get('symbol')!=symbol or row.get('interval')!='1m' or row.get('closed') is False
                or row.get('evidence_kind')=='SYNTHETIC_QUOTE' or ':RISK_ONLY' in str(row.get('source_id')).upper()
                or end>observed):raise ValueError('BAR_SNAPSHOT_SCOPE')
        values={k:row[k] for k in ('open','high','low','close','volume','amount')}
        if any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) for v in values.values()):raise ValueError('BAR_VALUE')
        if any(values[k]<=0 for k in ('open','high','low','close')) or values['volume']<0 or values['amount']<0 or values['low']>min(values['open'],values['close']) or values['high']<max(values['open'],values['close']):raise ValueError('BAR_OHLC')
        if end in rows:raise ValueError('BAR_DUPLICATE')
        if end.date()==at.date() and end<=at:rows[end]=row
    # The execution receipt pins this filtered original row list, not a newly
    # computed factor or reconstructed synthetic quote.
    filtered=[row for end,row in sorted(rows.items())]
    digest=hashlib.sha256(json.dumps(filtered,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    if expected_input_sha256 is not None and digest!=expected_input_sha256:raise ValueError('EXECUTION_INPUT_SHA_MISMATCH')
    output=[]
    for origin,last in ((time(9,30),time(11,30)),(time(13),time(15))):
        end=datetime.combine(at.date(),origin,tzinfo=TZ)+timedelta(minutes=5)
        while end.time()<=last and end<=at:
            group=[rows.get(end-timedelta(minutes=i)) for i in range(4,-1,-1)]
            if all(row is not None for row in group):
                output.append(dict(at=end.isoformat(),open=group[0]['open'],high=max(r['high'] for r in group),low=min(r['low'] for r in group),close=group[-1]['close'],volume=sum(r['volume'] for r in group),amount=sum(r['amount'] for r in group),closed=True))
            end+=timedelta(minutes=5)
    return output,digest

def project_snapshot(root,connect,*,symbol,snapshot_id,cutoff,event_at,expected_input_sha256,connection=None):
    db=connection or connect(root,'storage/minute/minute_bars.sqlite3')
    try:
        row=db.execute("SELECT snapshot_id,symbol,interval,decision_as_of,payload_sha256,payload_zlib FROM minute_decision_snapshots WHERE snapshot_id=? AND symbol=? AND interval='1m' LIMIT 2",(snapshot_id,symbol)).fetchall()
    finally:
        if connection is None:db.close()
    if len(row)!=1:raise ValueError('EXACT_EXECUTION_SNAPSHOT_MISSING')
    row=row[0]
    if aware(row['decision_as_of']).replace(second=0,microsecond=0)!=aware(event_at):raise ValueError('SNAPSHOT_EVENT_TIME_MISMATCH')
    compressed=row['payload_zlib']
    if not isinstance(compressed,bytes) or len(compressed)>MAX_RAW:raise ValueError('SNAPSHOT_SIZE')
    inflater=zlib.decompressobj()
    try:raw=inflater.decompress(compressed,MAX_RAW+1)
    except zlib.error as error:raise ValueError('SNAPSHOT_COMPRESSION_INVALID') from error
    if len(raw)>MAX_RAW or not inflater.eof or inflater.unused_data or hashlib.sha256(raw).hexdigest()!=row['payload_sha256']:raise ValueError('SNAPSHOT_SHA_OR_SIZE')
    bars,input_sha=closed_five(raw.decode(),symbol=symbol,cutoff=cutoff,decision_as_of=row['decision_as_of'],expected_input_sha256=expected_input_sha256)
    for bar in bars:bar.update(sha256=row['payload_sha256'],source_id=snapshot_id,close_source='EXECUTION_AGGREGATED_5M_NOT_NATIVE')
    return dict(bars=bars,bar_source=dict(basis='EXECUTION_AGGREGATED_5M_NOT_NATIVE',snapshot_id=snapshot_id,snapshot_sha256=row['payload_sha256'],execution_input_sha256=input_sha,cutoff=cutoff,coverage='EXACT_CAPTURED_EXECUTION_PREFIX_NOT_FULL_DAY_OR_NATIVE_48'))
