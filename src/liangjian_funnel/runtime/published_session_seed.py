"""Exact immutable captured rowset verification; no metadata guessing or writes."""
from contextlib import closing
from datetime import datetime,timedelta
from pathlib import Path
import hashlib,json,re,sqlite3,time
from ..data.cache import _payload,_market_payload
from ..data.mootdx import MinuteBar
from .history_freshness import read_previous_session_seed,validate_session_seed


def rowset_hash(bars):
    return hashlib.sha256(json.dumps([bar.model_dump(mode='json') for bar in sorted(bars,key=lambda b:b.bar_end)],
        sort_keys=True,separators=(',',':')).encode()).hexdigest()


def read_published_session_seed(path,symbol,*,target_trade_date,calendar,as_of,expected_rowset_sha256,clock=lambda:datetime.now().astimezone()):
    bars,proof=read_previous_session_seed(path,symbol,target_trade_date=target_trade_date,calendar=calendar,as_of=as_of)
    if proof['status']!='READY':return bars,proof
    def failed(reason):
        return (),dict(proof,status='DATA_BLOCKED',reason_code=reason,
            original_rowset_sha256=expected_rowset_sha256,canonical_rowset_sha256=rowset_hash(bars))
    if not re.fullmatch('[0-9a-f]{64}',str(expected_rowset_sha256)):
        return failed('HISTORICAL_5M_ROWSET_PIN_INVALID')
    if rowset_hash(bars)==expected_rowset_sha256:
        return bars,dict(proof,rowset_binding='CANONICAL_CACHE_EXACT_HASH')
    # The complete payload already exists in the immutable version ledger.
    # Match canonical market fields AND provider, then require the original
    # full 48-row hash. Never infer units from a provider name or edit the DB.
    now=clock()
    if not isinstance(now,datetime) or now.utcoffset() is None:return failed('HISTORICAL_5M_VERSION_CLOCK_INVALID')
    canonical={bar.bar_end:bar for bar in bars};options={stamp:{} for stamp in canonical};pins={}
    start=min(canonical).replace(hour=0,minute=0);end=start+timedelta(days=1)
    try:
        with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=.15)) as db:
            db.execute('pragma query_only=ON');db.execute('pragma busy_timeout=150')
            deadline=time.monotonic()+.15
            db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            rows=db.execute('SELECT version_id,observed_at,CASE WHEN length(payload_json)<=16384 THEN payload_json END FROM minute_bar_versions WHERE symbol=? AND interval=? AND bar_end>=? AND bar_end<? ORDER BY bar_end,observed_at,version_id LIMIT 769',
                (bars[0].symbol,'5m',start.isoformat(),end.isoformat())).fetchall()
        if len(rows)>768:return failed('HISTORICAL_5M_VERSION_READ_BOUND')
        for version,observed,raw in rows:
            if not isinstance(raw,str) or not re.fullmatch('[0-9a-f]{64}',str(version)):
                return failed('HISTORICAL_5M_VERSION_PAYLOAD_BOUND')
            if hashlib.sha256(raw.encode()).hexdigest()!=version:return failed('HISTORICAL_5M_VERSION_HASH_CONFLICT')
            captured=datetime.fromisoformat(observed)
            if captured.utcoffset() is None or captured>now:return failed('HISTORICAL_5M_VERSION_FUTURE')
            bar=MinuteBar.model_validate_json(raw);original=canonical.get(bar.bar_end)
            if original is None:continue
            if bar.source_id!=original.source_id or _market_payload(_payload(bar))!=_market_payload(_payload(original)):continue
            if bar.model_dump(mode='json')==original.model_dump(mode='json'):continue
            key=rowset_hash([bar]);options[bar.bar_end][key]=bar;pins[key]=version
        selected=[];versions=[]
        for stamp in sorted(canonical):
            candidates=options[stamp]
            if len(candidates)>1:return failed('HISTORICAL_5M_VERSION_AMBIGUOUS')
            if candidates:
                key,bar=next(iter(candidates.items()));selected.append(bar);versions.append(pins[key])
            else:selected.append(canonical[stamp])
        if rowset_hash(selected)!=expected_rowset_sha256:return failed('HISTORICAL_5M_ORIGINAL_ROWSET_UNPROVEN')
        selected,checked=validate_session_seed(symbol,selected,target_trade_date=target_trade_date,calendar=calendar,as_of=as_of)
        if checked['status']!='READY':return (),checked
        return selected,dict(checked,rowset_binding='IMMUTABLE_CAPTURED_VERSION_EXACT_HASH',
            original_rowset_sha256=expected_rowset_sha256,canonical_rowset_sha256=rowset_hash(bars),
            restored_original_count=len(versions),version_ids_sha256=hashlib.sha256(json.dumps(versions).encode()).hexdigest(),
            verification_observed_at=now.isoformat(),source_requests=0,production_writes=0)
    except (sqlite3.Error,ValueError,TypeError,OSError):
        return failed('HISTORICAL_5M_VERSION_READ_UNPROVEN')
