"""Independent loopback DTO acquisition. No production discovery or job calls."""
from __future__ import annotations
from datetime import datetime,time
import hashlib,http.client,json,math,os
from pathlib import Path
from time import monotonic as _monotonic
from urllib.parse import urlsplit
from uuid import uuid4
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
CAP=1024*1024
PATH='/api/shadow/job-runs-readonly'
VERSION='shadow-node-sample/1'

def _stamp(v):
    v=v if isinstance(v,datetime) else datetime.fromisoformat(v)
    if v.tzinfo is None or v.utcoffset() is None:raise ValueError('AWARE_NODE_CLOCK_REQUIRED')
    return v.astimezone(TZ)

def _raw(v):return json.dumps(v,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode('utf8')
def _sha(v):return hashlib.sha256(v).hexdigest()
def _pairs(pairs):
    out={}
    for k,v in pairs:
        if k in out:raise ValueError('DUPLICATE_NODE_JSON_KEY')
        out[k]=v
    return out
def _decode(v):return json.loads(v,object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('NONFINITE_NODE_JSON')))

def inspect_node_sample(raw,*,trade_date,requested_at,received_at,previous_node=None):
    result=dict(schema_version=VERSION,status='DATA_LIMITED',reason_codes=[],source_authenticated=False)
    try:
        begin,end=_stamp(requested_at),_stamp(received_at)
        if (begin.date().isoformat()!=trade_date or end.date()!=begin.date() or end<begin
                or (end-begin).total_seconds()>2 or not time(16)<=begin.time()<=end.time()<=time(16,45)):
            raise ValueError('NODE_SAMPLE_CLOCK_OR_WINDOW_INVALID')
        if not isinstance(raw,bytes) or len(raw)>CAP:raise ValueError('NODE_RESPONSE_SIZE_OR_BYTES_INVALID')
        obj=_decode(raw);node=obj.get('node') if isinstance(obj,dict) else None
        if not isinstance(node,dict) or not isinstance(obj.get('recentJobRuns'),list):raise ValueError('NODE_ORIGINAL_DTO_REQUIRED')
        pid,start=node.get('pid'),node.get('startedAtEpochMs')
        if (type(pid) is not int or not 0<pid<=2**53-1 or type(start) not in (int,float)
            or not math.isfinite(start) or not 0<start<=_stamp(node.get('observedAt')).timestamp()*1000):
            raise ValueError('NODE_IDENTITY_INVALID')
        observed=_stamp(node['observedAt'])
        if not begin<=observed<=end:raise ValueError('NODE_SERVER_CLOCK_CONFLICT')
        identity=dict(pid=pid,startedAtEpochMs=start)
        result.update(node={**identity,'observedAt':observed.isoformat()},raw_sha256=_sha(raw))
        if start>datetime.combine(begin.date(),time(16),TZ).timestamp()*1000 or (
                previous_node is not None and identity!=previous_node):
            raise ValueError('NODE_RESTART_OR_HISTORY_UNPROVEN')
        result.update(status='READY',reason_codes=[])
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError) as exc:
        code=str(exc)
        result['reason_codes']=[code if code and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ_' for c in code) else 'NODE_SAMPLE_INVALID']
    return result

def _endpoint(value):
    parsed=urlsplit(value)
    if (parsed.scheme!='http' or parsed.hostname not in ('127.0.0.1','::1') or parsed.port is None
        or not 0<parsed.port<65536 or parsed.username is not None or parsed.password is not None
        or parsed.path!=PATH or parsed.query or parsed.fragment):raise ValueError('EXPLICIT_LOOPBACK_NODE_ENDPOINT_REQUIRED')
    return parsed

def _http_transport(endpoint,*,headers,timeout,monotonic):
    parsed=_endpoint(endpoint);deadline=monotonic()+timeout
    # HTTPConnection neither follows redirects nor consults environment proxies.
    connection=http.client.HTTPConnection(parsed.hostname,parsed.port,timeout=timeout)
    try:
        connection.request('GET',PATH,headers=headers)
        socket=connection.sock
        left=deadline-monotonic()
        if left<=0:raise TimeoutError
        if socket is not None:socket.settimeout(left)
        response=connection.getresponse();chunks=[];size=0
        while True:
            left=deadline-monotonic()
            if left<=0:raise TimeoutError
            if socket is not None:socket.settimeout(left)
            chunk=response.read1(min(65536,CAP+1-size))
            if not chunk:break
            chunks.append(chunk);size+=len(chunk)
            if size>CAP:raise ValueError('NODE_RESPONSE_SIZE_OR_BYTES_INVALID')
        return dict(status=response.status,raw=b''.join(chunks),
            content_type=response.getheader('Content-Type',''),encoding=response.getheader('Content-Encoding','identity'))
    finally:connection.close()

def _write(path,body):
    path.parent.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() for p in (path,*path.parents)):raise ValueError('NODE_ARCHIVE_SYMLINK_REFUSED')
    with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as f:f.write(body)

def verify_ready_sample(raw,sample,*,observed_at):
    """Recheck raw bytes/receipt, never trust caller READY as a certificate."""
    _endpoint(sample['endpoint']);now=_stamp(observed_at);end=_stamp(sample['received_at'])
    if (sample.get('schema_version')!=VERSION or sample.get('status')!='READY'
        or sample.get('restart_unproven') is not False or sample.get('raw_sha256')!=_sha(raw)
        or sample.get('receipt_sha256')!=_sha(_raw({k:v for k,v in sample.items() if k!='receipt_sha256'}))
        or now<end or (now-end).total_seconds()>62):raise ValueError('NODE_SAMPLE_PIN_OR_FRESHNESS_UNPROVEN')
    result=inspect_node_sample(raw,trade_date=sample['trade_date'],requested_at=sample['requested_at'],
        received_at=sample['received_at'],previous_node=sample['identity_baseline'])
    if result['status']!='READY' or result['node']!=sample.get('node'):raise ValueError('NODE_SAMPLE_IDENTITY_UNPROVEN')
    return result['node']

class LoopbackNodeSampler:
    """One request/60s, restored raw pins and node identity; no retries/catch-up."""
    def __init__(self,endpoint,archive_root,*,token_env_name='LIANGJIAN_DASHBOARD_TOKEN',
                 clock=None,monotonic=None,transport=None):
        _endpoint(endpoint)
        if not token_env_name or not all(c.isalnum() or c=='_' for c in token_env_name):raise ValueError('TOKEN_ENV_NAME_INVALID')
        self.endpoint=endpoint;self.root=Path(archive_root).resolve();self.token_env_name=token_env_name
        if any(p.is_symlink() for p in (Path(archive_root),*Path(archive_root).parents)):raise ValueError('NODE_ARCHIVE_SYMLINK_REFUSED')
        self.clock=clock or (lambda:datetime.now(TZ));self.monotonic=monotonic or _monotonic
        self.transport=transport or _http_transport;self.days={};self.last_clock=None
        files=sorted([*self.root.glob('*/*.request.json'),*self.root.glob('*/*.sample.json')])
        if len(files)>10000:raise ValueError('NODE_ARCHIVE_SCOPE_EXCEEDED')
        for path in files:
            if path.is_symlink() or path.stat().st_size>65536:raise ValueError('NODE_ARCHIVE_INVALID')
            receipt=_decode(path.read_bytes());pin=receipt.get('receipt_sha256')
            if (receipt.get('schema_version')!=VERSION or receipt.get('endpoint')!=endpoint
                or pin!=_sha(_raw({k:v for k,v in receipt.items() if k!='receipt_sha256'}))):raise ValueError('NODE_ARCHIVE_PIN_CONFLICT')
            if receipt.get('raw_path'):
                body=Path(receipt['raw_path'])
                if (not body.resolve().is_relative_to(self.root) or body.is_symlink()
                    or body.stat().st_size>CAP or _sha(body.read_bytes())!=receipt['raw_sha256']):raise ValueError('NODE_ARCHIVE_RAW_CONFLICT')
                if receipt.get('status')=='READY':verify_ready_sample(body.read_bytes(),receipt,observed_at=receipt['received_at'])
            day=receipt['trade_date'];previous=self.days.get(day)
            if (previous is None or _stamp(previous['requested_at'])<_stamp(receipt['requested_at'])
                or previous['requested_at']==receipt['requested_at'] and receipt.get('receipt_kind','FINAL')=='FINAL'):
                self.days[day]=receipt

    def sample_if_due(self,*,observed_at):
        observed=_stamp(observed_at);day=observed.date().isoformat();last=self.days.get(day)
        if self.last_clock is not None and observed<self.last_clock:
            return dict(status='DATA_LIMITED',reason_codes=['NODE_CLIENT_CLOCK_REGRESSED'],source_authenticated=False)
        self.last_clock=observed
        if not time(16)<=observed.time()<=time(16,45):return dict(status='NOT_DUE',reason_codes=['OUTSIDE_NODE_SAMPLE_WINDOW'])
        if last is not None:
            elapsed=(observed-_stamp(last['requested_at'])).total_seconds()
            if elapsed<0:return dict(status='DATA_LIMITED',reason_codes=['NODE_CLIENT_CLOCK_REGRESSED'])
            if elapsed<60:
                if last.get('status')=='READY':
                    try:
                        path=Path(last['raw_path'])
                        if path.is_symlink() or path.stat().st_size>CAP:raise ValueError
                        verify_ready_sample(path.read_bytes(),last,observed_at=observed)
                    except Exception:
                        return dict(last,status='DATA_LIMITED',reason_codes=['NODE_CACHED_ORIGINAL_UNPROVEN'],raw_path=None,raw_sha256=None)
                return dict(last)
        baseline=last.get('identity_baseline') if last else None
        restart=last.get('restart_unproven',False) if last else False
        requested=_stamp(self.clock());body=None
        name=requested.strftime('%H%M%S%f')+'-'+uuid4().hex
        reservation=dict(schema_version=VERSION,receipt_kind='REQUEST_STARTED',status='DATA_LIMITED',
            reason_codes=['NODE_REQUEST_UNFINISHED'],endpoint=self.endpoint,trade_date=day,
            requested_at=requested.isoformat(),received_at=requested.isoformat(),identity_baseline=baseline,
            restart_unproven=restart,raw_path=None,raw_sha256=None,source_authenticated=False)
        reservation['receipt_sha256']=_sha(_raw(reservation))
        try:_write(self.root/day/(name+'.request.json'),_raw(reservation))
        except Exception:
            return dict(status='DATA_LIMITED',reason_codes=['NODE_SAMPLE_ARCHIVE_FAILED'],source_authenticated=False)
        # If killed during HTTP, the separate original request record keeps its
        # 60s slot on restart. It is never promoted to a completed response.
        self.days[day]=reservation
        began=self.monotonic()
        result=dict(status='DATA_LIMITED',reason_codes=['NODE_SAMPLE_ACQUISITION_FAILED'],source_authenticated=False)
        try:
            actual_request=_stamp(self.clock())
            if actual_request<requested or requested<observed:raise ValueError('NODE_CLIENT_CLOCK_REGRESSED')
            if actual_request.date().isoformat()!=day or not time(16)<=actual_request.time()<=time(16,45):
                raise ValueError('NODE_REQUEST_AFTER_FENCE_OR_WRONG_DAY')
            requested=actual_request
            headers={'Accept':'application/json','Accept-Encoding':'identity'}
            token=os.environ.get(self.token_env_name)
            if token:headers['Authorization']='Bearer '+token
            response=self.transport(self.endpoint,headers=headers,timeout=2.,monotonic=self.monotonic)
            received=_stamp(self.clock());elapsed=self.monotonic()-began
            body=response['raw']
            if not isinstance(body,bytes) or len(body)>CAP:body=None;raise ValueError('NODE_RESPONSE_SIZE_OR_BYTES_INVALID')
            if not 0<=elapsed<=2:raise ValueError('NODE_SAMPLE_TOTAL_BUDGET_EXCEEDED')
            if response['status']!=200:raise ValueError('NODE_HTTP_STATUS_NOT_OK')
            if response['content_type'].split(';')[0].strip().lower()!='application/json' or response['encoding'].lower() not in ('','identity'):
                raise ValueError('NODE_RESPONSE_FORMAT_INVALID')
            result=inspect_node_sample(body,trade_date=day,requested_at=requested,received_at=received,previous_node=baseline)
            if 'NODE_RESTART_OR_HISTORY_UNPROVEN' in result['reason_codes']:restart=True
            if restart:result.update(status='DATA_LIMITED',reason_codes=['NODE_RESTART_OR_HISTORY_UNPROVEN'])
            if result['status']=='READY' and baseline is None:baseline={k:result['node'][k] for k in ('pid','startedAtEpochMs')}
        except Exception as exc:
            code=str(exc)
            if code in {'NODE_CLIENT_CLOCK_REGRESSED','NODE_REQUEST_AFTER_FENCE_OR_WRONG_DAY',
                'NODE_SAMPLE_TOTAL_BUDGET_EXCEEDED','NODE_RESPONSE_SIZE_OR_BYTES_INVALID',
                'NODE_HTTP_STATUS_NOT_OK','NODE_RESPONSE_FORMAT_INVALID'}:result['reason_codes']=[code]
            received=_stamp(self.clock())
        receipt=dict(result,schema_version=VERSION,endpoint=self.endpoint,trade_date=day,
            receipt_kind='FINAL',
            requested_at=requested.isoformat(),received_at=received.isoformat(),identity_baseline=baseline,
            restart_unproven=restart,raw_path=None,raw_sha256=None,source_authenticated=False)
        try:
            if body is not None:
                path=self.root/day/(name+'.body');_write(path,body)
                receipt.update(raw_path=str(path),raw_sha256=_sha(body))
            receipt['receipt_sha256']=_sha(_raw(receipt))
            _write(self.root/day/(name+'.sample.json'),_raw(receipt))
        except Exception:
            receipt.update(status='DATA_LIMITED',reason_codes=['NODE_SAMPLE_ARCHIVE_FAILED'],raw_path=None,raw_sha256=None)
        self.days[day]=receipt
        return dict(receipt)

__all__=['inspect_node_sample','verify_ready_sample','LoopbackNodeSampler']
