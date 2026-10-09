"""Independent slow board references. Shadow-only; never manufacture BK identities.

Public responses and local files are hash-bound evidence. Observation time is
not a provider update date. No quotes, money-flow estimates or execution gates
are supplied by this module.
"""
from __future__ import annotations

import hashlib
import base64
import html
import json
import math
import re
import struct
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

from ..reporting import atomic_write_json
from ..redaction import sanitize

SCHEMA = 'liangjian-board-reference/1.0.0'
TZ = ZoneInfo('Asia/Shanghai')
SINA_BASE = 'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.'


class ReferenceError(ValueError):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def aware(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ReferenceError('REFERENCE_TIME_INVALID')
    return value.astimezone(TZ)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ReferenceError('REFERENCE_DUPLICATE_JSON_KEY')
        result[key] = value
    return result


def parse_json(text):
    try:
        return json.loads(text, object_pairs_hook=_unique_object)
    except (ValueError, TypeError) as exc:
        raise ReferenceError('REFERENCE_JSON_INVALID') from exc


def _text(body):
    for encoding in ('utf-8-sig', 'gb18030'):
        try:
            return body.decode(encoding)
        except UnicodeDecodeError:
            pass
    raise ReferenceError('REFERENCE_ENCODING_INVALID')


def _symbol(code):
    match = re.fullmatch(r'(?:(sh|sz|bj))?(\d{6})(?:\.(SH|SZ|BJ))?', str(code), re.I)
    if not match:
        raise ReferenceError('REFERENCE_SYMBOL_INVALID')
    prefix, digits, suffix = match.groups()
    market = 'SH' if digits.startswith(('6','5','900')) else 'SZ' if digits.startswith(('0','1','2','3')) else 'BJ' if digits.startswith(('4','8','92')) else None
    if not market or (prefix and prefix.upper() != market) or (suffix and suffix.upper() != market):
        raise ReferenceError('REFERENCE_SYMBOL_MARKET_MISMATCH')
    return f'{digits}.{market}'


def _a_share(symbol):
    code,market=_symbol(symbol).split('.')
    return (market=='SH' and code.startswith('6')) or (market=='SZ' and code.startswith(('000','001','002','003','300','301','302'))) or market=='BJ'


class BoardReferenceClient:
    """Bounded, low-frequency public GETs. No cookies or anti-bot bypass."""
    def __init__(self, *, get=None, now=None, sleep=time.sleep, interval_seconds=0.25,
                 max_requests=1000, deadline_seconds=300, max_body_bytes=4_000_000,
                 max_evidence_bytes=20_000_000):
        self.get = get or requests.get
        self.now = now or (lambda: datetime.now(TZ))
        self.sleep = sleep
        self.interval = max(0.0, float(interval_seconds))
        self.max_requests = int(max_requests)
        self.deadline = time.monotonic() + float(deadline_seconds)
        self.max_body_bytes = int(max_body_bytes)
        self.max_evidence_bytes = int(max_evidence_bytes)
        self.evidence_bytes = 0
        self.requests = 0
        self.pages = []

    def fetch(self, url, params=None):
        remaining = self.deadline-time.monotonic()
        if remaining <= self.interval:
            raise ReferenceError('REFERENCE_DEADLINE')
        if self.requests >= self.max_requests:
            raise ReferenceError('REFERENCE_REQUEST_BUDGET')
        if self.requests:
            self.sleep(self.interval)
        remaining = self.deadline-time.monotonic()
        if remaining <= 0:
            raise ReferenceError('REFERENCE_DEADLINE')
        self.requests += 1
        evidence = {'url': url, 'params': params or {}, 'received_at': None}
        self.pages.append(evidence)
        try:
            response = self.get(url, params=params, timeout=(min(3,remaining/2),min(8,remaining/2)),
                                headers={'User-Agent':'Mozilla/5.0 (compatible; LiangjianReference/1.0)',
                                         'Referer':urlparse(url)._replace(path='/',query='',fragment='').geturl()})
        except Exception as exc:
            evidence['transport_error'] = type(exc).__name__
            raise ReferenceError('REFERENCE_TRANSPORT_UNAVAILABLE') from exc
        evidence.update(received_at=aware(self.now()).isoformat(), http_status=response.status_code,
                        body_bytes=len(response.content), body_sha256=hashlib.sha256(response.content).hexdigest())
        if time.monotonic() >= self.deadline:
            raise ReferenceError('REFERENCE_DEADLINE')
        if len(response.content) > self.max_body_bytes:
            raise ReferenceError('REFERENCE_BODY_LIMIT')
        self.evidence_bytes += len(response.content)
        if self.evidence_bytes > self.max_evidence_bytes:
            raise ReferenceError('REFERENCE_EVIDENCE_LIMIT')
        evidence['body'] = _text(response.content)
        evidence['body_base64'] = base64.b64encode(response.content).decode('ascii')
        if response.status_code != 200:
            raise ReferenceError(f'REFERENCE_HTTP_{response.status_code}')
        if not response.content:
            raise ReferenceError('REFERENCE_BODY_EMPTY')
        return evidence['body']


def _result(client, start, source, kind, board_id, records=None, *, error=None, **extra):
    body = {'schema_version':SCHEMA, 'source_id':source, 'kind':kind, 'board_id':board_id,
            'available':error is None, 'complete':error is None, 'reason_code':error or 'OK',
            'observed_at':aware(client.now()).isoformat(), 'source_updated_at':None,
            'execution_scope':'SHADOW','production_publish_forbidden':True,
            'records':records or [],'pages':client.pages[start:], **extra}
    # Public raw bytes survive report redaction in a separate hash-bound field.
    body = sanitize(body)
    body['content_hash'] = digest(body)
    return body


def collect_sina_catalog(client, category='concept'):
    start=len(client.pages)
    try:
        if category not in {'concept','industry'}:
            raise ReferenceError('REFERENCE_CATEGORY_INVALID')
        url='https://money.finance.sina.com.cn/q/view/newFLJK.php' if category=='concept' else 'https://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php'
        text=client.fetch(url,{'param':'class'} if category=='concept' else None)
        match=re.fullmatch(r'\s*(?:var\s+[A-Za-z_][A-Za-z_0-9]*\s*=\s*)?(\{.*\})\s*;?\s*',text,re.S)
        if not match:
            raise ReferenceError('REFERENCE_CATALOG_INVALID')
        payload=parse_json(match[1]);records=[]
        for code,value in payload.items():
            if not isinstance(value,str):raise ReferenceError('REFERENCE_CATALOG_INVALID')
            cells=value.split(',')
            if not re.fullmatch(r'(?:gn|new|hangye)_[A-Za-z0-9_]+',code) or len(cells)<3 or cells[0]!=code or not cells[1].strip():
                raise ReferenceError('REFERENCE_BOARD_IDENTITY_INVALID')
            count=_count(cells[2]);records.append({'board_id':code,'name':cells[1].strip(),'display_count':count})
        if not records:raise ReferenceError('REFERENCE_CATALOG_EMPTY')
        return _result(client,start,'SINA','catalog',category,records,catalog_scope=category)
    except (ReferenceError,ValueError,TypeError,AttributeError) as exc:
        return _result(client,start,'SINA','catalog',category,error=str(exc) if isinstance(exc,ReferenceError) else 'REFERENCE_CATALOG_INVALID')


def _count(value):
    if isinstance(value,bool) or not re.fullmatch(r'\d+',str(value)):
        raise ReferenceError('REFERENCE_COUNT_INVALID')
    return int(value)


def collect_sina_members(client, board_id, board_name, *, max_pages=30):
    start=len(client.pages);conflict=None
    try:
        if not re.fullmatch(r'(?:gn|new|hangye)_[A-Za-z0-9_]+',board_id) or not board_name:
            raise ReferenceError('REFERENCE_BOARD_IDENTITY_INVALID')
        total=_count(parse_json(client.fetch(SINA_BASE+'getHQNodeStockCount',{'node':board_id})))
        page_count=math.ceil(total/80)
        if not total or page_count>max_pages:raise ReferenceError('REFERENCE_PAGE_LIMIT_OR_EMPTY')
        records=[]
        for page in range(1,page_count+1):
            items=parse_json(client.fetch(SINA_BASE+'getHQNodeData',{'node':board_id,'page':page,'num':80,'sort':'symbol','asc':1,'symbol':'','_s_r_a':'page'}))
            expected=min(80,total-(page-1)*80)
            if not isinstance(items,list):raise ReferenceError('REFERENCE_PAGE_INCOMPLETE')
            if len(items)!=expected:
                conflict={'provider_total':total,'page':page,'expected_page_count':expected,'received_page_count':len(items)}
                raise ReferenceError('REFERENCE_PROVIDER_COUNT_CONFLICT' if len(items)>expected else 'REFERENCE_PAGE_INCOMPLETE')
            for item in items:
                if not isinstance(item,dict):raise ReferenceError('REFERENCE_MEMBER_INVALID')
                symbol=_symbol(item.get('symbol'))
                if symbol[:6]!=item.get('code') or not str(item.get('name','')).strip():raise ReferenceError('REFERENCE_MEMBER_INVALID')
                records.append({'symbol':symbol,'name':item['name'].strip()})
        if len({x['symbol'] for x in records})!=total:raise ReferenceError('REFERENCE_MEMBER_DUPLICATED')
        final=_count(parse_json(client.fetch(SINA_BASE+'getHQNodeStockCount',{'node':board_id})))
        if final!=total:raise ReferenceError('REFERENCE_TOTAL_CHANGED')
        return _result(client,start,'SINA','members',board_id,sorted(records,key=lambda x:x['symbol']),board_name=board_name,
                       pagination={'page_count':page_count,'page_size':80,'provider_total':total,'received_count':len(records),'complete':True})
    except (ReferenceError,ValueError,TypeError,KeyError,AttributeError) as exc:
        return _result(client,start,'SINA','members',board_id,error=str(exc) if isinstance(exc,ReferenceError) else 'REFERENCE_MEMBER_INVALID',board_name=board_name,pagination_conflict=conflict)


class _Links(HTMLParser):
    def __init__(self):
        super().__init__();self.links=[];self.link=None;self.row=0;self.in_row=False;self.table_stack=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='table':self.table_stack.append('m-table' in attrs.get('class','').split())
        if tag=='tr':self.row+=1;self.in_row=bool(self.table_stack and self.table_stack[-1])
        if tag=='a':self.link=[attrs.get('href',''),'',self.row if self.in_row else None]
    def handle_data(self,data):
        if self.link is not None:self.link[1]+=data
    def handle_endtag(self,tag):
        if tag=='a' and self.link is not None:self.links.append(self.link);self.link=None
        if tag=='tr':self.in_row=False
        if tag=='table' and self.table_stack:self.table_stack.pop()


def collect_ths_catalog(client):
    start=len(client.pages)
    try:
        parser=_Links();parser.feed(client.fetch('https://q.10jqka.com.cn/gn/'));entries={}
        for href,name,_ in parser.links:
            link=urlparse(href)
            match=re.fullmatch(r'/gn/detail/code/(\d{6})/?',link.path)
            if link.hostname!='q.10jqka.com.cn' or not match:continue
            code=match[1];name=html.unescape(name).strip()
            if not name:raise ReferenceError('REFERENCE_BOARD_NAME_EMPTY')
            if code in entries and entries[code]!=name:raise ReferenceError('REFERENCE_BOARD_NAME_CONFLICT')
            entries[code]=name
        if not entries:raise ReferenceError('REFERENCE_CATALOG_EMPTY')
        return _result(client,start,'THS_WEB','catalog','concept',[{'board_id':k,'name':v} for k,v in sorted(entries.items())],catalog_scope='PAGE_DIRECTORY_NOT_ALL_PROVIDER_CLASSES')
    except ReferenceError as exc:return _result(client,start,'THS_WEB','catalog','concept',error=str(exc))


def _ths_page(text):
    match=re.search(r'class=[\"\']page_info[\"\'][^>]*>\s*(\d+)/(\d+)\s*<',text)
    if not match:raise ReferenceError('REFERENCE_PAGINATION_MISSING')
    parser=_Links();parser.feed(text);rows={}
    for href,name,row in parser.links:
        link=urlparse(href)
        code=re.fullmatch(r'/(\d{6})/?',link.path)
        if row is None or link.hostname!='stockpage.10jqka.com.cn' or not code:continue
        symbol=_symbol(code[1])
        if row in rows and rows[row]['symbol']!=symbol:raise ReferenceError('REFERENCE_MEMBER_ROW_CONFLICT')
        if row not in rows or not name.strip().isdigit():rows[row]={'symbol':symbol,'name':html.unescape(name).strip()}
    if not rows:raise ReferenceError('REFERENCE_MEMBERS_EMPTY')
    return int(match[1]),int(match[2]),list(rows.values())


def collect_ths_members(client,board_id,board_name,*,max_pages=50):
    start=len(client.pages)
    try:
        if not re.fullmatch(r'\d{6}',board_id) or not board_name:raise ReferenceError('REFERENCE_BOARD_IDENTITY_INVALID')
        text=client.fetch(f'https://q.10jqka.com.cn/gn/detail/code/{board_id}/')
        # The site's title is generic. Its visible board header and hidden
        # index identity must agree; menu links alone are not identity proof.
        headers=re.findall(r'<h3[^>]*>\s*([^<]+)\s*<span[^>]*>\s*(\d+)\s*</span>\s*</h3>',text,re.S|re.I)
        clid=re.search(r'id=[\"\']clid[\"\'][^>]*value=[\"\'](\d+)[\"\']',text)
        if not clid or not any(html.unescape(name).strip()==board_name and code==clid[1] for name,code in headers):
            raise ReferenceError('REFERENCE_BOARD_PAGE_IDENTITY_MISMATCH')
        current,total,first=_ths_page(text)
        if current!=1 or not 1<=total<=max_pages:raise ReferenceError('REFERENCE_PAGE_LIMIT_OR_IDENTITY')
        records=first;size=len(first)
        for page in range(2,total+1):
            actual,claimed,items=_ths_page(client.fetch(f'https://q.10jqka.com.cn/gn/detail/field/199112/order/desc/page/{page}/ajax/1/code/{board_id}/'))
            if actual!=page or claimed!=total:raise ReferenceError('REFERENCE_PAGE_IDENTITY_CHANGED')
            if (page<total and len(items)!=size) or not 1<=len(items)<=size:raise ReferenceError('REFERENCE_PAGE_INCOMPLETE')
            records.extend(items)
        if len({x['symbol'] for x in records})!=len(records):raise ReferenceError('REFERENCE_MEMBER_DUPLICATED')
        return _result(client,start,'THS_WEB','members',board_id,sorted(records,key=lambda x:x['symbol']),board_name=board_name,
                       pagination={'page_count':total,'page_size':size,'provider_total':None,'received_count':len(records),'complete':True,'count_evidence':'PROVIDER_PAGE_COUNT_NOT_INDEPENDENT_MEMBER_TOTAL'})
    except ReferenceError as exc:return _result(client,start,'THS_WEB','members',board_id,error=str(exc),board_name=board_name)


def import_tdx_blocks(path,*,now,source_updated_at):
    """Read local concept/style/index files; never use mtime as supplier date."""
    client=BoardReferenceClient(now=lambda:now);start=0
    try:
        stamp=aware(source_updated_at)
        if stamp>aware(now):raise ReferenceError('REFERENCE_SOURCE_TIME_FUTURE')
        path=Path(path)
        if path.name not in {'block_gn.dat','block_fg.dat','block_zs.dat'}:raise ReferenceError('TDX_FILE_KIND_UNSUPPORTED')
        if path.stat().st_size>16_000_000:raise ReferenceError('REFERENCE_BODY_LIMIT')
        raw=path.read_bytes()
        client.pages.append({'file_name':path.name,'body_bytes':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest(),'received_at':aware(now).isoformat()})
        if len(raw)<386:raise ReferenceError('TDX_HEADER_INCOMPLETE')
        count=struct.unpack_from('<H',raw,384)[0]
        if not count or len(raw)!=386+2813*count:raise ReferenceError('TDX_FILE_LENGTH_MISMATCH')
        boards=[];names=set()
        for index in range(count):
            block=raw[386+2813*index:386+2813*(index+1)]
            name=block[:9].split(b'\0')[0].decode('gbk').strip()
            num,_=struct.unpack_from('<HH',block,9)
            if not name or name in names or not 1<=num<=400:raise ReferenceError('TDX_BLOCK_INVALID')
            names.add(name);members=[_symbol(block[13+i*7:13+(i+1)*7].split(b'\0')[0].decode('ascii')) for i in range(num)]
            if len(set(members))!=num:raise ReferenceError('REFERENCE_MEMBER_DUPLICATED')
            boards.append({'board_id':f'{path.stem}:{hashlib.sha256(name.encode()).hexdigest()[:16]}','name':name,'member_count':num,'records':[{'symbol':s,'name':''} for s in sorted(members)]})
        return _result(client,start,'TDX_LOCAL','catalog_with_members',path.stem,boards,source_updated_at=stamp.isoformat(),
                       source_time_basis='OPERATOR_SUPPLIED_NOT_FILE_MTIME',file_sha256=hashlib.sha256(raw).hexdigest())
    except (ReferenceError,OSError,UnicodeError,struct.error) as exc:
        return _result(client,start,'TDX_LOCAL','catalog_with_members',Path(path).stem,error=str(exc) if isinstance(exc,ReferenceError) else 'TDX_FILE_INVALID')


def import_tdx_industries(catalog_path,stock_path,*,now,source_updated_at):
    """Industry catalog + full stock classification, including parent prefixes."""
    client=BoardReferenceClient(now=lambda:now)
    try:
        stamp=aware(source_updated_at)
        if stamp>aware(now):raise ReferenceError('REFERENCE_SOURCE_TIME_FUTURE')
        raw_files=[]
        for path in (Path(catalog_path),Path(stock_path)):
            if path.stat().st_size>16_000_000:raise ReferenceError('REFERENCE_BODY_LIMIT')
            raw=path.read_bytes();raw_files.append(raw)
            client.pages.append({'file_name':path.name,'body_bytes':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest(),'received_at':aware(now).isoformat()})
        catalogs=[];codes=set();stocks=[];symbols=set()
        for line in raw_files[0].decode('gbk').splitlines():
            if not line.strip():continue
            cells=line.split('|')
            if len(cells)!=6:raise ReferenceError('TDX_INDUSTRY_CATALOG_INVALID')
            name,code,category,_,_,sector=cells
            if category!='2':continue
            if not name or not re.fullmatch(r'\d{6}',code) or not sector or code in codes:raise ReferenceError('TDX_INDUSTRY_CATALOG_INVALID')
            codes.add(code);catalogs.append((name,code,sector))
        for line in raw_files[1].decode('gbk').splitlines():
            if not line.strip():continue
            cells=line.split('|')
            if len(cells)!=6:raise ReferenceError('TDX_INDUSTRY_STOCK_INVALID')
            market,code,sector,_,_,_=cells
            if market not in {'0','1','2'}:raise ReferenceError('TDX_INDUSTRY_MARKET_INVALID')
            symbol=_symbol(f"{code}.{ {'0':'SZ','1':'SH','2':'BJ'}[market] }")
            if symbol in symbols:raise ReferenceError('REFERENCE_MEMBER_DUPLICATED')
            symbols.add(symbol);stocks.append((symbol,sector))
        if not catalogs or not stocks:raise ReferenceError('TDX_INDUSTRY_EMPTY')
        boards=[]
        for name,code,sector in catalogs:
            members=sorted(symbol for symbol,stock_sector in stocks if stock_sector==sector or (len(sector)==5 and stock_sector.startswith(sector)))
            boards.append({'board_id':f'TDX:{code}','name':name,'member_count':len(members),'records':[{'symbol':s,'name':''} for s in members]})
        return _result(client,0,'TDX_LOCAL','catalog_with_members','industry',boards,source_updated_at=stamp.isoformat(),
                       source_time_basis='OPERATOR_SUPPLIED_NOT_FILE_MTIME',
                       unclassified_stock_count=sum(not sector for _,sector in stocks),stock_universe_count=len(stocks),
                       file_sha256=digest([p['body_sha256'] for p in client.pages]))
    except (ReferenceError,OSError,UnicodeError) as exc:
        return _result(client,0,'TDX_LOCAL','catalog_with_members','industry',error=str(exc) if isinstance(exc,ReferenceError) else 'TDX_FILE_INVALID')


def collect_em_f10_tags(client,symbol):
    """Different Eastmoney endpoint, not an independent vendor or complete board."""
    start=len(client.pages)
    try:
        symbol=_symbol(symbol);code,market=symbol.split('.')
        payload=parse_json(client.fetch('https://emweb.securities.eastmoney.com/PC_HSF10/CoreConception/PageAjax',{'code':market+code}))
        tags=payload.get('ssbk')
        if not isinstance(tags,list) or not tags:raise ReferenceError('REFERENCE_F10_TAGS_EMPTY')
        records=[];seen=set();stock_name=None
        for tag in tags:
            if tag.get('SECUCODE')!=symbol or tag.get('SECURITY_CODE')!=code:raise ReferenceError('REFERENCE_F10_STOCK_MISMATCH')
            board_id=str(tag.get('BOARD_CODE',''));name=tag.get('BOARD_NAME');company=tag.get('SECURITY_NAME_ABBR')
            if not re.fullmatch(r'\d{1,10}',board_id) or not isinstance(name,str) or not name.strip() or not isinstance(company,str) or not company.strip():raise ReferenceError('REFERENCE_F10_TAG_INVALID')
            if board_id in seen:raise ReferenceError('REFERENCE_F10_TAG_DUPLICATED')
            if stock_name is not None and company!=stock_name:raise ReferenceError('REFERENCE_F10_STOCK_NAME_CONFLICT')
            stock_name=company;seen.add(board_id);records.append({'board_id':board_id,'name':name.strip()})
        return _result(client,start,'EM_F10','stock_tags',symbol,records,stock_name=stock_name,
                       taxonomy_scope='STOCK_DISCLOSED_F10_TAGS_NO_PROVIDER_BOARD_TOTAL')
    except (ReferenceError,ValueError,TypeError,AttributeError) as exc:
        return _result(client,start,'EM_F10','stock_tags',symbol,error=str(exc) if isinstance(exc,ReferenceError) else 'REFERENCE_F10_INVALID')


def invert_f10_universe(universe,references,*,now=None,now_provider=None,max_age_days=14):
    """Partial stock samples never become allegedly full board memberships."""
    symbols=[_symbol(s) for s in universe]
    if not symbols or len(set(symbols))!=len(symbols):raise ReferenceError('REFERENCE_UNIVERSE_INVALID')
    current=now_provider or (lambda:now)
    cutoff=aware(current());valid={}
    for reference in references:
        if not _valid(reference) or reference.get('source_id')!='EM_F10' or reference.get('kind')!='stock_tags':continue
        cutoff=aware(current());stamp=aware(datetime.fromisoformat(reference['observed_at']))
        if stamp>cutoff or (cutoff.date()-stamp.date()).days>max_age_days:continue
        symbol=reference['board_id']
        if symbol not in valid or stamp>aware(datetime.fromisoformat(valid[symbol]['observed_at'])):
            # Keep raw evidence in immutable per-stock files, not a second graph.
            valid[symbol]={k:reference[k] for k in ('board_id','stock_name','records','observed_at','content_hash')}
    cutoff=aware(current())
    valid={s:r for s,r in valid.items() if (cutoff.date()-aware(datetime.fromisoformat(r['observed_at'])).date()).days<=max_age_days}
    missing=sorted(set(symbols)-valid.keys());groups={};captures=[]
    client=BoardReferenceClient(now=lambda:cutoff)
    metadata={'input_universe_count':len(symbols),'input_universe_hash':digest(sorted(symbols)),
              'covered_stock_count':len(set(symbols)&valid.keys()),'missing_symbols':missing,
              'source_reference_hashes':{s:valid[s]['content_hash'] for s in sorted(set(symbols)&valid.keys())},
              'catalog_scope':'INPUT_UNIVERSE_ONLY_NOT_PROVIDER_BOARD_TOTAL'}
    if missing:return _result(client,0,'EM_F10','catalog_with_members','reverse_universe',error='REFERENCE_UNIVERSE_INCOMPLETE',**metadata)
    for symbol in sorted(symbols):
        ref=valid[symbol];captures.append(aware(datetime.fromisoformat(ref['observed_at'])))
        for tag in ref['records']:
            code=tag['board_id'];name=tag['name']
            if code in groups and groups[code]['name']!=name:
                return _result(client,0,'EM_F10','catalog_with_members','reverse_universe',error='REFERENCE_BOARD_NAME_CONFLICT',**metadata)
            board=groups.setdefault(code,{'board_id':code,'name':name,'records':[]})
            board['records'].append({'symbol':symbol,'name':ref['stock_name']})
    boards=[{**g,'member_count':len(g['records'])} for _,g in sorted(groups.items())]
    # Assembly does not refresh an old component's observation date.
    client.now=lambda:min(captures)
    return _result(client,0,'EM_F10','catalog_with_members','reverse_universe',boards,
                   assembled_at=cutoff.isoformat(),member_observed_at_range=[min(captures).isoformat(),max(captures).isoformat()],**metadata)


def _valid(payload):
    if not isinstance(payload,dict):return False
    body={k:v for k,v in payload.items() if k not in {'age_days','fallback_reused','warning'}}
    expected=body.pop('content_hash',None)
    try:actual=digest(body)
    except (ValueError,TypeError):return False
    if not (payload.get('schema_version')==SCHEMA and expected==actual and payload.get('execution_scope')=='SHADOW'
            and payload.get('production_publish_forbidden') is True and payload.get('available') is True and payload.get('complete') is True):return False
    records=payload.get('records')
    if not isinstance(records,list) or not records:return False
    try:
        if payload['kind']=='members':
            symbols=[_symbol(r['symbol']) for r in records]
            p=payload['pagination']
            return len(set(symbols))==len(symbols) and p['complete'] is True and p['received_count']==len(symbols) and (p['provider_total'] is None or p['provider_total']==len(symbols))
        ids=[r['board_id'] for r in records]
        return len(set(ids))==len(ids) and all(isinstance(r['name'],str) and r['name'].strip() for r in records)
    except (ValueError,TypeError,KeyError):return False


def write_reference(root,payload):
    if not isinstance(payload,dict):raise ReferenceError('REFERENCE_SNAPSHOT_INVALID')
    body=dict(payload);expected=body.pop('content_hash',None)
    if payload.get('schema_version')!=SCHEMA or expected!=digest(body):raise ReferenceError('REFERENCE_HASH_INVALID')
    path=Path(root)/f"board-reference-{payload['source_id']}-{expected}.json"
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*',str(payload['source_id'])):raise ReferenceError('REFERENCE_SOURCE_INVALID')
    if path.exists():
        if json.loads(path.read_text(encoding='utf-8'))!=payload:raise ReferenceError('REFERENCE_IMMUTABLE_CONFLICT')
        return path
    result=atomic_write_json(path,payload)
    if json.loads(result.read_text(encoding='utf-8'))!=payload:raise ReferenceError('REFERENCE_PERSISTENCE_CHANGED')
    return result


def load_reference(root,source,board_id,*,now,max_age_days=14,warn_age_days=7,update_failed=False,kind='members'):
    cutoff=aware(now);candidates=[]
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*',str(source)) or not 0<=warn_age_days<=max_age_days:raise ReferenceError('REFERENCE_LOAD_POLICY_INVALID')
    for path in Path(root).glob(f'board-reference-{source}-*.json'):
        try:
            p=json.loads(path.read_text(encoding='utf-8'))
            if not _valid(p) or p['source_id']!=source or p['board_id']!=board_id or p['kind']!=kind:continue
            stamp=aware(datetime.fromisoformat(p['observed_at']))
            if stamp<=cutoff:candidates.append((stamp,p))
        except (ValueError,TypeError,KeyError,OSError):continue
    if not candidates:return {'available':False,'reason_code':'REFERENCE_CACHE_NOT_FOUND'}
    stamp,p=max(candidates,key=lambda x:x[0]);age=(cutoff.date()-stamp.date()).days
    if age>max_age_days:return {'available':False,'reason_code':'REFERENCE_CACHE_EXPIRED','age_days':age}
    return {**p,'age_days':age,'fallback_reused':bool(update_failed or age>=warn_age_days),'warning':'REFERENCE_UPDATE_FAILED_FALLBACK' if update_failed else 'REFERENCE_AGE_WARNING' if age>=warn_age_days else None}


def tdx_board_members(reference,board_id,*,now,max_age_days=14):
    """Expose a selected local board without losing its original file lineage."""
    if not _valid(reference) or reference['source_id']!='TDX_LOCAL' or reference['kind']!='catalog_with_members':
        raise ReferenceError('TDX_REFERENCE_INVALID')
    cutoff=aware(now);observed=aware(datetime.fromisoformat(reference['observed_at']))
    updated=aware(datetime.fromisoformat(reference['source_updated_at']))
    if observed>cutoff or updated>observed or (cutoff.date()-updated.date()).days>max_age_days:
        raise ReferenceError('TDX_SOURCE_EXPIRED_OR_FUTURE')
    board=next((r for r in reference['records'] if r['board_id']==board_id),None)
    if not board:raise ReferenceError('TDX_BOARD_NOT_FOUND')
    records=board['records']
    if not records:raise ReferenceError('TDX_BOARD_EMPTY')
    if board['member_count']!=len(records) or len({r['symbol'] for r in records})!=len(records):raise ReferenceError('TDX_BLOCK_INVALID')
    client=BoardReferenceClient(now=lambda:observed);client.pages=list(reference['pages'])
    return _result(client,0,'TDX_LOCAL','members',board_id,records,board_name=board['name'],
                   source_updated_at=reference['source_updated_at'],source_file_hash=reference['file_sha256'],
                   source_catalog_hash=reference['content_hash'],pagination={'page_count':1,'page_size':None,'provider_total':len(records),'received_count':len(records),'complete':True})


def audit_theme_bindings(themes,catalogs):
    rows=[]
    for theme in themes:
        aliases={str(x).strip() for x in [theme['name'],*theme.get('aliases',[])]};matches=[]
        for catalog in catalogs:
            if not _valid(catalog):continue
            for board in catalog['records']:
                if board['name'] in aliases:
                    matches.append({'source_id':catalog['source_id'],'board_id':board['board_id'],'board_name':board['name'],'catalog_hash':catalog['content_hash'],'approved':False})
        rows.append({'theme_id':theme['theme_id'],'status':'REVIEW_REQUIRED' if matches else 'UNMAPPED','candidates':matches})
    return {'execution_scope':'SHADOW','production_publish_forbidden':True,'theme_count':len(rows),'matched_theme_count':sum(bool(x['candidates']) for x in rows),'themes':rows}


def project_theme_binding(binding,catalog,members,*,now,max_age_days=14):
    """Create an auditable shadow projection, not a production membership file."""
    if binding.get('approved') is not True:raise ReferenceError('REFERENCE_BINDING_NOT_APPROVED')
    if not _valid(catalog) or not _valid(members):raise ReferenceError('REFERENCE_INCOMPLETE')
    if catalog.get('catalog_scope')=='INPUT_UNIVERSE_ONLY_NOT_PROVIDER_BOARD_TOTAL':
        raise ReferenceError('REFERENCE_INPUT_UNIVERSE_NOT_FULL_MEMBERSHIP')
    source=binding.get('source_id');code=binding.get('board_id');name=binding.get('board_name')
    if binding.get('category') and catalog.get('catalog_scope') != binding['category']:
        raise ReferenceError('REFERENCE_BINDING_CATEGORY_MISMATCH')
    if catalog['source_id']!=source or members['source_id']!=source or members['board_id']!=code or members.get('board_name')!=name:
        raise ReferenceError('REFERENCE_BINDING_IDENTITY_MISMATCH')
    if members.get('source_catalog_hash') and members['source_catalog_hash'] != catalog['content_hash']:
        raise ReferenceError('REFERENCE_CATALOG_VERSION_MISMATCH')
    if {'board_id':code,'name':name} not in [{'board_id':x['board_id'],'name':x['name']} for x in catalog['records']]:
        raise ReferenceError('REFERENCE_BINDING_IDENTITY_MISMATCH')
    cutoff=aware(now)
    if binding.get('effective_from') and datetime.fromisoformat(binding['effective_from']).date() > cutoff.date():
        raise ReferenceError('REFERENCE_BINDING_NOT_YET_EFFECTIVE')
    for reference in (catalog,members):
        stamp=aware(datetime.fromisoformat(reference['observed_at']))
        if stamp>cutoff or (cutoff.date()-stamp.date()).days>max_age_days:raise ReferenceError('REFERENCE_BINDING_TIME_INVALID')
        if reference.get('source_updated_at'):
            updated=aware(datetime.fromisoformat(reference['source_updated_at']))
            if updated>stamp or (cutoff.date()-updated.date()).days>max_age_days:raise ReferenceError('REFERENCE_BINDING_SOURCE_TIME_INVALID')
    records=[r for r in members['records'] if _a_share(r['symbol'])]
    if not records:raise ReferenceError('REFERENCE_NO_A_SHARES')
    return {'theme_id':binding['theme_id'],'source_id':source,'source_board_id':code,'source_board_name':name,
            'records':records,'excluded_non_a_share_symbols':[r['symbol'] for r in members['records'] if not _a_share(r['symbol'])],
            'observed_at':members['observed_at'],'source_updated_at':members['source_updated_at'],
            'source_catalog_hash':catalog['content_hash'],'source_membership_hash':members['content_hash'],
            'execution_scope':'SHADOW','production_publish_forbidden':True}


def resolve_theme_reference(theme_id,bindings,catalogs,memberships,*,now,max_age_days=14):
    """Explicit prioritized failover. Sources are selected, never silently unioned."""
    choices=[b for b in bindings if b.get('theme_id')==theme_id and b.get('approved') is True]
    priorities=[b.get('priority') for b in choices]
    if not choices or any(isinstance(p,bool) or not isinstance(p,int) or p<1 for p in priorities) or len(set(priorities))!=len(priorities):
        raise ReferenceError('REFERENCE_PRIORITY_BINDINGS_INVALID')
    attempts=[]
    for binding in sorted(choices,key=lambda b:b['priority']):
        try:
            catalogs_for_source=[c for c in catalogs if c.get('source_id')==binding['source_id'] and _valid(c)
                                 and (c.get('catalog_scope')==binding['category'] if binding.get('category')
                                      else any(r.get('board_id')==binding['board_id'] for r in c['records']))]
            members_for_source=[m for m in memberships if m.get('source_id')==binding['source_id'] and m.get('board_id')==binding['board_id'] and _valid(m)]
            if not catalogs_for_source or not members_for_source:raise ReferenceError('REFERENCE_SOURCE_NOT_FOUND')
            cutoff=aware(now)
            # Never let a future cache mask an older valid version.
            catalogs_for_source=[c for c in catalogs_for_source if aware(datetime.fromisoformat(c['observed_at']))<=cutoff]
            members_for_source=[m for m in members_for_source if aware(datetime.fromisoformat(m['observed_at']))<=cutoff]
            if not catalogs_for_source or not members_for_source:raise ReferenceError('REFERENCE_SOURCE_TIME_INVALID')
            latest_catalog=max(catalogs_for_source,key=lambda c:c['observed_at'])
            if not any(r.get('board_id')==binding['board_id'] and r.get('name')==binding['board_name'] for r in latest_catalog['records']):
                raise ReferenceError('REFERENCE_BINDING_IDENTITY_MISMATCH')
            member=max(members_for_source,key=lambda m:m['observed_at'])
            if member.get('source_catalog_hash'):
                catalogs_for_source=[c for c in catalogs_for_source if c['content_hash']==member['source_catalog_hash']]
                if not catalogs_for_source:raise ReferenceError('REFERENCE_CATALOG_VERSION_MISMATCH')
            catalog=max(catalogs_for_source,key=lambda c:c['observed_at'])
            projection=project_theme_binding(binding,catalog,member,now=now,max_age_days=max_age_days)
            attempts.append({'source_id':binding['source_id'],'board_id':binding['board_id'],'reason_code':'OK'})
            return {'available':True,'projection':projection,'fallback_reused':len(attempts)>1,'attempts':attempts,
                    'execution_scope':'SHADOW','production_publish_forbidden':True}
        except (ReferenceError,ValueError,TypeError,KeyError) as exc:
            attempts.append({'source_id':binding.get('source_id'),'board_id':binding.get('board_id'),
                             'reason_code':str(exc) if isinstance(exc,ReferenceError) else 'REFERENCE_SOURCE_INVALID'})
    return {'available':False,'reason_code':'REFERENCE_ALL_SOURCES_BLOCKED','attempts':attempts,
            'execution_scope':'SHADOW','production_publish_forbidden':True}
