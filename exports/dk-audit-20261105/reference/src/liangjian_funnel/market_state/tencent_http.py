"""M28 Tencent-only bounded collection, preserving the retired DK1 ledger.

Exact request plans, durable pre-send accounting and terminal+1s spacing.
The caller must provide fresh track-one clearance before every live send.
No module-global transport patch, credentials, retries or TLS exceptions.
"""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import time

import httpx

from .research_http import ProbeLedger, _ObservedStream, _json
from .bars15m import TZ


class TencentLedger(ProbeLedger):
    def __init__(self, path, *, parent_path, clock=lambda: datetime.now(TZ), sleep=time.sleep):
        super().__init__(Path(path), clock=clock, sleep=sleep)
        self.parent_path = Path(parent_path)
        if any(p.is_symlink() for p in (self.parent_path, *self.parent_path.parents)):
            raise ValueError('T2_PARENT_LEDGER_INVALID')
        parent = ProbeLedger(self.parent_path).read()
        if not self.parent_path.is_file() or not parent:
            raise ValueError('T2_PARENT_LEDGER_REQUIRED')
        counts = {name: sum(r['event']['kind'] == 'RESERVED' and r['event']['source'] == name for r in parent)
                  for name in ('TENCENT', 'EASTMONEY')}
        identity = dict(kind='M28_ORIGIN', parent_sha256=hashlib.sha256(self.parent_path.read_bytes()).hexdigest(),
            prior_dk_tencent=counts['TENCENT'], prior_em_retired=counts['EASTMONEY'],
            dk_limit=20, mkt_daily_limit=30, policy='CLAUDE_M28_20261030')
        with self.locked():
            rows = self.read()
            if not rows:
                self._append(identity, rows)
            elif rows[0]['event'] != identity:
                raise ValueError('T2_PARENT_LEDGER_CHANGED_OR_POLICY_CONFLICT')

    def summary(self):
        rows = self.read()
        origin = rows[0]['event']
        day = self.clock().astimezone(TZ).date().isoformat()
        attempts = [r['event'] for r in rows if r['event']['kind'] == 'RESERVED']
        return dict(dk_tencent=origin['prior_dk_tencent']+sum(r['source'] == 'DK_TENCENT' for r in attempts),
            em_retired=origin['prior_em_retired'],
            mkt_today=sum(r['source'] == 'MKT_TENCENT' and r['at'][:10] == day for r in attempts),
            controlled_model=sum(r['source'] == 'CONTROLLED_MODEL' for r in attempts))

    def reserve(self, source, *, request_key=None):
        if source not in {'MKT_TENCENT', 'DK_TENCENT', 'CONTROLLED_MODEL'}:
            raise ValueError('T2_TENCENT_ONLY')
        with self.locked():
            rows = self.read()
            resolved={r['event']['resolved_failure_seq'] for r in rows if r['event']['kind']=='OPS_PARSER_FIX_VERIFIED'}
            if any(r['event']['kind'] in {'SOURCE_HTTP_FAILED', 'SOURCE_TRANSPORT_FAILED'}
                    or r['event']['kind']=='SOURCE_BUSINESS_FAILED' and r['seq'] not in resolved for r in rows):
                raise ValueError('T2_COLLECTION_STOPPED_AFTER_FAILURE')
            counts = self.summary()
            if ((source == 'DK_TENCENT' and counts['dk_tencent'] >= 20)
                    or (source == 'MKT_TENCENT' and counts['mkt_today'] >= 30)
                    or (source == 'CONTROLLED_MODEL' and counts['controlled_model'] >= 1)):
                raise ValueError('T2_SOURCE_BUDGET_EXHAUSTED')
            previous = [r['event'] for r in rows if r['event']['kind'] == 'RESERVED']
            if any(r.get('request_key') == request_key for r in previous):
                raise ValueError('T2_DUPLICATE_REQUEST_NO_RETRY')
            invoked = [r['event'] for r in rows if r['event']['kind'] == 'TRANSPORT_INVOKED']
            now = self.clock()
            if not isinstance(now, datetime) or now.utcoffset() is None:
                raise ValueError('T2_CLOCK_INVALID')
            if invoked:
                terminal = [r['event'] for r in rows if r['event'].get('reservation') == invoked[-1]['reservation']
                    and r['event']['kind'] in {'STREAM_CLOSED', 'TRANSPORT_EXCEPTION', 'REDIRECT_REJECTED'}]
                if not terminal:
                    raise ValueError('T2_PREVIOUS_REQUEST_NOT_TERMINAL')
                reference = datetime.fromisoformat(terminal[-1]['at'])
                gap = (now-reference).total_seconds()
                if gap < 0:
                    raise ValueError('T2_CLOCK_MOVED_BACKWARDS')
                if gap < 1:
                    self.sleep(1-gap)
                    now = self.clock()
                    if (now-reference).total_seconds() < 1:
                        raise ValueError('T2_SEND_SPACING_FAILED')
            return self._append(dict(kind='RESERVED', source=source, request_key=request_key, at=now.isoformat()), rows)['seq']

    def verify_321_parser_fix(self, report_path, *, config_dir, preflight_sha256):
        """Resolve only the proven321-row OPS bound error, without refetching.

        Every old failure/counter stays. Other HTTP/transport/business failures
        remain stopped; the original request fingerprint remains non-repeatable.
        """
        from .calendar import market_calendar
        from .config import load_market_definitions
        from .index_trend import index_close_facts
        from .tencent_daily import decode_payload,parse_daily
        import re
        report_path=Path(report_path)
        if (not report_path.resolve().is_relative_to(self.path.parent.resolve())
                or any(p.is_symlink() for p in (report_path,*report_path.parents))
                or re.fullmatch('[0-9a-f]{64}',preflight_sha256) is None):
            raise ValueError('T2_PARSER_FIX_PROOF_SCOPE_INVALID')
        report_bytes=report_path.read_bytes();report=json.loads(report_bytes)
        if (report.get('failure',{}).get('reason')!='INDEX_UNADJUSTED_DAY_ROWS_REQUIRED'
                or report.get('status')!='FAILED_STOPPED_NO_RETRY' or len(report.get('attempts',[]))!=1):
            raise ValueError('T2_PARSER_FIX_NOT_THIS_FAILURE')
        attempt=report['attempts'][0];key=attempt['key']
        definitions=load_market_definitions(config_dir)
        if key not in definitions['registry']['indices'] or attempt['kind']!='DAILY' or attempt['status_code']!=200:
            raise ValueError('T2_PARSER_FIX_NOT_THIS_FAILURE')
        raw_path=report_path.parent/(key+'-daily.body')
        if raw_path.is_symlink():raise ValueError('T2_PARSER_FIX_PROOF_SCOPE_INVALID')
        raw=raw_path.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=attempt['input_sha256']:raise ValueError('T2_PARSER_FIX_INPUT_CHANGED')
        payload,_=decode_payload(raw)
        data=parse_daily(payload,registry=definitions['registry'],key=key,
            trade_date=datetime.strptime(report['trade_date'],'%Y-%m-%d').date(),input_sha256=attempt['input_sha256'],
            capture_started_at=attempt['capture_started_at'],capture_completed_at=attempt['capture_completed_at'])
        facts=index_close_facts(key,data,definitions=definitions,calendar=market_calendar())
        if data['returned_row_count']!=321 or facts['status']!='READY':raise ValueError('T2_PARSER_FIX_NOT_VERIFIED')
        with self.locked():
            rows=self.read();failed=[r for r in rows if r['event']['kind']=='SOURCE_BUSINESS_FAILED']
            if not failed or any(r['event']['kind']=='OPS_PARSER_FIX_VERIFIED' and r['event']['resolved_failure_seq']==failed[-1]['seq'] for r in rows):
                raise ValueError('T2_PARSER_FIX_ALREADY_RESOLVED_OR_NO_FAILURE')
            failed_at=datetime.fromisoformat(failed[-1]['event']['at'])
            if not datetime.fromisoformat(report['started_at'])<=failed_at<=datetime.fromisoformat(report['finished_at']):
                raise ValueError('T2_PARSER_FIX_WRONG_FAILURE_BINDING')
            self._append(dict(kind='OPS_PARSER_FIX_VERIFIED',resolved_failure_seq=failed[-1]['seq'],
                source_report_sha256=hashlib.sha256(report_bytes).hexdigest(),raw_input_sha256=attempt['input_sha256'],
                preflight_sha256=preflight_sha256,policy='LOCAL_321_ROW_BOUND_FIX_NO_REFETCH',at=self.clock().isoformat()),rows)
        return dict(schema_version='track2-parser-fix/1',status='VERIFIED_WITHOUT_REFETCH',
            source_report_sha256=hashlib.sha256(report_bytes).hexdigest(),raw_input_sha256=attempt['input_sha256'],
            original_failure_preserved=True,counts=self.summary(),normalized=data,facts=facts)


def request_identity(method, url, params=None):
    value = httpx.URL(url) if params is None else httpx.URL(url, params=params)
    return (method, value.scheme, value.host, value.port, value.path, tuple(sorted(value.params.multi_items())))


class TencentTransport(httpx.BaseTransport):
    def __init__(self, delegate, ledger, *, plan, clearance, trade_date, controlled_model=False):
        if controlled_model and not isinstance(delegate, httpx.MockTransport):
            raise ValueError('T2_CONTROLLED_MODEL_REQUIRES_MOCK_TRANSPORT')
        self.delegate, self.ledger, self.clearance, self.controlled_model = delegate, ledger, clearance, controlled_model
        datetime.strptime(trade_date, '%Y-%m-%d')
        self.trade_date = trade_date
        self.plan = {}
        for item in plan:
            identity = request_identity('GET', item['url'], item['params'])
            _, scheme, host, port, path, _ = identity
            if (scheme != 'https' or port not in (None, 443) or item['source'] not in {'MKT_TENCENT', 'DK_TENCENT'}
                    or (host, path) not in {
                        ('proxy.finance.qq.com', '/ifzqgtimg/appstock/app/newfqkline/get'),
                        ('ifzq.gtimg.cn', '/appstock/app/kline/mkline'), ('qt.gtimg.cn', '/q')}):
                raise ValueError('T2_REQUEST_PLAN_INVALID')
            if identity in self.plan:
                raise ValueError('T2_REQUEST_PLAN_DUPLICATE')
            self.plan[identity] = item['source']

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        if (url.scheme != 'https' or url.port not in (None, 443) or url.username or url.password or url.fragment
                or len(list(url.params.multi_items())) != len(dict(url.params))):
            raise ValueError('T2_URL_SCOPE_DENIED')
        identity = request_identity(request.method, str(url))
        source = self.plan.get(identity)
        if (self.controlled_model and request.method == 'POST' and url.host == 'ai-api.finpoints.tech'
                and url.path == '/v1/chat/completions' and not url.params):
            source = 'CONTROLLED_MODEL'
        if source is None or source != 'CONTROLLED_MODEL' and (request.method != 'GET' or request.content):
            raise ValueError('T2_URL_SCOPE_DENIED')
        self.clearance()  # executed before reserving AND again after any wait
        key = hashlib.sha256(_json(dict(identity=identity, collection_trade_date=self.trade_date,
            body_sha256=hashlib.sha256(request.content).hexdigest())).encode()).hexdigest()
        reservation = self.ledger.reserve(source, request_key=key)
        self.clearance()
        self.ledger.event(dict(kind='TRANSPORT_INVOKED', source=source, reservation=reservation))
        try:
            response = self.delegate.handle_request(request)
        except BaseException as exc:
            self.ledger.event(dict(kind='TRANSPORT_EXCEPTION', source=source, reservation=reservation,
                exception_type=type(exc).__name__))
            self.ledger.event(dict(kind='SOURCE_TRANSPORT_FAILED', source=source, reservation=reservation))
            raise
        self.ledger.event(dict(kind='RESPONSE_HEADERS', source=source, reservation=reservation, status_code=response.status_code))
        if 300 <= response.status_code < 400:
            response.close()
            self.ledger.event(dict(kind='REDIRECT_REJECTED', source=source, reservation=reservation))
            self.ledger.event(dict(kind='SOURCE_HTTP_FAILED', source=source, reservation=reservation, status_code=response.status_code))
            raise ValueError('T2_REDIRECT_DENIED')
        response.stream = _ObservedStream(response.stream, self.ledger, source, reservation)
        if response.status_code >= 400:
            self.ledger.event(dict(kind='SOURCE_HTTP_FAILED', source=source, reservation=reservation, status_code=response.status_code))
        return response

    def close(self):
        self.delegate.close()


def live_clearance(path, *, clock=lambda: datetime.now(TZ)):
    """A fresh T1 board readback must prove the WHOLE workflow is stopped."""
    path = Path(path)
    if not path.is_file() or any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('T2_CHAIN_CLEARANCE_MISSING')
    data = json.loads(path.read_bytes())
    stamp = datetime.fromisoformat(data['observed_at'])
    if (data.get('schema_version') != 't2-track1-clearance/1' or data.get('task_id') != 'liangjian#T1'
            or data.get('status') != 'TERMINAL_NO_WORKFLOW_RUNNING' or data.get('message_kind') not in {'note', 'decision'}
            or not str(data.get('message_id', '')).startswith('liangjian#M')
            or stamp.utcoffset() is None or not 0 <= (clock()-stamp).total_seconds() <= 120):
        raise ValueError('T2_CHAIN_RUNNING_OR_CLEARANCE_STALE')
    return data
