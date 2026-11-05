"""T2-1124 source-only HTTP transport with durable cumulative reservations.

No global monkeypatch, model endpoint or retry is installed in live mode.
Controlled-model compatibility is restricted to an explicit MockTransport.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import subprocess
from zoneinfo import ZoneInfo

import httpx

from .dk_sources import EASTMONEY_PARAMS, TENCENT_PARAMS

TZ = ZoneInfo('Asia/Shanghai')
LIMITS = {'EASTMONEY': 5, 'TENCENT': 20, 'CONTROLLED_MODEL': 1}
NOT_BEFORE = datetime(2026, 10, 29, 17, tzinfo=TZ)
SOURCE_DEADLINE = datetime(2026, 10, 29, 23, 45, tzinfo=TZ)
REQUIRED_PREFLIGHT_CHECKS = frozenset({'SOURCE_SEND_MODES', 'MODEL_STREAM_SUCCESS', 'MODEL_STREAM_FAILURE',
    'URL_NEGATIVES', 'REDIRECT_REJECTED', 'BUDGET_RESTART', 'RATE_AND_WINDOW', 'NO_RETRY',
    'TRANSPORT_SPACING_AND_TERMINAL'})


def preflight_hashes(root):
    root = Path(root)
    files = ('src/liangjian_funnel/market_state/research_http.py',
             'src/liangjian_funnel/market_state/dk_sources.py',
             'src/liangjian_funnel/market_state/bars15m.py',
             'src/liangjian_funnel/pipeline/model_client.py', 'tests/test_track2_http_preflight.py',
             'scripts/track2_collect_dk1.py')
    return {name: hashlib.sha256((root/name).read_bytes()).hexdigest() for name in files}


def source_head(root):
    return subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'],
                          check=True, capture_output=True, text=True).stdout.strip()


def verify_preflight(path, root):
    proof = json.loads(Path(path).read_bytes())
    if (proof.get('schema_version') != 'track2-http-offline-preflight/1' or proof.get('status') != 'PASSED'
            or set(proof.get('checks', [])) != REQUIRED_PREFLIGHT_CHECKS
            or proof.get('source_hashes') != preflight_hashes(root) or proof.get('head') != source_head(root)
            or proof.get('actual_source_http') != 0 or proof.get('actual_model_http') != 0
            or proof.get('real_socket_forbidden') is not True):
        raise ValueError('T2_PREFLIGHT_MISSING_STALE_OR_FAILED')
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class ProbeLedger:
    def __init__(self, path: Path, *, clock=lambda: datetime.now(TZ), sleep=time.sleep):
        self.path = Path(path)
        if any(p.is_symlink() for p in (self.path, *self.path.parents)):
            raise ValueError('T2_LEDGER_SYMLINK_FORBIDDEN')
        self.clock, self.sleep = clock, sleep
        self.thread = threading.get_ident()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def locked(self):
        if threading.get_ident() != self.thread:
            raise ValueError('T2_SINGLE_THREAD_REQUIRED')
        lock = self.path.with_suffix(self.path.suffix + '.lock')
        fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            yield
        finally:
            os.close(fd)
            lock.unlink()

    def read(self):
        rows, previous = [], '0' * 64
        if self.path.exists():
            raw = self.path.read_bytes()
            if raw and not raw.endswith(b'\n'):
                raise ValueError('T2_LEDGER_CHAIN_INVALID')
            for line in raw.splitlines():
                row = json.loads(line)
                core = {key: row[key] for key in ('seq', 'previous_sha256', 'event')}
                if (_json(row).encode('utf-8') != line
                        or row['seq'] != len(rows)+1 or row['previous_sha256'] != previous
                        or hashlib.sha256(_json(core).encode('utf-8')).hexdigest() != row['sha256']):
                    raise ValueError('T2_LEDGER_CHAIN_INVALID')
                previous = row['sha256']
                rows.append(row)
        return rows

    def _append(self, event, rows):
        core = dict(seq=len(rows)+1, previous_sha256=rows[-1]['sha256'] if rows else '0'*64, event=event)
        row = {**core, 'sha256': hashlib.sha256(_json(core).encode('utf-8')).hexdigest()}
        with self.path.open('ab') as output:
            output.write((_json(row)+'\n').encode('utf-8'))
            output.flush()
            os.fsync(output.fileno())
        return row

    def event(self, event):
        with self.locked():
            return self._append(dict(event, at=self.clock().isoformat()), self.read())

    def reserve(self, source, *, request_key=None):
        if source not in LIMITS:
            raise ValueError('T2_SOURCE_NOT_ALLOWED')
        with self.locked():
            now = self.clock()
            if not isinstance(now, datetime) or now.utcoffset() is None:
                raise ValueError('T2_CLOCK_INVALID')
            if not NOT_BEFORE <= now < SOURCE_DEADLINE:
                raise ValueError('T2_SOURCE_TIME_WINDOW_CLOSED')
            rows = self.read()
            attempts = [row['event'] for row in rows if row['event']['kind'] == 'RESERVED']
            if sum(event['source'] == source for event in attempts) >= LIMITS[source]:
                raise ValueError('T2_SOURCE_BUDGET_EXHAUSTED')
            if request_key is not None and any(event.get('request_key') == request_key for event in attempts):
                raise ValueError('T2_DUPLICATE_REQUEST_NO_RETRY')
            if attempts:
                # Reservation timestamps precede fsync and delegate entry; their
                # gaps cannot prove actual send spacing. Wait a full second
                # after the previous call's terminal event instead. That event
                # is necessarily later than entry, including across restarts.
                reference = datetime.fromisoformat(attempts[-1]['at'])
                invoked = [row['event'] for row in rows if row['event']['kind'] == 'TRANSPORT_INVOKED']
                if invoked:
                    previous = invoked[-1]['reservation']
                    terminal = [row['event'] for row in rows
                                if row['event'].get('reservation') == previous
                                and row['event']['kind'] in {'STREAM_CLOSED', 'TRANSPORT_EXCEPTION', 'REDIRECT_REJECTED'}]
                    if not terminal:
                        raise ValueError('T2_PREVIOUS_REQUEST_NOT_TERMINAL')
                    reference = max(reference, datetime.fromisoformat(terminal[-1]['at']))
                gap = (now-reference).total_seconds()
                if gap < 0:
                    raise ValueError('T2_CLOCK_MOVED_BACKWARDS')
                if gap < 1:
                    self.sleep(1-gap)
                    now = self.clock()
                    if ((now-reference).total_seconds() < 1
                            or not NOT_BEFORE <= now < SOURCE_DEADLINE):
                        raise ValueError('T2_INTERVAL_OR_DEADLINE_NOT_SATISFIED')
            return self._append(dict(kind='RESERVED', source=source, request_key=request_key,
                                     at=now.isoformat()), rows)['seq']

    def summary(self):
        rows = self.read()
        return {source: sum(row['event']['kind'] == 'RESERVED' and row['event']['source'] == source
                            for row in rows) for source in LIMITS}


class _ObservedStream(httpx.SyncByteStream):
    def __init__(self, stream, ledger, source, reservation, *, max_bytes=8*1024*1024):
        self.stream, self.ledger, self.source, self.reservation = stream, ledger, source, reservation
        self.bytes, self.complete, self.closed, self.max_bytes = 0, False, False, max_bytes

    def __iter__(self):
        for chunk in self.stream:
            self.bytes += len(chunk)
            if self.bytes > self.max_bytes:
                raise ValueError('T2_RESPONSE_TOO_LARGE')
            yield chunk
        self.complete = True

    def close(self):
        if self.closed:
            return
        try:
            self.stream.close()
        finally:
            self.closed = True
            self.ledger.event(dict(kind='STREAM_CLOSED', source=self.source, reservation=self.reservation,
                                   bytes=self.bytes, fully_read=self.complete))


class AuditedTransport(httpx.BaseTransport):
    def __init__(self, delegate: httpx.BaseTransport, ledger: ProbeLedger, *, controlled_model=False):
        if controlled_model and not isinstance(delegate, httpx.MockTransport):
            raise ValueError('T2_CONTROLLED_MODEL_REQUIRES_MOCK_TRANSPORT')
        self.delegate, self.ledger, self.controlled_model = delegate, ledger, controlled_model

    def _source(self, request):
        url = request.url
        if (url.scheme != 'https' or url.port not in (None, 443) or url.username or url.password or url.fragment):
            raise ValueError('T2_URL_SCOPE_DENIED')
        pairs = list(url.params.multi_items())
        query = dict(pairs)
        if len(query) != len(pairs):
            raise ValueError('T2_DUPLICATE_QUERY_PARAMETER')
        if request.method == 'GET' and not request.content:
            if (url.host == 'push2his.eastmoney.com' and url.path == '/api/qt/stock/kline/get'
                    and query == EASTMONEY_PARAMS):
                return 'EASTMONEY'
            if (url.host == 'ifzq.gtimg.cn' and url.path == '/appstock/app/kline/mkline'
                    and query == TENCENT_PARAMS):
                return 'TENCENT'
        if (self.controlled_model and url.host == 'ai-api.finpoints.tech'
                and url.path == '/v1/chat/completions' and request.method == 'POST' and not pairs):
            return 'CONTROLLED_MODEL'
        raise ValueError('T2_URL_SCOPE_DENIED')

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        source = self._source(request)
        request_key = hashlib.sha256(_json(dict(method=request.method, host=request.url.host,
            path=request.url.path, query=dict(request.url.params),
            body_sha256=hashlib.sha256(request.content).hexdigest())).encode('utf-8')).hexdigest()
        reservation = self.ledger.reserve(source, request_key=request_key)
        self.ledger.event(dict(kind='TRANSPORT_INVOKED', source=source, reservation=reservation))
        try:
            response = self.delegate.handle_request(request)
        except BaseException as exc:
            self.ledger.event(dict(kind='TRANSPORT_EXCEPTION', source=source, reservation=reservation,
                                   exception_type=type(exc).__name__))
            raise
        self.ledger.event(dict(kind='RESPONSE_HEADERS', source=source, reservation=reservation,
                               status_code=response.status_code))
        if 300 <= response.status_code < 400:
            response.close()
            self.ledger.event(dict(kind='REDIRECT_REJECTED', source=source, reservation=reservation))
            raise ValueError('T2_REDIRECT_DENIED')
        response.stream = _ObservedStream(response.stream, self.ledger, source, reservation)
        return response

    def close(self):
        self.delegate.close()
