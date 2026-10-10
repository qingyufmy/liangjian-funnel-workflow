"""Independent shadow rule-version evidence; no production integration or I/O writes.

Only the three approved raw versions can be confirmed, never a new revision.
Same-version observation is not proof of absence of every other revision.
Existing price_limits reviewed-through constants are deliberately untouched.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import date, datetime, time
import hashlib
from html.parser import HTMLParser
from io import BytesIO
import json
import math
from pathlib import Path
import queue
import re
import threading
import time as timer
from zoneinfo import ZoneInfo

from .calendar import ExchangeTradingCalendar
from .shadow_pit_sources import RawResponseReceipt, record_raw_response

SH = ZoneInfo('Asia/Shanghai')
VERSION = 'shadow-rule-preflight/1'
REVIEW_OBSERVED = date(2026, 10, 10)
PLANNED_DUE = date(2026, 10, 16)
EFFECTIVE = '2026-07-06'
APPROVED_RECEIPT_SHA = 'bbcdb3dd7ddf487593d88b89c10a49176907aa837b1c924d6161c0d7b8a8a813'
# Immutable reviewed inputs, not a mutable caller-approved allowlist.
SPECIFICATIONS = (
    ('sse-notice', 'sse-notice.html', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/fund/trading/c/c_20260424_10817739.shtml',
     '12886b912a2160c03c414d2b56ff67c772b18e3247f2c97fc2dfcd185b6e0a33'),
    ('szse-rules', 'szse-rules.pdf', 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf',
     '9b66f8b0db70f84a25ef1ccb4ee2351001724e408117552d75f6d8993483c586'),
    ('szse-notice', 'szse-notice.html', 'https://investor.szse.cn/lawrules/rule/trade/t20260424_620190.html',
     '0dcb0971ac96123f369178ce8f5397d948190d41281175694664de1d1dd4dce5'),
)


class RulePreflightError(ValueError):
    """Stable non-sensitive evidence failure code."""


@dataclass(frozen=True)
class ApprovedRuleDocument:
    document_id: str
    path: str
    url: str
    raw_sha256: str
    raw_response: bytes


@dataclass(frozen=True)
class ApprovedRuleArchive:
    receipt_raw: bytes
    documents: tuple[ApprovedRuleDocument, ...]


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def canonical_receipt_bytes(result):
    """Canonical result payload without its top-level checksum; not a validator."""
    return _canonical({key:value for key,value in result.items() if key != 'receipt_sha256'})


def _sealed(result):
    result['receipt_sha256'] = hashlib.sha256(canonical_receipt_bytes(result)).hexdigest()
    return result


def _stamp(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result.astimezone(SH)
    except (TypeError, ValueError, AttributeError):
        raise RulePreflightError('RULE_CLOCK_UNPROVEN') from None


class _HTMLText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in {'script','style'}:
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in {'script','style'} and self.hidden:
            self.hidden -= 1

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def _parse_document(document_id, raw):
    """Parse actual pinned bytes, using formal effective clause, not meta alone."""
    try:
        if document_id == 'szse-rules':
            from pypdf import PdfReader
            if not raw.startswith(b'%PDF-'):
                raise ValueError
            pdf = PdfReader(BytesIO(raw), strict=True)
            if pdf.is_encrypted or not pdf.pages or len(pdf.pages) > 100:
                raise ValueError
            text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
            title = '深圳证券交易所交易规则'
        else:
            if b'<html' not in raw.lower():
                raise ValueError
            parser = _HTMLText()
            parser.feed(raw.decode('utf-8', errors='strict'))
            parser.close()
            text = ''.join(parser.parts)
            title = '上海证券交易所交易规则' if document_id == 'sse-notice' else '深圳证券交易所交易规则'
        compact = re.sub(r'\s+', '', text)
        if title not in compact or '2026年修订' not in compact:
            raise ValueError
        effective_dates = {date(int(y),int(m),int(d)).isoformat() for y,m,d in
            re.findall(r'自([0-9]{4})年([0-9]{1,2})月([0-9]{1,2})日起施行', compact)}
        if effective_dates != {EFFECTIVE}:
            raise ValueError
        return {'formal_effective_date':EFFECTIVE,
                'effective_date_basis':'FORMAL_NOTICE_OR_RULE_BODY',
                'parsed_text_sha256':hashlib.sha256(text.encode('utf-8')).hexdigest(),
                'metadata_conflicts':['META_NOT_EFFECTIVE_VS_FORMAL_BODY'] if '尚未施行' in compact else []}
    except Exception:
        raise RulePreflightError('RULE_CONTENT_UNPARSEABLE') from None


def _approved(approved):
    if not isinstance(approved, ApprovedRuleArchive):
        raise RulePreflightError('APPROVED_ARCHIVE_UNPROVEN')
    if hashlib.sha256(approved.receipt_raw).hexdigest() != APPROVED_RECEIPT_SHA:
        raise RulePreflightError('APPROVED_ARCHIVE_UNPROVEN')
    try:
        manifest = json.loads(approved.receipt_raw)
        indexed = {row['path']:row for row in manifest['rows']}
        if len(indexed) != 3 or len(manifest['rows']) != 3 or len(approved.documents) != 3:
            raise ValueError
        for doc, spec in zip(approved.documents, SPECIFICATIONS):
            identity, path, url, sha = spec
            if (doc.document_id,doc.path,doc.url,doc.raw_sha256) != spec:
                raise ValueError
            row = indexed[path]
            if (hashlib.sha256(doc.raw_response).hexdigest() != sha or row['url'] != url
                    or row['final_url'] != url or row['sha256'] != sha
                    or row['bytes'] != len(doc.raw_response) or row['http_status'] != 200
                    or row['status'] != 'RAW_RESPONSE_ARCHIVED'
                    or _stamp(row['observed_at']).date() != REVIEW_OBSERVED):
                raise ValueError
    except Exception:
        raise RulePreflightError('APPROVED_ARCHIVE_UNPROVEN') from None
    return approved


def load_approved_rule_archive(root):
    """Read only fixed literal files under caller directory; no approving new bytes."""
    try:
        root = Path(root).resolve(strict=True)
        receipt_path = root / 'receipt.json'
        if receipt_path.resolve(strict=True).parent != root:
            raise ValueError
        raw = receipt_path.read_bytes()
        docs = []
        for identity, path, url, sha in SPECIFICATIONS:
            source = root / path
            if source.resolve(strict=True).parent != root:
                raise ValueError
            docs.append(ApprovedRuleDocument(identity,path,url,sha,source.read_bytes()))
        return _approved(ApprovedRuleArchive(raw, tuple(docs)))
    except Exception:
        raise RulePreflightError('APPROVED_ARCHIVE_UNPROVEN') from None


def _initial(trade_date):
    return dict(schema_version=VERSION, status='DATA_LIMITED', confirmed_trade_date=None,
        requested_trade_date=str(trade_date), rule_review_observed_at=str(REVIEW_OBSERVED),
        planned_revalidation_due=str(PLANNED_DUE), rule_source_effective_date=EFFECTIVE,
        approved_receipt_sha256=APPROVED_RECEIPT_SHA, rows=[], reason_codes=[],
        production_integration='UNWIRED', strict_derived_api_status='RULE_DATE_UNPROVEN',
        rule_constant_modified=False, latest_revision_absence_proven=False,
        future_no_change_proven=False, source_authenticated=False,
        source_requests_started=0, model_calls=0, notification_calls=0,
        receipt_hash_basis='CANONICAL_RESULT_NOT_HTTP_RAW', original_response_hash_basis='HTTP_RESPONSE_CONTENT_BYTES')


def _day_clock(trade_date, known_at):
    try:
        day = date.fromisoformat(trade_date) if isinstance(trade_date,str) else trade_date
        now = _stamp(known_at)
        if type(day) is not date or now.date() != day:
            raise RulePreflightError('RULE_TRADE_DATE_MISMATCH')
        if not REVIEW_OBSERVED <= day <= PLANNED_DUE:
            raise RulePreflightError('RULE_REVALIDATION_PLAN_WINDOW_EXCEEDED')
        if not ExchangeTradingCalendar().is_trading_day(day):
            raise RulePreflightError('RULE_NON_TRADING_DAY')
        if now.time().replace(tzinfo=None) >= time(9,26):
            raise RulePreflightError('RULE_PREFLIGHT_CUTOFF_MISSED')
        return day, now
    except RulePreflightError:
        raise
    except Exception:
        raise RulePreflightError('RULE_TRADE_DATE_OR_CALENDAR_UNPROVEN') from None


def confirm_rule_version(approved, observations, *, trade_date, known_at):
    """Verify exact raw/receipt hashes, parser and same-session arrival clocks.

    Pure result, no archive writes. A status string alone is never accepted.
    """
    result = _initial(trade_date)
    try:
        _approved(approved)
        day, now = _day_clock(trade_date, known_at)
        result['known_at'] = now.isoformat()
        if not isinstance(observations,(list,tuple)) or len(observations) != 3:
            raise RulePreflightError('RULE_THREE_SOURCES_REQUIRED')
        indexed = {}
        for record in observations:
            if not isinstance(record,RawResponseReceipt) or record.endpoint in indexed:
                raise RulePreflightError('RULE_DUPLICATE_OR_UNBOUND_SOURCE')
            indexed[record.endpoint] = record
        for doc in approved.documents:
            record = indexed.get(doc.url)
            if record is None:
                raise RulePreflightError('RULE_SOURCE_SCOPE_MISMATCH')
            # Recompute all raw and metadata checksums; no trusting typed strings.
            frozen = record_raw_response(record.raw_response, source_ref=record.source_ref, endpoint=record.endpoint,
                request_parameters=record.request_parameters, request_options=record.request_options,
                request_started_at=record.request_started_at, response_received_at=record.response_received_at,
                http_status=record.http_status, complete=record.complete, byte_kind=record.byte_kind, clock_basis=record.clock_basis)
            if record.raw_sha256 != frozen.raw_sha256 or record.receipt_sha256 != frozen.receipt_sha256:
                raise RulePreflightError('RULE_RESPONSE_BINDING_MISMATCH')
            audit = dict(document_id=doc.document_id, url=doc.url,
                approved_raw_sha256=doc.raw_sha256, **record.metadata(),
                source_receipt_sha256=record.receipt_sha256, binding_verified=True,
                validation_status='DATA_LIMITED',
                raw_response_base64=base64.b64encode(record.raw_response).decode('ascii'))
            result['rows'].append(audit)
            if record.request_parameters or record.byte_kind != 'HTTP_RESPONSE_CONTENT_BYTES':
                raise RulePreflightError('RULE_HTTP_RAW_SCOPE_UNPROVEN')
            if record.complete is not True or record.http_status != 200:
                raise RulePreflightError('RULE_HTTP_INCOMPLETE')
            start, received = _stamp(record.request_started_at), _stamp(record.response_received_at)
            if start.date() != day or received.date() != day:
                raise RulePreflightError('RULE_ARRIVAL_TRADE_DATE_MISMATCH')
            if not start <= received <= now:
                raise RulePreflightError('RULE_ARRIVAL_CLOCK_UNPROVEN')
            if received.time().replace(tzinfo=None) >= time(9,26):
                raise RulePreflightError('RULE_PREFLIGHT_CUTOFF_MISSED')
            if record.raw_sha256 != doc.raw_sha256:
                raise RulePreflightError('RULE_VERSION_CHANGED_NO_AUTO_ACCEPT')
            try:
                parsed = _parse_document(doc.document_id, record.raw_response)
            except Exception:
                raise RulePreflightError('RULE_CONTENT_UNPARSEABLE') from None
            audit.update(parsed, validation_status='RULE_VERSION_CONFIRMED')
        result.update(status='RULE_VERSION_CONFIRMED', confirmed_trade_date=str(day))
    except RulePreflightError as exc:
        result['reason_codes'] = [str(exc)]
    except Exception:
        result['reason_codes'] = ['RULE_RESPONSE_EVIDENCE_UNPROVEN']
    return _sealed(result)


def fetch_rule_preflight(approved, *, trade_date, fetch_response=None, clock=None, budget_seconds=10):
    """Independent premarket injection, at most three calls, bounded caller wait.

    No default HTTP client/retry/scheduler. An uncooperative transport may keep
    its daemon request alive after timeout; it cannot issue subsequent calls,
    write artifacts or publish a late result. Caller transport must honor the
    remaining timeout. This is not a 09:26 production-budget proof.
    """
    initial = _initial(trade_date)
    wall = clock or (lambda: datetime.now(SH))
    try:
        if (isinstance(budget_seconds,bool) or not isinstance(budget_seconds,(int,float))
                or not math.isfinite(budget_seconds) or not 0 < budget_seconds <= 30):
            raise RulePreflightError('RULE_PREFLIGHT_BUDGET_INVALID')
        _approved(approved)
        _day_clock(trade_date, wall())
        if fetch_response is None:
            raise RulePreflightError('HTTP_UNWIRED')
    except RulePreflightError as exc:
        initial['reason_codes'] = [str(exc)]
        return _sealed(initial)
    except Exception:
        initial['reason_codes'] = ['RULE_CLOCK_UNPROVEN']
        return _sealed(initial)
    deadline = timer.monotonic() + budget_seconds
    mailbox = queue.Queue()
    stopped = threading.Event()
    count = [0]

    def worker():
        for doc in approved.documents:
            remaining = deadline - timer.monotonic()
            if stopped.is_set() or remaining <= 0:
                return
            try:
                started = _stamp(wall())
                # Injected clocks may block: recompute after the call, never
                # start another HTTP request from a pre-clock stale budget.
                remaining = deadline - timer.monotonic()
                if stopped.is_set() or remaining <= 0:
                    return
                count[0] += 1
                response = fetch_response(doc.url, remaining)
                received = _stamp(wall())
                if stopped.is_set() or timer.monotonic() >= deadline:
                    return
                if response.url != doc.url:
                    raise RulePreflightError('RULE_HTTP_REDIRECT_UNPROVEN')
                record = record_raw_response(response.content, source_ref='official-rule:'+doc.document_id,
                    endpoint=doc.url, request_parameters={}, request_options={'timeout_argument':remaining},
                    request_started_at=started,response_received_at=received,http_status=response.status_code,
                    byte_kind='HTTP_RESPONSE_CONTENT_BYTES',
                    clock_basis='INJECTED_CLOCK_NOT_AUTHENTICATED' if clock else 'PROCESS_WALL_CLOCK')
                mailbox.put((timer.monotonic(),record,None))
            except Exception as exc:
                code = str(exc) if isinstance(exc,RulePreflightError) else 'RULE_HTTP_FAILED'
                mailbox.put((timer.monotonic(),None,code))
                return

    try:
        threading.Thread(target=worker, name='shadow-rule-preflight', daemon=True).start()
    except Exception:
        stopped.set()
        initial.update(reason_codes=['RULE_PREFLIGHT_WORKER_START_FAILED'],source_requests_started=count[0])
        return _sealed(initial)
    records, error = [], None
    while len(records) < 3:
        remaining = deadline - timer.monotonic()
        if remaining <= 0:
            error = 'RULE_PREFLIGHT_BUDGET_EXCEEDED'
            break
        try:
            arrival, record, error = mailbox.get(timeout=remaining)
        except queue.Empty:
            error = 'RULE_PREFLIGHT_BUDGET_EXCEEDED'
            break
        if arrival >= deadline:
            error = 'RULE_PREFLIGHT_BUDGET_EXCEEDED'
        if error:
            break
        records.append(record)
    stopped.set()
    if error:
        initial.update(reason_codes=[error], source_requests_started=count[0])
        return _sealed(initial)
    try:
        known = wall()
    except Exception:
        initial.update(reason_codes=['RULE_CLOCK_UNPROVEN'],source_requests_started=count[0])
        return _sealed(initial)
    result = confirm_rule_version(approved, records, trade_date=trade_date, known_at=known)
    if timer.monotonic() >= deadline:
        initial.update(reason_codes=['RULE_PREFLIGHT_BUDGET_EXCEEDED'], source_requests_started=count[0])
        return _sealed(initial)
    result['source_requests_started'] = count[0]
    return _sealed(result)


__all__ = ['load_approved_rule_archive','confirm_rule_version','fetch_rule_preflight',
           'canonical_receipt_bytes','ApprovedRuleArchive','ApprovedRuleDocument','RulePreflightError']
