"""Same-response byte packaging for standalone shadow PIT; default UNWIRED.

No requests/client construction, retry, limiter, DB, model or notification.
HTTP response.content, decoded text and composite consumer JSON have distinct
hash bases. Caller provenance declarations are not HTTP/source authentication.
"""
from __future__ import annotations

import base64
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from ..data.tencent_minute import TencentIntradayAdapter, TencentMarketDataError
from ..evaluation.ablation.price_limits import RULE_FROM, resolve_price_limits
from .calendar import ExchangeTradingCalendar
from .shadow_pit_capture import FIELDS, PITSourceReceipt, REQUIRED_IDENTITY

SH = ZoneInfo('Asia/Shanghai')
VERSION = 'shadow-pit-source-package/1'
SYMBOL = re.compile(r'[0-9]{6}\.(SH|SZ|BJ)')
HASH = re.compile(r'[a-f0-9]{64}')
FORBIDDEN = re.compile(r'(?i)api[_-]?key|token|secret|password|bearer|authorization|cookie')


class SourceEvidenceError(ValueError):
    """Stable public code; never provider exception/body/credential text."""


def _stamp(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result.astimezone(SH)
    except (ValueError, TypeError, AttributeError):
        raise SourceEvidenceError('SOURCE_CLOCK_UNPROVEN') from None


def _canonical(value):
    def keys(body):
        if isinstance(body, Mapping):
            if any(not isinstance(key, str) for key in body):
                raise SourceEvidenceError('SOURCE_JSON_KEYS_INVALID')
            for item in body.values():
                keys(item)
        elif isinstance(body, (list, tuple)):
            for item in body:
                keys(item)
    try:
        keys(value)
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise SourceEvidenceError('SOURCE_JSON_INVALID') from None


def _json(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise SourceEvidenceError('SOURCE_JSON_DUPLICATE_KEY')
            result[key] = value
        return result
    try:
        result = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(SourceEvidenceError('SOURCE_JSON_NONFINITE')))
        if not isinstance(result, dict):
            raise SourceEvidenceError('SOURCE_JSON_OBJECT_REQUIRED')
        return result
    except (TypeError, ValueError, UnicodeError):
        raise SourceEvidenceError('SOURCE_JSON_INVALID') from None


def _safe_ref(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9:/._ -]{1,512}', value) or FORBIDDEN.search(value):
        raise SourceEvidenceError('SOURCE_REFERENCE_UNSAFE')
    return value


def _parameters(value):
    if not isinstance(value, dict):
        raise SourceEvidenceError('REQUEST_PARAMETERS_INVALID')
    def check(body):
        if isinstance(body, dict):
            for key, item in body.items():
                if not isinstance(key, str) or FORBIDDEN.search(key):
                    raise SourceEvidenceError('REQUEST_PARAMETERS_UNSAFE')
                check(item)
        elif isinstance(body, list):
            for item in body:
                check(item)
    check(value)
    return _json(_canonical(value))


@dataclass(frozen=True)
class RawResponseReceipt:
    source_ref: str
    endpoint: str
    request_parameters: dict
    request_options: dict
    request_started_at: str
    response_received_at: str
    raw_response: bytes
    raw_sha256: str
    http_status: int
    complete: bool
    byte_kind: str
    clock_basis: str
    receipt_sha256: str

    def metadata(self):
        return dict(schema_version=VERSION, source_ref=self.source_ref, endpoint=self.endpoint,
                    request_parameters=_parameters(self.request_parameters), request_options=_parameters(self.request_options),
                    request_started_at=self.request_started_at,
                    response_received_at=self.response_received_at, raw_sha256=self.raw_sha256,
                    raw_size=len(self.raw_response), http_status=self.http_status, complete=self.complete,
                    byte_kind=self.byte_kind, clock_basis=self.clock_basis, source_authenticated=False)


def record_raw_response(raw_response, *, source_ref, endpoint, request_parameters,
                        request_started_at, response_received_at, http_status=200,
                        complete=True, byte_kind='CONSUMER_INPUT_BYTES',
                        clock_basis='CALLER_SUPPLIED_NOT_AUTHENTICATED', request_options=None):
    """Freeze supplied bytes; no text re-encoding may claim HTTP bytes."""
    if not isinstance(raw_response, bytes) or not raw_response or len(raw_response) > 16 * 1024 * 1024:
        raise SourceEvidenceError('RAW_RESPONSE_BYTES_REQUIRED')
    if type(http_status) is not int or not 100 <= http_status <= 599 or type(complete) is not bool:
        raise SourceEvidenceError('SOURCE_TRANSPORT_STATUS_INVALID')
    if byte_kind not in {'HTTP_RESPONSE_CONTENT_BYTES', 'CONSUMER_INPUT_BYTES', 'ORIGINAL_PACKAGE_BYTES'}:
        raise SourceEvidenceError('SOURCE_BYTE_KIND_UNPROVEN')
    if clock_basis not in {'PROCESS_WALL_CLOCK', 'INJECTED_CLOCK_NOT_AUTHENTICATED', 'CALLER_SUPPLIED_NOT_AUTHENTICATED'}:
        raise SourceEvidenceError('SOURCE_CLOCK_BASIS_UNPROVEN')
    start, received = _stamp(request_started_at), _stamp(response_received_at)
    if start > received:
        raise SourceEvidenceError('SOURCE_CLOCK_ORDER_INVALID')
    result = RawResponseReceipt(_safe_ref(source_ref), _safe_ref(endpoint), _parameters(request_parameters),
        _parameters(request_options if request_options is not None else {}),
        start.isoformat(), received.isoformat(), raw_response, hashlib.sha256(raw_response).hexdigest(),
        http_status, complete, byte_kind, clock_basis, '')
    return RawResponseReceipt(**{**result.__dict__, 'receipt_sha256': hashlib.sha256(_canonical(result.metadata())).hexdigest()})


def _validate(receipt, *, observed_at=None):
    if not isinstance(receipt, RawResponseReceipt):
        raise SourceEvidenceError('RAW_RECEIPT_UNPROVEN')
    expected = record_raw_response(receipt.raw_response, source_ref=receipt.source_ref, endpoint=receipt.endpoint,
        request_parameters=receipt.request_parameters, request_started_at=receipt.request_started_at,
        response_received_at=receipt.response_received_at, http_status=receipt.http_status,
        complete=receipt.complete, byte_kind=receipt.byte_kind, clock_basis=receipt.clock_basis,
        request_options=receipt.request_options)
    if receipt.raw_sha256 != expected.raw_sha256 or receipt.receipt_sha256 != expected.receipt_sha256:
        raise SourceEvidenceError('SOURCE_HASH_MISMATCH')
    if observed_at is not None and _stamp(receipt.response_received_at) > _stamp(observed_at):
        raise SourceEvidenceError('SOURCE_FUTURE')
    if receipt.complete is not True or not 200 <= receipt.http_status < 300:
        raise SourceEvidenceError('SOURCE_PARTIAL')
    return receipt


class RawReceiptArchive:
    """New isolated file directory, immutable receipt folders, no database."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=False)

    def load(self, receipt_sha256):
        if not isinstance(receipt_sha256, str) or not HASH.fullmatch(receipt_sha256):
            raise SourceEvidenceError('SOURCE_ARCHIVE_ID_INVALID')
        folder = self.root / receipt_sha256
        try:
            raw = (folder / 'response.bin').read_bytes()
            meta = _json((folder / 'receipt.json').read_bytes())
            record = record_raw_response(raw, **{key: meta[key] for key in (
                'source_ref','endpoint','request_parameters','request_options','request_started_at','response_received_at',
                'http_status','complete','byte_kind','clock_basis')})
            if record.receipt_sha256 != receipt_sha256 or record.metadata() != meta:
                raise SourceEvidenceError('SOURCE_ARCHIVE_CONFLICT')
            return record
        except (OSError, KeyError, SourceEvidenceError):
            raise SourceEvidenceError('SOURCE_ARCHIVE_CONFLICT') from None

    def store(self, record):
        _validate(record)
        destination = self.root / record.receipt_sha256
        if destination.exists():
            if self.load(record.receipt_sha256) != record:
                raise SourceEvidenceError('SOURCE_ARCHIVE_CONFLICT')
            return record.receipt_sha256
        stage = Path(tempfile.mkdtemp(prefix='pending-', dir=self.root))
        try:
            for name, content in [('response.bin', record.raw_response), ('receipt.json', _canonical(record.metadata()))]:
                with (stage / name).open('xb') as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
            try:
                stage.rename(destination)
            except FileExistsError:
                if self.load(record.receipt_sha256) != record:
                    raise SourceEvidenceError('SOURCE_ARCHIVE_CONFLICT')
        finally:
            # Only this invocation's two named temporary files; no recursive cleanup.
            if stage.exists():
                for name in ('response.bin', 'receipt.json'):
                    (stage / name).unlink(missing_ok=True)
                stage.rmdir()
        return record.receipt_sha256


class RecordingTextFetcher:
    """Injectable Tencent text_fetcher: one existing transport call, no retry.

    fetch_response(url, params, timeout) must return the original response with
    bytes .content and .status_code/.raise_for_status(). Default is UNWIRED.
    Captures response.content, not compressed wire/TLS bytes. Archive failure
    only records a stable diagnostic; the provider gets the same decoded text.
    """
    def __init__(self, fetch_response=None, *, clock=None, archive=None):
        self.fetch_response, self.clock, self.archive = fetch_response, clock, archive
        self.receipts, self.archive_errors = [], []

    def __call__(self, url, params, timeout):
        if self.fetch_response is None:
            raise SourceEvidenceError('SOURCE_UNWIRED')
        if url != 'https://qt.gtimg.cn/q' or not isinstance(params, Mapping):
            raise SourceEvidenceError('TENCENT_REQUEST_SCOPE_INVALID')
        frozen_params = _parameters(dict(params))
        if set(frozen_params) != {'q'} or not re.fullmatch(r'(sh|sz|bj)[0-9]{6}', str(frozen_params['q'])):
            raise SourceEvidenceError('TENCENT_REQUEST_SCOPE_INVALID')
        clock = self.clock or (lambda: datetime.now(SH))
        started = _stamp(clock())
        try:
            response = self.fetch_response(url, dict(frozen_params), timeout)
        except Exception:
            raise SourceEvidenceError('SOURCE_REQUEST_FAILED') from None
        received = _stamp(clock())
        raw = response.content
        if not isinstance(raw, bytes):
            raise SourceEvidenceError('RAW_RESPONSE_BYTES_REQUIRED')
        try:
            saved = record_raw_response(raw, source_ref='tencent:qt.gtimg.cn:q', endpoint=url,
                request_parameters=frozen_params, request_started_at=started, response_received_at=received,
                http_status=response.status_code, byte_kind='HTTP_RESPONSE_CONTENT_BYTES',
                clock_basis='INJECTED_CLOCK_NOT_AUTHENTICATED' if self.clock else 'PROCESS_WALL_CLOCK',
                request_options={'timeout_argument':timeout})
            self.receipts.append(saved)
            if self.archive is not None:
                try:
                    self.archive.store(saved)
                except Exception:
                    self.archive_errors.append('SOURCE_ARCHIVE_WRITE_FAILED')
        except SourceEvidenceError:
            self.archive_errors.append('SOURCE_RECEIPT_INVALID')
        try:
            response.raise_for_status()
        except Exception:
            raise SourceEvidenceError('SOURCE_HTTP_ERROR') from None
        return raw.decode('gbk', errors='replace')  # Requests.text with encoding=gbk has this same behavior.


@dataclass(frozen=True)
class TickerCatalogPackage:
    pages: tuple[RawResponseReceipt, ...]
    complete: bool


@dataclass(frozen=True)
class JSONFieldSource:
    receipt: RawResponseReceipt
    field_map: dict | None = None


def _fields(source, keys, *, observed_at):
    if not isinstance(source, JSONFieldSource):
        raise SourceEvidenceError('JSON_FIELD_SOURCE_UNPROVEN')
    _validate(source.receipt, observed_at=observed_at)
    root = _json(source.receipt.raw_response)
    paths = source.field_map or {key: '/' + key for key in keys}
    if not isinstance(paths, dict) or any(key not in keys for key in paths):
        raise SourceEvidenceError('JSON_FIELD_MAPPING_INVALID')
    result = {}
    for field, path in paths.items():
        if not isinstance(path, str) or not path.startswith('/') or len(path) > 512:
            raise SourceEvidenceError('JSON_FIELD_MAPPING_INVALID')
        item = root
        try:
            for key in path.split('/')[1:]:
                key = key.replace('~1', '/').replace('~0', '~')
                item = item[int(key)] if isinstance(item, list) else item[key]
        except (KeyError, IndexError, ValueError, TypeError):
            item = None
        result[field] = item
    return result


def _decimal(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
        return result if result.is_finite() and result > 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _catalog_identity(package, symbol, now):
    if not isinstance(package, TickerCatalogPackage):
        raise SourceEvidenceError('CATALOG_PACKAGE_UNWIRED')
    if package.complete is not True or not package.pages:
        raise SourceEvidenceError('CATALOG_PACKAGE_PARTIAL')
    rows, totals, offset, observations, pointers = [], set(), 0, [], []
    for page_index, page in enumerate(package.pages):
        _validate(page, observed_at=now)
        if page.endpoint != '/api/meta/tickers/list' or _stamp(page.response_received_at).date() != now.date():
            raise SourceEvidenceError('CATALOG_SOURCE_DATE_OR_ENDPOINT_UNPROVEN')
        params = page.request_parameters
        if params.get('asset_type') != 'a-share' or params.get('exchange') != 'SH,SZ,BJ' or params.get('offset') != offset:
            raise SourceEvidenceError('CATALOG_PAGE_SCOPE_UNPROVEN')
        limit = params.get('limit')
        if type(limit) is not int or limit <= 0:
            raise SourceEvidenceError('CATALOG_PAGE_SCOPE_UNPROVEN')
        body = _json(page.raw_response)
        data = body.get('data')
        if body.get('code') not in (0, '0') or isinstance(body.get('code'), bool) or not isinstance(data, dict):
            raise SourceEvidenceError('CATALOG_RESPONSE_INVALID')
        items = data.get('item', data.get('items'))
        total = data.get('total', data.get('pagination', {}).get('total') if isinstance(data.get('pagination'), dict) else None)
        if not isinstance(items, list) or len(items) > limit or type(total) is not int or total < 0:
            raise SourceEvidenceError('CATALOG_COMPLETENESS_UNPROVEN')
        totals.add(total)
        rows.extend(items)
        pointer_key = 'item' if 'item' in data else 'items'
        pointers.extend((page_index, f'/data/{pointer_key}/{i}') for i in range(len(items)))
        offset += len(items)
        observations.append(_stamp(page.response_received_at))
    if len(totals) != 1 or totals != {len(rows)}:
        raise SourceEvidenceError('CATALOG_COMPLETENESS_UNPROVEN')
    by_symbol = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise SourceEvidenceError('CATALOG_ROW_INVALID')
        identity = row.get('symbol', row.get('thscode', row.get('ticker')))
        if identity is None and isinstance(row.get('code'), str) and row.get('exchange') in {'SH','SZ','BJ'}:
            identity = row['code'] + '.' + row['exchange']
        if not isinstance(identity, str) or not SYMBOL.fullmatch(identity):
            raise SourceEvidenceError('CATALOG_ROW_IDENTITY_UNPROVEN')
        if identity in by_symbol:
            raise SourceEvidenceError('CATALOG_DUPLICATE_SYMBOL')
        by_symbol[identity] = (row, index)
    if symbol not in by_symbol:
        raise SourceEvidenceError('CATALOG_SYMBOL_MISSING')
    row, index = by_symbol[symbol]
    aliases = [row[key] for key in ('listing_date','listed_date','list_date','ipo_date','上市日期') if row.get(key) is not None]
    if len(set(aliases)) > 1:
        raise SourceEvidenceError('CATALOG_LISTING_DATE_CONFLICT')
    listed = aliases[0] if aliases else None
    try:
        if not isinstance(listed, str) or date.fromisoformat(listed).isoformat() != listed or date.fromisoformat(listed) > now.date():
            listed = None
    except ValueError:
        listed = None
    identity = {key: row.get(key) for key in ('board','security_type','security_status','is_st','limit_regime')}
    if type(identity['is_st']) is not bool:
        identity['is_st'] = None
    identity.update(listing_date=listed, listing_date_basis='SOURCE_LISTING_DATE' if listed else None,
                    listing_observed_at=max(observations).isoformat(), catalog_row_index=index,
                    extraction_proof={'page_index':pointers[index][0], 'row_pointer':pointers[index][1],
                        'listing_date_pointer':pointers[index][1]+'/'+next((key for key in ('listing_date','listed_date','list_date','ipo_date','上市日期') if row.get(key) is not None), 'listing_date'),
                        'identity_fields':['board','security_type','security_status','is_st','limit_regime']})
    return identity


def _crosscheck(preclose, prior, corporate, *, symbol, now):
    result = {'status':'UNKNOWN','source_basis':'VENDOR_RELAYED_EXCHANGE_PRECLOSE',
              'equivalent_displayed_basis':False,'prior_raw_close':None,
              'corporate_action_status_authenticated':False,'reason':'PRECLOSE_CROSSCHECK_UNPROVEN'}
    previous = ExchangeTradingCalendar().previous_trading_day(now.date()).isoformat()
    price = _decimal(preclose)
    close = None
    if prior is not None:
        try:
            values = _fields(prior, ('symbol','trade_date','adjust_mode','close'), observed_at=now)
            close = _decimal(values.get('close'))
            arrived = _stamp(prior.receipt.response_received_at)
            if (values.get('symbol') != symbol or values.get('trade_date') != previous
                    or values.get('adjust_mode') not in {'raw','none'} or close is None
                    or arrived.date().isoformat() < previous
                    or (arrived.date().isoformat() == previous and arrived.time().replace(tzinfo=None) < time(15,0))):
                raise SourceEvidenceError('T1_RAW_CLOSE_UNPROVEN')
            result['prior_raw_close'] = str(close)
        except SourceEvidenceError:
            close = None
            result['reason'] = 'T1_RAW_CLOSE_UNPROVEN'
    if corporate is not None:
        try:
            values = _fields(corporate, ('symbol','ex_date','reference_price','reference_basis','event_id'), observed_at=now)
            ref = _decimal(values.get('reference_price'))
            if (values.get('symbol') != symbol or values.get('ex_date') != now.date().isoformat()
                    or values.get('reference_basis') != 'EXCHANGE_CORPORATE_ACTION_REFERENCE'
                    or not isinstance(values.get('event_id'), str) or not values['event_id'] or ref is None):
                raise SourceEvidenceError('CORPORATE_ACTION_REFERENCE_UNPROVEN')
            if ref == price:
                result.update(status='MATCHED_CORPORATE_ACTION_REFERENCE', equivalent_displayed_basis=True, reason=None,
                              reference_price=str(ref), event_id=values['event_id'])
                return result
            result.update(status='MISMATCH', reason='PRECLOSE_CROSSCHECK_MISMATCH')
            return result
        except SourceEvidenceError:
            result['reason'] = 'CORPORATE_ACTION_REFERENCE_UNPROVEN'
            return result
    if close is not None:
        if close == price:
            result.update(status='MATCHED_T1_RAW_CLOSE', equivalent_displayed_basis=True, reason=None)
        else:
            result.update(status='MISMATCH', reason='PRECLOSE_CROSSCHECK_MISMATCH')
    return result


def build_tencent_pit_source(symbol, quote_receipt, *, observed_at, ticker_catalog=None,
                             prior_close=None, corporate_action=None):
    """Pure byte-bounded wrapper result; source_receipt feeds existing capture.

    The composite consumer JSON embeds reversible original bytes and receipts.
    Crosschecked vendor preclose has an explicit compatibility mapping to the
    frozen capture vocabulary, while its vendor origin remains in the proof.
    No-ST name is NOT ordinary evidence; identity comes only from raw catalog
    explicit fields. Explicit Tencent limit indices remain intentionally pending.
    """
    result = {'schema_version':VERSION,'status':'DATA_LIMITED','reason_codes':[],
              'source_receipt':None,'evidence':None,'source_authenticated':False,
              'limits':{'status':'UNKNOWN','upper':None,'lower':None},'annotations':[],
              'explicit_limit_mapping_status':'PENDING_REAL_0926_MULTI_BOARD_SAMPLES',
              'source_fetch_count':0,'model_calls':0,'customer_notification_calls':0,'production_mutation':'NONE'}
    try:
        now = _stamp(observed_at)
        if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
            raise SourceEvidenceError('QUOTE_SYMBOL_UNPROVEN')
        _validate(quote_receipt, observed_at=now)
        expected = symbol[-2:].lower() + symbol[:6]
        if quote_receipt.endpoint != 'https://qt.gtimg.cn/q' or quote_receipt.request_parameters != {'q':expected}:
            raise SourceEvidenceError('QUOTE_REQUEST_SCOPE_MISMATCH')
        try:
            text = quote_receipt.raw_response.decode('gbk', errors='strict')
        except UnicodeError:
            raise SourceEvidenceError('QUOTE_ENCODING_UNPROVEN') from None
        variables = re.findall(r'\bv_((?:sh|sz|bj)[0-9]{6})\s*=', text)
        if variables != [expected]:
            raise SourceEvidenceError('QUOTE_SYMBOL_MISMATCH')
        try:
            quote = TencentIntradayAdapter._quote(text, symbol)
        except TencentMarketDataError:
            raise SourceEvidenceError('QUOTE_PAYLOAD_UNPROVEN') from None
        quote_at, received = _stamp(quote.quote_time), _stamp(quote_receipt.response_received_at)
        if quote_at.date() != now.date() or received.date() != now.date():
            raise SourceEvidenceError('QUOTE_DATE_MISMATCH')
        if quote_at > received or quote_at > now:
            raise SourceEvidenceError('QUOTE_FUTURE')
        if quote_at.time().replace(tzinfo=None) < time(9,25) or (now-quote_at).total_seconds() > 180:
            raise SourceEvidenceError('QUOTE_AUCTION_EXPIRED')
        if not time(9,26) <= now.time().replace(tzinfo=None) < time(9,30):
            raise SourceEvidenceError('CAPTURE_WINDOW_MISSED')
        if not ExchangeTradingCalendar().is_trading_day(now.date()):
            raise SourceEvidenceError('NON_TRADING_DAY')
        evidence = {key:None for key in FIELDS}
        evidence.update(symbol=symbol, trade_date=now.date().isoformat(), observed_at=quote_at.isoformat(),
                        security_name=quote.name, security_name_basis='SOURCE_QUOTE_NAME',
                        preclose=quote.previous_close, preclose_basis='VENDOR_RELAYED_EXCHANGE_PRECLOSE',
                        rule_effective_from=RULE_FROM.isoformat())
        reasons = []
        extraction_proofs = {'quote':{'parser':'TencentIntradayAdapter._quote','security_name_index':1,
                                     'security_code_index':2,'previous_close_index':4,'quote_time_index':30,
                                     'explicit_limit_indices':None}}
        try:
            identity = _catalog_identity(ticker_catalog, symbol, now)
            evidence.update({key:val for key,val in identity.items() if key in FIELDS})
            extraction_proofs['catalog'] = identity['extraction_proof']
        except SourceEvidenceError as exc:
            reasons.append(str(exc))
        if 'ST' in quote.name.upper():
            evidence['is_st'] = True
        cross = _crosscheck(quote.previous_close, prior_close, corporate_action, symbol=symbol, now=now)
        result['preclose_crosscheck'] = cross
        if cross['equivalent_displayed_basis']:
            evidence['preclose_basis'] = 'EXCHANGE_DISPLAYED_PRECLOSE'
        else:
            reasons.append(cross['reason'])
        # Capture/ledger compare numeric prices. Keep the exact decimal text in
        # the proof, not as a string price that falsely appears different.
        evidence['prior_raw_close'] = float(cross['prior_raw_close']) if cross['prior_raw_close'] is not None else None
        if cross['status'] == 'MATCHED_CORPORATE_ACTION_REFERENCE':
            result['annotations'].append('EX_RIGHTS_REFERENCE_OBSERVED')
        for key in REQUIRED_IDENTITY:
            if evidence.get(key) is None or evidence.get(key) == '':
                reasons.append('IDENTITY_FIELDS_INCOMPLETE')
        records = {'quote': quote_receipt}
        if isinstance(ticker_catalog, TickerCatalogPackage):
            records.update({f'catalog_page_{i}': page for i,page in enumerate(ticker_catalog.pages) if isinstance(page, RawResponseReceipt)})
        for key, source in [('prior_close',prior_close),('corporate_action',corporate_action)]:
            if isinstance(source, JSONFieldSource) and isinstance(source.receipt, RawResponseReceipt):
                records[key] = source.receipt
                keys = ('symbol','trade_date','adjust_mode','close') if key == 'prior_close' else ('symbol','ex_date','reference_price','reference_basis','event_id')
                extraction_proofs[key] = {'field_map':source.field_map if source.field_map is not None else {field:'/'+field for field in keys},
                                         'semantics_authenticated':False}
        # Retain safely extracted audit fields if an ancillary receipt is
        # rejected; no consumer receipt or known limits is issued on that path.
        result['evidence'] = evidence
        for record in records.values():
            _validate(record, observed_at=now)
        bindings = {key:record.metadata() | {'receipt_sha256':record.receipt_sha256} for key,record in records.items()}
        originals = {key:{'encoding':'BASE64','bytes':base64.b64encode(record.raw_response).decode('ascii')} for key,record in records.items()}
        payload = dict(schema_version=VERSION, evidence=evidence, source_bindings=bindings,
                       original_responses=originals, preclose_crosscheck=cross,
                       extraction_proofs=extraction_proofs,
                       source_packaging_reason_codes=list(dict.fromkeys(reasons)),
                       catalog_package_complete=ticker_catalog.complete if isinstance(ticker_catalog,TickerCatalogPackage) else None,
                       source_implementation_sha256={
                           'shadow_pit_sources.py':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                           'tencent_minute.py':hashlib.sha256(Path(__file__).parents[1].joinpath('data/tencent_minute.py').read_bytes()).hexdigest(),
                           'price_limits.py':hashlib.sha256(Path(__file__).parents[1].joinpath('evaluation/ablation/price_limits.py').read_bytes()).hexdigest()},
                       annotations=result['annotations'], source_authenticated=False,
                       raw_hash_basis='ORIGINAL_RESPONSE_CONTENT_OR_CALLER_PACKAGE_BYTES',
                       composite_hash_basis='CONSUMER_JSON_BYTES_NOT_HTTP_RESPONSE')
        raw = _canonical(payload)
        if len(raw) > 16 * 1024 * 1024:
            raise SourceEvidenceError('SOURCE_COMPOSITE_TOO_LARGE')
        captured = max(_stamp(record.response_received_at) for record in records.values())
        src = PITSourceReceipt('shadow:composite-source-package', captured, True, raw_response=raw,
            byte_kind='CONSUMER_INPUT_BYTES', field_map={key:'/evidence/'+key for key in FIELDS})
        bound_evidence = evidence | {'source_ref':src.source_ref,'source_input_sha256':hashlib.sha256(raw).hexdigest()}
        limits = resolve_price_limits({**bound_evidence,'price_limit_evidence':bound_evidence}, symbol=symbol, at=now)
        if limits['status'] != 'KNOWN':
            reasons.append(limits.get('evidence_reason') or 'PRICE_LIMITS_UNKNOWN')
        result.update(status='COMPLETE' if not reasons else 'DATA_LIMITED', reason_codes=list(dict.fromkeys(reasons)),
                      source_receipt=src, evidence=evidence, limits=limits,
                      original_quote_raw_sha256=quote_receipt.raw_sha256,
                      consumer_input_sha256=hashlib.sha256(raw).hexdigest(), source_bindings=bindings)
    except SourceEvidenceError as exc:
        result['reason_codes'] = [str(exc)]
    except Exception:
        result['reason_codes'] = ['SOURCE_PACKAGE_INVALID']
    return result


__all__ = ['RawResponseReceipt','record_raw_response','RecordingTextFetcher','RawReceiptArchive',
           'TickerCatalogPackage','JSONFieldSource','build_tencent_pit_source','SourceEvidenceError']
