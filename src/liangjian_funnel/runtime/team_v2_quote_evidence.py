"""0277: typed replay-only quotes and append-only original quote evidence.

The archive stores complete typed MarketQuote + QuoteResult fields, not raw
provider HTTP bytes. Replay prices never acquire production source authority.
"""
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from ..data.mootdx import MinuteBar
from ..data.tencent_minute import MarketQuote,QuoteResult
from .team_v2_sources import stamp
from .team_v2_store import canonical

REPLAY_BASIS='REPLAY_BAR_DERIVED'


@dataclass(frozen=True)
class ReplayBarDerivedQuote:
    symbol:str
    last:Decimal
    quote_time:datetime
    original_bar_sha256:str
    bid:None=None
    ask:None=None
    quote_basis:str=REPLAY_BASIS
    source_id:str=REPLAY_BASIS

    def __post_init__(self):
        if (self.quote_basis!=REPLAY_BASIS or self.source_id!=REPLAY_BASIS
                or self.bid is not None or self.ask is not None
                or not isinstance(self.last,Decimal) or not self.last.is_finite() or self.last<=0
                or not isinstance(self.quote_time,datetime) or self.quote_time.utcoffset() is None
                or not re.fullmatch('[0-9a-f]{64}',self.original_bar_sha256)):
            raise ValueError('REPLAY_QUOTE_CONTRACT_INVALID')

    @property
    def price(self):return self.last


def replay_quote_from_closed_bar(bar,*,decision_as_of):
    if (not isinstance(bar,MinuteBar) or bar.interval!='1m' or bar.evidence_kind!='MARKET_BAR'
            or bar.bar_end>=stamp(decision_as_of)):
        raise ValueError('REPLAY_CLOSED_BAR_REQUIRED')
    return ReplayBarDerivedQuote(bar.symbol,Decimal(str(bar.close)),bar.bar_end,
        hashlib.sha256(bar.model_dump_json().encode()).hexdigest())


def assert_original_market_quote(quote):
    if (not isinstance(quote,MarketQuote) or isinstance(quote,ReplayBarDerivedQuote)
            or getattr(quote,'quote_basis',None)==REPLAY_BASIS
            or str(getattr(quote,'source_id','')).startswith(REPLAY_BASIS)):
        raise ValueError('CONTROL_REPLAY_QUOTE_FORBIDDEN')


def archive_legacy_monitor_quotes(settings, quotes, *, decision_at, used_at):
    """0277 C also applies to RECORD; reuse received typed objects, no fetch.

    Legacy mode has no owned execution packet/SQL-rowset proof. Keep that
    absence explicit rather than minting a row hash or execution permission.
    """
    current, observed = stamp(decision_at), stamp(used_at)
    sources = {}
    for symbol, quote in sorted(quotes.items()):
        if not isinstance(quote, QuoteResult) or quote.symbol != symbol:
            raise ValueError('CONTROL_QUOTE_ARCHIVE_SYMBOL_MISMATCH')
        if quote.quote is not None:
            assert_original_market_quote(quote.quote)
        sources[symbol] = dict(quote=quote.model_dump(mode='json'))
    if not sources:
        return dict(status='NO_QUOTES', count=0, reason_code='NO_RECEIVED_QUOTE_OBJECTS')
    packet = dict(captured_at=observed.isoformat(), binding=dict(minute=current.isoformat()),
        scope=dict(trade_date=current.date().isoformat(), plan_rowset_sha256=None),
        scope_basis='LEGACY_RECORD_NO_OWNED_SQL_ROWSET_CLAIM', sources=sources)
    packet['packet_sha256'] = hashlib.sha256(canonical(packet).encode()).hexdigest()
    return archive_monitor_quotes(settings, packet, used_at=observed)


def archive_monitor_quotes(settings,packet,*,used_at):
    """Write once before use, byte-compare retries; source errors are retained.

    Separate owned database, immutable UPDATE/DELETE triggers. A read consumer
    opens mode=ro. This does not edit any historical/production business table.
    """
    observed=stamp(used_at);body={k:v for k,v in packet.items() if k!='packet_sha256'}
    if (hashlib.sha256(canonical(body).encode()).hexdigest()!=packet.get('packet_sha256')
            or packet['scope']['trade_date']!=observed.date().isoformat()
            or stamp(packet['captured_at'])>observed):
        raise ValueError('CONTROL_QUOTE_ARCHIVE_PACKET_BINDING_INVALID')
    root=Path(settings.root).resolve();directory=root/'state/team_v2';path=directory/'market_quotes.sqlite3'
    if directory.is_symlink() or path.is_symlink() or directory.resolve()!=root/'state/team_v2':
        raise ValueError('CONTROL_QUOTE_ARCHIVE_SCOPE_INVALID')
    rows=[]
    for symbol,source in sorted(packet['sources'].items()):
        raw=source['quote'];typed=QuoteResult.model_validate(raw)
        if typed.symbol!=symbol:raise ValueError('CONTROL_QUOTE_ARCHIVE_SYMBOL_MISMATCH')
        if typed.quote is not None:
            assert_original_market_quote(typed.quote)
            if typed.quote.symbol!=symbol:raise ValueError('CONTROL_QUOTE_ARCHIVE_SYMBOL_MISMATCH')
        record=dict(schema_version='team-original-market-quote/1',symbol=symbol,
            quote_basis='ORIGINAL_PROVIDER_QUOTE' if typed.quote is not None else 'ORIGINAL_QUOTE_UNAVAILABLE',
            packet_sha256=packet['packet_sha256'],decision_minute=packet['binding']['minute'],
            source_captured_at=packet['captured_at'],scope_rowset_sha256=packet['scope']['plan_rowset_sha256'],
            quote_result=raw,source_claim='COMPLETE_TYPED_OBJECT_NOT_RAW_PROVIDER_HTTP')
        if 'scope_basis' in packet:
            record['scope_basis'] = packet['scope_basis']
        text=canonical(record);quote_text=canonical(raw.get('quote'))
        key=hashlib.sha256(canonical([packet['packet_sha256'],symbol]).encode()).hexdigest()
        rows.append((key,observed.date().isoformat(),symbol,packet['binding']['minute'],packet['packet_sha256'],text,
            hashlib.sha256(text.encode()).hexdigest(),quote_text,hashlib.sha256(quote_text.encode()).hexdigest()))
    if path.exists():
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.1)) as db:
            db.execute('PRAGMA query_only=ON')
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if (tables!={'quote_archive_meta','original_market_quotes'}
                    or db.execute('SELECT version FROM quote_archive_meta').fetchall()!=[('team-quote-archive/1',)]):
                raise ValueError('CONTROL_QUOTE_ARCHIVE_SCOPE_INVALID')
    directory.mkdir(parents=True,exist_ok=True)
    with closing(sqlite3.connect(path,timeout=.1)) as db,db:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS quote_archive_meta(version TEXT PRIMARY KEY);
            INSERT OR IGNORE INTO quote_archive_meta VALUES ('team-quote-archive/1');
            CREATE TABLE IF NOT EXISTS original_market_quotes (
                quote_id TEXT PRIMARY KEY,trade_date TEXT NOT NULL,symbol TEXT NOT NULL,
                decision_minute TEXT NOT NULL,packet_sha256 TEXT NOT NULL,body_json TEXT NOT NULL,
                body_sha256 TEXT NOT NULL,quote_json TEXT NOT NULL,quote_sha256 TEXT NOT NULL,first_used_at TEXT NOT NULL,
                UNIQUE(packet_sha256,symbol));
            CREATE INDEX IF NOT EXISTS original_quotes_day_minute ON original_market_quotes(trade_date,decision_minute,symbol);
            CREATE TRIGGER IF NOT EXISTS original_quotes_no_update BEFORE UPDATE ON original_market_quotes
                BEGIN SELECT RAISE(ABORT,'QUOTE_ARCHIVE_IMMUTABLE'); END;
            CREATE TRIGGER IF NOT EXISTS original_quotes_no_delete BEFORE DELETE ON original_market_quotes
                BEGIN SELECT RAISE(ABORT,'QUOTE_ARCHIVE_IMMUTABLE'); END;
        ''')
        db.execute('BEGIN IMMEDIATE')
        for row in rows:
            previous=db.execute('SELECT quote_id,trade_date,symbol,decision_minute,packet_sha256,body_json,body_sha256,quote_json,quote_sha256 '
                'FROM original_market_quotes WHERE quote_id=?',(row[0],)).fetchone()
            if previous is not None:
                if tuple(row)!=previous:raise ValueError('CONTROL_QUOTE_ARCHIVE_CONTENT_CONFLICT')
            else:db.execute('INSERT INTO original_market_quotes VALUES (?,?,?,?,?,?,?,?,?,?)',(*row,observed.isoformat()))
    return dict(status='ARCHIVED',count=len(rows),quote_ids=[r[0] for r in rows],packet_sha256=packet['packet_sha256'],
        basis='ORIGINAL_TYPED_QUOTES_APPEND_ONLY_NO_RAW_HTTP_CLAIM')
