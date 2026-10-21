"""Independent internal-only delivery; no customer configuration or sender.

Commit the claim before HTTP and never retry an uncertain delivery. Webhooks
cannot promise exactly-once delivery: a crash after claiming stays SENDING for
inspection rather than duplicating a message. Network work belongs exclusively
to the notification command, never the minute trading transaction.
"""
from contextlib import closing
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import httpx

from .team_v2_store import canonical


_DESTINATION = re.compile(r'https://open\.(?:feishu\.cn|larksuite\.com)/open-apis/bot/v2/hook/[A-Za-z0-9_-]+\Z')
_SENSITIVE = re.compile(r'(?i)(?:open-apis/bot/|webhook|api[_-]?key|access[_-]?token|authorization\s*[:=]|bearer\s+|sk-[A-Za-z0-9_-]{12,})')
_ZONE = ZoneInfo('Asia/Shanghai')


def _body_text(body):
    # Only server-built text cards are supported; arbitrary model/provider JSON
    # cannot accidentally carry credentials or a destination in extra fields.
    if (not isinstance(body, dict) or set(body) != {'msg_type', 'content'}
            or body.get('msg_type') != 'text' or not isinstance(body.get('content'), dict)
            or set(body['content']) != {'text'} or not isinstance(body['content']['text'], str)
            or not body['content']['text'].strip()):
        raise ValueError('INTERNAL_CARD_UNSAFE')
    text = canonical(body)
    if len(text.encode()) > 32768 or _SENSITIVE.search(text):
        raise ValueError('INTERNAL_CARD_UNSAFE')
    return text


def _day(now):
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError('INTERNAL_CLOCK_INVALID')
    return now.astimezone(_ZONE).date().isoformat()


class InternalSender:
    """Reload just the internal destination; never import any env file keys."""
    def __init__(self, env_file, *, env, transport=None):
        self.env_file = Path(env_file)
        self.env = env
        self.transport = transport
        self.last_receipt = {}

    def _destination(self):
        value = self.env.get('SHADOW_RESEARCH_WEBHOOK')
        if value is None:
            try:
                with self.env_file.open('rb') as stream:
                    raw = stream.read(65537)
                if len(raw) > 65536:
                    return None
                values = []
                for line in raw.decode('utf-8-sig').splitlines():
                    key, sep, item = line.strip().partition('=')
                    if sep and key.strip() == 'SHADOW_RESEARCH_WEBHOOK':
                        values.append(item.strip())
                if len(values) != 1:
                    return None
                value = values[0]
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                    value = value[1:-1]
            except (OSError, UnicodeError):
                return None
        return value if isinstance(value, str) and _DESTINATION.fullmatch(value) else None

    def ready(self):
        return self._destination() is not None

    def send(self, body):
        text = _body_text(body)
        self.last_receipt = {'http_status':None,'provider_code':None,'provider_message_sha256':None}
        destination = self._destination()
        if destination is None:
            return 'NOT_CONFIGURED'
        try:
            with httpx.Client(transport=self.transport, timeout=2, follow_redirects=False, trust_env=False) as client:
                with client.stream('POST', destination, content=text.encode(),
                        headers={'Content-Type': 'application/json'}) as response:
                    self.last_receipt['http_status']=response.status_code
                    if response.status_code < 200 or response.status_code >= 300:
                        return 'UNKNOWN'
                    chunks = []; size = 0
                    for chunk in response.iter_bytes():
                        size += len(chunk)
                        if size > 16384:
                            return 'UNKNOWN'
                        chunks.append(chunk)
                    result = json.loads(b''.join(chunks))
                    code = result.get('code') if isinstance(result, dict) else None
                    message=result.get('msg') if isinstance(result,dict) else None
                    if isinstance(message,str):
                        self.last_receipt['provider_message_sha256']=hashlib.sha256(message.encode()).hexdigest()
                        self.last_receipt['provider_message_bytes']=len(message.encode())
                        self.last_receipt['message_class']=next((label for label,pattern in (
                            ('KEYWORD_POLICY',r'(?i)keyword'),('SIGNATURE_POLICY',r'(?i)sign|timestamp'),
                            ('RATE_LIMIT',r'(?i)rate|frequen'),('IP_POLICY',r'(?i)\bip\b|white.?list'))
                            if re.search(pattern,message)), 'UNCLASSIFIED')
                    if type(code) is not int:
                        return 'UNKNOWN'
                    self.last_receipt['provider_code']=code
                    return 'SENT' if code == 0 else 'REJECTED'
        except Exception:
            # No provider response, exception text, destination, or request is
            # persisted or logged: even network exception strings can be secret.
            return 'UNKNOWN'


class InternalOutbox:
    def __init__(self, store):
        self.store = store
        with closing(store._connect()) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS team_notifications (
                    event_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,trade_date TEXT NOT NULL,
                    kind TEXT NOT NULL,source_id TEXT NOT NULL,observed_at TEXT NOT NULL,
                    priority INTEGER NOT NULL,body_json TEXT NOT NULL,body_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',result_code TEXT,
                    UNIQUE(account_id,trade_date,kind,source_id));
                CREATE INDEX IF NOT EXISTS team_notifications_pending
                    ON team_notifications(trade_date,status,priority,event_id);
                CREATE TABLE IF NOT EXISTS team_guard_rejections (
                    account_id TEXT NOT NULL,trade_date TEXT NOT NULL,decision_id TEXT NOT NULL,
                    PRIMARY KEY(account_id,trade_date,decision_id));
                CREATE TABLE IF NOT EXISTS team_internal_delivery_receipts (
                    event_id TEXT PRIMARY KEY,observed_at TEXT NOT NULL,receipt_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS team_internal_failure_notices (
                    event_id TEXT PRIMARY KEY,noticed_at TEXT NOT NULL);
            ''')

    def enqueue(self, *, account_id, trade_date, kind, source_id, body, observed_at, priority=50):
        text = _body_text(body)
        if not all(isinstance(item, str) and 0 < len(item) <= 256
                   for item in (account_id, kind, source_id)):
            raise ValueError('INTERNAL_EVENT_BINDING_INVALID')
        if date.fromisoformat(trade_date).isoformat() != trade_date or _day(observed_at) != trade_date:
            raise ValueError('INTERNAL_EVENT_DATE_INVALID')
        if type(priority) is not int or not 0 <= priority <= 100:
            raise ValueError('INTERNAL_PRIORITY_INVALID')
        event_id = hashlib.sha256(canonical([account_id, trade_date, kind, source_id]).encode()).hexdigest()
        digest = hashlib.sha256(text.encode()).hexdigest()
        with closing(self.store._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT body_json,body_sha256,priority FROM team_notifications WHERE event_id=?',
                (event_id,)).fetchone()
            if row is not None:
                if row != (text, digest, priority):
                    raise ValueError('INTERNAL_EVENT_BINDING_CONFLICT')
                return event_id
            db.execute('INSERT INTO team_notifications(event_id,account_id,trade_date,kind,source_id,observed_at,'
                'priority,body_json,body_sha256) VALUES (?,?,?,?,?,?,?,?,?)',
                (event_id, account_id, trade_date, kind, source_id, observed_at.isoformat(), priority, text, digest))
        return event_id

    def record_guard_rejection(self, account_id, trade_date, decision_id):
        with closing(self.store._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT OR IGNORE INTO team_guard_rejections VALUES (?,?,?)',
                (account_id, trade_date, decision_id))
            return db.execute('SELECT COUNT(*) FROM team_guard_rejections WHERE account_id=? AND trade_date=?',
                (account_id, trade_date)).fetchone()[0]

    def flush(self, sender, *, now, maximum=3):
        day = _day(now)
        if type(maximum) is not int or not 1 <= maximum <= 3:
            raise ValueError('INTERNAL_FLUSH_BOUND_INVALID')
        result = dict(attempted=0, sent=0, reason_code='INTERNAL_FLUSH_COMPLETED')
        if not sender.ready():
            return dict(result, reason_code='INTERNAL_WEBHOOK_NOT_CONFIGURED')
        for _ in range(maximum):
            with closing(self.store._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute("SELECT event_id,body_json,body_sha256 FROM team_notifications "
                    "WHERE trade_date=? AND status='PENDING' ORDER BY priority,event_id LIMIT 1", (day,)).fetchone()
                if row is None:
                    break
                event_id, text, digest = row
                if hashlib.sha256(text.encode()).hexdigest() != digest:
                    raise ValueError('INTERNAL_CARD_HASH_MISMATCH')
                body = json.loads(text)
                _body_text(body)
                db.execute("UPDATE team_notifications SET status='SENDING' WHERE event_id=?", (event_id,))
            result['attempted'] += 1
            try:
                outcome = sender.send(body)
            except Exception:
                outcome = 'UNKNOWN'
            if outcome not in {'SENT', 'REJECTED', 'NOT_CONFIGURED'}:
                outcome = 'UNKNOWN'
            status = 'PENDING' if outcome == 'NOT_CONFIGURED' else outcome
            with closing(self.store._connect()) as db, db:
                db.execute("UPDATE team_notifications SET status=?,result_code=? WHERE event_id=? AND status='SENDING'",
                    (status, 'INTERNAL_' + outcome, event_id))
                receipt=getattr(sender,'last_receipt',{})
                allowed={'http_status','provider_code','provider_message_sha256','provider_message_bytes','message_class'}
                safe={key:value for key,value in receipt.items() if key in allowed}
                db.execute('INSERT OR IGNORE INTO team_internal_delivery_receipts VALUES (?,?,?)',
                    (event_id,now.isoformat(),canonical(safe)))
            result['sent'] += outcome == 'SENT'
            if outcome == 'NOT_CONFIGURED':
                result['reason_code'] = 'INTERNAL_WEBHOOK_NOT_CONFIGURED'
                break
            if outcome in {'UNKNOWN','REJECTED'}:break
        with closing(self.store._connect()) as db:
            unconfirmed = db.execute("SELECT COUNT(*) FROM team_notifications WHERE trade_date=? "
                "AND status IN ('SENDING','UNKNOWN','REJECTED')", (day,)).fetchone()[0]
            pending = db.execute("SELECT COUNT(*) FROM team_notifications WHERE trade_date=? AND status='PENDING'", (day,)).fetchone()[0]
            unseen=[r[0] for r in db.execute("SELECT n.event_id FROM team_notifications n "
                "LEFT JOIN team_internal_failure_notices f ON n.event_id=f.event_id "
                "WHERE n.trade_date=? AND n.status IN ('SENDING','UNKNOWN','REJECTED') AND f.event_id IS NULL",(day,))]
            for event in unseen:
                db.execute('INSERT OR IGNORE INTO team_internal_failure_notices VALUES (?,?)',(event,now.isoformat()))
            db.commit()
        result['unconfirmed'] = unconfirmed
        result['pending'] = pending
        result['new_failure_notices']=len(unseen)
        if unseen and result['reason_code'] == 'INTERNAL_FLUSH_COMPLETED':
            result['reason_code'] = 'INTERNAL_DELIVERY_UNCONFIRMED'
        return result
