"""0881 delivery routing and durable, state-sensitive notification windows.

This ledger records observations and delivery claims, never trading state.
Claim before transport; an uncertain send is never silently attempted again.
"""
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

ZONE = ZoneInfo('Asia/Shanghai')
CUSTOMER_KINDS = frozenset({'PREMARKET_A3', 'PREMARKET_BATCH_READY',
    'PREMARKET_A3_STATUS', 'A4_EFFECTIVE', 'A4_EXECUTION_RESULT',
    'A4_RISK_PENDING', 'A4_POSITION_DATA_HEALTH',
    'A5_MIDDAY_REVIEW', 'A5_POST_CLOSE_REVIEW'})
DUAL_KINDS = frozenset({'A4_REVIEW_ENGINE_ONLY', 'A4_REVIEW_ENGINE_ONLY_ENDED',
    'A4_ENGINE_ONLY_START', 'A4_ENGINE_ONLY_END', 'PREMARKET_BATCH_READY',
    'A4_REVIEW_RESTORED', 'FILL', 'PREMARKET_A3_STATUS'})
IMMEDIATE_KINDS = DUAL_KINDS | frozenset({'A4_ENGINE_ONLY_ADMISSION',
    'A4_EXECUTION_RESULT', 'A4_EFFECTIVE', 'A4_RISK_PENDING',
    'A4_POSITION_CHANGED', 'A4_STOP_LOSS', 'A4_BUY_VALIDATION_REJECTED',
    'A4_REVIEW_ORIGINAL_BUY_REJECTED', 'A4_MODEL_FAILURE_LATCH',
    'A4_NODE_IDENTITY_CHANGED', 'NATURAL_CLOSE_AUDIT_TERMINATED',
    'AUDITED_RESEARCH_FAILED', 'PROCESS_TERMINATED', 'CLOSE_CHAIN_TERMINATED',
    'NATURAL_CLOSE_TIMEOUT','CLOSE_STAGE_BUDGET_EXHAUSTED','CLOSE_AUDIT_GUARD_STOP','A4_ORIGINAL_ENTRY_REJECTED',
    'DECISION','POSITION','REGIME','SETTLEMENT','PREMARKET_A3'})


def routes(kind):
    return ('CUSTOMER', 'INTERNAL') if kind in DUAL_KINDS else (
        ('CUSTOMER',) if kind in CUSTOMER_KINDS else ('INTERNAL',))


def _stamp(now):
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError('NOTIFICATION_CLOCK_UNPROVEN')
    return now.astimezone(ZONE)


class NotificationPolicy:
    def __init__(self, path):
        self.path = Path(path)
        if self.path.is_symlink() or self.path.parent.is_symlink():
            raise ValueError('NOTIFICATION_LEDGER_SCOPE_INVALID')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS notice_events (
                    event_key TEXT PRIMARY KEY,trade_date TEXT NOT NULL,
                    kind TEXT NOT NULL,symbol TEXT NOT NULL,state TEXT NOT NULL,
                    observed_at TEXT NOT NULL,group_id TEXT,disposition TEXT NOT NULL,
                    payload_json TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS notice_events_day_kind ON notice_events(trade_date,kind);
                CREATE TABLE IF NOT EXISTS notice_groups (
                    group_id TEXT PRIMARY KEY,trade_date TEXT NOT NULL,kind TEXT NOT NULL,
                    symbol TEXT NOT NULL,state TEXT NOT NULL,mode TEXT NOT NULL,
                    first_at TEXT NOT NULL,last_at TEXT NOT NULL,count INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,summary_state TEXT NOT NULL DEFAULT 'OPEN');
                CREATE TABLE IF NOT EXISTS notice_delivery_receipts (
                    event_key TEXT NOT NULL,channel TEXT NOT NULL,status TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,PRIMARY KEY(event_key,channel));
            ''')

    def _connect(self):
        return sqlite3.connect(self.path, timeout=2)

    def observe(self, *, event_key, kind, symbol, state, now, payload, immediate=False):
        local = _stamp(now); day = local.date().isoformat()
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False)
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT disposition,group_id,payload_json FROM notice_events WHERE event_key=?',
                (event_key,)).fetchone()
            if old:
                if old[2] != raw:
                    raise ValueError('NOTIFICATION_EVENT_BINDING_CONFLICT')
                return dict(disposition='DUPLICATE', group_id=old[1], routes=routes(kind))
            # Financial/position changes and the 0881 whitelist always bypass
            # both the rolling window and the daily system budget.
            urgent = immediate or kind in IMMEDIATE_KINDS
            previous_state = db.execute('SELECT state FROM notice_events WHERE trade_date=? AND kind=? AND symbol=? '
                'ORDER BY rowid DESC LIMIT 1',(day,kind,symbol)).fetchone()
            if previous_state and previous_state[0] != state:
                urgent = True
            count = db.execute('SELECT COUNT(*) FROM notice_events WHERE trade_date=? AND kind=?',
                (day, kind)).fetchone()[0]
            mode = 'DAY' if not urgent and routes(kind) == ('INTERNAL',) and count >= 3 else 'WINDOW'
            group = None
            if not urgent:
                if mode == 'DAY':
                    group = db.execute("SELECT group_id,first_at FROM notice_groups WHERE trade_date=? AND kind=? "
                        "AND mode='DAY' AND summary_state='OPEN' ORDER BY first_at DESC LIMIT 1", (day,kind)).fetchone()
                else:
                    group = db.execute("SELECT group_id,first_at FROM notice_groups WHERE trade_date=? AND kind=? "
                        "AND symbol=? AND state=? AND mode='WINDOW' AND summary_state='OPEN' ORDER BY first_at DESC LIMIT 1",
                        (day,kind,symbol,state)).fetchone()
                    if group and local.timestamp()-datetime.fromisoformat(group[1]).timestamp() >= 900:
                        group = None
            disposition = 'MERGED' if mode == 'DAY' or group else 'IMMEDIATE'
            group_id = group[0] if group else hashlib.sha256(event_key.encode()).hexdigest()
            if not urgent:
                if group:
                    db.execute('UPDATE notice_groups SET last_at=?,count=count+1,payload_json=? WHERE group_id=?',
                        (local.isoformat(),raw,group_id))
                else:
                    db.execute('INSERT INTO notice_groups(group_id,trade_date,kind,symbol,state,mode,first_at,last_at,count,payload_json) '
                        'VALUES (?,?,?,?,?,?,?,?,1,?)', (group_id,day,kind,symbol,state,mode,local.isoformat(),local.isoformat(),raw))
            db.execute('INSERT INTO notice_events VALUES (?,?,?,?,?,?,?,?,?)',
                (event_key,day,kind,symbol,state,local.isoformat(),None if urgent else group_id,disposition,raw))
        return dict(disposition=disposition, group_id=None if urgent else group_id, routes=routes(kind))

    def claim_summaries(self, *, now, close_day=False, owner=None):
        local = _stamp(now); output = []
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute("SELECT group_id,kind,symbol,state,mode,first_at,last_at,count,payload_json "
                "FROM notice_groups WHERE trade_date=? AND summary_state='OPEN'", (local.date().isoformat(),)).fetchall()
            for gid,kind,symbol,state,mode,first,last,count,raw in rows:
                payload=json.loads(raw)
                if owner=='native' and 'body' not in payload:
                    continue
                if owner=='workflow' and 'body' in payload:
                    continue
                due = close_day if mode == 'DAY' else local.timestamp()-datetime.fromisoformat(first).timestamp() >= 900
                if not due:
                    continue
                # A window without duplicates is closed without a second post.
                send = mode == 'DAY' or count > 1
                db.execute('UPDATE notice_groups SET summary_state=? WHERE group_id=?',
                    ('CLAIMED' if send else 'CLOSED_NO_DUPLICATES',gid))
                if send:
                    output.append(dict(event_key='summary:'+gid,kind=kind,symbol=symbol,state=state,
                        mode=mode,count=count,first_at=first,last_at=last,payload=payload,
                        routes=('INTERNAL',) if mode=='DAY' else routes(kind)))
        return output

    def receipt(self, event_key, channel, status, receipt):
        with closing(self._connect()) as db, db:
            db.execute('INSERT INTO notice_delivery_receipts VALUES (?,?,?,?) ON CONFLICT(event_key,channel) '
                'DO UPDATE SET status=excluded.status,receipt_json=excluded.receipt_json',
                (event_key,channel,status,json.dumps(receipt,ensure_ascii=False,sort_keys=True)))

    def counts(self, day):
        with closing(self._connect()) as db:
            observed = dict(db.execute('SELECT disposition,COUNT(*) FROM notice_events WHERE trade_date=? GROUP BY disposition',(day,)))
            delivered = dict(db.execute('SELECT channel,COUNT(*) FROM notice_delivery_receipts r JOIN notice_events e '
                "ON e.event_key=r.event_key WHERE e.trade_date=? AND status IN ('SENT','QUEUED_INTERNAL') GROUP BY channel",(day,)))
        return dict(observations=observed,deliveries_or_queued=delivered)
