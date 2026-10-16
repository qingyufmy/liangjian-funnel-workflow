"""Independent Team V2 meeting state; never creates or edits execution plans.

One account/day owns the failure latch. Model completion and retries are kept
in the separate judgment journal; a repeated meeting cannot advance state twice.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from .risk_engine import RiskEnvelope
from .team_v2_risk_meeting import RiskMeetingResult, apply_premarket_review


def canonical(value):
    def encode(item):
        if isinstance(item, Decimal): return str(item)
        raise TypeError('TEAM_STATE_JSON_INVALID')
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False, default=encode)


def assert_database_scope(path: Path, *, marker: str, version: str):
    """Fail before CREATE when pointed at a legacy/unknown database."""
    if marker not in {'team_v2_meta', 'model_research_meta'}:
        raise ValueError('TEAM_DATABASE_MARKER_INVALID')
    if not path.exists(): return
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=2)) as db:
        db.execute('PRAGMA query_only=ON')
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables and (marker not in tables or db.execute('SELECT version FROM '+marker).fetchall() != [(version,)]):
            raise ValueError('TEAM_DATABASE_SCOPE_INVALID')


class TeamMeetingStore:
    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()
        assert_database_scope(self.path, marker='team_v2_meta', version='team-runtime/1')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS team_v2_meta(version TEXT PRIMARY KEY);
                INSERT OR IGNORE INTO team_v2_meta VALUES ('team-runtime/1');
                CREATE TABLE IF NOT EXISTS risk_day_state (
                    account_id TEXT NOT NULL, trade_date TEXT NOT NULL,
                    consecutive_failures INTEGER NOT NULL, halted INTEGER NOT NULL,
                    last_observed_at TEXT NOT NULL, PRIMARY KEY(account_id,trade_date));
                CREATE TABLE IF NOT EXISTS risk_meetings (
                    meeting_id TEXT PRIMARY KEY, source_call_id TEXT NOT NULL,
                    account_id TEXT NOT NULL, trade_date TEXT NOT NULL,
                    observed_at TEXT NOT NULL, binding_sha256 TEXT NOT NULL,
                    input_json TEXT NOT NULL, result_json TEXT NOT NULL,
                    UNIQUE(account_id,trade_date,source_call_id));
                CREATE TABLE IF NOT EXISTS pm_decisions (
                    meeting_id TEXT PRIMARY KEY, source_call_id TEXT NOT NULL,
                    account_id TEXT NOT NULL, trade_date TEXT NOT NULL,
                    input_sha256 TEXT NOT NULL, result_json TEXT NOT NULL,
                    UNIQUE(account_id,trade_date,source_call_id));
                CREATE TABLE IF NOT EXISTS trader_sessions (
                    decision_id TEXT PRIMARY KEY, source_call_id TEXT NOT NULL,
                    account_id TEXT NOT NULL, trade_date TEXT NOT NULL,
                    input_sha256 TEXT NOT NULL, result_json TEXT NOT NULL,
                    UNIQUE(account_id,trade_date,source_call_id));
                CREATE TABLE IF NOT EXISTS trader_session_claims (
                    decision_id TEXT PRIMARY KEY, input_sha256 TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS risk_session_requests (
                    meeting_id TEXT PRIMARY KEY, input_sha256 TEXT NOT NULL,
                    observed_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS eod_requests (
                    meeting_id TEXT PRIMARY KEY, input_sha256 TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS eod_reviews (
                    meeting_id TEXT PRIMARY KEY, input_sha256 TEXT NOT NULL,
                    result_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS pm_override (
                    meeting_id TEXT NOT NULL, symbol TEXT NOT NULL,
                    pm_call_id TEXT NOT NULL, risk_call_id TEXT NOT NULL,
                    evidence_json TEXT NOT NULL, PRIMARY KEY(meeting_id,symbol));
                CREATE TABLE IF NOT EXISTS regime_observations (
                    trade_date TEXT NOT NULL,tick TEXT NOT NULL,input_sha256 TEXT NOT NULL,
                    result_json TEXT NOT NULL,PRIMARY KEY(trade_date,tick));
                CREATE TABLE IF NOT EXISTS regime_transitions (
                    transition_id TEXT PRIMARY KEY,trade_date TEXT NOT NULL,tick TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,notification_status TEXT NOT NULL DEFAULT 'PENDING');
                COMMIT;
            """)

    def _connect(self):
        return sqlite3.connect(self.path, timeout=2)

    def record_live_regime(self,*,facts,tick,observed_at,prior,rules):
        from .team_v2_regime import advance,frozen_candidates
        primary,emotion=frozen_candidates(facts,tick=tick,observed_at=observed_at,rules=rules)
        stamp=tick.astimezone(ZoneInfo('Asia/Shanghai')).isoformat();day=stamp[:10]
        binding=hashlib.sha256(canonical(dict(facts=facts,tick=stamp,observed_at=observed_at.isoformat(),
            prior=prior,config_sha256=rules['config_sha256'])).encode()).hexdigest()
        with closing(self._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT input_sha256,result_json FROM regime_observations WHERE trade_date=? AND tick=?',
                (day,stamp)).fetchone()
            if old:
                if old[0]!=binding:raise ValueError('REGIME_OBSERVATION_BINDING_CONFLICT')
                return json.loads(old[1])
            latest=db.execute('SELECT result_json FROM regime_observations WHERE trade_date=? ORDER BY tick DESC LIMIT 1',
                (day,)).fetchone()
            previous=json.loads(latest[0])['state'] if latest else {}
            result=advance(previous,tick=tick,primary=primary,emotion=emotion,prior=prior,rules=rules)
            result.update(source_input_sha256=binding,config_sha256=rules['config_sha256'])
            db.execute('INSERT INTO regime_observations VALUES (?,?,?,?)',(day,stamp,binding,canonical(result)))
            for transition in result['transitions']:
                evidence=dict(transition,source_input_sha256=binding,config_sha256=rules['config_sha256'])
                tid=hashlib.sha256(canonical(evidence).encode()).hexdigest()
                db.execute('INSERT INTO regime_transitions(transition_id,trade_date,tick,evidence_json) VALUES (?,?,?,?)',
                    (tid,day,stamp,canonical(evidence)))
        return result

    def claim_eod(self, meeting_id: str, *, input_sha256: str):
        if any(not isinstance(v,str) or not v.strip() for v in (meeting_id,input_sha256)):
            raise ValueError('EOD_ID_INVALID')
        with closing(self._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT input_sha256 FROM eod_requests WHERE meeting_id=?',(meeting_id,)).fetchone()
            if row:
                if row[0]!=input_sha256:raise ValueError('EOD_BINDING_CONFLICT')
            else:db.execute('INSERT INTO eod_requests VALUES (?,?)',(meeting_id,input_sha256))

    def load_eod(self, meeting_id: str, *, input_sha256: str):
        with closing(self._connect()) as db:
            row=db.execute('SELECT input_sha256,result_json FROM eod_reviews WHERE meeting_id=?',(meeting_id,)).fetchone()
        if row is None:return None
        if row[0]!=input_sha256:raise ValueError('EOD_BINDING_CONFLICT')
        return json.loads(row[1])

    def record_eod(self, *, meeting_id, input_sha256, pm_call_id, risk_call_id, result):
        """Final review and each actual PM override commit together or not at all."""
        body=canonical(result)
        with closing(self._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            request=db.execute('SELECT input_sha256 FROM eod_requests WHERE meeting_id=?',(meeting_id,)).fetchone()
            if request!=(input_sha256,):raise ValueError('EOD_BINDING_CONFLICT')
            old=db.execute('SELECT input_sha256,result_json FROM eod_reviews WHERE meeting_id=?',(meeting_id,)).fetchone()
            if old:
                if old!=(input_sha256,body):raise ValueError('EOD_BINDING_CONFLICT')
                return json.loads(old[1])
            db.execute('INSERT INTO eod_reviews VALUES (?,?,?)',(meeting_id,input_sha256,body))
            for row in result['overnight_actions']:
                if row['overridden']:
                    db.execute('INSERT INTO pm_override VALUES (?,?,?,?,?)',
                        (meeting_id,row['symbol'],pm_call_id,risk_call_id,canonical(row)))
        return json.loads(body)

    def claim_risk_session(self, meeting_id: str, *, input_sha256: str, observed_at: datetime):
        if (any(not isinstance(v,str) or not v.strip() for v in (meeting_id,input_sha256))
                or not isinstance(observed_at,datetime) or observed_at.utcoffset() is None):
            raise ValueError('A6_SESSION_ID_INVALID')
        with closing(self._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT input_sha256,observed_at FROM risk_session_requests WHERE meeting_id=?',
                (meeting_id,)).fetchone()
            if old:
                if old[0]!=input_sha256:raise ValueError('A6_SESSION_BINDING_CONFLICT')
                return datetime.fromisoformat(old[1])
            db.execute('INSERT INTO risk_session_requests VALUES (?,?,?)',(meeting_id,input_sha256,observed_at.isoformat()))
        return observed_at

    def load_risk_session(self, meeting_id: str, *, input_sha256: str):
        with closing(self._connect()) as db:
            request=db.execute('SELECT input_sha256 FROM risk_session_requests WHERE meeting_id=?',(meeting_id,)).fetchone()
            if request is None or request[0]!=input_sha256:raise ValueError('A6_SESSION_BINDING_CONFLICT')
            row=db.execute('SELECT result_json FROM risk_meetings WHERE meeting_id=?',(meeting_id,)).fetchone()
        return self._decode(row[0]) if row else None

    def load_trader(self, decision_id: str, *, input_sha256: str):
        with closing(self._connect()) as db:
            row=db.execute('SELECT input_sha256,result_json FROM trader_sessions WHERE decision_id=?',
                (decision_id,)).fetchone()
        if row is None:return None
        if row[0]!=input_sha256:raise ValueError('TRADER_SESSION_BINDING_CONFLICT')
        return json.loads(row[1])

    def claim_trader(self, decision_id: str, *, input_sha256: str):
        if any(not isinstance(v,str) or not v.strip() for v in (decision_id,input_sha256)):
            raise ValueError('TRADER_SESSION_ID_INVALID')
        with closing(self._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT input_sha256 FROM trader_session_claims WHERE decision_id=?',
                (decision_id,)).fetchone()
            if old:
                if old[0]!=input_sha256:raise ValueError('TRADER_SESSION_BINDING_CONFLICT')
                return
            db.execute('INSERT INTO trader_session_claims VALUES (?,?)',(decision_id,input_sha256))

    def record_trader(self, *, decision_id: str, source_call_id: str, account_id: str,
                      trade_date: str, input_sha256: str, result: dict):
        if (any(not isinstance(v,str) or not v.strip() for v in
                (decision_id,source_call_id,account_id,input_sha256))
                or date.fromisoformat(trade_date).isoformat()!=trade_date):
            raise ValueError('TRADER_SESSION_ID_INVALID')
        text=canonical(result)
        with closing(self._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            rows=db.execute('SELECT input_sha256,result_json FROM trader_sessions WHERE decision_id=? OR '
                '(account_id=? AND trade_date=? AND source_call_id=?)',
                (decision_id,account_id,trade_date,source_call_id)).fetchall()
            if rows:
                if len(rows)!=1 or rows[0]!=(input_sha256,text):raise ValueError('TRADER_SESSION_BINDING_CONFLICT')
                return json.loads(rows[0][1])
            db.execute('INSERT INTO trader_sessions VALUES (?,?,?,?,?,?)',
                (decision_id,source_call_id,account_id,trade_date,input_sha256,text))
        return json.loads(text)

    def load_pm(self, meeting_id: str, *, input_sha256: str):
        with closing(self._connect()) as db:
            row = db.execute('SELECT input_sha256,result_json FROM pm_decisions WHERE meeting_id=?',
                             (meeting_id,)).fetchone()
        if row is None: return None
        if row[0] != input_sha256: raise ValueError('PM_MEETING_BINDING_CONFLICT')
        return json.loads(row[1])

    def record_pm(self, *, meeting_id: str, source_call_id: str, account_id: str,
                  trade_date: str, input_sha256: str, result: dict):
        if (any(not isinstance(value, str) or not value.strip() for value in
                (meeting_id, source_call_id, account_id, input_sha256))
                or date.fromisoformat(trade_date).isoformat() != trade_date):
            raise ValueError('PM_MEETING_ID_INVALID')
        text = canonical(result)
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute('SELECT input_sha256,result_json FROM pm_decisions WHERE meeting_id=? OR '
                '(account_id=? AND trade_date=? AND source_call_id=?)',
                (meeting_id, account_id, trade_date, source_call_id)).fetchall()
            if rows:
                if len(rows) != 1 or rows[0] != (input_sha256, text):
                    raise ValueError('PM_MEETING_BINDING_CONFLICT')
                return json.loads(rows[0][1])
            db.execute('INSERT INTO pm_decisions VALUES (?,?,?,?,?,?)',
                (meeting_id, source_call_id, account_id, trade_date, input_sha256, text))
        return json.loads(text)

    def load_pm_allocation(self, meeting_id: str, *, account_id: str, trade_date: str):
        from .team_v2_allocation import compile_pm_allocation
        with closing(self._connect()) as db:
            row=db.execute('SELECT account_id,trade_date,input_sha256,result_json,source_call_id FROM pm_decisions '
                'WHERE meeting_id=?',(meeting_id,)).fetchone()
        if row is None:return None
        if row[:2]!=(account_id,trade_date):raise ValueError('PM_ALLOCATION_ACCOUNT_DATE_MISMATCH')
        result=json.loads(row[3])
        if result.get('source_call_id')!=row[4]:raise ValueError('PM_ALLOCATION_BINDING_INVALID')
        return compile_pm_allocation(result,account_id=account_id,trade_date=trade_date,
            meeting_id=meeting_id,input_sha256=row[2])

    def record_premarket(self, *, meeting_id: str, source_call_id: str, account_id: str,
                          trade_date: str, observed_at: datetime, envelopes: tuple[RiskEnvelope, ...],
                          output, validated: bool, engine_utilization_cap: Decimal) -> RiskMeetingResult:
        if any(not isinstance(value,str) or not value.strip() for value in (meeting_id, source_call_id, account_id)):
            raise ValueError('TEAM_MEETING_ID_INVALID')
        if (not isinstance(observed_at,datetime) or observed_at.tzinfo is None or observed_at.utcoffset() is None
                or observed_at.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()!=trade_date
                or date.fromisoformat(trade_date).isoformat()!=trade_date):
            raise ValueError('TEAM_MEETING_DATE_INVALID')
        if any(e.trade_date != trade_date for e in envelopes): raise ValueError('TEAM_MEETING_ENVELOPE_DATE_MISMATCH')
        body = dict(account_id=account_id, trade_date=trade_date, source_call_id=source_call_id,
            envelopes=[asdict(e) for e in envelopes], output=output, validated=validated,
            engine_utilization_cap=engine_utilization_cap)
        text = canonical(body); binding = hashlib.sha256(text.encode()).hexdigest()
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            previous = db.execute('SELECT binding_sha256,result_json FROM risk_meetings WHERE meeting_id=? OR '
                '(account_id=? AND trade_date=? AND source_call_id=?)',
                (meeting_id,account_id,trade_date,source_call_id)).fetchall()
            if previous:
                if len(previous)!=1 or previous[0][0]!=binding:raise ValueError('TEAM_MEETING_BINDING_CONFLICT')
                return self._decode(previous[0][1])
            state = db.execute('SELECT consecutive_failures,halted,last_observed_at FROM risk_day_state WHERE account_id=? AND trade_date=?',
                               (account_id,trade_date)).fetchone()
            if state and datetime.fromisoformat(state[2]) > observed_at:
                raise ValueError('TEAM_MEETING_OUT_OF_ORDER')
            failures = max(state[0], 2 if state[1] else 0) if state else 0
            result = apply_premarket_review(envelopes, output, engine_utilization_cap=engine_utilization_cap,
                validated=validated, previous_failures=failures)
            db.execute('INSERT INTO risk_meetings VALUES (?,?,?,?,?,?,?,?)',
                (meeting_id,source_call_id,account_id,trade_date,observed_at.isoformat(),binding,text,canonical(asdict(result))))
            db.execute('INSERT INTO risk_day_state VALUES (?,?,?,?,?) ON CONFLICT(account_id,trade_date) DO UPDATE SET '
                'consecutive_failures=excluded.consecutive_failures,halted=excluded.halted,last_observed_at=excluded.last_observed_at',
                (account_id,trade_date,result.consecutive_failures,int(result.status=='HALT'),observed_at.isoformat()))
            return result

    @staticmethod
    def _decode(text):
        data = json.loads(text)
        rows = []
        for row in data['envelopes']:
            row['risk_budget'] = Decimal(row['risk_budget'])
            row['blocked_reasons'] = tuple(row['blocked_reasons']); row['binding_limits'] = tuple(row['binding_limits'])
            rows.append(RiskEnvelope(**row))
        return RiskMeetingResult(data['status'],tuple(rows),Decimal(data['utilization_cap']),
            data['consecutive_failures'],tuple(data['reason_codes']))
