"""0315 A4 review-only daily circuit breaker; never changes risk admission.

Count actual model attempts exactly once. Three consecutive invalid/failed
reviews latch engine-only for this account/day, across process restarts. Alert
delivery remains in the independent internal outbox, never in a trading call.
"""
from contextlib import closing
from datetime import date,datetime
import json

from .team_v2_store import canonical


class ReviewDayLatch:
    def __init__(self,store):
        self.store=store
        with closing(store._connect()) as db,db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS trader_review_days (
                    account_id TEXT NOT NULL,trade_date TEXT NOT NULL,body_json TEXT NOT NULL,
                    PRIMARY KEY(account_id,trade_date));
                CREATE TABLE IF NOT EXISTS trader_review_attempts (
                    decision_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,trade_date TEXT NOT NULL,
                    valid INTEGER NOT NULL,result_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS trader_review_attempt_classes (
                    decision_id TEXT PRIMARY KEY,counts_as_failure INTEGER NOT NULL);
            ''')

    def read(self,account_id,trade_date):
        date.fromisoformat(trade_date)
        with closing(self.store._connect()) as db:
            row=db.execute('SELECT body_json FROM trader_review_days WHERE account_id=? AND trade_date=?',
                (account_id,trade_date)).fetchone()
        return json.loads(row[0]) if row else dict(consecutive_failures=0,engine_only=False)

    def record(self,*,account_id,trade_date,decision_id,valid,observed_at,counts_as_failure=None):
        from .team_v2_sources import stamp
        if type(valid) is not bool or stamp(observed_at).date().isoformat()!=trade_date:
            raise ValueError('A4_REVIEW_DAY_BINDING_INVALID')
        counted=not valid if counts_as_failure is None else counts_as_failure
        if type(counted) is not bool or (valid and counted):
            raise ValueError('A4_REVIEW_FAILURE_CLASS_INVALID')
        with closing(self.store._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT account_id,trade_date,valid,result_json FROM trader_review_attempts WHERE decision_id=?',
                (decision_id,)).fetchone()
            if old:
                if old[:3]!=(account_id,trade_date,int(valid)):raise ValueError('A4_REVIEW_ATTEMPT_CONFLICT')
                classification=db.execute('SELECT counts_as_failure FROM trader_review_attempt_classes WHERE decision_id=?',
                    (decision_id,)).fetchone()
                if bool(classification[0] if classification else not valid)!=counted:
                    raise ValueError('A4_REVIEW_ATTEMPT_CONFLICT')
                return json.loads(old[3])
            row=db.execute('SELECT body_json FROM trader_review_days WHERE account_id=? AND trade_date=?',
                (account_id,trade_date)).fetchone()
            state=json.loads(row[0]) if row else dict(consecutive_failures=0,engine_only=False)
            if not state['engine_only']:
                state['consecutive_failures']=state['consecutive_failures']+1 if counted else 0
                if state['consecutive_failures']>=3:
                    state.update(engine_only=True,trigger_decision_id=decision_id,triggered_at=stamp(observed_at).isoformat())
            text=canonical(state)
            db.execute('INSERT OR REPLACE INTO trader_review_days VALUES (?,?,?)',(account_id,trade_date,text))
            db.execute('INSERT INTO trader_review_attempts VALUES (?,?,?,?,?)',
                (decision_id,account_id,trade_date,int(valid),text))
            db.execute('INSERT INTO trader_review_attempt_classes VALUES (?,?)',(decision_id,int(counted)))
        return state

    def ensure_alert(self,account_id,trade_date,state):
        if not state['engine_only']:return None
        from .team_v2_internal import InternalOutbox
        return InternalOutbox(self.store).enqueue(account_id=account_id,trade_date=trade_date,
            kind='A4_REVIEW_ENGINE_ONLY',source_id='A4_REVIEW_ENGINE_ONLY:'+trade_date,
            observed_at=datetime.fromisoformat(state['triggered_at']),priority=90,
            body=dict(msg_type='text',content=dict(text='A4_REVIEW_ENGINE_ONLY: '+trade_date+
                '；连续三次无有效裁决，当日停用模型审核。降级模式·仅引擎：原PM/数据/风控门外，加严区间交集、严格禁追价、正常总仓位50%，每次准入尝试另报。')))

    def ensure_recovery_alert(self,account_id,trade_date,observed_at):
        """Only an actual validated review can announce next-day restoration."""
        with closing(self.store._connect()) as db:
            previous=db.execute('SELECT trade_date,body_json FROM trader_review_days '
                'WHERE account_id=? AND trade_date<? ORDER BY trade_date DESC LIMIT 1',
                (account_id,trade_date)).fetchone()
        if previous is None or not json.loads(previous[1]).get('engine_only'):
            return None
        from .team_v2_internal import InternalOutbox
        return InternalOutbox(self.store).enqueue(account_id=account_id,trade_date=trade_date,
            kind='A4_REVIEW_RESTORED',source_id='A4_REVIEW_RESTORED:'+trade_date,
            observed_at=observed_at,priority=10,body=dict(msg_type='text',content=dict(
                text='A4 模型复核已恢复：'+trade_date+' 首次有效复核已落盘；上一交易日仅引擎闩锁未延续。')))
