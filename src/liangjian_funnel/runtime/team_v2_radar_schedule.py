"""Owned radar trigger claims. No timer, provider, model or order side effect.

The claim is durable before a future dispatcher calls a model. A crash/timeout
never authorizes a duplicate call. Results must be explicitly reconciled.
"""
from contextlib import closing
import hashlib,json

from .team_v2_intraday_sessions import radar_market_context
from .team_v2_sources import stamp
from .team_v2_store import TeamMeetingStore,canonical
from .trader_guardrails import is_closed_trader_tick


class RadarTriggerStore:
    def __init__(self,path):
        self.store=TeamMeetingStore(path)
        with closing(self.store._connect()) as db,db:
            db.execute('''CREATE TABLE IF NOT EXISTS intraday_radar_requests(
                request_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,trade_date TEXT NOT NULL,
                tick TEXT NOT NULL,source_sha256 TEXT NOT NULL,claimed_at TEXT NOT NULL,
                used_switch INTEGER NOT NULL,body_json TEXT NOT NULL,
                UNIQUE(account_id,trade_date,tick))''')

    def claim_due(self,*,account_id,observed_at,calendar):
        if not isinstance(account_id,str) or not account_id.strip():raise ValueError('RADAR_ACCOUNT_INVALID')
        at=stamp(observed_at);day=at.date().isoformat()
        if calendar.is_trading_day(at.date()) is not True:return dict(status='NOOP',reason_code='NON_TRADING_DAY')
        tick=at.replace(minute=at.minute-at.minute%5,second=0,microsecond=0)
        if not is_closed_trader_tick(tick):return dict(status='NOOP',reason_code='RADAR_NOT_CLOSED_SESSION_TICK')
        fixed=tick.strftime('%H:%M') in {'10:00','13:30'}
        with closing(self.store._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            raw=db.execute('SELECT input_sha256,result_json FROM regime_observations WHERE trade_date=? AND tick=?',
                (day,tick.isoformat())).fetchone()
            if raw is None:return dict(status='DATA_BLOCK',reason_code='RADAR_CURRENT_G5_OBSERVATION_MISSING')
            state=json.loads(raw[1]);market=radar_market_context(state,observed_at=at)
            transitions=[json.loads(r[0]) for r in db.execute(
                'SELECT evidence_json FROM regime_transitions WHERE trade_date=? AND tick=?',(day,tick.isoformat()))]
            switched=market['emotion_state']=='ON' and any(r.get('kind')=='emotion'
                and r.get('previous')=='EMOTION_OFF' and r.get('current')=='EMOTION_ON'
                and r.get('source_input_sha256')==raw[0] for r in transitions)
            if not fixed and not switched:return dict(status='NOOP',reason_code='RADAR_NO_AUTHORIZED_TRIGGER')
            prior=db.execute('SELECT body_json FROM intraday_radar_requests WHERE account_id=? AND trade_date=? AND tick=?',
                (account_id,day,tick.isoformat())).fetchone()
            if prior:return dict(status='ALREADY_CLAIMED_NO_RECALL',request=json.loads(prior[0]))
            count,used=db.execute('SELECT count(*),coalesce(sum(used_switch),0) FROM intraday_radar_requests WHERE account_id=? AND trade_date=?',
                (account_id,day)).fetchone()
            if count>=3:return dict(status='NOOP',reason_code='RADAR_DAILY_THREE_ALREADY_CLAIMED')
            if switched and used:
                if not fixed:return dict(status='NOOP',reason_code='RADAR_SWITCH_TRIGGER_ALREADY_CLAIMED')
                switched=False
            reasons=(['FIXED_'+tick.strftime('%H%M')] if fixed else [])+(['CONFIRMED_OFF_TO_ON'] if switched else [])
            rid=hashlib.sha256(canonical(dict(account=account_id,day=day,tick=tick.isoformat())).encode()).hexdigest()
            body=dict(request_id=rid,account_id=account_id,trade_date=day,tick=tick.isoformat(),claimed_at=at.isoformat(),
                source_sha256=raw[0],trigger_reasons=reasons,market=market,state='CLAIMED_NOT_CALLED',
                permission='TRIGGER_ONLY_NO_PLAN_OR_ORDER_AUTHORITY')
            db.execute('INSERT INTO intraday_radar_requests VALUES (?,?,?,?,?,?,?,?)',
                (rid,account_id,day,tick.isoformat(),raw[0],at.isoformat(),int(switched),canonical(body)))
            return dict(status='CLAIMED',request=body)
