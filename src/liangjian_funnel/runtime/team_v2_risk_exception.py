"""A6 temporary restrictions and exception reviews in the owned Team database.

No model action clears an engine/broker halt. Exit/reduce rows are proposals,
not orders; the control edge must still apply quantity, T+1 and source guards.
"""
from collections.abc import Mapping
from contextlib import closing
from datetime import date,datetime
from decimal import Decimal
import hashlib
import json
from zoneinfo import ZoneInfo

from .team_v2_store import canonical


def apply_exception_review(output,*,targets,previous_controls,validated,previous_failures,hard_halt):
    from ..pipeline.team_v2_risk_schema import exception_domain
    domain=exception_domain(targets)
    if type(previous_failures) is not int or previous_failures<0 or type(hard_halt) is not bool or type(validated) is not bool:
        raise ValueError('A6_EXCEPTION_STATE_INVALID')
    controls={}
    for row in previous_controls:
        if (not isinstance(row,Mapping) or set(row)!={'target','target_id','action','owner'}
                or row['target'] not in ('symbol','cluster','account') or not isinstance(row['target_id'],str)
                or not row['target_id'] or row['owner']!='A6' or row['action']!='HALT_NEW_ENTRY'):
            raise ValueError('A6_EXCEPTION_CONTROL_INVALID')
        key=(row['target'],row['target_id'])
        if key in controls:raise ValueError('A6_EXCEPTION_CONTROL_INVALID')
        controls[key]=dict(row)
    original=dict(controls);rows=[]
    valid=validated and isinstance(output,Mapping) and output.get('session')=='INTRADAY_EXCEPTION'
    if valid:
        rows=output.get('exception_actions');seen=set()
        valid=isinstance(rows,list)
        if valid:
            for row in rows:
                if (not isinstance(row,Mapping) or not isinstance(row.get('target'),str)
                        or not isinstance(row.get('target_id'),str)):
                    valid=False;break
                key=(row['target'],row['target_id']);action=row.get('action')
                if (key not in domain or key in seen or action not in ('REDUCE_HALF','EXIT','HALT_NEW_ENTRY','RESUME','NO_ACTION')
                        or (not domain[key]['data_ready'] and action in ('NO_ACTION','RESUME'))
                        or (action=='RESUME' and (hard_halt or key not in original
                            or not domain[key]['trigger_cleared']))):
                    valid=False;break
                seen.add(key)
            valid=valid and seen==set(domain)
    latched=previous_failures>=2
    count=previous_failures if latched else 0 if valid else previous_failures+1
    status='HALT' if latched or count>=2 or hard_halt else 'READY' if valid else 'DEGRADED'
    actions=[]
    if valid and not latched:
        for row in rows:
            key=(row['target'],row['target_id']);action=row['action']
            if action=='HALT_NEW_ENTRY':controls[key]=dict(target=key[0],target_id=key[1],action=action,owner='A6')
            elif action=='RESUME':controls.pop(key)
            if action in ('EXIT','REDUCE_HALF'):
                actions.append(dict(target=key[0],target_id=key[1],action=action,reason_code=row.get('reason_code')))
    return dict(status=status,consecutive_failures=count,
        risk_unit='NONE' if status=='HALT' else 'PROBE' if status=='DEGRADED' else None,
        max_batches=1 if status in ('HALT','DEGRADED') else None,
        controls=[controls[k] for k in sorted(controls)],position_directives=actions,
        reason_codes=['A6_DEGRADED_HALT'] if count>=2 else ['ENGINE_HARD_HALT'] if hard_halt
            else ['A6_REVIEW_DEGRADED'] if not valid else [],
        internal_alert_status='PENDING' if count>=2 else 'NOT_REQUIRED')


class RiskExceptionRuntime:
    def __init__(self,store):
        self.store=store
        with closing(store._connect()) as db,db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS risk_exception_requests (
                    meeting_id TEXT PRIMARY KEY,input_sha256 TEXT NOT NULL,
                    context_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS risk_exception_reviews (
                    meeting_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,trade_date TEXT NOT NULL,
                    source_call_id TEXT NOT NULL,binding_sha256 TEXT NOT NULL,result_json TEXT NOT NULL,
                    UNIQUE(account_id,trade_date,source_call_id));
                CREATE TABLE IF NOT EXISTS risk_exception_controls (
                    account_id TEXT NOT NULL,trade_date TEXT NOT NULL,
                    controls_json TEXT NOT NULL,version INTEGER NOT NULL,
                    PRIMARY KEY(account_id,trade_date));
                CREATE TABLE IF NOT EXISTS risk_exception_caps (
                    account_id TEXT NOT NULL,trade_date TEXT NOT NULL,
                    cap_pct TEXT NOT NULL,PRIMARY KEY(account_id,trade_date));
                CREATE TABLE IF NOT EXISTS risk_exception_control_origins (
                    account_id TEXT NOT NULL,trade_date TEXT NOT NULL,target TEXT NOT NULL,target_id TEXT NOT NULL,
                    source_call_id TEXT NOT NULL,evidence_json TEXT NOT NULL,
                    PRIMARY KEY(account_id,trade_date,target,target_id));
                CREATE INDEX IF NOT EXISTS risk_exception_requests_account_day ON risk_exception_requests (
                    json_extract(context_json,'$.account_id'),json_extract(context_json,'$.trade_date'));
            ''')

    def context(self,meeting_id):
        with closing(self.store._connect()) as db:
            row=db.execute('SELECT context_json FROM risk_exception_requests WHERE meeting_id=?',(meeting_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def claim(self,meeting_id,*,account_id,trade_date,observed_at,input_sha256,targets,hard_halt,deadline_at=None,source_call_id=None,trigger_evidence=None):
        from ..pipeline.team_v2_risk_schema import exception_domain
        exception_domain(targets)
        if (any(not isinstance(v,str) or not v.strip() for v in (meeting_id,account_id,input_sha256))
                or not isinstance(observed_at,datetime) or observed_at.utcoffset() is None
                or observed_at.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()!=trade_date
                or date.fromisoformat(trade_date).isoformat()!=trade_date or type(hard_halt) is not bool):
            raise ValueError('A6_EXCEPTION_CONTEXT_INVALID')
        if any(t['target']=='account' and t['target_id']!=account_id for t in targets):
            raise ValueError('A6_EXCEPTION_ACCOUNT_MISMATCH')
        base=dict(account_id=account_id,trade_date=trade_date,targets=targets,hard_halt=hard_halt,trigger_evidence=trigger_evidence)
        with closing(self.store._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT input_sha256,context_json FROM risk_exception_requests WHERE meeting_id=?',(meeting_id,)).fetchone()
            if old:
                context=json.loads(old[1])
                if old[0]!=input_sha256 or canonical({k:context[k] for k in base})!=canonical(base):
                    raise ValueError('A6_EXCEPTION_BINDING_CONFLICT')
                return context
            prior=db.execute('SELECT controls_json,version FROM risk_exception_controls WHERE account_id=? AND trade_date=?',
                (account_id,trade_date)).fetchone()
            state=db.execute('SELECT consecutive_failures,halted,last_observed_at FROM risk_day_state WHERE account_id=? AND trade_date=?',
                (account_id,trade_date)).fetchone()
            if state and datetime.fromisoformat(state[2])>observed_at:raise ValueError('A6_EXCEPTION_OUT_OF_ORDER')
            context=dict(base,observed_at=observed_at.isoformat(),controls=json.loads(prior[0]) if prior else [],
                control_version=prior[1] if prior else 0,day_state=list(state) if state else None,
                deadline_at=deadline_at.isoformat() if deadline_at is not None else None)
            context['source_call_id']=source_call_id
            cap=db.execute('SELECT cap_pct FROM risk_exception_caps WHERE account_id=? AND trade_date=?',
                (account_id,trade_date)).fetchone()
            context['utilization_cap']=cap[0] if cap else None
            db.execute('INSERT INTO risk_exception_requests VALUES (?,?,?)',(meeting_id,input_sha256,canonical(context)))
        return context

    def load(self,meeting_id,*,input_sha256):
        with closing(self.store._connect()) as db:
            request=db.execute('SELECT input_sha256 FROM risk_exception_requests WHERE meeting_id=?',(meeting_id,)).fetchone()
            if request!=(input_sha256,):raise ValueError('A6_EXCEPTION_BINDING_CONFLICT')
            row=db.execute('SELECT result_json FROM risk_exception_reviews WHERE meeting_id=?',(meeting_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def restrictions(self,account_id,trade_date):
        """A fresh single snapshot; the monitor must intersect with morning caps."""
        with closing(self.store._connect()) as db,db:
            db.execute('BEGIN')
            controls=db.execute('SELECT controls_json FROM risk_exception_controls WHERE account_id=? AND trade_date=?',
                (account_id,trade_date)).fetchone()
            state=db.execute('SELECT consecutive_failures,halted FROM risk_day_state WHERE account_id=? AND trade_date=?',
                (account_id,trade_date)).fetchone()
            cap=db.execute('SELECT cap_pct FROM risk_exception_caps WHERE account_id=? AND trade_date=?',
                (account_id,trade_date)).fetchone()
            pending=db.execute('SELECT count(*) FROM risk_exception_requests q '
                'LEFT JOIN risk_exception_reviews r ON q.meeting_id=r.meeting_id WHERE r.meeting_id IS NULL '
                "AND json_extract(q.context_json,'$.account_id')=? AND json_extract(q.context_json,'$.trade_date')=?",
                (account_id,trade_date)).fetchone()[0]
            origins=[json.loads(r[0]) for r in db.execute('SELECT evidence_json FROM risk_exception_control_origins '
                'WHERE account_id=? AND trade_date=? ORDER BY target,target_id',(account_id,trade_date))]
        return dict(controls=json.loads(controls[0]) if controls else [],
            consecutive_failures=max(state[0] if state else 0,2 if state and state[1] else 0,
                min(2,pending)),utilization_cap=cap[0] if cap else None,pending_reviews=pending,origins=origins)

    def expire_pending(self,account_id,trade_date,*,now):
        """An abandoned expired request is one failure, never a provider retry."""
        with closing(self.store._connect()) as db:
            pending=[(mid,pin,json.loads(body)) for mid,pin,body in db.execute(
                'SELECT q.meeting_id,q.input_sha256,q.context_json FROM risk_exception_requests q '
                'LEFT JOIN risk_exception_reviews r ON q.meeting_id=r.meeting_id WHERE r.meeting_id IS NULL '
                "AND json_extract(q.context_json,'$.account_id')=? AND json_extract(q.context_json,'$.trade_date')=?",
                (account_id,trade_date))]
        settled=[]
        for mid,pin,body in sorted(pending,key=lambda row:row[2]['observed_at']):
            if (body['account_id']!=account_id or body['trade_date']!=trade_date
                    or not body.get('deadline_at') or not body.get('source_call_id')
                    or now<=datetime.fromisoformat(body['deadline_at'])):continue
            try:
                self.record(mid,source_call_id=body['source_call_id'],input_sha256=pin,output=None,validated=False)
                settled.append(mid)
            except ValueError as exc:
                if str(exc) not in {'A6_EXCEPTION_STATE_CHANGED','A6_EXCEPTION_BINDING_CONFLICT'}:raise
                # A concurrent/later authority cannot be overwritten. The
                # remaining pending request still tightens restrictions().
        return settled

    def record(self,meeting_id,*,source_call_id,input_sha256,output,validated):
        if not isinstance(source_call_id,str) or not source_call_id:raise ValueError('A6_EXCEPTION_CALL_INVALID')
        with closing(self.store._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            request=db.execute('SELECT input_sha256,context_json FROM risk_exception_requests WHERE meeting_id=?',(meeting_id,)).fetchone()
            if request is None or request[0]!=input_sha256:raise ValueError('A6_EXCEPTION_BINDING_CONFLICT')
            context=json.loads(request[1]);account=context['account_id'];day=context['trade_date']
            binding=hashlib.sha256(canonical(dict(request=input_sha256,context=context,
                source_call_id=source_call_id,output=output,validated=validated)).encode()).hexdigest()
            previous=db.execute('SELECT binding_sha256,result_json FROM risk_exception_reviews WHERE meeting_id=? OR '
                '(account_id=? AND trade_date=? AND source_call_id=?)',(meeting_id,account,day,source_call_id)).fetchall()
            if previous:
                if len(previous)!=1 or previous[0][0]!=binding:raise ValueError('A6_EXCEPTION_BINDING_CONFLICT')
                return json.loads(previous[0][1])
            prior=db.execute('SELECT controls_json,version FROM risk_exception_controls WHERE account_id=? AND trade_date=?',
                (account,day)).fetchone()
            state=db.execute('SELECT consecutive_failures,halted,last_observed_at FROM risk_day_state WHERE account_id=? AND trade_date=?',
                (account,day)).fetchone()
            # Model output cannot overwrite a concurrent restriction/morning latch.
            if ((prior[1] if prior else 0)!=context['control_version']
                    or (list(state) if state else None)!=context['day_state']):
                raise ValueError('A6_EXCEPTION_STATE_CHANGED')
            failures=max(state[0],2 if state[1] else 0) if state else 0
            result=apply_exception_review(output,targets=context['targets'],previous_controls=context['controls'],
                validated=validated,previous_failures=failures,hard_halt=context['hard_halt'])
            cap=context['utilization_cap']
            if result['status']=='READY':
                # The strict model schema checked the engine ceiling. Persist
                # this intraday floor too: a later KEEP/RESUME cannot raise it.
                from ..pipeline.team_v2_risk_schema import number
                amount=number(output.get('utilization_cap',{}).get('cap_pct'))
                if amount is None or not 0<=amount<=100:raise ValueError('A6_EXCEPTION_CAP_INVALID')
                cap=str(min(Decimal(cap),amount)) if cap is not None else str(amount)
                db.execute('INSERT INTO risk_exception_caps VALUES (?,?,?) ON CONFLICT(account_id,trade_date) '
                    'DO UPDATE SET cap_pct=excluded.cap_pct',(account,day,cap))
            result['utilization_cap']=cap
            result.update(source_call_id=source_call_id,observed_at=context['observed_at'])
            if result['status']=='READY':
                evidence=context.get('trigger_evidence') or {}
                for action in output['exception_actions']:
                    key=(account,day,action['target'],action['target_id'])
                    if action['action']=='RESUME':
                        db.execute('DELETE FROM risk_exception_control_origins WHERE account_id=? AND trade_date=? AND target=? AND target_id=?',key)
                    elif action['action']=='HALT_NEW_ENTRY':
                        events=[e for e in evidence.get('events',[]) if (e['target'],e['target_id'])==key[2:]]
                        if events:
                            prior_origin=db.execute('SELECT evidence_json FROM risk_exception_control_origins WHERE '
                                'account_id=? AND trade_date=? AND target=? AND target_id=?',key).fetchone()
                            original=json.loads(prior_origin[0]) if prior_origin else dict(target=key[2],target_id=key[3],events=[])
                            known={canonical(e) for e in original['events']}
                            original['events'].extend(e for e in events if canonical(e) not in known)
                            db.execute('INSERT INTO risk_exception_control_origins VALUES (?,?,?,?,?,?) ON CONFLICT '
                                '(account_id,trade_date,target,target_id) DO UPDATE SET source_call_id=excluded.source_call_id,evidence_json=excluded.evidence_json',
                                (*key,source_call_id,canonical(original)))
            db.execute('INSERT INTO risk_exception_controls VALUES (?,?,?,?) ON CONFLICT(account_id,trade_date) DO UPDATE SET '
                'controls_json=excluded.controls_json,version=excluded.version',
                (account,day,canonical(result['controls']),context['control_version']+1))
            db.execute('INSERT INTO risk_day_state VALUES (?,?,?,?,?) ON CONFLICT(account_id,trade_date) DO UPDATE SET '
                'consecutive_failures=excluded.consecutive_failures,halted=excluded.halted,last_observed_at=excluded.last_observed_at',
                (account,day,result['consecutive_failures'],int(result['consecutive_failures']>=2),context['observed_at']))
            db.execute('INSERT INTO risk_exception_reviews VALUES (?,?,?,?,?,?)',
                (meeting_id,account,day,source_call_id,binding,canonical(result)))
        return result


def restrict_exception_envelopes(envelopes,*,plans,account_id,restrictions):
    """Only new-entry caps change; never releases a morning or engine restriction."""
    from dataclasses import replace
    from .risk_engine import conservative_envelope
    controls={(r['target'],r['target_id']) for r in restrictions['controls']}
    failures=restrictions['consecutive_failures'];result=[]
    for envelope in envelopes:
        plan=plans[envelope.plan_id];changed=envelope
        if failures:changed=conservative_envelope(changed,consecutive_failures=failures)
        if controls.intersection({('account',account_id),('symbol',plan.symbol),('cluster',plan.cluster)}):
            changed=replace(changed,eligible=False,risk_unit='NONE',max_shares_cap=0,a6_tightened=True,
                blocked_reasons=tuple(dict.fromkeys((*changed.blocked_reasons,'A6_TEMPORARY_HALT_NEW_ENTRY'))))
        result.append(changed)
    return tuple(result)
