"""Configured-risk edges after A4, bounded A6 review, guarded paper exits.

No provider, customer notification, or execution-plan write. 0253 allows HALT
to cancel only the corresponding remaining BUY reservations, never held exits.
"""
from contextlib import closing
from dataclasses import asdict
from decimal import Decimal as D
import hashlib
import json

from liangjian_paper_broker.ledger import dump

from .team_v2_exception_session import run_risk_exception
from .team_v2_execution import admit_trader_signal
from .team_v2_risk_exception import RiskExceptionRuntime
from .team_v2_store import canonical
from .team_v2_regime import REGIMES,number
from .team_v2_sources import stamp


def exception_triggers(snapshot,*,plans,limits,regime,account_id,tick,trigger_policy=None):
    """Only configured boundaries and actual confirmed primary transitions."""
    tick=stamp(tick);events={};annotations=['REDUCTION_LINE_NOT_CONFIGURED']
    if trigger_policy is None:annotations.append('NEAR_LIMIT_NOT_CONFIGURED')
    else:
        from .team_v2_exception_policy import validate_exception_policy
        validate_exception_policy({k:v for k,v in trigger_policy.items() if k!='config_sha256'})
    def event(target,identity,reason,**facts):
        row=events.setdefault((target,identity),dict(target=target,target_id=identity,reason_codes=[]))
        if reason not in row['reason_codes']:row['reason_codes'].append(reason)
        if 'transition' in facts:row.setdefault('transitions_by_reason',{})[reason]=facts['transition']
        row.update(facts)
    try:
        nav=number(snapshot['account']['nav']);opening=number(snapshot['risk_day']['day_open_nav'])
        peak=number(snapshot['peak_nav'])
        if any(v is None or v<=0 for v in (nav,opening,peak)):raise ValueError
        reasons=snapshot['risk_day']['halt_reason_codes']
        if not isinstance(reasons,(list,tuple)):raise ValueError
        hard=bool(snapshot['global_halt'] or reasons)
        if nav<=opening*(1-limits.daily_loss_halt_pct):
            event('account',account_id,'DAILY_LOSS_HALT');hard=True
        elif trigger_policy and nav<=opening*(1-limits.daily_loss_halt_pct*D(str(trigger_policy['daily_loss_review_at']))):
            event('account',account_id,'DAILY_LOSS_REVIEW')
        if nav<=peak*(1-limits.drawdown_halt_pct):
            event('account',account_id,'DRAWDOWN_HALT');hard=True
        elif nav<=peak*(1-limits.drawdown_half_pct):event('account',account_id,'DRAWDOWN_HALF_RISK')
        elif trigger_policy and nav<=peak*(1-limits.drawdown_half_pct*D(str(trigger_policy['drawdown_review_at']))):
            event('account',account_id,'DRAWDOWN_REVIEW')
        if hard and not events:event('account',account_id,'ENGINE_PERSISTED_HALT')
        clusters={}
        for position in snapshot['positions']:
            owner=plans.get(position['plan_id']);amount=number(position['market_value'])
            if (owner is None or owner.symbol!=position['symbol'] or amount is None or amount<0
                    or position['mark_minute_end']!=tick.isoformat()):raise ValueError
            clusters[owner.cluster]=clusters.get(owner.cluster,D(0))+amount
            if trigger_policy:
                references=snapshot.get('entry_risk_references',{}).get(owner.plan_id)
                mark=amount/D(position['qty']) if type(position['qty']) is int and position['qty']>0 else None
                if not references or mark is None:
                    annotations.append('POSITION_STOP_REFERENCE_UNAVAILABLE:'+owner.plan_id)
                else:
                    for ref in references:
                        distance=number(ref['initial_stop_distance']);stop=number(ref['stop_price'])
                        if distance is None or stop is None or distance<=0:raise ValueError
                        if mark-stop<=distance*D(str(trigger_policy['position_stop_proximity'])):
                            event('symbol',owner.symbol,'POSITION_STOP_PROXIMITY',entry_risk_reference=ref)
        for order in snapshot['open_orders']:
            if order['side']!='BUY':continue
            owner=plans.get(order['plan_id']);qty=order['remaining_qty'];price=number(order['no_chase_price'])
            if order['order_type']=='LIMIT':
                limit=number(order['limit_price'])
                if limit is None:raise ValueError
                price=min(limit,price) if price is not None else None
            if (owner is None or owner.symbol!=order['symbol'] or type(qty) is not int or qty<=0
                    or price is None or price<=0):raise ValueError
            clusters[owner.cluster]=clusters.get(owner.cluster,D(0))+price*qty
        for cluster,amount in sorted(clusters.items()):
            if not cluster or cluster=='UNKNOWN':raise ValueError
            if amount>=nav*limits.max_cluster_pct:event('cluster',cluster,'CLUSTER_CAP')
            elif trigger_policy and amount>=nav*limits.max_cluster_pct*D(str(trigger_policy['cluster_exposure_review_at'])):
                event('cluster',cluster,'CLUSTER_EXPOSURE_REVIEW')
        if regime.get('live_authority') is True and regime.get('regime') in REGIMES:
            for transition in regime.get('transitions',[]):
                if (transition.get('kind')=='primary' and transition.get('previous') in REGIMES
                        and transition.get('current')==regime['regime'] and transition['previous']!=transition['current']
                        and transition.get('tick')==tick.isoformat()):
                    event('account',account_id,'MARKET_REGIME_CHANGED',transition=transition)
                elif (transition.get('kind')=='emotion' and transition.get('previous') in {'EMOTION_ON','EMOTION_OFF'}
                        and transition.get('current')==regime.get('emotion_state')
                        and transition['previous']!=transition['current'] and transition.get('tick')==tick.isoformat()):
                    event('account',account_id,'EMOTION_STATE_CHANGED',transition=transition)
        return dict(status='READY',hard_halt=hard,events=[events[k] for k in sorted(events)],annotations=annotations)
    except (KeyError,ValueError,TypeError):
        return dict(status='DATA_LIMITED',hard_halt=True,events=[],annotations=annotations,
            reason_code='A6_TRIGGER_ACCOUNT_OR_SCOPE_UNPROVEN')


def _tick_store(store):
    with closing(store._connect()) as db,db:
        db.execute('CREATE TABLE IF NOT EXISTS risk_exception_trigger_checks ('
            'account_id TEXT NOT NULL,trade_date TEXT NOT NULL,tick TEXT NOT NULL,'
            'input_sha256 TEXT NOT NULL,body_json TEXT NOT NULL,result_json TEXT,'
            'PRIMARY KEY(account_id,trade_date,tick))')
        db.execute('CREATE TABLE IF NOT EXISTS risk_exception_review_wakeups ('
            'account_id TEXT NOT NULL,trade_date TEXT NOT NULL,target TEXT NOT NULL,target_id TEXT NOT NULL,'
            'reason_code TEXT NOT NULL,meeting_id TEXT NOT NULL,'
            'PRIMARY KEY(account_id,trade_date,target,target_id,reason_code,meeting_id))')


def claim_wakeups(store,*,account,day,meeting_id,events,maximum):
    """A shared batch consumes each object's/type's quota once, before HTTP."""
    accepted=[];exhausted=[]
    with closing(store._connect()) as db,db:
        db.execute('BEGIN IMMEDIATE')
        for event in events:
            required=sorted(set(event['reason_codes']))
            if event.get('release_check'):
                can_review=True
                for reason in required:
                    key=(account,day,event['target'],event['target_id'],reason)
                    rows=db.execute('SELECT meeting_id FROM risk_exception_review_wakeups WHERE account_id=? '
                        'AND trade_date=? AND target=? AND target_id=? AND reason_code=?',key).fetchall()
                    if meeting_id not in {r[0] for r in rows} and len(rows)>=maximum:can_review=False
                if not can_review:
                    exhausted.append(dict(target=event['target'],target_id=event['target_id'],reason_code='RELEASE_REVIEW_QUOTA'))
                    continue
            reasons=[]
            for reason in required:
                key=(account,day,event['target'],event['target_id'],reason)
                old=db.execute('SELECT 1 FROM risk_exception_review_wakeups WHERE account_id=? AND trade_date=? '
                    'AND target=? AND target_id=? AND reason_code=? AND meeting_id=?',(*key,meeting_id)).fetchone()
                count=db.execute('SELECT count(*) FROM risk_exception_review_wakeups WHERE account_id=? AND trade_date=? '
                    'AND target=? AND target_id=? AND reason_code=?',key).fetchone()[0]
                if old or count<maximum:
                    db.execute('INSERT OR IGNORE INTO risk_exception_review_wakeups VALUES (?,?,?,?,?,?)',(*key,meeting_id))
                    reasons.append(reason)
                else:exhausted.append(dict(target=event['target'],target_id=event['target_id'],reason_code=reason))
            if reasons:accepted.append(dict(event,reason_codes=reasons))
    return accepted,exhausted


def cleared_origins(origins,*,trigger,regime):
    """A stricter sufficient test: original checks clear and no new same-object risk.

    Numeric triggers are all re-evaluated by exception_triggers under the same
    frozen policy. A flip restriction needs a confirmed return to its original
    previous state; disappearance of a one-tick transition is not a release.
    Missing entry references, engine halts, or missing original provenance can
    never supply a successful release proof.
    """
    active={(e['target'],e['target_id']) for e in trigger['events']};cleared=[]
    if trigger['status']!='READY' or trigger['hard_halt']:return cleared
    numeric={'DAILY_LOSS_REVIEW','DRAWDOWN_REVIEW','DRAWDOWN_HALF_RISK','CLUSTER_EXPOSURE_REVIEW','CLUSTER_CAP'}
    for origin in origins:
        if (origin['target'],origin['target_id']) in active:continue
        codes=[];valid=True
        for event in origin['events']:
            for code in event['reason_codes']:
                codes.append(code)
                if code in numeric:continue
                if code=='POSITION_STOP_PROXIMITY':
                    if any(a.startswith('POSITION_STOP_REFERENCE_UNAVAILABLE:') for a in trigger['annotations']):valid=False
                elif code in {'MARKET_REGIME_CHANGED','EMOTION_STATE_CHANGED'}:
                    field='regime' if code=='MARKET_REGIME_CHANGED' else 'emotion_state'
                    previous=event.get('transitions_by_reason',{}).get(code,event.get('transition',{})).get('previous')
                    if not regime.get('live_authority') or previous is None or regime.get(field)!=previous:valid=False
                else:valid=False
        if valid and codes:cleared.append(dict(target=origin['target'],target_id=origin['target_id'],reason_codes=sorted(set(codes))))
    return cleared


def _tick_context(store,*,account,day,tick,binding,body):
    with closing(store._connect()) as db,db:
        db.execute('BEGIN IMMEDIATE')
        old=db.execute('SELECT input_sha256,body_json,result_json FROM risk_exception_trigger_checks '
            'WHERE account_id=? AND trade_date=? AND tick=?',(account,day,tick)).fetchone()
        if old:
            if old[0]!=binding:raise ValueError('A6_TRIGGER_BINDING_CONFLICT')
            return json.loads(old[1]),json.loads(old[2]) if old[2] else None
        later=db.execute('SELECT 1 FROM risk_exception_trigger_checks WHERE account_id=? AND trade_date=? AND tick>?',
            (account,day,tick)).fetchone()
        if later:raise ValueError('A6_TRIGGER_OUT_OF_ORDER')
        prior=db.execute('SELECT body_json,result_json FROM risk_exception_trigger_checks '
            'WHERE account_id=? AND trade_date=? ORDER BY tick DESC LIMIT 1',(account,day)).fetchone()
        body=dict(body,previous=json.loads(prior[0])['trigger'] if prior else None,
            previous_result=json.loads(prior[1]) if prior and prior[1] else None)
        db.execute('INSERT INTO risk_exception_trigger_checks VALUES (?,?,?,?,?,NULL)',
            (account,day,tick,binding,canonical(body)))
    return body,None


def _finish(store,account,day,tick,result):
    text=canonical(result)
    with closing(store._connect()) as db,db:
        db.execute('BEGIN IMMEDIATE')
        old=db.execute('SELECT result_json FROM risk_exception_trigger_checks WHERE account_id=? AND trade_date=? AND tick=?',
            (account,day,tick)).fetchone()
        if old is None:raise ValueError('A6_TRIGGER_CLAIM_MISSING')
        if old[0] is not None and old[0]!=text:raise ValueError('A6_TRIGGER_RESULT_CONFLICT')
        db.execute('UPDATE risk_exception_trigger_checks SET result_json=? WHERE account_id=? AND trade_date=? AND tick=?',
            (text,account,day,tick))
    return json.loads(text)


def run_exception_after_trader(*,session_kwargs,live_state,engine_utilization_cap,capacity_slots,cluster_caps,
        trader_result,envelopes,closed_bars,deadline_monotonic,monotonic,all_plans=None,trigger_policy=None):
    args=dict(session_kwargs)
    if all_plans is not None:args['all_plans']=all_plans
    store=args['store'];broker=args['broker'];tick=stamp(args['closed_5m_end'])
    account=args['account_id'];day=args['trade_date'];plans=args['all_plans'];_tick_store(store)
    source=dict(account_id=account,day=day,tick=tick.isoformat(),packet=args['minute_snapshot_id'],
        trader=trader_result,live_state=live_state,limits=asdict(args['limits']),
        model=args['model'],run_id=args['run_id'],required_envelope=args['required_envelope'],trigger_policy=trigger_policy,
        capacity_slots=capacity_slots,cluster_caps=cluster_caps,
        prompt_pins={r:args['repository'].document(r).sha256 for r in ('A4','A6')},
        plans={pid:asdict(p) for pid,p in sorted(plans.items())})
    source=json.loads(dump(source))
    binding=hashlib.sha256(canonical(source).encode()).hexdigest()
    # Snapshot capture is after the trader reservations, once per frozen tick.
    frozen=store.freeze_control_context('risk-source:'+args['decision_id'],source_sha256=binding,
        body=dict(snapshot=broker.planning_snapshot(),source=source,observed_at=args['clock']().isoformat(),
            engine_utilization_cap=engine_utilization_cap))
    engine_utilization_cap=frozen['engine_utilization_cap']
    trigger=exception_triggers(frozen['snapshot'],plans=plans,limits=args['limits'],regime=live_state,
        account_id=account,tick=tick,trigger_policy=trigger_policy)
    body,old=_tick_context(store,account=account,day=day,tick=tick.isoformat(),binding=binding,
        body=dict(snapshot=frozen['snapshot'],trigger=trigger,observed_at=frozen['observed_at']))
    if old is not None:return old
    trigger=body['trigger'];snapshot=body['snapshot'];runtime=RiskExceptionRuntime(store)
    if trigger['status']!='READY':
        return _finish(store,account,day,tick.isoformat(),dict(status='DATA_LIMITED',trigger=trigger,review=None,admissions=[]))
    restrictions=runtime.restrictions(account,day);controls=restrictions['controls']
    current={(e['target'],e['target_id']):e for e in trigger['events']}
    previous={(e['target'],e['target_id']):e for e in (body['previous'] or {}).get('events',[])}
    targets=[dict(target=k[0],target_id=k[1],data_ready=True,trigger_cleared=False) for k in sorted(current)]
    owned={(r['target'],r['target_id']) for r in controls}
    release=cleared_origins(restrictions['origins'],trigger=trigger,regime=live_state)
    targets.extend(dict(target=r['target'],target_id=r['target_id'],data_ready=True,trigger_cleared=True)
        for r in release if (r['target'],r['target_id']) in owned)
    failed=(body['previous_result'] or {}).get('review') or {}
    needed=bool(targets) and (current!=previous or bool(release) or failed.get('status')=='DEGRADED')
    if not needed:
        return _finish(store,account,day,tick.isoformat(),dict(status='NO_TRIGGER',trigger=trigger,review=None,admissions=[]))
    if trigger_policy:
        events,exhausted=claim_wakeups(store,account=account,day=day,meeting_id='exception:'+args['decision_id'],
            events=trigger['events']+[dict(r,release_check=True) for r in release],
            maximum=trigger_policy['max_reviews_per_object_type_day'])
        allowed={(e['target'],e['target_id']) for e in events}
        targets=[t for t in targets if (t['target'],t['target_id']) in allowed]
        if not targets:
            return _finish(store,account,day,tick.isoformat(),dict(status='OBSERVE_ONLY_REVIEW_QUOTA',trigger=trigger,
                exhausted=exhausted,review=None,admissions=[]))
    repo=args['repository'];values={k:{'status':'UNAVAILABLE'} for k in repo.document('A6').placeholders}
    catalog=dict(categories=['ACCOUNT','RISK_EVENT'],items=[
        dict(id='RISK:ACCOUNT',category='ACCOUNT',value=snapshot),
        dict(id='RISK:TRIGGERS',category='RISK_EVENT',value=trigger)])
    values.update(ACCOUNT_SNAPSHOT=snapshot,POSITIONS=snapshot['positions'],MARKET_STATE=live_state,
        PLANS=[asdict(p) for p in plans.values()],TRADER_SIGNALS_TODAY=[trader_result],
        RISK_EVENTS_TODAY=trigger['events'],EXCEPTION_CONTEXT=trigger,EVIDENCE_CATALOG=catalog,
        RISK_SESSION='INTRADAY_EXCEPTION')
    values=json.loads(dump(values))
    prompt=repo.prepare('A6',values,session='INTRADAY_EXCEPTION')
    required=dict(args['required_envelope'],stage_id='A6',prompt_version=prompt.role_version)
    mid='exception:'+args['decision_id'];call_id='A6:'+mid
    review=run_risk_exception(repository=repo,client=args['client'],journal=args['journal'],store=store,
        meeting_id=mid,call_id=call_id,account_id=account,run_id=args['run_id'],model=args['model'],
        trade_date=day,source_trade_date=args['source_trade_date'],clock=args['clock'],required_envelope=required,
        replacements=values,targets=targets,hard_halt=trigger['hard_halt'],engine_utilization_cap=engine_utilization_cap,
        capacity_slots=capacity_slots,cluster_caps=cluster_caps,limits=args['limits'],timeout_seconds=20,
        max_output_tokens=args['max_output_tokens'],deadline_monotonic=deadline_monotonic,monotonic=monotonic,
        allow_model_call=args['allow_model_call'])
    halted=set()
    for pid,owner in plans.items():
        if review['status']=='HALT' or any(
                r['target']=='account' and r['target_id']==account or
                r['target']=='symbol' and r['target_id']==owner.symbol or
                r['target']=='cluster' and r['target_id']==owner.cluster for r in review['controls']):
            halted.add(pid)
    cancellations=broker.cancel_buy_reservations(mid,plan_ids=halted) if halted else []
    admissions=[];commands={}
    for directive in review['position_directives']:
        for row in snapshot['positions']:
            owner=plans.get(row['plan_id'])
            match=(directive['target']=='account' and directive['target_id']==account or
                directive['target']=='symbol' and directive['target_id']==row['symbol'] or
                directive['target']=='cluster' and owner and directive['target_id']==owner.cluster)
            if match and owner is not None:
                previous_action=commands.get(owner.plan_id)
                commands[owner.plan_id]='SELL_SIGNAL' if directive['action']=='EXIT' or previous_action=='SELL_SIGNAL' else 'REDUCE_SIGNAL'
    for pid,action in sorted(commands.items()):
        if monotonic()>=deadline_monotonic:break
        owner=plans[pid];bar=closed_bars.get(owner.symbol)
        if bar is None or not bar.tradability_known or bar.halted:continue
        signal='A6:'+hashlib.sha256(canonical([account,mid,pid,action]).encode()).hexdigest()
        admissions.append(admit_trader_signal(broker=broker,signal_id=signal,action=action,plan=owner,
            envelope=envelopes[pid],limits=args['limits'],all_plans=plans,
            now=stamp(body['observed_at']).replace(second=0,microsecond=0),
            closed_5m_end=tick,mark_as_of=tick,regime=args['regime'],live_market_ready=args['live_market_ready'],
            snapshot_ready=True,mode='control',calendar=args['calendar'],input_sha256=binding,
            deadline_monotonic=deadline_monotonic,monotonic=monotonic))
    return _finish(store,account,day,tick.isoformat(),dict(status=review['status'],trigger=trigger,review=review,
        cancellations=cancellations,admissions=admissions))


def record_coordinator_failure(args):
    """Local integration/storage/schema faults also tighten once per tick."""
    runtime=RiskExceptionRuntime(args['store']);mid='coordinator-failure:'+args['decision_id']
    binding=hashlib.sha256(canonical([args['account_id'],args['trade_date'],args['decision_id'],
        args['minute_snapshot_id']]).encode()).hexdigest()
    runtime.claim(mid,account_id=args['account_id'],trade_date=args['trade_date'],observed_at=args['clock'](),
        input_sha256=binding,targets=[dict(target='account',target_id=args['account_id'],data_ready=False,trigger_cleared=False)],
        hard_halt=False)
    old=runtime.load(mid,input_sha256=binding)
    if old is not None:return dict(old,failure_persisted=True)
    result=runtime.record(mid,source_call_id='LOCAL:'+mid,input_sha256=binding,output=None,validated=False)
    return dict(result,failure_persisted=True)
