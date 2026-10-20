"""Read-only Team V2 A5 projection. Each DB has its own snapshot boundary.

No source fetch, RuntimeStore, model, settlement, order, or legacy writes. The
caller freezes the assembled view before model I/O. Independent DB snapshots
are identified, never misrepresented as a cross-database atomic transaction.
"""
from contextlib import closing
from datetime import datetime
from decimal import Decimal as D
import hashlib
import json
import sqlite3
from pathlib import Path
from zoneinfo import ZoneInfo

from .team_v2_store import assert_database_scope,canonical


def review_degradation_metrics(rows,trade_date):
    """Only two consecutive exchange days warrant the author review flag."""
    if not isinstance(rows,list):return dict(status='NO_REVIEW_DAY_EVIDENCE',report_to_claude=False)
    states={r['trade_date']:json.loads(r['body_json']) for r in rows}
    from datetime import date
    from .calendar import ExchangeTradingCalendar
    previous=ExchangeTradingCalendar().previous_trading_day(date.fromisoformat(trade_date)).isoformat()
    current=states.get(trade_date,{})
    return dict(status='AVAILABLE',engine_only_today=bool(current.get('engine_only')),
        consecutive_review_failures=current.get('consecutive_failures',0),previous_trade_date=previous,
        report_to_claude=bool(current.get('engine_only') and states.get(previous,{}).get('engine_only')),
        delivery='A5_INTERNAL_REVIEW_NOT_CUSTOMER')


def candidate_review_metrics(rows):
    counts=dict(candidate_count=0,selected_count=0,actual_model_objects=0,confirmed_count=0,
        veto_count=0,failed_review_count=0,engine_exit_fallback_count=0)
    seen=set();timeouts=0;unknown=0
    for row in rows if isinstance(rows,list) else []:
        result=json.loads(row['result_json']);review=result.get('candidate_review')
        if not isinstance(review,dict):continue
        if row['decision_id'] in seen:raise ValueError('A5_DUPLICATE_CANDIDATE_REVIEW')
        seen.add(row['decision_id'])
        for key in counts:counts[key]+=review[key]
        if review['timeout_count'] is None:unknown+=review['failed_review_count']
        else:timeouts+=review['timeout_count']
    denominator=counts['actual_model_objects']
    return dict(status='AVAILABLE' if seen else 'NO_CANDIDATE_REVIEW_EVIDENCE',**counts,
        confirmation_rate=str(D(counts['confirmed_count'])/D(denominator)) if denominator else None,
        veto_rate=str(D(counts['veto_count'])/D(denominator)) if denominator else None,
        known_timeout_count=timeouts,unclassified_failure_objects=unknown,
        timeout_rate=str(D(timeouts)/D(denominator)) if denominator and not unknown else None,
        model_denominator='ACTUAL_SENT_CANDIDATE_OBJECTS_NOT_ALL_TRACKED_PLANS')


def trader_scope_metrics(rows):
    counts={'scope_plan_ticks':0,'selected_plan_ticks':0,'model_attempted_plan_ticks':0,'deferred_plan_ticks':0,'carried_wait_plan_ticks':0}
    seen=set();sessions=0
    for row in rows if isinstance(rows,list) else []:
        result=json.loads(row['result_json']);scope=result.get('scope')
        if not isinstance(scope,dict):continue  # Capture-only shard rows are not another scope.
        if row['decision_id'] in seen:raise ValueError('A5_DUPLICATE_TRADER_SCOPE')
        seen.add(row['decision_id']);sessions+=1
        for decision in scope.get('decisions',[]):
            counts['scope_plan_ticks']+=1
            if decision['reason_code']=='DEFERRED_CAPACITY':counts['deferred_plan_ticks']+=1
            elif decision['reason_code']=='CARRIED_WAIT':counts['carried_wait_plan_ticks']+=1
            elif decision.get('selected_for_model',decision.get('model_judgment_denominator',False)):counts['selected_plan_ticks']+=1
            if decision.get('model_judgment_denominator'):counts['model_attempted_plan_ticks']+=1
    denominator=counts['selected_plan_ticks']+counts['deferred_plan_ticks']
    return dict(status='AVAILABLE' if sessions else 'NO_SCOPED_BATCH_EVIDENCE',**counts,
        scheduled_candidate_plan_ticks=denominator,
        deferred_capacity_rate=str(D(counts['deferred_plan_ticks'])/D(denominator)) if denominator else None,
        carried_wait_model_judgments=0,scope_batches=sessions)


def _hash(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def pm_deployment_target_metrics(rows):
    """Copy frozen server-journal targets into A5's numeric citation domain.

    Targets are objectives, not deployed cash or execution permission. Invalid
    or absent originals remain absent; this never repairs a model response.
    """
    result=[]
    for row in rows if isinstance(rows,list) else []:
        raw=row.get('result_json')
        try:body=json.loads(raw) if isinstance(raw,str) else raw
        except ValueError:continue
        if not isinstance(body,dict) or body.get('status')!='READY':continue
        target=body.get('deployment_target')
        if not isinstance(target,dict):continue
        def number(value):
            if isinstance(value,bool):return None
            try:v=D(str(value))
            except Exception:return None
            return v if v.is_finite() else None
        pct=number(target.get('target_pct'));bounds=target.get('baseline_range')
        if (pct is None or not 0<=pct<=100 or not isinstance(bounds,list) or len(bounds)!=2):continue
        low,high=map(number,bounds)
        if low is None or high is None or not 0<=low<=pct<=high<=100:continue
        result.append(dict(meeting_id=row.get('meeting_id'),target_pct=str(pct),
            baseline_range=[str(low),str(high)],source_result_sha256=_hash(body),
            source='FROZEN_PM_DECISION_NOT_A6_VOTE',execution_authority=False))
    return dict(status='AVAILABLE' if result else 'UNAVAILABLE',observations=result,
        interpretation='OBJECTIVE_NOT_ACTUAL_UTILIZATION_OR_ORDER_PERMISSION')


def read_paper_review_evidence(path,*,trade_date,observed_at):
    """No broker constructor/settlement. Missing settlement is not zero NAV."""
    path=Path(path)
    if not path.is_file():return dict(status='UNAVAILABLE',settlement=None,reason_code='PAPER_DATABASE_MISSING')
    if path.is_symlink():raise ValueError('PAPER_DATABASE_SCOPE_INVALID')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {'paper_meta','paper_orders','paper_fills','daily_nav','paper_admissions'}<=tables:
            raise ValueError('PAPER_DATABASE_SCOPE_INVALID')
        settlement=db.execute('SELECT payload_json FROM daily_nav WHERE trade_date=?',(trade_date,)).fetchone()
        rows=dict(orders=[dict(r) for r in db.execute('SELECT * FROM paper_orders WHERE trade_date=? ORDER BY order_id',(trade_date,))],
            fills=[dict(r) for r in db.execute('SELECT * FROM paper_fills WHERE substr(minute_end,1,10)=? ORDER BY fill_id',(trade_date,))],
            admissions=[dict(r) for r in db.execute('SELECT * FROM paper_admissions WHERE trade_date=? ORDER BY signal_id',(trade_date,))])
        # One query-only snapshot. Account values are observations for A5,
        # never passed back as risk/execution authority or as settlement.
        meta=dict(db.execute('SELECT key,value FROM paper_meta'))
        lots=[dict(r) for r in db.execute('SELECT * FROM paper_lots WHERE qty>0')]
        marks={r['symbol']:dict(r) for r in db.execute('SELECT * FROM paper_marks')}
        positions={};cost=D(0);market_value=D(0);complete=True
        for row in lots:
            key=(row['plan_id'],row['symbol']);mark=marks.get(row['symbol']);cost+=D(row['cost'])
            current=bool(mark and mark['trade_date']==trade_date
                and datetime.fromisoformat(mark['minute_end'])<=observed_at)
            complete=complete and current
            if current:market_value+=D(mark['price'])*row['qty']
            item=positions.setdefault(key,dict(plan_id=row['plan_id'],symbol=row['symbol'],qty=0,sellable_qty=0))
            item['qty']+=row['qty'];item['sellable_qty']+=row['qty'] if row['open_date']<trade_date else 0
        cash=D(meta['cash']);account=dict(cash=str(cash),nav=str(cash+market_value) if complete else None,
            positions_value=str(market_value) if complete else None,pnl_realized=meta['realized'],
            pnl_unrealized=str(market_value-cost) if complete else None,
            valuation_scope='CURRENT_DAY_MARKS_NOT_SETTLEMENT')
        if settlement is not None:
            account=json.loads(settlement['payload_json'])['account']
        db.rollback()
    for row in rows['fills']:
        if datetime.fromisoformat(row['minute_end'])>observed_at:raise ValueError('A5_FUTURE_PAPER_FILL')
    return dict(status='CAPTURED',rows=rows,content_sha256=_hash(rows),
        account=account,positions=list(positions.values()),
        valuation_evidence=dict(meta=meta,lots=lots,marks=marks),
        valuation_evidence_sha256=_hash(dict(meta=meta,lots=lots,marks=marks)),
        settlement=json.loads(settlement[0]) if settlement else None)


def _read(path,marker,version,queries):
    if not path.is_file():return {'status':'UNAVAILABLE','reason_code':'OWNED_DATABASE_MISSING'}
    if path.is_symlink():raise ValueError('TEAM_DATABASE_SCOPE_INVALID')
    assert_database_scope(path,marker=marker,version=version)
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        result={}
        for name,(table,sql,params) in queries.items():
            result[name]=([dict(r) for r in db.execute(sql,params)] if table in tables
                else {'status':'UNAVAILABLE','reason_code':'OWNED_TABLE_NOT_INSTALLED'})
        db.rollback()
    return dict(status='CAPTURED',rows=result,content_sha256=_hash(result))


def build_team_review_facts(*,legacy_facts,store,journal,paper_snapshot,settlement,
        account_id,trade_date,review_kind,observed_at):
    if (not isinstance(observed_at,datetime) or observed_at.utcoffset() is None
            or observed_at.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()!=trade_date or review_kind not in {'MIDDAY','POST_CLOSE'}
            or not isinstance(legacy_facts,dict) or legacy_facts.get('trade_date')!=trade_date
            or legacy_facts.get('review_kind')!=review_kind or not account_id):
        raise ValueError('A5_FACT_CONTEXT_INVALID')
    original={k:v for k,v in legacy_facts.items() if k!='input_hash'}
    if _hash(original)!=legacy_facts.get('input_hash'):raise ValueError('A5_LEGACY_FACT_HASH_MISMATCH')
    tables={
        name:(name,f'SELECT * FROM {name} WHERE account_id=? AND trade_date=? ORDER BY 1',(account_id,trade_date))
        for name in ('risk_meetings','pm_decisions','trader_sessions','risk_exception_reviews','record_fallback_state')}
    tables.update(
        trader_review_days=('trader_review_days','SELECT * FROM trader_review_days WHERE account_id=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 2',(account_id,trade_date)),
        morning_engine=('morning_engine','SELECT * FROM morning_engine WHERE account_id=? AND trade_date=?',(account_id,trade_date)),
        risk_day_state=('risk_day_state','SELECT * FROM risk_day_state WHERE account_id=? AND trade_date=?',(account_id,trade_date)))
    # Older unbound EOD rows are not silently assigned to the current account.
    tables['eod_reviews']=('eod_reviews',"SELECT * FROM eod_reviews WHERE "
        "json_extract(result_json,'$.account_id')=? AND json_extract(result_json,'$.trade_date')=? ORDER BY meeting_id",
        (account_id,trade_date))
    team=_read(store.path,'team_v2_meta','team-runtime/1',tables)
    calls=set();rows=team.get('rows',{})
    for name in ('risk_meetings','pm_decisions','trader_sessions','risk_exception_reviews'):
        for row in rows.get(name,[]) if isinstance(rows.get(name),list) else []:
            calls.add(row['source_call_id'])
    for row in rows.get('eod_reviews',[]):
        body=json.loads(row['result_json'])
        calls.update(v for v in (body.get('source_pm_call_id'),body.get('source_risk_call_id')) if v)
    run_ids=legacy_facts.get('source_run_ids',[])
    if not isinstance(run_ids,list) or any(not isinstance(v,str) for v in run_ids):raise ValueError('A5_SOURCE_RUN_IDS_INVALID')
    call_values=sorted(calls);run_values=sorted(set(run_ids))
    terms=['0'];values=[]
    if call_values:terms.append('call_id IN ('+','.join('?' for _ in call_values)+')');values.extend(call_values)
    if run_values:terms.append('run_id IN ('+','.join('?' for _ in run_values)+')');values.extend(run_values)
    where='execution_trade_date=? AND ('+' OR '.join(terms)+')'
    judgments=_read(journal.path,'model_research_meta','model-research/1',{
        'calls':('model_calls','SELECT * FROM model_calls WHERE '+where+' ORDER BY call_id',(trade_date,*values)),
        'judgments':('model_judgments','SELECT * FROM model_judgments WHERE call_id IN '
            '(SELECT call_id FROM model_calls WHERE '+where+') ORDER BY call_id,ordinal',(trade_date,*values))})
    for row in judgments.get('rows',{}).get('calls',[]):
        if datetime.fromisoformat(row['captured_at'])>observed_at:raise ValueError('A5_FUTURE_MODEL_CAPTURE')
        if hashlib.sha256(row['output_json'].encode()).hexdigest()!=row['output_hash']:
            raise ValueError('MODEL_OUTPUT_HASH_MISMATCH')
    for row in judgments.get('rows',{}).get('judgments',[]):
        if hashlib.sha256(row['judgment_json'].encode()).hexdigest()!=row['judgment_sha256']:
            raise ValueError('MODEL_JUDGMENT_HASH_MISMATCH')
    if settlement is not None and settlement.get('trade_date')!=trade_date:
        raise ValueError('A5_SETTLEMENT_DATE_MISMATCH')
    account=(paper_snapshot or {}).get('account',{})
    nav=account.get('nav');value=account.get('positions_value')
    utilization=str(D(value)/D(nav)*100) if nav is not None and value is not None and D(nav)>0 else None
    metrics=dict(legacy_facts.get('metrics') or {})
    metrics['trader_model_scope']=trader_scope_metrics(rows.get('trader_sessions'))
    metrics['trader_candidate_review']=candidate_review_metrics(rows.get('trader_sessions'))
    metrics['trader_review_degradation']=review_degradation_metrics(rows.get('trader_review_days'),trade_date)
    # 0290: name original observation denominators explicitly. Counts are
    # copied from frozen metrics, never reconstructed from filtered prose.
    def count(value):return value if type(value) is int and value>=0 else None
    original_actions=metrics.get('a4_action_counts')
    original_actions=original_actions if isinstance(original_actions,dict) else {}
    metrics['observation_counts']=dict(
        observed_minutes=count(metrics.get('a4_monitor_observation_count')),
        start_confirmation=count(original_actions.get('START_CONFIRMATION')),
        plan_invalidated=count(original_actions.get('PLAN_INVALIDATED')),
        empty_scope=count(original_actions.get('EMPTY_SCOPE')),
        denominator='ORIGINAL_MONITOR_OBSERVATION_ROWS_NOT_DISTINCT_WALL_CLOCK_MINUTES')
    targets=(legacy_facts.get('business_audit') or {}).get('target_buy_signals') or {}
    low=count(targets.get('minimum'));high=count(targets.get('maximum'))
    if (low,high)!=(2,5):low=high=None
    metrics['targets']=dict(daily_buy_signals_minimum=low,daily_buy_signals_maximum=high,
        status='FROZEN_APPROVED_TARGET' if (low,high)==(2,5) else 'UNAVAILABLE',
        target_is_not_quota=True)
    # Explicit original counts, not focus+watch arithmetic by the model. The
    # role may quote only metrics; missing/invalid originals stay unavailable.
    a2=legacy_facts.get('a2',{})
    for name in ('quant_evaluated_count','llm_reviewed_count'):
        value=a2.get(name) if isinstance(a2,dict) else None
        metrics['a2_'+name]=value if type(value) is int and value>=0 else None
    metrics['team_v2']=dict(nav=nav,utilization_pct=utilization,
        benchmark_hs300_return=None,benchmark_csi1000_return=None,entry_hit_rate=None,
        strict_t1_return=None,strict_t3_return=None,
        model_calls=len(judgments.get('rows',{}).get('calls',[])),
        unavailable_reason='NO_BENCHMARK_OR_MATURED_COMPARISON_PROVIDED')
    metrics['pm_deployment_targets']=pm_deployment_target_metrics(rows.get('pm_decisions'))
    from ..pipeline.team_v2_catalyst_gaps import catalyst_gap_summary
    a1_themes=[theme for call in judgments.get('rows',{}).get('calls',[])
        if call['role']=='A1' and call['validated'] and call['session']=='POLICY_MACRO_DISCOVERY'
        for theme in json.loads(call['output_json']).get('structural_themes',[])]
    metrics['a1_catalyst_evidence']=dict(catalyst_gap_summary(a1_themes),
        denominator='VALIDATED_DISCOVERY_THEME_JUDGMENTS_NOT_STOCKS_OR_TRADES')
    from ..review.portfolio_diagnostics import strategy_portfolio_diagnostics
    portfolio=strategy_portfolio_diagnostics(legacy_facts)
    metrics['strategy_portfolio_diagnostics']=portfolio
    from ..pipeline.team_v2_feedback import read_method_feedback
    calibration={role:read_method_feedback(journal.path,recipient=role,as_of=observed_at)
        for role in ('A1','A2','A3','A4','A6')}
    snapshot=dict(schema_version='team-a5-facts/1',account_id=account_id,trade_date=trade_date,review_kind=review_kind,
        observed_at=observed_at.isoformat(),legacy_fact_sha256=legacy_facts['input_hash'],
        a2=legacy_facts.get('a2',{'status':'UNAVAILABLE'}),a3=legacy_facts.get('a3',{'status':'UNAVAILABLE'}),
        a4=dict(legacy=legacy_facts.get('a4',{'status':'UNAVAILABLE'}),team=rows.get('trader_sessions')),
        a6=team,paper_broker=dict(snapshot=paper_snapshot,settlement=settlement),
        outcomes=legacy_facts.get('outcomes',{'status':'UNAVAILABLE'}),
        independent_verification=legacy_facts.get('independent_verification',{'status':'UNAVAILABLE'}),
        counterexamples=legacy_facts.get('counterexamples',
            (legacy_facts.get('independent_verification') or {}).get('counterexamples',[])),model_judgments=judgments,
        metrics=metrics,legacy_facts=legacy_facts,strategy_portfolio_diagnostics=portfolio,
        method_calibration=calibration,
        consistency='PER_DATABASE_READ_TRANSACTIONS_NOT_CROSS_DATABASE_ATOMIC')
    categories=['LEGACY','ACCOUNT','A6','MODEL','METRICS'];items=[
        dict(id='A5:LEGACY',category='LEGACY',sha256=legacy_facts['input_hash']),
        dict(id='A5:ACCOUNT',category='ACCOUNT',sha256=_hash(snapshot['paper_broker'])),
        dict(id='A5:A6',category='A6',sha256=_hash(team)),
        dict(id='A5:MODEL',category='MODEL',sha256=_hash(judgments)),
        dict(id='METRICS:DAILY',category='METRICS',sha256=_hash(metrics)),
        dict(id='METRICS:STRATEGY_PORTFOLIO',category='METRICS',sha256=_hash(portfolio))]
    snapshot['citation_catalog']=dict(categories=categories,items=items)
    items.extend(dict(id='A5:METHOD:'+role,category='METRICS',sha256=_hash(stats)) for role,stats in calibration.items())
    snapshot['input_hash']=_hash(snapshot)
    return json.loads(canonical(snapshot))
