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


def _hash(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


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
        for name in ('risk_meetings','pm_decisions','trader_sessions','risk_exception_reviews')}
    tables.update(
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
    metrics['team_v2']=dict(nav=nav,utilization_pct=utilization,
        benchmark_hs300_return=None,benchmark_csi1000_return=None,entry_hit_rate=None,
        strict_t1_return=None,strict_t3_return=None,
        model_calls=len(judgments.get('rows',{}).get('calls',[])),
        unavailable_reason='NO_BENCHMARK_OR_MATURED_COMPARISON_PROVIDED')
    snapshot=dict(schema_version='team-a5-facts/1',account_id=account_id,trade_date=trade_date,review_kind=review_kind,
        observed_at=observed_at.isoformat(),legacy_fact_sha256=legacy_facts['input_hash'],
        a2=legacy_facts.get('a2',{'status':'UNAVAILABLE'}),a3=legacy_facts.get('a3',{'status':'UNAVAILABLE'}),
        a4=dict(legacy=legacy_facts.get('a4',{'status':'UNAVAILABLE'}),team=rows.get('trader_sessions')),
        a6=team,paper_broker=dict(snapshot=paper_snapshot,settlement=settlement),
        outcomes=legacy_facts.get('outcomes',{'status':'UNAVAILABLE'}),
        independent_verification=legacy_facts.get('independent_verification',{'status':'UNAVAILABLE'}),
        counterexamples=legacy_facts.get('counterexamples',
            (legacy_facts.get('independent_verification') or {}).get('counterexamples',[])),model_judgments=judgments,
        metrics=metrics,legacy_facts=legacy_facts,
        consistency='PER_DATABASE_READ_TRANSACTIONS_NOT_CROSS_DATABASE_ATOMIC')
    categories=['LEGACY','ACCOUNT','A6','MODEL','METRICS'];items=[
        dict(id='A5:LEGACY',category='LEGACY',sha256=legacy_facts['input_hash']),
        dict(id='A5:ACCOUNT',category='ACCOUNT',sha256=_hash(snapshot['paper_broker'])),
        dict(id='A5:A6',category='A6',sha256=_hash(team)),
        dict(id='A5:MODEL',category='MODEL',sha256=_hash(judgments)),
        dict(id='METRICS:DAILY',category='METRICS',sha256=_hash(metrics))]
    snapshot['citation_catalog']=dict(categories=categories,items=items)
    snapshot['input_hash']=_hash(snapshot)
    return json.loads(canonical(snapshot))
