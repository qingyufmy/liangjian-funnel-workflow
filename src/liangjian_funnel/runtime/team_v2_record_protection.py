"""0283: keep the owned CONTROL ledger protected after RECORD fallback.

No model, new BUY, legacy ledger migration, or source collection. Matching and
hard stops consume the same closed packet and caller deadline as CONTROL.
"""
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3

from ..data.mootdx import MinuteBar
from .team_v2_control_minute import run_control_minute
from .team_v2_minute_projection import project_closed_minute
from .team_v2_sources import decode_plan,stamp
from .team_v2_store import canonical


def has_owned_exposure(settings):
    path=Path(settings.root)/'state/team_v2/paper.db'
    if not path.is_file():return False
    if path.is_symlink():raise ValueError('TEAM_DATABASE_SCOPE_INVALID')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        return bool(db.execute('SELECT 1 FROM paper_lots WHERE qty>0 LIMIT 1').fetchone()
            or db.execute("SELECT 1 FROM paper_orders WHERE status IN ('OPEN','PARTIAL') LIMIT 1").fetchone())


def cancel_owned_buys(broker):
    pending=[o for o in broker.planning_snapshot()['open_orders'] if o['side']=='BUY']
    key=hashlib.sha256(canonical(sorted(o['order_id'] for o in pending)).encode()).hexdigest()
    return broker.cancel_buy_reservations('record-buy-cancel:'+key,plan_ids={o['plan_id'] for o in pending})


def protect_owned_minute(*,broker,store,clock,account_id,plans,bars,deadline_monotonic,reason_code):
    now=stamp(clock());day=now.date().isoformat()
    with closing(store._connect()) as db,db:
        db.execute('CREATE TABLE IF NOT EXISTS record_fallback_state '
            '(account_id TEXT,trade_date TEXT,started_at TEXT,reason_code TEXT,PRIMARY KEY(account_id,trade_date))')
        db.execute('INSERT OR IGNORE INTO record_fallback_state VALUES (?,?,?,?)',
            (account_id,day,now.isoformat(),reason_code))
        first=db.execute('SELECT started_at FROM record_fallback_state WHERE account_id=? AND trade_date=?',
            (account_id,day)).fetchone()[0]
    # Cancel BEFORE on_bar can fill an old BUY. Same batch is idempotent; a new
    # unexpected order set gets its own cancellation identity, never reused ACK.
    cancelled=cancel_owned_buys(broker)
    result=run_control_minute(session_kwargs=dict(broker=broker,store=store,clock=clock,
        account_id=account_id,all_plans=plans),closed_bars=bars,deadline_monotonic=deadline_monotonic,model_enabled=False)
    return dict(status='RECORD_FALLBACK_ACTIVE',started_at=first,reason_code=reason_code,
        cancelled_buys=cancelled,protection=result,model_calls=0,new_buy_orders=0,legacy_entry_engine='NOT_RUN')


def protect_packet(settings,*,scope,packet,broker,store,clock,account_id,deadline_monotonic,reason_code,tradability_loader):
    raw={r['plan_id']:json.loads(r['payload_json']) for r in scope['plan_rows']};plans={};bars={};problems=[]
    for row in scope['plans']:
        plan=decode_plan(row);plans[plan.plan_id]=plan;packed=packet['sources'].get(plan.symbol,{})
        one=tuple(MinuteBar.model_validate(b) for b in packed.get('bars',()))
        bar=project_closed_minute(one,(),observed_at=stamp(packet['captured_at'])).get('paper_bar')
        proof=tradability_loader(settings,symbol=plan.symbol,now=stamp(packet['captured_at']),bar=bar,
            quote=None,plan=raw[plan.plan_id],minute_bars=one) if bar is not None else {'status':'DATA_LIMITED'}
        if bar is not None and packed.get('minute_complete') and proof.get('status')=='KNOWN':
            bars[plan.symbol]=replace(bar,tradability_known=True,halted=proof['halted'],
                limit_up=proof['limit_up'],limit_down=proof['limit_down'])
        else:problems.append(dict(plan_id=plan.plan_id,reason_code='RECORD_PROTECTIVE_MINUTE_UNPROVEN'))
    result=protect_owned_minute(broker=broker,store=store,clock=clock,account_id=account_id,plans=plans,bars=bars,
        deadline_monotonic=deadline_monotonic,reason_code=reason_code)
    return {**result,'source_reasons':problems,'packet_sha256':packet['packet_sha256'],'mode':'record'}
