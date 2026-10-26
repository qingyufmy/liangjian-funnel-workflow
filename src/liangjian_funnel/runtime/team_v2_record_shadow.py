"""Record-mode deterministic evidence, never a model vote or a paper admission."""
from datetime import datetime
import json
import time
from pathlib import Path
import sqlite3

from liangjian_paper_broker.ledger import dump
from .team_v2_execution import admit_trader_signal
from .team_v2_morning import ACCOUNT


def has_shadow_scope(settings, *, now):
    """No owned position is required to observe an already active plan."""
    path=Path(getattr(settings,'state_db_path',Path(settings.root)/'state/workflow.sqlite3'))
    if not path.is_file() or path.is_symlink():return False
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.5) as db:
        db.execute('PRAGMA query_only=ON')
        return db.execute("SELECT 1 FROM execution_plans WHERE status='ACTIVE_TODAY' "
            "AND expires_at>=? LIMIT 1",(now.isoformat(),)).fetchone() is not None


def build_shadow(*,store,broker,plans,evaluations,envelopes,active,limits,clock,cutoff,
        regime,live_market_ready,calendar,packet,utilization_cap,pattern_bindings,deadline,decision):
    rows=[]
    policy=store.load_pm_allocation(store.pm_meeting_id,account_id=ACCOUNT,trade_date=cutoff.date().isoformat())
    for pid,plan in plans.items():
        technical=evaluations.get(pid)
        row=dict(plan_id=pid,symbol=plan.symbol,technical=technical,
            technical_reference_price=str(plan.price),execution_cutoff=cutoff.isoformat(),
            model_status='NOT_RUN_RECORD',model_authority=False,order_authority=False,fill_status='UNKNOWN')
        if time.monotonic()>=deadline:
            row.update(status='UNPROVEN',reason_codes=['SHADOW_REMAINDER_EXHAUSTED'])
        elif pid not in active:
            row.update(status='BLOCKED',reason_codes=['PLAN_NOT_ACTIVE'])
        elif technical is None:
            row.update(status='UNPROVEN',reason_codes=['TECHNICAL_EVALUATION_UNAVAILABLE'])
        elif technical.get('action')=='DATA_BLOCK':
            row.update(status='DATA_BLOCK',reason_codes=technical.get('reason_codes',[]))
        elif technical.get('action') not in {'BUY_SIGNAL','ADD_SIGNAL'}:
            row.update(status='NO_TECHNICAL_ENTRY',reason_codes=technical.get('reason_codes',[]))
        else:
            now=clock()
            result=admit_trader_signal(broker=broker,signal_id='shadow:'+decision+':'+pid,
                action=technical['action'],plan=plan,envelope=envelopes[pid],limits=limits,
                all_plans=plans,now=now,closed_5m_end=cutoff,mark_as_of=cutoff,regime=regime,
                live_market_ready=live_market_ready,snapshot_ready=True,mode='record',calendar=calendar,
                input_sha256=packet['packet_sha256'],pm_allocation=policy,deadline_monotonic=deadline,
                utilization_cap_pct=utilization_cap,entry_pattern_binding=pattern_bindings.get(pid),preview_only=True)
            judgment=result['judgment']
            row.update(status='DETERMINISTIC_ENTRY_ELIGIBLE_BEFORE_MODEL' if judgment.get('status')!='REJECTED'
                and judgment.get('shares',0)>0 else 'BLOCKED',reason_codes=judgment.get('reason_codes',[]),
                risk_preview=judgment,admission_input_sha256=result['input_sha256'])
        rows.append(row)
    body=json.loads(dump(dict(schema_version='record-deterministic-shadow/1',
        status='OBSERVED',packet_sha256=packet['packet_sha256'],captured_at=packet['captured_at'],
        observed_at=clock().isoformat(),rows=rows,model_http=0,order_authority=False,
        fill_status='UNKNOWN',counterfactual_limit='PER_PLAN_DETERMINISTIC_GATES_ONLY_MODEL_FILL_AND_JOINT_ALLOCATION_UNKNOWN')))
    # A distinct namespace cannot consume an admission ID or poison the normal
    # control context if an explicitly approved future unlock occurs this tick.
    return store.freeze_control_context('deterministic-shadow:'+decision,
        source_sha256=packet['packet_sha256'],body=body)
