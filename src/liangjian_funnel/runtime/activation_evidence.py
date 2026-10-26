"""Append-only activation decisions, including the original quote and bounds."""
from datetime import datetime
import hashlib
import json

from .publication_receipt import object_hash


def ensure_table(db):
    db.execute('''CREATE TABLE IF NOT EXISTS plan_activation_evidence (
        event_id TEXT PRIMARY KEY,plan_id TEXT NOT NULL,observed_at TEXT NOT NULL,
        entrypoint TEXT NOT NULL,action TEXT NOT NULL,reason_code TEXT NOT NULL,
        body_json TEXT NOT NULL,body_sha256 TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_activation_plan_time ON plan_activation_evidence(plan_id,observed_at)')


def decision(plan, quote, *, now, entrypoint, reason_code):
    payload=json.loads(plan.get('payload_json') or '{}') if 'payload_json' in plan else plan.get('payload',{})
    if hasattr(quote,'model_dump'):quote=quote.model_dump(mode='json')
    raw_quote=dict(quote) if isinstance(quote,dict) else None
    try:
        high=float(payload['trigger_high'])
        chase=(payload.get('no_chase') or payload.get('no_chase_price') or high*1.05
               if entrypoint=='MORNING_REVIEW' else
               payload.get('no_chase') if payload.get('no_chase') is not None else high*1.05)
        effective=dict(stop_level=float(payload['stop_level']),trigger_high=high,no_chase=float(chase))
    except (TypeError,KeyError,ValueError):effective=None
    return dict(schema='plan-activation-evidence/1',plan_id=plan['plan_id'],symbol=plan['symbol'],
        source_run_id=payload.get('source_run_id'),observed_at=now.isoformat(),entrypoint=entrypoint,
        reason_code=reason_code,plan_payload_sha256=object_hash(payload),
        thresholds={k:payload.get(k) for k in ('stop_level','trigger_high','no_chase','no_chase_price')},
        effective_thresholds=effective,
        quote=raw_quote,quote_sha256=object_hash(raw_quote) if raw_quote is not None else None,
        quote_evidence='ORIGINAL_QUOTE' if raw_quote is not None else 'UNAVAILABLE')


def batch_decisions(plans, quotes, failures, *, now, entrypoint):
    by_plan={row['plan_id']:row['reason_code'] for row in failures if row.get('plan_id')}
    by_symbol={row['symbol']:row['reason_code'] for row in failures if row.get('symbol')}
    return {row['plan_id']:decision(row,quotes.get(row['symbol']),now=now,entrypoint=entrypoint,
        reason_code=by_plan.get(row['plan_id'],by_symbol.get(row['symbol'],'AUCTION_PRICE_CONTRACT_PASSED')))
        for row in plans}


def append_on_connection(db, plan, *, after_status, observed_at, evidence=None, action=None):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='plan_activation_evidence'").fetchone():
        raise ValueError('ACTIVATION_EVIDENCE_SCHEMA_MAINTENANCE_REQUIRED')
    body=dict(evidence or decision(dict(plan),None,now=datetime.fromisoformat(observed_at),
        entrypoint='STATE_API',reason_code='CALLER_QUOTE_EVIDENCE_UNAVAILABLE'))
    payload=json.loads(plan['payload_json'])
    if body.get('plan_id')!=plan['plan_id'] or body.get('plan_payload_sha256')!=object_hash(payload):
        raise ValueError('ACTIVATION_DECISION_PLAN_BINDING_CONFLICT')
    if body.get('quote_sha256')!=(object_hash(body['quote']) if body.get('quote') is not None else None):
        raise ValueError('ACTIVATION_DECISION_QUOTE_HASH_CONFLICT')
    body.update(before_status=plan['status'],after_status=after_status,persisted_at=observed_at,
        decision_reference_at=body['observed_at'],decision_recorded_at=observed_at)
    action=action or ('ACTIVATED' if after_status=='ACTIVE_TODAY' else 'INVALIDATED' if after_status=='INVALIDATED' else 'BLOCKED')
    body['action']=action
    text=json.dumps(body,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
    digest=hashlib.sha256(text.encode()).hexdigest()
    db.execute('INSERT OR IGNORE INTO plan_activation_evidence VALUES (?,?,?,?,?,?,?,?)',
        (digest,plan['plan_id'],body['observed_at'],body['entrypoint'],action,body['reason_code'],text,digest))
    return digest
