"""Offline validation of actual closed-day original model quality receipts."""
from datetime import date
import hashlib
import json
from pathlib import Path


def validate_model_change_gate(path, *, expected_sha256, head, source_trade_date, models):
    path=Path(path);raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=expected_sha256:raise ValueError('MODEL_GATE_SHA_MISMATCH')
    body=json.loads(raw)
    if (body.get('head')!=head or body.get('source_trade_date')!=source_trade_date
            or date.fromisoformat(source_trade_date).isoformat()!=source_trade_date
            or body.get('models')!=models):raise ValueError('MODEL_GATE_BINDING_MISMATCH')
    calls=body.get('calls')
    if not isinstance(calls,list):raise ValueError('MODEL_GATE_CALLS_MISSING')
    for role in ('A2','A3','A6','PM'):
        rows=[r for r in calls if r.get('role')==role]
        if not rows or any(r.get('validated')!=1 or r.get('issues')!=[] for r in rows):
            raise ValueError('MODEL_GATE_RAW_NOT_VALIDATED:'+role)
        for row in rows:
            original=Path(row['receipt_path']);payload=original.read_bytes()
            if hashlib.sha256(payload).hexdigest()!=row['receipt_sha256']:
                raise ValueError('MODEL_GATE_ORIGINAL_MISMATCH')
    if not 0<body.get('actual_http',0)<=24:raise ValueError('MODEL_GATE_HTTP_BUDGET_INVALID')
    return body
