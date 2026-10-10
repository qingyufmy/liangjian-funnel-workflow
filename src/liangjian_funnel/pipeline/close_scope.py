"""Pure scope bookkeeping. This module does not admit stocks or fetch facts."""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path

from ..reporting import atomic_write_json
from ..redaction import sanitize


def _hash(value: Mapping) -> str:
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,
        sort_keys=True,separators=(',',':')).encode()).hexdigest()


def _json_shape(value, *, redact=True):
    """Reject lossy keys, then freeze the same sanitized shape written to disk."""
    def normalize(item):
        if isinstance(item, Mapping):
            output = {}
            for key, child in item.items():
                try:
                    # JSON's bool/null/float key rules, not arbitrary str(key).
                    text = next(iter(json.loads(json.dumps({key:None},allow_nan=False))))
                except (TypeError, ValueError) as exc:
                    raise ValueError('SCOPE_JSON_KEY_INVALID') from exc
                if text in output:
                    raise ValueError('SCOPE_JSON_KEY_COLLISION')
                output[text] = normalize(child)
            return output
        if isinstance(item, (list, tuple)):
            return [normalize(child) for child in item]
        return item
    try:
        normalized = normalize(value)
        return json.loads(json.dumps(sanitize(normalized) if redact else normalized,
            ensure_ascii=False,allow_nan=False))
    except (TypeError, ValueError) as exc:
        if str(exc).startswith('SCOPE_JSON_KEY_'):
            raise
        raise ValueError('SCOPE_JSON_VALUE_INVALID') from exc


def _restore_proven_v2_ma_keys(value):
    """Only the proven discovery producer's 5/20/60 maps; never other keys."""
    restored = deepcopy(value)
    discovery = restored.get('discovery')
    records = discovery.get('records') if isinstance(discovery, Mapping) else None
    if isinstance(records, list):
        for record in records:
            evidence = record.get('evidence') if isinstance(record, Mapping) else None
            if not isinstance(evidence, dict):
                continue
            for name in ('ma','previous_ma'):
                values = evidence.get(name)
                if isinstance(values, dict) and set(values) == {'5','20','60'}:
                    evidence[name] = {int(key):child for key,child in values.items()}
    return restored


def _hash_fields(payload: Mapping) -> dict:
    value = dict(payload)
    value['receipt_hash'] = _hash({k:v for k,v in value.items()
        if k not in {'recorded_at','receipt_hash','observation_hash'}})
    value['observation_hash'] = _hash({k:v for k,v in value.items() if k != 'observation_hash'})
    return value


def hash_scope_receipt(payload: Mapping) -> dict:
    """Separate stable input identity from immutable observation evidence."""
    value = (_json_shape(payload) if payload.get('schema_version') == 'close-scope-receipt/3'
             else dict(payload))
    return _hash_fields(value)


def validate_scope_receipt(value: Mapping) -> None:
    if value.get('schema_version') == 'close-scope-receipt/1':
        if _hash({k:v for k,v in value.items() if k != 'receipt_hash'}) != value.get('receipt_hash'):
            raise ValueError('CLOSE_SCOPE_RECEIPT_HASH_MISMATCH')
    elif value.get('schema_version') in {'close-scope-receipt/2','close-scope-receipt/3'}:
        # Producer signs sanitized bytes; a reader must not erase changes to
        # those persisted bytes by applying redaction a second time.
        expected = (_hash_fields(_json_shape(value,redact=False))
                    if value.get('schema_version') == 'close-scope-receipt/3'
                    else hash_scope_receipt(value))
        if (value.get('schema_version') == 'close-scope-receipt/2'
                and expected['receipt_hash'] != value.get('receipt_hash')):
            expected = hash_scope_receipt(_restore_proven_v2_ma_keys(value))
        if expected['receipt_hash'] != value.get('receipt_hash'):
            raise ValueError('CLOSE_SCOPE_RECEIPT_HASH_MISMATCH')
        if expected['observation_hash'] != value.get('observation_hash'):
            raise ValueError('SCOPE_RECEIPT_OBSERVATION_HASH_MISMATCH')
    else:
        raise ValueError('CLOSE_SCOPE_RECEIPT_SCHEMA_MISMATCH')


def build_scope_ledger(*, a1_symbols: Iterable[str], hot_symbols: Iterable[str],
                       discovery_symbols: Iterable[str], g0_symbols: Iterable[str]) -> dict:
    """Mirror the existing union/intersection, retaining all source overlaps.

    Inputs must be the original selected sets, not lists reconstructed from
    later caches. The caller owns evidence validation and original timestamps.
    This receipt has no execution authority and applies no new size limit.
    """
    a1, hot, discovery, g0 = (set(value) for value in
        (a1_symbols, hot_symbols, discovery_symbols, g0_symbols))
    requested = a1 | hot | discovery
    selected = requested & g0
    sources = [('A1',a1), ('HOT100',hot), ('EARLY_DISCOVERY',discovery)]
    rows = [{'symbol':symbol,'sources':[name for name, members in sources if symbol in members]}
            for symbol in sorted(selected)]
    payload = {'schema_version':'close-scope-ledger/1',
        'execution_authority':False, 'status':'RECORDED' if selected else 'EMPTY',
        'source_sets':{name:sorted(members) for name,members in sources},
        'g0_symbols':sorted(g0), 'selected_symbols':sorted(selected),
        'selected_records':rows, 'added':[row for row in rows if row['symbol'] not in a1],
        'excluded_a1':sorted(a1-selected), 'excluded_requested':sorted(requested-selected),
        'counts':{'a1':len(a1),'selected':len(selected),'added':len(selected-a1),
                  'excluded_a1':len(a1-selected),'net_growth':len(selected)-len(a1)}}
    payload['scope_hash'] = hashlib.sha256(json.dumps(payload,ensure_ascii=False,
        sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return payload


def seal_scope_receipt(*, root: Path, run_id: str, research_as_of: datetime,
                       market_data_as_of: datetime, a1_reference: dict | None,
                       a1_symbols: Iterable[str], hot_payload: dict, discovery: dict,
                       g0_symbols: Iterable[str], selected_symbols: Iterable[str]) -> Path:
    """Archive the actual union before expensive sync; never decide admission.

    The timestamp of receipt creation is real observation time. A later A1
    generation creates a new file. An identical retry reuses the original
    observation, never claiming that the earlier file was observed later.
    Keep unselected discovery leads as counterevidence, outside the source set.
    """
    if (research_as_of.tzinfo is None or market_data_as_of.tzinfo is None
            or market_data_as_of > research_as_of):
        raise ValueError('INVALID_SCOPE_TIME')
    scope = build_scope_ledger(a1_symbols=a1_symbols,
        hot_symbols=[str(row.get('symbol') or '').strip().upper()
                     for row in hot_payload.get('records',[])
                     if isinstance(row,Mapping) and str(row.get('symbol') or '').strip()],
        discovery_symbols=[row['symbol'] for row in discovery.get('records',[])
                           if row.get('review_budget_selected')], g0_symbols=g0_symbols)
    if set(scope['selected_symbols']) != set(selected_symbols):
        raise ValueError('SELECTED_SCOPE_MISMATCH')
    payload = {'schema_version':'close-scope-receipt/3','run_id':run_id,
        'research_as_of':research_as_of.isoformat(),
        'market_data_as_of':market_data_as_of.isoformat(),
        'recorded_at':datetime.now(timezone.utc).isoformat(),
        'a1_reference':a1_reference,
        'binding_status':'ORIGINAL_RUN_REFERENCE' if a1_reference else 'UNBOUND_A1_REFERENCE',
        'execution_authority':False,'scope':scope,'hot100':hot_payload,'discovery':discovery}
    payload = hash_scope_receipt(payload)
    path = Path(root) / ('close-scope-'+research_as_of.strftime('%Y%m%d')+'-'+payload['receipt_hash']+'.json')
    if path.exists():
        existing = json.loads(path.read_text(encoding='utf-8'))
        validate_scope_receipt(existing)
        if {k:v for k,v in existing.items() if k not in {'recorded_at','observation_hash'}} != {
                k:v for k,v in payload.items() if k not in {'recorded_at','observation_hash'}}:
            raise ValueError('SCOPE_RECEIPT_CONFLICT')
        return path
    atomic_write_json(path,payload)
    return path
