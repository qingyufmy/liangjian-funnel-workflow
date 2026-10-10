"""Explicit LOCAL_REFERENCE projection; observations are not execution factors.

No collector or alternate-source call occurs here. Legacy EASTMONEY factor
normalization is intentionally untouched pending a separate policy decision.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import math
from typing import Any, Mapping, Sequence

from .a2_market import (
    _content_hash, TENCENT_CAPITAL_FLOW_PROVIDER, WINDOWS, unavailable_capital_flow_snapshot,
)


def project_local_capital_evidence(
    snapshot: Mapping[str, Any], *, as_of: datetime, expected_symbols: Sequence[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Retain genuine today metadata without renormalizing missing history.

    Hash/source/date/scope checks bind the observation, not its acquisition
    authenticity or business-time availability. Formal dual-cutoff collector
    wiring remains a separate requirement and is explicitly unproven here.
    """
    symbols = tuple(dict.fromkeys(expected_symbols))
    rows = snapshot.get('by_symbol')
    reason = 'OK'
    if snapshot.get('source_id') != TENCENT_CAPITAL_FLOW_PROVIDER:
        reason = 'LOCAL_RAW_SOURCE_MISMATCH'
    elif snapshot.get('content_hash') != _content_hash(snapshot):
        reason = 'LOCAL_RAW_HASH_MISMATCH'
    elif snapshot.get('trade_date') != as_of.date().isoformat():
        reason = 'LOCAL_RAW_TRADE_DATE_MISMATCH'
    elif not isinstance(rows, Mapping) or set(rows) != set(symbols):
        reason = 'LOCAL_RAW_UNIVERSE_MISMATCH'
    observed = reason == 'OK' and snapshot.get('available') is True
    if reason == 'OK' and not observed:
        reason = str(snapshot.get('reason_code') or 'SOURCE_UNAVAILABLE')
    evidence_rows = {}
    if observed:
        for symbol in symbols:
            row = rows[symbol]
            metrics = row.get('metrics') if isinstance(row, Mapping) else None
            today = metrics.get('today') if isinstance(metrics, Mapping) else None
            evidence_rows[symbol] = {
                'symbol': symbol, 'evidence_only': True,
                'today': deepcopy(today) if isinstance(today, Mapping) else None,
                'composite_score': None,
            }
    evidence: dict[str, Any] = {
        'schema_version': 'a2-local-capital-today-evidence/1',
        'source_id': TENCENT_CAPITAL_FLOW_PROVIDER,
        'source_role': 'RAW_OBSERVATION_NOT_EXECUTION', 'evidence_only': True,
        'available': observed, 'reason_code': reason,
        'trade_date': snapshot.get('trade_date'), 'as_of': snapshot.get('as_of'),
        'ingested_at': snapshot.get('ingested_at'),
        'raw_snapshot_sha256': snapshot.get('content_hash'),
        'raw_snapshot_available': snapshot.get('available') is True,
        'cutoff_contract_status': 'FORMAL_COLLECTOR_NOT_YET_WIRED',
        'acquisition_authenticated': False,
        'by_symbol': evidence_rows,
        'composite_score_status': 'DATA_LIMITED',
        'missing_required_windows': ['3d', '5d', '10d'],
    }
    evidence['content_hash'] = _content_hash(evidence)
    execution = unavailable_capital_flow_snapshot(
        as_of=as_of, expected_symbols=symbols,
        source_id=TENCENT_CAPITAL_FLOW_PROVIDER,
        reason_code='LOCAL_COMPOSITE_HISTORY_UNAVAILABLE',
    )
    execution.update(
        source_role='EXECUTION', evidence_only=True,
        raw_today_evidence_sha256=evidence['content_hash'],
        composite_score_status='DATA_LIMITED',
        weighting_policy='NO_RENORMALIZATION_NO_HISTORY_PADDING',
    )
    execution['content_hash'] = _content_hash(execution)
    return execution, evidence


def inspect_legacy_capital_weighting(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Annotate an actual symbol-factor baseline, without changing its score.

    This is an audit of the old formula, not acceptance or a new weighting
    policy. Board weights are deliberately not applied to symbol factors.
    """
    audit: dict[str, Any] = {
        'schema_version': 'capital-weighting-observation/1',
        'scope': 'SYMBOL_FACTOR_NOT_BOARD_FACTOR',
        'input_content_hash': snapshot.get('content_hash'),
        'source_id': snapshot.get('source_id'), 'by_symbol': {},
        'policy_approved': False, 'original_snapshot_mutated': False,
        'label_only': True,
        'original_weights': {key: weight for key, _, weight in WINDOWS},
    }
    if snapshot.get('content_hash') != _content_hash(snapshot):
        return {**audit, 'status': 'DATA_LIMITED', 'reason_code': 'INPUT_HASH_MISMATCH'}
    rows = snapshot.get('by_symbol')
    if not isinstance(rows, Mapping):
        return {**audit, 'status': 'DATA_LIMITED', 'reason_code': 'ROWS_UNPROVEN'}
    def number(value):
        return (float(value) if type(value) in (int, float)
                and math.isfinite(value) else None)
    for symbol, row in rows.items():
        row = row if isinstance(row, Mapping) else {}
        metrics = row.get('metrics')
        metrics = metrics if isinstance(metrics, Mapping) else {}
        observed, missing, weighted = [], [], 0.0
        for key, _, weight in WINDOWS:
            metric = metrics.get(key)
            metric = metric if isinstance(metric, Mapping) else {}
            value = number(metric.get('cross_section_percentile'))
            if metric.get('availability_state') == 'OBSERVED_VALUE' and value is not None and 0 <= value <= 100:
                observed.append((key, weight))
                weighted += value * weight
            else:
                missing.append(key)
        declared_weight = number(row.get('available_weight'))
        original_score = number(row.get('capital_flow_score'))
        actual_weight = sum(weight for _, weight in observed)
        formula_proven = (
            declared_weight is not None and actual_weight > 0
            and math.isclose(declared_weight, actual_weight, abs_tol=1e-6)
            and original_score is not None and any(key == 'today' for key, _ in observed)
            and math.isclose(original_score, weighted / actual_weight, abs_tol=0.000051)
            and row.get('available') is True
        )
        audit['by_symbol'][str(symbol)] = {
            'normalization_state': ('FULL_WINDOWS' if not missing else 'DEGRADED_RENORMALIZED')
                if formula_proven else 'DATA_LIMITED',
            'observed_weight': declared_weight if formula_proven else None,
            'observed_windows': [key for key, _ in observed], 'missing_windows': missing,
            'original_score': original_score,
            'original_score_preserved': True,
        }
    audit['status'] = 'OBSERVATION_ONLY'
    return audit


def capital_weighting_label(audit: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    """Copy a proven row label, keeping absent/invalid evidence UNKNOWN.

    These fields have no authority to accept/reject a candidate or change a
    score. In particular A-LABEL approval does not approve a weighting policy.
    """
    row = audit.get('by_symbol', {}).get(symbol, {})
    return {
        'normalization_state': row.get('normalization_state', 'DATA_LIMITED'),
        'observed_weight': row.get('observed_weight'),
        'observed_windows': list(row.get('observed_windows', [])),
        'missing_windows': list(row.get('missing_windows', [key for key, _, _ in WINDOWS])),
        'original_weights': dict(audit.get('original_weights', {})),
        'original_score': row.get('original_score'),
        'original_score_preserved': True,
        'input_content_hash': audit.get('input_content_hash'),
        'label_only': True,
        'policy_approved': False,
    }
