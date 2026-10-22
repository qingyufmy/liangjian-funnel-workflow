"""Build a reviewed-batch only from bound, captured isolated review originals.

No Store construction, provider calls, writes or green-status inputs. A6 raw
schema failures repaired by quota normalization are deliberately not accepted:
their effective READY result is not an originally validated model answer.
"""
from collections.abc import Mapping
from contextlib import closing
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from .team_v2_store import canonical

_SHA = re.compile('[0-9a-f]{64}')
_PRODUCTION = Path('/media/qy/120aebe1-8c98-412b-a490-3797cae5656b/Agu/liangjian-funnel-workflow')


def _require(value, reason):
    if not value:
        raise ValueError('REVIEWED_EVIDENCE_' + reason)


def _sha(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _text_hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _json(value):
    _require(isinstance(value, str) and len(value.encode()) <= 16 * 1024 * 1024, 'ORIGINAL_TOO_LARGE_OR_MISSING')
    body = json.loads(value)
    _require(canonical(body) == value, 'ORIGINAL_CANONICAL_CONFLICT')
    return body


def _ro(path, *, root, marker, version):
    _require(path.is_file() and not path.is_symlink() and path.resolve() == path
        and path.is_relative_to(root), 'SOURCE_PATH_INVALID')
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)
    try:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        _require([row[0] for row in db.execute('SELECT version FROM ' + marker)] == [version], 'DATABASE_MARKER_INVALID')
    except Exception:
        db.close()
        raise
    return db


def _original_call(db, *, call_id, role, session, day, source_day, rowset_sha, risk_sha):
    row = db.execute('SELECT * FROM model_calls WHERE call_id=?', (call_id,)).fetchone()
    _require(row is not None, 'MODEL_CAPTURE_MISSING')
    row = dict(row)
    captured_at = datetime.fromisoformat(row['captured_at'])
    _require(captured_at.utcoffset() is not None and captured_at <= datetime.now().astimezone(), 'MODEL_CAPTURE_TIME_INVALID')
    _require(row['role'] == role and row['session'] == session
        and row['execution_trade_date'] == day and row['source_trade_date'] == source_day, 'MODEL_SCOPE_MISMATCH')
    _require(row['validated'] == 1 and json.loads(row['issues_json']) == [], 'MODEL_RAW_NOT_VALIDATED')
    raw = _json(row['output_json'])
    _require(_text_hash(row['output_json']) == row['output_hash'], 'MODEL_RAW_HASH_CONFLICT')
    envelope = raw.get('envelope', {})
    _require(all(envelope.get(field) == row[column] for field, column in (
        ('run_id', 'run_id'), ('stage_id', 'role'), ('model_name', 'model'),
        ('source_trade_date', 'source_trade_date'), ('execution_trade_date', 'execution_trade_date')))
        and envelope.get('input_snapshot_ids') == [rowset_sha]
        and envelope.get('config_hash') == risk_sha, 'MODEL_ENVELOPE_BINDING_CONFLICT')
    judgments = []
    for ordinal, item in enumerate(db.execute('SELECT * FROM model_judgments WHERE call_id=? ORDER BY ordinal', (call_id,))):
        value = _json(item['judgment_json'])
        _require(item['ordinal'] == ordinal and item['validated'] == 1
            and _text_hash(item['judgment_json']) == item['judgment_sha256']
            and item['decision_id'] == _sha([call_id, ordinal, item['judgment_path']])
            and value.get('path') == item['judgment_path']
            and item['plan_id'] == value.get('plan_id') and item['symbol'] == value.get('symbol'), 'MODEL_JUDGMENT_HASH_CONFLICT')
        judgments.append((item['judgment_path'], item['judgment_json']))
    fields = ('run_id', 'role', 'session', 'source_trade_date', 'execution_trade_date', 'model',
        'prompt_hash', 'input_hash', 'catalog_hash', 'output_hash', 'latency_ms', 'provider_attempts',
        'validated', 'issues_json', 'sentinel_count', 'output_json')
    _require(_sha({**{field: row[field] for field in fields}, 'judgments': judgments}) == row['binding_sha256'],
        'MODEL_CAPTURE_BINDING_CONFLICT')
    claim = db.execute('SELECT state FROM model_call_claims WHERE call_id=?', (call_id,)).fetchone()
    _require(claim is not None and claim['state'] == 'CAPTURED', 'MODEL_CAPTURE_CLAIM_UNPROVEN')
    evidence = db.execute('SELECT catalog_json,catalog_sha256 FROM model_call_evidence WHERE call_id=?', (call_id,)).fetchone()
    _require(evidence is not None, 'MODEL_CATALOG_MISSING')
    catalog = _json(evidence['catalog_json'])
    _require(_text_hash(evidence['catalog_json']) == evidence['catalog_sha256'] == row['catalog_hash'], 'MODEL_CATALOG_HASH_CONFLICT')
    return row, raw, catalog


def _catalog_plans(catalog, source_rows, *, complete):
    expected = {row['plan_id']: row for row in source_rows}
    found = {}
    for item in catalog.get('items', []):
        if item.get('category') != 'PLAN':
            continue
        fact = item.get('value')
        _require(isinstance(fact, Mapping) and fact.get('plan_id') in expected, 'CATALOG_PLAN_SCOPE_CONFLICT')
        pid = fact['plan_id']
        row = expected[pid]
        _require(pid not in found and fact.get('symbol') == row['symbol']
            and fact.get('original_payload_sha256') == _text_hash(row['payload_json'])
            and fact.get('source_row_sha256') == _sha(row), 'CATALOG_PLAN_SOURCE_CONFLICT')
        found[pid] = item['id']
    _require(set(found) == set(expected) if complete else set(found) <= set(expected), 'CATALOG_PLAN_COVERAGE_CONFLICT')
    return {ref: pid for pid, ref in found.items()}


def _a6(team, models, *, meeting_id, scope_id, account_id, day, engine):
    row = team.execute('SELECT * FROM risk_meetings WHERE meeting_id=?', (meeting_id,)).fetchone()
    claim = team.execute('SELECT input_sha256 FROM risk_session_requests WHERE meeting_id=?', (meeting_id,)).fetchone()
    _require(row is not None and claim is not None, 'A6_MEETING_OR_REQUEST_MISSING')
    row = dict(row)
    _require(row['account_id'] == account_id and row['trade_date'] == day
        and row['source_call_id'].startswith(scope_id + ':'), 'A6_MEETING_SCOPE_CONFLICT')
    _require(_SHA.fullmatch(claim['input_sha256']) is not None, 'A6_REQUEST_HASH_INVALID')
    inputs, result = _json(row['input_json']), _json(row['result_json'])
    _require(_text_hash(row['input_json']) == row['binding_sha256'], 'A6_MEETING_INPUT_HASH_CONFLICT')
    _require(inputs.get('validated') is True and inputs.get('account_id') == account_id
        and inputs.get('trade_date') == day and inputs.get('source_call_id') == row['source_call_id']
        and inputs.get('envelopes') == engine['envelopes']
        and Decimal(str(inputs.get('engine_utilization_cap'))) == Decimal(str(engine['engine_utilization_cap'])), 'A6_ENGINE_INPUT_CONFLICT')
    source = engine['source']
    captured, raw, catalog = _original_call(models, call_id=row['source_call_id'], role='A6', session='PREMARKET',
        day=day, source_day=source['source_trade_date'], rowset_sha=source['plan_rowset_sha256'], risk_sha=engine['risk_limits_sha256'])
    _require(inputs['output'] == raw, 'A6_RAW_MEETING_OUTPUT_CONFLICT')
    _catalog_plans(catalog, source['plan_rows'], complete=True)
    projection = team.execute('SELECT source_sha256,body_json FROM control_minute_context WHERE decision_id=?',
        ('a6-input-projection:' + meeting_id,)).fetchone()
    _require(projection is not None and projection['source_sha256'] == captured['input_hash']
        and _json(projection['body_json']).get('projected_input_hash') == captured['input_hash'], 'A6_MODEL_INPUT_BINDING_CONFLICT')
    from .team_v2_morning import _envelopes
    from .team_v2_risk_meeting import apply_premarket_review
    actual = apply_premarket_review(_envelopes(inputs['envelopes']), raw,
        engine_utilization_cap=Decimal(str(inputs['engine_utilization_cap'])), validated=True, previous_failures=0)
    _require(result.get('status') == 'READY' and result.get('consecutive_failures') == 0
        and json.loads(canonical(asdict(actual))) == result, 'A6_RESULT_RECOMPUTATION_CONFLICT')
    result_sha = _sha(result)
    evidence = dict(meeting_id=meeting_id, call_id=row['source_call_id'], input_sha256=claim['input_sha256'],
        raw_sha256=captured['output_hash'], journal_ref='state/team_v2/model.db#' + row['source_call_id'], validated=True,
        risk_result_sha256=result_sha, engine_envelopes_sha256=_sha(engine['envelopes']),
        plan_rowset_sha256=source['plan_rowset_sha256'], binding_sha256=row['binding_sha256'],
        model_input_sha256=captured['input_hash'], meeting_record=dict(meeting_id=meeting_id,
            source_call_id=row['source_call_id'], input_sha256=claim['input_sha256'], result_sha256=result_sha))
    return result, evidence, captured['run_id']


def _pm(team, models, *, meeting_id, scope_id, account_id, day, engine, risk, review_run):
    row = team.execute('SELECT * FROM pm_decisions WHERE meeting_id=?', (meeting_id,)).fetchone()
    _require(row is not None, 'PM_MEETING_MISSING')
    row = dict(row)
    _require(row['account_id'] == account_id and row['trade_date'] == day
        and row['source_call_id'].startswith(scope_id + ':') and _SHA.fullmatch(row['input_sha256']) is not None, 'PM_MEETING_SCOPE_CONFLICT')
    result = _json(row['result_json'])
    _require(result.get('status') == 'READY' and result.get('model_validated') is True
        and result.get('decision_authority') == 'VALIDATED_MODEL'
        and not any(key.startswith('fallback_') for key in result)
        and result.get('source_call_id') == row['source_call_id'], 'PM_NOT_MODEL_VALIDATED')
    source = engine['source']
    captured, raw, catalog = _original_call(models, call_id=row['source_call_id'], role='A3', session='PM_PREMARKET',
        day=day, source_day=source['source_trade_date'], rowset_sha=source['plan_rowset_sha256'], risk_sha=engine['risk_limits_sha256'])
    _require(captured['run_id'] == review_run and result.get('input_hash') == captured['input_hash']
        and result.get('server_projection', {}).get('raw_output_sha256') == captured['output_hash'], 'PM_MODEL_INPUT_OR_RAW_CONFLICT')
    plan_refs = _catalog_plans(catalog, source['plan_rows'], complete=False)
    from .team_v2_morning import _envelopes
    from ..pipeline.team_v2_pm_schema import project_pm_compact
    envelopes = _envelopes(risk['envelopes'])
    clusters = {plan['plan_id']: plan['cluster'] for plan in source['plans']}
    bounds = tuple(min(Decimal(str(value)), Decimal(risk['utilization_cap'])) for value in engine['baseline_range'])
    derived, checked = project_pm_compact(raw, catalog, plan_refs=plan_refs, required_envelope=raw['envelope'],
        envelopes=envelopes, plan_clusters=clusters, free_slots=engine['capacity_slots'], baseline_range=bounds,
        utilization_cap=Decimal(risk['utilization_cap']), high_conviction_ids=set())
    _require(checked.valid, 'PM_RAW_REVALIDATION_FAILED')
    _require(all(result.get(key) == derived.get(key) for key in
        ('slot_plan', 'plan_ranking', 'derived_shortfall', 'plan_allocations'))
        and result.get('server_projection', {}).get('derived_output_sha256') == _sha(derived)
        and result.get('allocation_plan_bindings') == {item.plan_id: [item.symbol, clusters[item.plan_id]] for item in envelopes}
        and all(result.get(key) == raw.get(key) for key in
            ('deployment_target', 'shortfall_reason', 'handoff_notes', 'what_would_change_my_mind')), 'PM_RESULT_RECOMPUTATION_CONFLICT')
    return result, dict(meeting_id=meeting_id, call_id=row['source_call_id'], input_sha256=row['input_sha256'],
        raw_sha256=captured['output_hash'], journal_ref='state/team_v2/model.db#' + row['source_call_id'], validated=True,
        model_input_sha256=captured['input_hash'], result_sha256=_sha(result))


def build_reviewed_batch_evidence(*, isolated_root=None, review_root=None, source_namespace='ISOLATED',
        workflow_db_path=None, scope_id, account_id, trade_date, lane_id, run_id,
        a6_meeting_id, pm_meeting_id, candidate_rows):
    """Return a bundle only after actual read-only originals prove both reviews.

    This builds inert evidence. It never stages, publishes, activates, replaces
    plans or grants a switch outside the separate PRE_AUCTION commit gate.
    """
    if source_namespace not in {'ISOLATED','NATURAL_MORNING'}:raise ValueError('REVIEWED_EVIDENCE_NAMESPACE_INVALID')
    if (source_namespace=='ISOLATED' and (isolated_root is None or review_root is not None)
            or source_namespace=='NATURAL_MORNING' and (review_root is None or isolated_root is not None)):
        raise ValueError('REVIEWED_EVIDENCE_NAMESPACE_ROOT_INVALID')
    root = Path(isolated_root if source_namespace=='ISOLATED' else review_root)
    _require(root.is_absolute() and root.resolve() == root and root.is_dir() and not root.is_symlink()
        and (source_namespace!='ISOLATED' or root != _PRODUCTION and not root.is_relative_to(_PRODUCTION))
        and len(root.parts) >= 3, 'ISOLATED_ROOT_INVALID')
    _require(isinstance(scope_id, str) and re.fullmatch('[A-Za-z0-9_-]{1,40}', scope_id) is not None, 'SCOPE_ID_INVALID')
    _require(all(isinstance(value, str) and value.startswith(scope_id + ':') for value in (a6_meeting_id, pm_meeting_id))
        and a6_meeting_id != pm_meeting_id, 'INDEPENDENT_MEETING_IDS_REQUIRED')
    try:
        with closing(_ro(root / 'state/team_v2/team.db', root=root, marker='team_v2_meta', version='team-runtime/1')) as team, \
                closing(_ro(root / 'state/team_v2/model.db', root=root, marker='model_research_meta', version='model-research/1')) as models:
            row = team.execute('SELECT body_json FROM morning_engine WHERE account_id=? AND trade_date=?', (account_id, trade_date)).fetchone()
            _require(row is not None, 'ENGINE_ORIGINAL_MISSING')
            engine = _json(row['body_json'])
            source = engine.get('source', {})
            _require(engine.get('status') == 'COMPLETE' and engine.get('account_id') == account_id
                and engine.get('trade_date') == trade_date and source.get('status') == 'COMPLETE'
                and source.get('trade_date') == trade_date and source.get('lane_id') == lane_id
                and _sha(source.get('plan_rows')) == source.get('plan_rowset_sha256'), 'ENGINE_SCOPE_OR_ROWSET_CONFLICT')
            natural=None
            if source_namespace=='NATURAL_MORNING':
                from types import SimpleNamespace
                from .reviewed_batch_natural import read_natural_candidate
                path=Path(workflow_db_path)
                _require(path.resolve().is_relative_to(root), 'NATURAL_WORKFLOW_OUTSIDE_ROOT')
                natural=read_natural_candidate(SimpleNamespace(state_db_path=path,research_primary_lane_id=lane_id),day=trade_date)
                _require(natural is not None and natural['scope_id']==scope_id
                    and source.get('source_namespace')=='NATURAL_MORNING_CANDIDATE'
                    and source.get('candidate_scope_id')==scope_id
                    and source.get('candidate_sha256')==natural['body_sha256']
                    and source.get('candidate_revision')==natural['revision']
                    and source['plan_rows']==natural['body']['plan_rows']
                    and natural['body']['run_id']==run_id, 'NATURAL_CANDIDATE_SOURCE_CONFLICT')
            from .calendar import ExchangeTradingCalendar
            calendar = ExchangeTradingCalendar()
            target = date.fromisoformat(trade_date)
            _require(calendar.is_trading_day(target)
                and source.get('source_trade_date') == calendar.previous_trading_day(target).isoformat(), 'SOURCE_TARGET_CALENDAR_CONFLICT')
            _require(isinstance(candidate_rows, list) and candidate_rows
                and all(row.get('payload', {}).get('source_run_id') == run_id for row in candidate_rows), 'CANDIDATE_SOURCE_RUN_INVALID')
            risk, a6_evidence, review_run = _a6(team, models, meeting_id=a6_meeting_id, scope_id=scope_id,
                account_id=account_id, day=trade_date, engine=engine)
            pm, pm_evidence = _pm(team, models, meeting_id=pm_meeting_id, scope_id=scope_id,
                account_id=account_id, day=trade_date, engine=engine, risk=risk, review_run=review_run)
            _require(team.execute('PRAGMA query_only').fetchone()[0] == models.execute('PRAGMA query_only').fetchone()[0] == 1,
                'READ_ONLY_GUARD_FAILED')
            body = dict(schema_version='reviewed-batch/1', switch_mode='PRE_AUCTION', scope_id=scope_id,
                account_id=account_id, trade_date=trade_date, lane_id=lane_id, run_id=run_id,
                status='READY', engine=engine, risk=risk, pm=pm, candidate_rows=candidate_rows,
                evidence=dict(a6=a6_evidence, pm=pm_evidence))
            if natural is not None:
                body.update(review_source_namespace='NATURAL_MORNING',natural_candidate=dict(scope_id=scope_id,
                    body_sha256=natural['body_sha256'],revision=natural['revision']))
            from .reviewed_batch_switch import validate_bundle
            return validate_bundle(body)
    except sqlite3.Error as exc:
        raise ValueError('REVIEWED_EVIDENCE_DATABASE_SOURCE_UNAVAILABLE') from exc
