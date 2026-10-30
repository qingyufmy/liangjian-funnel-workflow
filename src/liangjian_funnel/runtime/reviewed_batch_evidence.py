"""Build a reviewed-batch only from bound, captured isolated review originals.

No Store construction, provider calls, writes or green-status inputs. A6 raw
remains immutable; the narrowly approved LJ-099 projection is independently
recomputed and bound separately from original model validation.
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
from zoneinfo import ZoneInfo

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


def _original_call(db, *, call_id, role, session, day, source_day, rowset_sha, risk_sha, allow_effective_a6=False):
    row = db.execute('SELECT * FROM model_calls WHERE call_id=?', (call_id,)).fetchone()
    _require(row is not None, 'MODEL_CAPTURE_MISSING')
    row = dict(row)
    captured_at = datetime.fromisoformat(row['captured_at'])
    _require(captured_at.utcoffset() is not None and captured_at <= datetime.now().astimezone(), 'MODEL_CAPTURE_TIME_INVALID')
    _require(row['role'] == role and row['session'] == session
        and row['execution_trade_date'] == day and row['source_trade_date'] == source_day, 'MODEL_SCOPE_MISMATCH')
    from ..pipeline.team_v2_effective_risk import repairable_issue
    captured_issues=json.loads(row['issues_json'])
    _require((row['validated'] == 1 and captured_issues == [])
        or (allow_effective_a6 and role == 'A6' and session == 'PREMARKET'
            and row['validated']==0 and captured_issues and all(repairable_issue(x) for x in captured_issues)), 'MODEL_RAW_NOT_VALIDATED')
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
        _require(item['ordinal'] == ordinal and item['validated'] == row['validated']
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
        day=day, source_day=source['source_trade_date'], rowset_sha=source['plan_rowset_sha256'], risk_sha=engine['risk_limits_sha256'], allow_effective_a6=team.execute(
            "SELECT 1 FROM control_minute_context WHERE decision_id=?",('a6-effective-review:'+meeting_id,)).fetchone() is not None)
    if engine.get('morning_window_authorization') is not None:
        from .morning_window_authority import morning_deadline
        cutoff=morning_deadline(day,source['source_trade_date'],authorization=engine['morning_window_authorization'],source=source)
        _require(datetime.fromisoformat(captured['captured_at'])<cutoff,'A6_CAPTURE_AFTER_DEADLINE')
    from .a6_effective_review import rebuild_effective_review
    effective, effective_proof = rebuild_effective_review(team, meeting_id=meeting_id,
        captured=captured, raw=raw, catalog=catalog, engine=engine)
    _require(inputs['output'] == effective, 'A6_RAW_MEETING_OUTPUT_CONFLICT')
    _catalog_plans(catalog, source['plan_rows'], complete=True)
    projection = team.execute('SELECT source_sha256,body_json FROM control_minute_context WHERE decision_id=?',
        ('a6-input-projection:' + meeting_id,)).fetchone()
    _require(projection is not None and projection['source_sha256'] == captured['input_hash']
        and _json(projection['body_json']).get('projected_input_hash') == captured['input_hash'], 'A6_MODEL_INPUT_BINDING_CONFLICT')
    from .team_v2_morning import _envelopes
    from .team_v2_risk_meeting import apply_premarket_review
    actual = apply_premarket_review(_envelopes(inputs['envelopes']), effective,
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
    if effective_proof is not None:evidence['effective_review'] = effective_proof
    return result, evidence, captured['run_id']


def _pm_deadline_in_session(deadline,day,*,authorization=None,source=None):
    if not isinstance(deadline,datetime) or deadline.utcoffset() is None:return False
    local=deadline.astimezone(ZoneInfo('Asia/Shanghai'))
    from .morning_window_authority import morning_deadline
    cutoff=morning_deadline(day,source.get('source_trade_date')if source else None,authorization=authorization,source=source)
    return local.date().isoformat()==day and local<=cutoff


def _pm(team, models, *, meeting_id, scope_id, account_id, day, engine, risk, review_run,subset_ids=None):
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
    if result.get('server_projection',{}).get('schema')=='PM_BATCH_SET_PROJECTION/1':
        _require(subset_ids is None,'PM_NESTED_BATCHES_FORBIDDEN')
        return _pm_batches(team,models,row=row,result=result,scope_id=scope_id,account_id=account_id,
            day=day,engine=engine,risk=risk,review_run=review_run)
    captured, raw, catalog = _original_call(models, call_id=row['source_call_id'], role='A3', session='PM_PREMARKET',
        day=day, source_day=source['source_trade_date'], rowset_sha=source['plan_rowset_sha256'], risk_sha=engine['risk_limits_sha256'])
    _require(captured['run_id'] == review_run and result.get('input_hash') == captured['input_hash']
        and result.get('server_projection', {}).get('raw_output_sha256') == captured['output_hash'], 'PM_MODEL_INPUT_OR_RAW_CONFLICT')
    if result.get('deadline_audit'):
        deadline=datetime.fromisoformat(result['deadline_audit']['deadline_at'])
        _require(_pm_deadline_in_session(deadline,day,authorization=result['deadline_audit'].get('window_authorization'),source=source) and engine.get('morning_window_authorization')==result['deadline_audit'].get('window_authorization') and datetime.fromisoformat(captured['captured_at'])<deadline,
            'PM_CAPTURE_AFTER_DEADLINE')
    plan_refs = _catalog_plans(catalog, source['plan_rows'], complete=False)
    from .team_v2_morning import _envelopes
    from ..pipeline.team_v2_pm_schema import project_pm_compact
    envelopes = _envelopes(risk['envelopes'])
    clusters = {plan['plan_id']: plan['cluster'] for plan in source['plans']}
    if subset_ids is not None:
        _require(set(plan_refs.values())==set(subset_ids),'PM_BATCH_SOURCE_COVERAGE_CONFLICT')
        envelopes=tuple(e for e in envelopes if e.plan_id in subset_ids)
        clusters={pid:cluster for pid,cluster in clusters.items() if pid in subset_ids}
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


def _pm_batches(team,models,*,row,result,scope_id,account_id,day,engine,risk,review_run):
    """Revalidate real children; an aggregate has no pretend model_calls row."""
    from .team_v2_morning import _envelopes
    from .team_v2_sources import meeting_plan_facts
    from .team_v2_pm_policy import priority_order
    from .team_v2_pm_geometry import pm_geometry_domain
    from .team_v2_pm_batches import aggregate_pm_outputs
    source=engine['source'];audit=result.get('batch_audit') or {}
    envelopes=_envelopes(risk['envelopes']);by_id={e.plan_id:e for e in envelopes}
    geometry,_=pm_geometry_domain(source['plan_rows'])
    expected=[pid for pid in priority_order(meeting_plan_facts(source['plan_rows']))
        if pid in geometry and by_id[pid].eligible and by_id[pid].risk_unit in {'STANDARD','PROBE'}]
    max_positions=engine['capacity_slots']+len({p['symbol'] for p in engine['snapshot']['positions']})
    capacity=2*max_positions
    _require(audit.get('version')=='pm-batches/1' and audit.get('parallel') is True
        and audit.get('expected_plan_ids')==expected and audit.get('capacity')==capacity,'PM_BATCH_DOMAIN_CONFLICT')
    deadline=datetime.fromisoformat(audit['deadline_at'])
    _require(_pm_deadline_in_session(deadline,day,authorization=audit.get('window_authorization'),source=source) and engine.get('morning_window_authorization')==audit.get('window_authorization'),'PM_BATCH_DEADLINE_CONFLICT')
    groups=[expected[i:i+capacity] for i in range(0,len(expected),capacity)]
    batches=audit.get('batches',[])
    _require(len(batches)==len(groups),'PM_BATCH_PARTITION_CONFLICT')
    outputs=[];original_set=[];reviewed=[];unreviewed=[];catalog_items={};categories=None;child_proofs=[]
    for ordinal,(batch,ids) in enumerate(zip(batches,groups),1):
        call=row['source_call_id']+f':batch:{ordinal:03}'
        _require(batch.get('ordinal')==ordinal and batch.get('plan_ids')==ids
            and batch.get('call_id')==call and batch.get('meeting_id')==row['meeting_id']+f':batch:{ordinal:03}',
            'PM_BATCH_IDENTITY_CONFLICT')
        if batch.get('status')!='VALIDATED_MODEL':
            _require(batch.get('status')=='PM_NOT_REVIEWED_TIME_BUDGET','PM_BATCH_FAILURE_CANNOT_GRANT')
            trace=models.execute('SELECT body_json,body_sha256 FROM model_call_telemetry WHERE call_id=?',(call,)).fetchone()
            timeout=False
            if trace:
                _require(_text_hash(trace['body_json'])==trace['body_sha256'],'PM_BATCH_FAILURE_TRACE_CONFLICT')
                timeout=json.loads(trace['body_json']).get('failure_code') in {
                    'PM_FIRST_PACKET_TIMEOUT','PM_TOTAL_TIMEOUT','MODEL_WALL_CLOCK_TIMEOUT','MODEL_TOTAL_DEADLINE_EXCEEDED'}
            observed=datetime.fromisoformat(audit['cutoff_observed_at'])
            budget=team.execute('SELECT source_sha256,body_json FROM control_minute_context WHERE decision_id=?',
                ('pm-batch-budget:'+batch['meeting_id'],)).fetchone()
            _require(budget is not None and budget['source_sha256']==row['input_sha256'], 'PM_BATCH_BUDGET_ORIGINAL_MISSING')
            frozen=json.loads(budget['body_json'])['timeout_seconds']
            not_enough=batch.get('remaining_seconds',0)<frozen or frozen<=0
            _require(timeout or not_enough or observed.timestamp()>=deadline.timestamp()-2,'PM_BATCH_UNREVIEWED_UNPROVEN')
            unreviewed.extend(ids);continue
        child,proof=_pm(team,models,meeting_id=batch['meeting_id'],scope_id=scope_id,
            account_id=account_id,day=day,engine=engine,risk=risk,review_run=review_run,subset_ids=set(ids))
        _require(_sha(child)==batch.get('result_sha256'),'PM_BATCH_RESULT_HASH_CONFLICT')
        captured,raw,catalog=_original_call(models,call_id=call,role='A3',session='PM_PREMARKET',
            day=day,source_day=source['source_trade_date'],rowset_sha=source['plan_rowset_sha256'],risk_sha=engine['risk_limits_sha256'])
        _require(captured['input_hash']==batch.get('input_hash') and captured['output_hash']==batch.get('raw_sha256')
            and datetime.fromisoformat(captured['captured_at'])<deadline,'PM_BATCH_CAPTURE_CUTOFF_CONFLICT')
        if categories is None:categories=catalog['categories']
        _require(categories==catalog['categories'],'PM_BATCH_CATALOG_CATEGORY_CONFLICT')
        for item in catalog['items']:
            _require(item['id'] not in catalog_items or catalog_items[item['id']]==item,'PM_BATCH_SHARED_FACT_CONFLICT')
            catalog_items[item['id']]=item
        reviewed.extend(ids);outputs.append(raw);child_proofs.append(proof)
        original_set.append(dict(call_id=call,input_hash=captured['input_hash'],raw_sha256=captured['output_hash']))
    _require(outputs and audit.get('reviewed_plan_ids')==[pid for pid in expected if pid in reviewed]
        and audit.get('unreviewed_plan_ids')==[pid for pid in expected if pid in unreviewed]
        and set(reviewed).isdisjoint(unreviewed) and set(reviewed)|set(unreviewed)==set(expected),'PM_BATCH_COVERAGE_CONFLICT')
    catalog=dict(categories=categories,items=list(catalog_items.values()))
    refs=_catalog_plans(catalog,source['plan_rows'],complete=False)
    clusters={p['plan_id']:p['cluster'] for p in source['plans']}
    bounds=tuple(min(Decimal(str(value)),Decimal(risk['utilization_cap'])) for value in engine['baseline_range'])
    raw,derived=aggregate_pm_outputs(outputs,catalog=catalog,plan_refs=refs,context=dict(required_envelope=outputs[0]['envelope'],
        envelopes=envelopes,plan_clusters=clusters,free_slots=engine['capacity_slots'],baseline_range=bounds,
        utilization_cap=Decimal(risk['utilization_cap']),high_conviction_ids=set()))
    projection=result['server_projection']
    _require(projection.get('original_calls')==original_set and projection.get('raw_output_sha256')==_sha(original_set)
        and projection.get('derived_output_sha256')==_sha(derived)
        and all(result.get(key)==derived.get(key) for key in ('slot_plan','plan_ranking','plan_allocations','derived_shortfall'))
        and all(result.get(key)==raw.get(key) for key in ('deployment_target','shortfall_reason','handoff_notes','what_would_change_my_mind')),
        'PM_BATCH_RECOMPUTATION_CONFLICT')
    allocated={x['plan_id'] for x in result['plan_allocations']}
    _require(allocated<=set(reviewed) and not allocated&set(unreviewed),'PM_BATCH_UNREVIEWED_ALLOCATION_FORBIDDEN')
    return result,dict(meeting_id=row['meeting_id'],call_id=row['source_call_id'],input_sha256=row['input_sha256'],
        raw_sha256=_sha(original_set),journal_ref='state/team_v2/model.db#ACTUAL_BATCH_SET:'+_sha(original_set),
        validated=True,source_kind='ACTUAL_MODEL_CALL_SET',calls=child_proofs,result_sha256=_sha(result))


def build_reviewed_batch_evidence(*, isolated_root=None, review_root=None, source_namespace='ISOLATED',
        workflow_db_path=None, workflow_output_dir=None, minute_cache_dir=None, scope_id, account_id, trade_date, lane_id, run_id,
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
                natural=read_natural_candidate(SimpleNamespace(state_db_path=path,research_primary_lane_id=lane_id,
                    workflow_output_dir=Path(workflow_output_dir or Path(review_root)/'outputs'),
                    minute_cache_dir=Path(minute_cache_dir or Path(review_root)/'storage/minute')),day=trade_date)
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
            if engine.get('morning_window_authorization')is not None:body['morning_window_authorization']=engine['morning_window_authorization']
            from .reviewed_batch_switch import validate_bundle
            return validate_bundle(body)
    except sqlite3.Error as exc:
        raise ValueError('REVIEWED_EVIDENCE_DATABASE_SOURCE_UNAVAILABLE') from exc
