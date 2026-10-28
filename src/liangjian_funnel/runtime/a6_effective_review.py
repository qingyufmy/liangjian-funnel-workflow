"""One validation/projection contract shared by A6, PM and CAS evidence."""
from ..pipeline.team_v2_contract import TeamValidation
from ..pipeline.team_v2_effective_risk import nonexistent_gap_ids, project_effective_risk
from ..pipeline.team_v2_risk_schema import validate_risk_premarket
from .regeneration_data_limits import gap_reviewed_ids


def validate_review(raw, catalog, arguments, requirements, *, derived=False):
    checked = validate_risk_premarket(raw, catalog, **arguments)
    missing = set(requirements) - gap_reviewed_ids(raw, requirements, allow_capacity_trimmed=derived)
    rows=raw.get('envelope_adjustments',[]) if isinstance(raw,dict) else []
    rows=rows if isinstance(rows,list) else []
    reserved = [r['plan_id'] for r in rows
        if isinstance(r,dict) and isinstance(r.get('plan_id'),str)
        and r.get('reason_code')=='CAPACITY_TRIMMED'] if isinstance(raw,dict) and not derived else []
    issues = tuple(dict.fromkeys((*checked.issues,
        *(f'A6_SERVER_REASON_CODE_RESERVED:{pid}' for pid in reserved),
        *(f'A6_DATA_LIMITED_REVIEW_REQUIRED:{pid}' for pid in sorted(missing)),
        *(f'A6_NONEXISTENT_SOURCE_GAP_REFERENCE:{pid}'
          for pid in sorted(nonexistent_gap_ids(raw, requirements))))))
    return TeamValidation(not issues, issues, checked.judgments, checked.sentinel_count)


def effective_review(raw, *, catalog, arguments, requirements, quota_context,
                     captured_issues=()):
    checked = validate_review(raw, catalog, arguments, requirements)
    issues = tuple(dict.fromkeys((*captured_issues, *checked.issues)))
    from .team_v2_risk_meeting import apply_premarket_review
    tightened=apply_premarket_review(arguments['envelopes'],raw,
        engine_utilization_cap=arguments['engine_utilization_cap'],validated=True,previous_failures=0)
    return project_effective_risk(raw, quota_context=quota_context,
        requirements=requirements,
        eligible_plan_ids={e.plan_id for e in tightened.envelopes if e.eligible},
        original_issues=issues,
        validate=lambda value: validate_review(value, catalog, arguments, requirements, derived=True))


def rebuild_effective_review(team, *, meeting_id, captured, raw, catalog, engine):
    """Never trust a stored derived READY label; recompute exact bound proof."""
    from .team_v2_morning import _envelopes
    from .team_v2_sources import meeting_plan_facts
    from .team_v2_pm_policy import priority_order
    from .regeneration_data_limits import a6_gap_requirements
    from ..pipeline.team_v2_cluster_quota import build_cluster_quota_context
    from .team_v2_store import canonical
    import hashlib, json
    envelopes = _envelopes(engine['envelopes'])
    context = build_cluster_quota_context(engine_slots=engine['capacity_slots'],
        cluster_caps=engine['cluster_caps'],
        plan_clusters={p['plan_id']: p['cluster'] for p in engine['source']['plans']},
        plan_priority_order=priority_order(meeting_plan_facts(engine['source']['plan_rows'])))
    arguments = dict(required_envelope=raw['envelope'], envelopes=envelopes,
        engine_utilization_cap=engine['engine_utilization_cap'], capacity_slots=engine['capacity_slots'],
        cluster_caps=engine['cluster_caps'], unready_plan_ids={e.plan_id for e in envelopes if not e.eligible},
        cluster_plan_counts=context['cluster_plan_counts'])
    requirements = a6_gap_requirements(catalog, envelopes)
    stored = team.execute('SELECT source_sha256,body_json FROM control_minute_context WHERE decision_id=?',
                          ('a6-effective-review:'+meeting_id,)).fetchone()
    if stored is None:
        if captured['validated'] != 1 or json.loads(captured['issues_json']) != []:
            raise ValueError('A6_EFFECTIVE_ORIGINAL_NOT_VALIDATED')
        if not validate_review(raw, catalog, arguments, requirements).valid:
            raise ValueError('A6_EFFECTIVE_ORIGINAL_REVALIDATION_FAILED')
        return raw, None
    body = json.loads(stored['body_json'])
    projection = effective_review(raw, catalog=catalog, arguments=arguments,
        requirements=requirements, quota_context=context,
        captured_issues=json.loads(captured['issues_json']))
    if (stored['source_sha256'] != captured['input_hash'] or projection is None
            or body != projection[1] or canonical(body) != stored['body_json']):
        raise ValueError('A6_EFFECTIVE_PROOF_RECOMPUTATION_CONFLICT')
    return projection[0], dict(schema_version=body['schema_version'],
        derived_proof_sha256=hashlib.sha256(stored['body_json'].encode()).hexdigest(),
        effective_raw_sha256=body['effective_raw_sha256'],
        original_validated=bool(captured['validated']),
        capacity_arithmetic_invalid=body['capacity_arithmetic_invalid'],
        projection_kind=body['projection_kind'],
        candidate_count=body['candidate_count'], candidate_eligible_count=body['candidate_eligible_count'],
        capacity_trimmed_count=body['capacity_trimmed_count'])


def verify_premarket_capture(directory, *, engine, meeting_id, scope_id, account_id, day):
    from contextlib import closing
    import sqlite3
    from .reviewed_batch_evidence import _a6
    connections = []
    try:
        for name in ('team.db', 'model.db'):
            db = sqlite3.connect((directory/name).as_uri()+'?mode=ro', uri=True, timeout=2)
            connections.append(db)
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA query_only=ON')
        _a6(*connections, meeting_id=meeting_id, scope_id=scope_id,
            account_id=account_id, day=day, engine=engine)
        return True
    except (ValueError, sqlite3.Error, KeyError, TypeError):
        return False
    finally:
        for db in connections:
            db.close()


def read_capacity_summary(workflow_path, *, day, cutoff):
    """A5 reads the committed original bundle, never estimates from plan count."""
    from .reviewed_batch_switch import load_active_reviewed_batch
    from pathlib import Path
    from contextlib import closing
    from datetime import datetime
    import sqlite3
    workflow_path=Path(workflow_path)
    try:
        active = load_active_reviewed_batch(workflow_path, account_id='local-paper', trade_date=day)
        if active is None:
            return dict(status='NOT_AVAILABLE', reason_code='NO_REVIEWED_CAPACITY_BINDING')
        with closing(sqlite3.connect(workflow_path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            row = db.execute('SELECT switched_at FROM reviewed_batch_switch_audit WHERE scope_id=?',
                             (active['scope_id'],)).fetchone()
        if row is None or datetime.fromisoformat(row[0]) > cutoff:
            return dict(status='NOT_AVAILABLE', reason_code='CAPACITY_BINDING_AFTER_CUTOFF')
        proof = active['bundle']['evidence']['a6'].get('effective_review')
        if proof is None:
            return dict(status='NOT_AVAILABLE', reason_code='LEGACY_REVIEW_NO_CAPACITY_PROJECTION')
        if not proof['capacity_arithmetic_invalid']:
            return dict(status='UNCHANGED_CAPACITY', reason_code='LABEL_ONLY_ORIGINAL_QUALIFICATIONS_PRESERVED')
        envelopes=active['bundle']['risk']['envelopes']
        trimmed=[e['plan_id'] for e in envelopes if 'CAPACITY_TRIMMED' in e.get('blocked_reasons',[])]
        eligible=[e['plan_id'] for e in envelopes if e['eligible']]
        if (len(envelopes)!=proof['candidate_count'] or len(trimmed)!=proof['capacity_trimmed_count']
                or len(eligible)!=proof['candidate_eligible_count']):
            raise ValueError('CAPACITY_SUMMARY_RESULT_CONFLICT')
        return dict(status='AVAILABLE', scope_id=active['scope_id'], bundle_sha256=active['bundle_sha256'],
            trimmed_plan_ids=trimmed,eligible_plan_ids=eligible,
            **{key:proof[key] for key in ('candidate_count','candidate_eligible_count',
                                         'capacity_trimmed_count','derived_proof_sha256')})
    except (ValueError, sqlite3.Error, KeyError, OSError):
        return dict(status='NOT_AVAILABLE', reason_code='CAPACITY_BINDING_UNREADABLE')
