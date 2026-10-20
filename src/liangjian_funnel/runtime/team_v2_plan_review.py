"""Isolated A3 close blind-review transport and after-the-fact comparison.

This callable is a prerequisite for the formal model dry-run and natural
research adapter. It does not publish plans, fetch market data, notify anyone,
or activate itself in the legacy close research workflow.
"""
from collections.abc import Mapping
from contextlib import closing
from datetime import date, datetime
import hashlib
import json
import time
from zoneinfo import ZoneInfo

from ..pipeline.team_v2_contract import blind_plan_facts
from ..pipeline.team_v2_plan_review_schema import validate_plan_review
from ..pipeline.team_v2_a3_retry import a3_validation_feedback,bind_a3_retry_feedback
from ..pipeline.team_v2_model import team_call_binding
from .team_v2_eod import _response
from .team_v2_plan_recovery import recover_plan_review
from .team_v2_store import canonical

FACT_FIELDS = set('as_of strategy_version monthly_state monthly_partial_observation weekly_closed_state '
    'weekly_partial_observation daily_state daily_ma daily_macd daily_volume_state relative_strength '
    'setup_pattern cycle_alignment entry_reference_zone no_chase_price daily_invalidation reference_price '
    'reference_price_as_of price_discovery a4_deferred_conditions a4_required_entry_rules a4_exit_rules '
    'plan_expiry reward_risk stop_distance_pct price_levels_hash factor_snapshot_hash strategy_facts '
    'tradability event_calendar liquidity'.split())
DECISION_FIELDS = set('status eligibility eligible gate_results required_conditions met_conditions '
    'unmet_conditions veto_conditions first_blocking_gate all_failed_gates publication_state research_state '
    'execution_permission research_only_reason reason_codes plan_mode plan_priority priority_reasons '
    'strategy_checks review_status technical_score score_breakdown engine_verdict trigger_result'.split())


def _hash(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _blind(value):
    if isinstance(value, Mapping):
        return {k: _blind(v) for k, v in value.items() if k not in DECISION_FIELDS}
    if isinstance(value, (list, tuple)):
        return [_blind(v) for v in value]
    return value


def plan_review_inputs(replacements, candidate_domain, engine_decisions):
    """Explicit technical projection; conclusions stay in the owned source.

    Facts are not fabricated from a PASS/FAIL gate. An absent source field is
    absent in the prompt; a model must return DATA_GAP if required facts lack.
    """
    if (not isinstance(engine_decisions, Mapping) or set(engine_decisions) != set(candidate_domain)
            or any(not isinstance(v, Mapping) or type(v.get('eligible')) is not bool
                for v in engine_decisions.values())):
        raise ValueError('A3_PLAN_ENGINE_DOMAIN_INVALID')
    facts = replacements.get('A3_FACT_SHEET')
    if not isinstance(facts, Mapping) or set(facts) != set(candidate_domain):
        raise ValueError('A3_PLAN_FACT_DOMAIN_INVALID')
    values = dict(replacements)
    values['A3_CANDIDATE_POOL'] = [dict(symbol=symbol, strategy_profile=row['strategy_profile'],
        cluster=row['cluster'], candidate_origin=row['candidate_origin'], risk_unit=row['risk_unit'])
        for symbol, row in sorted(candidate_domain.items())]
    values['A3_FACT_SHEET'] = {}
    for symbol, raw in facts.items():
        if not isinstance(raw, Mapping):
            raise ValueError('A3_PLAN_FACT_DOMAIN_INVALID')
        values['A3_FACT_SHEET'][symbol] = _blind(blind_plan_facts(
            {key: value for key, value in raw.items() if key in FACT_FIELDS}))
    return values


class PlanReviewRuntime:
    def __init__(self, store):
        self.store = store
        with closing(store._connect()) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS team_plan_reviews ('
                'review_id TEXT PRIMARY KEY,input_sha256 TEXT NOT NULL,result_json TEXT NOT NULL)')

    def load(self, review_id, binding):
        with closing(self.store._connect()) as db:
            row = db.execute('SELECT input_sha256,result_json FROM team_plan_reviews WHERE review_id=?',
                (review_id,)).fetchone()
        if row is None:
            return None
        if row[0] != binding:
            raise ValueError('A3_PLAN_REVIEW_BINDING_CONFLICT')
        return json.loads(row[1])

    def record(self, review_id, binding, result):
        text = canonical(result)
        with closing(self.store._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT input_sha256,result_json FROM team_plan_reviews WHERE review_id=?',
                (review_id,)).fetchone()
            if old:
                if old != (binding, text):
                    raise ValueError('A3_PLAN_REVIEW_BINDING_CONFLICT')
                return json.loads(old[1])
            db.execute('INSERT INTO team_plan_reviews VALUES (?,?,?)', (review_id, binding, text))
        return json.loads(text)


def run_plan_review(*, repository, client, journal, store, review_id, call_id, run_id, model,
        source_trade_date, trade_date, clock, required_envelope, replacements, candidate_domain,
        engine_decisions, timeout_seconds, max_output_tokens, allow_model_call=False,deadline_monotonic=None):
    if not allow_model_call:
        raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    now = clock()
    try:
        dates_valid = (date.fromisoformat(source_trade_date).isoformat() == source_trade_date
            and date.fromisoformat(trade_date).isoformat() == trade_date)
    except (TypeError, ValueError):
        dates_valid = False
    if (not isinstance(now, datetime) or now.utcoffset() is None
            or not dates_valid
            or not source_trade_date <= now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat() <= trade_date):
        raise ValueError('A3_PLAN_REVIEW_DATE_INVALID')
    catalog = replacements['EVIDENCE_CATALOG']
    validate_plan_review(None, catalog, required_envelope=required_envelope, candidate_domain=candidate_domain)
    values = plan_review_inputs(replacements, candidate_domain, engine_decisions)
    request = repository.prepare_messages('A3', values, required_envelope=required_envelope, session='PLAN_REVIEW')
    context = dict(replacements=replacements, candidate_domain=candidate_domain, engine_decisions=engine_decisions,
        run_id=run_id, model=model, call_id=call_id, source_trade_date=source_trade_date, trade_date=trade_date,
        required_envelope=required_envelope, prompt_input_hash=request.input_hash,
        timeout_seconds=timeout_seconds, max_output_tokens=max_output_tokens)
    binding = _hash(context)
    # Claim original full source before HTTP, so a restart cannot review a later
    # engine verdict under the same review identity.
    store.freeze_control_context('a3-plan-source:' + review_id, source_sha256=binding, body=context)
    runtime = PlanReviewRuntime(store)
    if (previous := runtime.load(review_id, binding)) is not None:
        return previous
    original_request=request;source_request=request
    history=[];call_ids=[];output=None;valid=False;issues=()
    for attempt in range(1,4):
        request=original_request if attempt==1 else bind_a3_retry_feedback(original_request,
            a3_validation_feedback(output,catalog,issues,required_envelope=required_envelope,
                candidate_domain=candidate_domain),correction_attempt=attempt)
        current_call=call_id if attempt==1 else call_id+':correction:'+str(attempt)+':'+request.input_hash[:16]
        expected=dict(run_id=run_id,role='A3',session='PLAN_REVIEW',source_trade_date=source_trade_date,
            execution_trade_date=trade_date,model=model,prompt_hash=request.prompt.sha256,
            input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
        captured=journal.load_captured(current_call,expected=expected)
        remaining=None if deadline_monotonic is None else deadline_monotonic-time.monotonic()
        timeout=float(timeout_seconds) if remaining is None else min(float(timeout_seconds),remaining)
        budget_key='a3-plan-attempt-budget:'+review_id+':'+str(attempt)
        # Each semantic request has an independent timeout, clipped by the
        # original absolute research deadline. Captures can be replayed after
        # a crash without another HTTP; an expired budget cannot start work.
        if captured is None and timeout<=0:
            issues=tuple(dict.fromkeys((*issues,'RESEARCH_DEADLINE_EXCEEDED')))
            break
        budget=store.freeze_control_context(budget_key,source_sha256=request.input_hash,
            body=dict(timeout_seconds=timeout if captured is None else float(timeout_seconds),
                max_output_tokens=max_output_tokens))
        if captured is None and budget['timeout_seconds']>timeout:
            failed=journal.load_failed(current_call,request_binding=team_call_binding(request,
                run_id=run_id,model=model,session='PLAN_REVIEW',**budget))
            if failed is None:
                issues=tuple(dict.fromkeys((*issues,'TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT')))
                break
        call_ids.append(current_call)
        source_request=request
        output,valid,issues=_response(request=request,client=client,journal=journal,call_id=current_call,
            run_id=run_id,model=model,source_trade_date=source_trade_date,trade_date=trade_date,
            **budget,catalog=catalog,clock=clock,session='PLAN_REVIEW',validator=lambda raw:validate_plan_review(
                raw,catalog,required_envelope=required_envelope,candidate_domain=candidate_domain))
        history.append(dict(attempt=attempt,call_id=current_call,input_hash=request.input_hash,
            timeout_seconds=budget['timeout_seconds'],valid=bool(valid),reason_codes=list(issues)))
        if valid or 'TEAM_MODEL_CALL_FAILED' in issues:break
    source_call_id=call_ids[-1] if call_ids else call_id
    provider_validated = valid
    projection_audit = None
    projection = None if valid else recover_plan_review(output, catalog, required_envelope=required_envelope,
        candidate_domain=candidate_domain)
    if projection is not None:
        effective, audit = projection
        checked = validate_plan_review(effective, catalog, required_envelope=required_envelope,
            candidate_domain=candidate_domain)
        key = 'a3-plan-projection:' + review_id
        projection_audit = dict(**audit, source_call_id=source_call_id, audit_context_key=key,
            provider_raw_sha256=_hash(output), effective_raw_sha256=_hash(effective))
        store.freeze_control_context(key, source_sha256=source_request.input_hash,
            body=dict(**projection_audit, effective_raw=effective))
        output, valid, issues = effective, checked.valid, checked.issues
    votes = {row['symbol']: row for name in ('core_watch_pool', 'secondary_watch_pool', 'rejected_candidates')
        for row in output[name]} if valid else {}
    comparisons = [dict(symbol=symbol, engine_eligible=engine_decisions[symbol]['eligible'],
        model_review_status=votes[symbol]['review_status'] if valid else 'DATA_GAP',
        engine_and_model_pass=bool(valid and engine_decisions[symbol]['eligible']
            and votes[symbol]['review_status'] == 'PASS')) for symbol in sorted(candidate_domain)]
    result = dict(status='READY' if valid else 'A3_REVIEW_DATA_GAP', source_call_id=source_call_id,
        source_trade_date=source_trade_date, execution_trade_date=trade_date, source_sha256=binding,
        report=output if valid else None, reason_codes=list(issues), comparisons=comparisons,
        publication_authorized=False, provider_validated=provider_validated,
        server_projection=projection_audit,team_call_ids=call_ids,validation_history=history,
        semantic_attempts=len(call_ids))
    return runtime.record(review_id, binding, result)
