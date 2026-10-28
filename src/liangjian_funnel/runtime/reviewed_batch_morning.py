"""Independent candidate engine/A6/PM, before the actual target-day auction.

Only the isolated root is writable. The original natural morning's account,
held-owner and external evidence are read, never reopened as a paper account or
relabelled fresh. No broker, source collector, notification or plan publisher.
"""
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, time
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from zoneinfo import ZoneInfo

import yaml

from .team_v2_store import TeamMeetingStore, canonical
from .team_v2_sources import stamp, read_planning_scope, decode_plan, meeting_plan_facts
from .team_v2_morning import ACCOUNT, _directory, _envelopes
from .risk_engine import AccountRisk, PositionRisk, RiskLimits, build_planning_envelope
from .calendar import ExchangeTradingCalendar

TZ = ZoneInfo('Asia/Shanghai')


def _require(value, code):
    if not value:
        raise ValueError('CANDIDATE_REVIEW_' + code)


def _sha(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _window(scope_id, clock, calendar):
    _require(isinstance(scope_id, str) and re.fullmatch('[A-Za-z0-9_-]{1,40}', scope_id) is not None, 'SCOPE_ID_INVALID')
    now = stamp(clock())
    _require(calendar.is_trading_day(now.date()) is True and now.time().replace(tzinfo=None) < time(9, 26),
        'PRE_AUCTION_WINDOW_MISSED')
    return now


def _isolation(settings, baseline_team_db):
    root, baseline = Path(settings.root), Path(baseline_team_db)
    _require(root.is_absolute() and root.resolve() == root and not root.is_symlink(), 'ISOLATED_ROOT_INVALID')
    _require(baseline.is_absolute() and baseline.resolve() == baseline and not baseline.is_symlink()
        and baseline.is_file() and baseline.name == 'team.db' and baseline.parent.name == 'team_v2', 'BASELINE_PATH_INVALID')
    baseline_root = baseline.parent.parent.parent
    _require(root != baseline_root and not root.is_relative_to(baseline_root)
        and not baseline_root.is_relative_to(root), 'BASELINE_ROOT_NOT_ISOLATED')
    for name in ('state_db_path', 'workflow_output_dir', 'research_checkpoint_dir', 'minute_cache_dir',
                 'fact_cache_db_path', 'feature_store_db_path'):
        path = Path(getattr(settings, name))
        _require(path.resolve().is_relative_to(root) and not path.is_symlink(), 'WRITE_PATH_NOT_ISOLATED')
    return root, baseline


def _read_engine(path, *, day):
    _require(path.is_file() and not path.is_symlink(), 'ENGINE_SOURCE_MISSING')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        _require(db.execute('SELECT version FROM team_v2_meta').fetchall() == [('team-runtime/1',)], 'BASELINE_DATABASE_INVALID')
        row = db.execute('SELECT body_json FROM morning_engine WHERE account_id=? AND trade_date=?', (ACCOUNT, day)).fetchone()
        _require(row is not None, 'BASELINE_ENGINE_MISSING')
        text = row[0]
        _require(len(text.encode()) <= 16 * 1024 * 1024, 'BASELINE_ENGINE_TOO_LARGE')
        body = json.loads(text)
        _require(canonical(body) == text and db.execute('PRAGMA query_only').fetchone()[0] == 1, 'BASELINE_BODY_INVALID')
    return body, hashlib.sha256(text.encode()).hexdigest()


def _limits(settings, now):
    from .team_v2_first_day import load_first_day_policy, first_day_limits
    limits = RiskLimits.from_yaml(settings.root / 'config/risk_limits.yaml')
    policy = load_first_day_policy(settings.root / 'config/risk_limits.yaml')
    return first_day_limits(limits, policy, now=now), policy


def _configuration(settings, baseline):
    risk = (settings.root / 'config/risk_limits.yaml').read_bytes()
    deployment = (settings.root / 'config/team_v2_deployment.yaml').read_bytes()
    _require(hashlib.sha256(risk).hexdigest() == baseline.get('risk_limits_sha256')
        and hashlib.sha256(deployment).hexdigest() == baseline.get('deployment_policy_sha256'), 'BASELINE_CONFIGURATION_CHANGED')
    return risk, deployment


def _held_positions(baseline, now):
    from ..review.observability import _cluster
    snapshot = baseline['snapshot']
    _require(snapshot['risk_day'].get('trade_date') == now.date().isoformat(), 'ACCOUNT_RISK_DAY_MISMATCH')
    held = baseline['held_plan_bindings']
    owners = {row['plan_id']: row for row in held}
    positions = snapshot['positions']
    _require(len(owners) == len(held) and len({row['plan_id'] for row in positions}) == len(positions)
        and set(owners) == {row['plan_id'] for row in positions}, 'HELD_OWNER_SCOPE_INVALID')
    result = []
    for position in positions:
        owner = owners[position['plan_id']]
        row = owner['original_row']
        raw = json.loads(row['payload_json'])
        cluster, basis = _cluster(row)
        _require(_sha(row) == owner['row_sha256'] and owner['plan_id'] == row['plan_id']
            and owner['symbol'] == row['symbol'] == position['symbol'] == raw['symbol']
            and owner['cluster'] == cluster != 'UNKNOWN' and owner['cluster_basis'] == basis
            and position['market_value'] is not None, 'HELD_OWNER_REFERENCE_INVALID')
        result.append(PositionRisk(owner['plan_id'], owner['symbol'], owner['cluster'], position['qty'],
            position['sellable_qty'], D(position['market_value']), D(position['avg_cost'])))
    account, basis = snapshot['account'], snapshot['risk_day']
    total = sum((position.market_value for position in result), D(0))
    _require(D(account['cash']) + total == D(account['nav'])
        and D(account['positions_value']) == total, 'ACCOUNT_POSITION_TOTAL_CONFLICT')
    view = AccountRisk(D(account['nav']), D(account['cash']), D(basis['day_open_nav']),
        max(D(snapshot['peak_nav']), D(account['nav'])), D(account['frozen']),
        hard_halt=bool(snapshot['global_halt'] or basis['halt_reason_codes']))
    return tuple(result), view


def _source(settings, source, *, now, calendar):
    observed = stamp(source['observed_at'])
    _require(observed.date() == now.date() and observed <= now and observed.time().replace(tzinfo=None) < time(9, 26)
        and source.get('status') == 'COMPLETE' and source.get('trade_date') == now.date().isoformat()
        and source.get('source_trade_date') == calendar.previous_trading_day(now.date()).isoformat()
        and source.get('lane_id') == settings.research_primary_lane_id
        and source.get('market_regime') is not None and source.get('plan_rows'), 'CANDIDATE_SOURCE_INVALID')
    actual = read_planning_scope(settings, now=observed, calendar=calendar)
    _require(actual == source and _sha(source['plan_rows']) == source['plan_rowset_sha256'], 'CANDIDATE_SOURCE_NOT_DATABASE_ORIGINAL')
    _require(all(row['status'] == 'PENDING_MORNING_REVIEW' and row.get('valid_from') is None for row in source['plan_rows']),
        'CANDIDATE_NOT_INERT_PENDING')


def prepare_candidate_morning_engine(*, settings, source, baseline_team_db, scope_id, clock=None, calendar=None):
    """Reprice only envelopes over the frozen original account, never its ledger."""
    clock = clock or (lambda: datetime.now(TZ))
    calendar = calendar or ExchangeTradingCalendar()
    now = _window(scope_id, clock, calendar)
    root, baseline_path = _isolation(settings, baseline_team_db)
    _source(settings, source, now=now, calendar=calendar)
    baseline, baseline_sha = _read_engine(baseline_path, day=now.date().isoformat())
    _require({'snapshot', 'held_plan_bindings', 'risk_limits_sha256', 'deployment_policy_sha256',
        'external_adjustment', 'rule_preflight', 'emotion_baseline', 'source', 'observed_at'} <= set(baseline), 'BASELINE_BODY_INCOMPLETE')
    _require(baseline.get('status') == 'COMPLETE' and baseline.get('session') == 'ENGINE_PREMARKET'
        and baseline.get('account_id') == ACCOUNT and baseline.get('trade_date') == source['trade_date']
        and stamp(baseline['observed_at']).date() == now.date() and stamp(baseline['observed_at']) <= now
        and baseline['source'].get('trade_date') == source['trade_date']
        and baseline['source'].get('lane_id') == settings.research_primary_lane_id
        and baseline['source'].get('source_trade_date') == source['source_trade_date']
        and stamp(baseline['source']['observed_at']).date() == now.date()
        and stamp(baseline['source']['observed_at']) <= now
        and baseline['source'].get('status') == 'COMPLETE'
        and _sha(baseline['source']['plan_rows']) == baseline['source'].get('plan_rowset_sha256'), 'BASELINE_ENGINE_INVALID')
    risk_raw, deployment_raw = _configuration(settings, baseline)
    positions, account = _held_positions(baseline, now)
    limits, first_policy = _limits(settings, now)
    plans = tuple(decode_plan(row) for row in source['plans'])
    from .team_v2_first_day import probe_envelope
    envelopes = tuple(probe_envelope(build_planning_envelope(plan, account, positions, limits, now=now,
        regime=source['market_regime']), first_policy, now=now, probe_multiplier=limits.probe_multiplier) for plan in plans)
    from ..pipeline.team_v2_pm_schema import PM_PRIMARY_CLUSTER_CAP
    policy = yaml.safe_load(deployment_raw)
    _require(policy.get('schema_version') == 'team-deployment/1', 'DEPLOYMENT_POLICY_INVALID')
    bounds = policy['ranges'][source['market_regime']]
    _require(isinstance(bounds, list) and len(bounds) == 2 and all(type(value) is int for value in bounds)
        and 0 <= bounds[0] <= bounds[1] <= 95, 'DEPLOYMENT_RANGE_INVALID')
    external = baseline['external_adjustment']
    adjustment = external.get('value_pp')
    _require(type(adjustment) in (int, float) and D(str(adjustment)).is_finite(), 'FROZEN_EXTERNAL_ADJUSTMENT_INVALID')
    maximum = yaml.safe_load(risk_raw)['external_adjustment']['maximum_utilization_pct']
    bounds = [max(0, min(maximum, value + adjustment)) for value in bounds]
    _window(scope_id, clock, calendar)
    body = dict(status='COMPLETE', session='ENGINE_PREMARKET', trade_date=source['trade_date'],
        observed_at=stamp(clock()).isoformat(), account_id=ACCOUNT, source=deepcopy(source),
        snapshot=deepcopy(baseline['snapshot']), held_plan_bindings=deepcopy(baseline['held_plan_bindings']),
        envelopes=[asdict(value) for value in envelopes], capacity_slots=max(0, limits.max_positions - len({p.symbol for p in positions})),
        cluster_caps={key: min(limits.max_positions, PM_PRIMARY_CLUSTER_CAP) for key in sorted({p.cluster for p in plans})},
        baseline_range=bounds, engine_utilization_cap=bounds[1],
        deployment_policy_sha256=baseline['deployment_policy_sha256'], risk_limits_sha256=baseline['risk_limits_sha256'],
        external_adjustment=deepcopy(external), rule_preflight=deepcopy(baseline['rule_preflight']),
        emotion_baseline=deepcopy(baseline['emotion_baseline']), authority='PLANNING_ONLY_NOT_EXECUTION_PERMISSION',
        candidate_scope_id=scope_id, account_snapshot_basis=dict(status='ORIGINAL_NATURAL_MORNING_NOT_REFRESHED',
            baseline_team_db=str(baseline_path), baseline_engine_sha256=baseline_sha,
            original_observed_at=baseline['observed_at'], account_snapshot_sha256=_sha(baseline['snapshot']),
            held_bindings_sha256=_sha(baseline['held_plan_bindings'])))
    store = TeamMeetingStore(_directory(settings) / 'team.db')
    previous = store.load_morning_engine(ACCOUNT, source['trade_date'])
    if previous is not None:
        _require(previous.get('candidate_scope_id') == scope_id
            and previous['source'] == source and previous.get('account_snapshot_basis') == body['account_snapshot_basis']
            and all(_sha(previous.get(key)) == _sha(body[key]) for key in ('envelopes', 'capacity_slots', 'cluster_caps',
                'baseline_range', 'engine_utilization_cap', 'risk_limits_sha256', 'deployment_policy_sha256',
                'external_adjustment', 'rule_preflight', 'emotion_baseline', 'snapshot', 'held_plan_bindings')),
            'ISOLATED_ENGINE_ALREADY_BOUND')
        return previous
    return store.record_morning_engine(ACCOUNT, source['trade_date'], body)


def run_candidate_morning_reviews(*, settings, scope_id, clock=None, calendar=None, client=None):
    """Real existing business sessions, independent IDs, no fixed stage windows."""
    clock = clock or (lambda: datetime.now(TZ))
    calendar = calendar or ExchangeTradingCalendar()
    now = _window(scope_id, clock, calendar)
    directory = _directory(settings)
    frozen, _ = _read_engine(directory / 'team.db', day=now.date().isoformat())
    _require(frozen.get('candidate_scope_id') == scope_id and frozen.get('status') == 'COMPLETE'
        and frozen.get('session') == 'ENGINE_PREMARKET' and frozen.get('account_id') == ACCOUNT
        and stamp(frozen['observed_at']).date() == now.date() and stamp(frozen['observed_at']) <= now,
        'ISOLATED_ENGINE_SCOPE_MISMATCH')
    _isolation(settings, frozen['account_snapshot_basis']['baseline_team_db'])
    _configuration(settings, frozen)
    _source(settings, frozen['source'], now=now, calendar=calendar)
    original, baseline_sha = _read_engine(Path(frozen['account_snapshot_basis']['baseline_team_db']), day=now.date().isoformat())
    _require(baseline_sha == frozen['account_snapshot_basis']['baseline_engine_sha256']
        and _sha(frozen['snapshot']) == frozen['account_snapshot_basis']['account_snapshot_sha256']
        and _sha(frozen['held_plan_bindings']) == frozen['account_snapshot_basis']['held_bindings_sha256'], 'BASELINE_OR_FROZEN_ACCOUNT_CHANGED')
    # Reuse the same pure envelope construction and compare every frozen risk
    # input before allowing HTTP. Existing isolated engine reuse never updates
    # its original timestamp or creates a broker/account.
    prepare_candidate_morning_engine(settings=settings, source=frozen['source'],
        baseline_team_db=frozen['account_snapshot_basis']['baseline_team_db'], scope_id=scope_id,
        clock=clock, calendar=calendar)
    from ..pipeline.model_client import OpenAICompatibleModelClient
    from ..pipeline.team_v2_judgments import ModelJudgmentJournal
    from ..pipeline.team_v2_prompts import TeamPromptRepository
    repository = TeamPromptRepository(settings.root / 'prompts/team_v2')
    store, journal = TeamMeetingStore(directory / 'team.db'), ModelJudgmentJournal(directory / 'model.db')
    client = client or OpenAICompatibleModelClient(settings, max_attempts=1)
    day, source = now.date().isoformat(), frozen['source']
    envelopes = _envelopes(frozen['envelopes'])
    meeting_plans = meeting_plan_facts(source['plan_rows'])
    catalog = {'categories': ['ACCOUNT', 'MARKET', 'PLAN'], 'items': [
        dict(id='MORNING:ACCOUNT', category='ACCOUNT', value=frozen['snapshot']),
        dict(id='MORNING:MARKET', category='MARKET', value=dict(market_regime=source['market_regime'], external_adjustment=frozen['external_adjustment'])),
        *[dict(id='MORNING:PLAN:' + row['plan_id'], category='PLAN', value=row) for row in meeting_plans]]}
    run_id = scope_id + ':review:' + day
    def arguments(role, session, items):
        required = dict(schema_version='team-envelope/1', run_id=run_id, stage_id=role,
            source_trade_date=source['source_trade_date'], execution_trade_date=day,
            as_of=frozen['observed_at'], knowledge_cutoff=frozen['observed_at'],
            input_snapshot_ids=[source['plan_rowset_sha256']], config_version='team-v2/1',
            config_hash=frozen['risk_limits_sha256'], prompt_version=repository.prepare(role,
                {key: [] for key in repository.document(role).placeholders} | {'RISK_SESSION': 'PREMARKET'}, session=session).role_version,
            model_name=settings.review_model, market_regime=source['market_regime'], status='FROZEN', reason_codes=[])
        values = {key: dict(status='NOT_AVAILABLE') for key in repository.document(role).placeholders}
        values.update(EVIDENCE_CATALOG=deepcopy(catalog), ACCOUNT_SNAPSHOT=frozen['snapshot']['account'], POSITIONS=frozen['snapshot']['positions'],
            MARKET_STATE=dict(market_regime=source['market_regime'], basis='FROZEN_PRIOR_NOT_INTRADAY'),
            PLANS=meeting_plans, PLANS_TODAY=meeting_plans, PRIOR_OUTCOME_FEEDBACK=dict(status='INSUFFICIENT_TEN_DAYS'),
            OVERNIGHT_EXTERNAL_SNAPSHOT=frozen['external_adjustment'])
        return dict(repository=repository, client=client, journal=journal, store=store, account_id=ACCOUNT,
            run_id=run_id, model=settings.review_model, trade_date=day, source_trade_date=source['source_trade_date'],
            clock=clock, required_envelope=required, replacements=values, envelopes=items,
            timeout_seconds=100, max_output_tokens=settings.model_max_output_tokens, allow_model_call=True)
    from ..pipeline.team_v2_cluster_quota import build_cluster_quota_context
    from .team_v2_pm_policy import priority_order, load_pm_policy, pm_output_budget
    from .team_v2_risk_policy import load_risk_policy, risk_output_budget
    from .team_v2_risk_session import run_risk_premarket
    from .team_v2_pm_session import run_pm_premarket
    limits, _ = _limits(settings, now)
    clusters = {row['plan_id']: row['cluster'] for row in source['plans']}
    quota = build_cluster_quota_context(engine_slots=frozen['capacity_slots'], cluster_caps=frozen['cluster_caps'],
        plan_clusters=clusters, plan_priority_order=priority_order(meeting_plans))
    risk_policy = load_risk_policy(settings.root)
    risk_args = arguments('A6', 'PREMARKET', envelopes)
    risk_args.update(timeout_seconds=risk_policy['timeout_seconds'], max_output_tokens=risk_output_budget(
        risk_policy, len(meeting_plans), len(frozen['cluster_caps']), settings.model_max_output_tokens))
    risk_id, risk_call = scope_id + ':risk', scope_id + ':A6'
    pm_id, pm_call = scope_id + ':pm', scope_id + ':PM'
    risk = run_risk_premarket(**risk_args, meeting_id=risk_id, call_id=risk_call,
        engine_utilization_cap=D(frozen['engine_utilization_cap']), capacity_slots=frozen['capacity_slots'],
        cluster_caps=frozen['cluster_caps'], unready_plan_ids={row.plan_id for row in envelopes if not row.eligible}, limits=limits, quota_context=quota)
    base = dict(scope_id=scope_id, trade_date=day, a6_meeting_id=risk_id, a6_call_id=risk_call,
        pm_meeting_id=pm_id, pm_call_id=pm_call, risk=json.loads(canonical(asdict(risk))), production_writes=0, publication_calls=0)
    if risk.status != 'READY':
        return dict(base, status='BLOCKED', reason_code='CANDIDATE_A6_NOT_READY', pm_called=False)
    from .a6_effective_review import verify_premarket_capture
    if not verify_premarket_capture(directory,engine=frozen,meeting_id=risk_id,
            scope_id=scope_id,account_id=ACCOUNT,day=day):
        return dict(base,status='BLOCKED',reason_code='CANDIDATE_A6_ORIGINAL_NOT_VALIDATED',pm_called=False)
    _window(scope_id, clock, calendar)
    _configuration(settings, frozen)
    pm_policy = load_pm_policy(settings.root)
    pm_args = arguments('A3', 'PM_PREMARKET', risk.envelopes)
    pm_args['replacements']['MARKET_STATE']['a6_morning'] = dict(status=risk.status, reason_codes=list(risk.reason_codes))
    from .team_v2_pm_geometry import pm_geometry_domain
    geometry_ids, geometry_audit = pm_geometry_domain(source['plan_rows'])
    pm_args.update(timeout_seconds=pm_policy['timeout_seconds'], max_output_tokens=pm_output_budget(pm_policy,
        len(meeting_plans), settings.model_max_output_tokens, max_positions=limits.max_positions))
    pm = run_pm_premarket(**pm_args, meeting_id=pm_id, call_id=pm_call, plan_clusters=clusters,
        free_slots=frozen['capacity_slots'], baseline_range=tuple(min(D(value), risk.utilization_cap) for value in frozen['baseline_range']),
        utilization_cap=risk.utilization_cap, high_conviction_ids=set(), fallback_enabled=False,
        fallback_priority_order=tuple(priority_order(meeting_plans)), max_new_entries=limits.max_new_entries,
        max_positions=limits.max_positions, geometry_plan_ids=geometry_ids, geometry_audit=geometry_audit,
        a6_validated=True, a6_degraded_conservative=False)
    return dict(base, status='READY' if pm.get('status') == 'READY' and pm.get('model_validated') is True
        and pm.get('decision_authority') == 'VALIDATED_MODEL' else 'BLOCKED', pm_called=True, pm=pm)
