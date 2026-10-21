"""Read-only production plan projection. No legacy store constructor or writes.

Original row strings and their hashes are retained. This is a PLANNING view,
never a current quote/tradability proof or an execution permission.
"""
from contextlib import closing
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from ..review.observability import _cluster
from .risk_engine import PlanRisk, plan_risk_fields
from .team_v2_store import canonical

TZ = ZoneInfo('Asia/Shanghai')
REGIMES = {'TREND_MAINLINE', 'ROTATION_NO_MAINLINE', 'REPAIR', 'RISK_OFF_RETREAT',
           'BEAR_RISK', 'ICE_POINT', 'ICE_POINT_NO_NEW_ENTRY'}


def stamp(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('TEAM_SOURCE_TIMESTAMP_INVALID')
    return value.astimezone(TZ)


def publication_daily_atr14(payload):
    """Verified prior-day daily Wilder ATR, audit only, never a new entry veto."""
    from ..pipeline.local_fact_cache import canonical_json_hash
    facts=payload.get('strategy_facts')
    if not isinstance(facts,dict):return None
    shadow=facts.get('shadow_inputs')
    if not isinstance(shadow,dict):return None
    evidence=shadow.get('atr14_evidence')
    if not isinstance(evidence,dict) or evidence.get('status')!='AVAILABLE':return None
    try:
        body={k:v for k,v in evidence.items() if k!='evidence_sha256'}
        if (evidence.get('schema_version')!='publication-atr-evidence/1'
                or evidence.get('atr_method')!='WILDER_TR14_SEED_MEAN'
                or evidence.get('symbol')!=payload.get('symbol')
                or evidence.get('adjust')!='none'
                or canonical_json_hash(body)!=evidence.get('evidence_sha256')
                or stamp(evidence['last_closed_daily_bar_end'])>stamp(evidence['knowledge_at'])):return None
        value=Decimal(str(evidence['atr14']))
        return value if value.is_finite() and value>0 else None
    except (ValueError,TypeError,KeyError,ArithmeticError):return None


def read_planning_scope(settings, *, now, calendar):
    """One short RO transaction, same lane/date as the original opening scope.

    DB expiry is authoritative; a stale payload expiry is retained verbatim.
    Missing reference/cluster/geometry is explicit, not a midpoint or code-prefix
    substitute. A malformed plan prevents a complete morning projection.
    """
    now = stamp(now); day = now.date().isoformat()
    path = Path(settings.state_db_path)
    if not path.is_file() or path.is_symlink():
        raise ValueError('TEAM_SOURCE_DATABASE_UNAVAILABLE')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        try:
            rows = [dict(r) for r in db.execute(
                "SELECT * FROM execution_plans WHERE lane_id=? AND status IN "
                "('PENDING_MORNING_REVIEW','ACTIVE_TODAY') AND substr(expires_at,1,10)=? "
                "ORDER BY symbol,plan_id", (settings.research_primary_lane_id, day))]
        finally:
            db.execute('ROLLBACK')
    previous = calendar.previous_trading_day(now.date()).isoformat()
    plans = []; problems = []; regimes = set()
    for row in rows:
        try:
            raw = json.loads(row['payload_json'])
            if not isinstance(raw, dict) or raw.get('symbol') != row['symbol']:
                raise ValueError('TEAM_PLAN_IDENTITY_INVALID')
            if not isinstance(raw.get('source_run_id'), str) or not raw['source_run_id'].strip():
                raise ValueError('TEAM_PLAN_SOURCE_RUN_MISSING')
            start = stamp(row['valid_from']) if row['valid_from'] is not None else None
            end = stamp(row['expires_at'])
            if (end.date() != now.date() or (start is None and row['status']!='PENDING_MORNING_REVIEW')
                    or (start is not None and (start.date()!=now.date() or not start<end))):
                raise ValueError('TEAM_PLAN_DATABASE_DATE_INVALID')
            reference_at = stamp(raw.get('reference_price_as_of'))
            if reference_at.date().isoformat() != previous or reference_at > now:
                raise ValueError('TEAM_PLAN_CLOSED_REFERENCE_UNPROVEN')
            label, _ = _cluster(row)
            if label == 'UNKNOWN': raise ValueError('TEAM_PLAN_CLUSTER_UNPROVEN')
            # These are the publication payload's original numeric fields.
            low = Decimal(str(raw['trigger_low'])); high = Decimal(str(raw['trigger_high']))
            stop = Decimal(str(raw['stop_level'])); chase = Decimal(str(raw['no_chase']))
            price = Decimal(str(raw['reference_price']))
            if (not all(v.is_finite() and v > 0 for v in (low,high,stop,chase,price))
                    or not stop < low <= high <= chase):
                raise ValueError('TEAM_PLAN_GEOMETRY_UNPROVEN')
            regime = raw.get('market_regime') or raw.get('market_environment')
            if regime not in REGIMES: raise ValueError('TEAM_PLAN_MARKET_PRIOR_UNPROVEN')
            # PLANNING deliberately does not claim today's actual tradability.
            plan = PlanRisk(row['plan_id'],row['symbol'],label,row['status'],start,end,
                price,stop,low,high,chase,invalidation_price=stop,tradable=True,data_ready=True,
                daily_atr14=publication_daily_atr14(raw),**plan_risk_fields(raw))
            value=asdict(plan)
            value.update(valid_from=start.isoformat() if start is not None else None,expires_at=end.isoformat())
            plans.append(value); regimes.add(regime)
        except (ValueError,TypeError,KeyError,ArithmeticError) as exc:
            # Only our finite stable codes, never arbitrary exception contents.
            own={'TEAM_PLAN_IDENTITY_INVALID','TEAM_PLAN_SOURCE_RUN_MISSING','TEAM_PLAN_DATABASE_DATE_INVALID',
                'TEAM_PLAN_CLOSED_REFERENCE_UNPROVEN','TEAM_PLAN_CLUSTER_UNPROVEN','TEAM_PLAN_GEOMETRY_UNPROVEN',
                'TEAM_PLAN_MARKET_PRIOR_UNPROVEN','TEAM_SOURCE_TIMESTAMP_INVALID'}
            reason=str(exc) if str(exc) in own else 'TEAM_PLAN_PLANNING_INPUT_UNPROVEN'
            problems.append({'plan_id':row['plan_id'],'reason_code':reason})
    if len(regimes)>1: problems.append({'reason_code':'TEAM_PLAN_MARKET_PRIOR_CONFLICT'})
    body = dict(schema_version='team-planning-source/1',trade_date=day,observed_at=now.isoformat(),
        lane_id=settings.research_primary_lane_id,plan_rows=rows,
        plan_rowset_sha256=hashlib.sha256(canonical(rows).encode()).hexdigest(),
        plans=plans,market_regime=next(iter(regimes)) if len(regimes)==1 else None,
        source_trade_date=previous,reason_codes=problems,
        status='DATA_LIMITED' if problems else 'COMPLETE',
        authority='PLANNING_ONLY_NO_CURRENT_TRADABILITY_OR_ACTIVATION')
    return json.loads(canonical(body))


def decode_plan(row):
    value = dict(row)
    for key in ('valid_from','expires_at'):
        value[key] = stamp(value[key]) if value[key] is not None else None
    return PlanRisk(**value)


def meeting_plan_facts(rows):
    """Original planning fields, not repeated full A3 audit/payload JSON blobs.

    Frozen raw rows stay in the owned source receipt. This projection grants no
    activation, repairs no date and truncates no handoff/evidence/exit rules.
    """
    fields=('source_run_id','strategy_profile','strategy_version','primary_theme','theme_id',
        'trigger_low','trigger_high','trigger_zone','entry_reference_zone','stop_level',
        'invalidation_level','daily_invalidation','no_chase','no_chase_price','first_resistance',
        'pressure_reduce_price','reference_price','reference_price_as_of','plan_expiry',
        'market_regime','market_environment','risk_unit','max_batches','plan_priority',
        'A2_UNAVAILABLE','A2_REVIEW_DEGRADED','PATTERN_B','SERVER_FALLBACK','degradation_tags',
        'DATA_LIMITED','source_data_limits',
        'plan_priority_rank','priority_reasons','trading_plan_brief','trade_plan_brief','trade_brief',
        'handoff_notes','what_would_change_my_mind','supporting_refs','opposing_refs',
        'opposing_search_scope','confirmation_conditions','a4_required_entry_rules',
        'a4_deferred_conditions','a4_exit_rules','plan_premises','time_stop_sessions',
        'expected_holding_sessions','setup_type','setup_pattern','company_name','name')
    result=[];seen=set()
    for row in rows:
        raw=json.loads(row['payload_json']);pid=row['plan_id'];cluster,basis=_cluster(row)
        if (pid in seen or raw.get('symbol')!=row['symbol'] or cluster=='UNKNOWN'
                or not isinstance(raw.get('source_run_id'),str) or not raw['source_run_id'].strip()):
            raise ValueError('MEETING_PLAN_SOURCE_BINDING_INVALID')
        seen.add(pid)
        result.append(dict(plan_id=pid,symbol=row['symbol'],lane_id=row['lane_id'],status=row['status'],
            valid_from=row.get('valid_from'),expires_at=row['expires_at'],cluster=cluster,cluster_basis=basis,
            source_row_sha256=hashlib.sha256(canonical(row).encode()).hexdigest(),
            original_payload_sha256=hashlib.sha256(row['payload_json'].encode()).hexdigest(),
            publication_facts={key:raw[key] for key in fields if key in raw},
            missing_fields=[key for key in ('handoff_notes','what_would_change_my_mind','plan_priority_rank') if key not in raw]))
    return result


def historical_meeting_plan_facts(rows):
    """Prior-publication facts only, never an archived later lifecycle state.

    The original row hash is retained for audit. Absence of an 08:38 lifecycle
    snapshot is not repaired to PENDING or ACTIVE. The live meeting projection
    remains unchanged and may legitimately see the current owned source state.
    """
    result=meeting_plan_facts(rows)
    for item in result:
        item.pop('status');item.pop('valid_from')
        item['lifecycle_evidence_status']='UNAVAILABLE_AT_HISTORICAL_PLANNING_CUTOFF'
    return result


def read_held_bindings(settings, positions):
    """Original owner identity/cluster for an already-held, possibly old plan.

    Does not require today's activation/expiry and never reactivates that plan.
    A held symbol with no provable owner cannot disappear from exposure totals.
    """
    identities={row['plan_id']:row['symbol'] for row in positions}
    if len(identities)!=len(positions):raise ValueError('MORNING_HELD_PLAN_SCOPE_UNPROVEN')
    if not identities:return []
    path=Path(settings.state_db_path)
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        try:
            rows=[dict(row) for row in db.execute('SELECT * FROM execution_plans WHERE lane_id=? AND plan_id IN ('+
                ','.join('?' for _ in identities)+') ORDER BY plan_id',(settings.research_primary_lane_id,*identities))]
        finally:db.execute('ROLLBACK')
    if {row['plan_id'] for row in rows}!=set(identities):raise ValueError('MORNING_HELD_PLAN_SCOPE_UNPROVEN')
    result=[]
    for row in rows:
        raw=json.loads(row['payload_json']);label,basis=_cluster(row)
        if row['symbol']!=identities[row['plan_id']] or raw.get('symbol')!=row['symbol'] or label=='UNKNOWN':
            raise ValueError('MORNING_HELD_PLAN_SCOPE_UNPROVEN')
        result.append(dict(plan_id=row['plan_id'],symbol=row['symbol'],cluster=label,cluster_basis=basis,
            original_row=row,row_sha256=hashlib.sha256(canonical(row).encode()).hexdigest()))
    return result


def read_execution_scope(settings, *, now, calendar, morning_source, positions):
    """Current activation + unchanged morning payload in one short RO view.

    Morning status/valid_from are not execution authority. Historical held
    owners remain in the view for exits, never reactivated or added to PM slots.
    A corrupt individual plan is excluded with its own stable reason.
    """
    now=stamp(now);day=now.date().isoformat()
    if calendar.is_trading_day(now.date()) is not True:
        return dict(status='NOOP',reason_codes=[{'reason_code':'NON_TRADING_DAY'}],plans=[],active_plan_ids=[])
    held_only=morning_source is None
    frozen_at=stamp(morning_source.get('observed_at')) if not held_only else now
    if not held_only and (frozen_at>now or frozen_at.date()!=now.date()
            or frozen_at>=frozen_at.replace(hour=9,minute=30,second=0,microsecond=0)
            or morning_source.get('status')!='COMPLETE' or morning_source.get('trade_date')!=day
            or morning_source.get('lane_id')!=settings.research_primary_lane_id):
        raise ValueError('EXECUTION_MORNING_SOURCE_UNPROVEN')
    original=morning_source['plan_rows'] if not held_only else []
    if not held_only and hashlib.sha256(canonical(original).encode()).hexdigest()!=morning_source.get('plan_rowset_sha256'):
        raise ValueError('EXECUTION_MORNING_SOURCE_HASH_MISMATCH')
    expected={r['plan_id']:r for r in original}
    if len(expected)!=len(original):raise ValueError('EXECUTION_MORNING_PLAN_ID_AMBIGUOUS')
    held={r['plan_id']:r['symbol'] for r in positions}
    if len(held)!=len(positions):raise ValueError('EXECUTION_HELD_PLAN_ID_AMBIGUOUS')
    ids=sorted(set(expected)|set(held));path=Path(settings.state_db_path)
    if not path.is_file() or path.is_symlink():raise ValueError('TEAM_SOURCE_DATABASE_UNAVAILABLE')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        try:
            query="SELECT * FROM execution_plans WHERE lane_id=? AND ("
            match=('plan_id IN ('+','.join('?' for _ in ids)+')') if ids else '0'
            if not held_only:match+=" OR (status='ACTIVE_TODAY' AND substr(expires_at,1,10)=?)"
            rows=[dict(r) for r in db.execute(query+match+") ORDER BY symbol,plan_id",
                (settings.research_primary_lane_id,*ids,*(() if held_only else (day,))))]
        finally:db.execute('ROLLBACK')
    plans=[];active=[];reasons=[];seen={r['plan_id'] for r in rows}
    for pid in sorted(set(ids)-seen):
        reasons.append(dict(plan_id=pid,reason_code='EXECUTION_BOUND_PLAN_MISSING'))
    for row in rows:
        pid=row['plan_id'];old=expected.get(pid)
        def limited(code):reasons.append(dict(plan_id=pid,reason_code=code))
        if old is None and pid not in held:
            limited('EXECUTION_PLAN_OUTSIDE_MORNING_SCOPE');continue
        if old is not None and any(row[key]!=old[key] for key in ('symbol','payload_json','expires_at')):
            limited('EXECUTION_MORNING_PAYLOAD_CHANGED');continue
        try:
            raw=json.loads(row['payload_json']);label,_=_cluster(row)
            if (not isinstance(raw,dict) or raw.get('symbol')!=row['symbol'] or label=='UNKNOWN'
                    or (pid in held and held[pid]!=row['symbol'])
                    or not isinstance(raw.get('source_run_id'),str) or not raw['source_run_id'].strip()):
                raise ValueError
            start=stamp(row['valid_from']) if row['valid_from'] is not None else None
            expiry=stamp(row['expires_at'])
            # Never manufacture valid_from for INVALIDATED/PENDING rows. An
            # inactive row with no activation cannot own a new Team position.
            if start is None and row['status']!='PENDING_MORNING_REVIEW':
                if pid in held:limited('EXECUTION_HELD_ACTIVATION_UNPROVEN')
                continue
            if start is not None and start>=expiry:raise ValueError
            geometry={k:Decimal(str(raw[v])) for k,v in dict(price='reference_price',stop_price='stop_level',
                entry_low='trigger_low',entry_high='trigger_high',no_chase='no_chase').items()}
            if (not all(v.is_finite() and v>0 for v in geometry.values())
                    or not geometry['stop_price']<geometry['entry_low']<=geometry['entry_high']<=geometry['no_chase']):
                raise ValueError
            plan=PlanRisk(pid,row['symbol'],label,row['status'],start,expiry,**geometry,
                invalidation_price=geometry['stop_price'],tradable=False,data_ready=False,
                daily_atr14=publication_daily_atr14(raw),**plan_risk_fields(raw))
            plans.append(asdict(plan))
            if (old is not None and row['status']=='ACTIVE_TODAY' and start is not None
                    and start.date()==now.date() and expiry.date()==now.date() and start<=now<expiry):
                active.append(pid)
        except (ValueError,TypeError,KeyError,ArithmeticError):limited('EXECUTION_ORIGINAL_PLAN_INPUT_UNPROVEN')
    body=dict(schema_version='team-execution-source/1',trade_date=day,observed_at=now.isoformat(),
        status='DATA_LIMITED' if reasons else 'COMPLETE',lane_id=settings.research_primary_lane_id,
        plan_rows=rows,plan_rowset_sha256=hashlib.sha256(canonical(rows).encode()).hexdigest(),
        morning_rowset_sha256=morning_source['plan_rowset_sha256'] if not held_only else None,plans=plans,
        active_plan_ids=sorted(active),held_plan_ids=held,reason_codes=reasons,
        authority='HELD_ONLY_NO_MORNING_NO_ENTRY' if held_only else 'CURRENT_DATABASE_ACTIVATION_ONLY_NO_CURRENT_TRADABILITY_OR_PRICE')
    return json.loads(json.dumps(body,default=lambda x:x.isoformat() if isinstance(x,datetime) else str(x)))
