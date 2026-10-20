"""Actual current breadth/emotion + explicitly historical sector context.

Sector-history geometry is captured from the natural auction-base parent;
it is not yesterday's regime and is never relabelled as intraday history.
No provider, model, activation or HTTP sender is created here. Capacity faults
enqueue the existing internal-only durable outbox, never send from this path.
"""
import hashlib
import json
from pathlib import Path
from datetime import timedelta
import time as timer

from .team_v2_rule_source import _write_once
from .team_v2_sources import stamp
from .team_v2_store import canonical
from .team_v2_regime import load_rules
from .trader_guardrails import is_closed_trader_tick
from .team_v2_sector_context import (AUTHORITY, PROJECTION_VERSION, SectorContextFailure,
    bounded_read, enqueue_capacity_alert, load_capacity, projected_body, require_projected_capacity)


def _capture_capacity_failure(root,now,failure):
    from .team_v2_store import TeamMeetingStore
    event=enqueue_capacity_alert(TeamMeetingStore(root/'state/team_v2/team.db'),now=now,failure=failure)
    return dict(status='DATA_LIMITED',reason_code=failure.reason_code,**failure.diagnostics,
        internal_alert_event_id=event,internal_alert_status='PENDING')


def capture_sector_context(settings,prepared,*,known_at):
    now=stamp(known_at);root=Path(settings.root).resolve()
    directory=root/'state/team_v2/sector_context'
    if directory.resolve()!=directory:raise ValueError('TEAM_SECTOR_DIRECTORY_INVALID')
    cycle=prepared.snapshot.data.get('SECTOR_CYCLE_SNAPSHOT',{})
    policy=load_capacity(settings)
    body=projected_body(dict(trade_date=str(now.date()),
        known_at=now.isoformat(),source_snapshot_id=prepared.snapshot.snapshot_id,
        source_snapshot_hash=prepared.snapshot.snapshot_hash,cycle=cycle,
        authority=AUTHORITY))
    try:size=require_projected_capacity(body,policy)
    except SectorContextFailure as exc:
        return _capture_capacity_failure(root,now,exc)
    path=directory/(str(now.date())+'.json')
    pin=body['body_sha256']
    if path.exists():
        # Existing v1 evidence is immutable. A retry may accept the same parent
        # and projected facts, but must not replace its signed original body.
        # auction_base writes its READY marker *after* capture. An interrupted
        # identical producer retry must not require that not-yet-written marker.
        if path.resolve()!=path:raise ValueError('TEAM_SECTOR_CONTEXT_CONFLICT')
        try:
            old=json.loads(bounded_read(path,max(policy['legacy_input_max_bytes'],policy['projected_max_bytes']),
                'TEAM_SECTOR_CONTEXT_INPUT_SIZE_EXCEEDED'))
        except SectorContextFailure as exc:return _capture_capacity_failure(root,now,exc)
        pin=old.pop('body_sha256')
        if hashlib.sha256(canonical(old).encode()).hexdigest()!=pin:
            raise ValueError('TEAM_SECTOR_CONTEXT_CONFLICT')
        previous=projected_body(old) if old.get('schema_version')=='team-sector-context/1' else dict(old,body_sha256=pin)
        if previous!=body:
            raise ValueError('TEAM_SECTOR_CONTEXT_CONFLICT')
    else:_write_once(path,body)
    return dict(status='CAPTURED',body_sha256=pin,
        projection_version=PROJECTION_VERSION,projected_bytes=size,projected_max_bytes=policy['projected_max_bytes'])


def load_sector_context(settings,*,now,store=None):
    now=stamp(now);root=Path(settings.root).resolve();day=str(now.date())
    path=root/'state/team_v2/sector_context'/(day+'.json')
    try:
        policy=load_capacity(settings)
        if path.resolve()!=path:raise SectorContextFailure('TEAM_SECTOR_CONTEXT_PATH_INVALID')
        raw=bounded_read(path,max(policy['legacy_input_max_bytes'],policy['projected_max_bytes']),
            'TEAM_SECTOR_CONTEXT_INPUT_SIZE_EXCEEDED')
        body=json.loads(raw);pin=body.pop('body_sha256')
        if hashlib.sha256(canonical(body).encode()).hexdigest()!=pin:
            raise SectorContextFailure('TEAM_SECTOR_CONTEXT_HASH_MISMATCH')
        schema=body['schema_version']
        if (schema not in ('team-sector-context/1','team-sector-context/2') or body['authority']!=AUTHORITY
                or schema=='team-sector-context/2' and (body.get('projection_version')!=PROJECTION_VERSION
                    or not isinstance(body.get('source_cycle_sha256'),str) or len(body['source_cycle_sha256'])!=64
                    or any(c not in '0123456789abcdef' for c in body['source_cycle_sha256']))):
            raise SectorContextFailure('TEAM_SECTOR_CONTEXT_SCHEMA_INVALID')
        if schema=='team-sector-context/1' and len(raw)>policy['legacy_input_max_bytes']:
            raise SectorContextFailure('TEAM_SECTOR_CONTEXT_INPUT_SIZE_EXCEEDED',
                actual_bytes=len(raw),limit_bytes=policy['legacy_input_max_bytes'])
        known=stamp(body['known_at'])
        if body['trade_date']!=day or known.date()!=now.date() or known>now:
            raise SectorContextFailure('TEAM_SECTOR_CONTEXT_TIME_UNPROVEN')
        marker=Path(settings.workflow_output_dir)/'auction_base'/(day+'.json')
        if marker.resolve()!=marker:raise SectorContextFailure('TEAM_SECTOR_PARENT_PATH_INVALID')
        parent=json.loads(bounded_read(marker,policy['parent_marker_max_bytes'],'TEAM_SECTOR_PARENT_SIZE_EXCEEDED'))
        if (parent['status']!='READY' or parent['prepared_snapshot']['snapshot_hash']!=body['source_snapshot_hash']
                or parent['prepared_snapshot']['snapshot_id']!=body['source_snapshot_id']):
            raise SectorContextFailure('TEAM_SECTOR_PARENT_BINDING_UNPROVEN')
        if stamp(body['cycle']['as_of'])>known:raise SectorContextFailure('TEAM_SECTOR_CYCLE_TIME_UNPROVEN')
        projected=projected_body(body) if schema=='team-sector-context/1' else dict(body,body_sha256=pin)
        size=require_projected_capacity(projected,policy)
        cycle=projected['cycle']
        return dict(status='READY',cycle=cycle,known_at=known.isoformat(),body_sha256=pin,
            source_snapshot_id=body['source_snapshot_id'],source_snapshot_hash=body['source_snapshot_hash'],
            source_cycle_sha256=projected['source_cycle_sha256'],
            source_schema_version=schema,projection_version=PROJECTION_VERSION,
            source_bytes=len(raw),projected_bytes=size,projected_max_bytes=policy['projected_max_bytes'])
    except Exception as exc:
        failure=exc if isinstance(exc,SectorContextFailure) else SectorContextFailure(
            'TEAM_SECTOR_CONTEXT_FILE_UNAVAILABLE' if isinstance(exc,OSError) else 'TEAM_SECTOR_CONTEXT_INVALID')
        result=dict(status='DATA_LIMITED',cycle={},known_at=None,reason_code=failure.reason_code,**failure.diagnostics)
        if store is not None and 'SIZE_EXCEEDED' in failure.reason_code:
            result.update(internal_alert_event_id=enqueue_capacity_alert(store,now=now,failure=failure),
                internal_alert_status='PENDING')
        return result


def record_current_regime(settings,store,*,live,cutoff,observed_at,prior):
    """One natural closed 5m observation, retries reuse its frozen result."""
    tick=stamp(cutoff).replace(minute=cutoff.minute-cutoff.minute%5,second=0,microsecond=0)
    day=str(tick.date());old=store.load_live_regime(day,tick)
    if old is not None:return old
    if not is_closed_trader_tick(cutoff):return None
    source=live.get('team_v2_source',{});sector=load_sector_context(settings,now=observed_at,store=store)
    market=dict(live);market['as_of']=source.get('source_as_of')
    facts=dict(market=market,emotion=source.get('emotion',{}),sector_cycle=sector['cycle'],
        sector_cycle_known_at=sector['known_at'],sector_context_binding=sector.get('body_sha256'),
        sector_context_diagnostics={key:sector[key] for key in ('reason_code','actual_bytes','limit_bytes',
            'internal_alert_event_id','internal_alert_status','source_schema_version','source_bytes',
            'projected_bytes','projected_max_bytes') if key in sector},
        full_market_frame_sha256=source.get('frame_sha256'))
    return store.record_live_regime(facts=facts,tick=tick,observed_at=stamp(observed_at),
        prior=prior,rules=load_rules(Path(settings.root)/'config/regime_live.yaml'))


def run_source_only(settings,market_data,*,clock,calendar,gate,deadline,minute_sources=None):
    """No paper account/plan required; reuse natural monitor, no new timer.

    This path only collects market evidence and advances G5. It cannot create
    a trading account, model judgment, order, or execution plan.
    """
    from .live_market import load_or_refresh_live_market_state
    from .team_v2_emotion_baseline import observe_full_market
    from .team_v2_store import TeamMeetingStore
    now=stamp(clock());cutoff=now.replace(second=0,microsecond=0)-timedelta(minutes=1)
    end=min(deadline,timer.monotonic()+8)
    work=gate.call(lambda:minute_sources.team_market(settings,clock=clock,calendar=calendar,deadline=end)
        if minute_sources is not None else load_or_refresh_live_market_state(settings,market_data,as_of=stamp(clock()),
        minimum_source_as_of=cutoff.replace(minute=cutoff.minute-cutoff.minute%5),
        snapshot_observer=lambda result:observe_full_market(settings,result,
            observed_at=stamp(clock()),calendar=calendar,deadline=end)),deadline=end,name='team-g5-source-only')
    live=work.value if work.status=='READY' else {'status':'DATA_BLOCKED'}
    try:
        store=TeamMeetingStore(Path(settings.root)/'state/team_v2/team.db')
        state=record_current_regime(settings,store,live=live,cutoff=cutoff,observed_at=stamp(clock()),prior='DATA_BLOCK')
    except Exception:
        return dict(status='DEGRADED',reason_code='G5_SOURCE_ONLY_UPDATE_FAILED',new_entries=0,model_calls=0)
    return dict(status='NOOP',reason_code='G5_SOURCE_ONLY_NO_TRADING_ACCOUNT_OR_MORNING',
        live_regime=state,market_status=live.get('status'),new_entries=0,model_calls=0)
