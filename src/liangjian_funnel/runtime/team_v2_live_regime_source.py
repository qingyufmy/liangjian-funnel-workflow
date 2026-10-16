"""Actual current breadth/emotion + explicitly historical sector context.

Sector-history geometry is captured from the natural auction-base parent;
it is not yesterday's regime and is never relabelled as intraday history.
No new provider, model, activation or notification path is created here.
"""
import hashlib
import json
from pathlib import Path

from .team_v2_emotion_baseline import _sealed
from .team_v2_rule_source import _write_once
from .team_v2_sources import stamp
from .team_v2_store import canonical
from .team_v2_regime import load_rules
from .trader_guardrails import is_closed_trader_tick


def capture_sector_context(settings,prepared,*,known_at):
    now=stamp(known_at);root=Path(settings.root).resolve()
    directory=root/'state/team_v2/sector_context'
    if directory.resolve()!=directory:raise ValueError('TEAM_SECTOR_DIRECTORY_INVALID')
    cycle=prepared.snapshot.data.get('SECTOR_CYCLE_SNAPSHOT',{})
    body=_sealed(dict(schema_version='team-sector-context/1',trade_date=str(now.date()),
        known_at=now.isoformat(),source_snapshot_id=prepared.snapshot.snapshot_id,
        source_snapshot_hash=prepared.snapshot.snapshot_hash,cycle=cycle,
        authority='HISTORICAL_SECTOR_GEOMETRY_NOT_YESTERDAY_REGIME'))
    path=directory/(str(now.date())+'.json')
    if path.exists():
        if path.is_symlink() or path.read_text(encoding='utf-8')!=canonical(body):
            raise ValueError('TEAM_SECTOR_CONTEXT_CONFLICT')
    else:_write_once(path,body)
    return dict(status='CAPTURED',body_sha256=body['body_sha256'])


def load_sector_context(settings,*,now):
    now=stamp(now);root=Path(settings.root).resolve();day=str(now.date())
    path=root/'state/team_v2/sector_context'/(day+'.json')
    try:
        if path.resolve()!=path or path.stat().st_size>2*1024*1024:raise ValueError
        body=json.loads(path.read_bytes());pin=body.pop('body_sha256')
        if hashlib.sha256(canonical(body).encode()).hexdigest()!=pin:raise ValueError
        known=stamp(body['known_at'])
        if body['trade_date']!=day or known.date()!=now.date() or known>now:raise ValueError
        marker=Path(settings.workflow_output_dir)/'auction_base'/(day+'.json')
        if marker.is_symlink() or marker.stat().st_size>2*1024*1024:raise ValueError
        parent=json.loads(marker.read_bytes())
        if (parent['status']!='READY' or parent['prepared_snapshot']['snapshot_hash']!=body['source_snapshot_hash']
                or parent['prepared_snapshot']['snapshot_id']!=body['source_snapshot_id']):raise ValueError
        cycle=body['cycle']
        if stamp(cycle['as_of'])>known:raise ValueError
        return dict(cycle=cycle,known_at=known.isoformat(),body_sha256=pin,
            source_snapshot_id=body['source_snapshot_id'],source_snapshot_hash=body['source_snapshot_hash'])
    except Exception:return dict(cycle={},known_at=None,reason_code='LIVE_SECTOR_CONTEXT_UNPROVEN')


def record_current_regime(settings,store,*,live,cutoff,observed_at,prior):
    """One natural closed 5m observation, retries reuse its frozen result."""
    tick=stamp(cutoff).replace(minute=cutoff.minute-cutoff.minute%5,second=0,microsecond=0)
    day=str(tick.date());old=store.load_live_regime(day,tick)
    if old is not None:return old
    if not is_closed_trader_tick(cutoff):return None
    source=live.get('team_v2_source',{});sector=load_sector_context(settings,now=observed_at)
    market=dict(live);market['as_of']=source.get('source_as_of')
    facts=dict(market=market,emotion=source.get('emotion',{}),sector_cycle=sector['cycle'],
        sector_cycle_known_at=sector['known_at'],sector_context_binding=sector.get('body_sha256'),
        full_market_frame_sha256=source.get('frame_sha256'))
    return store.record_live_regime(facts=facts,tick=tick,observed_at=stamp(observed_at),
        prior=prior,rules=load_rules(Path(settings.root)/'config/regime_live.yaml'))
