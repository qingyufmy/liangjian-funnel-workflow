"""Separate the immutable capture clock from the last closed market session."""
from datetime import datetime
from zoneinfo import ZoneInfo


def research_times(snapshot, calendar):
    from ..runtime.same_day_recovery_time import recovery_times
    recovery = recovery_times(snapshot, calendar)
    if recovery is not None:
        return recovery
    from ..runtime.research_update import research_update_times
    updated = research_update_times(snapshot, calendar)
    if updated is not None:
        return updated
    captured = snapshot.as_of
    if not isinstance(captured, datetime) or captured.utcoffset() is None:
        raise ValueError('TEAM_RESEARCH_SOURCE_TIME_REQUIRED')
    captured = captured.astimezone(ZoneInfo('Asia/Shanghai'))
    from ..runtime.closed_session_capture import KEY, validate_capture_proof, aware
    if KEY in snapshot.data and getattr(snapshot, 'capture_root', None) is not None:
        return _derived_capture_times(snapshot, calendar)
    if KEY in snapshot.data:
        proof = validate_capture_proof(snapshot.data[KEY], captured_at=captured, calendar=calendar)
        market = aware(proof['source_market_close_at'])
        if snapshot.data.get('MARKET_DATA_AS_OF') != market.isoformat():
            raise ValueError('CLOSED_SESSION_CAPTURE_MARKET_MISMATCH')
        return captured, market
    if calendar.is_trading_day(captured.date()) is True:
        if captured.hour < 15:
            raise ValueError('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED')
        # Keep the existing natural-close request contract byte-for-byte.
        return captured, captured

    # run-next-session-prep already binds this cutoff when it collects market
    # facts. Do not relabel the Sunday snapshot (including weekend news) or
    # invent a market timestamp when that binding is missing/stale.
    raw = snapshot.data.get('MARKET_DATA_AS_OF')
    try:
        market = datetime.fromisoformat(raw) if isinstance(raw, str) else raw
    except ValueError:
        market = None
    if not isinstance(market, datetime) or market.utcoffset() is None:
        raise ValueError('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED')
    market = market.astimezone(ZoneInfo('Asia/Shanghai'))
    if (market > captured or market.hour < 15
            or market.date() != calendar.previous_trading_day(captured.date())):
        raise ValueError('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED')
    return captured, market


def _derived_capture_times(snapshot, calendar):
    from ..runtime.closed_session_capture import KEY, aware, validate_capture_proof
    from ..runtime.calendar import latest_closed_research_session
    from ..runtime.publication_receipt import object_hash
    from ..runtime.research_update import replay_snapshot_clock_chain
    base = snapshot.capture_root
    def require(value, reason):
        if not value:
            raise ValueError('CLOSED_SESSION_CAPTURE_' + reason)
    require(not getattr(base, 'proof_chain', ()) and getattr(base, 'capture_root', None) is None,
        'ROOT_REQUIRED')
    require(object_hash(base.data) == base.snapshot_hash, 'ROOT_HASH_CHANGED')
    proof = validate_capture_proof(base.data.get(KEY), captured_at=base.as_of, calendar=calendar)
    knowledge = aware(snapshot.as_of)
    session = latest_closed_research_session(knowledge, calendar)
    if (aware(proof['capture_completed_at']) > knowledge
            or proof['source_trade_date'] != session.source_trade_date.isoformat()
            or proof['target_trade_date'] != session.target_trade_date.isoformat()):
        raise ValueError('CLOSED_SESSION_CAPTURE_STALE_OR_FUTURE')
    return replay_snapshot_clock_chain(snapshot, base, proof, now=knowledge, require=require)


def research_input_failure(exc, snapshot):
    """Bounded contract evidence carried into the persisted StageAudit."""
    import re
    import traceback
    from pathlib import Path
    raw = str(exc)
    code = raw if (re.fullmatch(r'[A-Z][A-Z0-9_]{1,159}', raw) and raw.startswith(
        ('CLOSED_SESSION_', 'CLOSE_RESEARCH_', 'TEAM_', 'A3_', 'CONTROL_', 'MODEL_'))) else 'TEAM_RESEARCH_INPUT_INVALID'
    proof = snapshot.data.get('CLOSED_SESSION_CAPTURE', {})
    from collections.abc import Mapping
    proof = proof if isinstance(proof, Mapping) else {}
    root = getattr(snapshot, 'capture_root', None)
    fields = dict(snapshot_id=snapshot.snapshot_id, snapshot_hash=snapshot.snapshot_hash,
        knowledge_as_of=str(snapshot.as_of), root_snapshot_id=getattr(root, 'snapshot_id', None),
        root_snapshot_hash=getattr(root, 'snapshot_hash', None),
        root_capture_as_of=str(getattr(root, 'as_of', None)),
        capture_as_of=proof.get('capture_as_of'), capture_completed_at=proof.get('capture_completed_at'),
        source_trade_date=proof.get('source_trade_date'), target_trade_date=proof.get('target_trade_date'))
    frames = [dict(file=Path(f.filename).name, function=f.name, line=f.lineno)
        for f in traceback.extract_tb(exc.__traceback__)[-12:]]
    return code, dict(exception_type=type(exc).__name__, exception_code=code,
        fields=fields, frames=frames)
