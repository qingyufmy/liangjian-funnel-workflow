"""0635: immutable market-session identity, separate from actual capture time."""
from collections.abc import Mapping
from datetime import datetime
from zoneinfo import ZoneInfo
from .calendar import closed_research_session

KEY = 'CLOSED_SESSION_CAPTURE'


def aware(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('CLOSED_SESSION_CAPTURE_TIME_INVALID')
    return value.astimezone(ZoneInfo('Asia/Shanghai'))


def build_capture_proof(*, source_trade_date, started_at, captured_at, completed_at, calendar):
    started, captured, completed = map(aware, (started_at, captured_at, completed_at))
    session = closed_research_session(completed, calendar)
    initial = closed_research_session(started, calendar)
    if (source_trade_date != session.source_trade_date.isoformat()
            or initial.source_trade_date != session.source_trade_date
            or not session.market_close <= started <= captured <= completed):
        raise ValueError('CLOSED_SESSION_CAPTURE_BINDING_INVALID')
    proof = dict(authority='0635', source_trade_date=source_trade_date,
        target_trade_date=session.target_trade_date.isoformat(),
        source_market_close_at=session.market_close.isoformat(),
        capture_started_at=started.isoformat(), capture_as_of=captured.isoformat(),
        capture_completed_at=completed.isoformat(),
        publication_deadline=session.publication_deadline.isoformat())
    if session.publication_deadline.hour==9 and session.publication_deadline.minute==15:
        from .closed_session_policy import RECOVERY_AUTHORIZATION
        proof.update(deadline_override=session.publication_deadline.isoformat(),
            deadline_override_authority=RECOVERY_AUTHORIZATION['authority'],
            deadline_override_reason=RECOVERY_AUTHORIZATION['reason'])
    return proof


def validate_capture_proof(proof, *, captured_at, calendar, now=None, source_trade_date=None):
    if not isinstance(proof, Mapping):
        raise ValueError('CLOSED_SESSION_CAPTURE_PROOF_REQUIRED')
    expected = build_capture_proof(source_trade_date=proof.get('source_trade_date'),
        started_at=proof.get('capture_started_at'), captured_at=captured_at,
        completed_at=proof.get('capture_completed_at'), calendar=calendar)
    if dict(proof) != expected or (source_trade_date is not None
            and expected['source_trade_date'] != source_trade_date):
        raise ValueError('CLOSED_SESSION_CAPTURE_BINDING_INVALID')
    if now is not None:
        current = aware(now)
        session = closed_research_session(current, calendar)
        if (aware(expected['capture_completed_at']) > current
                or session.source_trade_date.isoformat() != expected['source_trade_date']
                or session.target_trade_date.isoformat() != expected['target_trade_date']):
            raise ValueError('CLOSED_SESSION_CAPTURE_STALE_OR_FUTURE')
    return expected


def generation_capture_proof(generation, *, calendar, now, source_trade_date):
    if not hasattr(generation, 'manifest'):
        return None  # Compatibility with legacy receivers; caller keeps old date gate.
    manifest = generation.manifest
    if not isinstance(manifest, Mapping) or not isinstance(manifest.get('delta', {}), Mapping):
        raise ValueError('CLOSED_SESSION_CAPTURE_PROOF_INVALID')
    daily = manifest.get('delta', {}).get('daily_refresh_proof', {})
    if not isinstance(daily, Mapping):
        raise ValueError('CLOSED_SESSION_CAPTURE_PROOF_INVALID')
    if 'closed_session' not in daily:
        return None  # Old same-day generations retain their original contract.
    proof = daily['closed_session']
    validated = validate_capture_proof(proof, captured_at=generation.as_of,
        calendar=calendar, now=now, source_trade_date=source_trade_date)
    if (daily.get('snapshot_id') != generation.snapshot_id
            or daily.get('snapshot_hash') != generation.snapshot_hash):
        raise ValueError('CLOSED_SESSION_CAPTURE_SNAPSHOT_MISMATCH')
    return validated


def validate_research_capture_proof(proof, *, captured_at, calendar, now, source_trade_date):
    """Reuse an original valid seed after publication closes, for research only.

    The captured proof is unchanged and still had to satisfy its original
    capture window. Only its current-day publication eligibility is irrelevant.
    """
    from .calendar import latest_closed_research_session
    validated = validate_capture_proof(proof, captured_at=captured_at,
        calendar=calendar, source_trade_date=source_trade_date)
    current = aware(now)
    session = latest_closed_research_session(current, calendar)
    if (aware(validated['capture_completed_at']) > current
            or validated['source_trade_date'] != session.source_trade_date.isoformat()
            or validated['target_trade_date'] != session.target_trade_date.isoformat()):
        raise ValueError('RESEARCH_UPDATE_SEED_STALE_OR_FUTURE')
    return validated
