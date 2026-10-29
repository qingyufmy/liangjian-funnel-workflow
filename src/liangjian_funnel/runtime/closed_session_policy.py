"""One scoped1195 grant; absence/mismatched session retains06:30."""
from datetime import datetime
RECOVERY_AUTHORIZATION = dict(authority='Claude1195', source_trade_date='2026-10-29',
    target_trade_date='2026-10-30', effective_from='2026-10-30T07:00:00+08:00',
    closed_session_deadline='2026-10-30T09:15:00+08:00',
    reason='Tony07:00 directive; factual closed market capture window')

def authorized_deadline(session, *, authorization=None):
    grant = RECOVERY_AUTHORIZATION if authorization is None else authorization
    if not grant:
        return None
    if grant != RECOVERY_AUTHORIZATION:
        raise ValueError('CLOSED_SESSION_OVERRIDE_AUTHORITY_INVALID')
    if (session.source_trade_date.isoformat()!=grant['source_trade_date']
            or session.target_trade_date.isoformat()!=grant['target_trade_date']
            or session.wall_clock<datetime.fromisoformat(grant['effective_from'])):
        return None
    return datetime.fromisoformat(grant['closed_session_deadline'])
