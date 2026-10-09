"""Shared date contract for published plans, activation and status views."""
import json
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')


def plan_time(value):
    """Parse an explicit aware timestamp, without filling missing dates."""
    try:
        stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value or ''))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            return None
        return stamp.astimezone(TZ)
    except (ValueError, TypeError):
        return None


def activation_reason(plan, at):
    """Return a rejection code; never extend or infer an absent expiry."""
    current = plan_time(at)
    if current is None:
        return 'PLAN_SESSION_TIME_INVALID'
    expiry = plan_time(plan.get('expires_at'))
    if expiry is None:
        return 'PLAN_EXPIRY_INVALID'
    if expiry < current:
        return 'PLAN_EXPIRED'
    if expiry.date() != current.date():
        return 'PLAN_SESSION_MISMATCH'
    try:
        payload = plan.get('payload_json') or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        target = payload.get('target_trade_date')
        if target and target != current.date().isoformat():
            return 'PLAN_SESSION_MISMATCH'
    except (ValueError, TypeError, AttributeError):
        return 'PLAN_PAYLOAD_INVALID'
    return None


def active_plan_reason(plan, at):
    """Validate an active interval at an explicit operation/replay clock.

    Entry validity does not control the independent position protection lane.
    Direct publication checks at valid_from, allowing a same-session scheduled
    start; omitted generic activation checks the retained start at wall time.
    """
    reason = activation_reason(plan, at)
    if reason:
        return reason
    start = plan_time(plan.get('valid_from'))
    if start is None:
        return 'PLAN_VALID_FROM_INVALID'
    current = plan_time(at)
    if start.date() != current.date():
        return 'PLAN_SESSION_MISMATCH'
    if start > current:
        return 'PLAN_NOT_YET_VALID'
    return None


def visible_current_plan(plan, at):
    if plan.get('status') == 'PENDING_MORNING_REVIEW':
        # Tomorrow's pending publication remains visible, but cannot be active today.
        return activation_reason(plan, at) in (None, 'PLAN_SESSION_MISMATCH') and _pending_not_expired(plan, at)
    if activation_reason(plan, at):
        return False
    if plan.get('status') != 'ACTIVE_TODAY':
        return False
    try:
        start = datetime.fromisoformat(str(plan.get('valid_from') or ''))
        return start.tzinfo is not None and start.astimezone(TZ).date() == at.astimezone(TZ).date() and start <= at
    except (TypeError, ValueError):
        return False


def _pending_not_expired(plan, at):
    try:
        expiry = datetime.fromisoformat(str(plan.get('expires_at') or ''))
        return expiry.tzinfo is not None and expiry >= at
    except (TypeError, ValueError):
        return False
