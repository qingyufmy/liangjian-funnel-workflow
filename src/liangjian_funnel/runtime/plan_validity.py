"""Shared date contract for published plans, activation and status views."""
import json
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')


def activation_reason(plan, at):
    """Return a rejection code; never extend or infer an absent expiry."""
    if at.tzinfo is None:
        return 'PLAN_SESSION_TIME_INVALID'
    current = at.astimezone(TZ)
    try:
        expiry = datetime.fromisoformat(str(plan.get('expires_at') or ''))
        if expiry.tzinfo is None:
            return 'PLAN_EXPIRY_INVALID'
        expiry = expiry.astimezone(TZ)
    except (ValueError, TypeError):
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
