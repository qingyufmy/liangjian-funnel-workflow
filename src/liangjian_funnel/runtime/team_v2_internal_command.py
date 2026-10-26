"""Dedicated bounded internal delivery command; no trading/model acquisition."""
from datetime import datetime, time
import os

from .calendar import ExchangeTradingCalendar
from .team_v2_internal import InternalOutbox, InternalSender
from .team_v2_internal_events import InternalEventProjector
from .team_v2_morning import ACCOUNT, TZ, _directory
from .team_v2_sources import stamp
from .team_v2_store import TeamMeetingStore


def run_internal_delivery(settings, *, clock=None, calendar=None, sender=None):
    clock = clock or (lambda:datetime.now(TZ)); now = stamp(clock())
    calendar = calendar or ExchangeTradingCalendar()
    if calendar.is_trading_day(now.date()) is not True:
        return dict(status='NOOP', reason_code='NON_TRADING_DAY')
    if not time(8, 45) <= now.time().replace(tzinfo=None) < time(17, 56):
        return dict(status='NOOP', reason_code='INTERNAL_DELIVERY_WINDOW_CLOSED')
    directory = _directory(settings)
    if not (directory/'team.db').is_file():
        return dict(status='NOOP', reason_code='TEAM_ACCOUNT_NOT_INITIALIZED')
    store = TeamMeetingStore(directory/'team.db'); outbox = InternalOutbox(store)
    from .node_identity import observe_node_identity
    try:node_identity=observe_node_identity(settings,store,now=now)
    except (OSError,ValueError):node_identity=dict(status='UNAVAILABLE',reason_code='NODE_IDENTITY_OBSERVATION_UNAVAILABLE')
    from .monitor_slow_warning import project_slow_warning
    try:slow_warning=project_slow_warning(settings,now=now)
    except (OSError,ValueError,KeyError,TypeError):slow_warning=dict(status='UNAVAILABLE',reason_code='MONITOR_WARNING_UNPROVEN')
    projector = InternalEventProjector(outbox)
    projection = projector.project(paper_path=directory/'paper.db', account_id=ACCOUNT, now=now,
        rule_alert_path=directory/'rules'/(now.date().isoformat()+'.alert.json'))
    sender = sender or InternalSender(settings.root.parent/'liangjian-shadow/internal_alert.env', env=os.environ)
    delivery = outbox.flush(sender, now=now, maximum=3)
    projector.sync_acknowledgments(account_id=ACCOUNT, trade_date=now.date().isoformat())
    good = projection['status'] == 'READY' and delivery['reason_code'] == 'INTERNAL_FLUSH_COMPLETED'
    return dict(status='READY' if good else 'DATA_LIMITED', trade_date=now.date().isoformat(),
        reason_code=delivery['reason_code'], projection_reason_codes=projection['reason_codes'],
        attempted=delivery['attempted'], sent=delivery['sent'],
        pending=delivery.get('pending'),unconfirmed=delivery.get('unconfirmed'),
        delivery_health='UNCONFIRMED' if delivery.get('unconfirmed') else 'CONFIRMED',
        new_failure_notices=delivery.get('new_failure_notices',0),node_identity=node_identity,slow_warning=slow_warning)
