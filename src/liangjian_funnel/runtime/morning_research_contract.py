"""Morning consumes the previous exchange session's close; never runs research.

07:00 auction-base is evidence preparation, 08:30 is a read-only plan report,
and 09:26 only validates auction quotes and activates already published plans.
Failed/unavailable research is an explicit internal alert, not market EMPTY_SCOPE.
"""
from ..research_slots import research_slot, require_research_slot
from datetime import datetime
import json
import hashlib
from pathlib import Path
import re

GOOD = frozenset({'READY', 'READY_DEGRADED', 'PUBLISHED'})


def inspect_morning_research(store, settings, plans, *, now, calendar=None):
    from .calendar import ExchangeTradingCalendar
    previous = (calendar or ExchangeTradingCalendar()).previous_trading_day(now.date()).isoformat()
    lane = getattr(settings, 'research_primary_lane_id', 'lane_1')
    all_records = store.list_workflow_runs(limit=200)
    records = [row for row in all_records
               if row.get('lane_id') == lane and research_slot(row.get('slot')) == 'close'
               and row.get('trade_date') == previous]
    latest = max(records, key=lambda row: (row.get('updated_at') or '', row['run_id']), default=None)
    sources, rejected = {}, []
    for plan in plans:
        try:
            payload = json.loads(plan.get('payload_json') or '{}')
            source = payload.get('source_run_id')
        except (TypeError, ValueError, AttributeError):
            source = None
        key = (source, plan.get('lane_id'))
        if key not in sources:
            record = store.get_workflow_run(source, key[1]) if isinstance(source, str) and source else None
            # A completed late-night recovery/weekend prep retains the real
            # market session in its bounded existing summary, not wall-clock day.
            summary = None
            if isinstance(source, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', source):
                path = Path(settings.workflow_output_dir) / 'runs' / (source + '.json')
                try:
                    if path.is_file() and not path.is_symlink() and path.stat().st_size <= 2_000_000:
                        value = json.loads(path.read_bytes())
                        if isinstance(value, dict) and value.get('run_id') == source:
                            summary = value
                except (OSError, ValueError):
                    pass
            market_day = (summary or {}).get('market_trade_date') or (record or {}).get('trade_date')
            slot = research_slot((summary or {}).get('slot') or (record or {}).get('slot'))
            status = (summary or {}).get('status') or (record or {}).get('status')
            reason = ('MORNING_SOURCE_RESEARCH_UNPROVEN' if not record and not summary
                      else 'MORNING_SOURCE_NOT_PREVIOUS_CLOSE' if slot != 'close' or market_day != previous
                      else 'MORNING_SOURCE_RESEARCH_FAILED' if status not in GOOD else None)
            sources[key] = dict(source_run_id=source, market_trade_date=market_day, slot=slot,
                                status=status, reason_code=reason)
        if sources[key]['reason_code']:
            rejected.append(dict(plan_id=plan['plan_id'], reason_code=sources[key]['reason_code']))
    if latest and latest['status'] not in GOOD:
        issue = 'PREVIOUS_CLOSE_RESEARCH_FAILED'
    elif rejected:
        issue = 'MORNING_PLAN_SOURCE_UNPROVEN'
    elif not latest and not plans:
        issue = 'PREVIOUS_CLOSE_RESEARCH_UNAVAILABLE'
    else:
        issue = None
    return dict(schema='morning-research-contract/1', previous_session=previous,
                plan_origin='PREVIOUS_SESSION_CLOSE_ONLY', activation='AUCTION_VALIDATION_ONLY',
                research_model_calls=0, status='BLOCKED' if issue else 'READY', reason_code=issue,
                latest_prior_close={k: latest.get(k) for k in ('run_id','status','trade_date','slot')} if latest else None,
                source_checks=list(sources.values()), rejected_plans=rejected,
                reference_scan_limit=200, reference_scan_truncated=len(all_records) >= 200)


def enqueue_morning_research_alert(settings, diagnostic, *, now):
    """Durable internal queue only. The existing internal job owns actual send."""
    reason = diagnostic['reason_code']
    if not reason:
        return dict(status='NOT_NEEDED', actual_http=0)
    root = getattr(settings, 'root', None)
    path = Path(root) / 'state/team_v2/team.db' if root is not None else None
    if path is None or not path.is_file() or path.is_symlink():
        return dict(status='UNAVAILABLE', reason_code='INTERNAL_STORE_UNAVAILABLE', actual_http=0)
    from .team_v2_store import TeamMeetingStore
    from .team_v2_internal import InternalOutbox
    prior = diagnostic['previous_session']
    source = (diagnostic.get('latest_prior_close') or {}).get('run_id') or 'UNAVAILABLE'
    body = {'msg_type':'text', 'content':{'text':
        f'量见晨间研究告警：{reason}；上一交易日={prior}；研究={source}。'
        '本轮不启动A2/A3，不造计划，不沿用过期名单；竞价仅验证已发布的合格收盘计划。'}}
    identity = hashlib.sha256(json.dumps([prior, reason, source]).encode()).hexdigest()
    try:
        event = InternalOutbox(TeamMeetingStore(path)).enqueue(account_id='local-paper',
            trade_date=now.date().isoformat(), kind='ALERT', source_id='morning-research:' + identity,
            body=body, observed_at=now, priority=5)
    except Exception:
        return dict(status='UNAVAILABLE', reason_code='INTERNAL_ALERT_QUEUE_FAILED', actual_http=0)
    return dict(status='QUEUED_PENDING_DELIVERY', event_id=event, actual_http=0)


def morning_research_diagnostic(app, plans, *, now):
    result = inspect_morning_research(app.store, app.settings, plans, now=now,
                                      calendar=getattr(app, 'trading_calendar', None))
    result['internal_alert'] = enqueue_morning_research_alert(app.settings, result, now=now)
    return result
