"""Owned paper commands: no legacy workflow/store, provider, or notification."""
from datetime import datetime, time
from zoneinfo import ZoneInfo

from .broker.local_paper import create_broker
from .calendar import ExchangeTradingCalendar


def run_paper_settle(settings, *, now=None, calendar=None):
    now=now or datetime.now(ZoneInfo('Asia/Shanghai'))
    if not isinstance(now,datetime) or now.utcoffset() is None:raise ValueError('PAPER_CLOCK_INVALID')
    now=now.astimezone(ZoneInfo('Asia/Shanghai'));calendar=calendar or ExchangeTradingCalendar()
    if calendar.is_trading_day(now.date()) is not True:
        return dict(status='NOOP',reason_code='NON_TRADING_DAY')
    if now.time().replace(tzinfo=None)<time(15,30):
        return dict(status='NOOP',reason_code='PAPER_SETTLE_WINDOW_NOT_OPEN')
    directory=settings.root/'state/team_v2';database=directory/'paper.db'
    # Never invent an empty account to make a missing operational result green.
    if not database.is_file():return dict(status='BLOCKED',reason_code='PAPER_ACCOUNT_NOT_INITIALIZED')
    if directory.is_symlink() or database.is_symlink() or directory.resolve()!=settings.root.resolve()/'state/team_v2':
        raise ValueError('PAPER_DATABASE_SCOPE_INVALID')
    broker=create_broker('local_paper',database=database,paper_config=settings.root/'config/paper_broker.yaml',
        risk_config=settings.root/'config/risk_limits.yaml',clock=lambda:now,calendar=calendar,
        run_id='paper-settle:'+now.date().isoformat())
    from .team_v2_close_bar import run_close_bar
    # Reuse original frozen close only; settlement never starts provider work.
    run_close_bar(settings,clock=lambda:now,calendar=calendar,reuse_only=True,broker=broker)
    report=broker.settle(now.date().isoformat())
    if report['reconcile']['status']!='MATCH':
        return {**report,'status':'RECONCILIATION_FAILED','settlement_status':report['status'],
            'reason_code':'BROKER_RECONCILE_MISMATCH'}
    return report
