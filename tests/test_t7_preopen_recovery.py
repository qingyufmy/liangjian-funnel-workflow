"""Pre-open attention uses today's verified source and preserves prior runs."""
from datetime import datetime, date
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel import cli, workflow
from liangjian_funnel.settings import Settings

TZ = ZoneInfo('Asia/Shanghai')


@pytest.mark.parametrize('stamp,explicit_market,expected', [
    ('2026-11-03T07:10:00+08:00', False, date(2026, 11, 3)),
    ('2026-11-03T07:10:00+08:00', True, date(2026, 11, 2)),
    ('2026-11-07T07:10:00+08:00', False, date(2026, 11, 6)),
    ('2026-11-03T09:26:00+08:00', False, date(2026, 11, 2)),
    ('2026-11-03T15:10:00+08:00', False, date(2026, 11, 3)),
])
def test_actual_snapshot_collector_date(tmp_path, monkeypatch, stamp, explicit_market, expected):
    now = datetime.fromisoformat(stamp)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)
    monkeypatch.setattr(workflow, 'datetime', Clock)
    monkeypatch.setattr(workflow, 'load_yaml', lambda _: {})
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    app = SimpleNamespace(settings=Settings.from_env({}, root=tmp_path),
                          trading_calendar=ExchangeTradingCalendar())
    seen = []
    class Collected(Exception): pass
    def collector(**kwargs):
        seen.append(kwargs)
        raise Collected()
    monkeypatch.setattr(workflow, 'collect_eastmoney_hot100', collector)
    with pytest.raises(Collected):
        workflow.WorkflowApplication._prepare_snapshot(app, as_of=now,
            market_data_as_of=now if explicit_market else None)
    assert seen[0]['expected_trade_date'] == expected
    assert seen[0]['as_of'] == now
    assert 'force_refresh' not in seen[0]


def test_cli_new_snapshot_gets_distinct_run_without_reusing_daily_receipt(tmp_path, monkeypatch):
    calls = []
    def run(*args, **kwargs):
        calls.append(kwargs)
        raise workflow.WorkflowError('TEST_CAPTURED')
    monkeypatch.setattr(cli, 'WorkflowApplication', lambda settings: SimpleNamespace(run_research=run))
    settings = Settings.from_env({}, root=tmp_path)
    for snapshot in ('snapshot-first', 'snapshot-second', 'snapshot-first'):
        cli.main(['run-research', '--slot', 'morning', '--primary-only', '--same-day-recovery',
                  '--as-of', '2026-11-03T07:10:00+08:00', '--snapshot-id', snapshot], settings=settings)
    ids = [v['run_id_override'] for v in calls]
    assert ids[0] != ids[1] and ids[0] == ids[2]
    assert all(v.startswith('same-day-recovery-2026-11-03-') and len(v) <= 64 for v in ids)
    assert all(v['same_day_recovery'] and v['from_active_a1'] for v in calls)
