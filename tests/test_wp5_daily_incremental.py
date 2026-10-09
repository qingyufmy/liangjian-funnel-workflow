from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache

TZ = ZoneInfo('Asia/Shanghai')
NOW = datetime(2026, 10, 9, 15, 10, tzinfo=TZ)


def history():
    return [{'date_ms': int((NOW.replace(hour=0, minute=0)-timedelta(days=30-i)).timestamp()*1000),
             'open_price': 10+i, 'high_price': 12+i, 'low_price': 9+i,
             'close_price': 11+i, 'volume': 1000+i, 'turnover': 10000+i} for i in range(31)]


class Client:
    def __init__(self, rows=None):
        self.rows = history() if rows is None else rows
        self.calls = []

    def history_1d(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        rows = [row for row in self.rows if kwargs['start'] <= row['date_ms'] < kwargs['end']]
        return HithinkFetchResult(endpoint='history', ok=True, complete=True, reason_code='OK',
            items=tuple(HithinkRow.model_validate(r) for r in rows), fetch_time=NOW)


def seed(cache, symbol, rows):
    cache.upsert_daily_bars([{'symbol': symbol,
        'timestamp': datetime.fromtimestamp(r['date_ms']/1000, TZ), 'adjust': 'none',
        'fetched_at': NOW-timedelta(days=1), 'payload': r} for r in rows])
    cache.update_sync_state('HITHINK_DAILY_1D', symbol, last_success=NOW-timedelta(days=1),
                            cursor={'through': str(rows[-1]['date_ms'])}, status='READY', reason=None)


def test_incremental_requests_only_after_latest_closed_bar(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    seed(cache, '600000.SH', history()[:-1])
    client = Client()
    result = HithinkIncrementalSynchronizer(cache).sync(client, ['600000.SH'],
        as_of=NOW, include_financial=False)
    assert client.calls[0][1]['start'] == history()[-2]['date_ms']+1
    assert result.daily_requests['600000.SH']['mode'] == 'INCREMENTAL'
    assert result.failures == {}


def test_two_hundred_incremental_series_equal_full_fetch(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    symbols = [f'{600000+i}.SH' for i in range(200)]
    for symbol in symbols:
        seed(cache, symbol, history()[:-1])
    client = Client()
    incremental = HithinkIncrementalSynchronizer(cache).sync(client, symbols,
        as_of=NOW, compact_daily_bars=31, include_financial=False)
    full = HithinkIncrementalSynchronizer(LocalFactCache(tmp_path/'full.sqlite3')).sync(
        Client(), symbols, as_of=NOW, compact_daily_bars=31, include_financial=False)
    assert incremental.daily == full.daily
    assert len(client.calls) == 200
    assert all(row['mode'] == 'INCREMENTAL' for row in incremental.daily_requests.values())


def test_short_new_stock_history_requests_bootstrap_not_narrow_delta(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    seed(cache, '600000.SH', history()[-5:-1])
    client = Client()
    result = HithinkIncrementalSynchronizer(cache).sync(client, ['600000.SH'],
        as_of=NOW, include_financial=False)
    assert result.daily_requests['600000.SH']['reason_code'] == 'HISTORY_SHORT_BOOTSTRAP'
    assert client.calls[0][1]['start'] < history()[0]['date_ms']


def test_confirmed_factor_change_forces_full_rebuild_and_retains_old_versions(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    seed(cache, '600000.SH', history()[:-1])
    revised = [{**row, 'close_price': row['close_price']*0.5} for row in history()]
    client = Client(revised)
    result = HithinkIncrementalSynchronizer(cache).sync(client, ['600000.SH'],
        as_of=NOW, include_financial=False,
        daily_reset_reasons={'600000.SH': 'ADJUSTMENT_FACTOR_CHANGED'})
    assert result.daily_requests['600000.SH']['mode'] == 'FULL_REFRESH'
    assert result.daily_requests['600000.SH']['reason_code'] == 'ADJUSTMENT_FACTOR_CHANGED'
    assert result.daily['600000.SH'][-1]['close_price'] == revised[-1]['close_price']
    assert cache.get_coverage(symbol='600000.SH')['daily']['rows'] >= 31
    original = cache.query_daily_bars('600000.SH', as_of=NOW-timedelta(days=1))
    assert original[-1]['payload']['close_price'] == history()[-2]['close_price']


def test_factor_reset_partial_history_is_blocked_not_repaired_by_latest_day(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    seed(cache, '600000.SH', history()[:-1])
    client = Client(history()[-1:])
    result = HithinkIncrementalSynchronizer(cache).sync(client, ['600000.SH'],
        as_of=NOW, include_financial=False,
        daily_reset_reasons={'600000.SH': 'ADJUSTMENT_FACTOR_CHANGED'})
    assert 'DAILY:FULL_REFRESH_HISTORY_INCOMPLETE' in result.failures['600000.SH']
    assert len(client.calls) == 1
    assert len(cache.query_daily_bars('600000.SH')) == 30
    resumed = Client(history()[-1:])
    result = HithinkIncrementalSynchronizer(cache).sync(resumed, ['600000.SH'],
        as_of=NOW, include_financial=False)
    assert result.daily_requests['600000.SH']['reason_code'] == 'ADJUSTMENT_FACTOR_CHANGED'
    assert 'DAILY:FULL_REFRESH_HISTORY_INCOMPLETE' in result.failures['600000.SH']


def test_empty_suspended_or_delisted_response_does_not_manufacture_bar(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    seed(cache, '600000.SH', history()[:-1])
    result = HithinkIncrementalSynchronizer(cache).sync(Client([]), ['600000.SH'],
        as_of=NOW, include_financial=False)
    assert 'DAILY:LATEST_CLOSED_DAY_MISSING' in result.failures['600000.SH']
    assert cache.get_sync_state('HITHINK_DAILY_1D', '600000.SH')['status'] == 'FAILED'
    assert len(cache.query_daily_bars('600000.SH')) == 30


def test_radar_does_not_scan_financial_state_table(tmp_path, monkeypatch):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    monkeypatch.setattr(cache, 'list_sync_state', lambda: pytest.fail('financial state scan'))
    HithinkIncrementalSynchronizer(cache).sync(Client(), ['600000.SH'],
        as_of=NOW, include_financial=False)


def test_future_cached_revision_is_not_incremental_cursor(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    seed(cache, '600000.SH', history()[:-1])
    future = {**history()[-1], 'date_ms': history()[-1]['date_ms']+86400000}
    seed(cache, '600000.SH', [future])
    client = Client()
    result = HithinkIncrementalSynchronizer(cache).sync(client, ['600000.SH'],
        as_of=NOW, include_financial=False)
    assert client.calls[0][1]['start'] == history()[-2]['date_ms']+1
    assert result.failures == {}
