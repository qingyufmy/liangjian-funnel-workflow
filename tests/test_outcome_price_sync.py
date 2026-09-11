import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.evaluation.price_sync import refresh_current_outcome_prices
from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.settings import Settings
from liangjian_funnel.cli import main


NOW = datetime(2026, 9, 11, 16, 10, tzinfo=ZoneInfo('Asia/Shanghai'))


class Client:
    calls = []
    mode = 'ready'

    def __init__(self, settings):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def history_1d(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        if self.mode == 'exception':
            raise RuntimeError('secret-must-not-leak')
        days = [-1, 0, 1] if self.mode == 'ready' else [-1]
        return HithinkFetchResult(endpoint='history', ok=True, complete=True,
            reason_code='OK', fetch_time=NOW, pages=1, total=len(days), limit=100,
            items=tuple(HithinkRow.model_validate({'symbol': symbol,
                'date_ms': int((NOW.replace(hour=0, minute=0) + timedelta(days=d)).timestamp()*1000),
                'close': 0 if self.mode == 'invalid' else 10, 'high': 11, 'low': 9}) for d in days))


def store(rows):
    return SimpleNamespace(list_outcome_labels=lambda **kw: rows)


def label(symbol='002295.SZ', trade_date='2026-09-10', **kw):
    return {'symbol': symbol, 'trade_date': trade_date, 'stage': 'A2', 'decision': 'REJECT', **kw}


def test_tracks_rejected_stock_outside_research_without_future_bar(tmp_path):
    Client.calls = []; Client.mode = 'ready'
    settings = Settings.from_env({}, root=tmp_path)
    rows = [label(), label(), label('600001.SH', '2026-08-10'),
            label('600002.SH', labeled_at='done'), label('600003.SH', '2026-09-11')]
    result = refresh_current_outcome_prices(store(rows), settings, now=NOW, client_factory=Client)
    assert result['updated_symbols'] == ['002295.SZ']
    assert result['requests'] == 1
    assert result['status'] == 'COMPLETED'
    assert Client.calls[0][1]['adjust'] == 'none'
    cache = LocalFactCache(settings.fact_cache_db_path)
    assert len(cache.query_daily_bars('002295.SZ')) == 2
    assert rows[0]['decision'] == 'REJECT'
    again = refresh_current_outcome_prices(store(rows), settings, now=NOW, client_factory=Client)
    assert again['requests'] == 0 and not again['network_used']


def test_missing_suspension_like_response_stays_missing_and_requests_bounded(tmp_path):
    Client.calls = []; Client.mode = 'stale'
    settings = Settings.from_env({}, root=tmp_path)
    result = refresh_current_outcome_prices(store([label(), label('600001.SH')]),
        settings, now=NOW, client_factory=Client, max_requests=1)
    assert result['status'] == 'DATA_LIMITED'
    assert result['requests'] == 1
    assert len(result['deferred_symbols']) == 1
    assert len(result['missing_symbols']) == 2
    assert not result['updated_symbols']


def test_before_close_and_weekend_never_fetch(tmp_path):
    settings = Settings.from_env({}, root=tmp_path)
    for current in (NOW.replace(hour=14), NOW+timedelta(days=1)):
        Client.calls = []
        result = refresh_current_outcome_prices(store([label()]), settings, now=current, client_factory=Client)
        assert result['status'] == 'NOOP' and Client.calls == []


def test_source_exception_is_redacted_and_does_not_fake_completion(tmp_path):
    Client.calls = []; Client.mode = 'exception'
    result = refresh_current_outcome_prices(store([label()]), Settings.from_env({}, root=tmp_path),
        now=NOW, client_factory=Client)
    assert result['status'] == 'DATA_LIMITED'
    assert 'secret' not in str(result)
    assert result['missing_symbols'] == ['002295.SZ']


def test_monday_includes_friday_but_not_same_day_signals(tmp_path):
    Client.calls = []; Client.mode = 'stale'
    result = refresh_current_outcome_prices(store([label(trade_date='2026-09-11'),
        label('600001.SH', '2026-09-14')]), Settings.from_env({}, root=tmp_path),
        now=NOW+timedelta(days=3), client_factory=Client)
    assert result['tracked_symbol_count'] == 1
    assert result['requests'] == 1


def test_bad_price_is_not_a_successful_refresh(tmp_path):
    Client.calls = []; Client.mode = 'invalid'
    settings = Settings.from_env({}, root=tmp_path)
    result = refresh_current_outcome_prices(store([label()]), settings, now=NOW, client_factory=Client)
    assert result['status'] == 'DATA_LIMITED'
    assert LocalFactCache(settings.fact_cache_db_path).query_daily_bars('002295.SZ') == []


def test_explicit_refresh_cli_keeps_incomplete_prices_visible(tmp_path, monkeypatch, capsys):
    settings = Settings.from_env({}, root=tmp_path)
    LocalFactCache(settings.fact_cache_db_path)
    monkeypatch.setattr('liangjian_funnel.evaluation.price_sync.refresh_current_outcome_prices',
        lambda *_: {'status': 'DATA_LIMITED', 'network_used': True, 'missing_symbols': ['002295.SZ']})
    assert main(['run-outcomes-refresh'], settings=settings) == 2
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'DATA_LIMITED'
    assert result['network_used'] is True
    assert result['models_called'] is False
