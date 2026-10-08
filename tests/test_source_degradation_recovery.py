from datetime import datetime, timedelta
import time
from types import SimpleNamespace

from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
from liangjian_funnel.data.live_fetch import fetch_live_window
from liangjian_funnel.data.publication import confirm_publications
from liangjian_funnel.data.session_windows import TZ
from liangjian_funnel.data.live_quote import SinaQuoteAdapter, normalize_sina_quote, fetch_current_quote
from liangjian_funnel.data.tencent_minute import QuoteResult, MarketQuote, TencentIntradayAdapter
from liangjian_funnel.data.source_health import source_health, summarize_health
from liangjian_funnel.runtime import process_identity
from liangjian_funnel.runtime.bounded_work import BoundedWorkGate, run_many_bounded
import pytest


AT = datetime(2026, 10, 8, 9, 31, tzinfo=TZ)
SYMBOL = "000001.SZ"


def ready():
    row = MinuteBar(symbol=SYMBOL, interval="1m", bar_end=AT, open=10,
                    high=10, low=10, close=10, volume=100, amount=1000, source_id="TEST")
    return FetchResult(symbol=SYMBOL, interval="1m", requested_bars=1,
                       returned_bars=1, bars=(row,), complete=True, reason_code="OK")


def test_hung_primary_cannot_consume_independent_fallback_budget():
    def hung(*args, **kwargs):
        time.sleep(0.8)
        return ready()
    started = time.monotonic()
    result = fetch_live_window(SimpleNamespace(fetch_bars=hung),
                               SimpleNamespace(fetch_bars=lambda *a, **k: ready()),
                               SYMBOL, "1m", 1, AT, deadline=started + 0.3)
    assert result.complete
    assert time.monotonic() - started < 0.6


def test_publication_retry_returns_before_hung_dependency_and_keeps_good_symbol():
    started = time.monotonic()
    failed = ready().model_copy(update={"complete": False, "reason_code": "TENCENT_REQUEST_FAILED"})
    def hung(symbol):
        time.sleep(1.2)
        return symbol, {"1m": ready()}
    result = confirm_publications({"good": {"1m": ready()}, "bad": {"1m": failed}},
                                  hung, at=AT, deadline=started + 0.65,
                                  sleep=lambda _: None, closed_window=True)
    assert time.monotonic() - started < 0.85
    assert not result["good"].get("publication_error")
    assert result["bad"].get("publication_error")


def test_fallback_old_day_remains_blocked_and_attempts_are_preserved():
    failed = ready().model_copy(update={"bars": (), "returned_bars": 0,
                                       "complete": False, "reason_code": "TENCENT_REQUEST_FAILED"})
    stale = ready().model_copy(update={"bars": (ready().bars[0].model_copy(
        update={"bar_end": AT - timedelta(days=1)}),)})
    result = fetch_live_window(SimpleNamespace(fetch_bars=lambda *a, **k: failed),
                               SimpleNamespace(fetch_bars=lambda *a, **k: stale),
                               SYMBOL, "1m", 1, AT)
    assert not result.complete
    assert len(result.source_attempts) >= 2
    assert any(row["reason_code"] == "CURRENT_SESSION_WINDOW_INVALID" for row in result.source_attempts)


def quote(at=AT, symbol=SYMBOL, source="TEST"):
    return QuoteResult(symbol=symbol, reason_code="OK", complete=True,
        quote=MarketQuote(symbol=symbol, quote_time=at, price=10, open=10, previous_close=10,
                          volume=100, amount=1000, source_id=source))


def sina_raw(stamp=AT):
    fields = ['0'] * 33
    fields[0:4] = ['测试', '10', '10', '10']
    fields[8:10] = ['100', '1000']
    fields[30:32] = [stamp.strftime('%Y-%m-%d'), stamp.strftime('%H:%M:%S')]
    return 'var hq_str_sz000001="' + ','.join(fields) + '";'


def test_sina_normalization_identity_units_and_no_candle_authority():
    row = normalize_sina_quote(sina_raw(), SYMBOL)
    assert row.volume == 100 and row.amount == 1000
    assert row.source_id == 'SINA:hq.sinajs.cn'
    assert 'bar_end' not in row.model_dump()
    with pytest.raises(ValueError):
        normalize_sina_quote(sina_raw(), '000002.SZ')


@pytest.mark.parametrize('stamp,reason', [(AT-timedelta(days=1), 'QUOTE_TRADE_DATE_MISMATCH'),
    (AT-timedelta(seconds=91), 'QUOTE_NOT_CURRENT'), (AT+timedelta(minutes=1), 'QUOTE_NOT_CURRENT')])
def test_sina_old_future_quotes_never_recover_a4(stamp, reason):
    result = SinaQuoteAdapter(text_fetcher=lambda *a: sina_raw(stamp)).fetch_quote(SYMBOL, as_of=AT)
    assert not result.complete and result.reason_code == reason


def test_quote_recovery_preserves_primary_failure_and_independent_origin():
    primary = SimpleNamespace(fetch_quote=lambda *a, **k: QuoteResult(symbol=SYMBOL, reason_code='TENCENT_QUOTE_REQUEST_FAILED'))
    backup = SinaQuoteAdapter(text_fetcher=lambda *a: sina_raw())
    result = fetch_current_quote(primary, backup, SYMBOL, as_of=AT)
    assert result.complete and result.quote.source_id == 'SINA:hq.sinajs.cn'
    assert [x['complete'] for x in result.source_attempts] == [False, True]
    assert result.source_attempts[0]['reason_code'] == 'TENCENT_QUOTE_REQUEST_FAILED'
    assert result.source_attempts[0]['source_provider'] == 'SimpleNamespace'
    assert result.source_attempts[0]['requested_symbol'] == SYMBOL
    assert result.source_attempts[1]['returned_symbol'] == SYMBOL


def test_hung_quote_primary_does_not_starve_backup():
    def hung(*a, **k):
        time.sleep(0.8)
        return quote()
    started = time.monotonic()
    result = fetch_current_quote(SimpleNamespace(fetch_quote=hung),
                                 SinaQuoteAdapter(text_fetcher=lambda *a: sina_raw()),
                                 SYMBOL, as_of=AT, deadline=started+0.3)
    assert result.complete and result.quote.source_id.startswith('SINA:')
    assert time.monotonic()-started < 0.6


@pytest.mark.parametrize('value', [quote(symbol='000002.SZ'),
    quote().model_copy(update={'quote': quote().quote.model_copy(update={'price': float('inf')})}),
    QuoteResult(symbol=SYMBOL, complete=True, reason_code='OK')])
def test_complete_flag_does_not_bypass_quote_identity_numeric_and_presence(value):
    result = fetch_current_quote(SimpleNamespace(fetch_quote=lambda *a, **k: value), None, SYMBOL, as_of=AT)
    assert not result.complete


def test_tencent_response_from_wrong_security_not_renamed_as_requested_security():
    result = TencentIntradayAdapter(text_fetcher=lambda *a: 'v_sz000002="bad";').fetch_quote(SYMBOL, as_of=AT)
    assert not result.complete


def test_source_health_auxiliary_failure_is_not_required_input_failure():
    pack = {'1m': ready(), 'quote': quote(), 'auxiliary_error': 'AUXILIARY_5M_DEFERRED'}
    item = source_health(SYMBOL, pack, at=AT, cutoff=AT)
    assert item['required_inputs_ready']
    assert not item['auxiliary']['execution_dependency']
    assert item['one_minute']['gap_count'] == 0
    assert item['derived_periods']['5m']['actual_last_eob'] is None
    incomplete = source_health(SYMBOL, {'quote': quote()}, at=AT, cutoff=AT)
    assert incomplete['state'] == 'DATA_BLOCKED' and incomplete['one_minute']['gap_count'] == 1
    assert incomplete['quote']['fresh']


def test_daily_health_deduplicates_pairs_and_labels_legacy_evidence_unknown():
    item = source_health(SYMBOL, {'1m': ready(), 'quote': quote()}, at=AT, cutoff=AT)
    record = {'symbols': {SYMBOL: {'source_health': item}, 'legacy': {'state': 'GREEN'}}}
    summary = summarize_health([record, record])
    assert summary['decision_symbol_pairs'] == 1
    assert summary['states'] == {'REQUIRED_INPUTS_READY': 1}
    assert summary['legacy_rows_without_health'] == 2


@pytest.mark.parametrize('owner,started,dead', [
    ('process:host:boot:123:100', '100', False),
    ('process:host:boot:123:100', '101', True),
    ('process:host:boot:123:100', FileNotFoundError(), True),
    ('process:host:boot:123:100', PermissionError(), False),
    ('process:other-host:boot:123:100', '101', False),
    ('process:host:old-boot:123:100', '100', True),
    ('liangjian-runtime', '101', False),
])
def test_process_owner_requires_provable_local_death(monkeypatch, owner, started, dead):
    monkeypatch.setattr(process_identity, '_host', lambda: 'host')
    monkeypatch.setattr(process_identity.Path, 'read_text', lambda self: 'boot')
    def read_start(pid):
        if isinstance(started, Exception):
            raise started
        return started
    monkeypatch.setattr(process_identity, '_started', read_start)
    assert process_identity.owner_is_dead(owner) is dead


def test_new_quote_source_is_shadow_by_default_and_not_called(monkeypatch):
    from liangjian_funnel.workflow import WorkflowApplication
    def forbidden():
        raise AssertionError('shadow source may not enter execution')
    monkeypatch.setattr('liangjian_funnel.data.live_quote.SinaQuoteAdapter', forbidden)
    app = SimpleNamespace(market_data=SimpleNamespace(fetch_quote=lambda *a, **k: quote()))
    assert WorkflowApplication._fetch_live_quote(app, SYMBOL, AT).complete


def test_dependency_result_after_absolute_deadline_never_enters_frozen_packet():
    tick = {'now': 0.0}
    def late():
        tick['now'] = 2.0
        return ready()
    value = BoundedWorkGate(1).call(late, deadline=1.0, clock=lambda: tick['now'])
    assert value.status == 'TIMED_OUT' and value.value is None
    tick['now'] = 0.0
    result = run_many_bounded({'late': late}, deadline=1.0, gate=BoundedWorkGate(1), clock=lambda: tick['now'])
    assert result['late'].status == 'TIMED_OUT' and result['late'].value is None
