"""Claude review counterexamples; local caches only, no production effects."""
from threading import Event

from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.pipeline.disclosure_scope import build_disclosure_prefilter
from liangjian_funnel.pipeline.disclosure_scope import audit_disclosure_scope
from liangjian_funnel.pipeline.disclosure_maintenance import run_maintenance
from test_wp5_daily_incremental import Client, NOW, history, seed
from test_wp5_disclosure_prefilter import DAY, bars
from test_wp5_disclosure_maintenance import queue, NOW as NIGHT


def test_incremental_revalidates_last_three_closed_bars(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite')
    seed(cache, '600000.SH', history()[:-1])
    client = Client()
    HithinkIncrementalSynchronizer(cache).sync(client, ['600000.SH'], as_of=NOW,
                                             include_financial=False)
    assert client.calls[0][1]['start'] == history()[-4]['date_ms']


def test_detected_revision_forces_complete_rebuild_preserving_old_versions(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite')
    seed(cache, '600000.SH', history()[:-1])
    revised = [{**r, 'close_price': r['close_price']/2} for r in history()]
    client = Client(revised)
    result = HithinkIncrementalSynchronizer(cache).sync(client, ['600000.SH'], as_of=NOW,
                                                       include_financial=False)
    assert len(client.calls) == 2
    assert result.daily_requests['600000.SH']['reason_code'] == 'HISTORICAL_REVISION_CONFIRMED'
    assert result.daily['600000.SH'][-2]['close_price'] == revised[-2]['close_price']
    assert cache.query_daily_bars('600000.SH', as_of=NOW.replace(day=8))[-1]['payload']['close_price'] == history()[-2]['close_price']
    proof = result.daily_requests['600000.SH']['revision_evidence']
    assert len(proof) == 3 and all(r['cached_hash'] != r['source_hash'] for r in proof)


def test_missing_overlap_cannot_be_repaired_by_only_latest_day(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite')
    seed(cache, '600000.SH', history()[:-1])
    client = Client(history()[-1:])
    result = HithinkIncrementalSynchronizer(cache).sync(client, ['600000.SH'], as_of=NOW,
                                                       include_financial=False)
    assert 'DAILY:INCREMENTAL_OVERLAP_INCOMPLETE' in result.failures['600000.SH']
    assert len(client.calls) == 1
    assert cache.get_sync_state('HITHINK_DAILY_1D', '600000.SH')['status'] == 'FAILED'


def test_revision_partial_rebuild_remains_blocked_on_next_attempt(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite')
    seed(cache, '600000.SH', history()[:-1])
    revised = [{**r, 'close_price': r['close_price']/2} for r in history()]
    class Partial(Client):
        def history_1d(self, symbol, **kwargs):
            if self.calls:
                self.rows = revised[-1:]
            return super().history_1d(symbol, **kwargs)
    first = HithinkIncrementalSynchronizer(cache).sync(Partial(revised), ['600000.SH'],
        as_of=NOW, include_financial=False)
    assert 'DAILY:FULL_REFRESH_HISTORY_INCOMPLETE' in first.failures['600000.SH']
    assert cache.get_sync_state('HITHINK_DAILY_1D', '600000.SH')['cursor']['pending_reset_reason'] == 'HISTORICAL_REVISION_CONFIRMED'
    second = HithinkIncrementalSynchronizer(cache).sync(Client(revised[-1:]), ['600000.SH'],
        as_of=NOW, include_financial=False)
    assert 'DAILY:FULL_REFRESH_HISTORY_INCOMPLETE' in second.failures['600000.SH']
    assert cache.query_daily_bars('600000.SH')[-1]['payload'] == history()[-2]


def test_explicit_unavailable_board_does_not_retain_entire_scope():
    symbols = ['A', 'B', 'C']
    result = build_disclosure_prefilter(symbols=symbols, trade_date=DAY,
        daily={s: bars() for s in symbols}, selected_board={'available': False, 'by_symbol': {}},
        event_symbols=[], event_sources_complete=False, hot_symbols=['A'])
    assert result['candidate_symbols'] == ['A']
    assert result['deferred_symbols'] == ['B', 'C']


def test_incomplete_events_do_not_retain_observed_negative_on_valid_board():
    result = build_disclosure_prefilter(symbols=['A', 'B'], trade_date=DAY,
        daily={'A': bars(False), 'B': bars(False)},
        selected_board={'available': True, 'trade_date': DAY.isoformat(), 'by_symbol': {}},
        event_symbols=[], event_sources_complete=False, hot_symbols=['A'])
    assert result['candidate_symbols'] == ['A']


def test_board_contract_conflict_is_not_covered_even_if_hot_anchor_retained():
    result = build_disclosure_prefilter(symbols=['A'], trade_date=DAY,
        daily={'A': bars()}, selected_board={'available': False},
        event_symbols=[], event_sources_complete=True, hot_symbols=['A'])
    audit = audit_disclosure_scope(result, ['A'], decisions=[{
        'symbol': 'A', 'local_eligible_for_review': True, 'trend_core_eligible': True}])
    assert audit['status'] == 'SCOPE_MISS'
    assert audit['channel_contract_conflict_symbols'] == ['A']


def test_compact_output_is_not_expanded_by_thirty_bar_readiness_window(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite')
    seed(cache, '600000.SH', history()[:-1])
    result = HithinkIncrementalSynchronizer(cache).sync(Client(), ['600000.SH'],
        as_of=NOW, include_financial=False, compact_daily_bars=5)
    assert result.daily['600000.SH'] == history()[-5:]


def test_hung_night_collector_returns_at_deadline_and_never_accepts_late_success(tmp_path):
    release, done = Event(), Event()
    def collect(*args):
        try:
            release.wait(2)
            return {'symbol': args[0], 'ok': True, 'recent_complete': True,
                    'business_complete': True, 'pdf_complete': True}
        finally:
            done.set()
    try:
        result = run_maintenance(queue(tmp_path), output_dir=tmp_path/'reports', now=NIGHT,
            execute=True, collect=collect, can_reuse=lambda *a: False, budget_seconds=0.05)
        # Real I/O tail latency belongs to VM OPERATIONS evidence, not an
        # arbitrary Windows scheduling margin. The absolute-deadline behavior
        # is exercised deterministically below; hanging workers keep the slot.
        assert result['status'] == 'PARTIAL_FAILURE'
        assert result['rows'][0]['reason_code'] == 'DEADLINE_EXCEEDED'
        again = run_maintenance(queue(tmp_path), output_dir=tmp_path/'reports', now=NIGHT,
            execute=True, collect=collect, can_reuse=lambda *a: False, budget_seconds=0.05)
        assert again['rows'][0]['reason_code'] == 'BACKPRESSURE'
    finally:
        release.set()
        assert done.wait(1)


def test_night_absolute_deadline_rejects_late_success_and_starts_no_more_work(tmp_path):
    tick = {"now": 100.0}
    calls = []

    def collect(symbol, *args):
        calls.append(symbol)
        tick["now"] = 101.0  # dependency completes after the 100.05 cutoff
        return {"symbol": symbol, "ok": True,
                "business_complete": True, "pdf_complete": True}

    result = run_maintenance(queue(tmp_path, size=4), output_dir=tmp_path/'reports', now=NIGHT,
        execute=True, collect=collect, can_reuse=lambda *a: False,
        budget_seconds=0.05, clock=lambda: tick["now"])
    assert calls == ["000000.SZ"]
    assert result["status"] == "PARTIAL_FAILURE"
    assert [row["reason_code"] for row in result["rows"]] == [
        "DEADLINE_EXCEEDED", "MAINTENANCE_BUDGET_EXHAUSTED"]
    assert all(row["ok"] is False for row in result["rows"])
    assert result["resumed_symbols"] == []
    assert result["elapsed_seconds"] == 1.0
