"""Local-only pipeline counterexamples; no provider, model or runtime DB."""
from threading import Event
import time

import pytest

from liangjian_funnel.pipeline.disclosure_pipeline import DisclosurePipeline, DisclosurePipelineError
from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.settings import Settings
from test_wp5_daily_incremental import Client, NOW, history, seed


def test_scope_mode_defaults_to_shadow_and_invalid_value_fails(tmp_path):
    assert Settings.from_env({}, root=tmp_path).disclosure_scope_mode == 'SHADOW'
    assert Settings.from_env({'LIANGJIAN_DISCLOSURE_SCOPE_MODE': 'CANDIDATE_DOMAIN'}, root=tmp_path).disclosure_scope_mode == 'CANDIDATE_DOMAIN'
    with pytest.raises(ValueError):
        Settings.from_env({'LIANGJIAN_DISCLOSURE_SCOPE_MODE': 'best-effort'}, root=tmp_path)


def test_industry_order_groups_existing_members_without_dropping_unmapped():
    from liangjian_funnel.pipeline.disclosure_pipeline import industry_batch_order
    ordered = ('A', 'B', 'C', 'D', 'U')
    rows = [{'thscode': s, 'memberships': [{'industry_thscode': node}]} for s, node in
            [('A', '884001.TI'), ('B', '884002.TI'), ('C', '884001.TI'), ('D', '884002.TI')]]
    assert industry_batch_order(ordered, rows, ['884001.TI', '884002.TI']) == ('A', 'C', 'B', 'D', 'U')


def test_daily_batch_callback_receives_latest_closed_inputs_before_next_batch(tmp_path):
    cache = LocalFactCache(tmp_path/'cache.sqlite3')
    symbols = ['600000.SH', '600001.SH']
    for symbol in symbols:
        seed(cache, symbol, history()[:-1])
    client = Client()
    observed = []
    def callback(daily, failures):
        observed.append((tuple(daily), len(client.calls), failures))
        assert all(rows[-1]['date_ms'] == history()[-1]['date_ms'] for rows in daily.values())
    result = HithinkIncrementalSynchronizer(cache, batch_size=1).sync(
        client, symbols, as_of=NOW, include_financial=False, daily_batch_callback=callback)
    assert observed == [(('600000.SH',), 1, {}), (('600001.SH',), 2, {})]
    assert result.failures == {}


def test_disclosures_start_while_later_daily_work_is_still_running():
    started, release = Event(), Event()
    def fetch(symbol):
        started.set()
        assert release.wait(1)
        return symbol, 'recent', False, 'business', False
    with DisclosurePipeline(fetch, workers=1, deadline=time.monotonic()+2) as pipeline:
        with pipeline.stage('daily'):
            pipeline.submit('600000.SH', input_hash='a'*64)
            assert started.wait(1)
            # This models the still-in-progress next daily batch, not a sleep
            # timing assertion that could pass without actual concurrency.
            release.set()
            assert pipeline.results(['600000.SH'])[0][0] == '600000.SH'
        receipt = pipeline.receipt()
    assert receipt['overlap_seconds'] > 0
    assert receipt['submitted_symbols'] == ['600000.SH']


def test_same_symbol_is_not_queried_twice_and_input_revision_blocks():
    calls = []
    with DisclosurePipeline(lambda s: calls.append(s) or (s, 1, 2, 3, 4), workers=1,
                            deadline=time.monotonic()+2) as pipeline:
        pipeline.submit('600000.SH', input_hash='same')
        pipeline.submit('600000.SH', input_hash='same')
        assert pipeline.results(['600000.SH']) == [("600000.SH", 1, 2, 3, 4)]
        with pytest.raises(DisclosurePipelineError, match='INPUT_CHANGED'):
            pipeline.validate_domain(['600000.SH'], {'600000.SH': 'changed'})
    assert calls == ['600000.SH']


def test_final_discovery_catches_up_without_losing_or_capping_candidates():
    with DisclosurePipeline(lambda s: (s, 1, 2, 3, 4), workers=2,
                            deadline=time.monotonic()+2) as pipeline:
        pipeline.submit('600000.SH', input_hash='a')
        pipeline.validate_domain(['600000.SH', '600001.SH'], {'600000.SH': 'a', '600001.SH': 'b'})
        pipeline.submit('600001.SH', input_hash='b')
        assert [row[0] for row in pipeline.results(['600001.SH', '600000.SH'])] == ['600001.SH', '600000.SH']
        pipeline.validate_domain(['600001.SH'], {'600001.SH': 'b'})
        assert pipeline.receipt()['domain_anomalies'][0]['symbols'] == ['600000.SH']


def test_deadline_does_not_accept_late_results_or_wait_on_shutdown():
    started, release = Event(), Event()
    def fetch(symbol):
        started.set()
        release.wait(2)
        return symbol, 1, 2, 3, 4
    before = time.monotonic()
    with DisclosurePipeline(fetch, workers=1, deadline=before+.1) as pipeline:
        pipeline.submit('600000.SH', input_hash='a')
        assert started.wait(.5)
        with pytest.raises(DisclosurePipelineError, match='DEADLINE'):
            pipeline.results(['600000.SH'])
    assert time.monotonic()-before < .8
    release.set()


def test_daily_callback_is_optional_and_does_not_change_sync_projection(tmp_path):
    def sync(path, callback=None):
        cache = LocalFactCache(path)
        seed(cache, '600000.SH', history()[:-1])
        return HithinkIncrementalSynchronizer(cache).sync(Client(), ['600000.SH'],
            as_of=NOW, include_financial=False, daily_batch_callback=callback)
    plain = sync(tmp_path/'plain.sqlite3')
    observed = sync(tmp_path/'observed.sqlite3', lambda *_: None)
    assert plain == observed


def test_source_exception_is_not_an_empty_success_and_resources_close_after_last_worker():
    closed = Event()
    def fetch(_):
        raise ValueError('fixture source failure')
    with DisclosurePipeline(fetch, workers=1, deadline=time.monotonic()+2,
                            close_resources=closed.set) as pipeline:
        pipeline.submit('600000.SH', input_hash='a')
        with pytest.raises(ValueError, match='fixture source failure'):
            pipeline.results(['600000.SH'])
        assert pipeline.receipt()['completed_symbols'] == []
        assert not closed.is_set()
    assert closed.wait(.5)


def test_wrong_security_result_is_blocked():
    with DisclosurePipeline(lambda _: ('600001.SH', 1, 2, 3, 4), workers=1,
                            deadline=time.monotonic()+2) as pipeline:
        pipeline.submit('600000.SH', input_hash='a')
        with pytest.raises(DisclosurePipelineError, match='SYMBOL_MISMATCH'):
            pipeline.results(['600000.SH'])


def test_idle_deadline_closes_resources_and_cannot_accept_new_work():
    closed = Event()
    with DisclosurePipeline(lambda s: (s, 1, 2, 3, 4), workers=1,
                            deadline=time.monotonic()+.05, close_resources=closed.set) as pipeline:
        assert closed.wait(.5)
        with pytest.raises(DisclosurePipelineError, match='DEADLINE'):
            pipeline.submit('600000.SH', input_hash='a')


def test_candidate_scope_miss_blocks_a2_before_review_but_keeps_evidence(tmp_path):
    import json
    from types import SimpleNamespace
    from liangjian_funnel.pipeline.research import ResearchPipeline, ResearchPipelineError
    from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
    from liangjian_funnel.pipeline.disclosure_scope import build_disclosure_prefilter
    from liangjian_funnel.pipeline.feature_store import content_hash
    from test_wp5_disclosure_prefilter import DAY, bars
    scope = build_disclosure_prefilter(symbols=['A', 'B'], trade_date=DAY,
        daily={'A': bars(False), 'B': bars(False)}, selected_board={
            'available': True, 'trade_date': DAY.isoformat(), 'by_symbol': {}},
        event_symbols=[], event_sources_complete=True, hot_symbols=['A'])
    scope.update(mode='CANDIDATE_DOMAIN', changes_query_scope=True)
    scope['scope_hash'] = content_hash({k: v for k, v in scope.items() if k != 'scope_hash'})
    gate = DeterministicGateResult(stage='A2_LOCAL_ROLE', decisions=(),
        review_symbols=('A', 'B'), monitor_symbols=(), rejected_symbols=())
    with pytest.raises(ResearchPipelineError, match='CANDIDATE_SCOPE_NOT_COVERED'):
        ResearchPipeline._persist_gate(SimpleNamespace(output_dir=tmp_path, feature_store=None),
            'run', 'lane', gate, SimpleNamespace(data={'DISCLOSURE_PREFILTER_SHADOW': scope}, as_of=None))
    receipt = json.loads((tmp_path/'disclosure_scope_shadow/run/lane.json').read_text(encoding='utf-8'))
    assert receipt['status'] == 'SCOPE_MISS' and receipt['missing_symbols'] == ['B']
    assert receipt['mode'] == 'CANDIDATE_DOMAIN'
