"""Exercise the actual prepare_snapshot route with local fake providers.

No app private query methods, synchronizer or pipeline are mocked. Only source
transport and unrelated post-collection manifest storage are replaced. These
are code-path tests, not real source/replay acceptance.
"""
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest

from liangjian_funnel import workflow as wf
from liangjian_funnel.data.cninfo import CninfoFetchResult
from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer
from liangjian_funnel.pipeline.feature_store import content_hash
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.settings import Settings
from test_wp5_daily_incremental import NOW, history, seed


class Captured(Exception):
    def __init__(self, payload):
        self.payload = payload


def route(tmp_path, monkeypatch, mode='SHADOW', *, direct=False, event_ok=True, overlap=False):
    symbols = ['600000.SH', '600001.SH', '600002.SH']
    queried = []
    query_started = Event()
    daily_queries = []
    def result(items=(), **meta):
        return HithinkFetchResult(endpoint='fixture', ok=True, complete=True, reason_code='OK',
            items=tuple(HithinkRow.model_validate(r) for r in items), fetch_time=NOW, metadata=meta)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr(wf, 'datetime', Clock)
    from liangjian_funnel.pipeline import close_scope
    monkeypatch.setattr(close_scope, 'datetime', Clock)
    from liangjian_funnel.data import disclosure_router
    monkeypatch.setattr(disclosure_router, 'datetime', Clock)
    settings = Settings.from_env({'LIANGJIAN_DISCLOSURE_SCOPE_MODE': mode,
                                 'LIANGJIAN_OPEN_MACRO_ENABLED': 'false'}, root=tmp_path).model_copy(update={
        'open_macro_enabled': False, 'research_checkpoint_dir': tmp_path.parent/(tmp_path.name[-2:]+'c')})
    records = tuple(SimpleNamespace(symbol=s, amount=1e9) for s in symbols)
    universe = SimpleNamespace(ready=True, records=records, research_candidates=records,
        lineage=SimpleNamespace(research_candidate_count=3))
    monkeypatch.setattr(wf.UniverseSnapshot, 'from_records', lambda *_a, **_k: universe)
    board = {'available': False, 'trade_date': NOW.date().isoformat(), 'by_symbol': {}}
    monkeypatch.setattr(wf, 'collect_eastmoney_hot100', lambda **_: {
        'available': True, 'trade_date': NOW.date().isoformat(), 'records': [{'symbol': symbols[0]}]})
    monkeypatch.setattr(wf, 'collect_rotation_theme_snapshot', lambda **_: deepcopy(board))
    from liangjian_funnel.data import hithink_board_reference
    monkeypatch.setattr(hithink_board_reference, 'configured_rotation_memberships', lambda *_a, **_k: {})
    monkeypatch.setattr(hithink_board_reference, 'rotation_snapshot_directory', lambda *_: tmp_path/'rotation')
    class Source:
        def __init__(self, *_a, **_k):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def ticker_catalog(self, **_):
            return result()
        market_snapshot = ticker_catalog
        def ths_index_catalog(self, **_):
            return result()
        def history_1d(self, symbol, **kwargs):
            daily_queries.append(symbol)
            if overlap and symbol == symbols[1]:
                assert query_started.wait(1), 'disclosure did not start during full-market daily sync'
            rows = [r for r in history() if kwargs['start'] <= r['date_ms'] < kwargs['end']]
            return result(rows)
        def financial_income(self, symbol, **_):
            return result([{'report_date_ms': history()[-1]['date_ms']}])
        financial_indicators = financial_income
        financial_balance = financial_income
        financial_cash_flow = financial_income
        income_statements = financial_income
        balance_sheets = financial_income
        cash_flow_statements = financial_income
    monkeypatch.setattr(wf, 'HithinkClient', Source)
    membership = result([{'thscode': s, 'memberships': [
        {'industry_thscode': '884001.TI', 'industry_name': 'fixture'}]} for s in symbols])
    monkeypatch.setattr(wf, 'collect_ths_industry_membership', lambda *_a, **_k: membership)
    monkeypatch.setattr(wf, 'collect_ths_taxonomy_membership', lambda *_a, **_k: result())
    monkeypatch.setattr(wf, 'collect_ths_industry_history', lambda *_a, **_k: result())
    monkeypatch.setattr(wf, 'collect_market_results', lambda *_a, **_k: {
        name: result(market_trade_date=NOW.date().isoformat()).model_copy(
            update={'ok': event_ok, 'complete': event_ok}) for name in
        ('LIMIT_UP_POOL', 'LIMIT_DOWN_POOL', 'LIMIT_BREAK_POOL', 'LIMIT_UP_LADDER')})
    monkeypatch.setattr(wf, 'recover_required_market_results', lambda _c, values, **_: (values, []))
    monkeypatch.setattr(wf, 'load_yaml', lambda *_: {})
    # Preserve global discovery ranking; the synthetic fixture explicitly
    # names the outsider. Do not manufacture a per-batch review quota.
    from liangjian_funnel.pipeline import data_sync
    monkeypatch.setattr(data_sync, 'discover_early_setups', lambda daily, **_: {
        'records': [{'symbol': symbols[2], 'signals': []}] if symbols[2] in daily else [],
        'universe_count': 1, 'scanned_count': 1, 'data_gaps': []})
    class Official(Source):
        def fetch_announcements(self, symbol, start, end, search_keyword=''):
            queried.append(symbol)
            query_started.set()
            return CninfoFetchResult(symbol=symbol, start_date=start, end_date=end,
                ok=True, complete=True, reason_code='OK', announcements=(), fetched_at=NOW,
                metadata={'search_keyword': search_keyword})
    for name in ('CninfoClient', 'BseClient', 'SseDisclosureClient', 'SzseDisclosureClient'):
        monkeypatch.setattr(wf, name, Official)
    # The real router and query/cache methods run; all its transports are local.
    class Policy(Source):
        def fetch_documents(self, *_):
            return SimpleNamespace(ok=True, complete=True)
    monkeypatch.setattr(wf, 'GovPolicyClient', Policy)
    monkeypatch.setattr(wf, 'normalize_hithink_results', lambda *_a, **_k: SimpleNamespace(as_of=NOW))
    monkeypatch.setattr(wf, 'normalize_gov_policy_result', lambda *_a, **_k: SimpleNamespace(as_of=NOW))
    monkeypatch.setattr(wf, 'normalize_cninfo_results', lambda values, **_: SimpleNamespace(
        as_of=NOW, values={s: v.model_dump(mode='json') for s, v in values.items()}))
    monkeypatch.setattr(wf, 'normalize_open_news_results', lambda *_a, **_k: SimpleNamespace(as_of=NOW))
    monkeypatch.setattr(wf, 'merge_fact_manifests', lambda values: SimpleNamespace(as_of=NOW, values=values[2].values))
    monkeypatch.setattr(wf, 'manifest_projection', lambda value: {'fixture_disclosures': value.values})
    monkeypatch.setattr(wf, 'evaluate_data_readiness', lambda *_a, **_k: SimpleNamespace(
        ready=True, as_dict=lambda: {'ready': True}))
    class Store:
        def __init__(self, root):
            self.root = root
        def write_manifest(self, *_):
            return self.root/'fixture-manifest.json'
    monkeypatch.setattr(wf, 'FactStore', Store)
    monkeypatch.setattr(wf.FrozenInputSnapshot, 'freeze', lambda *_a, **kwargs: (_ for _ in ()).throw(Captured(kwargs)))
    # Use a concrete test subclass rather than an uninitialized production app.
    class App(wf.WorkflowApplication):
        def __init__(self):
            self.settings = settings
            self.trading_calendar = ExchangeTradingCalendar()
            self.fact_cache = LocalFactCache(tmp_path/'fixture-cache.sqlite3')
            for symbol in symbols:
                seed(self.fact_cache, symbol, history()[:-1])
            self.fact_synchronizer = HithinkIncrementalSynchronizer(self.fact_cache, batch_size=1)
            self.lark_publisher = SimpleNamespace(publish_rotation_theme_health=lambda *_a, **_k: None)
        def _collect_open_news(self, *_):
            return {}
        def _supplement_cninfo_business_evidence(self, *_a, **_k):
            # No documents in this fixture: business admission is NOT claimed.
            pass
    app = App()
    method = app._prepare_snapshot if direct else app.prepare_snapshot
    with pytest.raises(Captured) as captured:
        method(as_of=NOW, candidate_symbols=tuple(symbols[:2]), materialize_feature_source=False)
    return captured.value.payload, queried, daily_queries


def test_shadow_wrapper_and_original_path_freeze_identical_bytes(tmp_path, monkeypatch):
    # Reuse the same root/path to compare actual projected fact hashes, with a
    # fresh identical fixture cache per run. Receipt observation time is fixed.
    first, first_queries, _ = route(tmp_path, monkeypatch, direct=True)
    import sqlite3
    with sqlite3.connect(tmp_path/'fixture-cache.sqlite3') as db:
        for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            db.execute(f'DELETE FROM "{name}"')
    second, second_queries, _ = route(tmp_path, monkeypatch)
    def differences(a, b, path=''):
        if isinstance(a, dict) and isinstance(b, dict):
            return [item for key in sorted(a.keys() | b.keys())
                    for item in differences(a.get(key), b.get(key), f'{path}/{key}')]
        return [] if a == b else [(path, a, b)]
    assert not differences(first, second), differences(first, second)
    assert content_hash(first) == content_hash(second)
    assert sorted(first_queries) == sorted(second_queries)
    assert not list((tmp_path/'state').rglob('disclosure-pipeline-*.json'))


def test_candidate_pipeline_runs_before_global_daily_finishes_and_catches_discovery(tmp_path, monkeypatch):
    payload, queried, daily = route(tmp_path, monkeypatch, 'CANDIDATE_DOMAIN', overlap=True)
    assert set(queried) == {'600000.SH', '600002.SH'}
    assert len(daily) == 3
    assert set(payload['fact_payload']['fixture_disclosures']) == {'600000.SH', '600002.SH'}
    prefilter = payload['fact_payload']['disclosure_prefilter_shadow']
    assert prefilter['deferred_symbols'] == ['600001.SH']
    assert prefilter['mode'] == 'CANDIDATE_DOMAIN'


def test_global_event_failure_prevents_all_company_queries(tmp_path, monkeypatch):
    with pytest.raises(wf.WorkflowError, match='MARKET_EMOTION_FACTS_NOT_READY'):
        route(tmp_path, monkeypatch, 'CANDIDATE_DOMAIN', event_ok=False)


@pytest.mark.parametrize('already_failed', [True, False])
def test_pipeline_receipt_io_failure_preserves_original_failure_or_blocks_success(tmp_path, monkeypatch, already_failed):
    class Client:
        def __init__(self, **_):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
    for name in ('CninfoClient', 'BseClient', 'SseDisclosureClient', 'SzseDisclosureClient'):
        monkeypatch.setattr(wf, name, Client)
    monkeypatch.setattr(wf, 'atomic_write_json', lambda *_: (_ for _ in ()).throw(OSError('fixture full disk')))
    class App(wf.WorkflowApplication):
        def __init__(self):
            self.settings = Settings.from_env({'LIANGJIAN_DISCLOSURE_SCOPE_MODE': 'CANDIDATE_DOMAIN'}, root=tmp_path)
        def _prepare_snapshot(self, *, start_disclosure_pipeline, **_):
            start_disclosure_pipeline(['600000.SH'])
            if already_failed:
                raise wf.WorkflowError('ORIGINAL_TEST_FAILURE')
            return object()
    if already_failed:
        with pytest.raises(wf.WorkflowError, match='ORIGINAL_TEST_FAILURE'):
            App().prepare_snapshot(as_of=NOW, candidate_symbols=('600000.SH',))
    else:
        with pytest.raises(OSError, match='fixture full disk'):
            App().prepare_snapshot(as_of=NOW, candidate_symbols=('600000.SH',))
