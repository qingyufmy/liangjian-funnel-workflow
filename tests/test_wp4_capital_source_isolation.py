import copy
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.a2_market import (
    _content_hash, build_capital_flow_snapshot, unavailable_capital_flow_snapshot,
    TENCENT_CAPITAL_FLOW_PROVIDER,
)
from liangjian_funnel.pipeline.deterministic import _capital_flow_available
from liangjian_funnel.settings import Settings
from liangjian_funnel.pipeline.snapshot import FrozenInputSnapshot, UniverseSnapshot
from liangjian_funnel.workflow import WorkflowApplication

NOW = datetime(2026, 10, 9, 15, 0, tzinfo=ZoneInfo('Asia/Shanghai'))
SYMBOLS = ['600519.SH', '000001.SZ']


def raw_today():
    return build_capital_flow_snapshot(
        frames={'today': [{'代码': '600519', '主力净流入-净额': 120, '主力净流入-净占比': 12},
                          {'代码': '000001', '主力净流入-净额': -50, '主力净流入-净占比': -5}]},
        as_of=NOW, ingested_at=NOW, expected_symbols=SYMBOLS,
        source_id=TENCENT_CAPITAL_FLOW_PROVIDER, source_ref_prefix='tencent',
    )


def research(tmp_path, monkeypatch, mode, *, healthy):
    (tmp_path/'config').mkdir()
    (tmp_path/'config'/'exchange_rules.yaml').write_text(
        "schema_version: liangjian-exchange-rules/1.0.0\n"
        "snapshot_id: CN-A-SIMULATION-20260706\neffective_from: '2026-07-06'\n"
        "simulation_only: true\nexternal_orders: false\nt_plus_one: true\nlot_size: 100\n"
        "sources: {sse: {}, szse: {}, bse: {}}\n", encoding='utf-8',
    )
    settings = Settings.from_env({
        'LIANGJIAN_A2_CAPITAL_FLOW_ENABLED': 'true',
        'LIANGJIAN_ROTATION_MEMBERSHIP_SOURCE': mode,
    }, root=tmp_path)
    calls = {'tencent': 0, 'eastmoney': 0, 'board': 0}
    raw = raw_today() if healthy else unavailable_capital_flow_snapshot(
        as_of=NOW, expected_symbols=SYMBOLS, reason_code='SOURCE_UNAVAILABLE',
        source_id=TENCENT_CAPITAL_FLOW_PROVIDER,
    )
    def tencent(**kwargs):
        calls['tencent'] += 1
        return copy.deepcopy(raw)
    def eastmoney(**kwargs):
        calls['eastmoney'] += 1
        result = raw_today()
        result['source_id'] = 'EASTMONEY_STOCK_CAPITAL_FLOW'
        result['content_hash'] = _content_hash(result)
        return result
    def board(**kwargs):
        calls['board'] += 1
        return {'available': True, 'records': [{'board_name': 'fixture'}]}
    monkeypatch.setattr('liangjian_funnel.workflow.collect_tencent_capital_flow', tencent)
    monkeypatch.setattr('liangjian_funnel.workflow.collect_eastmoney_capital_flow', eastmoney)
    monkeypatch.setattr('liangjian_funnel.workflow.collect_eastmoney_board_flow', board)
    universe = UniverseSnapshot.from_records(
        [{'thscode': symbol, 'name': symbol} for symbol in SYMBOLS],
        [{'thscode': symbol, 'last_price': 8, 'prev_price': 9, 'volume': 10, 'turnover': 100}
         for symbol in SYMBOLS], as_of=NOW,
    )
    facts = {key: {'available': True, 'reason_code': 'OK', 'event_time': NOW.isoformat(),
                  'fetch_time': NOW.isoformat(), 'record_count': 1, 'records': [{'value': 0}]}
             for key in ('LIMIT_UP_POOL', 'LIMIT_DOWN_POOL', 'LIMIT_BREAK_POOL', 'LIMIT_UP_LADDER')}
    frozen = FrozenInputSnapshot.freeze(universe, as_of=NOW, max_candidates=2,
                                        fact_payload={'facts': facts})
    result = WorkflowApplication._research_input(
        SimpleNamespace(settings=settings), frozen=frozen, universe=universe,
        technical={}, g0_symbols=SYMBOLS, source_failures={},
        raw_snapshot_path=tmp_path/'not-read.json', as_of=NOW,
    )
    return result, calls, raw


@pytest.mark.parametrize('healthy', [True, False])
def test_local_reference_never_calls_eastmoney_critical_chain(tmp_path, monkeypatch, healthy):
    result, calls, raw = research(tmp_path, monkeypatch, 'LOCAL_REFERENCE', healthy=healthy)
    assert calls == {'tencent': 1, 'eastmoney': 0, 'board': 0}
    assert result['CAPITAL_FLOW_SNAPSHOT']['available'] is False
    assert result['BOARD_CAPITAL_FLOW_SNAPSHOT']['available'] is False
    assert not _capital_flow_available(result)
    assert all(row['capital_flow_score'] is None
               for row in result['CAPITAL_FLOW_SNAPSHOT']['by_symbol'].values())
    evidence = result['CAPITAL_FLOW_RAW_TODAY_EVIDENCE']
    assert evidence['evidence_only'] is True
    assert evidence['available'] is healthy
    assert evidence['raw_snapshot_sha256'] == raw['content_hash']
    assert evidence['cutoff_contract_status'] == 'FORMAL_COLLECTOR_NOT_YET_WIRED'
    assert result['BOARD_CAPITAL_FLOW_SNAPSHOT']['shadow_collection_status'] == 'NOT_COLLECTED'


@pytest.mark.parametrize('healthy', [True, False])
def test_eastmoney_mode_preserves_original_provider_routing(tmp_path, monkeypatch, healthy):
    result, calls, raw = research(tmp_path, monkeypatch, 'EASTMONEY', healthy=healthy)
    assert calls == {'tencent': 1, 'eastmoney': int(not healthy), 'board': 6}
    assert result['CAPITAL_FLOW_SNAPSHOT']['available'] is True
    assert result['BOARD_CAPITAL_FLOW_SNAPSHOT']['available'] is True
    assert 'CAPITAL_FLOW_RAW_TODAY_EVIDENCE' not in result
    assert raw['content_hash'] == _content_hash(raw)


def test_local_projection_preserves_raw_today_without_normalizing_missing_windows():
    from liangjian_funnel.data.capital_source_policy import project_local_capital_evidence
    raw = raw_today()
    original = copy.deepcopy(raw)
    execution, evidence = project_local_capital_evidence(raw, as_of=NOW, expected_symbols=SYMBOLS)
    assert raw == original
    assert execution['available'] is False
    for symbol in SYMBOLS:
        assert evidence['by_symbol'][symbol]['today'] == raw['by_symbol'][symbol]['metrics']['today']
        assert execution['by_symbol'][symbol]['capital_flow_score'] is None
    assert execution['content_hash'] == _content_hash(execution)
    assert evidence['content_hash'] == _content_hash(evidence)


@pytest.mark.parametrize('problem', ['source', 'hash', 'scope', 'date'])
def test_local_evidence_cannot_authenticate_wrong_cached_source(problem):
    from liangjian_funnel.data.capital_source_policy import project_local_capital_evidence
    raw = raw_today()
    if problem == 'source':
        raw['source_id'] = 'EASTMONEY_STOCK_CAPITAL_FLOW'
    elif problem == 'scope':
        raw['by_symbol'].pop(SYMBOLS[0])
    elif problem == 'date':
        raw['trade_date'] = '2026-10-08'
    raw['content_hash'] = _content_hash(raw) if problem != 'hash' else '0'*64
    execution, evidence = project_local_capital_evidence(raw, as_of=NOW, expected_symbols=SYMBOLS)
    assert not evidence['available']
    assert evidence['reason_code'].startswith('LOCAL_RAW_')
    assert not execution['available']
    assert all(row['capital_flow_score'] is None for row in execution['by_symbol'].values())


def test_shadow_labels_original_renormalization_without_rewriting_factor():
    from liangjian_funnel.data.capital_source_policy import inspect_legacy_capital_weighting
    raw = raw_today()
    original = copy.deepcopy(raw)
    audit = inspect_legacy_capital_weighting(raw)
    assert audit['input_content_hash'] == raw['content_hash']
    assert audit['scope'] == 'SYMBOL_FACTOR_NOT_BOARD_FACTOR'
    for symbol in SYMBOLS:
        row = audit['by_symbol'][symbol]
        assert row['normalization_state'] == 'DEGRADED_RENORMALIZED'
        assert row['observed_weight'] == pytest.approx(.35)
        assert row['missing_windows'] == ['3d', '5d', '10d']
        assert row['original_score'] == raw['by_symbol'][symbol]['capital_flow_score']
    assert raw == original


def test_weighting_audit_does_not_guess_missing_formula_evidence():
    from liangjian_funnel.data.capital_source_policy import inspect_legacy_capital_weighting
    raw = raw_today()
    raw['by_symbol'][SYMBOLS[0]].pop('available_weight')
    raw['content_hash'] = _content_hash(raw)
    audit = inspect_legacy_capital_weighting(raw)
    assert audit['by_symbol'][SYMBOLS[0]]['normalization_state'] == 'DATA_LIMITED'
    assert audit['by_symbol'][SYMBOLS[0]]['observed_weight'] is None
