"""Actual ResearchPipeline dispatch to Team blind review, no live resources."""
from datetime import datetime
from copy import deepcopy
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from liangjian_funnel.pipeline.research import FrozenInputSnapshot, ResearchPipeline, StageAudit
from liangjian_funnel.settings import Settings


NOW = datetime.fromisoformat('2026-10-16T16:30:00+08:00')


def pipeline(tmp_path):
    return ResearchPipeline(Settings.from_env({}, root=tmp_path), now=lambda: NOW,
        enable_feature_store=False, prompt_repository=Path(__file__).resolve().parents[1] / 'prompts')


def stage_args():
    return dict(lane_id='lane_1', model='deepseek-v4-pro-0813', stage='A3',
        snapshot=FrozenInputSnapshot('original', {}, as_of=NOW), upstream_output={},
        upstream_symbols=set(), bundle=None, run_id='original-close')


def test_actual_stage_selects_explicit_team_adapter_not_legacy_prompt_path(tmp_path):
    app = pipeline(tmp_path); seen = []
    expected = StageAudit('lane_1', 'model', 'A3', 'VALIDATED', 'original', 'new-role-hash',
        'new-input-hash', None, 0, 0, None, (), ())
    def invoke(owner, **kwargs):
        seen.append((owner, kwargs)); return expected
    app.team_research_adapter = SimpleNamespace(supports=lambda stage: stage == 'A3', run_stage=invoke)
    assert app._run_stage(**stage_args()) == expected and len(seen) == 1


def test_no_adapter_preserves_existing_legacy_blocked_prompt_result(tmp_path):
    assert pipeline(tmp_path)._run_stage(**stage_args()).reason_codes == ('PROMPT_REPOSITORY_BLOCKED',)


def actual_fixture(tmp_path, *, veto=False, invalid=False, eligibility='QUALIFIED', model_risk_unit='PROBE',
                   quarantine=False):
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.pipeline.team_v2_research import TeamResearchAdapter
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    from test_team_v2_plan_review import response
    root = Path(__file__).resolve().parents[1]
    seen = []
    def handler(request):
        body = json.loads(request.read()); seen.append(body)
        runtime = json.loads(body['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        output = response(); output['envelope'] = runtime['required_envelope']
        candidates, _ = json.JSONDecoder().raw_decode(body['messages'][0]['content'].split(
            'A3_CANDIDATE_POOL=', 1)[1].lstrip())
        template = output['core_watch_pool'][0]
        output['core_watch_pool'] = [dict(deepcopy(template), symbol=row['symbol'], cluster=row['cluster'],
            strategy_profile=row['strategy_profile'], plan_priority_rank=index,
            risk_unit=model_risk_unit,
            supporting_refs=['A3:TECH:' + row['symbol']], opposing_refs=['A3:MARKET'])
            for index, row in enumerate(candidates, 1)]
        if veto:
            row = output['core_watch_pool'].pop()
            for field in ('plan_priority_rank', 'trade_brief', 'what_would_change_my_mind'):
                row.pop(field)
            row.update(review_status='VETO', risk_unit='NO_ENTRY', reason_codes=['DISCLOSURE_CONFLICT'])
            output['secondary_watch_pool'].append(row)
        if invalid:
            output['core_watch_pool'][0]['no_chase_price'] = 999
        if quarantine:
            output['core_watch_pool'][0]['trade_brief'] = {}
        return httpx.Response(200, json={'choices': [{'message': {'content': json.dumps(output)}}]}, request=request)
    settings = Settings.from_env({'LIANGJIAN_MODEL_API_KEY': 'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'}, root=tmp_path)
    journal = ModelJudgmentJournal(tmp_path / 'models.sqlite3'); store = TeamMeetingStore(tmp_path / 'team.sqlite3')
    calendar = SimpleNamespace(is_trading_day=lambda day: day.isoformat() == '2026-10-16',
        next_trading_day=lambda day: datetime.fromisoformat('2026-10-19').date())
    adapter = TeamResearchAdapter(repository=TeamPromptRepository(root / 'prompts/team_v2'),
        journal=journal, store=store, calendar=calendar, config_version='frozen-config', config_hash='a' * 64)
    client = OpenAICompatibleModelClient(settings, transport=httpx.MockTransport(handler), max_attempts=1)
    app = ResearchPipeline(settings, model_client=client, now=lambda: NOW, enable_feature_store=False,
        prompt_repository=root / 'prompts', team_research_adapter=adapter)
    context = dict(symbol='000001.SZ', strategy_profile='TREND_MA5', eligibility=eligibility,
        route_permission='ALLOW_A4', stock_behavior_type='TREND', strategy_version='a3-a4-three-strategy/1',
        execution_permission='ALLOW', candidate_origin='WATCH_ONLY', daily_ma={'ma5': 10},
        entry_reference_zone={'low': 10, 'high': 11}, daily_invalidation=9, no_chase_price=12,
        a4_required_entry_rules=['original confirmation'], reason_codes=['ORIGINAL_ENGINE_VERDICT'],
        met_conditions=['ALL_MET'], plan_expiry='2026-10-19T15:00:00+08:00')
    data = dict(A3_DETERMINISTIC_CONTEXT={'000001.SZ': context}, A3_CANDIDATE_ORIGIN={'000001.SZ': 'WATCH_ONLY'},
        MARKET_REGIME_SNAPSHOT={'regime': 'ROTATION_NO_MAINLINE'}, DETERMINISTIC_RESEARCH_V2_ENABLED=True,
        PRICE_LEVELS={'000001.SZ': dict(available=True, trigger_zone={'low': 10, 'high': 11},
            invalidation=9, first_resistance=15, stop_distance_pct=.05, reward_risk=3)})
    args = stage_args(); args.update(bundle=app.prompts.load(),
        snapshot=FrozenInputSnapshot('original-a3', data, as_of=NOW), upstream_symbols={'000001.SZ'},
        projection_symbols={'000001.SZ'},
        upstream_output={'focus_pool': [], 'watch_only_pool': [{'symbol': '000001.SZ', 'theme_id': 'T',
            'candidate_origin': 'WATCH_ONLY', 'market_role': 'TREND_CORE'}],
            'model_nominations': [{'symbol': '000003.SZ'}]})
    return app, args, seen


def test_actual_research_stage_team_http_and_original_price_provenance(tmp_path):
    app, args, seen = actual_fixture(tmp_path)
    before = deepcopy(dict(args['snapshot'].data))
    audit = app._run_stage(**args)
    assert audit.status == 'VALIDATED', audit.reason_codes
    row = audit.output['core_watch_pool'][0]
    assert row['risk_unit'] == 'PROBE' and row['trigger_zone'] == {'low': 10, 'high': 11}
    assert row['no_chase_price'] == 12 and row['strategy_profile'] == 'TREND_MA5'
    assert row['trade_brief']['confirm_cadence'] == '5m' and row['plan_priority_rank'] == 1
    text = json.dumps(seen[0]['messages'])
    assert 'ORIGINAL_ENGINE_VERDICT' not in text and 'ALL_MET' not in text and '000003.SZ' not in text
    assert 'SERVER-AUTHORIZED ROTATION POLICY' not in text and 'PM_PREMARKET（' not in text
    assert dict(args['snapshot'].data) == before
    assert app._run_stage(**args).output == audit.output and len(seen) == 1


@pytest.mark.parametrize('veto,invalid,eligibility', [
    (True, False, 'QUALIFIED'), (False, True, 'QUALIFIED'), (False, False, 'DATA_GAP')])
def test_model_or_engine_rejection_never_becomes_executable_core(tmp_path, veto, invalid, eligibility):
    app, args, seen = actual_fixture(tmp_path, veto=veto, invalid=invalid, eligibility=eligibility)
    result = app._run_stage(**args)
    assert len(seen) == (3 if invalid else 1)
    assert result.output is None or result.output['core_watch_pool'] == []
    if result.output is not None:
        assert all(row['risk_unit'] == 'NO_ENTRY' for row in result.output['secondary_watch_pool'])


def test_team_stage_never_loads_legacy_prompt_checkpoint(tmp_path):
    app, args, seen = actual_fixture(tmp_path)
    app.checkpoint_store = {'old-key': {'status': 'VALIDATED', 'output': {'unsafe': 'legacy'}}}
    key, prepared = app._checkpoint_key(**{k: v for k, v in args.items() if k not in {'bundle'}},
        bundle=args['bundle'], a1_discovery_context=None)
    assert key is None and prepared is None and seen == []


def test_batch_priority_merge_is_only_display_and_preserves_original_ranks(tmp_path):
    app, _, _ = actual_fixture(tmp_path)
    output = {'core_watch_pool': [{'symbol': '000002.SZ', 'plan_priority_rank': 1, 'risk_unit': 'PROBE'},
        {'symbol': '000001.SZ', 'plan_priority_rank': 1, 'risk_unit': 'PROBE'}]}
    original = deepcopy(output)
    merged = app.team_research_adapter.finalize_a3_output(output)
    assert [row['plan_priority_rank'] for row in merged['core_watch_pool']] == [2, 1]
    assert all(row['model_priority_rank'] == 1 and row['risk_unit'] == 'PROBE' for row in merged['core_watch_pool'])
    assert output == original


def test_actual_batch_runner_merges_all_blind_votes_without_rank_collision(tmp_path, monkeypatch):
    from liangjian_funnel.pipeline.research import a3, common
    app, args, seen = actual_fixture(tmp_path)
    monkeypatch.setattr(a3, '_A3_BATCH_SIZE', 1)
    monkeypatch.setattr(common, '_A3_BATCH_SIZE', 1)
    original = args['snapshot']; data = deepcopy(dict(original.data)); upstream = args['upstream_output']
    for symbol in ('000002.SZ', '000004.SZ'):
        data['A3_DETERMINISTIC_CONTEXT'][symbol] = dict(deepcopy(data['A3_DETERMINISTIC_CONTEXT']['000001.SZ']), symbol=symbol)
        data['A3_CANDIDATE_ORIGIN'][symbol] = 'WATCH_ONLY'
        data['PRICE_LEVELS'][symbol] = deepcopy(data['PRICE_LEVELS']['000001.SZ'])
        upstream['watch_only_pool'].append(dict(deepcopy(upstream['watch_only_pool'][0]), symbol=symbol))
    merged = app._run_a3_batched(lane_id=args['lane_id'], model=args['model'],
        snapshot=FrozenInputSnapshot('three-originals', data, as_of=NOW), upstream_output=upstream,
        upstream_symbols=set(data['A3_CANDIDATE_ORIGIN']), bundle=args['bundle'], run_id=args['run_id'])
    assert merged.status == 'VALIDATED', merged.reason_codes
    assert len(seen) == 3 and len(merged.output['core_watch_pool']) == 3
    assert sorted(row['plan_priority_rank'] for row in merged.output['core_watch_pool']) == [1, 2, 3]
    assert all(row['model_priority_rank'] == 1 for row in merged.output['core_watch_pool'])


def test_source_date_and_contract_error_never_reaches_model(tmp_path):
    app, args, seen = actual_fixture(tmp_path)
    args['snapshot'] = FrozenInputSnapshot('not-closed', dict(args['snapshot'].data),
        as_of=NOW.replace(hour=9))
    audit = app._run_stage(**args)
    assert audit.status == 'BLOCKED' and audit.reason_codes == ('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED',)
    assert seen == []


def test_full_pipeline_gate_merge_preserves_all_local_candidates(tmp_path):
    from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
    app, args, seen = actual_fixture(tmp_path)
    local = dict(symbol='000002.SZ', status='LOCAL_MONITOR', eligibility='DATA_GAP',
        strategy_profile='NO_NEXT_DAY_PLAN', reason_codes=['ACTUAL_DATA_GAP'])
    gate = DeterministicGateResult('A3_LOCAL_TECHNICAL',
        (dict(args['snapshot'].data['A3_DETERMINISTIC_CONTEXT']['000001.SZ'], status='REVIEW_CANDIDATE'), local),
        ('000001.SZ',), ('000002.SZ',), ())
    upstream = deepcopy(args['upstream_output'])
    upstream['watch_only_pool'].append({'symbol': '000002.SZ', 'theme_id': 'T'})
    audit = app._run_v2_downstream_review(lane_id=args['lane_id'], model=args['model'], stage='A3',
        snapshot=args['snapshot'], upstream_output=upstream, full_upstream_symbols={'000001.SZ', '000002.SZ'},
        gate=gate, bundle=args['bundle'], run_id=args['run_id'])
    assert audit.status.startswith('VALIDATED') or audit.status.startswith('DEGRADED'), audit.reason_codes
    assert audit.output is not None and len(seen) == 1
    assert {row['symbol'] for pool in ('core_watch_pool', 'secondary_watch_pool', 'rejected_candidates')
        for row in audit.output[pool]} == {'000001.SZ', '000002.SZ'}


def test_restart_never_spends_original_request_timeout_beyond_remaining_parent(tmp_path, monkeypatch):
    app, args, seen = actual_fixture(tmp_path)
    adapter = app.team_research_adapter
    freeze = adapter.store.freeze_control_context
    def original_larger_budget(key, **kwargs):
        body = freeze(key, **kwargs)
        return {**body, 'timeout_seconds': 999} if key.startswith('a3-research-budget:') else body
    monkeypatch.setattr(adapter.store, 'freeze_control_context', original_larger_budget)
    audit = app._run_stage(**args)
    assert audit.reason_codes == ('TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT',) and seen == []


def test_focus_original_probe_mode_cannot_be_upgraded_to_standard(tmp_path):
    app, args, seen = actual_fixture(tmp_path, model_risk_unit='STANDARD')
    data = deepcopy(dict(args['snapshot'].data))
    data['A3_CANDIDATE_ORIGIN']['000001.SZ'] = 'FOCUS'
    data['A3_DETERMINISTIC_CONTEXT']['000001.SZ'].update(candidate_origin='FOCUS', plan_mode='PROBE')
    args['snapshot'] = FrozenInputSnapshot('original-probe', data, as_of=NOW)
    args['upstream_output']['focus_pool'] = args['upstream_output'].pop('watch_only_pool')
    audit = app._run_stage(**args)
    assert audit.status == 'VALIDATED' and audit.output is not None and len(seen) == 3
    assert audit.output['core_watch_pool'][0]['risk_unit'] == 'PROBE'
    projection = audit.diagnostics['server_projection']
    assert projection['status'] == 'VALIDATED'
    assert projection['original_issues'] == ['A3_PLAN_PASS_RISK_INVALID']
    assert projection['actions'][0]['code'] == 'SERVER_CLAMPED_RISK_UNIT'
    with sqlite3.connect(app.team_research_adapter.journal.path) as db:
        captured = db.execute('SELECT validated,output_json FROM model_calls WHERE role=?', ('A3',)).fetchone()
    assert captured[0] == 0
    assert json.loads(captured[1])['core_watch_pool'][0]['risk_unit'] == 'STANDARD'


def test_malformed_original_strategy_facts_is_explicit_source_block(tmp_path):
    app, args, seen = actual_fixture(tmp_path)
    data = deepcopy(dict(args['snapshot'].data))
    data['A3_DETERMINISTIC_CONTEXT']['000001.SZ']['strategy_facts'] = ['wrong-type']
    args['snapshot'] = FrozenInputSnapshot('malformed-original', data, as_of=NOW)
    audit = app._run_stage(**args)
    assert audit.status == 'BLOCKED' and audit.reason_codes == ('TEAM_RESEARCH_A3_FACTS_INVALID',)
    assert seen == []


def test_quarantined_model_row_stays_non_executable_through_actual_legacy_postpolicy(tmp_path):
    app, args, seen = actual_fixture(tmp_path, quarantine=True)
    original = deepcopy(dict(args['snapshot'].data))
    audit = app._run_stage(**args)
    assert audit.status == 'VALIDATED', audit.reason_codes
    assert len(seen) == 3 and audit.output['core_watch_pool'] == []
    row = audit.output['secondary_watch_pool'][0]
    assert row['symbol'] == '000001.SZ' and row['review_status'] == 'DATA_GAP'
    assert row['risk_unit'] == 'NO_ENTRY' and 'SERVER_QUARANTINED_ROW' in row['reason_codes']
    assert dict(args['snapshot'].data) == original
    projection = audit.diagnostics['server_projection']
    assert projection['status'] == 'VALIDATED'
    assert projection['actions'][0]['code'] == 'SERVER_QUARANTINED_ROW'
