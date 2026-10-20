"""Real model-client transport mocked at HTTP; independent on-disk stores."""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository, _ENVELOPE_KEYS
from liangjian_funnel.runtime.team_v2_pm_session import run_pm_premarket
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.settings import Settings
from test_team_v2_pm_schema import CATALOG, env, ranking_response as legacy_response


def response():
    output=legacy_response();output.pop('plan_ranking');output.pop('plan_allocations')
    output['plan_decisions']=[dict(plan_ref='PLAN:p1',decision='ACCEPT',size_tier='STANDARD',
        reason_code='ACCEPT_STRUCTURE',note='冻结形态')]
    return output

NOW = datetime.fromisoformat('2026-10-19T08:38:00+08:00')
MODEL = 'deepseek-v4-pro-0813'


def fixture(tmp_path, *, invalid=False, offline=False):
    envelope = {key: 'FROZEN' for key in _ENVELOPE_KEYS}
    envelope.update(stage_id='A3', model_name=MODEL, run_id='pm-local',
        source_trade_date='2026-10-16', execution_trade_date='2026-10-19')
    repo = TeamPromptRepository(Path(__file__).resolve().parents[1]/'prompts/team_v2')
    values = {key: [] for key in repo.document('A3').placeholders}
    values['EVIDENCE_CATALOG'] = deepcopy(CATALOG)
    values['EVIDENCE_CATALOG']['categories'].append('PLAN')
    values['EVIDENCE_CATALOG']['items'].append(dict(id='PLAN:p1',category='PLAN',
        value=dict(plan_id='p1',symbol='000001.SZ',cluster='bank',publication_facts={'plan_priority_rank':1})))
    output = response(); output['envelope'] = envelope
    if invalid: output['plan_decisions'][0]['size_tier'] = 'MAX'
    seen = []
    def handler(request):
        seen.append(json.loads(request.read()))
        if offline: raise httpx.ConnectError('PRIVATE_PROVIDER_DETAILS', request=request)
        return httpx.Response(200, json={'choices': [{'message': {'content': json.dumps(output)}}]}, request=request)
    settings = Settings.from_env({'LIANGJIAN_MODEL_API_KEY': 'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'}, root=tmp_path)
    kwargs = dict(repository=repo, client=OpenAICompatibleModelClient(settings,
        transport=httpx.MockTransport(handler), max_attempts=1),
        journal=ModelJudgmentJournal(tmp_path/'model.sqlite3'), store=TeamMeetingStore(tmp_path/'team.sqlite3'),
        meeting_id='pm:20261019', call_id='A3:pm:20261019', account_id='paper', run_id='pm-local',
        model=MODEL, trade_date='2026-10-19', source_trade_date='2026-10-16', clock=lambda: NOW,
        required_envelope=envelope, replacements=values, envelopes=(env(),), plan_clusters={'p1': 'bank'},
        free_slots=2, baseline_range=(Decimal(50), Decimal(75)), utilization_cap=Decimal(75),
        high_conviction_ids=set(), timeout_seconds=30, max_output_tokens=4096, allow_model_call=True,
        geometry_plan_ids=frozenset({'p1'}))
    return kwargs, seen


def test_pm_actual_client_schema_and_both_journals_restart_without_another_call(tmp_path):
    kwargs, seen = fixture(tmp_path); frozen = deepcopy(kwargs['replacements'])
    result = run_pm_premarket(**kwargs)
    assert result['status'] == 'READY' and result['slot_plan'][0]['primary_plan_id'] == 'p1'
    kwargs['store'] = TeamMeetingStore(tmp_path/'team.sqlite3')
    kwargs['journal'] = ModelJudgmentJournal(tmp_path/'model.sqlite3')
    assert run_pm_premarket(**kwargs) == result and len(seen) == 1
    assert kwargs['replacements'] == frozen
    assert len(kwargs['journal'].summary()) == 1
    with sqlite3.connect(tmp_path/'team.sqlite3') as db:
        assert db.execute('SELECT count(*) FROM pm_decisions').fetchone()[0] == 1
        assert db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall() == []


@pytest.mark.parametrize('offline',[False,True])
def test_capture_then_meeting_commit_crash_resumes_original_output_without_provider_retry(tmp_path, monkeypatch,offline):
    kwargs, seen = fixture(tmp_path,offline=offline)
    def broken(**_): raise sqlite3.OperationalError('TEST_ONLY_STORAGE_FAULT')
    monkeypatch.setattr(kwargs['store'], 'record_pm', broken)
    with pytest.raises(sqlite3.OperationalError): run_pm_premarket(**kwargs)
    kwargs['store'] = TeamMeetingStore(tmp_path/'team.sqlite3')
    assert run_pm_premarket(**kwargs)['status'] == ('PM_DEGRADED_NO_ALLOCATION' if offline else 'READY') and len(seen) == 1


@pytest.mark.parametrize('invalid,offline', [(True, False), (False, True)])
def test_model_failure_does_not_allocate_slots_or_expose_exception_text(tmp_path, invalid, offline):
    kwargs, seen = fixture(tmp_path, invalid=invalid, offline=offline)
    result = run_pm_premarket(**kwargs)
    assert result['status'] == 'PM_DEGRADED_NO_ALLOCATION'
    assert result['slot_plan'] == [] and result['plan_allocations'] == []
    assert result['deployment_target'] is None
    assert run_pm_premarket(**kwargs) == result and len(seen) == 1
    assert b'PRIVATE_PROVIDER_DETAILS' not in (tmp_path/'team.sqlite3').read_bytes()
    if invalid:
        with sqlite3.connect(tmp_path/'model.sqlite3') as db:
            output = json.loads(db.execute('SELECT output_json FROM model_calls').fetchone()[0])
            assert output['plan_decisions'][0]['size_tier'] == 'MAX'


def test_model_claim_in_flight_never_creates_a_false_degraded_winner(tmp_path):
    kwargs, seen = fixture(tmp_path)
    kwargs['journal'].claim(kwargs['call_id'], 'owned-by-other-worker')
    with pytest.raises(ValueError, match='MODEL_CALL_BINDING_CONFLICT'): run_pm_premarket(**kwargs)
    with sqlite3.connect(tmp_path/'team.sqlite3') as db:
        assert db.execute('SELECT count(*) FROM pm_decisions').fetchone()[0] == 0
    assert seen == []


def test_changed_frozen_context_or_account_cannot_reuse_previous_meeting(tmp_path):
    kwargs, seen = fixture(tmp_path); run_pm_premarket(**kwargs)
    kwargs['account_id'] = 'other-account'
    with pytest.raises(ValueError, match='PM_MEETING_BINDING_CONFLICT'): run_pm_premarket(**kwargs)
    assert len(seen) == 1


def test_authorization_and_invalid_server_context_stop_before_http(tmp_path):
    kwargs, seen = fixture(tmp_path); kwargs['allow_model_call'] = False
    with pytest.raises(ValueError, match='NOT_AUTHORIZED'): run_pm_premarket(**kwargs)
    kwargs['allow_model_call'] = True; kwargs['free_slots'] = -1
    with pytest.raises(ValueError, match='PM_SERVER_CONTEXT_INVALID'): run_pm_premarket(**kwargs)
    assert seen == []


def test_captured_output_binding_and_sha_are_rechecked_before_resume(tmp_path, monkeypatch):
    kwargs, seen = fixture(tmp_path)
    def broken(**_): raise sqlite3.OperationalError('TEST_ONLY_STORAGE_FAULT')
    monkeypatch.setattr(kwargs['store'], 'record_pm', broken)
    with pytest.raises(sqlite3.OperationalError): run_pm_premarket(**kwargs)
    kwargs['store'] = TeamMeetingStore(tmp_path/'team.sqlite3')
    with sqlite3.connect(tmp_path/'model.sqlite3') as db:
        db.execute("UPDATE model_calls SET output_json='{}'")
    with pytest.raises(ValueError, match='MODEL_OUTPUT_HASH_MISMATCH'): run_pm_premarket(**kwargs)
    assert len(seen) == 1
