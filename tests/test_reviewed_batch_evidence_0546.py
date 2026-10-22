"""Read actual session/journal tables produced with mocked HTTP, never live I/O."""
from copy import deepcopy
from datetime import date
import hashlib
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.runtime.reviewed_batch_evidence import build_reviewed_batch_evidence
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore, canonical
from test_team_v2_morning import fixture, advance, CAL


SCOPE = 'review-scope'
DAY = '2026-10-19'


def prepared(tmp_path, monkeypatch):
    import liangjian_funnel.runtime.team_v2_morning as morning
    settings, clock = fixture(tmp_path, trade_date=DAY)
    with sqlite3.connect(settings.state_db_path) as db:
        payload = json.loads(db.execute('SELECT payload_json FROM execution_plans').fetchone()[0])
        db.execute('UPDATE execution_plans SET payload_json=?', (canonical(payload),))
    original_risk, original_pm = morning.run_risk_premarket, morning.run_pm_premarket
    original_load = TeamMeetingStore.load_morning_risk
    def risk(**kwargs):
        kwargs.update(meeting_id=SCOPE + ':risk', call_id=SCOPE + ':A6')
        return original_risk(**kwargs)
    def pm(**kwargs):
        kwargs.update(meeting_id=SCOPE + ':pm', call_id=SCOPE + ':PM')
        return original_pm(**kwargs)
    def load(self, account_id, day, *, source_call_id):
        return original_load(self, account_id, day, source_call_id=SCOPE + ':A6')
    monkeypatch.setattr(morning, 'run_risk_premarket', risk)
    monkeypatch.setattr(morning, 'run_pm_premarket', pm)
    monkeypatch.setattr(TeamMeetingStore, 'load_morning_risk', load)
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from test_team_v2_risk_schema import output as risk_output
    from test_team_v2_pm_session import response as pm_output
    calls = []
    def handler(request):
        req = json.loads(request.read())
        runtime = json.loads(req['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        calls.append(req)
        envelope = runtime['required_envelope']
        raw = risk_output() if envelope['stage_id'] == 'A6' else pm_output()
        raw['envelope'] = envelope
        if envelope['stage_id'] == 'A6':
            raw['utilization_cap']['cap_pct'] = 65
            # One actual plan: original answer passes without quota repair.
            raw['capacity_for_tomorrow'].update(plans_acceptable=1, cluster_quota={'bank': 1})
        else:
            raw['plan_decisions'][0].update(plan_ref='MORNING:PLAN:p1', size_tier='PROBE')
            raw['deployment_target'].update(target_pct=60, baseline_range=[40, 65])
        body = json.dumps(raw).replace('"C1"', '"MORNING:ACCOUNT"').replace('"M1"', '"MORNING:MARKET"')
        body = body.replace('"A"', '"MORNING:ACCOUNT"').replace('"M"', '"MORNING:MARKET"')
        return httpx.Response(200, json={'choices': [{'message': {'content': body}}]}, request=request)
    api = OpenAICompatibleModelClient(settings, transport=httpx.MockTransport(handler), max_attempts=1)
    engine = morning.run_morning_stage(settings, 'engine', clock=lambda: clock.now, calendar=CAL)
    advance(clock, 8, 35)
    assert morning.run_morning_stage(settings, 'risk', clock=lambda: clock.now, calendar=CAL, client=api)['status'] == 'READY'
    advance(clock, 8, 38)
    assert morning.run_morning_stage(settings, 'pm', clock=lambda: clock.now, calendar=CAL, client=api)['model_validated'] is True
    assert len(calls) == 2
    row = engine['source']['plan_rows'][0]
    candidates = [dict(plan_id=row['plan_id'], lane_id=row['lane_id'], symbol=row['symbol'],
        status='PENDING_MORNING_REVIEW', valid_from=None, expires_at=row['expires_at'],
        plan_version=1, payload=json.loads(row['payload_json']))]
    return settings, dict(isolated_root=settings.root, scope_id=SCOPE, account_id='local-paper', trade_date=DAY,
        lane_id=settings.research_primary_lane_id, run_id=payload['source_run_id'],
        a6_meeting_id=SCOPE + ':risk', pm_meeting_id=SCOPE + ':pm', candidate_rows=candidates)


def snapshot(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob('*') if path.is_file()}


def test_actual_review_journals_build_inert_bundle_without_writes(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    before = snapshot(settings.root)
    body = build_reviewed_batch_evidence(**arguments)
    assert body['status'] == 'READY' and body['switch_mode'] == 'PRE_AUCTION'
    assert body['evidence']['a6']['meeting_record']['meeting_id'] == SCOPE + ':risk'
    assert body['evidence']['a6']['risk_result_sha256'] == hashlib.sha256(canonical(body['risk']).encode()).hexdigest()
    assert body['evidence']['a6']['engine_envelopes_sha256'] == hashlib.sha256(canonical(body['engine']['envelopes']).encode()).hexdigest()
    assert body['pm']['model_validated'] is True
    assert snapshot(settings.root) == before


@pytest.mark.parametrize('role,field,value,reason', [
    ('A6', 'validated', 0, 'MODEL_RAW_NOT_VALIDATED'),
    ('A3', 'validated', 0, 'MODEL_RAW_NOT_VALIDATED'),
    ('A6', 'output_hash', 'f' * 64, 'MODEL_RAW_HASH_CONFLICT'),
    ('A3', 'input_hash', 'f' * 64, 'MODEL_CAPTURE_BINDING_CONFLICT'),
    ('A6', 'execution_trade_date', '2026-10-20', 'MODEL_SCOPE_MISMATCH'),
])
def test_invalid_or_tampered_capture_cannot_be_ready(tmp_path, monkeypatch, role, field, value, reason):
    settings, arguments = prepared(tmp_path, monkeypatch)
    with sqlite3.connect(settings.root / 'state/team_v2/model.db') as db:
        db.execute('UPDATE model_calls SET ' + field + '=? WHERE role=?', (value, role))
    with pytest.raises(ValueError, match=reason):
        build_reviewed_batch_evidence(**arguments)


def test_missing_catalog_is_blocked_not_filled_from_green_meeting(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    with sqlite3.connect(settings.root / 'state/team_v2/model.db') as db:
        db.execute('DELETE FROM model_call_evidence WHERE call_id=?', (SCOPE + ':A6',))
    with pytest.raises(ValueError, match='MODEL_CATALOG_MISSING'):
        build_reviewed_batch_evidence(**arguments)


def test_old_or_mixed_meeting_identity_is_rejected(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    arguments['a6_meeting_id'] = 'morning-risk:' + DAY
    with pytest.raises(ValueError, match='INDEPENDENT_MEETING_IDS_REQUIRED'):
        build_reviewed_batch_evidence(**arguments)


def test_wrong_a6_engine_input_rehashed_still_fails_semantic_binding(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    with sqlite3.connect(settings.root / 'state/team_v2/team.db') as db:
        inputs = json.loads(db.execute('SELECT input_json FROM risk_meetings').fetchone()[0])
        inputs['envelopes'][0]['max_shares_cap'] += 100
        text = canonical(inputs)
        db.execute('UPDATE risk_meetings SET input_json=?,binding_sha256=?', (text, hashlib.sha256(text.encode()).hexdigest()))
    with pytest.raises(ValueError, match='A6_ENGINE_INPUT_CONFLICT'):
        build_reviewed_batch_evidence(**arguments)


def test_rehashed_a6_result_cannot_exceed_recomputed_original(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    with sqlite3.connect(settings.root / 'state/team_v2/team.db') as db:
        result = json.loads(db.execute('SELECT result_json FROM risk_meetings').fetchone()[0])
        result['utilization_cap'] = '100'
        db.execute('UPDATE risk_meetings SET result_json=?', (canonical(result),))
    with pytest.raises(ValueError, match='A6_RESULT_RECOMPUTATION_CONFLICT'):
        build_reviewed_batch_evidence(**arguments)


def test_pm_fallback_never_becomes_reviewed_bundle(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    with sqlite3.connect(settings.root / 'state/team_v2/team.db') as db:
        result = json.loads(db.execute('SELECT result_json FROM pm_decisions').fetchone()[0])
        result['decision_authority'] = 'SERVER_FALLBACK_NOT_MODEL_VALIDATED'
        db.execute('UPDATE pm_decisions SET result_json=?', (canonical(result),))
    with pytest.raises(ValueError, match='PM_NOT_MODEL_VALIDATED'):
        build_reviewed_batch_evidence(**arguments)


def test_caller_candidate_payload_cannot_replace_actual_reviewed_source(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    arguments['candidate_rows'][0]['payload']['no_chase'] = 100
    with pytest.raises(Exception, match='REVIEWED_BATCH_ENGINE_PLAN_CONFLICT'):
        build_reviewed_batch_evidence(**arguments)


def test_missing_engine_is_not_rebuilt_by_constructor(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    with sqlite3.connect(settings.root / 'state/team_v2/team.db') as db:
        db.execute('DELETE FROM morning_engine')
    with pytest.raises(ValueError, match='ENGINE_ORIGINAL_MISSING'):
        build_reviewed_batch_evidence(**arguments)


def test_judgment_field_tampering_is_detected(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    with sqlite3.connect(settings.root / 'state/team_v2/model.db') as db:
        db.execute("UPDATE model_judgments SET symbol='000002.SZ' WHERE call_id=?", (SCOPE + ':A6',))
    with pytest.raises(ValueError, match='MODEL_JUDGMENT_HASH_CONFLICT'):
        build_reviewed_batch_evidence(**arguments)


def test_pm_meeting_allocation_cannot_change_captured_model_vote(tmp_path, monkeypatch):
    settings, arguments = prepared(tmp_path, monkeypatch)
    with sqlite3.connect(settings.root / 'state/team_v2/team.db') as db:
        result = json.loads(db.execute('SELECT result_json FROM pm_decisions').fetchone()[0])
        result['plan_allocations'][0]['intended_risk_unit'] = 'STANDARD'
        db.execute('UPDATE pm_decisions SET result_json=?', (canonical(result),))
    with pytest.raises(ValueError, match='PM_RESULT_RECOMPUTATION_CONFLICT'):
        build_reviewed_batch_evidence(**arguments)
