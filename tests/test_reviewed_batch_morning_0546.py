"""Actual candidate sessions over synthetic frozen sources; no live HTTP."""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.runtime.reviewed_batch_morning import prepare_candidate_morning_engine, run_candidate_morning_reviews
from liangjian_funnel.runtime.reviewed_batch_evidence import build_reviewed_batch_evidence
from liangjian_funnel.runtime.team_v2_store import canonical
from liangjian_funnel.runtime.team_v2_sources import read_planning_scope
from test_team_v2_morning import fixture, CAL

DAY = '2026-10-19'
SCOPE = 'candidate-test'


def sha(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def files(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob('*') if path.is_file()}


def setup(tmp_path, monkeypatch):
    import liangjian_funnel.runtime.team_v2_morning as natural
    baseline_settings, baseline_clock = fixture(tmp_path / 'baseline', trade_date=DAY)
    baseline = natural.run_morning_stage(baseline_settings, 'engine', clock=lambda: baseline_clock.now, calendar=CAL)
    # Controlled original natural-morning account contains an old owner. Its
    # original row/hash is retained; no candidate plan impersonates that owner.
    old_row = deepcopy(baseline['source']['plan_rows'][0])
    old_row.update(plan_id='held-002270', symbol='002270.SZ', status='INVALIDATED',
        valid_from='2026-10-16T10:00:00+08:00', expires_at='2026-10-16T15:00:00+08:00')
    payload = json.loads(old_row['payload_json'])
    payload.update(symbol='002270.SZ', cluster_label='machinery')
    old_row['payload_json'] = canonical(payload)
    from liangjian_funnel.review.observability import _cluster
    cluster, basis = _cluster(old_row)
    baseline['held_plan_bindings'] = [dict(plan_id='held-002270', symbol='002270.SZ', cluster=cluster,
        cluster_basis=basis, original_row=old_row, row_sha256=sha(old_row))]
    baseline['snapshot']['positions'] = [dict(plan_id='held-002270', symbol='002270.SZ', qty=1000,
        sellable_qty=1000, market_value='100000', avg_cost='100')]
    baseline['snapshot']['account'].update(cash='900000', nav='1000000', positions_value='100000', pnl_unrealized='0')
    baseline_path = baseline_settings.root / 'state/team_v2/team.db'
    with sqlite3.connect(baseline_path) as db:
        db.execute('UPDATE morning_engine SET body_json=?', (canonical(baseline),))
    settings, clock = fixture(tmp_path / 'isolated', trade_date=DAY)
    clock.now = datetime.fromisoformat(DAY + 'T08:42:00+08:00')
    with sqlite3.connect(settings.state_db_path) as db:
        payload = json.loads(db.execute('SELECT payload_json FROM execution_plans').fetchone()[0])
        payload['source_run_id'] = 'candidate-research'
        db.execute('UPDATE execution_plans SET plan_id=?,valid_from=NULL,payload_json=?', ('new-plan', canonical(payload)))
    source = read_planning_scope(settings, now=clock.now, calendar=CAL)
    assert source['status'] == 'COMPLETE'
    # The candidate API must not open/reinitialize a paper broker or collect.
    def forbidden(*args, **kwargs): raise AssertionError('COLLECTION_OR_BROKER_FORBIDDEN')
    monkeypatch.setattr(natural, 'create_broker', forbidden)
    import liangjian_funnel.runtime.team_v2_emotion_baseline as emotion
    monkeypatch.setattr(emotion, 'prepare_emotion_baseline', forbidden)
    return settings, clock, source, baseline_settings, baseline_path, baseline


def model(settings, *, invalid_a6=False, quota_bad=False):
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from test_team_v2_risk_schema import output as risk_output
    from test_team_v2_pm_session import response as pm_output
    calls = []
    def handler(request):
        req = json.loads(request.read())
        runtime = json.loads(req['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        envelope = runtime['required_envelope']
        calls.append(envelope['stage_id'])
        raw = risk_output() if envelope['stage_id'] == 'A6' else pm_output()
        raw['envelope'] = envelope
        if envelope['stage_id'] == 'A6':
            raw['envelope_adjustments'][0]['plan_id'] = 'new-plan'
            raw['utilization_cap']['cap_pct'] = 99 if invalid_a6 else 65
            raw['capacity_for_tomorrow'].update(plans_acceptable=2 if quota_bad else 1,
                cluster_quota={'bank': 2 if quota_bad else 1})
        else:
            raw['plan_decisions'][0].update(plan_ref='MORNING:PLAN:new-plan', size_tier='PROBE')
            raw['deployment_target'].update(target_pct=60, baseline_range=[40, 65])
        body = json.dumps(raw).replace('"C1"', '"MORNING:ACCOUNT"').replace('"M1"', '"MORNING:MARKET"')
        body = body.replace('"A"', '"MORNING:ACCOUNT"').replace('"M"', '"MORNING:MARKET"')
        return httpx.Response(200, json={'choices': [{'message': {'content': body}}]}, request=request)
    return OpenAICompatibleModelClient(settings, transport=httpx.MockTransport(handler), max_attempts=1), calls


def prepare(settings, clock, source, baseline_path):
    return prepare_candidate_morning_engine(settings=settings, source=source, baseline_team_db=baseline_path,
        scope_id=SCOPE, clock=lambda: clock.now, calendar=CAL)


def test_isolated_engine_real_sessions_evidence_keeps_002270_baseline_and_no_publish(tmp_path, monkeypatch):
    settings, clock, source, baseline_settings, baseline_path, baseline = setup(tmp_path, monkeypatch)
    before = files(baseline_settings.root)
    engine = prepare(settings, clock, source, baseline_path)
    assert engine['snapshot'] == baseline['snapshot'] and engine['held_plan_bindings'] == baseline['held_plan_bindings']
    assert engine['capacity_slots'] == baseline['capacity_slots'] - 1
    assert engine['account_snapshot_basis']['original_observed_at'] == baseline['observed_at']
    assert engine['envelopes'][0]['purpose'] == 'PLANNING'
    assert not (settings.root / 'state/team_v2/paper.db').exists()
    api, calls = model(settings)
    result = run_candidate_morning_reviews(settings=settings, scope_id=SCOPE, clock=lambda: clock.now, calendar=CAL, client=api)
    assert result['status'] == 'READY' and calls == ['A6', 'A3'] and result['publication_calls'] == 0
    row = source['plan_rows'][0]
    candidate = dict(plan_id=row['plan_id'], lane_id=row['lane_id'], symbol=row['symbol'], status='PENDING_MORNING_REVIEW',
        valid_from=None, expires_at=row['expires_at'], payload=json.loads(row['payload_json']), plan_version=1)
    bundle = build_reviewed_batch_evidence(isolated_root=settings.root, scope_id=SCOPE, account_id='local-paper',
        trade_date=DAY, lane_id=settings.research_primary_lane_id, run_id='candidate-research',
        a6_meeting_id=result['a6_meeting_id'], pm_meeting_id=result['pm_meeting_id'], candidate_rows=[candidate])
    assert bundle['status'] == 'READY' and bundle['engine']['held_plan_bindings'][0]['symbol'] == '002270.SZ'
    with sqlite3.connect(settings.state_db_path) as db:
        assert db.execute('SELECT plan_id,status FROM execution_plans').fetchall() == [('new-plan', 'PENDING_MORNING_REVIEW')]
    assert files(baseline_settings.root) == before


def test_a6_failure_never_calls_pm_or_creates_paper_account(tmp_path, monkeypatch):
    settings, clock, source, _, baseline_path, _ = setup(tmp_path, monkeypatch)
    prepare(settings, clock, source, baseline_path)
    api, calls = model(settings, invalid_a6=True)
    result = run_candidate_morning_reviews(settings=settings, scope_id=SCOPE, clock=lambda: clock.now, calendar=CAL, client=api)
    assert result['status'] == 'BLOCKED' and result['pm_called'] is False and calls == ['A6']
    assert not (settings.root / 'state/team_v2/paper.db').exists()


@pytest.mark.parametrize('changed', ['window', 'config', 'source', 'baseline_date', 'held_hash'])
def test_preparation_failures_have_no_isolated_team_write(tmp_path, monkeypatch, changed):
    settings, clock, source, _, baseline_path, baseline = setup(tmp_path, monkeypatch)
    if changed == 'window': clock.now = clock.now.replace(hour=9, minute=26)
    elif changed == 'config':
        with (settings.root / 'config/risk_limits.yaml').open('a', encoding='utf-8') as stream: stream.write('\n# candidate mismatch\n')
    elif changed == 'source': source['plan_rows'][0]['status'] = 'ACTIVE_TODAY'
    else:
        if changed == 'baseline_date': baseline['trade_date'] = '2026-10-16'
        else: baseline['held_plan_bindings'][0]['row_sha256'] = 'f' * 64
        with sqlite3.connect(baseline_path) as db:
            db.execute('UPDATE morning_engine SET body_json=?', (canonical(baseline),))
    with pytest.raises(ValueError, match='CANDIDATE_REVIEW_'):
        prepare(settings, clock, source, baseline_path)
    assert not (settings.root / 'state/team_v2/team.db').exists()


def test_baseline_and_isolated_roots_must_not_overlap(tmp_path, monkeypatch):
    settings, clock, source, baseline_settings, baseline_path, _ = setup(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match='BASELINE_ROOT_NOT_ISOLATED'):
        prepare_candidate_morning_engine(settings=baseline_settings, source=source, baseline_team_db=baseline_path,
            scope_id=SCOPE, clock=lambda: clock.now, calendar=CAL)


def test_config_or_window_change_after_engine_prevents_model_call(tmp_path, monkeypatch):
    settings, clock, source, _, baseline_path, _ = setup(tmp_path, monkeypatch)
    prepare(settings, clock, source, baseline_path)
    api, calls = model(settings)
    clock.now = clock.now.replace(hour=9, minute=26)
    with pytest.raises(ValueError, match='PRE_AUCTION_WINDOW_MISSED'):
        run_candidate_morning_reviews(settings=settings, scope_id=SCOPE, clock=lambda: clock.now, calendar=CAL, client=api)
    assert calls == []


def test_frozen_engine_envelope_tampering_prevents_any_model_call(tmp_path, monkeypatch):
    settings, clock, source, _, baseline_path, _ = setup(tmp_path, monkeypatch)
    prepare(settings, clock, source, baseline_path)
    with sqlite3.connect(settings.root / 'state/team_v2/team.db') as db:
        engine = json.loads(db.execute('SELECT body_json FROM morning_engine').fetchone()[0])
        engine['envelopes'][0]['max_shares_cap'] += 100
        db.execute('UPDATE morning_engine SET body_json=?', (canonical(engine),))
    api, calls = model(settings)
    with pytest.raises(ValueError, match='ISOLATED_ENGINE_ALREADY_BOUND'):
        run_candidate_morning_reviews(settings=settings, scope_id=SCOPE, clock=lambda: clock.now, calendar=CAL, client=api)
    assert calls == []


def test_effective_a6_ready_with_invalid_original_still_skips_pm(tmp_path, monkeypatch):
    settings, clock, source, _, baseline_path, _ = setup(tmp_path, monkeypatch)
    prepare(settings, clock, source, baseline_path)
    api, calls = model(settings, quota_bad=True)
    result = run_candidate_morning_reviews(settings=settings, scope_id=SCOPE, clock=lambda: clock.now, calendar=CAL, client=api)
    assert result['risk']['status'] == 'READY'
    assert result['status'] == 'BLOCKED' and result['reason_code'] == 'CANDIDATE_A6_ORIGINAL_NOT_VALIDATED'
    assert result['pm_called'] is False and calls == ['A6']
