"""Actual recipe producers -> actual publisher -> pinned offline reader.

Synthetic local originals only. No model, provider, production DB or customer
notification is involved; a pass is not natural-publication acceptance.
"""
from dataclasses import replace
from datetime import datetime, date
from types import SimpleNamespace
import json
import sqlite3

import pytest

from test_wp5_auction_publication_export import fixture as db_fixture, save, sha, NOW, RUN, SYMBOL, TZ
from liangjian_funnel.pipeline import research as research
from liangjian_funnel.runtime.auction_publication_export import (
    FilePin, FrozenDatabase, PublicationReference, ReadOnlyPublicationReader,
)
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.runtime import state as runtime_state
from liangjian_funnel.runtime.publication_receipt import object_hash
from liangjian_funnel.settings import Settings
from liangjian_funnel import workflow


SOURCE = datetime(2026, 10, 8, 15, 10, tzinfo=TZ)
PUBLISHED = datetime(2026, 10, 8, 16, 10, tzinfo=TZ)


def current_call(tmp_path, monkeypatch, *, strip_proof=False, before_publish=None, team=False):
    db, old_ref = db_fixture(tmp_path, run_status='READY_TO_PUBLISH')
    raw = old_ref.audit.read()['final_output']['core_watch_pool'][0]
    if team:
        raw.update(first_resistance=20,minimum_reward_risk=2.5)
    data = {'TRADABILITY_FLAGS': {SYMBOL: {'tradable': True}},
        'FACTOR_SNAPSHOT': {SYMBOL: {'timeframes': {'daily': {}, 'weekly': {}, '5m': {}}}},
        'SELECTED_BOARD_SNAPSHOT': {'available': False, 'boards': []}}
    if team:
        data['snapshot_manifest']={'as_of':SOURCE.isoformat(),'source_failures':{}}
    base = research.FrozenInputSnapshot('actual-base-fixture', data, as_of=SOURCE)
    base_pin = save(tmp_path / 'base.json', {'snapshot_id':base.snapshot_id,
        'snapshot_hash':base.snapshot_hash, 'as_of':SOURCE.isoformat(), 'data':dict(base.data)})

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return PUBLISHED if tz is None else PUBLISHED.astimezone(tz)
    monkeypatch.setattr(workflow, 'datetime', Clock)
    monkeypatch.setattr(runtime_state, '_now', lambda: PUBLISHED)
    news_app = SimpleNamespace(_collect_open_news=lambda *a, **kw: ())

    def callback(**kwargs):
        if kwargs['stage'] == 'A2':
            return workflow.WorkflowApplication._stage_snapshot_enricher(news_app, **kwargs)
        return {'A3_TECHNICAL_SCOPE':[SYMBOL], 'A3_TECHNICAL_READY':[SYMBOL]}
    engine = research.ResearchPipeline(Settings.from_env({},root=tmp_path), model_client=object(),
        output_dir=tmp_path/'research', stage_snapshot_enricher=callback, enable_feature_store=False)
    def enrich(stage):
        return engine._enrich_stage_snapshot(stage=stage,lane_id='lane_1',model='fixture',
            upstream_symbols={SYMBOL},snapshot=base)
    a2 = research._with_authorized_rotation_policy(enrich('A2'))
    a2 = research._with_a2_bottleneck_context(a2,SimpleNamespace(decisions=(
        {'symbol':SYMBOL,'bottleneck_context':{'original':True}},)))
    a3 = research._with_a3_candidate_context(enrich('A3'),{SYMBOL:'FOCUS'})
    a3 = research._with_a3_deterministic_context(a3,SimpleNamespace(decisions=(
        {'symbol':SYMBOL,'strategy_profile':'MA520_SWING','eligibility':'QUALIFIED'},)))
    authority = {'symbol':SYMBOL,'execution_permission':'REQUIRES_A3_A4_CONFIRMATION',
                 'research_only_reason':None}
    output = {'core_watch_pool':[raw],'secondary_watch_pool':[]}
    def stage(name, snapshot, value):
        return research.StageAudit('lane_1','fixture',name,'VALIDATED',snapshot.snapshot_id,
            None,None,object_hash(value),0,1,'team_v2' if team and name=='A3' else None,(SYMBOL,),(),output=value,
            snapshot_proof=None if strip_proof else research._snapshot_proof(snapshot))
    a1_stage = stage('A1',base,{})
    a2_stage = stage('A2',a2,{'focus_pool':[authority]})
    a3_stage = stage('A3',a3,output)
    lane = research.LaneResult('lane_1','fixture','READY',(a1_stage,a2_stage,a3_stage),output,
        audit_path=tmp_path/'audit.json')
    audit_pin = save(lane.audit_path,lane.as_dict())
    result = research.ResearchRunResult(RUN,SOURCE,base.snapshot_id,base.snapshot_hash,'READY',
        (lane,),(lane.audit_path,),None)
    with sqlite3.connect(db.path) as connection:
        connection.execute('DELETE FROM execution_plans')
        connection.execute('UPDATE workflow_runs SET snapshot_hash=?',(base.snapshot_hash,))

    class Store:
        def list_execution_plans(self, **kwargs): return []
        def publish_plan_batch(self, rows, **kwargs):
            with sqlite3.connect(db.path) as connection:
                for row in rows:
                    connection.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?,?,?,?)',
                        (row['plan_id'],row['lane_id'],row['symbol'],row['status'],1,None,
                        row['expires_at'].isoformat(),json.dumps(row['payload']),PUBLISHED.isoformat(),PUBLISHED.isoformat()))
                connection.row_factory=sqlite3.Row
                return tuple(dict(row) for row in connection.execute('SELECT * FROM execution_plans'))
        def _write(self, operation):
            with sqlite3.connect(db.path) as connection:
                return operation(connection)
        def mark_workflow_runs_published(self, *args):
            return RuntimeStore.mark_workflow_runs_published(self, *args)
    app = SimpleNamespace(store=Store(),settings=SimpleNamespace(workflow_output_dir=tmp_path/'outputs'),
        fact_cache=None,trading_calendar=ExchangeTradingCalendar())
    if before_publish is not None:
        before_publish(lane, base, a3)
    publication = workflow.WorkflowApplication._publish_plans(app,result,'close',SOURCE,
        snapshot_data=base.data,minimum_trade_date=date(2026,10,9),publication_snapshot_path=base_pin.path)
    assert len(publication['created']) == 1
    captured = publication['projection_manifests'][0]
    assert captured['status'] == 'CAPTURED_NOT_VALIDATED', captured
    summary = old_ref.summary.read()
    summary['source_as_of'] = SOURCE.isoformat()
    summary['snapshot'] = {'snapshot_id':base.snapshot_id,'snapshot_hash':base.snapshot_hash}
    summary['plan_publication'] = publication
    ref = PublicationReference(RUN,'lane_1',save(old_ref.summary.path,summary),audit_pin,
        FilePin(__import__('pathlib').Path(captured['path']),captured['sha256']))
    return FrozenDatabase(db.path,sha(db.path),NOW), ref, (a2_stage,a3_stage)


def exported(db,ref):
    return ReadOnlyPublicationReader(db).export(publications=(ref,), target_trade_date=NOW.date(),
        account_ids=('paper:lane_1',),calendar=ExchangeTradingCalendar())


def test_actual_producer_recipes_and_current_call_publish_reconcile(tmp_path,monkeypatch):
    db,ref,stages=current_call(tmp_path,monkeypatch)
    original_hashes=(sha(db.path),ref.audit.sha256,ref.projection_manifest.sha256)
    result=exported(db,ref)
    assert result['scope_complete'] is True, result['reason_codes']
    assert original_hashes==(sha(db.path),sha(ref.audit.path),sha(ref.projection_manifest.path))
    receipt=result['plans'][0]['publication_projection_receipt']
    assert receipt['evidence_complete'] is True
    assert receipt['published_at']==PUBLISHED.isoformat()
    assert [node['recipe_kind'] for node in receipt['authority_snapshot_chain']]==[
        'BASE_RESEARCH_DATA','A2_STOCK_NEWS','A2_ROTATION_POLICY','A2_BOTTLENECK_CONTEXT']
    assert [node['recipe_kind'] for node in receipt['snapshot_chain']]==[
        'BASE_RESEARCH_DATA','STAGE_OVERLAY','A3_CANDIDATE_CONTEXT','A3_DETERMINISTIC_CONTEXT']
    assert receipt['acquisition_provenance_authenticated'] is False
    assert receipt['eligibility_released'] is False


def test_actual_producer_capture_does_not_make_missing_recipes_pass(tmp_path,monkeypatch):
    db,ref,_=current_call(tmp_path,monkeypatch,strip_proof=True)
    result=exported(db,ref)
    assert result['scope_complete'] is False
    assert 'AUDIT_OVERLAY_BINDING_UNPROVEN' in result['reason_codes']
    assert result['plans']==[]


def test_new_manifest_does_not_implicitly_relax_default_legacy_reader(tmp_path,monkeypatch):
    db,ref,_=current_call(tmp_path,monkeypatch)
    result=exported(db,replace(ref,projection_manifest=None))
    assert result['scope_complete'] is False
    assert 'AUDIT_OVERLAY_BINDING_UNPROVEN' in result['reason_codes']


def test_team_geometry_cluster_actual_publication_roundtrip(tmp_path,monkeypatch):
    db,ref,_=current_call(tmp_path,monkeypatch,team=True)
    result=exported(db,ref)
    assert result["scope_complete"],result["reason_codes"]
    receipt=result["plans"][0]["publication_projection_receipt"]
    assert receipt["evidence_complete"] and receipt["plan_geometry_evidence_complete"]
