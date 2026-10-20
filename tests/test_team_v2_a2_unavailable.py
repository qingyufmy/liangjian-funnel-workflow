"""P0-9 is server-owned WATCH provenance, never a fabricated A2 pass."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import json,sqlite3
import pytest

from liangjian_funnel.pipeline.research import common as legacy
from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
from liangjian_funnel.pipeline.team_v2_a2_fallback import build_a2_unavailable,proven_a2_unavailable,notify_a2_unavailable
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.settings import Settings

NOW=datetime.fromisoformat('2026-10-20T16:30:00+08:00')


def row(symbol='000001.SZ',status='REVIEW_CANDIDATE'):
    return dict(symbol=symbol,name='Frozen',status=status,role='TREND_CORE',theme_id='T',node_id='N',
        upstream_candidate_id='a1:'+symbol,stock_behavior_type='TREND',route_permission=['TREND_MA5','MA520_SWING'],
        route='MARKET_CORE',eligible_routes=['MARKET_CORE'],score=80,top_rotation_theme=True,
        trend_core_eligible=True,execution_permission='ALLOW_A4',data_sufficiency_state='SUFFICIENT',
        reason_codes=[],bottleneck_context={})


def setup_case(tmp_path,monkeypatch,*,reason='A2_MARKET_READ_INVALID'):
    gate=DeterministicGateResult('A2_LOCAL_ROLE',(row(),),('000001.SZ',),(),())
    data={'STRICT_AGENT_RULES':True,'A2_POOL_TARGETS':{'pool_min':0,'pool_max':60},
        'MARKET_EMOTION_SNAPSHOT':{'emotion_cycle_stage':'LATENT'}}
    snapshot=legacy._with_a2_bottleneck_context(legacy.FrozenInputSnapshot('frozen',data,as_of=NOW),gate)
    failed=legacy.StageAudit(lane='lane_1',model='offline',stage='A2',status='BLOCKED',snapshot_id=snapshot.snapshot_id,
        prompt_hash='a'*64,input_hash='b'*64,output_hash=None,latency_ms=10,attempts=3,
        thinking_variant='ORIGINAL_FAILED_NOT_REWRITTEN',symbols=(),reason_codes=(reason,),output=None,
        diagnostics={'validation_history':[[reason]]*3,'provider_raw_sha256':'c'*64})
    adapter=SimpleNamespace(supports=lambda stage:stage=='A2',store=TeamMeetingStore(tmp_path/'team.db'))
    app=legacy.ResearchPipeline(Settings.from_env({},root=tmp_path),model_client=object(),output_dir=tmp_path/'audit',
        enable_feature_store=False,team_research_adapter=adapter,now=lambda:NOW)
    monkeypatch.setattr(app,'_run_stage_with_checkpoint',lambda **kwargs:failed)
    a1={'active_research_pool':[dict(symbol='000001.SZ',company_name='Frozen',theme_id='T',node_id='N',candidate_id='a1:000001.SZ')]}
    args=dict(lane_id='lane_1',model='offline',stage='A2',snapshot=snapshot,upstream_output=a1,
        full_upstream_symbols={'000001.SZ'},gate=gate,bundle=None,run_id='offline-a2-unavailable')
    return app,args,failed


def test_actual_downstream_any_a2_failure_gets_proven_probe_handoff_and_one_alert(tmp_path,monkeypatch):
    app,args,failed=setup_case(tmp_path,monkeypatch)
    before=deepcopy(failed)
    audit=app._run_v2_downstream_review(**args)
    assert audit.status=='A2_REVIEW_DEGRADED',audit.reason_codes
    assert audit.diagnostics['model_review_passed'] is False
    assert audit.output['A2_UNAVAILABLE'] and audit.output['focus_pool']==[]
    assert audit.output['A2_UNAVAILABLE_SCOPE']==['000001.SZ']
    selected=legacy._build_a3_candidate_domain(audit.output)
    assert selected[1]=={'000001.SZ':'WATCH_ONLY'}
    assert selected[0]['focus_pool'][0]['risk_unit']=='PROBE'
    assert selected[0]['focus_pool'][0]['theme_score']==80
    assert selected[0]['focus_pool'][0]['theme_stage']=='LATENT'
    assert failed==before and failed.status=='BLOCKED'
    assert app._run_v2_downstream_review(**args).output==audit.output
    with sqlite3.connect(tmp_path/'team.db') as db:
        assert db.execute("SELECT count(*) FROM team_notifications WHERE kind='ALERT' AND status='PENDING'").fetchone()[0]==1


def test_only_a1_intersect_engine_review_candidate_is_handoff_scope():
    decisions=[row(),row('000002.SZ'),row('000003.SZ','WATCH_ONLY')]
    contexts={r['symbol']:{'deterministic_status':r['status']} for r in decisions}
    failed=SimpleNamespace(status='BLOCKED',input_hash='i',prompt_hash='p',reason_codes=('A2_BAD_ENUM',))
    output,proof=build_a2_unavailable(failed_audit=failed,decisions=decisions,a1_symbols={'000001.SZ','000003.SZ'},
        contexts=contexts,envelope={},snapshot_id='s',snapshot_hash='h',market_emotion={'emotion_cycle_stage':'LATENT'},
        project_row=legacy._gate_item_from_decision)
    assert proof['allowed_scope']==output['A2_UNAVAILABLE_SCOPE']==['000001.SZ']
    assert proven_a2_unavailable(output,{'A2_UNAVAILABLE_EVIDENCE':proof,'A2_BOTTLENECK_CONTEXT':contexts})
    # Appended WATCH rows cannot expand the fallback candidate domain.
    output['watch_only_pool'].append(legacy._gate_item_from_decision(row('000003.SZ','WATCH_ONLY'),'A2','WATCH_ONLY'))
    assert set(legacy._build_a3_candidate_domain(output)[1])=={'000001.SZ'}
    tampered=deepcopy(proof);tampered['allowed_scope'].append('000003.SZ')
    assert not proven_a2_unavailable(output,{'A2_UNAVAILABLE_EVIDENCE':tampered,'A2_BOTTLENECK_CONTEXT':contexts})


def test_original_technical_or_data_veto_remains_effective_in_fallback():
    candidate=legacy._gate_item_from_decision(row(),'A2','WATCH_ONLY')
    candidate.update(A2_UNAVAILABLE=True,data_sufficiency_state='DATA_GAP')
    output={'A2_UNAVAILABLE':True,'A2_UNAVAILABLE_SCOPE':['000001.SZ'],'focus_pool':[],'watch_only_pool':[candidate]}
    assert legacy._build_a3_candidate_domain(output)[1]=={}


def test_a3_failure_is_not_hidden_by_a2_degraded_status():
    statuses=lambda *values:[SimpleNamespace(status=s) for s in values]
    assert legacy._lane_status_from_stages(statuses('VALIDATED','A2_REVIEW_DEGRADED','BLOCKED'))=='BLOCKED'
    assert legacy._lane_status_from_stages(statuses('VALIDATED','A2_REVIEW_DEGRADED','VALIDATED'))=='READY_DEGRADED'


def test_a2_success_never_enters_fallback():
    assert build_a2_unavailable(failed_audit=SimpleNamespace(status='VALIDATED'),decisions=[],a1_symbols=set(),
        contexts={},envelope={},snapshot_id='s',snapshot_hash='h',market_emotion={},project_row=legacy._gate_item_from_decision) is None


@pytest.mark.parametrize('invalid,eligibility',[(False,'QUALIFIED'),(True,'QUALIFIED'),(False,'DATA_GAP')])
def test_fallback_uses_actual_a3_adapter_without_bypassing_geometry_or_engine(tmp_path,invalid,eligibility):
    from test_team_v2_research_adapter import actual_fixture
    app,args,seen=actual_fixture(tmp_path,invalid=invalid,eligibility=eligibility,model_risk_unit='PROBE')
    upstream=deepcopy(args['upstream_output'])
    upstream['A2_UNAVAILABLE']=True
    upstream['A2_UNAVAILABLE_SCOPE']=['000001.SZ']
    upstream['watch_only_pool'][0].update(A2_UNAVAILABLE=True,A2_REVIEW_DEGRADED=True,
        theme_score=80,theme_stage='LATENT',risk_unit='PROBE')
    args['upstream_output']=upstream
    before=deepcopy(dict(args['snapshot'].data))
    audit=app._run_stage(**args)
    assert len(seen)==(3 if invalid else 1)
    if invalid or eligibility=='DATA_GAP':
        assert not (audit.output or {}).get('core_watch_pool')
    else:
        assert audit.status=='VALIDATED',audit.reason_codes
        plan=audit.output['core_watch_pool'][0]
        assert plan['A2_UNAVAILABLE'] is True and plan['A2_REVIEW_DEGRADED'] is True
        assert plan['risk_unit']=='PROBE' and plan['candidate_origin']=='WATCH_ONLY'
        assert plan['trigger_zone']=={'low':10,'high':11} and plan['daily_invalidation']==9
    assert args['snapshot'].data==before


def test_unproven_output_cannot_suppress_original_rotation_validation(tmp_path,monkeypatch):
    app,args,_=setup_case(tmp_path,monkeypatch)
    audit=app._run_v2_downstream_review(**args)
    output=deepcopy(audit.output)
    # No server proof in the original snapshot: a marker alone is not authority.
    assert not proven_a2_unavailable(output,args['snapshot'].data)
    output['watch_only_pool'][0]['symbol']='999999.SZ'
    issues=legacy._validate_output(output,stage='A2',model='offline',snapshot_id=args['snapshot'].snapshot_id,
        upstream_symbols={'000001.SZ'},snapshot_data=args['snapshot'].data)
    assert any('OUTSIDE_UPSTREAM' in issue for issue in issues)
