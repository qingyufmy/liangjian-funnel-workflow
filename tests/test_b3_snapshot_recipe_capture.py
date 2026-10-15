import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from liangjian_funnel.pipeline import research as r
from liangjian_funnel.settings import Settings


def base():
    return r.FrozenInputSnapshot('base',{'KEEP':{'value':7},'FACTOR_SNAPSHOT':{
        '600000.SH':{'timeframes':{'daily':{'close':10},'weekly':{},'monthly':{},'5m':{'secret':'intraday'}},
                     'technical_summary':{'timeframes':{'daily':{},'15m':{}}},'retained':True}}},as_of='2026-10-15T15:10:00+08:00')


def pipeline(tmp_path, callback):
    return r.ResearchPipeline(Settings.from_env({},root=tmp_path),model_client=object(),
        output_dir=tmp_path/'audit',stage_snapshot_enricher=callback,enable_feature_store=False)


def enrich(p,snapshot,stage='A3'):
    return p._enrich_stage_snapshot(stage=stage,lane_id='lane_1',model='test',upstream_symbols={'600000.SH'},snapshot=snapshot)


def replay_data(parent,node):
    data=dict(parent.data)
    recipe=node['data_recipe']
    if recipe['operation']=='SHALLOW_OVERLAY':data.update(recipe['overlay'])
    elif recipe['operation']=='A3_DETERMINISTIC_CONTEXT':
        factors={}
        for symbol,raw in data.get('FACTOR_SNAPSHOT',{}).items():
            if not isinstance(raw,dict):continue
            factor=dict(raw)
            if isinstance(raw.get('timeframes'),dict):factor['timeframes']={k:v for k,v in raw['timeframes'].items() if k in {'daily','weekly','monthly'}}
            if isinstance(raw.get('technical_summary'),dict):
                summary=dict(raw['technical_summary'])
                if isinstance(summary.get('timeframes'),dict):summary['timeframes']={k:v for k,v in summary['timeframes'].items() if k in {'daily','weekly','monthly'}}
                factor['technical_summary']=summary
            factors[symbol]=factor
        if isinstance(data.get('FACTOR_SNAPSHOT'),dict):data['FACTOR_SNAPSHOT']=factors
        data['A3_DETERMINISTIC_CONTEXT']=recipe['context']
    else:raise AssertionError(recipe)
    return data


def assert_recipe(parent,result,kind):
    node=result.proof_chain[-1]
    assert node['recipe_kind']==kind and node['status']=='CAPTURED'
    assert node['parent_snapshot_id']==parent.snapshot_id
    assert node['parent_snapshot_hash']==parent.snapshot_hash
    assert node['snapshot_id']==result.snapshot_id
    assert node['snapshot_hash']==result.snapshot_hash==r._sha256_json(node['hash_recipe'])
    assert replay_data(parent,node)==dict(result.data)


def test_overlay_is_captured_at_actual_transform_without_changing_hash_or_prompt(tmp_path):
    source=base();overlay={'ADDED':{'x':1}}
    result=enrich(pipeline(tmp_path,lambda **kw:overlay),source)
    expected_hash=r._sha256_json({'base_snapshot_hash':source.snapshot_hash,'stage':'A3','overlay':overlay})
    expected=r.FrozenInputSnapshot(f'base:a3:{expected_hash[:12]}',{**source.data,**overlay},expected_hash,source.as_of)
    assert result.snapshot_id==expected.snapshot_id and result.snapshot_hash==expected.snapshot_hash
    assert r._canonical_json(r._runtime_input(result,'lane_1','test','A3',{},set()))==r._canonical_json(r._runtime_input(expected,'lane_1','test','A3',{},set()))
    assert 'proof_chain' not in result.data
    assert_recipe(source,result,'STAGE_OVERLAY')
    assert r._freeze_snapshot(result).proof_chain==result.proof_chain


def test_real_a2_and_a3_transforms_have_exact_recipes(tmp_path):
    source=base()
    a2_gate=SimpleNamespace(decisions=({'symbol':'600000.SH','name':'stock','bottleneck_context':{'existing':1}},))
    a2=r._with_a2_bottleneck_context(source,a2_gate)
    assert_recipe(source,a2,'A2_BOTTLENECK_CONTEXT')
    a3=r._with_a3_candidate_context(source,{'600000.SH':'focus','BAD':'unknown'})
    assert_recipe(source,a3,'A3_CANDIDATE_CONTEXT')
    gate=SimpleNamespace(decisions=({'symbol':'600000.SH','strategy_profile':'TREND_MA5','eligibility':'QUALIFIED'},))
    final=r._with_a3_deterministic_context(a3,gate)
    assert_recipe(a3,final,'A3_DETERMINISTIC_CONTEXT')
    assert '5m' not in final.data['FACTOR_SNAPSHOT']['600000.SH']['timeframes']
    assert source.data['FACTOR_SNAPSHOT']['600000.SH']['timeframes']['5m']


def test_complete_wrapper_does_not_invent_a2_parent_chain(tmp_path):
    source=base();a2=r._with_a2_bottleneck_context(source,SimpleNamespace(decisions=()))
    independently=enrich(pipeline(tmp_path,lambda **kw:{'STAGE':'A3'}),source)
    result=enrich(pipeline(tmp_path,lambda **kw:independently),a2)
    assert result.proof_chain==independently.proof_chain
    assert result.proof_chain[0]['parent_snapshot_id']=='base'


@pytest.mark.parametrize('wrapper', ['frozen','mapping'])
def test_unknown_full_wrapper_is_explicitly_unproven_not_reverse_engineered(tmp_path,wrapper):
    source=base();unknown=r.FrozenInputSnapshot('custom:a3:not-a-recipe',{'anything':'unrelated'},as_of=source.as_of)
    value=unknown if wrapper=='frozen' else {'snapshot_id':unknown.snapshot_id,'snapshot_hash':unknown.snapshot_hash,'data':unknown.data}
    result=enrich(pipeline(tmp_path,lambda **kw:value),source)
    proof=r._snapshot_proof(result)
    assert proof['status']=='UNPROVEN'
    assert proof['chain'][-1]['hash_recipe'] is None
    assert proof['chain'][-1]['recipe_kind']=='UNKNOWN_WRAPPER'
    assert result.snapshot_id==unknown.snapshot_id and dict(result.data)==dict(unknown.data)


def test_stage_audit_proof_is_separate_from_model_business_output():
    s=base();proof=r._snapshot_proof(s)
    audit=r.StageAudit('lane_1','test','A3','VALIDATED',s.snapshot_id,None,None,None,0,0,None,(),(),output={'core_watch_pool':[]},snapshot_proof=proof)
    encoded=audit.as_dict()
    assert encoded['snapshot_proof']==proof and encoded['output']=={'core_watch_pool':[]}
    assert 'snapshot_proof' not in encoded['output']


@pytest.mark.parametrize('board',[None,{}, {'available':True,'boards':[]}])
def test_rotation_policy_recipe_keeps_original_business_expression(board):
    from liangjian_funnel.data.rotation_policy import authorized_rotation_policy, POLICY_ID
    source=base()
    if board is not None:source=replace(source,data={**source.data,'SELECTED_BOARD_SNAPSHOT':board})
    original=dict(source.data)
    if isinstance(original.get('SELECTED_BOARD_SNAPSHOT'),dict):original['SELECTED_BOARD_SNAPSHOT']=authorized_rotation_policy(original['SELECTED_BOARD_SNAPSHOT'])
    original.update(A2_ROTATION_THEME_COUNT=8,A2_AUTHORIZED_ROTATION_POLICY=POLICY_ID)
    expected_hash=r._sha256_json({'source_hash':source.snapshot_hash,'policy':POLICY_ID,'board_overlay':original.get('SELECTED_BOARD_SNAPSHOT')})
    result=r._with_authorized_rotation_policy(source)
    assert dict(result.data)==original and result.snapshot_hash==expected_hash
    assert result.snapshot_id==source.snapshot_id+':rotation-policy'
    assert_recipe(source,result,'A2_ROTATION_POLICY')


@pytest.mark.parametrize('early_stop',[False,True])
def test_actual_v2_audit_always_attaches_real_snapshot_without_prompt_changes(tmp_path,monkeypatch,early_stop):
    from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
    source=r._with_a3_candidate_context(base(),{'600000.SH':'FOCUS'})
    engine=pipeline(tmp_path,None)
    monkeypatch.setattr(engine,'_deadline_exceeded',lambda:early_stop)
    result=engine._run_v2_downstream_review(lane_id='lane_1',model='test',stage='A3',snapshot=source,
        upstream_output={},full_upstream_symbols=set(),gate=DeterministicGateResult('A3_LOCAL',(),(),(),()),bundle=None,run_id='local-proof')
    assert result.snapshot_proof==r._snapshot_proof(source)
    assert result.as_dict()['snapshot_proof']['chain'][-1]['recipe_kind']=='A3_CANDIDATE_CONTEXT'
    assert 'snapshot_proof' not in (result.output or {})
    no_proof=replace(source,proof_chain=())
    assert r._canonical_json(r._runtime_input(source,'lane_1','test','A3',{},set()))==r._canonical_json(r._runtime_input(no_proof,'lane_1','test','A3',{},set()))


def test_hash_preserving_scope_projection_is_not_falsely_claimed_proven():
    source=r._with_a3_candidate_context(base(),{'600000.SH':'FOCUS'})
    result=r._restrict_snapshot_to_symbols(source,{'600000.SH'})
    assert result.snapshot_id==source.snapshot_id and result.snapshot_hash==source.snapshot_hash
    assert r._snapshot_proof(result)['status']=='UNPROVEN'


def test_malformed_custom_metadata_is_unproven_not_a_business_crash(tmp_path):
    source=replace(base(),proof_chain=('not-a-recipe',))
    result=enrich(pipeline(tmp_path,lambda **kw:source),base())
    assert r._snapshot_proof(result)['status']=='UNPROVEN'
