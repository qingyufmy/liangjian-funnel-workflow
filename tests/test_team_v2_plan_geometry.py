from copy import deepcopy
from dataclasses import replace
from decimal import Decimal

import pytest

from liangjian_funnel.pipeline.team_v2_plan_geometry import project_team_plan_geometry,GEOMETRY_FIELD,PUBLICATION_CONTRACT


def plan(target=14,low=10,high=11,stop=9):
    return dict(symbol='002530.SZ',trigger_low=low,trigger_high=high,trigger_zone=dict(low=low,high=high),
        entry_reference_zone=dict(low=low,high=high),stop_level=stop,invalidation_level=stop,
        first_resistance=target,minimum_reward_risk=2.5,risk_unit='PROBE')


@pytest.mark.parametrize('target,expected',[(11,'EMPTY'),(14,'PARTIAL'),(20,'FULL')])
def test_three_intersections_preserve_original_and_only_lower_upper(target,expected):
    original=plan(target);before=deepcopy(original)
    projection=project_team_plan_geometry(original)
    assert original==before and projection['audit']['classification']==expected
    if expected=='EMPTY':
        assert projection['status']=='OBSERVATION_ONLY' and projection['payload'] is None
        assert projection['audit']['reason_code']=='PLAN_GEOMETRY_RR_INFEASIBLE'
    else:
        out=projection['payload'];upper=Decimal(str(out['trigger_high']))
        assert out['trigger_low']==original['trigger_low'] and upper<=Decimal(str(original['trigger_high']))
        assert out['first_resistance']==original['first_resistance'] and out['stop_level']==original['stop_level']
        assert Decimal(str(target))-upper >= Decimal('2.5')*(upper-Decimal('9'))
        if expected=='FULL':assert {k:v for k,v in out.items() if k!=GEOMETRY_FIELD}==original


def test_002530_actual_frozen_geometry_is_infeasible_not_market_no_setup():
    raw=plan(target=7.49,low=7.1633,high=7.23,stop=7.01)
    out=project_team_plan_geometry(raw)
    assert out['audit']['classification']=='EMPTY'
    assert Decimal(out['audit']['rr_entry_ceiling'])<Decimal('7.1633')


@pytest.mark.parametrize('alias',['entry_reference_zone','entry_zone','pullback_zone'])
def test_partial_projection_never_widens_a_stricter_zone_alias(alias):
    raw=plan(target=8,low=7.2,high=7.5,stop=7)
    raw[alias]={'low':7.21,'high':7.25};before=deepcopy(raw)
    result=project_team_plan_geometry(raw)
    assert result['audit']['classification']=='PARTIAL'
    out=result['payload']
    assert out[alias]==before[alias]
    assert Decimal(str(out['trigger_zone']['high']))<=Decimal('7.285714285714285714285714285714286')
    assert raw==before


def test_each_high_max_and_scalar_upper_is_independently_nonincreasing():
    raw=plan(target=8,low=7.2,high=7.5,stop=7)
    raw['trigger_high']='7.24'
    raw['trigger_zone']['max']='7.235'
    raw['entry_reference_zone']={'min':'7.21','low':7.22,'high':7.4,'max':'7.23'}
    raw['strategy_facts']={'entry_reference_zone':{'low':7.2,'high':7.5},'opaque':'original'}
    before=deepcopy(raw);out=project_team_plan_geometry(raw)['payload']
    assert out['trigger_high']==before['trigger_high']
    assert out['trigger_zone']['max']==before['trigger_zone']['max']
    for name in ('trigger_zone','entry_reference_zone'):
        for key in ('high','max'):
            assert Decimal(str(out[name][key]))<=Decimal(str(before[name][key]))
        for key in ('low','min'):
            if key in before[name]:assert out[name][key]==before[name][key]
    assert out['strategy_facts']==before['strategy_facts'] and raw==before


@pytest.mark.parametrize('upper',[{}, {'high':None}, {'max':None}, {'high':'unknown'}, {'high':False}])
def test_alias_without_a_valid_upper_is_not_fabricated_or_repaired(upper):
    raw=plan(target=8,low=7.2,high=7.5,stop=7)
    raw['entry_reference_zone']={'low':7.21,**upper}
    raw['trigger_high']=None
    out=project_team_plan_geometry(raw)['payload']
    assert out['entry_reference_zone']==raw['entry_reference_zone']
    assert out['trigger_high'] is None


def test_missing_scalar_upper_is_not_added_and_equal_upper_is_not_reformatted():
    raw=plan(target=8,low=7.2,high=7.5,stop=7);raw.pop('trigger_high')
    number=project_team_plan_geometry(raw)['payload']['trigger_zone']['high']
    raw['entry_reference_zone']['high']=str(number)
    out=project_team_plan_geometry(raw)['payload']
    assert 'trigger_high' not in out
    assert out['entry_reference_zone']['high']==str(number)


@pytest.mark.parametrize('change',[dict(first_resistance=None),dict(stop_level=None,invalidation_level=None),
    dict(trigger_low=None,trigger_zone={},entry_reference_zone={}),dict(minimum_reward_risk=None),
    dict(first_resistance=float('inf'))])
def test_missing_or_invalid_inputs_are_unproven_not_invented(change):
    raw=plan();raw.update(change)
    if change.get('first_resistance')==float('inf'):
        with pytest.raises(ValueError):project_team_plan_geometry(raw)
    else:
        out=project_team_plan_geometry(raw)
        assert out['status']=='OBSERVATION_ONLY' and out['audit']['classification']=='UNPROVEN'


def test_current_a4_alias_precedence_and_frozen_snapshot_threshold():
    raw=plan();raw.pop('minimum_reward_risk');raw['first_resistance']=None
    raw['pressure_reduce_price']=14;raw['observation_target']=100
    out=project_team_plan_geometry(raw,minimum_reward_risk=2.5)
    assert out['audit']['target']=='14' and out['audit']['threshold_source']=='FROZEN_SNAPSHOT_MIN_REWARD_RISK'
    assert project_team_plan_geometry(raw)['audit']['classification']=='UNPROVEN'


def projection_inputs(target=14,team=True):
    from test_wp5_publication_receipt import fixture,bound
    from liangjian_funnel.workflow import _plan_payload
    from liangjian_funnel.runtime.publication_projection import capture_publication_projection
    inputs=fixture()
    raw={**inputs.raw_plan.value,'first_resistance':target,'minimum_reward_risk':2.5}
    output={**inputs.a3_audit.value['output'],'core_watch_pool':[raw]}
    audit={**inputs.a3_audit.value,'output':output,'output_hash':bound(output).sha256,
        'thinking_variant':'team_v2' if team else None}
    normalized=_plan_payload(raw)
    submitted={**normalized,'source_run_id':inputs.run_id}
    value=project_team_plan_geometry(submitted)['payload']
    inputs=replace(inputs,raw_plan=bound(raw),a3_audit=bound(audit),normalized_payload=bound(normalized),
        publication_contract=PUBLICATION_CONTRACT)
    return capture_publication_projection(inputs,shadow_projection=None,
        submitted_payload=bound(value),published_payload=bound(deepcopy(value)))


@pytest.mark.parametrize('target',[14,20])
def test_receipt_recomputes_team_partial_and_full_projection(target):
    from test_wp5_publication_receipt import build
    inputs=projection_inputs(target)
    out=build(inputs)
    assert out['evidence_complete'],out
    assert out['plan_geometry_evidence_complete'] and out['plan_geometry_gate']=='VERIFIED'
    assert any(row['basis']=='TEAM_GEOMETRY_TIGHTENING' for row in out['modifications'])


def test_receipt_geometry_and_existing_shadow_projection_are_independent():
    from test_wp5_publication_receipt import build,bound
    from liangjian_funnel.runtime.publication_projection import capture_publication_projection
    inputs=projection_inputs();shadow={'schema_version':'a4-shadow-inputs/1','status':'DATA_LIMITED',
        'gap_codes':['FROZEN_FACTOR_MISSING'],'execution_authority':False}
    payload={**inputs.submitted_payload.value,'strategy_facts':{'shadow_inputs':shadow}}
    inputs=capture_publication_projection(inputs,shadow_projection=bound(shadow),
        submitted_payload=bound(payload),published_payload=bound(deepcopy(payload)))
    assert build(inputs)['evidence_complete']


def test_receipt_snapshot_rr_must_be_frozen_not_self_declared_by_geometry_audit():
    from test_wp5_publication_receipt import fixture,bound,build,snapshot
    from liangjian_funnel.workflow import _plan_payload
    from liangjian_funnel.runtime.publication_projection import capture_publication_projection
    inputs=fixture();raw={**inputs.raw_plan.value,'first_resistance':14}
    output={**inputs.a3_audit.value['output'],'core_watch_pool':[raw]}
    audit={**inputs.a3_audit.value,'output':output,'output_hash':bound(output).sha256,'thinking_variant':'team_v2'}
    normalized=_plan_payload(raw);payload=project_team_plan_geometry({**normalized,'source_run_id':inputs.run_id},
        minimum_reward_risk=2.5)['payload']
    inputs=replace(inputs,raw_plan=bound(raw),a3_audit=bound(audit),normalized_payload=bound(normalized),
        publication_contract=PUBLICATION_CONTRACT)
    inputs=capture_publication_projection(inputs,shadow_projection=None,
        submitted_payload=bound(payload),published_payload=bound(deepcopy(payload)))
    result=build(inputs)
    assert not result['evidence_complete'] and 'TEAM_PLAN_GEOMETRY_NOT_PUBLISHABLE' in result['reason_codes']
    # Supply the actual frozen threshold through a valid original snapshot
    # chain, not through the final payload's own audit declaration.
    base=deepcopy(inputs.snapshot_chain[0].snapshot.value)
    base['data']['MIN_REWARD_RISK']=2.5;base['snapshot_hash']=bound(base['data']).sha256
    recipe=deepcopy(inputs.snapshot_chain[1].hash_recipe.value)
    recipe['base_snapshot_hash']=base['snapshot_hash']
    child=deepcopy(inputs.snapshot_chain[1].snapshot.value)
    child['snapshot_hash']=bound(recipe).sha256;child['snapshot_id']='base:a3:'+child['snapshot_hash'][:12]
    child['data']={**base['data'],**recipe['overlay']}
    chain=(snapshot(base),snapshot(child,recipe_kind='STAGE_OVERLAY',parent=base['snapshot_hash'],recipe=recipe))
    audit={**inputs.a3_audit.value,'snapshot_id':child['snapshot_id']}
    proven=replace(inputs,a3_audit=bound(audit),snapshot_chain=chain)
    assert build(proven)['evidence_complete'],build(proven)


@pytest.mark.parametrize('target',[11,14,20])
def test_new_manifest_contract_rejects_wholesale_geometry_removal_and_original_upper_restore(target):
    from test_wp5_publication_receipt import fixture,bound,build
    from liangjian_funnel.workflow import _plan_payload
    from liangjian_funnel.runtime.publication_projection import capture_publication_projection
    original=fixture();raw={**original.raw_plan.value,'first_resistance':target,'minimum_reward_risk':2.5}
    output={**original.a3_audit.value['output'],'core_watch_pool':[raw]}
    audit={**original.a3_audit.value,'output':output,'output_hash':bound(output).sha256,'thinking_variant':'team_v2'}
    normalized=_plan_payload(raw);restored_original={**normalized,'source_run_id':original.run_id}
    inputs=replace(original,raw_plan=bound(raw),a3_audit=bound(audit),normalized_payload=bound(normalized),
        publication_contract=PUBLICATION_CONTRACT)
    inputs=capture_publication_projection(inputs,shadow_projection=None,
        submitted_payload=bound(restored_original),published_payload=bound(deepcopy(restored_original)))
    receipt=build(inputs)
    assert not receipt['evidence_complete'] and not receipt['plan_geometry_evidence_complete']
    assert 'TEAM_PLAN_GEOMETRY_AUDIT_MISSING' in receipt['reason_codes']
    if target==11:assert 'TEAM_PLAN_GEOMETRY_NOT_PUBLISHABLE' in receipt['reason_codes']
    # Missing legacy version is not retroactively rewritten into a new gate
    # pass, even if old generic publication evidence remains complete.
    legacy=build(replace(inputs,publication_contract='legacy-publication/1'))
    assert legacy['evidence_complete'] and not legacy['plan_geometry_evidence_complete']
    assert legacy['plan_geometry_gate']=='LEGACY_NOT_EVALUATED'


def test_pinned_manifest_contract_reaches_reader_independently_of_payload(tmp_path):
    from test_b3_publication_reader import with_proof,rebind_audit,rewrite_manifest,save
    from test_wp5_publication_receipt import build
    from liangjian_funnel.runtime.auction_publication_export import ReadOnlyPublicationReader
    db,ref=with_proof(tmp_path)
    audit=ref.audit.read();audit['stages'][2]['thinking_variant']='team_v2'
    ref=rebind_audit(ref,audit)
    ref=rewrite_manifest(ref,lambda manifest:manifest.update(publication_contract=PUBLICATION_CONTRACT))
    summary=ref.summary.read()
    summary['plan_publication']['projection_manifest']['publication_contract']=PUBLICATION_CONTRACT
    ref=replace(ref,summary=save(ref.summary.path,summary))
    values,_,_=ReadOnlyPublicationReader(db)._projection_inputs(ref,ref.summary.read(),ref.audit.read())
    assert all(value.publication_contract==PUBLICATION_CONTRACT for value in values.values())
    for value in values.values():
        result=build(value)
        assert not result['evidence_complete'] and 'TEAM_PLAN_GEOMETRY_AUDIT_MISSING' in result['reason_codes']


def test_archive_contract_is_outside_payload_and_byte_pinned(tmp_path):
    import json
    from test_b3_publication_archive import source
    from liangjian_funnel.runtime.publication_archive import capture_plan_originals,write_publication_manifest
    from liangjian_funnel.runtime.publication_receipt import object_hash
    raw,a3,audit,base,_,row=source(tmp_path)
    a3['thinking_variant']='team_v2'
    audit.write_text(json.dumps({'stages':[a3]}),encoding='utf-8')
    original=capture_plan_originals(raw=raw,pool='core_watch_pool',index=0,a3_audit=a3,authority=None)
    result=write_publication_manifest(output_dir=tmp_path/'proofs',run_id='run',lane_id='lane_1',
        base_snapshot_path=base,base_snapshot_id='snap',base_snapshot_hash=object_hash({}),audit_path=audit,
        originals={row['plan_id']:original},published=[row],published_at='2026-10-15T16:30:00+08:00',
        publication_mode='CLOSE',minimum_trade_date='2026-10-16',publication_contract=PUBLICATION_CONTRACT)
    from pathlib import Path
    manifest=json.loads(Path(result['path']).read_bytes())
    assert result['publication_contract']==manifest['publication_contract']==PUBLICATION_CONTRACT
    assert manifest['manifest_sha256']==object_hash({k:v for k,v in manifest.items() if k!='manifest_sha256'})
    assert GEOMETRY_FIELD not in manifest['plans'][0]['published_payload']['value']


@pytest.mark.parametrize('mutation',['increase_high','alter_stop','alter_audit','drop_audit','non_team','forged_change_hash'])
def test_receipt_rejects_forged_geometry_even_if_payload_resealed(mutation):
    from test_wp5_publication_receipt import build,bound
    from liangjian_funnel.runtime.publication_projection import capture_publication_projection
    inputs=projection_inputs();payload=deepcopy(inputs.submitted_payload.value)
    if mutation=='increase_high':payload['trigger_high']=11;payload['trigger_zone']['high']=11
    elif mutation=='alter_stop':payload['stop_level']=8
    elif mutation=='alter_audit':payload[GEOMETRY_FIELD]['minimum_reward_risk']='1'
    elif mutation=='drop_audit':payload.pop(GEOMETRY_FIELD)
    elif mutation=='non_team':inputs=replace(inputs,a3_audit=bound({**inputs.a3_audit.value,'thinking_variant':None}))
    changed=capture_publication_projection(inputs,shadow_projection=None,
        submitted_payload=bound(payload),published_payload=bound(deepcopy(payload)))
    if mutation=='forged_change_hash':changed=replace(changed,changes=tuple(
        replace(c,projection_sha256='0'*64) if c.basis=='TEAM_GEOMETRY_TIGHTENING' else c for c in changed.changes))
    assert not build(changed)['evidence_complete']


@pytest.mark.parametrize('team,target,created,classification',[(True,11,0,'EMPTY'),(True,14,1,'PARTIAL'),
    (True,20,1,'FULL'),(True,None,0,'UNPROVEN'),(False,11,1,None)])
def test_actual_plan_publication_team_only_hook_keeps_a3_originals(team,target,created,classification):
    from test_w2_shadow_inputs_publication import publisher_fixture,AT,TARGET
    from liangjian_funnel.workflow import WorkflowApplication
    from liangjian_funnel.pipeline.research import StageAudit
    app,result,data,raw=publisher_fixture();raw['first_resistance']=target
    data['MIN_REWARD_RISK']=2.5
    stage=StageAudit(lane='lane_1',model='fixture',stage='A3',status='VALIDATED',
        snapshot_id=result.snapshot_id,prompt_hash=None,input_hash=None,output_hash=None,
        latency_ms=0,attempts=0,thinking_variant='team_v2' if team else None,
        symbols=(raw['symbol'],),reason_codes=(),output=result.lanes[0].final_output)
    result=replace(result,lanes=(replace(result.lanes[0],stages=(stage,)),))
    before=deepcopy(raw)
    summary=WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    assert raw==before and len(summary['created'])==created
    if team:
        assert summary['team_plan_geometry'][0]['geometry']['classification']==classification
        assert bool(summary['observation_pool'])==(created==0)
        if created:assert app.store.batch[0]['payload']['trigger_high']<=raw['trigger_zone']['high']
    else:assert summary['team_plan_geometry']==[]
