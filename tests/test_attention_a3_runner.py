from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import pytest
from liangjian_funnel.runtime.attention_research import (
    AttentionResearchBinding,prepare_attention_research,run_attention_research,
)
from liangjian_funnel.pipeline.feature_store import content_hash
from liangjian_funnel.pipeline.research import FrozenInputSnapshot
from test_hot100_independent_research_intake import attention_fixture,LEAD,NOW
from test_team_v2_research_adapter import actual_fixture

def full_fixture(tmp_path):
    app,args,_=attention_fixture(tmp_path/'a2',lambda raw,n:raw['theme_reviews'][-1].update(stage='ACCELERATION',new_entry_policy='ALLOW'))
    args['snapshot']=replace(args['snapshot'],data={**args['snapshot'].data,
        'A2_MARKET_REFERENCE':{'market_trade_date':NOW.date().isoformat()}})
    audit=app._run_stage(**args);assert audit.status=='VALIDATED',audit.reason_codes
    from test_attention_a3_technical import technical
    data,_,_=technical(tmp_path/'technical')
    data.update({k:deepcopy(args['snapshot'].data[k]) for k in (
        'EASTMONEY_HOT100_SNAPSHOT','THS_CONCEPT_MEMBERSHIP','g0_symbols','snapshot_manifest','A2_MARKET_REFERENCE')})
    a1=args['upstream_output'];snapshot=FrozenInputSnapshot('outside-attention-fixture',data,as_of=NOW)
    a3,model_args,seen=actual_fixture(tmp_path/'a3')
    prepared=prepare_attention_research(snapshot,a1,audit,source_trade_date=NOW.date().isoformat())
    assert prepared['technical']['first_pass_model_requests']==1,prepared['technical']['records']
    binding=AttentionResearchBinding(run_id='fixture-attention-r1',source_trade_date=NOW.date().isoformat(),
        snapshot_id=snapshot.snapshot_id,snapshot_hash=snapshot.snapshot_hash,snapshot_data_sha256=content_hash(data),
        a2_output_hash=audit.output_hash,domain_hash=prepared['domain']['content_hash'],
        technical_hash=prepared['technical']['content_hash'],deadline=NOW+timedelta(hours=1),
        maximum_first_pass_batches=1,authority_sha256='a'*64)
    kwargs=dict(snapshot=snapshot,a1_output=a1,a2_audit=audit,binding=binding,
        assert_transport_budget=lambda binding,batch:None,assert_current_inputs=lambda binding:None,
        lane_id='lane_1',model=model_args['model'],bundle=model_args['bundle'])
    return a3,kwargs,seen

def test_actual_team_mock_transport_separate_a3_case_has_real_call_ids_and_no_entry(tmp_path):
    app,args,seen=full_fixture(tmp_path)
    before=deepcopy((args['a1_output'],args['a2_audit'].as_dict(),dict(args['snapshot'].data)))
    result=run_attention_research(app,**args)
    assert result['status']=='RESEARCH_CASES_COMPLETED',result
    assert len(seen)==1
    row=result['records'][0]
    assert row['symbol']==LEAD and row['model_state']=='MODEL_REVIEWED' and row['team_call_ids']
    assert row['execution_permission']=='BLOCKED' and row['risk_unit']=='NO_ENTRY'
    assert row['model_result']['execution_permission']=='BLOCKED' and row['model_result']['risk_unit']=='NO_ENTRY'
    assert result['execution_plan_write_requested'] is False
    assert (args['a1_output'],args['a2_audit'].as_dict(),args['snapshot'].data)==before
    assert all(LEAD not in {r.get('symbol') for r in args['a2_audit'].output[p]} for p in ('focus_pool','watch_only_pool'))

@pytest.mark.parametrize('defect',['typed','deadline','hash','authority','data_hash'])
def test_missing_or_changed_binding_never_reaches_model(tmp_path,defect):
    app,args,seen=full_fixture(tmp_path);b=args['binding']
    args['binding'] = ({'purpose':b.purpose} if defect=='typed' else replace(b,**{
        'deadline':NOW-timedelta(seconds=1)} if defect=='deadline' else {
        'domain_hash':'f'*64} if defect=='hash' else {'authority_sha256':''} if defect=='authority' else {'snapshot_data_sha256':'f'*64}))
    with pytest.raises(ValueError,match='ATTENTION_RESEARCH_'):run_attention_research(app,**args)
    assert seen==[]

def test_zero_remaining_batches_records_deferral_without_model(tmp_path):
    app,args,seen=full_fixture(tmp_path);args['binding']=replace(args['binding'],maximum_first_pass_batches=0)
    result=run_attention_research(app,**args)
    assert not seen and result['status']=='RESEARCH_CASES_INCOMPLETE'
    assert result['records'][0]['model_state']=='NOT_MODEL_REVIEWED_BUDGET_OR_DEADLINE_DEFERRED'

def test_actual_transport_guard_rejection_cannot_start_model(tmp_path):
    app,args,seen=full_fixture(tmp_path)
    def exhausted(*a):raise ValueError('CUMULATIVE_HTTP_CAP_REACHED')
    args['assert_transport_budget']=exhausted
    with pytest.raises(ValueError,match='CUMULATIVE_HTTP_CAP_REACHED'):run_attention_research(app,**args)
    assert seen==[]


def test_enriched_child_keeps_original_admission_clock_and_requires_separate_binding(tmp_path):
    from liangjian_funnel.pipeline.research.common import _capture_snapshot_recipe
    app,args,seen=full_fixture(tmp_path);parent=args['snapshot'];new_at=NOW+timedelta(minutes=5)
    overlay={'snapshot_manifest':{**parent.data['snapshot_manifest'],'as_of':new_at.isoformat()}}
    recipe=dict(base_snapshot_hash=parent.snapshot_hash,stage='A3',overlay=overlay)
    child=FrozenInputSnapshot(parent.snapshot_id+':a3:'+content_hash(recipe)[:12],
        {**parent.data,**overlay},content_hash(recipe),new_at)
    child=_capture_snapshot_recipe(parent,child,recipe_kind='STAGE_OVERLAY',hash_recipe=recipe,
        data_recipe=dict(operation='SHALLOW_OVERLAY',overlay=overlay))
    prepared=prepare_attention_research(parent,args['a1_output'],args['a2_audit'],
        source_trade_date=NOW.date().isoformat(),technical_snapshot=child)
    assert prepared['domain']['records'][0]['capture_as_of']==NOW.isoformat()
    assert prepared['technical']['records'][0]['technical']['as_of']==new_at.isoformat()
    args['technical_snapshot']=child
    with pytest.raises(ValueError,match='FUTURE_INPUT'):run_attention_research(app,**args)
    app.now=lambda:NOW+timedelta(minutes=10)
    with pytest.raises(ValueError,match='EXACT_TECHNICAL_BINDING_REQUIRED'):run_attention_research(app,**args)
    args['binding']=replace(args['binding'],technical_snapshot_id=child.snapshot_id,
        technical_snapshot_hash=child.snapshot_hash,technical_snapshot_data_sha256=content_hash(dict(child.data)),
        domain_hash=prepared['domain']['content_hash'],technical_hash=prepared['technical']['content_hash'])
    result=run_attention_research(app,**args)
    assert len(seen)==1 and result['records'][0]['model_state']=='MODEL_REVIEWED'


def test_derived_snapshot_without_parent_recipe_is_rejected(tmp_path):
    app,args,seen=full_fixture(tmp_path)
    child=FrozenInputSnapshot('unproven-child',dict(args['snapshot'].data),as_of=NOW)
    with pytest.raises(ValueError,match='TECHNICAL_PARENT_SEED_UNPROVEN'):
        prepare_attention_research(args['snapshot'],args['a1_output'],args['a2_audit'],
            source_trade_date=NOW.date().isoformat(),technical_snapshot=child)
    assert seen==[]
