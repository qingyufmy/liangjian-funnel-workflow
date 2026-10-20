from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from liangjian_funnel.pipeline.research_overlay_resume import make_a2_stock_news_enricher,require_resume_arguments
from liangjian_funnel.runtime.publication_receipt import BoundObject,object_hash


def fixture():
    at=datetime.fromisoformat('2026-10-19T16:32:47+08:00')
    data={'MARKET_DATA_AS_OF':'2026-10-19T15:10:00+08:00','UNCHANGED':{'fact':7}}
    base=SimpleNamespace(snapshot_id='base',snapshot_hash=object_hash(data),as_of=at,data=data,proof_chain=())
    delta={'NEWS_HEAT_SNAPSHOT':{'as_of':'2026-10-19T16:35:50+08:00','rows':[]},'A2_STOCK_NEWS_SCOPE':['000001.SZ']}
    recipe={'base_snapshot_hash':base.snapshot_hash,'stage':'A2',**deepcopy(delta)};digest=object_hash(recipe)
    node=dict(schema_version='research-snapshot-recipe/1',status='CAPTURED',recipe_kind='A2_STOCK_NEWS',
        parent_snapshot_id='base',parent_snapshot_hash=base.snapshot_hash,parent_as_of=at.isoformat(),
        snapshot_id=f'base:a2:{digest[:12]}',snapshot_hash=digest,as_of='2026-10-19T16:35:50+08:00',
        hash_recipe=recipe,data_recipe=delta)
    calls=[]
    def fallback(**kwargs):calls.append(kwargs);return 'ORIGINAL_A3'
    def bind(value=node,digest=None):
        return make_a2_stock_news_enricher(proof=BoundObject(value,digest or object_hash(value)),
            prepared_snapshot=base,expected_base_hash=base.snapshot_hash,fallback=fallback,
            wall_clock=lambda:datetime.fromisoformat('2026-10-19T22:20:00+08:00'))
    return base,node,calls,bind


def test_capture_restored_without_mutating_base_node_or_fallback():
    base,node,calls,bind=fixture();before,old_data=deepcopy(node),deepcopy(base.data);callback=bind()
    restored=callback(stage='A2',lane_id='lane_1',model='pro',upstream_symbols=frozenset({'000001.SZ'}),snapshot=base)
    assert restored['as_of'].isoformat()=='2026-10-19T16:35:50+08:00'
    assert restored['data']=={**old_data,**node['data_recipe']} and restored['proof_chain']==(node,)
    assert base.data==old_data and node==before and calls==[]
    assert callback(stage='A3',lane_id='lane_1',model='pro',upstream_symbols=frozenset({'000001.SZ'}),snapshot=base)=='ORIGINAL_A3'
    assert len(calls)==1 and calls[0]['snapshot'] is base


@pytest.mark.parametrize('mutation',[
    lambda n:n.update(parent_snapshot_id='other'),lambda n:n.update(parent_snapshot_hash='0'*64),
    lambda n:n.update(parent_as_of='2026-10-19T16:32:48+08:00'),lambda n:n.update(snapshot_id='forged'),
    lambda n:n.update(snapshot_hash='0'*64),lambda n:n['data_recipe'].update(TRADE_PERMISSION='ALLOW'),
    lambda n:n['hash_recipe'].update(TRADE_PERMISSION='ALLOW'),
    lambda n:n['data_recipe']['NEWS_HEAT_SNAPSHOT'].update(rows=['forged']),
    lambda n:n.update(as_of='2026-10-20T00:01:00+08:00'),lambda n:n.update(as_of='2026-10-19T16:00:00+08:00'),
    lambda n:n.update(as_of='2026-10-19T16:35:50')])
def test_tampered_node_cannot_create_pipeline_callback(mutation):
    _,node,_,bind=fixture();mutation(node)
    with pytest.raises((ValueError,TypeError)):bind()


def test_bound_hash_and_actual_runtime_scope_checked_separately():
    base,_,_,bind=fixture()
    with pytest.raises(ValueError,match='NODE_OBJECT_HASH_CONFLICT'):bind(digest='0'*64)
    with pytest.raises(ValueError,match='SCOPE_CONFLICT'):
        bind()(stage='A2',lane_id='lane_1',model='pro',upstream_symbols=frozenset({'000002.SZ'}),snapshot=base)


def flags():
    return dict(normalized_slot='close',from_active_a1=True,primary_only=True,reuse_completed_snapshot=True,
        reuse_resume_snapshot=True,run_id_override='isolation-fresh',snapshot_expected_hash='a'*64,
        historical_replay=False,comparison_run=False,schedule_comparison=False,auction_refresh=False,
        same_day_recovery=False,target_trade_date=None,allow_non_trading_source=False,market_data_as_of=None,
        snapshot_id=None,models=None,primary_lane_ids=None,publish_plans=False,record_runtime=False)


@pytest.mark.parametrize('key,value',[
    ('normalized_slot','morning'),('from_active_a1',False),('primary_only',False),('reuse_completed_snapshot',False),
    ('reuse_resume_snapshot',False),('run_id_override',None),('run_id_override','../unsafe'),('snapshot_expected_hash','bad'),
    ('historical_replay',True),('comparison_run',True),('schedule_comparison',True),('auction_refresh',True),
    ('same_day_recovery',True),('target_trade_date','2026-10-20'),('allow_non_trading_source',True),
    ('market_data_as_of','2026-10-19'),('snapshot_id','other'),('models',('new-model',)),('primary_lane_ids',('lane_1',)),
    ('publish_plans',True),('record_runtime',True)])
def test_bad_flags_reject_before_any_resource_progress_registry_or_model(key,value):
    from liangjian_funnel.workflow import WorkflowApplication
    args=flags();args[key]=value;slot=args.pop('normalized_slot')
    # A bare object has no settings/store/resource hooks: passing these guards
    # therefore proves rejection happened before those side effects.
    with pytest.raises(ValueError,match='ARGUMENTS_INVALID'):
        WorkflowApplication.run_research(SimpleNamespace(),slot,frozen_a2_stock_news=object(),**args)


def test_run_local_callbacks_do_not_leak_between_runs():
    base,node,_,bind=fixture();one=bind();other=deepcopy(node)
    other['data_recipe']['NEWS_HEAT_SNAPSHOT']['rows']=['separate']
    other['hash_recipe']['NEWS_HEAT_SNAPSHOT']['rows']=['separate']
    other['snapshot_hash']=object_hash(other['hash_recipe']);other['snapshot_id']='base:a2:'+other['snapshot_hash'][:12]
    two=bind(other)
    args=dict(stage='A2',lane_id='lane_1',model='pro',upstream_symbols={'000001.SZ'},snapshot=base)
    assert one(**args)['data']['NEWS_HEAT_SNAPSHOT']['rows']==[]
    assert two(**args)['data']['NEWS_HEAT_SNAPSHOT']['rows']==['separate']
    assert one(**args)['data']['NEWS_HEAT_SNAPSHOT']['rows']==[]


class PipelineBoundaryReached(Exception):pass


def workflow_fixture(tmp_path,monkeypatch):
    import liangjian_funnel.workflow as workflow
    from liangjian_funnel.settings import Settings
    base,node,calls,bind=fixture()
    app=workflow.WorkflowApplication.__new__(workflow.WorkflowApplication)
    app.settings=Settings.from_env({},root=tmp_path)
    app.settings.source_config_path.parent.mkdir(parents=True,exist_ok=True)
    app.settings.source_config_path.write_text('agent_2: {}\n',encoding='utf-8')
    app._ensure_trading_day=Mock()
    app.a1_registry=SimpleNamespace(require_active=Mock(return_value=SimpleNamespace(
        as_of=base.as_of,payload={},generation_id='existing-a1')))
    app._load_research_resume_snapshot=Mock(return_value=SimpleNamespace(snapshot=base))
    app.prepare_snapshot=Mock(side_effect=AssertionError('No fresh source'))
    app._stage_snapshot_enricher=Mock(return_value='NATURAL_STAGE')
    app.prompts=object();app.model_client=object();app.research_checkpoints=object()
    captured=[]
    def factory(**kwargs):captured.append(kwargs);raise PipelineBoundaryReached()
    app._new_research_pipeline=factory
    snapshot=SimpleNamespace(as_dict=lambda:{})
    monkeypatch.setattr(workflow,'evaluate_resources',lambda *_:SimpleNamespace(allowed=True,snapshot=snapshot))
    monkeypatch.setattr(workflow,'measure_resources',lambda *_:snapshot)
    monkeypatch.setattr(workflow,'_active_a1_downstream_scope',lambda *_:('000001.SZ',))
    observer=Mock();monkeypatch.setattr(workflow,'_CloseResourceObservation',lambda *_:observer)
    args=flags();args.pop('normalized_slot');args.update(as_of=base.as_of,snapshot_expected_hash=base.snapshot_hash)
    return app,base,node,captured,args,observer


def test_actual_workflow_default_none_callback_identity_and_isolation_only_binding(tmp_path,monkeypatch):
    from liangjian_funnel.workflow import WorkflowApplication
    app,base,node,captured,args,observer=workflow_fixture(tmp_path,monkeypatch)
    for kwargs in ({},{'frozen_a2_stock_news':None}):
        with pytest.raises(PipelineBoundaryReached):WorkflowApplication.run_research(app,'close',**args,**kwargs)
        assert captured[-1]['stage_snapshot_enricher'] is app._stage_snapshot_enricher
        assert captured[-1]['runtime_store'] is None
    app._ensure_trading_day.assert_called_with(base.as_of,synchronize_accounts=True)
    natural_callback=app._stage_snapshot_enricher
    observer.reset_mock()
    with pytest.raises(PipelineBoundaryReached):
        WorkflowApplication.run_research(app,'close',**args,frozen_a2_stock_news=BoundObject(node,object_hash(node)))
    observer.start.assert_not_called()
    assert app._stage_snapshot_enricher is natural_callback
    callback=captured[-1]['stage_snapshot_enricher'];assert callback is not natural_callback
    app._ensure_trading_day.assert_called_with(base.as_of,synchronize_accounts=False)
    out=callback(stage='A2',lane_id='lane_1',model='pro',upstream_symbols={'000001.SZ'},snapshot=base)
    assert out['data']['NEWS_HEAT_SNAPSHOT']==node['data_recipe']['NEWS_HEAT_SNAPSHOT']
    assert out['as_of'].isoformat()==node['as_of']
    assert callback(stage='A3',lane_id='lane_1',model='pro',upstream_symbols={'000001.SZ'},snapshot=base)=='NATURAL_STAGE'
    app.prepare_snapshot.assert_not_called()


@pytest.mark.parametrize('failure',['missing','base_sha','existing_run'])
def test_overlay_cannot_fresh_fetch_change_base_or_reuse_old_run(tmp_path,monkeypatch,failure):
    from liangjian_funnel.workflow import WorkflowApplication,WorkflowError
    app,base,node,captured,args,_=workflow_fixture(tmp_path,monkeypatch)
    if failure=='missing':app._load_research_resume_snapshot.return_value=None
    elif failure=='base_sha':args['snapshot_expected_hash']='0'*64
    else:
        path=app.settings.workflow_output_dir/'runs'/f'{args["run_id_override"]}.json'
        path.parent.mkdir(parents=True);path.write_text('{}')
    with pytest.raises(WorkflowError):
        WorkflowApplication.run_research(app,'close',**args,frozen_a2_stock_news=BoundObject(node,object_hash(node)))
    assert captured==[];app.prepare_snapshot.assert_not_called()
