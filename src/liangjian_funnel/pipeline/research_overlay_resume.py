"""Run-local, isolation-only restoration of an already frozen A2 news node.

BoundObject proves byte integrity, not historical origin. The caller must pin
the original audit SHA/selector/source HEAD in its separate operation receipt.
No CLI, model/source access, publication, runtime DB or instance mutation.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
import re
from zoneinfo import ZoneInfo

from ..runtime.publication_receipt import BoundObject,PublicationReceiptBuilder,object_hash

SHANGHAI=ZoneInfo('Asia/Shanghai')
NODE_FIELDS=frozenset(('schema_version','status','recipe_kind','parent_snapshot_id',
    'parent_snapshot_hash','parent_as_of','snapshot_id','snapshot_hash','as_of','hash_recipe','data_recipe'))
DELTA_FIELDS=frozenset(('NEWS_HEAT_SNAPSHOT','A2_STOCK_NEWS_SCOPE'))


def _reject(reason):raise ValueError('A2_NEWS_RESUME_'+reason)


def _aware(value):
    result=value if isinstance(value,datetime) else datetime.fromisoformat(value)
    if result.utcoffset() is None:_reject('NAIVE_AS_OF')
    return result.astimezone(SHANGHAI)


def require_resume_arguments(*,normalized_slot,from_active_a1,primary_only,reuse_completed_snapshot,
        reuse_resume_snapshot,run_id_override,snapshot_expected_hash,historical_replay,comparison_run,
        schedule_comparison,auction_refresh,same_day_recovery,target_trade_date,allow_non_trading_source,
        market_data_as_of,snapshot_id,models,primary_lane_ids,publish_plans,record_runtime):
    if (normalized_slot!='close' or from_active_a1 is not True or primary_only is not True
            or reuse_completed_snapshot is not True or reuse_resume_snapshot is not True
            or not isinstance(run_id_override,str) or re.fullmatch(r'[A-Za-z0-9_.-]+',run_id_override) is None
            or not isinstance(snapshot_expected_hash,str) or re.fullmatch('[0-9a-f]{64}',snapshot_expected_hash) is None
            or historical_replay or comparison_run or schedule_comparison or auction_refresh or same_day_recovery
            or target_trade_date is not None or allow_non_trading_source or market_data_as_of is not None
            or snapshot_id is not None or models is not None or primary_lane_ids is not None
            or publish_plans is not False or record_runtime is not False):
        _reject('ARGUMENTS_INVALID')


def make_a2_stock_news_enricher(*,proof,prepared_snapshot,expected_base_hash,fallback,
        wall_clock=lambda:datetime.now(SHANGHAI)):
    if (not isinstance(proof,BoundObject) or not isinstance(proof.value,Mapping)
            or not isinstance(proof.sha256,str) or re.fullmatch('[0-9a-f]{64}',proof.sha256) is None
            or object_hash(proof.value)!=proof.sha256):_reject('NODE_OBJECT_HASH_CONFLICT')
    if prepared_snapshot.snapshot_hash!=expected_base_hash or not isinstance(prepared_snapshot.data,Mapping):
        _reject('BASE_BINDING_CONFLICT')
    node=deepcopy(dict(proof.value))
    if (set(node)!=NODE_FIELDS or node['schema_version']!='research-snapshot-recipe/1'
            or node['status']!='CAPTURED' or node['recipe_kind']!='A2_STOCK_NEWS'):_reject('NODE_CONTRACT_INVALID')

    def restore(snapshot,upstream_symbols):
        if (snapshot.snapshot_id!=prepared_snapshot.snapshot_id or snapshot.snapshot_hash!=expected_base_hash
                or node['parent_snapshot_id']!=snapshot.snapshot_id or node['parent_snapshot_hash']!=snapshot.snapshot_hash
                or node['parent_as_of']!=_aware(snapshot.as_of).isoformat()):_reject('PARENT_CONFLICT')
        recipe,delta=node['hash_recipe'],node['data_recipe']
        if (not isinstance(recipe,Mapping) or set(recipe)!={'base_snapshot_hash','stage',*DELTA_FIELDS}
                or recipe['stage']!='A2' or recipe['base_snapshot_hash']!=snapshot.snapshot_hash
                or not isinstance(delta,Mapping) or set(delta)!=DELTA_FIELDS
                or not isinstance(delta['NEWS_HEAT_SNAPSHOT'],Mapping)
                or object_hash(delta)!=object_hash({key:recipe[key] for key in DELTA_FIELDS})):_reject('RECIPE_CONFLICT')
        scope=delta['A2_STOCK_NEWS_SCOPE']
        if (not isinstance(scope,list) or not scope or any(not isinstance(s,str) or not s for s in scope)
                or scope!=sorted(set(scope)) or set(scope)!=set(upstream_symbols)):_reject('SCOPE_CONFLICT')
        digest=object_hash(recipe)
        if node['snapshot_hash']!=digest or node['snapshot_id']!=f'{snapshot.snapshot_id}:a2:{digest[:12]}':
            _reject('DERIVED_HASH_CONFLICT')
        captured,parent,now=_aware(node['as_of']),_aware(snapshot.as_of),_aware(wall_clock())
        if captured<parent or captured>now or captured.date()!=parent.date():_reject('AS_OF_CONFLICT')
        reasons=set();base=dict(snapshot_id=snapshot.snapshot_id,snapshot_hash=snapshot.snapshot_hash,
            as_of=parent.isoformat(),data=snapshot.data)
        expected,tag=PublicationReceiptBuilder._snapshot_transform('A2_STOCK_NEWS',base,deepcopy(recipe),reasons)
        if reasons or expected is None or tag!='a2':_reject('TRANSFORM_CONFLICT')
        return dict(snapshot_id=node['snapshot_id'],snapshot_hash=digest,as_of=captured,data=expected,
            proof_chain=(*getattr(snapshot,'proof_chain',()),deepcopy(node)))

    restore(prepared_snapshot,node['data_recipe'].get('A2_STOCK_NEWS_SCOPE',())
        if isinstance(node['data_recipe'],Mapping) else ())
    def enrich(*,stage,lane_id,model,upstream_symbols,snapshot):
        if stage=='A2':return restore(snapshot,upstream_symbols)
        return fallback(stage=stage,lane_id=lane_id,model=model,upstream_symbols=upstream_symbols,snapshot=snapshot)
    return enrich
