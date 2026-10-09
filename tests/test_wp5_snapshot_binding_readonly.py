from copy import deepcopy
from datetime import datetime
import importlib.util
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.snapshot import UniverseSnapshot, FrozenInputSnapshot
from liangjian_funnel.facts.contracts import FactSnapshotManifest

spec=importlib.util.spec_from_file_location('wp5_binding',Path(__file__).parents[1]/'scripts/audit_snapshot_binding_readonly.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def inputs():
    now=datetime(2026,10,8,16,33,tzinfo=ZoneInfo('Asia/Shanghai'))
    universe=UniverseSnapshot.from_records([{'thscode':'600000.SH','name':'fixture','price':10,'amount':1e8}],as_of=now)
    frozen=FrozenInputSnapshot.freeze(universe,as_of=now,daily_payload={'600000.SH':[{'close_price':10}]},
        fundamental_payload={'600000.SH':{'available':True}},max_candidates=1)
    raw=frozen.model_dump(mode='json')
    data={'snapshot_manifest':{'raw_snapshot_hash':frozen.snapshot_hash,
                              'raw_snapshot_path':'/isolated/'+frozen.snapshot_id+'.json'}}
    projection={'snapshot_id':'projection-fixture','snapshot_hash':module.canonical_hash(data),'data':data}
    output={'active_research_pool':[{'symbol':'600000.SH'}]}
    lane={'stages':[{'stage':'A1','snapshot_id':projection['snapshot_id'],'output':output,
                     'output_hash':module.canonical_hash(output)},
        {'stage':'A2','snapshot_id':'projection-fixture:a2:test','input_hash':'i','prompt_hash':'p','output':None}]}
    attempt={'snapshot_id':'projection-fixture:a2:test','input_hash':'i','prompt_hash':'p','snapshot_hash':'a2hash'}
    attempt['record_hash']=module.canonical_hash(attempt)
    return projection,raw,lane,attempt


def test_verified_raw_and_attempt_do_not_prove_unpreserved_overlay():
    value=module.verify_binding(*inputs())
    assert value['raw_canonical_hash_verified'] and value['a2_attempt_binding_verified']
    assert value['status']=='DATA_LIMITED' and not value['execution_authority']
    assert 'ORIGINAL_A2_NEWS_AND_BOTTLENECK_OVERLAY_PAYLOAD_NOT_VERIFIED' in value['limitations']


@pytest.mark.parametrize('fault',['projection','raw','path','lane','output','attempt','attempt_input'])
def test_any_tampered_binding_cannot_pass(fault):
    p,r,l,a=deepcopy(inputs())
    if fault=='projection':
        p['data']['changed']=True
    elif fault=='raw':
        r['daily_payload']['600000.SH'][0]['close_price']=100
    elif fault=='path':
        p['data']['snapshot_manifest']['raw_snapshot_path']='/other/bad.json'
        p['snapshot_hash']=module.canonical_hash(p['data'])
    elif fault=='lane':
        l['stages'][0]['snapshot_id']='other'
    elif fault=='output':
        l['stages'][0]['output']['active_research_pool']=[]
    elif fault=='attempt':
        a['input_hash']='other'
    else:
        a['input_hash']='other'
        a['record_hash']=module.canonical_hash({k:v for k,v in a.items() if k!='record_hash'})
    with pytest.raises(ValueError):
        module.verify_binding(p,r,l,a)


def fact_inputs():
    now=datetime(2026,10,8,16,33,tzinfo=ZoneInfo('Asia/Shanghai'))
    manifest=FactSnapshotManifest(snapshot_id='merged-fixture',as_of=now)
    facts=manifest.model_dump(mode='json')
    file_sha='f'*64
    refs={'snapshot_id':'merged-fixture','manifest_hash':file_sha,
          'store_relative_path':'snapshots/merged-fixture.json',
          'facts_sha256':manifest.facts_sha256}
    p={'as_of':now.isoformat(),'data':{'snapshot_manifest':{'fact_snapshot_id':'merged-fixture',
        'fact_manifest_hash':file_sha,'fact_store_relative_path':'snapshots/merged-fixture.json'}}}
    raw={'as_of':now.isoformat(),'fact_payload':refs}
    return p,raw,facts,file_sha


def test_manifest_internal_hash_and_both_references_are_required():
    proof=module.verify_fact_binding(*fact_inputs())
    assert proof['fact_manifest_hash_verified']
    assert proof['fact_internal_canonical_hash_verified']
    assert proof['fact_count']==0


@pytest.mark.parametrize('fault',['file_hash','raw_ref','projection_ref','path','internal_hash','future'])
def test_fact_pack_faults_never_gain_lineage_authority(fault):
    p,r,f,h=deepcopy(fact_inputs())
    if fault=='file_hash':
        h='a'*64
    elif fault=='raw_ref':
        r['fact_payload']['manifest_hash']='a'*64
    elif fault=='projection_ref':
        p['data']['snapshot_manifest']['fact_snapshot_id']='different'
    elif fault=='path':
        r['fact_payload']['store_relative_path']='snapshots/other.json'
    elif fault=='internal_hash':
        f['facts_sha256']='a'*64
    else:
        f['as_of']='2026-10-09T16:33:00+08:00'
    with pytest.raises(ValueError):
        module.verify_fact_binding(p,r,f,h)
