"""Verify explicit local raw/projection/lane bindings, never reconstruct overlays."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

from liangjian_funnel.pipeline.snapshot import FrozenInputSnapshot
from liangjian_funnel.facts.contracts import FactSnapshotManifest


def canonical_hash(value):
    digest = hashlib.sha256()
    for part in json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).iterencode(value):
        digest.update(part.encode('utf-8'))
    return digest.hexdigest()


def verify_binding(projection, raw, lane, attempt=None):
    data = projection['data']
    if canonical_hash(data) != projection['snapshot_hash']:
        raise ValueError('PROJECTION_CANONICAL_HASH_MISMATCH')
    frozen = FrozenInputSnapshot.model_validate(raw)
    manifest = data['snapshot_manifest']
    if (manifest['raw_snapshot_hash'] != frozen.snapshot_hash
            or Path(manifest['raw_snapshot_path']).name != frozen.snapshot_id+'.json'):
        raise ValueError('RAW_SNAPSHOT_BINDING_MISMATCH')
    stages = {s['stage']:s for s in lane['stages']}
    if len(stages) != len(lane['stages']):
        raise ValueError('DUPLICATE_LANE_STAGE')
    a1 = stages['A1']
    if a1['snapshot_id'] != projection['snapshot_id']:
        raise ValueError('LANE_BASE_SNAPSHOT_MISMATCH')
    for stage in stages.values():
        if stage.get('output') is not None and canonical_hash(stage['output']) != stage.get('output_hash'):
            raise ValueError('LANE_OUTPUT_HASH_MISMATCH')
    result = {'schema_version':'snapshot-binding-readonly/1','status':'DATA_LIMITED',
        'projection_hash_verified':True,'raw_canonical_hash_verified':True,
        'lane_base_and_outputs_verified':True,'snapshot_id':projection['snapshot_id'],
        'projection_hash':projection['snapshot_hash'],'raw_snapshot_hash':frozen.snapshot_hash,
        'raw_snapshot_id':frozen.snapshot_id,'a1_output_hash':a1['output_hash'],
        'a2_attempt_binding_verified':False,
        'limitations':['ORIGINAL_A2_NEWS_AND_BOTTLENECK_OVERLAY_PAYLOAD_NOT_VERIFIED',
            'ORIGINAL_RUNTIME_PARAMETERS_NOT_VERIFIED',
            'ORIGINAL_PRE_ANNOUNCEMENT_PREFILTER_RECEIPT_NOT_CREATED_BY_OLD_VERSION'],
        'execution_authority':False,'source_mutation':False}
    if attempt is not None:
        if canonical_hash({k:v for k,v in attempt.items() if k != 'record_hash'}) != attempt.get('record_hash'):
            raise ValueError('A2_ATTEMPT_HASH_MISMATCH')
        a2 = stages['A2']
        if any(attempt.get(key) != a2.get(key) for key in ('snapshot_id','input_hash','prompt_hash')):
            raise ValueError('A2_ATTEMPT_STAGE_BINDING_MISMATCH')
        if not attempt['snapshot_id'].startswith(projection['snapshot_id']+':a2:'):
            raise ValueError('A2_ATTEMPT_BASE_BINDING_MISMATCH')
        result.update(a2_attempt_binding_verified=True,a2_attempt_hash=attempt['record_hash'],
            a2_input_hash=a2['input_hash'],a2_overlay_hash=attempt['snapshot_hash'])
    return result


def verify_fact_binding(projection, raw, facts, file_sha):
    """Bind the copied manifest bytes AND its internal canonical fact hashes."""
    manifest=FactSnapshotManifest.model_validate(facts)
    p=projection['data']['snapshot_manifest']
    r=raw['fact_payload']
    expected_path='snapshots/'+manifest.snapshot_id+'.json'
    if (p.get('fact_snapshot_id') != manifest.snapshot_id
            or r.get('snapshot_id') != manifest.snapshot_id
            or p.get('fact_manifest_hash') != file_sha
            or r.get('manifest_hash') != file_sha
            or p.get('fact_store_relative_path') != expected_path
            or r.get('store_relative_path') != expected_path
            or r.get('facts_sha256') != manifest.facts_sha256):
        raise ValueError('FACT_MANIFEST_BINDING_MISMATCH')
    for cutoff in (projection['as_of'],raw['as_of']):
        moment=datetime.fromisoformat(cutoff)
        if moment.tzinfo is None or manifest.as_of > moment:
            raise ValueError('FACT_MANIFEST_TIME_BINDING_MISMATCH')
    return {'fact_manifest_hash_verified':True,
        'fact_internal_canonical_hash_verified':True,
        'fact_manifest_id':manifest.snapshot_id,'fact_count':len(manifest.facts),
        'facts_canonical_hash':manifest.facts_sha256}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('projection','raw','lane','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--attempt',type=Path)
    parser.add_argument('--facts',type=Path)
    args=parser.parse_args(argv)
    paths=[args.projection,args.raw,args.lane]+([args.attempt] if args.attempt else [])+([args.facts] if args.facts else [])
    if args.output.exists() or args.output.resolve() in {p.resolve() for p in paths}:
        raise ValueError('REFUSE_OVERWRITE')
    hashes=[]
    values=[]
    for path in paths:
        with path.open('rb') as stream:
            hashes.append(hashlib.file_digest(stream,'sha256').hexdigest())
        with path.open(encoding='utf-8') as stream:
            values.append(json.load(stream))
    proof=verify_binding(*values[:3],values[3] if args.attempt else None)
    if args.facts:
        proof.update(verify_fact_binding(values[0],values[1],values[-1],hashes[-1]))
    proof['input_files']=[{'path':str(path.resolve()),'sha256':sha} for path,sha in zip(paths,hashes)]
    for path,sha in zip(paths,hashes):
        with path.open('rb') as stream:
            if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:
                raise ValueError('INPUT_CHANGED_DURING_AUDIT')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x',encoding='utf-8') as stream:
        json.dump(proof,stream,ensure_ascii=False,indent=2)
    print(json.dumps({key:proof[key] for key in ('status','raw_canonical_hash_verified','a2_attempt_binding_verified')}))
    return 2  # partial lineage must never authorize scope promotion


if __name__=='__main__':
    raise SystemExit(main())
