"""Local-only byte and original canonical content verification of the copy."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo
from run_team_a5_frozen_dryrun import digest


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);a=p.parse_args()
    receipt=json.loads((a.directory/'RECEIPT.json').read_bytes());results=[]
    for row in receipt['files']:
        if row['kind']=='missing':results.append(row);continue
        path=a.directory/'files'/row['relative']
        assert path.stat().st_size==row['bytes']
        actual=hashlib.file_digest(path.open('rb'),'sha256').hexdigest();assert actual==row['sha256']
        result=dict(relative=row['relative'],bytes=row['bytes'],sha256=actual)
        if row['source_kind']=='snapshot':
            snapshot=json.loads(path.read_bytes());computed=digest(snapshot['data'])
            assert computed==snapshot['snapshot_hash']
            assert snapshot['snapshot_id'] in path.name and snapshot['as_of'][:10]==row['trade_date']
            result.update(snapshot_id=snapshot['snapshot_id'],snapshot_sha256=computed,canonical_verified=True)
        elif row['source_kind']=='a5_original':
            source=json.loads(path.read_bytes());facts=source['facts']
            computed=digest({k:v for k,v in facts.items() if k!='input_hash'})
            assert computed==source['input_hash']==facts['input_hash'] and facts['trade_date']==row['trade_date']
            result.update(fact_input_sha256=computed,canonical_verified=True)
        results.append(result)
    body=dict(actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),status='PASS',files=results,
        scope='ORIGINAL_INPUT_COPY_ONLY_NOT_MODEL_OR_CONTROL_REPLAY',production_writes=0,models=0,providers=0)
    with (a.directory/'LOCAL_HASH_VERIFICATION.json').open('x',encoding='utf-8') as f:json.dump(body,f,indent=2)
    print(json.dumps(dict(status='PASS',files=len(results),canonical_verified=sum(r.get('canonical_verified',False) for r in results),actual_at=body['actual_at'])))


if __name__=='__main__':main()
