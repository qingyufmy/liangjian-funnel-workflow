"""Verify the original supplement row-for-row; no strategy/model execution."""
from collections import Counter,defaultdict
from contextlib import closing
from datetime import datetime
import base64
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'artifacts/team-v2-20261016/source-1016-original-20261017T0740'
PIN='2168d8bd09f3f22353fa5b9430935a73b9ea103a787c8c566aee453c81960ed0'


def hash_file(path):
    with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)


def main():
    archive=SOURCE/'source.jsonl.gz';database=SOURCE/'frozen.sqlite3'
    assert hash_file(archive)==PIN,'ARCHIVE_PIN_MISMATCH'
    before=hash_file(database);original_hashes={};counts=Counter();file_count=0;complete=False
    with gzip.open(archive,'rt',encoding='utf-8') as stream:
        for line in stream:
            item=json.loads(line)
            if item['kind']=='row':
                name=item['table'];counts[name]+=1
                original_hashes.setdefault(name,hashlib.sha256()).update((canonical(item['row'])+'\n').encode())
            elif item['kind']=='file':
                relative=Path(item['relative'])
                assert not relative.is_absolute() and '..' not in relative.parts
                raw=(SOURCE/'files'/relative).read_bytes()
                assert raw==base64.b64decode(item['data_b64'])
                assert hashlib.sha256(raw).hexdigest()==item['sha256']
                file_count+=1
            elif item['kind']=='complete':complete=True
    assert complete,'ARCHIVE_NOT_COMPLETE'
    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        for name,expected in original_hashes.items():
            assert name.replace('_','').isalnum()
            actual=hashlib.sha256();n=0
            for row in db.execute('SELECT * FROM '+name+' ORDER BY rowid'):
                value={k:({'__b64__':base64.b64encode(v).decode()} if isinstance(v,bytes) else v) for k,v in dict(row).items()}
                actual.update((canonical(value)+'\n').encode());n+=1
            assert n==counts[name] and actual.digest()==expected.digest(),'ROW_COPY_MISMATCH:'+name
        snapshot_count=0;bars_count=0
        for row in db.execute('SELECT payload_sha256,payload_zlib FROM minute_decision_snapshots'):
            raw=zlib.decompress(row['payload_zlib'])
            assert hashlib.sha256(raw).hexdigest()==row['payload_sha256'],'MINUTE_ORIGINAL_HASH_MISMATCH'
            body=json.loads(raw);assert isinstance(body,list)
            snapshot_count+=1;bars_count+=len(body)
        events=Counter();per_plan=defaultdict(set);prices=0;full_quotes=0
        for row in db.execute('SELECT minute_end,action,reason_code,payload_json FROM monitor_events'):
            body=json.loads(row['payload_json']);events[(row['action'],row['reason_code'])]+=1
            if body.get('plan_id'):per_plan[body['plan_id']].add(row['minute_end'])
            strategy=body.get('strategy') or {};prices+=strategy.get('reference_price') is not None
            # Only an original complete quote object would prove these fields;
            # reference_price and a successful transport flag are not a quote.
            for key in ('quote','live_quote','realtime_quote'):
                value=body.get(key)
                full_quotes+=isinstance(value,dict) and {'symbol','price','quote_time','source_id','previous_close'}<=set(value)
        lifecycle=[dict(r) for r in db.execute('SELECT plan_id,symbol,status,exit_reason FROM a4_signal_lifecycles')]
    markets={}
    needed={'limit_up_count','limit_down_count','broken_board_rate','max_consecutive_boards'}
    for directory in sorted((SOURCE/'files/a4_live_market').iterdir()):
        if not directory.is_dir():continue
        files=sorted(directory.glob('*.json'));ready=0;fields=set()
        for path in files:
            body=json.loads(path.read_bytes());fields.update(body)
            ready+=body.get('status')=='READY'
        markets[directory.name]=dict(files=len(files),original_ready_summaries=ready,
            root_fields=sorted(fields),emotion_four_counts_proven=False,
            emotion_replay_status='DATA_BLOCK_ORIGINAL_COUNTS_NOT_ARCHIVED',
            missing_emotion_fields=sorted(needed-fields))
    assert hash_file(database)==before,'SOURCE_DB_CHANGED'
    print(json.dumps(dict(actual_observed_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        source_archive_sha256=PIN,source_database_sha256=before,source_bytes_unchanged=True,
        row_copy_hashes={name:value.hexdigest() for name,value in original_hashes.items()},
        table_counts=dict(counts),original_files_verified=file_count,snapshot_hashes_verified=snapshot_count,
        bars_inside_snapshots=bars_count,event_distribution=[dict(action=k[0],reason_code=k[1],count=v) for k,v in sorted(events.items())],
        plan_minute_coverage={k:len(v) for k,v in sorted(per_plan.items())},original_lifecycle=lifecycle,
        reference_price_scalars=prices,full_quote_objects=full_quotes,market_days=markets,
        purpose='ORIGINAL_SOURCE_VERIFICATION_NOT_TEAM_REPLAY_OR_COUNTERFACTUAL_REPAIR',
        production_writes=0,provider_calls=0,model_calls=0),ensure_ascii=True,sort_keys=True))


if __name__=='__main__':main()
