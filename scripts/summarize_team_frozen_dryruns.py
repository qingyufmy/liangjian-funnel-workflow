"""Actual journal-only latency/validation summary and scoped secret-byte audit."""
from contextlib import closing
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from run_team_a5_frozen_dryrun import model_settings,save
from liangjian_funnel.settings import load_dotenv


def main():
    base=ROOT/'artifacts/team-v2-20261016';now=datetime.now(ZoneInfo('Asia/Shanghai'))
    directories=sorted(p for prefix in ('a5-frozen-','a2-frozen-','a4-frozen-','dryrun-original-sources-','dryrun-source-inventory-')
        for p in base.glob(prefix+'*') if p.is_dir())
    rows={};failures=[]
    for directory in directories:
        for database in (directory/'model.db',directory/'state/team_v2/model.db'):
            if not database.is_file():continue
            with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
                db.execute('PRAGMA query_only=ON');db.row_factory=sqlite3.Row
                for row in db.execute('SELECT call_id,role,session,captured_at,latency_ms,provider_attempts,validated,prompt_hash,input_hash,output_hash FROM model_calls'):
                    if row['call_id'] in rows and dict(row)!=rows[row['call_id']]:raise ValueError('DRYRUN_CALL_ID_CONFLICT')
                    rows[row['call_id']]=dict(row)
        result=directory/'RESULT.json'
        if result.is_file():
            body=json.loads(result.read_bytes())
            if body.get('status')=='MODEL_CALL_FAILED':
                failures.append(dict(directory=directory.name,reason_code=body['reason_code'],actual_at=body['actual_finished_at']))
    sessions={('A1','POLICY_MACRO_DISCOVERY'):600,('A1','COMPANY_MAPPING'):600,
        ('A2','CLOSE_REVIEW'):600,('A3','PLAN_REVIEW'):600,('A3','PM_PREMARKET'):120,('A3','PM_POST_CLOSE'):50,
        ('A4','INTRADAY'):30,('A5','POST_CLOSE'):180,('A6','PREMARKET'):120,('A6','INTRADAY_EXCEPTION'):20,('A6','POST_CLOSE'):50}
    summary=[]
    for (role,session),budget in sessions.items():
        actual=[r for r in rows.values() if (r['role'],r['session'])==(role,session)]
        times=sorted(r['latency_ms']/1000 for r in actual)
        summary.append(dict(role=role,session=session,sample_count=len(actual),validated=sum(bool(r['validated']) for r in actual),
            p50_seconds=statistics.median(times) if times else None,p95_seconds=times[math.ceil(.95*len(times))-1] if times else None,
            maximum_seconds=max(times) if times else None,budget_seconds=budget,over_budget=sum(t>budget for t in times)))
    # Read only the configured provider credential values into this process.
    # Never store values/hashes/lengths, and never echo a matching byte string.
    current,_=model_settings(base,'vm-runtime')
    old=load_dotenv(Path('D:/dev_A股/liangjian_funnel_workflow/.env')).get('LIANGJIAN_MODEL_API_KEY')
    needles={v.encode() for v in (current.model_api_key.get_secret_value(),old) if v}
    overlap=max(map(len,needles))-1;scanned=0;bytes_scanned=0;hits=[]
    for directory in directories:
        for path in sorted(directory.rglob('*')):
            if not path.is_file() or path.is_symlink():continue
            scanned+=1;tail=b''
            with path.open('rb') as stream:
                for chunk in iter(lambda:stream.read(1024*1024),b''):
                    bytes_scanned+=len(chunk);check=tail+chunk
                    if any(needle in check for needle in needles):hits.append(str(path.relative_to(ROOT)));break
                    tail=check[-overlap:] if overlap else b''
    audit=dict(status='PASS' if not hits else 'FAILED',credential_storage='PROCESS_MEMORY_ONLY',exact_known_credential_scan=True,
        scope=[str(p.relative_to(ROOT)) for p in directories],files_scanned=scanned,bytes_scanned=bytes_scanned,
        matching_files=hits,not_claimed='NO_GLOBAL_MACHINE_OR_UNKNOWN_CREDENTIAL_SCAN')
    body=dict(actual_at=now.isoformat(),completed_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        actual_response_count=len(rows),validated_responses=sum(bool(r['validated']) for r in rows.values()),
        provider_failed_requests=failures,calls=list(rows.values()),role_session_latencies=summary,
        p50_method='MEDIAN',p95_method='NEAREST_RANK',small_sample_warning='N=0_IS_UNOBSERVED_NOT_ZERO;INVALID_RESPONSES_INCLUDED',
        credential_audit=audit,production_writes=0,model_calls=0,release_gate='NOT_PASSED')
    path=base/(now.strftime('%Y%m%dT%H%M%S%f')+'-REAL_DRYRUN_SUMMARY.json');save(path,body)
    print(json.dumps(dict(path=str(path.relative_to(ROOT)),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        actual_responses=len(rows),validated=body['validated_responses'],failed_provider_requests=len(failures),
        credential_audit_status=audit['status'],scanned_files=scanned,models=0,production_writes=0)))
    return 0 if not hits else 2


if __name__=='__main__':sys.exit(main())
