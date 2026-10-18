"""Derive G2 daily diagnostics from already-copied canonical A5 originals.

No store construction, models, providers or historical database scans.
Three original days do not prove the requested twenty-day coverage.
"""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.review.portfolio_diagnostics import strategy_portfolio_diagnostics


def digest(value):
    raw=json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sources',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();output=args.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('NEW_OWNED_FILE_REQUIRED')
    audit_path=args.sources/'LOCAL_HASH_VERIFICATION.json'
    audit=json.loads(audit_path.read_bytes())
    if audit.get('status')!='PASS' or sum(r.get('canonical_verified',False) for r in audit['files'])!=6:
        raise ValueError('ORIGINAL_SOURCE_NOT_CANONICAL_VERIFIED')
    export=json.loads((args.sources/'RECEIPT.json').read_bytes());results=[]
    for day in ('2026-10-14','2026-10-15','2026-10-16'):
        relative='a5-original/'+day+'.json';path=args.sources/'files'/relative
        proof=[r for r in export['files'] if r.get('relative')==relative]
        raw=path.read_bytes();file_hash=hashlib.sha256(raw).hexdigest()
        if len(proof)!=1 or file_hash!=proof[0]['sha256']:raise ValueError('ORIGINAL_FILE_CHANGED')
        original=json.loads(raw);facts=original['facts']
        if facts['trade_date']!=day or facts['input_hash']!=original['input_hash'] or digest(
                {k:v for k,v in facts.items() if k!='input_hash'})!=facts['input_hash']:
            raise ValueError('ORIGINAL_FACT_HASH_INVALID')
        results.append(dict(trade_date=day,original_file_sha256=file_hash,
            original_fact_input_sha256=facts['input_hash'],diagnostics=strategy_portfolio_diagnostics(facts)))
        if hashlib.sha256(path.read_bytes()).hexdigest()!=file_hash:raise ValueError('ORIGINAL_CHANGED_DURING_AUDIT')
    result=dict(actual_collected_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        status='THREE_ORIGINAL_DAYS_DERIVED_NOT_TWENTY_DAY_COMPLETE',days=results,
        original_verification_sha256=hashlib.sha256(audit_path.read_bytes()).hexdigest(),
        models=0,provider_calls=0,production_writes=0,natural_team_a5_acceptance=False)
    with output.open('x',encoding='utf-8') as stream:json.dump(result,stream,ensure_ascii=False,indent=2)
    print(json.dumps(result,ensure_ascii=False));return 0


if __name__=='__main__':sys.exit(main())
