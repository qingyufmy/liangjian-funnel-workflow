"""Offline date/ref comparisons, not a new policy or model-quality gate."""
import argparse,hashlib,json,sqlite3,subprocess,sys
from contextlib import closing
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
from run_team_a5_frozen_dryrun import save


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-audit',type=Path,required=True)
    p.add_argument('--response-db',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    out=a.output.resolve()
    if out.exists() or not out.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    source=json.loads(a.source_audit.read_bytes());index={}
    for row in source['dated_records']:
        refs=set(row.get('source_refs') or [])|{row[k] for k in ('source_url','fact_id') if isinstance(row.get(k),str)}
        for ref in refs:index.setdefault(ref,[]).append(row)
    with closing(sqlite3.connect(a.response_db.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        row=db.execute("SELECT output_json,output_hash,source_trade_date FROM model_calls WHERE role='A1' AND session='POLICY_MACRO_DISCOVERY'").fetchone()
    if row is None:raise ValueError('A1_CAPTURE_MISSING')
    output=json.loads(row[0]);results=[];themes=output.get('structural_themes',[])
    if row[2]!=source['source_proofs']['snapshot']['trade_date']:raise ValueError('A1_AUDIT_SOURCE_DATE_MISMATCH')
    def compare(path,ref,value):
        matches=index.get(ref,[]);dates=sorted({d[:len(value)] for r in matches for d in r['dates'].values()}) if isinstance(value,str) else []
        status='UNVERIFIED_DATE' if value=='UNKNOWN' else 'MATCH_ORIGINAL_DATE' if value in dates else 'DATE_NOT_BOUND_TO_ORIGINAL_REF'
        return dict(path=path,source_ref=ref,model_date=value,original_dates=dates,status=status,
            original_paths=[r['path'] for r in matches])
    for theme in themes:
        for i,catalyst in enumerate(theme.get('catalysts',[])):
            results.append(compare('theme:'+theme['theme_id']+':'+str(i),catalyst.get('source_ref'),catalyst.get('expected_date')))
    for i,calendar in enumerate(output.get('policy_calendar',[])):
        results.append(compare('calendar:'+str(i),calendar.get('source_ref'),calendar.get('date')))
    gaps=sum(any(c.get('event')=='NO_VERIFIED_CATALYST' for c in t.get('catalysts',[])) for t in themes)
    result=dict(actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),source_day=row[2],
        response_sha256=row[1],source_audit_sha256=hashlib.sha256(a.source_audit.read_bytes()).hexdigest(),
        themes=len(themes),sentinel_themes=gaps,sentinel_rate=gaps/len(themes) if themes else None,
        date_ref_comparisons=results,date_ref_mismatch_count=sum(r['status']=='DATE_NOT_BOUND_TO_ORIGINAL_REF' for r in results),
        authority='OFFLINE_REVIEW_EVIDENCE_ONLY_NO_NEW_QUALITY_GATE_OR_RAW_RESPONSE_RECLASSIFICATION',
        source_date_kind='PUBLICATION_OR_EVENT_NOT_A_FUTURE_SCHEDULE_CLAIM',model_calls=0,production_writes=0)
    out.mkdir(parents=True);save(out/'AUDIT.json',result)
    print(json.dumps(dict(path=str(out/'AUDIT.json'),sha256=hashlib.sha256((out/'AUDIT.json').read_bytes()).hexdigest(),
        comparisons=len(results),mismatches=result['date_ref_mismatch_count'],themes=len(themes),sentinels=gaps)));return 0

if __name__=='__main__':sys.exit(main())
