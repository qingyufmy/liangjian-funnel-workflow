"""Read-only evidence export and local A5 attribution revalidation, no model calls."""
import hashlib
import json
import subprocess
from pathlib import Path
from liangjian_funnel.facts.store import FactStore
from liangjian_funnel.review.daily import A5ReviewReport, _enforce_verified_findings

REMOTE = r'''
import pathlib,sqlite3,json,hashlib,collections
from datetime import datetime
r=pathlib.Path('/www/wwwroot/Agu/liangjian-funnel-workflow')
c=sqlite3.connect((r/'state/workflow.sqlite3').as_uri()+'?mode=ro',uri=True);c.row_factory=sqlite3.Row
rows=lambda sql:[dict(x) for x in c.execute(sql)]
events=rows("select action,reason_code,count(*) n,min(minute_end) first,max(minute_end) last from monitor_events where minute_end like '2026-09-23%' group by 1,2")
minutes=[x[0] for x in c.execute("select distinct minute_end from monitor_events where minute_end like '2026-09-23%' order by minute_end")]
notifications=rows("select kind,status,title,created_at,last_reason_code from notification_deliveries where created_at like '2026-09-23%'")
review=c.execute("select report_json,fact_snapshot_json,input_hash from a5_daily_reviews where trade_date='2026-09-23' and review_kind='MIDDAY' order by rowid desc limit 1").fetchone()
facts=json.loads(review['fact_snapshot_json']);report=json.loads(review['report_json'])
quality=collections.Counter()
for p in (r/'outputs/monitor/data_quality/2026-09-23').glob('*.json'):
 for value in json.loads(p.read_text())['symbols'].values(): quality[str((value.get('state'),value.get('decision_error')))]+=1
print(json.dumps(dict(events=events,minutes=minutes,notifications=notifications,quality=dict(quality),
 report=report,operational_evidence=facts.get('operational_evidence',[]),fact_input_hash=review['input_hash'],
 facts_sha256=hashlib.sha256(review['fact_snapshot_json'].encode()).hexdigest(),captured_at=datetime.now().isoformat()),ensure_ascii=True))
'''


def main():
    result = subprocess.run(['ssh','aurum-vm','/www/wwwroot/Agu/liangjian-funnel-workflow/.venv/bin/python -'],
                            input=REMOTE,text=True,capture_output=True,check=True,timeout=45)
    capture = json.loads(result.stdout)
    root = Path(__file__).resolve().parents[1]/'artifacts/a4-diagnosis-20260923'
    store = FactStore(root)
    identity = hashlib.sha256(result.stdout.encode()).hexdigest()[:12]
    store.write_json(root/f'capture-{identity}.json',capture)
    report = A5ReviewReport.model_validate(capture['report'])
    before = len(report.core_defects)
    _enforce_verified_findings(report, {'operational_evidence':capture['operational_evidence']})
    store.write_json(root/f'attribution-{identity}.json',report.model_dump(mode='json'))
    print(json.dumps(dict(capture=str(root/f'capture-{identity}.json'),minutes=len(capture['minutes']),
                         first=capture['minutes'][0],last=capture['minutes'][-1],quality=capture['quality'],
                         defects_before=before,defects_after=len(report.core_defects)),ensure_ascii=False))


if __name__ == '__main__':
    main()
