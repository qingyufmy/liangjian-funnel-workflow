"""Display actual server shortlist coverage, separately from model decisions."""
import hashlib
import json
import sqlite3


def annotate_pm(root,day,rows,connect):
    if not rows:return
    try:
        db=connect(root,'state/team_v2/team.db')
        try:
            originals=db.execute('SELECT meeting_id,result_json FROM pm_decisions WHERE account_id=? AND trade_date=? LIMIT 21',
                ('local-paper',day)).fetchall()
            choices=[]
            for row in originals:
                if len(row['result_json'].encode())>2_000_000:continue
                body=json.loads(row['result_json'])
                if body.get('session')=='PM_PREMARKET' and isinstance(body.get('shortlist_audit'),dict):
                    choices.append((row,body['shortlist_audit']))
            if len(choices)!=1:return
            original,audit=choices[0];selected=set(audit.get('selected_plan_ids') or [])
            excluded={r['plan_id']:r for r in audit.get('excluded',[]) if r.get('reason_code')=='PM_SHORTLIST_CUTOFF'}
            digest=hashlib.sha256(original['result_json'].encode()).hexdigest()
            for row in rows:
                pid=row['id']
                if pid in excluded:
                    row['pm_coverage']=dict(status='NOT_SENT_TO_PM',label='未进 PM 短名单',
                        reason_code='PM_SHORTLIST_CUTOFF',exclusion_basis=excluded[pid].get('exclusion_basis'),
                        model_rejection=False,source_id=original['meeting_id'],sha256=digest)
                elif pid in selected:
                    row['pm_coverage']=dict(status='PM_SHORTLIST',label='已进 PM 短名单',
                        source_id=original['meeting_id'],sha256=digest)
        finally:db.close()
    except (OSError,ValueError,KeyError,sqlite3.Error):
        return  # Absence is not a model rejection or an inferred shortlist.
