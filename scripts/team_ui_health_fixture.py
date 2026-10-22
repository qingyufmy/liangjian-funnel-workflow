"""Hash-bound local QA augmentation from actual whitelisted source metadata.

Only an already-created OS-temp frozen fixture is writable, never a production
root. Reconstructed metadata payloads are not claimed to be original raw JSON.
"""
import argparse
from contextlib import closing
import hashlib,json,sqlite3,tempfile
from pathlib import Path

SOURCE_SHA='c20c5c62464307315569e23a58134c9add55f4a953d35d73c6a500e74607d567'
def canonical(value):return json.dumps(value,ensure_ascii=True,sort_keys=True,separators=(',',':'))

def augment(root,metadata_path,ops_receipt=None):
    root=Path(root).resolve();path=Path(metadata_path);raw=path.read_bytes()
    if root.parent!=Path(tempfile.gettempdir()).resolve() or not root.name.startswith('team-ui-frozen-qa-') or hashlib.sha256(raw).hexdigest()!=SOURCE_SHA:raise ValueError('QA_ONLY_OR_SOURCE_SHA')
    data=json.loads(raw);day=data['trade_date'];marker_path=root/'state/team_ui_frozen_capture.json';marker=json.loads(marker_path.read_bytes())
    if marker.get('schema')!='team-ui-frozen-capture/1' or not data['monitor_coverage']['full_day_covered'] or len(data['monitor_events'])>1200:raise ValueError('QA_SOURCE_COVERAGE')
    with closing(sqlite3.connect(root/'state/workflow.sqlite3')) as db,db:
        columns=[r[1] for r in db.execute('PRAGMA table_info(monitor_events)')]
        for row in data['monitor_events']:
            if db.execute('SELECT 1 FROM monitor_events WHERE event_id=?',(row['event_id'],)).fetchone():continue
            body={'symbol':row['symbol'],'plan_id':row['plan_id'],'strategy':{'closed_5m_end':row.get('execution_bar_end'),'entry_confirmation':{'confirmed':row.get('confirmed'),'pattern':row.get('pattern')}}}
            values={**row,'payload_json':canonical(body)}
            db.execute('INSERT INTO monitor_events('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',[values.get(c) for c in columns])
        db.execute('CREATE TABLE IF NOT EXISTS notification_deliveries(delivery_id TEXT,kind TEXT,source_id TEXT,status TEXT,attempt_count INTEGER,last_reason_code TEXT,created_at TEXT,sent_at TEXT)')
        for row in data['notification_deliveries']:
            if not db.execute('SELECT 1 FROM notification_deliveries WHERE delivery_id=?',(row['delivery_id'],)).fetchone():
                db.execute('INSERT INTO notification_deliveries VALUES(?,?,?,?,?,?,?,?)',tuple(row.get(k) for k in ('delivery_id','kind','source_id','status','attempt_count','last_reason_code','created_at','sent_at')))
    with closing(sqlite3.connect(root/'state/team_v2/team.db')) as db,db:
        db.executescript('CREATE TABLE IF NOT EXISTS risk_meetings(meeting_id,source_call_id,account_id,trade_date,observed_at,result_json);CREATE TABLE IF NOT EXISTS pm_decisions(meeting_id,source_call_id,account_id,trade_date,result_json);CREATE TABLE IF NOT EXISTS trader_sessions(decision_id,source_call_id,account_id,trade_date,result_json);')
        for table,key in (('risk_meetings','meeting_id'),('pm_decisions','meeting_id'),('trader_sessions','decision_id')):
            for row in data[table]:
                if db.execute('SELECT 1 FROM '+table+' WHERE '+key+'=?',(row[key],)).fetchone():continue
                values=(row[key],row['source_call_id'],'local-paper',day)
                if table=='risk_meetings':values+= (row.get('observed_at'),)
                values+=(canonical(row['result']),)
                db.execute('INSERT INTO '+table+' VALUES('+','.join('?' for _ in values)+')',values)
    with closing(sqlite3.connect(root/'state/team_v2/model.db')) as db,db:
        db.execute('CREATE TABLE IF NOT EXISTS model_call_claims(call_id,state,failure_code)')
        for row in data['model_claims_tail']:
            if not db.execute('SELECT 1 FROM model_call_claims WHERE call_id=?',(row['call_id'],)).fetchone():db.execute('INSERT INTO model_call_claims VALUES(?,?,?)',(row['call_id'],row['state'],row.get('failure_code')))
    with closing(sqlite3.connect(root/'state/team_v2/paper.db')) as db,db:
        db.execute('CREATE TABLE IF NOT EXISTS paper_admissions(signal_id,trade_date,binding_hash,result_json)')
        for row in data['paper_admissions']:
            if not db.execute('SELECT 1 FROM paper_admissions WHERE signal_id=?',(row['signal_id'],)).fetchone():db.execute('INSERT INTO paper_admissions VALUES(?,?,?,?)',(row['signal_id'],row['trade_date'],'metadata-fixture-not-original-binding',canonical(row['result'])))
    # Replace the old admission whitelist with the fuller actual whitelist, not
    # new business results. All original source result SHAs remain attached.
    previous={r['signal_id']:r for r in marker['supplement']['admissions']}
    merged=[]
    for row in data['paper_admissions']:
        old=previous.get(row['signal_id'],{});old_body=old.get('result',{});new_body=row['result']
        merged.append({**old,**row,'result':{**old_body,**new_body,'binding':{**old_body.get('binding',{}),**new_body['binding']},'judgment':{**old_body.get('judgment',{}),**new_body['judgment']}}})
    marker['supplement']['admissions']=merged
    marker.update(monitor_complete=True,health_metadata_sha256=SOURCE_SHA,coverage='ACTUAL_FULL_DAY_MONITOR_METADATA_PLUS_FROZEN_LEDGER_NOT_FULL_RAW_PAYLOADS')
    marker_path.write_text(canonical(marker),encoding='utf8')
    if ops_receipt:
        receipt=Path(ops_receipt).read_bytes()
        if len(receipt)>65536:raise ValueError('OPS_RECEIPT_BUDGET')
        # It is consumed by the same normal production reader/validation path.
        (root/'state/team_v2/ui_health_events.json').write_bytes(receipt)
    return dict(scope='LOCAL_RECONSTRUCTED_SOURCE_METADATA_NOT_PRODUCTION',monitor_rows=len(data['monitor_events']),source_sha256=SOURCE_SHA,
                node_original_log_not_reconstructed=True,provider429_original_available=False)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('metadata');p.add_argument('--ops-receipt');a=p.parse_args();print(canonical(augment(a.root,a.metadata,a.ops_receipt)))
