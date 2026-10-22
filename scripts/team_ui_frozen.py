"""Local-only replay of approved raw rows; never connects to a VM or business API.

Rebuilds small throwaway SQLite tables from hash-verified frozen rows, then calls
the same read-only UI projector. Its report is API-vs-original, not DOM/browser
acceptance or a claim of a complete trading-day trace.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import tempfile
import base64
import zlib
from contextlib import closing

TABLES={'state/team_v2/paper.db':{'paper_meta','paper_orders','paper_fills','paper_lots','paper_marks'},
        'state/team_v2/model.db':{'model_calls'},'state/workflow.sqlite3':{'execution_plans','monitor_events'},
        'state/team_v2/team.db':{'risk_day_state','control_record_latch'}}

def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def sha(value):return hashlib.sha256(value).hexdigest()

def load_capture(path):
    raw=Path(path).read_bytes();data=json.loads(raw.decode('utf-8-sig'))
    if data.get('schema_version')!='frontend-frozen-rows/1':raise ValueError('FROZEN_SCHEMA')
    for name,rows in data['tables'].items():
        database,table=name.rsplit(':',1)
        if database not in TABLES or table not in TABLES[database] or len(rows)>500:raise ValueError('FROZEN_SCOPE')
        if sha(canonical(rows).encode())!=data['queries'][name]['rowset_sha256']:raise ValueError('FROZEN_ROW_HASH')
    return data,sha(raw)

def load_supplement(path):
    path=Path(path)
    if not path.is_file():return None
    raw=path.read_bytes();body=json.loads(raw.decode('utf-8-sig'))
    if body.get('schema_version')!='frontend-supplement/1' or len(body.get('calls',[]))>=100 or len(body.get('admissions',[]))>500:raise ValueError('SUPPLEMENT_SCOPE')
    if len(body.get('nodes',[]))!=1:raise ValueError('SUPPLEMENT_NODE_SCOPE')
    if not re.fullmatch('[a-f0-9]{40}',body.get('head','')):raise ValueError('SUPPLEMENT_HEAD')
    for row in body.get('admissions',[]):
        if not re.fullmatch('[a-f0-9]{64}',row.get('result_sha256','')) or row['result']['binding']['trade_date']!=row['trade_date']:raise ValueError('SUPPLEMENT_ADMISSION_SCOPE')
    return dict(body=body,sha256=sha(raw))

def load_minute_packet(path,expected_sha):
    raw=Path(path).read_bytes()
    if len(raw)>1024*1024 or sha(raw)!=expected_sha:raise ValueError('MINUTE_PACKET_BYTE_SHA')
    envelope=json.loads(raw);inflater=zlib.decompressobj()
    body=inflater.decompress(base64.b64decode(envelope['data'],validate=True),16*1024*1024+1)
    if len(body)>16*1024*1024 or not inflater.eof or inflater.unused_data or len(body)!=envelope['original_bytes'] or sha(body)!=envelope['original_sha256']:raise ValueError('MINUTE_PACKET_INNER_SHA')
    packet=json.loads(body)
    if packet.get('schema_version')!='a4-original-packet/0589' or len(packet['snapshots'])>48 or len(packet['events'])>48:raise ValueError('MINUTE_PACKET_SCOPE')
    for row in packet['snapshots']:
        if sha(row['payload_original_utf8'].encode())!=row['payload_sha256']:raise ValueError('MINUTE_SNAPSHOT_SHA')
    for row in packet['events']:
        if sha(row['payload_json'].encode())!=row['payload_sha256']:raise ValueError('MINUTE_EVENT_SHA')
    return dict(body=packet,sha256=expected_sha)

def build_fixture(data,root,supplement=None,minute_packet=None):
    root=Path(root)
    for database,tables in TABLES.items():
        path=root/database;path.parent.mkdir(parents=True,exist_ok=True)
        with closing(sqlite3.connect(path)) as db,db:
            for table in tables:
                rows=data['tables'].get(database+':'+table)
                if rows is None:continue
                if not rows:
                    if table=='control_record_latch':db.execute('CREATE TABLE control_record_latch(account_id TEXT,trade_date TEXT,body_json TEXT,body_sha256 TEXT)')
                    continue
                columns=list(rows[0])
                if any(not re.fullmatch('[a-z][a-z0-9_]*',c) for c in columns):raise ValueError('FROZEN_COLUMNS')
                types={c:('INTEGER' if all(r[c] is None or type(r[c]) is int for r in rows) else 'REAL' if all(r[c] is None or type(r[c]) in (int,float) for r in rows) else 'TEXT') for c in columns}
                db.execute('CREATE TABLE '+table+'('+','.join(c+' '+types[c] for c in columns)+')')
                db.executemany('INSERT INTO '+table+' VALUES('+','.join('?' for _ in columns)+')',[[row[c] for c in columns] for row in rows])
    # This marker only describes this explicitly reconstructed local fixture.
    # Real project roots without it retain the real live-source projection.
    marker=dict(schema='team-ui-frozen-capture/1',captured_at=data['captured_at'],coverage=data['coverage'],
                model_calls_complete=False,model_query=data['queries'].get('state/team_v2/model.db:model_calls'),
                node_pid_at_capture=data.get('node_pid'),head_at_capture=data.get('head'),roles=data.get('roles'),known_incidents=data.get('known_incidents'))
    if supplement:
        body=supplement['body'];original_ids={r['call_id'] for r in data['tables']['state/team_v2/model.db:model_calls']}
        if {r['call_id'] for r in body['calls']}!=original_ids:raise ValueError('SUPPLEMENT_CALL_ROWSET_CHANGED_REBUILD_REQUIRED')
        if any(not r['captured_at'].startswith(data['day']) for r in body['calls']):raise ValueError('SUPPLEMENT_CALL_DATE')
        marker.update(model_calls_complete=True,supplement=body,supplement_sha256=supplement['sha256'])
    if minute_packet:
        packet=minute_packet['body'];day=data['day']
        if any(not r['minute_end'].startswith(day) for r in packet['events']):raise ValueError('MINUTE_DAY')
        path=root/'storage/minute/minute_bars.sqlite3';path.parent.mkdir(parents=True,exist_ok=True)
        with closing(sqlite3.connect(path)) as db,db:
            db.execute('CREATE TABLE minute_decision_snapshots(snapshot_id,symbol,interval,decision_as_of,payload_sha256,payload_zlib,PRIMARY KEY(snapshot_id,symbol,interval))')
            for row in packet['snapshots']:
                db.execute('INSERT INTO minute_decision_snapshots VALUES(?,?,?,?,?,?)',(row['snapshot_id'],row['symbol'],row['interval'],row['decision_as_of'],row['payload_sha256'],zlib.compress(row['payload_original_utf8'].encode())))
        with closing(sqlite3.connect(root/'state/workflow.sqlite3')) as db,db:
            for row in packet['events']:
                existing=db.execute('SELECT payload_json FROM monitor_events WHERE event_id=?',(row['event_id'],)).fetchone()
                if existing:
                    if existing[0]!=row['payload_json']:raise ValueError('MINUTE_EVENT_CONFLICT')
                    continue
                columns=[x[1] for x in db.execute('PRAGMA table_info(monitor_events)')]
                db.execute('INSERT INTO monitor_events('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',[row[c] for c in columns])
        marker.update(minute_packet_sha256=minute_packet['sha256'],chart_coverage='15_MONITOR_TAIL_PLUS_12_SELECTED_EVENTS_AND_EXACT_1M_SNAPSHOTS_NOT_FULL_DAY')
    (root/'state/team_ui_frozen_capture.json').write_text(canonical(marker),encoding='utf-8')

def reconcile(path,minute_packet=None):
    from team_ui_readonly import project
    original,digest=load_capture(path);supplement=load_supplement(Path(path).with_name('frozen-runtime-supplement.json'))
    with tempfile.TemporaryDirectory(prefix='team-ui-frozen-') as directory:
        root=Path(directory);build_fixture(original,root,supplement,minute_packet)
        before={p.relative_to(root).as_posix():sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
        data=project(root,dict(kind='dashboard',trade_date=original['day'],limit=500,offset=0,days=20))
        checks=[]
        def check(name,actual,expected):checks.append(dict(field=name,actual=actual,expected=expected,match=actual==expected))
        meta={r['key']:r['value'] for r in original['tables']['state/team_v2/paper.db:paper_meta']}
        check('account.cash',data['account']['cash'],float(meta['cash']))
        check('account.realized',data['account']['realized'],float(meta['realized']))
        lots=[r for r in original['tables']['state/team_v2/paper.db:paper_lots'] if r['qty']>0]
        check('positions.total_qty',sum(x['qty'] for x in data['sections']['positions']['rows']),sum(x['qty'] for x in lots))
        if not lots:
            check('account.nav_when_no_positions',data['account']['nav'],float(meta['cash']))
            check('account.position_pct_when_no_positions',data['account']['position_pct'],0)
        by_id={r['id']:r for r in data['sections']['plans']['rows']}
        for row in original['tables']['state/workflow.sqlite3:execution_plans']:
            payload=json.loads(row['payload_json']);actual=by_id[row['plan_id']];prefix='plans.'+row['symbol']
            for field,expected in [('code',row['symbol']),('name',payload.get('company_name') or payload.get('name')),('status',row['status']),('stop',float(payload['stop_level'])),('risk_unit',payload.get('risk_unit')),('expires_at',row['expires_at']),('valid_from',row['valid_from']),('target',float(payload['first_resistance']) if payload.get('first_resistance') is not None else None),('planned_rr',float(payload['reward_risk']) if payload.get('reward_risk') is not None else None)]:check(prefix+'.'+field,actual.get(field),expected)
            check(prefix+'.zone_low',actual['zone'][0],float(payload['trigger_low']));check(prefix+'.zone_high',actual['zone'][1],float(payload['trigger_high']))
            check(prefix+'.payload_sha256',actual['source']['sha256'],sha(row['payload_json'].encode()))
        expected_fills=[r for r in original['tables']['state/team_v2/paper.db:paper_fills'] if r['minute_end'].startswith(original['day'])]
        by_fill={r['fill_id']:r for r in data['sections']['fills']['rows']}
        check('fills.today_count',len(by_fill),len(expected_fills))
        for row in expected_fills:
            for field in ('symbol','plan_id','side','qty','price','fee','minute_end'):
                check('fills.'+row['fill_id']+'.'+field,by_fill[row['fill_id']].get(field),row[field])
        if supplement:
            body=supplement['body'];node=body['nodes'][0];settings=node['settings']
            for field,expected in [('mode',settings['a4_trader_mode'].upper()),('production_pid_at_capture',node['pid']),('head_at_capture',body['head'])]:check('health.'+field,data['health'][field],expected)
            check('model_calls.captured_day_count',data['metrics']['model_calls'],len(body['calls']))
            check('health.monitor_model',data['health']['model_roles']['monitor'],settings['monitor_model'])
            check('health.team_intraday_model',data['health']['model_roles']['team_intraday'],settings['team_intraday_model'])
            check('admissions.only_requested_day',len(data['admissions']),len([a for a in body['admissions'] if a['trade_date']==original['day']]))
        if minute_packet:
            packet=minute_packet['body'];events={json.loads(e['payload_json'])['symbol']:e for e in sorted(packet['events'],key=lambda e:e['minute_end'])}
            snapshots={(s['snapshot_id'],s['symbol']):s for s in packet['snapshots'] if s['interval']=='1m'}
            for plan in data['sections']['plans']['rows']:
                event=json.loads(events[plan['code']]['payload_json']);execution=event['strategy']['execution_data'];snapshot=snapshots[(event['minute_snapshot_id'],plan['code'])];proof=plan.get('bar_source',{})
                check('chart.'+plan['code']+'.snapshot_sha256',proof.get('snapshot_sha256'),snapshot['payload_sha256'])
                check('chart.'+plan['code']+'.execution_input_sha256',proof.get('execution_input_sha256'),execution['input_sha256'])
                check('chart.'+plan['code']+'.cutoff',proof.get('cutoff'),execution['market_cutoff'])
                check('chart.'+plan['code']+'.basis',proof.get('basis'),'EXECUTION_AGGREGATED_5M_NOT_NATIVE')
                check('chart.'+plan['code']+'.no_future',all(bar['at']<=execution['market_cutoff'] for bar in plan['bars']),True)
        for name,digest_before in before.items():check('readonly.'+name,sha((root/name).read_bytes()),digest_before)
        failures=[c for c in checks if not c['match']]
        return dict(schema='team-ui-frozen-reconciliation/1',scope='API_VS_FROZEN_ORIGINAL_NOT_DOM_OR_FULL_DAY',source_sha256=digest,
                    captured_at=original['captured_at'],compared_fields=len(checks),differences=len(failures),checks=checks,
                    supplement_sha256=supplement['sha256'] if supplement else None,minute_packet_sha256=minute_packet['sha256'] if minute_packet else None,
                    known_gaps=['MODEL_HTTP_FAILURE_JOURNAL','FULL_DAY_MONITOR_TRACE','08:35_429_ORIGINAL','ACTUAL_BROWSER_STALE_OFFLINE_RECOVERY'],projection=data)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('capture');parser.add_argument('--report');parser.add_argument('--fixture-root');parser.add_argument('--minute-packet');parser.add_argument('--minute-sha');args=parser.parse_args()
    if args.fixture_root:
        root=Path(args.fixture_root).resolve()
        if root.parent!=Path(tempfile.gettempdir()).resolve() or not root.name.startswith('team-ui-frozen-qa-') or not root.is_dir() or any(root.iterdir()):raise ValueError('LOCAL_QA_TEMP_SCOPE')
        capture,_=load_capture(args.capture);build_fixture(capture,root,load_supplement(Path(args.capture).with_name('frozen-runtime-supplement.json')),load_minute_packet(args.minute_packet,args.minute_sha) if args.minute_packet else None)
        print(json.dumps(dict(kind='LOCAL_FROZEN_QA_NOT_PRODUCTION',captured_at=capture['captured_at'])))
        raise SystemExit(0)
    report=reconcile(args.capture,load_minute_packet(args.minute_packet,args.minute_sha) if args.minute_packet else None)
    if args.report:Path(args.report).write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k not in ('checks','projection')},ensure_ascii=True))
    raise SystemExit(bool(report['differences']))
