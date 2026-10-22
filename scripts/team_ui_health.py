"""Bounded display-only metadata from the real journals, never business objects.

No writes, provider requests, arbitrary paths or raw error/customer text leave
this projection. A truncated tail is explicitly not a complete-day count.
"""
import hashlib
import json
import re
import sqlite3
import time
from datetime import datetime,timedelta,timezone

TZ=timezone(timedelta(hours=8))
SIGNALS={'BUY_SIGNAL','ADD_SIGNAL','SELL_SIGNAL','REDUCE_SIGNAL','FORCED_RISK_EXIT'}
JOBS={'team-morning-engine','team-morning-risk','team-morning-pm','team-a5-midday','team-a5-close','monitor','close','morning'}

def digest(value):return hashlib.sha256(value if isinstance(value,bytes) else json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def stamp(value,day):
    try:
        parsed=datetime.fromisoformat(value)
        if parsed.utcoffset() is None:return None
        parsed=parsed.astimezone(TZ)
        return parsed.isoformat() if parsed.date().isoformat()==day else None
    except (TypeError,ValueError):return None

def safe(value):return value if isinstance(value,str) and re.fullmatch(r'[A-Za-z0-9_.:+-]{1,240}',value) else None

def node_events(root,day):
    path=root/'outputs/node'/('node-'+day+'.jsonl')
    result=dict(status='MISSING_SOURCE',events=[],complete=False,reason='NODE_LOG_MISSING')
    if not path.is_file():return result
    if path.is_symlink() or not path.resolve().is_relative_to(root):return {**result,'reason':'NODE_LOG_SCOPE'}
    before=path.stat();size=before.st_size;budget=3*1024*1024;start=max(0,size-budget);end=time.monotonic()+.06
    with path.open('rb') as stream:
        stream.seek(start)
        if start:stream.readline(16385) # discard cut first line, never fabricate it
        offset=stream.tell();raw=stream.read(budget)
    events=[];complete=start==0;scanned=0
    for line in raw.splitlines(keepends=True):
        here=offset;offset+=len(line);scanned+=1
        if scanned>12000 or time.monotonic()>end:complete=False;break
        if len(line)>16384 or not line.endswith(b'\n'):complete=False;continue
        try:row=json.loads(line)
        except (ValueError,UnicodeError):complete=False;continue
        if not isinstance(row,dict):complete=False;continue
        at=stamp(row.get('timestamp'),day)
        if not at:continue
        job=row.get('job');message=row.get('message');kind=None;code=None;pid=None
        # Only inspect messages belonging to the explicit runtime/meeting jobs.
        # Notification/card/research-output text is not a display source.
        if job is None and isinstance(message,str) and len(message)<=2048:
            try:meta=json.loads(message)
            except ValueError:meta={}
            if isinstance(meta,dict) and meta.get('event') in {'NODE_PROCESS_START','NODE_PROCESS_STOP'}:
                kind=meta['event'];pid=meta.get('pid') if type(meta.get('pid')) is int else None
        elif job in JOBS and isinstance(message,str) and len(message)<=8192:
            match=re.search(r'(?:HTTP(?:Error| Error)?[\s:]*|status(?:_code)?[\s:=\"\x27]*|Error code[\s:]*)(429|403)\b',message,re.I)
            if match:kind='PROVIDER_HTTP_ERROR';code=int(match[1])
            elif 'status=terminated' in message:kind='JOB_TERMINATED'
            elif 'status=failed' in message:kind='JOB_FAILED'
        if kind:
            events.append(dict(at=at,kind=kind,job=safe(job),http_status=code,pid=pid,source=dict(table='node-jsonl',id=str(here),sha256=digest(line))))
            if len(events)>=200:complete=False;break
    after=path.stat()
    if (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):complete=False
    return dict(status='AVAILABLE',events=events,complete=complete,source_bytes=size,read_bytes=len(raw),source_start_offset=start,
                reason=None if complete else 'BOUNDED_NODE_LOG_TAIL_NOT_FULL_DAY')

def monitor_counts(root,day,connect):
    result=dict(status='MISSING_SOURCE',signals=None,confirmations=None,reason='MONITOR_SOURCE_UNAVAILABLE')
    db=connect(root,'state/workflow.sqlite3')
    try:
        # rowid bound BEFORE parsing JSON. 1200 was approved against today's
        # 712-event journal; a smaller/incomplete tail must not become count0.
        raw_rows=list(db.execute("SELECT rowid,event_id,minute_end,action,effective,CASE WHEN length(payload_json)<=262144 THEN json_extract(payload_json,'$.plan_id','$.strategy.entry_confirmation.confirmed','$.strategy.entry_confirmation.pattern','$.strategy.closed_5m_end') END AS facts FROM monitor_events WHERE rowid IN (SELECT rowid FROM monitor_events ORDER BY rowid DESC LIMIT 1201) ORDER BY rowid"))
        rows=[]
        for row in raw_rows:
            item=dict(row);facts=json.loads(item.pop('facts') or '[null,null,null,null]')
            item.update(zip(('plan_id','confirmed','pattern','closed5'),facts));rows.append(item)
        prior=any(isinstance(r['minute_end'],str) and r['minute_end'][:10]<day for r in rows)
        complete=len(rows)<1201 or prior
        today=[r for r in rows if stamp(r['minute_end'],day)]
        signals=[r for r in today if r['effective']==1 and r['action'] in SIGNALS]
        confirmed={(r['plan_id'],r['closed5'],r['pattern']) for r in today if r['confirmed']==1 and r['plan_id'] and r['closed5']}
        shape_source=any(r['confirmed'] is not None for r in today)
        return dict(status='AVAILABLE',signals=len(signals) if complete else None,confirmations=len(confirmed) if complete and shape_source else None,
                    complete=complete,read_rows=len(rows),day_rows=len(today),first_at=today[0]['minute_end'] if today else None,last_at=today[-1]['minute_end'] if today else None,
                    reason=None if complete else 'MONITOR_TAIL_DOES_NOT_COVER_DAY',confirmation_reason=None if shape_source else 'CONFIRMED_BOOLEAN_NOT_RECORDED',
                    basis='CUSTOMER_A4_EFFECTIVE_MONITOR_SIGNALS_NOT_TEAM_ADMISSIONS',coverage_basis='APPEND_JOURNAL_TAIL_INCLUDES_PRIOR_DAY_BOUNDARY' if prior else 'ENTIRE_SMALL_MONITOR_TABLE',
                    source=dict(table='monitor_events',id='bounded-day-metadata',sha256=digest(today)))
    finally:db.close()

def notifications(root,day,connect):
    db=connect(root,'state/workflow.sqlite3')
    try:
        end=(datetime.fromisoformat(day)+timedelta(days=1)).date().isoformat()
        # Existing idx_notification_deliveries_created, no title/payload/body.
        rows=[dict(r) for r in db.execute('SELECT delivery_id,kind,source_id,status,attempt_count,last_reason_code,created_at,sent_at FROM notification_deliveries WHERE created_at>=? AND created_at<? ORDER BY created_at DESC,delivery_id DESC LIMIT 501',(day,end))]
        complete=len(rows)<=500
        return dict(status='AVAILABLE',complete=complete,sent=sum(r['status']=='SENT' for r in rows) if complete else None,failed=sum(r['status']=='FAILED' for r in rows) if complete else None,
                    rows=[dict(id=safe(r['delivery_id']),kind=safe(r['kind']),status=safe(r['status']),attempts=r['attempt_count'] if type(r['attempt_count']) is int else None,
                               reason_code=safe(r['last_reason_code']),at=stamp(r['sent_at'] or r['created_at'],day),source=dict(table='notification_deliveries',id=safe(r['delivery_id']),sha256=digest(r))) for r in rows[:80]],
                    reason=None if complete else 'NOTIFICATION_ROW_BUDGET')
    finally:db.close()

def team_review_counts(root,day,connect):
    db=connect(root,'state/team_v2/team.db')
    try:
        rows=list(db.execute("SELECT decision_id,source_call_id,CASE WHEN length(result_json)<=524288 THEN json_extract(result_json,'$.status','$.candidate_review.confirmed_count','$.candidate_review.candidate_count','$.candidate_review.selected_count','$.candidate_review.actual_model_objects','$.candidate_review.failed_review_count','$.candidate_review.timeout_count') END AS facts FROM trader_sessions WHERE account_id=? AND trade_date=? LIMIT 501",('local-paper',day)))
        complete=len(rows)<=500;stats=dict(confirmed_observations=0,candidates=0,selected=0,model_objects=0,review_failed=0,timeouts=0);known=True
        for row in rows:
            values=json.loads(row['facts'] or 'null')
            if not isinstance(values,list) or len(values)!=7 or any(type(v) is not int or v<0 for v in values[1:]):known=False;continue
            for key,value in zip(stats,values[1:]):stats[key]+=value
        return dict(status='AVAILABLE',complete=complete,session_rows=len(rows),**{key:value if known and complete else None for key,value in stats.items()},
                    basis='TEAM_CANDIDATE_REVIEW_OBSERVATIONS_NOT_UNIQUE_SIGNALS_OR_ORDERS',reason=None if known and complete else 'TEAM_REVIEW_COUNTS_INCOMPLETE')
    finally:db.close()

def meeting_events(root,day,connect):
    events=[];gaps=[]
    try:
        db=connect(root,'state/team_v2/team.db')
        try:
            for table,role in (('risk_meetings','A6'),('pm_decisions','PM')):
                try:
                    observed='observed_at' if table=='risk_meetings' else 'NULL AS observed_at'
                    rows=list(db.execute('SELECT meeting_id,source_call_id,'+observed+',result_json FROM '+table+' WHERE account_id=? AND trade_date=? LIMIT 65',('local-paper',day)))
                    if len(rows)>64:gaps.append(table+'_ROW_BUDGET');continue
                    for row in rows:
                        if len(row['result_json'])>262144:gaps.append(table+'_BODY_BUDGET');continue
                        body=json.loads(row['result_json'])
                        if not isinstance(body,dict):raise ValueError('MEETING_BODY_NOT_OBJECT')
                        at=stamp(row['observed_at'] or body.get('fallback_observed_at') or body.get('observed_at') or body.get('as_of'),day)
                        # Missing observed_at stays missing, never filled08:35/now.
                        events.append(dict(at=at,kind='RISK_MEETING' if role=='A6' else 'PM_MEETING',role=role,status=safe(body.get('status')),fallback=body.get('fallback') if isinstance(body.get('fallback'),bool) else None,
                                           model_validated=body.get('model_validated') if isinstance(body.get('model_validated'),bool) else None,
                                           reason_codes=[v for v in body.get('reason_codes',[]) if safe(v)][:12],decision_authority=safe(body.get('decision_authority')),
                                           source_call_id=safe(row['source_call_id']),source=dict(table=table,id=safe(row['meeting_id']),sha256=digest(row['result_json'].encode()))))
                except (sqlite3.Error,ValueError,TypeError):gaps.append(table+'_UNAVAILABLE')
        finally:db.close()
    except (OSError,ValueError,sqlite3.Error):gaps.append('TEAM_MEETING_DB_UNAVAILABLE')
    try:
        db=connect(root,'state/team_v2/model.db')
        try:
            for row in db.execute('SELECT call_id,state,failure_code FROM model_call_claims WHERE call_id IN (?,?)',('A6:morning:'+day,'A3:morning:'+day)):
                # CLAIMED does not mean an active task or a successful call.
                events.append(dict(at=None,kind='MODEL_CLAIM',role='A6' if row['call_id'].startswith('A6:') else 'PM',status=safe(row['state']),reason_code=safe(row['failure_code']),
                                   source=dict(table='model_call_claims',id=safe(row['call_id']),sha256=digest(dict(row)))))
        finally:db.close()
    except (OSError,ValueError,sqlite3.Error):gaps.append('MODEL_CLAIM_METADATA_UNAVAILABLE')
    return dict(events=events,gaps=gaps)

def ops_history(root,day):
    """Normal production reader of a bounded OPS-attested metadata receipt.

    It never reads arbitrary referenced paths or invents missing incident time.
    Original-receipt validation belongs to the pinned collector; this digest
    protects the published metadata bytes, not an unclaimed raw-log replay.
    """
    path=root/'state/team_v2/ui_health_events.json'
    if not path.is_file():return dict(events=[],status='MISSING_SOURCE',reason='OPS_HEALTH_HISTORY_NOT_PUBLISHED')
    if path.is_symlink() or not path.resolve().is_relative_to(root) or path.stat().st_size>65536:raise ValueError('HEALTH_RECEIPT_SCOPE')
    receipt=json.loads(path.read_bytes())
    if not isinstance(receipt,dict):raise ValueError('HEALTH_RECEIPT_OBJECT')
    body=receipt.get('body')
    if not isinstance(body,dict) or digest(body)!=receipt.get('body_sha256') or body.get('schema')!='team-ui-health-events/1' or body.get('producer')!='PINNED_READ_ONLY_OPS' or body.get('trade_date')!=day:raise ValueError('HEALTH_RECEIPT_BINDING')
    captured=stamp(body.get('captured_at'),day)
    if not captured or datetime.fromisoformat(captured)>datetime.now(TZ):raise ValueError('HEALTH_RECEIPT_CAPTURE_TIME')
    source_rows=body.get('events')
    if not isinstance(source_rows,list) or len(source_rows)>80:raise ValueError('HEALTH_RECEIPT_ROW_BUDGET')
    events=[]
    allowed={'MODEL_ROLE_CHANGED','NODE_OFFLINE_OBSERVED','NODE_RECOVERED_HEALTH200','PROVIDER_HTTP_ERROR','JOB_FAILED','JOB_TERMINATED'}
    for item in source_rows:
        if not isinstance(item,dict):raise ValueError('HEALTH_RECEIPT_EVENT_OBJECT')
        at=stamp(item.get('at'),day);source=item.get('source') or {}
        if not isinstance(source,dict):raise ValueError('HEALTH_RECEIPT_EVENT_SOURCE')
        if not at or at>captured or item.get('kind') not in allowed or not safe(source.get('id')) or not re.fullmatch('[a-f0-9]{64}',source.get('sha256') or ''):raise ValueError('HEALTH_RECEIPT_EVENT')
        roles=item.get('model_roles') or {}
        if not isinstance(roles,dict):raise ValueError('HEALTH_RECEIPT_EVENT_MODELS')
        events.append(dict(at=at,kind=item['kind'],role=safe(item.get('role')),job=safe(item.get('job')),http_status=item.get('http_status') if item.get('http_status') in (429,403) else None,
                           model_roles={key:val for key,val in roles.items() if key in ('research','review','monitor','team_intraday') and isinstance(val,str) and re.fullmatch('[A-Za-z0-9_./:+-]{1,120}',val)},
                           time_boundary='OBSERVATION_NOT_EXACT_OUTAGE_START' if item['kind']=='NODE_OFFLINE_OBSERVED' else 'OBSERVATION_NOT_EXACT_RECOVERY_START' if item['kind']=='NODE_RECOVERED_HEALTH200' else 'ORIGINAL_EVENT_TIME',
                           source=dict(table='ops-health-receipt',id=source['id'],sha256=source['sha256'],metadata_sha256=receipt['body_sha256'])))
    return dict(status='AVAILABLE',events=events,captured_at=captured,reason=None,source_basis='OPS_ATTESTED_ORIGINAL_METADATA_NOT_API_REPLAY')

def project_health(root,day,connect):
    meetings=meeting_events(root,day,connect)
    try:logs=node_events(root,day)
    except (OSError,ValueError,TypeError):logs=dict(status='MISSING_SOURCE',events=[],complete=False,reason='NODE_LOG_UNREADABLE')
    try:ops=ops_history(root,day)
    except (OSError,ValueError,TypeError):ops=dict(events=[],status='MISSING_SOURCE',reason='OPS_HEALTH_HISTORY_INVALID')
    try:counts=monitor_counts(root,day,connect)
    except (OSError,ValueError,sqlite3.Error):counts=dict(status='MISSING_SOURCE',signals=None,confirmations=None,reason='MONITOR_BUDGET_OR_SOURCE_UNAVAILABLE')
    try:delivery=notifications(root,day,connect)
    except (OSError,ValueError,sqlite3.Error):delivery=dict(status='MISSING_SOURCE',rows=[],sent=None,failed=None,reason='NOTIFICATION_SOURCE_UNAVAILABLE')
    try:team=team_review_counts(root,day,connect)
    except (OSError,ValueError,sqlite3.Error):team=dict(status='MISSING_SOURCE',reason='TEAM_REVIEW_SOURCE_UNAVAILABLE')
    events=sorted(meetings['events']+logs['events']+ops['events'],key=lambda e:e.get('at') or '')
    return dict(schema='team-ui-health-history/1',events=events[-220:],source_time=max((e['at'] for e in events if e.get('at')),default=None),
                node_log= {k:v for k,v in logs.items() if k!='events'},ops={k:v for k,v in ops.items() if k!='events'},gaps=meetings['gaps']+([] if logs.get('complete') else [logs['reason']])+([] if ops['status']=='AVAILABLE' else [ops['reason']]),monitor=counts,
                notifications=delivery,team_reviews=team,coverage='EXISTING_JOURNAL_METADATA_NOT_RETROSPECTIVE_AVAILABILITY_PROOF')
