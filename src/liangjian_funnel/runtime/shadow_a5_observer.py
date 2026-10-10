"""Independent read-only A5 source inspection; no business Store or callbacks.

The approved interval policy only emits A5_COMPLETED_OBSERVED for the independent
shadow report coordinator. It never emits production/notification SUCCEEDED.
No lease clock, health status, model output hash or mtime substitutes for a
real Node finishedAt or an approved report's original bytes.
"""
from __future__ import annotations

from datetime import date, datetime, time
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from time import monotonic
from zoneinfo import ZoneInfo

SH=ZoneInfo('Asia/Shanghai')
_SHA=re.compile(r'[0-9a-f]{64}')
_MAX=32*1024*1024


def _stamp(value):
    stamp=value if isinstance(value,datetime) else datetime.fromisoformat(value)
    if stamp.tzinfo is None:raise ValueError('NAIVE_SOURCE_CLOCK')
    return stamp.astimezone(SH)


def _canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def _sha(raw):return hashlib.sha256(raw).hexdigest()


def _object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('DUPLICATE_SOURCE_JSON_KEY')
        result[key]=value
    return result


def _decode(raw):
    if not isinstance(raw,(bytes,str)) or len(raw)>_MAX:raise ValueError('SOURCE_SIZE_OR_TYPE_INVALID')
    return json.loads(raw,object_pairs_hook=_object,
        parse_constant=lambda _:(_ for _ in ()).throw(ValueError('NONFINITE_SOURCE_JSON')))


def inspect_a5_completion(*,trade_date,observed_at,ledger_rows,approved_reports,
                          node_receipt_raw,stdout_rows,node_sample=None,require_node_identity=False):
    result={'schema_version':'shadow-a5-observation/1','status':'DATA_LIMITED',
        'reason_codes':[],'candidate':None,'production_mutation':False,
        'models':0,'customer_notifications':0,'source_authenticated':False}
    try:
        day=date.fromisoformat(trade_date);now=_stamp(observed_at)
        start=datetime.combine(day,time(16),SH);deadline=datetime.combine(day,time(16,45),SH)
        if now.date()!=day:raise ValueError('A5_OBSERVATION_DAY_MISMATCH')
        if now>deadline:raise ValueError('A5_OBSERVATION_AFTER_DEADLINE')
        if not isinstance(node_receipt_raw,bytes):raise ValueError('ORIGINAL_NODE_RECEIPT_REQUIRED')
        node_identity=None
        if require_node_identity:
            from .shadow_node_observer import verify_ready_sample
            node_identity=verify_ready_sample(node_receipt_raw,node_sample,observed_at=now)
        node=_decode(node_receipt_raw)
        jobs=node.get('recentJobRuns') if isinstance(node,dict) else None
        if not isinstance(jobs,list):raise ValueError('ORIGINAL_NODE_JOB_LIST_REQUIRED')
        parents=[]
        for job in jobs:
            if not isinstance(job,dict) or job.get('job')!='a5-close':continue
            began=_stamp(job.get('startedAt'))
            if began.date()==day:parents.append(job)
        # Running, failed and successful overlapping attempts are all retained:
        # a second parent prevents uniqueness, not just a second exit-zero one.
        if len(parents)!=1:raise ValueError('A5_PARENT_NOT_UNIQUE')
        parent=parents[0];began=_stamp(parent['startedAt']);ended=_stamp(parent.get('finishedAt'))
        if node_identity is not None:
            if (began.timestamp()*1000<node_identity['startedAtEpochMs']
                    or ended>_stamp(node_identity['observedAt'])):raise ValueError('A5_NODE_SOURCE_CLOCK_CONFLICT')
        if (parent.get('command')!='run-a5-close' or parent.get('status')!='succeeded'
            or type(parent.get('exitCode')) is not int or parent['exitCode']!=0
            or parent.get('signal') is not None or parent.get('reason') is not None
            or not isinstance(parent.get('runId'),str) or not parent['runId']
            or not start<=began<=ended<=now):raise ValueError('A5_PARENT_NOT_COMPLETED')
        stdout=[row for row in stdout_rows if isinstance(row,dict)
            and row.get('runId')==parent['runId'] and row.get('job')=='a5-close'
            and row.get('stream')=='stdout']
        messages=[row.get('message') for row in stdout]
        if not messages or not all(isinstance(m,str) for m in messages):raise ValueError('A5_STDOUT_MISSING')
        try:
            arrival=[_stamp(row.get('timestamp')) for row in stdout]
            if not all(began<=clock<=ended for clock in arrival):raise ValueError
        except (TypeError,ValueError,AttributeError):
            raise ValueError('A5_STDOUT_ARRIVAL_CLOCK_CONFLICT') from None
        dispatch_payload=_decode('\n'.join(messages))
        if not isinstance(dispatch_payload,dict):raise ValueError('A5_STDOUT_INVALID')
        dispatch=dispatch_payload.get('dispatch')
        if not isinstance(dispatch,list) or len(dispatch)!=1:raise ValueError('A5_DISPATCH_NOT_UNIQUE')
        item=dispatch[0]
        if (not isinstance(item,dict) or item.get('kind')!='a5_post_close_1600'
            or item.get('status')!='DISPATCHED' or item.get('reason_code') is not None
            or _stamp(item.get('due'))!=start
            or _stamp(dispatch_payload.get('time')).date()!=day):raise ValueError('A5_DISPATCH_NOT_SUCCESSFUL')
        dispatch_at=_stamp(dispatch_payload['time'])
        if not began<=dispatch_at<=ended:raise ValueError('A5_DISPATCH_CLOCK_CONFLICT')
        matches=[]
        for row in ledger_rows:
            if (not isinstance(row,dict) or row.get('trade_date')!=trade_date
                or row.get('review_kind')!='POST_CLOSE'):continue
            created=_stamp(row.get('created_at'))
            if began<=created<=ended:matches.append(row)
        if len(matches)!=1:raise ValueError('A5_ACCEPTED_ROW_NOT_UNIQUE')
        row=matches[0]
        if _stamp(row['created_at'])<dispatch_at:raise ValueError('A5_DISPATCH_CLOCK_CONFLICT')
        if (row.get('status') not in {'COMPLETED','DEGRADED'}
            or _stamp(row.get('cutoff_at'))!=datetime.combine(day,time(15),SH)
            or not all(isinstance(row.get(key),str) and _SHA.fullmatch(row[key])
                       for key in ('input_hash','prompt_hash','output_hash'))):
            raise ValueError('A5_ACCEPTED_ROW_INVALID')
        original=approved_reports.get(row.get('review_id'))
        if not isinstance(original,bytes):raise ValueError('APPROVED_REPORT_ORIGINAL_MISSING')
        approved=_decode(original)
        if not isinstance(approved,dict) or set(approved)!={'facts','report'}:raise ValueError('APPROVED_REPORT_INVALID')
        facts,report=approved['facts'],approved['report']
        if (not isinstance(facts,dict) or not isinstance(report,dict)
            or any(part.get('trade_date')!=trade_date or part.get('review_kind')!='POST_CLOSE'
                   for part in (facts,report))):raise ValueError('APPROVED_REPORT_IDENTITY_MISMATCH')
        if _stamp(facts.get('cutoff_at'))!=_stamp(row['cutoff_at']):raise ValueError('A5_FACT_CUTOFF_CONFLICT')
        if (facts!=_decode(row['fact_snapshot_json']) or report!=_decode(row['report_json'])):
            raise ValueError('APPROVED_REPORT_LEDGER_MISMATCH')
        claimed=facts.get('input_hash')
        computed=_sha(_canonical({key:value for key,value in facts.items() if key!='input_hash'}))
        if claimed!=row['input_hash'] or claimed!=computed:raise ValueError('A5_FACT_HASH_MISMATCH')
        result['candidate']={'trade_date':trade_date,'slot':'A5_POST_CLOSE_1600',
            'completed_at':ended.isoformat(),'report_quality':row['status'],
            'review_id':row['review_id'],'parent_run_id':parent['runId'],
            'ledger_row_id':row['review_id'],'parent_started_at':began.isoformat(),
            'parent_finished_at':ended.isoformat(),
            'source_ref':'a5_daily_reviews/'+row['review_id'],
            'source_sha256':_sha(original),
            'input_hash':row['input_hash'],'model_output_hash':row['output_hash'],
            'approved_json_sha256':_sha(original),'node_receipt_sha256':_sha(node_receipt_raw),
            'ledger_projection_sha256':_sha(_canonical(row)),
            'stdout_projection_sha256':_sha(_canonical(stdout_rows)),
            'observed_at':now.isoformat(),'correlation_basis':'UNIQUE_NONOVERLAPPING_INTERVAL'}
        result.update(result['candidate'])
        if node_identity is not None:
            result.update(node_pid=node_identity['pid'],node_started_at_epoch_ms=node_identity['startedAtEpochMs'],
                node_server_observed_at=node_identity['observedAt'],node_sample_receipt_sha256=node_sample['receipt_sha256'])
            result['candidate'].update(node_pid=node_identity['pid'],node_started_at_epoch_ms=node_identity['startedAtEpochMs'],
                node_server_observed_at=node_identity['observedAt'],node_sample_receipt_sha256=node_sample['receipt_sha256'])
        result['status']='A5_COMPLETED_OBSERVED'
        result['reason_codes']=[]
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError) as exc:
        code=str(exc)
        result['reason_codes']=[code if re.fullmatch('[A-Z0-9_]+',code) else 'A5_SOURCE_INVALID']
    return result


def _read_stable(path):
    with Path(path).open('rb') as handle:
        # fstat checks the same opened inode; pathname mtime is not a clock.
        import os
        before=os.fstat(handle.fileno())
        if before.st_size>_MAX:raise ValueError('SOURCE_TOO_LARGE')
        data=handle.read(_MAX+1);after=os.fstat(handle.fileno())
    if (len(data)>_MAX or (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns)
        or len(data)!=before.st_size):raise ValueError('SOURCE_CHANGED_DURING_READ')
    return data


def read_a5_observation(*,trade_date,observed_at,state_db,approved_output_root,
                        node_receipt_path,node_log_path,observation_clock=None,node_sample=None,require_node_identity=False):
    """Explicit local source paths only, never construct RuntimeStore/Settings.

    Node bytes must be a separately archived original dashboard DTO, not an
    invented jobs/latest.json. The function does not fetch it or write files.
    Active SQLite WAL is read through mode=ro, never immutable=1/checkpoint.
    """
    connection=None
    try:
        deadline=monotonic()+2.0
        date.fromisoformat(trade_date)
        database=Path(state_db).resolve(strict=True);root=Path(approved_output_root).resolve(strict=True)
        connection=sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=1)
        connection.row_factory=sqlite3.Row
        connection.set_progress_handler(lambda: int(monotonic()>=deadline),1000)
        connection.execute('BEGIN')
        sizes=list(connection.execute('''SELECT length(CAST(fact_snapshot_json AS BLOB)),
            length(CAST(report_json AS BLOB)) FROM a5_daily_reviews
            WHERE trade_date=? AND review_kind='POST_CLOSE' LIMIT 201''',(trade_date,)))
        if len(sizes)>200 or sum((row[0] or 0)+(row[1] or 0) for row in sizes)>_MAX:
            raise ValueError('A5_SOURCE_CONTENT_BUDGET_EXCEEDED')
        rows=[dict(row) for row in connection.execute('''SELECT review_id,trade_date,review_kind,
            cutoff_at,status,input_hash,prompt_hash,output_hash,fact_snapshot_json,report_json,
            markdown_path,created_at FROM a5_daily_reviews WHERE trade_date=? AND review_kind='POST_CLOSE'
            ORDER BY created_at DESC,review_id DESC LIMIT 201''',(trade_date,))]
        if len(rows)>200:raise ValueError('A5_ROW_SCOPE_EXCEEDED')
        reports={}
        for row in rows:
            path=Path(row['markdown_path']).with_suffix('.json').resolve(strict=True)
            if not path.is_relative_to(root):raise ValueError('APPROVED_REPORT_PATH_ESCAPE')
            reports[row['review_id']]=_read_stable(path)
        node=_read_stable(node_receipt_path)
        logs=[_decode(line) for line in _read_stable(node_log_path).splitlines() if line.strip()]
        if monotonic()>=deadline:raise ValueError('A5_OBSERVER_READ_BUDGET_EXCEEDED')
        actual_observed=_stamp(observation_clock()) if observation_clock is not None else _stamp(observed_at)
        if actual_observed<_stamp(observed_at):raise ValueError('A5_READ_CLOCK_REGRESSED')
        result=inspect_a5_completion(trade_date=trade_date,observed_at=actual_observed,
            ledger_rows=rows,approved_reports=reports,node_receipt_raw=node,stdout_rows=logs,
            node_sample=node_sample,require_node_identity=require_node_identity)
        if monotonic()>=deadline:raise ValueError('A5_OBSERVER_READ_BUDGET_EXCEEDED')
        if observation_clock is not None:
            finished=_stamp(observation_clock())
            if (finished<actual_observed or finished.date().isoformat()!=trade_date
                or finished.time().replace(tzinfo=None)>time(16,45)):
                raise ValueError('A5_READ_COMPLETED_AFTER_FENCE_OR_REGRESSED')
            if result.get('candidate') is not None:
                result['observed_at']=finished.isoformat()
                result['candidate']['observed_at']=finished.isoformat()
        return result
    except (OSError,ValueError,TypeError,AttributeError,sqlite3.Error):
        return {'schema_version':'shadow-a5-observation/1','status':'DATA_LIMITED',
            'reason_codes':['A5_READ_SOURCE_UNAVAILABLE'],'candidate':None,
            'production_mutation':False,'models':0,'customer_notifications':0,'source_authenticated':False}
    finally:
        if connection is not None:
            connection.rollback();connection.close()


__all__=['inspect_a5_completion','read_a5_observation']
