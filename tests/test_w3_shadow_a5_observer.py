"""Local byte/SQLite fixtures, no live DB, Node, models or notifications."""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import sqlite3

import pytest

from liangjian_funnel.runtime.shadow_a5_observer import inspect_a5_completion, read_a5_observation

DAY='2026-10-12'
def at(clock):return datetime.fromisoformat(DAY+'T'+clock+'+08:00')
def raw(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()

def fixture():
    facts={'trade_date':DAY,'review_kind':'POST_CLOSE','cutoff_at':at('15:00:00').isoformat()}
    facts['input_hash']=hashlib.sha256(raw(facts)).hexdigest()
    report={'trade_date':DAY,'review_kind':'POST_CLOSE','summary':'fixture'}
    row=dict(review_id='r1',trade_date=DAY,review_kind='POST_CLOSE',cutoff_at=facts['cutoff_at'],
        status='DEGRADED',input_hash=facts['input_hash'],prompt_hash='b'*64,output_hash='c'*64,
        fact_snapshot_json=raw(facts).decode(),report_json=raw(report).decode(),
        markdown_path='/not-consumed/by-pure-inspector.md',created_at=at('16:06:00').isoformat())
    job=dict(runId='node1',job='a5-close',command='run-a5-close',startedAt=at('16:00:02').isoformat(),
        finishedAt=at('16:06:01').isoformat(),exitCode=0,signal=None,status='succeeded',reason=None)
    payload={'time':at('16:00:02').isoformat(),'dispatch':[dict(kind='a5_post_close_1600',
        due=at('16:00:00').isoformat(),status='DISPATCHED',reason_code=None)]}
    logs=[dict(runId='node1',job='a5-close',stream='stdout',timestamp=at('16:06:00').isoformat(),message=raw(payload).decode())]
    return dict(trade_date=DAY,observed_at=at('16:06:02'),ledger_rows=[row],
        approved_reports={'r1':raw({'facts':facts,'report':report})},
        node_receipt_raw=raw({'recentJobRuns':[job]}),stdout_rows=logs)


def test_approved_interval_is_observed_completion_not_production_success():
    result=inspect_a5_completion(**fixture())
    assert result['status']=='A5_COMPLETED_OBSERVED'
    assert result['correlation_basis']=='UNIQUE_NONOVERLAPPING_INTERVAL'
    assert result['ledger_row_id']=='r1'
    assert result['parent_started_at']==at('16:00:02').isoformat()
    assert result['parent_finished_at']==at('16:06:01').isoformat()
    assert result['source_authenticated'] is False
    assert result['production_mutation'] is False
    assert result['reason_codes']==[]


def test_approved_observation_connects_to_shadow_only_coordinator():
    from test_w3_shadow_close_schedule import harness
    result=inspect_a5_completion(**fixture())
    engine,calls=harness();engine.poll(at('15:30:00'))
    final=engine.poll(at('16:06:03'),a5=result)
    assert final['status']=='FORMAL_WRITTEN'
    assert final['a5_status']=='A5_COMPLETED_OBSERVED'
    assert calls[-1][1]['a5']['ledger_row_id']=='r1'
    assert calls[-1][1]['a5']['parent_started_at']==at('16:00:02').isoformat()

def test_unique_degraded_completion_is_observed_not_fabricated_strong_link():
    value=fixture(); before=deepcopy(value);result=inspect_a5_completion(**value)
    assert result['status']=='A5_COMPLETED_OBSERVED'
    assert result['reason_codes']==[]
    assert result['candidate']['completed_at']==at('16:06:01').isoformat()
    assert result['candidate']['report_quality']=='DEGRADED'
    assert result['candidate']['approved_json_sha256']!=value['ledger_rows'][0]['output_hash']
    assert result['candidate']['correlation_basis']=='UNIQUE_NONOVERLAPPING_INTERVAL'
    assert value==before

@pytest.mark.parametrize('change',[
    {'exitCode':2},{'exitCode':False},{'status':'failed'}, {'signal':'SIGTERM'},
    {'finishedAt':None},{'finishedAt':at('16:07:00').isoformat()},
    {'finishedAt':'2026-10-12T16:06:01'}, {'job':'a5-midday'},
])
def test_noop_or_non_original_success_cannot_be_completion(change):
    value=fixture();node=json.loads(value['node_receipt_raw']);node['recentJobRuns'][0].update(change)
    value['node_receipt_raw']=raw(node)
    result=inspect_a5_completion(**value)
    assert result['status']=='DATA_LIMITED' and result['candidate'] is None

@pytest.mark.parametrize('dispatch', [[],[{'kind':'a5_post_close_1600','status':'SKIPPED'}],
    [{'kind':'a5_post_close_1600','status':'FAILED'}]])
def test_exit0_does_not_cover_skipped_python_dispatch(dispatch):
    value=fixture();value['stdout_rows'][0]['message']=raw({'dispatch':dispatch}).decode()
    assert inspect_a5_completion(**value)['candidate'] is None

@pytest.mark.parametrize('part',['facts','report'])
def test_ledger_and_approved_json_must_match(part):
    value=fixture();package=json.loads(value['approved_reports']['r1']);package[part]['injected']='wrong'
    value['approved_reports']['r1']=raw(package)
    assert inspect_a5_completion(**value)['candidate'] is None

def test_wrong_input_hash_or_missing_approved_file_not_accepted():
    value=fixture();value['ledger_rows'][0]['input_hash']='f'*64
    assert inspect_a5_completion(**value)['candidate'] is None
    value=fixture();value['approved_reports']={}
    assert inspect_a5_completion(**value)['candidate'] is None

def test_overlapping_parents_and_multiple_rows_are_not_guessed():
    value=fixture();node=json.loads(value['node_receipt_raw']);node['recentJobRuns'].append(
        {**node['recentJobRuns'][0],'runId':'node2','status':'running','finishedAt':None})
    value['node_receipt_raw']=raw(node)
    assert inspect_a5_completion(**value)['candidate'] is None
    value=fixture();value['ledger_rows'].append({**value['ledger_rows'][0],'review_id':'r2'})
    value['approved_reports']['r2']=value['approved_reports']['r1']
    assert inspect_a5_completion(**value)['candidate'] is None

def test_late_observation_does_not_claim_before_1645_availability():
    value=fixture();value['observed_at']=at('16:46:00')
    result=inspect_a5_completion(**value)
    assert result['candidate'] is None and 'A5_OBSERVATION_AFTER_DEADLINE' in result['reason_codes']

def test_duplicate_json_keys_and_nonfinite_numbers_are_rejected():
    value=fixture();value['node_receipt_raw']=b'{"recentJobRuns":[],"recentJobRuns":[]}'
    assert inspect_a5_completion(**value)['candidate'] is None
    value=fixture();value['node_receipt_raw']=b'{"recentJobRuns":[],"x":NaN}'
    assert inspect_a5_completion(**value)['candidate'] is None

def test_readonly_missing_db_does_not_create_file(tmp_path):
    path=tmp_path/'absent.sqlite'
    result=read_a5_observation(trade_date=DAY,observed_at=at('16:08:00'),state_db=path,
        approved_output_root=tmp_path,node_receipt_path=tmp_path/'node.json',node_log_path=tmp_path/'log.jsonl')
    assert result['status']=='DATA_LIMITED' and not path.exists()

def make_files(tmp_path):
    value=fixture();root=tmp_path/'approved';root.mkdir();package=root/'report.json'
    package.write_bytes(value['approved_reports']['r1']);value['ledger_rows'][0]['markdown_path']=str(package.with_suffix('.md'))
    database=tmp_path/'fixture.sqlite';connection=sqlite3.connect(database)
    keys=list(value['ledger_rows'][0]);connection.execute('create table a5_daily_reviews ('+','.join(k+' TEXT' for k in keys)+')')
    connection.execute('insert into a5_daily_reviews values ('+','.join('?' for _ in keys)+')',list(value['ledger_rows'][0].values()))
    connection.commit();connection.close()
    node=tmp_path/'node.json';node.write_bytes(value['node_receipt_raw'])
    log=tmp_path/'node.jsonl';log.write_bytes(b'\n'.join(raw(v) for v in value['stdout_rows']))
    return database,root,node,log,package

def test_actual_ro_read_and_source_bytes_do_not_change(tmp_path):
    database,root,node,log,package=make_files(tmp_path)
    paths=[database,node,log,package];before=[p.read_bytes() for p in paths]
    result=read_a5_observation(trade_date=DAY,observed_at=at('16:08:00'),state_db=database,
        approved_output_root=root,node_receipt_path=node,node_log_path=log)
    assert result['candidate']['report_quality']=='DEGRADED'
    assert result['candidate']['completed_at']==at('16:06:01').isoformat()
    assert [p.read_bytes() for p in paths]==before

def test_report_path_escape_is_not_read(tmp_path):
    database,root,node,log,package=make_files(tmp_path)
    outside=tmp_path/'outside.json';outside.write_bytes(package.read_bytes())
    connection=sqlite3.connect(database);connection.execute('update a5_daily_reviews set markdown_path=?',(str(outside.with_suffix('.md')),));connection.commit();connection.close()
    result=read_a5_observation(trade_date=DAY,observed_at=at('16:08:00'),state_db=database,
        approved_output_root=root,node_receipt_path=node,node_log_path=log)
    assert result['candidate'] is None

def test_readonly_ledger_content_budget_precedes_loading_large_json(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.shadow_a5_observer as module
    database,root,node,log,package=make_files(tmp_path)
    monkeypatch.setattr(module,'_MAX',64)
    calls=[]
    monkeypatch.setattr(module,'_read_stable',lambda path:calls.append(path))
    result=read_a5_observation(trade_date=DAY,observed_at=at('16:08:00'),state_db=database,
        approved_output_root=root,node_receipt_path=node,node_log_path=log)
    assert result['status']=='DATA_LIMITED' and calls==[]

def test_inspection_finishing_after_read_budget_is_not_accepted(tmp_path,monkeypatch):
    import liangjian_funnel.runtime.shadow_a5_observer as module
    database,root,node,log,package=make_files(tmp_path);clock=[0]
    monkeypatch.setattr(module,'monotonic',lambda:clock[0])
    def late(**kwargs):
        clock[0]=3
        return {'status':'TEST_LATE_RESULT_MUST_BE_DISCARDED'}
    monkeypatch.setattr(module,'inspect_a5_completion',late)
    result=read_a5_observation(trade_date=DAY,observed_at=at('16:08:00'),state_db=database,
        approved_output_root=root,node_receipt_path=node,node_log_path=log)
    assert result['status']=='DATA_LIMITED' and result['candidate'] is None


@pytest.mark.parametrize('clock',['15:59:59','16:07:00'])
def test_dispatch_clock_must_be_inside_real_parent_interval(clock):
    value=fixture();payload=json.loads(value['stdout_rows'][0]['message'])
    payload['time']=at(clock).isoformat();value['stdout_rows'][0]['message']=raw(payload).decode()
    result=inspect_a5_completion(**value)
    assert result['candidate'] is None
    assert result['reason_codes']==['A5_DISPATCH_CLOCK_CONFLICT']


@pytest.mark.parametrize('clock',[None,'15:59:59','16:07:00'])
def test_stdout_original_arrival_clock_must_be_proven(clock):
    value=fixture()
    if clock is None:value['stdout_rows'][0].pop('timestamp')
    else:value['stdout_rows'][0]['timestamp']=at(clock).isoformat()
    assert inspect_a5_completion(**value)['candidate'] is None


def test_fact_cutoff_cannot_differ_from_accepted_ledger_cutoff():
    value=fixture();package=json.loads(value['approved_reports']['r1'])
    facts=package['facts'];facts['cutoff_at']=at('14:30:00').isoformat()
    facts.pop('input_hash');facts['input_hash']=hashlib.sha256(raw(facts)).hexdigest()
    row=value['ledger_rows'][0];row['input_hash']=facts['input_hash'];row['fact_snapshot_json']=raw(facts).decode()
    value['approved_reports']['r1']=raw(package)
    result=inspect_a5_completion(**value)
    assert result['candidate'] is None
    assert result['reason_codes']==['A5_FACT_CUTOFF_CONFLICT']
