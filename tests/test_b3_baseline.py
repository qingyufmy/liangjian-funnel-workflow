import hashlib
import json
import sqlite3
import zlib

import pytest


def test_frozen_overlay_uses_original_runtime_policy_not_current_default():
    from copy import deepcopy
    from liangjian_funnel.evaluation.ablation.exporter import _audit_helpers
    _, overlay, _ = _audit_helpers(None)
    context = {'runtime_policy':{'STRUCTURE_FAIL_CONFIRM_BARS_15M':2}, 'unchanged':True}
    original = deepcopy(context)
    legacy = overlay(context, {})
    assert 'runtime_policy' not in legacy
    recorded = {'STRUCTURE_FAIL_CONFIRM_BARS_15M':1}
    result = overlay(context, {'runtime_policy':recorded})
    assert result['runtime_policy'] == recorded
    result['runtime_policy']['STRUCTURE_FAIL_CONFIRM_BARS_15M'] = 2
    assert recorded['STRUCTURE_FAIL_CONFIRM_BARS_15M'] == 1
    assert context == original
    with pytest.raises(ValueError, match='FROZEN_RUNTIME_POLICY_INVALID'):
        overlay(context, {'runtime_policy':None})

from liangjian_funnel.evaluation.b3_baseline import iter_frozen_windows, project_monitor_result, _history_event


@pytest.fixture
def source(tmp_path):
    path = tmp_path / 'source.sqlite3'
    c = sqlite3.connect(path)
    c.executescript('''
    CREATE TABLE execution_plans(plan_id TEXT,symbol TEXT,valid_from TEXT,expires_at TEXT,status TEXT,payload_json TEXT,updated_at TEXT);
    CREATE TABLE monitor_events(event_id TEXT,minute_end TEXT,action TEXT,reason_code TEXT,payload_json TEXT,created_at TEXT);
    CREATE TABLE minute_decision_snapshots(snapshot_id TEXT,symbol TEXT,interval TEXT,decision_as_of TEXT,captured_at TEXT,payload_sha256 TEXT,payload_zlib BLOB);
    CREATE TABLE a4_signal_lifecycles(trade_date TEXT);
    ''')
    at = '2026-10-15T09:32:00+08:00'
    c.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?)', ('p','600000.SH',at,'2026-10-15T15:00:00+08:00','INVALIDATED',json.dumps({'symbol':'600000.SH','strategy_profile':'TREND_MA5'}),'2026-10-15T09:32:01+08:00'))
    p = {'plan_id':'p','symbol':'600000.SH','minute_snapshot_id':'s','strategy':{'as_of':at,'action':'START_CONFIRMATION'}}
    c.execute('INSERT INTO monitor_events VALUES(?,?,?,?,?,?)',('e',at,'START_CONFIRMATION','A4_SESSION_WARMUP',json.dumps(p),'2026-10-15T09:32:10+08:00'))
    raw = json.dumps([{'symbol':'600000.SH','interval':'1m','bar_end':at,'open':10,'high':10,'low':10,'close':10,'volume':100,'amount':1000,'source_id':'TENCENT','adjust_mode':'none','volume_unit':'shares'}]).encode()
    c.execute('INSERT INTO minute_decision_snapshots VALUES(?,?,?,?,?,?,?)',('s','600000.SH','1m',at,'2026-10-15T09:32:02+08:00',hashlib.sha256(raw).hexdigest(),zlib.compress(raw)))
    c.commit(); c.close()
    return path


def test_window_stream_preserves_event_and_ignores_later_plan_status(source,tmp_path):
    rows = list(iter_frozen_windows(state_db=source,market_state_root=tmp_path))
    assert len(rows) == 1 and rows[0]['kind'] == 'WINDOW'
    assert rows[0]['event']['event_id'] == 'e'
    assert rows[0]['plan_row']['status'] == 'INVALIDATED'
    assert rows[0]['provenance']['source_snapshots']['1m']['snapshot_id'] == 's'
    assert rows[0]['observation_time'] == rows[0]['decision_time']


@pytest.mark.parametrize('mutation,expected',[
    ("UPDATE minute_decision_snapshots SET payload_sha256='bad'",'SNAPSHOT_HASH_MISMATCH'),
    ("UPDATE minute_decision_snapshots SET captured_at='2026-10-15T09:33:00+08:00'",'SNAPSHOT_CAPTURE_OR_EVENT_CLOCK_CONFLICT'),
    ("UPDATE monitor_events SET payload_json=replace(payload_json,'600000.SH','600001.SH')",'EVENT_PLAN_SYMBOL_MISMATCH'),
    ("DELETE FROM minute_decision_snapshots",'DECISION_1M_SNAPSHOT_MISSING'),
])
def test_gaps_are_explicit_not_silently_skipped(source,tmp_path,mutation,expected):
    c=sqlite3.connect(source);c.execute(mutation);c.commit();c.close()
    row=list(iter_frozen_windows(state_db=source,market_state_root=tmp_path))[0]
    assert row['kind']=='GAP' and row['reason']==expected


def test_snapshot_future_or_wrong_symbol_fails_closed(source,tmp_path):
    c=sqlite3.connect(source)
    raw=zlib.decompress(c.execute('select payload_zlib from minute_decision_snapshots').fetchone()[0])
    data=json.loads(raw);data[0]['bar_end']='2026-10-15T09:33:00+08:00'
    raw=json.dumps(data).encode();c.execute('update minute_decision_snapshots set payload_zlib=?,payload_sha256=?',(zlib.compress(raw),hashlib.sha256(raw).hexdigest()));c.commit();c.close()
    assert list(iter_frozen_windows(state_db=source,market_state_root=tmp_path))[0]['reason']=='FUTURE_BAR_AT_FROZEN_CUTOFF'


@pytest.mark.parametrize('change,reason',[
    ('symbol','SNAPSHOT_SYMBOL_OR_INTERVAL_MISMATCH'),
    ('duplicate','DUPLICATE_SNAPSHOT_BAR'),
])
def test_snapshot_contract_rejects_wrong_symbol_and_duplicate(source,tmp_path,change,reason):
    c=sqlite3.connect(source);raw=zlib.decompress(c.execute('select payload_zlib from minute_decision_snapshots').fetchone()[0]);data=json.loads(raw)
    if change=='symbol':data[0]['symbol']='600001.SH'
    else:data.append(data[0])
    raw=json.dumps(data).encode();c.execute('update minute_decision_snapshots set payload_zlib=?,payload_sha256=?',(zlib.compress(raw),hashlib.sha256(raw).hexdigest()));c.commit();c.close()
    assert list(iter_frozen_windows(state_db=source,market_state_root=tmp_path))[0]['reason']==reason


def test_empty_scope_is_non_strategy_record(source,tmp_path):
    c=sqlite3.connect(source);c.execute("UPDATE monitor_events SET payload_json='{}',action='EMPTY_SCOPE',reason_code='EMPTY_SCOPE'");c.commit();c.close()
    assert list(iter_frozen_windows(state_db=source,market_state_root=tmp_path))[0]['kind']=='NON_STRATEGY'


def test_pre_kernel_quote_block_is_not_a_synthetic_strategy_opportunity(source,tmp_path):
    c=sqlite3.connect(source);p=json.loads(c.execute('select payload_json from monitor_events').fetchone()[0]);p['strategy'].pop('action')
    c.execute('UPDATE monitor_events SET payload_json=?,action=?,reason_code=?',(json.dumps(p),'DATA_BLOCK','TENCENT_QUOTE_REQUEST_FAILED'));c.commit();c.close()
    row=list(iter_frozen_windows(state_db=source,market_state_root=tmp_path))[0]
    assert row['kind']=='PRE_KERNEL_BLOCK' and row['kernel_evaluated'] is False
    assert row['recorded_reason']=='TENCENT_QUOTE_REQUEST_FAILED'


def test_warmup_is_monitor_projection_not_a_strategy_change():
    assert project_monitor_result({'action':'DATA_BLOCK','reason_codes':['NO_CLOSED_15M']}) == ('START_CONFIRMATION','A4_SESSION_WARMUP')
    assert project_monitor_result({'action':'NO_ACTION','state':'PLAN_INVALIDATED','reason_codes':['X']}) == ('PLAN_INVALIDATED','X')


def test_episode_history_projection_matches_original_monitor():
    from datetime import datetime
    from liangjian_funnel.runtime.monitor import MonitorEngine
    event={'event_id':'e','minute_end':'2026-10-13T13:26:00+08:00','action':'LLM_VETO','reason_code':'LLM_VETO','effective':1,
           'payload_json':json.dumps({'plan_id':'p','strategy':{'many_facts':'not needed'}})}
    kwargs={'plan_id':'p','minute':datetime.fromisoformat('2026-10-13T13:27:00+08:00'),'action':'BUY_SIGNAL'}
    assert MonitorEngine._resolved_trigger_episode((event,),**kwargs)
    assert MonitorEngine._resolved_trigger_episode((_history_event(event,'p'),),**kwargs)
    assert 'many_facts' not in _history_event(event,'p')['payload_json']


def test_runner_keeps_real_kernel_mismatch_visible(source,tmp_path):
    from liangjian_funnel.evaluation.b3_baseline import run_baseline
    result=run_baseline(state_db=source,market_state_root=tmp_path,output=tmp_path/'run',captured_at='2026-10-15T09:32:00+08:00')
    assert result['raw_diff_count']==1
    assert result['raw_diffs'][0]['kernel_equal'] is False


def test_post_kernel_action_is_unverified_without_frozen_evidence(source,tmp_path,monkeypatch):
    from types import SimpleNamespace
    from liangjian_funnel.evaluation import b3_baseline
    c=sqlite3.connect(source);p=json.loads(c.execute('select payload_json from monitor_events').fetchone()[0]);p['strategy'].update(action='BUY_SIGNAL',reason_codes=['PASS'])
    c.execute('update monitor_events set action=?,reason_code=?,payload_json=?',('BUY_SIGNAL','PASS',json.dumps(p)));c.commit();c.close()
    def evaluator(*args,**kwargs):return SimpleNamespace(model_dump=lambda **kwargs:{'action':'BUY_SIGNAL','reason_codes':['PASS']})
    monkeypatch.setattr(b3_baseline,'evaluate_strategy',evaluator)
    result=b3_baseline.run_baseline(state_db=source,market_state_root=tmp_path,output=tmp_path/'unverified',captured_at='2026-10-15T09:32:00+08:00')
    row=result['raw_diffs'][0]
    assert row['kernel_equal'] and not row['monitor_equal']
    assert row['final_basis']=='POST_KERNEL_FINAL_ACTION_UNVERIFIED' and row['replay_final_action'] is None
