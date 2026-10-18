from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import sqlite3
import zlib

import pytest

from liangjian_funnel.evaluation.team_v2_replay_source import (
    SIX_DAYS, activation_reason, frozen_frames, project_snapshot, session_minutes,
)
from liangjian_funnel.runtime.team_v2_sources import stamp
from liangjian_funnel.runtime.team_v2_quote_evidence import assert_original_market_quote


def original():
    bar = dict(symbol='000001.SZ', interval='1m', bar_end='2026-10-15T09:31:00+08:00',
        open=10, high=10.1, low=9.9, close=10, volume=10000, amount=100000,
        source_id='TENCENT:ifzq.gtimg.cn', adjust_mode='none', volume_unit='shares',
        evidence_kind='MARKET_BAR', amount_kind='reported')
    raw = json.dumps([bar],sort_keys=True).encode()
    row = dict(snapshot_id='s1',symbol=bar['symbol'],interval='1m',
        decision_as_of='2026-10-15T09:32:00+08:00',captured_at='2026-10-15T09:32:12+08:00',
        payload_sha256=hashlib.sha256(raw).hexdigest(),payload_zlib=zlib.compress(raw))
    return row, bar


def project(row):
    return project_snapshot(row,symbol='000001.SZ',decision_at='2026-10-15T09:32:00+08:00',
        event_created_at='2026-10-15T09:32:15+08:00')


def test_snapshot_price_is_explicit_replay_basis_not_production_quote():
    row, _ = original(); before=deepcopy(row); result=project(row)
    assert result['quote']['last']=='10.0'
    assert result['quote']['bid'] is result['quote']['ask'] is None
    assert result['quote']['quote_time']=='2026-10-15T09:31:00+08:00'
    assert result['quote_basis']=='REPLAY_BAR_DERIVED'
    assert result['leader_open'] is False and result['emotion_state']=='DATA_BLOCK'
    assert result['tradability_status']=='ORIGINAL_RULE_PROOF_REQUIRED' and row==before
    with pytest.raises(ValueError,match='CONTROL_REPLAY_QUOTE_FORBIDDEN'):
        assert_original_market_quote(result['quote'])


@pytest.mark.parametrize('change,reason',[
    ({'payload_sha256':'0'*64},'HASH_MISMATCH'),
    ({'symbol':'000002.SZ'},'BINDING_INVALID'),
    ({'interval':'5m'},'BINDING_INVALID'),
    ({'decision_as_of':'2026-10-15T09:33:00+08:00'},'BINDING_INVALID'),
    ({'captured_at':'2026-10-15T09:32:16+08:00'},'BINDING_INVALID'),
])
def test_original_identity_hash_and_original_arrival_are_not_relabelled(change,reason):
    row,_=original()
    with pytest.raises(ValueError,match=reason): project(row|change)


@pytest.mark.parametrize('kind',['future','duplicate','other_symbol','previous_day','stale'])
def test_hash_valid_but_invalid_window_never_fills_from_later_or_other_history(kind):
    row,bar=original(); bars=[bar]
    if kind=='future':bars.append(bar|{'bar_end':'2026-10-15T09:32:00+08:00'})
    if kind=='duplicate':bars.append(bar)
    if kind=='other_symbol':bars=[bar|{'symbol':'000002.SZ'}]
    if kind=='previous_day':bars=[bar|{'bar_end':'2026-10-14T09:31:00+08:00'}]
    if kind=='stale':bars=[bar|{'bar_end':'2026-10-15T09:30:00+08:00'}]
    raw=json.dumps(bars).encode();row.update(payload_zlib=zlib.compress(raw),payload_sha256=hashlib.sha256(raw).hexdigest())
    with pytest.raises(ValueError,match='CLOSED_WINDOW_INVALID'):project(row)


def database(tmp_path,*,duplicate=False,conflict=False,missing=False):
    path=tmp_path/'source.sqlite3';snap,_=original()
    body=dict(plan_id='p1',symbol='000001.SZ',minute_snapshot_id='s1',strategy={'action':'WAIT'})
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE execution_plans(plan_id TEXT,lane_id TEXT,symbol TEXT,status TEXT,payload_json TEXT)')
        db.execute("INSERT INTO execution_plans VALUES ('p1','lane_1','000001.SZ','EXPIRED','{}')")
        db.execute('CREATE TABLE minute_decision_snapshots(snapshot_id TEXT,symbol TEXT,interval TEXT,'
            'decision_as_of TEXT,captured_at TEXT,payload_sha256 TEXT,payload_zlib BLOB)')
        if not missing:db.execute('INSERT INTO minute_decision_snapshots VALUES (?,?,?,?,?,?,?)',tuple(snap.values()))
        db.execute('CREATE TABLE monitor_events(event_id TEXT,event_key TEXT,lane_id TEXT,minute_end TEXT,'
            'action TEXT,reason_code TEXT,effective INT,payload_json TEXT,created_at TEXT)')
        for n in range(2 if duplicate or conflict else 1):
            b=body if not conflict or n==0 else body|{'strategy':{'action':'BUY'}}
            db.execute('INSERT INTO monitor_events VALUES (?,?,?,?,?,?,?,?,?)',
                (str(n),str(n),'lane_1',snap['decision_as_of'],'WAIT','WAIT',0,json.dumps(b),'2026-10-15T09:32:15+08:00'))
        db.execute('INSERT INTO monitor_events VALUES (?,?,?,?,?,?,?,?,?)',
            ('empty','empty','lane_1','2026-10-09T09:31:00+08:00','EMPTY_SCOPE','EMPTY_SCOPE',0,'{}','2026-10-09T09:31:01+08:00'))
    pin=hashlib.sha256(path.read_bytes()).hexdigest()
    return path,pin


def test_six_days_empty_day_duplicate_polls_and_239_missing_minutes_are_retained(tmp_path):
    path,pin=database(tmp_path,duplicate=True)
    frames=list(frozen_frames(path,expected_sha256=pin))
    days=[f for f in frames if f['kind']=='day']; minutes=[f for f in frames if f['kind']=='plan_minute']
    assert tuple(f['trade_date'] for f in days)==SIX_DAYS
    empty=next(f for f in days if f['trade_date']=='2026-10-09')
    assert empty['status']=='NO_PLAN_BOUND_EVENTS' and empty['original_event_count']==1
    assert len(minutes)==1 and len(minutes[0]['original_events'])==2
    assert minutes[0]['status']=='SOURCE_BOUND_RULES_PENDING'
    assert minutes[0]['final_plan_status_not_historical_authority']=='EXPIRED'
    coverage=next(f for f in frames if f['kind']=='coverage')
    assert coverage['observed_distinct_minutes']==1 and len(coverage['missing_original_event_minutes'])==239
    assert hashlib.sha256(path.read_bytes()).hexdigest()==pin


@pytest.mark.parametrize('option,reason',[('conflict','EVENT_CONFLICT'),('missing','SNAPSHOT_MISSING_OR_AMBIGUOUS')])
def test_conflicting_or_missing_source_is_data_limited_not_silently_skipped(tmp_path,option,reason):
    path,pin=database(tmp_path,**{option:True})
    frame=next(f for f in frozen_frames(path,expected_sha256=pin) if f['kind']=='plan_minute')
    assert frame['status']=='DATA_LIMITED' and 'source' not in frame
    assert any(reason in item for item in frame['reason_codes'])


def test_scope_hash_and_session_do_not_cherry_pick_dates_or_lunch(tmp_path):
    path,pin=database(tmp_path)
    with pytest.raises(ValueError,match='DATABASE_HASH_MISMATCH'):list(frozen_frames(path,expected_sha256='0'*64))
    with pytest.raises(ValueError,match='DAY_SCOPE_INVALID'):list(frozen_frames(path,expected_sha256=pin,days=('2026-10-16',)))
    values=session_minutes('2026-10-15')
    assert len(values)==240 and values[119].strftime('%H:%M')=='11:30' and values[120].strftime('%H:%M')=='13:01'
    assert all(not stamp('2026-10-15T11:30:00+08:00')<at<stamp('2026-10-15T13:01:00+08:00') for at in values)


def test_terminal_event_or_prior_terminal_cannot_become_an_active_probe_plan():
    row=dict(valid_from='2026-10-15T09:32:00+08:00',expires_at='2026-10-15T15:00:00+08:00',status='EXPIRED')
    frame=dict(decision_at='2026-10-15T10:01:00+08:00',original_action='START_CONFIRMATION')
    assert activation_reason(frame,row) is None  # archived final status is not historical authority
    assert activation_reason(frame|{'original_action':'PLAN_INVALIDATED'},row)=='REPLAY_ORIGINAL_TERMINAL_PLAN_NOT_ACTIVE'
    assert activation_reason(frame,row,terminal_seen=True)=='REPLAY_ORIGINAL_TERMINAL_PLAN_NOT_ACTIVE'
    assert activation_reason(frame,row|{'valid_from':None})=='REPLAY_ORIGINAL_ACTIVATION_UNPROVEN'
    assert activation_reason(frame|{'decision_at':row['expires_at']},row)=='REPLAY_ORIGINAL_PLAN_OUTSIDE_ACTIVE_WINDOW'
