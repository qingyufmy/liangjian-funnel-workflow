from datetime import timedelta
from types import SimpleNamespace
import hashlib
import json
import sqlite3

import pytest

from test_team_v2_broker_adapter import make
from test_team_v2_paper_settle_command import config
from test_team_v2_paper_ledger import stamp, bar
from test_team_v2_minute_projection import source
from liangjian_funnel.data.mootdx import FetchResult
from liangjian_funnel.data.session_windows import closed_window_ends
from liangjian_funnel.runtime.team_v2_store import canonical


def setup(tmp_path):
    tmp_path.mkdir(parents=True,exist_ok=True)
    settings=config(tmp_path)
    settings.state_db_path=tmp_path/'state/workflow.sqlite3'
    settings.research_primary_lane_id='lane_1'
    b,clock=make(tmp_path/'state/team_v2')
    b.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',
        client_order_id='close-test',plan_id='p1',signal_id='s1',no_chase_price='10.2',tradable=True)
    clock.now=stamp(tm='10:01:00');b.on_bar(bar())
    clock.now=stamp(tm='15:01:00')
    with sqlite3.connect(settings.state_db_path) as db:
        db.execute('CREATE TABLE execution_plans(plan_id TEXT,symbol TEXT,lane_id TEXT,payload_json TEXT)')
        db.execute('INSERT INTO execution_plans VALUES(?,?,?,?)',('p1','000001.SZ','lane_1',json.dumps({'symbol':'000001.SZ'})))
    calls=[]
    def fetch(symbol,cutoff,count,deadline):
        calls.append(symbol)
        rows=tuple(source(bar_end=end) for end in closed_window_ends(cutoff,'1m'))
        return FetchResult(symbol=symbol,interval='1m',requested_bars=count,returned_bars=count,
            bars=rows,complete=True,reason_code='OK')
    return settings,b,clock,calls,fetch


def run(settings,clock,fetch,**kwargs):
    from liangjian_funnel.runtime.team_v2_close_bar import run_close_bar
    return run_close_bar(settings,clock=lambda:clock.now,calendar=SimpleNamespace(is_trading_day=lambda _:True),
        fetch_bars=fetch,**kwargs)


def test_original_1500_edge_closes_known_mark_gap_and_settle_reuse_is_idempotent(tmp_path):
    from liangjian_funnel.runtime.team_v2_commands import run_paper_settle
    settings,b,clock,calls,fetch=setup(tmp_path)
    before=b.get_account()['cash'];orders=b.get_orders('2026-10-19')
    result=run(settings,clock,fetch)
    assert result['status']=='COMPLETE' and calls==['000001.SZ']
    assert b.get_positions()[0]['mark_minute_end']==stamp(tm='15:00:00').isoformat()
    assert b.get_account()['cash']==before and b.get_orders('2026-10-19')==orders
    assert run(settings,clock,fetch)['status']=='COMPLETE' and len(calls)==1
    clock.now=stamp(tm='15:30:00')
    settled=run_paper_settle(settings,now=clock.now,calendar=SimpleNamespace(is_trading_day=lambda _:True))
    assert settled['status']=='COMPLETE' and settled['account']['nav'] is not None
    assert run_paper_settle(settings,now=clock.now,calendar=SimpleNamespace(is_trading_day=lambda _:True))==settled


def test_missing_packet_remains_data_limited_alerted_once_no_refetch_at_settle(tmp_path):
    from liangjian_funnel.runtime.team_v2_commands import run_paper_settle
    settings,b,clock,calls,fetch=setup(tmp_path)
    def missing(*args):calls.append('missing');raise RuntimeError('PRIVATE_PROVIDER_MESSAGE')
    assert run(settings,clock,missing)['status']=='DATA_LIMITED'
    assert run(settings,clock,fetch)['status']=='DATA_LIMITED' and calls==['missing']
    clock.now=stamp(tm='15:30:00')
    report=run_paper_settle(settings,now=clock.now,calendar=SimpleNamespace(is_trading_day=lambda _:True))
    assert report['status']=='DATA_LIMITED' and report['account']['nav'] is None
    with sqlite3.connect(tmp_path/'state/team_v2/team.db') as db:
        rows=db.execute('SELECT kind,body_json,status FROM team_notifications').fetchall()
    assert len(rows)==1 and rows[0][0]=='CLOSE_BAR_MISSING' and rows[0][2]=='PENDING'
    assert 'PRIVATE' not in rows[0][1] and calls==['missing']


def test_original_monitor_packet_has_priority_without_source_call(tmp_path):
    settings,b,clock,calls,fetch=setup(tmp_path)
    cutoff=stamp(tm='15:00:00')
    rows=fetch('000001.SZ',cutoff,240,0).bars;calls.clear()
    body=dict(execution_cutoff=cutoff.isoformat(),captured_at=clock.now.isoformat(),
        sources={'000001.SZ':dict(minute_complete=True,bars=[r.model_dump(mode='json') for r in rows])})
    body['packet_sha256']=hashlib.sha256(canonical(body).encode()).hexdigest()
    directory=tmp_path/'state/team_v2/minutes';directory.mkdir()
    (directory/'20261019T1501.json').write_text(canonical(body),encoding='utf-8')
    assert run(settings,clock,fetch)['status']=='COMPLETE' and calls==[]


@pytest.mark.parametrize('tm',['15:00:59','15:05:01','15:30:00'])
def test_no_late_or_early_source_request(tmp_path,tm):
    settings,b,clock,calls,fetch=setup(tmp_path);clock.now=stamp(tm=tm)
    assert run(settings,clock,fetch)['status']=='NOOP' and not calls


def test_1459_or_future_substitute_cannot_become_a_close_mark(tmp_path):
    settings,b,clock,calls,fetch=setup(tmp_path)
    def wrong(*args):
        result=fetch(*args)
        return result.model_copy(update={'bars':result.bars[:-1], 'complete':True})
    assert run(settings,clock,wrong)['status']=='DATA_LIMITED'
    assert b.get_positions()[0]['mark_minute_end']!=stamp(tm='15:00:00').isoformat()


def test_pending_buy_matches_original_close_bar_without_creating_an_intent(tmp_path):
    settings,b,clock,calls,fetch=setup(tmp_path)
    clock.now=stamp(tm='14:59:00')
    b.place_order('000001.SZ','BUY',100,order_type='LIMIT',limit_price='10',
        client_order_id='pending-close',plan_id='p1',signal_id='s2',no_chase_price='10.2',tradable=True)
    clock.now=stamp(tm='15:01:00');before=len(b.get_orders('2026-10-19'))
    assert run(settings,clock,fetch)['fills']
    assert len(b.get_orders('2026-10-19'))==before
    assert run(settings,clock,fetch)['fills']==[]


def test_capture_crossing_1505_is_not_current_close_permission(tmp_path):
    settings,b,clock,calls,fetch=setup(tmp_path)
    def late(*args):
        result=fetch(*args);clock.now=stamp(tm='15:05:01');return result
    assert run(settings,clock,late)['status']=='DATA_LIMITED'


def test_interrupted_claim_does_not_repeat_an_unknown_source_request(tmp_path):
    settings,b,clock,calls,fetch=setup(tmp_path)
    directory=tmp_path/'state/team_v2/close-bars/2026-10-19';directory.mkdir(parents=True)
    (directory/'000001.SZ.claim.json').write_text('{}')
    assert run(settings,clock,fetch)['status']=='DATA_LIMITED' and not calls


def test_missing_original_owner_never_manufactures_tradability(tmp_path):
    settings,b,clock,calls,fetch=setup(tmp_path)
    with sqlite3.connect(settings.state_db_path) as db:db.execute('DELETE FROM execution_plans')
    assert run(settings,clock,fetch)['status']=='DATA_LIMITED'


def test_source_and_owned_packet_tampering_do_not_refetch_or_relabel(tmp_path):
    settings,b,clock,calls,fetch=setup(tmp_path)
    assert run(settings,clock,fetch)['status']=='COMPLETE'
    path=tmp_path/'state/team_v2/close-bars/2026-10-19/000001.SZ.json'
    body=json.loads(path.read_text());body['execution_cutoff']=stamp(tm='14:59:00').isoformat()
    path.write_text(canonical(body))
    assert run(settings,clock,fetch)['status']=='DATA_LIMITED' and len(calls)==1


def test_actual_http_mock_trader_to_fill_to_last_monitor_then_close_settle(tmp_path):
    from test_team_v2_control_minute import prepared, run as control
    from liangjian_funnel.runtime.team_v2_close_bar import run_close_bar
    settings,unused,unusedclock,calls,fetch=setup(tmp_path/'sources')
    args,seen,raw,clock=prepared(tmp_path/'session')
    control(args,bar(tm='10:00:00'))
    clock[0]=stamp(tm='10:02:00');control(args,bar(tm='10:01:00',volume_shares=300000))
    clock[0]=stamp(tm='10:03:00');assert control(args,bar(tm='10:02:00',volume_shares=300000))['fills']
    clock[0]=stamp(tm='15:00:00');control(args,bar(tm='14:59:00'),model_enabled=False)
    clock[0]=stamp(tm='15:01:00')
    result=run_close_bar(settings,clock=lambda:clock[0],calendar=SimpleNamespace(is_trading_day=lambda _:True),
        fetch_bars=fetch,broker=args['broker'])
    assert result['status']=='COMPLETE' and len(seen)==1
    clock[0]=stamp(tm='15:30:00')
    assert args['broker'].settle('2026-10-19')['status']=='COMPLETE'


@pytest.mark.parametrize('status,exit_code',[('COMPLETE',0),('NOOP',0),('DATA_LIMITED',2)])
def test_real_cli_never_constructs_legacy_application_or_store(tmp_path,monkeypatch,capsys,status,exit_code):
    import liangjian_funnel.cli as cli
    import liangjian_funnel.runtime.team_v2_close_bar as edge
    from liangjian_funnel.settings import Settings
    monkeypatch.setattr(edge,'run_close_bar',lambda _:dict(status=status))
    def forbidden(*args,**kwargs):raise AssertionError('LEGACY_CONSTRUCTOR_FORBIDDEN')
    monkeypatch.setattr(cli,'WorkflowApplication',forbidden);monkeypatch.setattr(cli,'RuntimeStore',forbidden)
    assert cli.main(['run-team-close-bar'],settings=Settings.from_env({},root=tmp_path))==exit_code
    assert status in capsys.readouterr().out
