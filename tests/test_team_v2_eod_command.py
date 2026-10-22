"""Natural EOD CLI path: existing account, exact morning policy, mock HTTP."""
from fixture_model_stream import sse_model_response
from datetime import datetime
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.runtime.team_v2_eod_command import run_eod_stage
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
from test_team_v2_morning import fixture,client as morning_client,advance,CAL,source_bytes
from test_team_v2_eod import risk_output
from test_team_v2_pm_schema import close_response


def prepared(tmp_path,*,bad=None):
    settings,clock=fixture(tmp_path);api,_=morning_client(settings)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)
    clock.now=datetime.fromisoformat('2026-10-19T16:30:00+08:00');seen=[]
    def handler(request):
        packet=json.loads(request.read());runtime=json.loads(packet['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        role=runtime['required_envelope']['stage_id'];seen.append(role)
        raw=close_response() if role=='A3' else risk_output(runtime['required_envelope'])
        raw['envelope']=runtime['required_envelope']
        if role=='A3':raw['overnight_stances']=[];raw['capacity_request'].update(slots=0,clusters={})
        else:raw['overnight_reviews']=[];raw['risk_event_attribution']=[]
        def refs(node):
            if isinstance(node,dict):
                if 'supporting_refs' in node:node['supporting_refs']=['EOD:ACCOUNT']
                if 'opposing_refs' in node:node['opposing_refs']=['EOD:MARKET']
                for value in node.values():refs(value)
            elif isinstance(node,list):
                for value in node:refs(value)
        refs(raw)
        if bad==role:raise httpx.ConnectError('PRIVATE_PROVIDER_DETAIL',request=request)
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    api=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1)
    return settings,clock,api,seen


def test_real_close_edge_freezes_then_pm_a6_and_never_changes_legacy_plan(tmp_path):
    settings,clock,api,seen=prepared(tmp_path);before=source_bytes(settings)
    result=run_eod_stage(settings,clock=lambda:clock.now,calendar=CAL,client=api)
    assert result['status']=='READY' and seen==['A3','A6'] and result['account_id']=='local-paper'
    clock.now=clock.now.replace(minute=35)
    assert run_eod_stage(settings,clock=lambda:clock.now,calendar=CAL,client=api)==result and seen==['A3','A6']
    assert source_bytes(settings)==before
    with sqlite3.connect(settings.root/'state/team_v2/team.db') as db:
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()


@pytest.mark.parametrize('bad',['A3','A6'])
def test_failure_is_not_synthetic_ready_and_retry_does_not_recall(tmp_path,bad):
    settings,clock,api,seen=prepared(tmp_path,bad=bad)
    result=run_eod_stage(settings,clock=lambda:clock.now,calendar=CAL,client=api)
    assert result['status']!='READY' and not result['overnight_actions']
    assert run_eod_stage(settings,clock=lambda:clock.now,calendar=CAL,client=api)==result
    assert seen==(['A3'] if bad=='A3' else ['A3','A6'])
    assert b'PRIVATE_PROVIDER_DETAIL' not in (settings.root/'state/team_v2/team.db').read_bytes()


def test_missing_account_nontrading_and_early_do_not_initialize_or_call(tmp_path):
    settings,clock=fixture(tmp_path)
    clock.now=datetime.fromisoformat('2026-10-19T14:00:00+08:00')
    assert run_eod_stage(settings,clock=lambda:clock.now,calendar=CAL)['reason_code']=='TEAM_EOD_WINDOW_NOT_OPEN'
    clock.now=clock.now.replace(hour=16)
    assert run_eod_stage(settings,clock=lambda:clock.now,calendar=CAL)['status']=='BLOCKED'
    assert not (settings.root/'state/team_v2').exists()


def test_actual_cli_eod_does_not_construct_legacy_application(tmp_path,monkeypatch,capsys):
    import liangjian_funnel.cli as cli
    import liangjian_funnel.runtime.team_v2_eod_command as eod
    settings,_=fixture(tmp_path)
    def forbidden(*a,**kw):raise AssertionError('LEGACY_CONSTRUCTOR_FORBIDDEN')
    monkeypatch.setattr(cli,'WorkflowApplication',forbidden);monkeypatch.setattr(cli,'RuntimeStore',forbidden)
    monkeypatch.setattr(eod,'run_eod_stage',lambda _:dict(status='READY',trade_date='2026-10-19',session='RISK_EOD'))
    assert cli.main(['run-team-eod'],settings=settings)==0
    assert json.loads(capsys.readouterr().out)['status']=='READY'
