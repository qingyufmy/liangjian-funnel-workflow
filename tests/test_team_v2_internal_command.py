from datetime import datetime
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime.team_v2_internal_command import run_internal_delivery
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from test_team_v2_internal import sender, URL
from test_team_v2_paper_ledger import broker, stamp


CAL=SimpleNamespace(is_trading_day=lambda d:d.isoformat()=='2026-10-19')


def prepared(tmp_path):
    root=tmp_path/'project';directory=root/'state/team_v2'
    TeamMeetingStore(directory/'team.db');b=broker(directory/'paper.db')
    return SimpleNamespace(root=root),b


def test_actual_command_with_missing_source_is_not_ready_or_account_creation(tmp_path):
    settings,b=prepared(tmp_path);b.path.unlink();send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    result=run_internal_delivery(settings,clock=lambda:stamp(tm='08:45:00'),calendar=CAL,sender=send)
    assert result['status']=='DATA_LIMITED' and result['sent']==1 and not b.path.exists()
    assert '缺失' in json.loads(seen[0].content)['content']['text']


def test_independent_command_uses_fake_sender_no_model_or_legacy_store(tmp_path,monkeypatch):
    settings,b=prepared(tmp_path);before=b.snapshot_bytes()
    import liangjian_funnel.workflow as workflow
    monkeypatch.setattr(workflow,'WorkflowApplication',lambda *a,**kw:pytest.fail('legacy workflow constructed'))
    send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    first=run_internal_delivery(settings,clock=lambda:stamp(tm='08:45:00'),calendar=CAL,sender=send)
    assert first['status']=='READY' and first['sent']==1
    assert run_internal_delivery(settings,clock=lambda:stamp(tm='08:46:00'),calendar=CAL,sender=send)['sent']==0
    assert len(seen)==1 and b.snapshot_bytes()==before


def test_actual_cli_routes_before_legacy_constructors_and_redacts_failures(tmp_path,monkeypatch,capsys):
    import liangjian_funnel.cli as cli
    import liangjian_funnel.runtime.team_v2_internal_command as command
    from liangjian_funnel.settings import Settings
    settings=Settings.from_env({},root=tmp_path)
    def forbidden(*a,**kw):raise AssertionError('LEGACY_FORBIDDEN')
    monkeypatch.setattr(cli,'WorkflowApplication',forbidden);monkeypatch.setattr(cli,'RuntimeStore',forbidden)
    monkeypatch.setattr(command,'run_internal_delivery',lambda _:dict(status='NOOP',reason_code='NON_TRADING_DAY'))
    assert cli.main(['run-team-internal'],settings=settings)==0
    assert json.loads(capsys.readouterr().out)['status']=='NOOP'
    monkeypatch.setattr(command,'run_internal_delivery',lambda _:(_ for _ in ()).throw(ValueError('PRIVATE_SECRET')))
    assert cli.main(['run-team-internal'],settings=settings)==2
    result=capsys.readouterr().out
    assert 'PRIVATE' not in result and json.loads(result)['reason_code']=='INTERNAL_DELIVERY_FAILED'


def test_missing_internal_secret_and_unknown_delivery_remain_operationally_degraded(tmp_path):
    settings,b=prepared(tmp_path);send,seen=sender(tmp_path)
    assert run_internal_delivery(settings,clock=lambda:stamp(),calendar=CAL,sender=send)['status']=='DATA_LIMITED'
    send,seen=sender(tmp_path,handler=lambda request:__import__('httpx').Response(500),env={'SHADOW_RESEARCH_WEBHOOK':URL})
    for tm in ('10:00:00','10:01:00'):
        result=run_internal_delivery(settings,clock=lambda:stamp(tm=tm),calendar=CAL,sender=send)
        assert result['status']=='DATA_LIMITED' and result['reason_code']=='INTERNAL_DELIVERY_UNCONFIRMED'
    assert len(seen)==1


@pytest.mark.parametrize('at',['2026-10-18T10:00:00+08:00','2026-10-19T08:44:59+08:00','2026-10-19T17:56:00+08:00'])
def test_noop_without_any_owned_database_creation(tmp_path,at):
    settings=SimpleNamespace(root=tmp_path/'absent')
    assert run_internal_delivery(settings,clock=lambda:datetime.fromisoformat(at),calendar=CAL)['status']=='NOOP'
    assert not settings.root.exists()


def test_0327_later_team_coach_card_can_flush_before_1756(tmp_path):
    settings,b=prepared(tmp_path);send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    result=run_internal_delivery(settings,clock=lambda:stamp(tm='17:55:00'),calendar=CAL,sender=send)
    assert result['reason_code']=='INTERNAL_FLUSH_COMPLETED'


def test_scope_rejects_team_database_symlink(tmp_path):
    settings,b=prepared(tmp_path)
    # Avoid requiring Windows symlink privileges: the directory boundary itself
    # must be checked before opening any DB through a redirect.
    from unittest.mock import patch
    original=Path.is_symlink
    with patch.object(Path,'is_symlink',lambda p:p.name=='team.db' or original(p)):
        with pytest.raises(ValueError,match='SCOPE_INVALID'):
            run_internal_delivery(settings,clock=lambda:stamp(),calendar=CAL)
