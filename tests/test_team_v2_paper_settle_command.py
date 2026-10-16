from pathlib import Path
from types import SimpleNamespace
import shutil
import sqlite3

import pytest

from liangjian_funnel.runtime.team_v2_commands import run_paper_settle
from test_team_v2_broker_adapter import make
from test_team_v2_paper_ledger import stamp


def config(tmp_path):
    root=Path(__file__).resolve().parents[1]
    (tmp_path/'config').mkdir()
    for name in ('paper_broker.yaml','risk_limits.yaml'):
        shutil.copyfile(root/'config'/name,tmp_path/'config'/name)
    return SimpleNamespace(root=tmp_path)


def test_natural_settle_owns_only_existing_paper_ledger_and_is_idempotent(tmp_path):
    settings=config(tmp_path)
    b,clock=make(tmp_path/'state/team_v2')
    clock.now=stamp(tm='15:30:00')
    result=run_paper_settle(settings,now=clock.now,calendar=SimpleNamespace(is_trading_day=lambda _:True))
    assert result['status']=='COMPLETE' and result['reconcile']['status']=='MATCH'
    assert run_paper_settle(settings,now=clock.now,calendar=SimpleNamespace(is_trading_day=lambda _:True))==result
    assert not (tmp_path/'state/workflow.sqlite3').exists()


@pytest.mark.parametrize('reason,now,trading',[
    ('PAPER_SETTLE_WINDOW_NOT_OPEN',stamp(tm='15:29:59'),True),
    ('NON_TRADING_DAY',stamp(tm='15:30:00'),False)])
def test_non_due_does_not_create_owned_or_legacy_database(tmp_path,reason,now,trading):
    settings=config(tmp_path)
    result=run_paper_settle(settings,now=now,calendar=SimpleNamespace(is_trading_day=lambda _:trading))
    assert result==dict(status='NOOP',reason_code=reason)
    assert not (tmp_path/'state').exists()


def test_missing_paper_ledger_cannot_be_presented_as_zero_position_success(tmp_path):
    settings=config(tmp_path)
    result=run_paper_settle(settings,now=stamp(tm='15:30:00'),calendar=SimpleNamespace(is_trading_day=lambda _:True))
    assert result['reason_code']=='PAPER_ACCOUNT_NOT_INITIALIZED' and result['status']=='BLOCKED'
    assert not (tmp_path/'state').exists()


@pytest.mark.parametrize('status,code',[('COMPLETE',0),('NOOP',0),('DATA_LIMITED',2),('RECONCILIATION_FAILED',2)])
def test_real_cli_routes_without_workflow_application_or_legacy_store(tmp_path,monkeypatch,capsys,status,code):
    import liangjian_funnel.cli as cli
    import liangjian_funnel.runtime.team_v2_commands as commands
    from liangjian_funnel.settings import Settings
    settings=config(tmp_path)
    active=Settings.from_env({},root=settings.root)
    def forbidden(*_,**__):raise AssertionError('LEGACY_CONSTRUCTOR_FORBIDDEN')
    monkeypatch.setattr(cli,'RuntimeStore',forbidden)
    monkeypatch.setattr(cli,'WorkflowApplication',forbidden)
    monkeypatch.setattr(commands,'run_paper_settle',lambda _:dict(status=status))
    assert cli.main(['run-paper-settle'],settings=active)==code
    assert status in capsys.readouterr().out


def test_corrupt_paper_account_is_not_success_or_automatically_repaired(tmp_path):
    settings=config(tmp_path);b,clock=make(tmp_path/'state/team_v2')
    with sqlite3.connect(b.ledger.path) as db:
        db.execute("UPDATE paper_meta SET value='999999' WHERE key='cash'")
    clock.now=stamp(tm='15:30:00')
    result=run_paper_settle(settings,now=clock.now,calendar=SimpleNamespace(is_trading_day=lambda _:True))
    assert result['status']=='RECONCILIATION_FAILED' and result['reason_code']=='BROKER_RECONCILE_MISMATCH'
    assert result['account']['cash']=='999999'


def test_missing_closing_mark_is_data_limited_not_complete(tmp_path):
    from test_team_v2_paper_ledger import bar
    settings=config(tmp_path);b,clock=make(tmp_path/'state/team_v2')
    b.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='isolated',
        plan_id='p1',signal_id='s1',no_chase_price='10.2',tradable=True)
    clock.now=stamp(tm='10:01:00');b.on_bar(bar())
    result=run_paper_settle(settings,now=stamp(tm='15:30:00'),calendar=SimpleNamespace(is_trading_day=lambda _:True))
    assert result['status']=='DATA_LIMITED' and result['account']['nav'] is None
