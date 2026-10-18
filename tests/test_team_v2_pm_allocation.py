"""PM permission must reach the atomic account snapshot, not just a report."""
from dataclasses import replace
from decimal import Decimal as D
import sqlite3
import pytest

from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal
from test_team_v2_execution import fixture,pm_result
from test_team_v2_risk_engine import plan,envelope
from test_team_v2_paper_ledger import bar,drive,stamp
from liangjian_paper_broker.matching import PaperRules
from liangjian_funnel.runtime.team_v2_allocation import compile_pm_allocation


def compile(raw):
    return compile_pm_allocation(raw,account_id='paper',trade_date='2026-10-19',meeting_id='pm-test',input_sha256='b'*64)


def test_buy_without_pm_allocation_is_not_executable(tmp_path):
    broker,args=fixture(tmp_path)
    args.pop('pm_allocation',None)
    result=admit_trader_signal(**args)
    assert not result['dispatched'] and result['judgment']['reason_codes']==['PM_ALLOCATION_UNAVAILABLE']
    assert broker.get_orders('2026-10-19')==()


def test_primary_open_buy_owns_backup_slot_before_fill_and_after_partial_fill(tmp_path):
    b,args=fixture(tmp_path)
    p2=replace(plan(),plan_id='p2',symbol='000002.SZ')
    plans={'p1':plan(),'p2':p2};raw=pm_result(plans)
    raw['slot_plan']=raw['slot_plan'][:1];raw['slot_plan'][0]['backup_plan_id']='p2'
    allocation=compile(raw)
    args.update(all_plans=plans,pm_allocation=allocation)
    assert admit_trader_signal(**args)['dispatched']
    second=args|dict(plan=p2,envelope=replace(envelope(),plan_id='p2',symbol=p2.symbol),signal_id='backup')
    result=admit_trader_signal(**second)
    assert not result['dispatched'] and result['judgment']['reason_codes']==['PM_SLOT_OCCUPIED']
    drive(b,bar(volume_shares=1000),PaperRules('2026-10-19'))
    b.clock=lambda:stamp(tm='10:01:00')
    result=admit_trader_signal(**(second|dict(signal_id='backup-after-partial',
        now=stamp(tm='10:01:00'),mark_as_of=stamp(tm='10:01:00'))))
    assert not result['dispatched'] and result['judgment']['reason_codes']==['PM_SLOT_OCCUPIED']


def test_unlisted_plan_has_no_new_entry_permission_but_cancel_is_independent(tmp_path):
    b,args=fixture(tmp_path);raw=pm_result(args['all_plans']);raw['slot_plan']=[]
    args['pm_allocation']=compile(raw)
    result=admit_trader_signal(**args)
    assert not result['dispatched'] and result['judgment']['reason_codes']==['PM_PLAN_NOT_ALLOCATED']
    result=admit_trader_signal(**(args|dict(action='CANCEL_SIGNAL',signal_id='cancel',pm_allocation=None)))
    assert result['cancelled_order_ids']==[]


def test_pm_probe_and_batch_count_only_tighten_current_envelope(tmp_path):
    b,args=fixture(tmp_path);raw=pm_result(args['all_plans'])
    raw['plan_allocations'][0].update(intended_risk_unit='PROBE',max_batches=1)
    args['pm_allocation']=compile(raw)
    result=admit_trader_signal(**args)
    assert result['dispatched'] and result['judgment']['shares']<=10000
    assert args['envelope'].risk_unit=='STANDARD' and args['envelope'].risk_budget==D(30000)


def test_cancel_releases_slot_for_backup_without_cancelling_protective_exit(tmp_path):
    b,args=fixture(tmp_path);p2=replace(plan(),plan_id='p2',symbol='000002.SZ')
    plans={'p1':plan(),'p2':p2};raw=pm_result(plans)
    raw['slot_plan']=raw['slot_plan'][:1];raw['slot_plan'][0]['backup_plan_id']='p2'
    args.update(all_plans=plans,pm_allocation=compile(raw))
    assert admit_trader_signal(**args)['dispatched']
    admit_trader_signal(**(args|dict(action='CANCEL_SIGNAL',signal_id='cancel-primary')))
    backup=admit_trader_signal(**(args|dict(plan=p2,envelope=replace(envelope(),plan_id='p2',symbol=p2.symbol),signal_id='backup')))
    assert backup['dispatched'] and len(b.get_orders('2026-10-19'))==2


def test_pm_one_batch_overrules_a6_two_batches_without_mutating_envelope(tmp_path):
    b,args=fixture(tmp_path);raw=pm_result(args['all_plans']);raw['plan_allocations'][0]['max_batches']=1
    args.update(envelope=replace(args['envelope'],max_batches=2),pm_allocation=compile(raw))
    assert admit_trader_signal(**args)['dispatched']
    row=admit_trader_signal(**(args|dict(signal_id='second-batch')))
    assert not row['dispatched'] and row['judgment']['reason_codes']==['TRADER_BATCH_CAP']
    assert args['envelope'].max_batches==2


def test_stale_or_missing_pm_does_not_turn_valid_model_into_buy_permission(tmp_path):
    from test_team_v2_trader_session import fixture as trader_fixture
    from liangjian_funnel.runtime.team_v2_trader_session import run_trader_session
    kwargs,seen,_=trader_fixture(tmp_path)
    kwargs['pm_meeting_id']='not-created'
    result=run_trader_session(**kwargs)
    assert result['model_validated'] and result['admissions'][0]['judgment']['reason_codes']==['PM_ALLOCATION_UNAVAILABLE']
    assert not result['admissions'][0]['dispatched'] and len(seen)==1


def test_invalid_pm_account_or_day_never_reuses_another_authority(tmp_path):
    from test_team_v2_trader_session import fixture as trader_fixture
    kwargs,seen,_=trader_fixture(tmp_path)
    with pytest.raises(ValueError,match='ACCOUNT_DATE_MISMATCH'):
        kwargs['store'].load_pm_allocation('pm-test',account_id='other',trade_date='2026-10-19')
    with pytest.raises(ValueError,match='ACCOUNT_DATE_MISMATCH'):
        kwargs['store'].load_pm_allocation('pm-test',account_id='paper',trade_date='2026-10-20')
    assert seen==[]


def test_actual_pm_client_persisted_allocation_reaches_actual_a4_client_and_broker(tmp_path):
    from test_team_v2_pm_session import fixture as pm_fixture
    from test_team_v2_trader_session import fixture as trader_fixture
    from liangjian_funnel.runtime.team_v2_pm_session import run_pm_premarket
    from liangjian_funnel.runtime.team_v2_trader_session import run_trader_session
    pm,pm_calls=pm_fixture(tmp_path/'morning')
    assert run_pm_premarket(**pm)['status']=='READY'
    kwargs,seen,_=trader_fixture(tmp_path/'trading')
    kwargs.update(store=pm['store'],pm_meeting_id=pm['meeting_id'])
    kwargs['all_plans']['p1']=replace(kwargs['all_plans']['p1'],cluster='bank')
    result=run_trader_session(**kwargs)
    assert result['admissions'][0]['dispatched'] and len(pm_calls)==len(seen)==1
    assert run_trader_session(**kwargs)==result and len(seen)==1
