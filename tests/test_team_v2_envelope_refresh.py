from dataclasses import replace
from decimal import Decimal as D

import pytest

from test_team_v2_execution import fixture
from test_team_v2_paper_ledger import stamp
from liangjian_funnel.runtime.risk_engine import AccountRisk,build_planning_envelope,conservative_envelope
from liangjian_funnel.runtime.team_v2_envelope_refresh import refresh_execution_envelopes


def setup(tmp_path):
    broker,args=fixture(tmp_path);plan=args['all_plans']['p1'];account=AccountRisk(D(1000000),D(1000000),D(1000000),D(1000000))
    planning=build_planning_envelope(replace(plan,status='PENDING_MORNING_REVIEW',valid_from=None),account,(),
        args['limits'],now=stamp(tm='08:33:00'),regime='TREND_MAINLINE')
    return plan,planning,dict(account=account,positions=(),limits=args['limits'],now=stamp(),regime='TREND_MAINLINE')


def test_activated_source_gets_fresh_execution_cap_but_never_expands_morning(tmp_path):
    plan,morning,args=setup(tmp_path)
    fresh,=refresh_execution_envelopes((plan,),morning_envelopes=(morning,),**args)
    assert fresh.eligible and fresh.purpose=='EXECUTION' and morning.purpose=='PLANNING'
    assert fresh.max_shares_cap<=morning.max_shares_cap and fresh.risk_budget<=morning.risk_budget


@pytest.mark.parametrize('kind',['pending','expired','price','source','suspend'])
def test_fresh_hard_failures_and_A6_suspend_cannot_be_promoted_by_activation(tmp_path,kind):
    plan,morning,args=setup(tmp_path)
    if kind=='pending':plan=replace(plan,status='PENDING_MORNING_REVIEW',valid_from=None)
    elif kind=='expired':args['now']=stamp(tm='15:01:00')
    elif kind=='price':plan=replace(plan,price=plan.no_chase+D('.1'))
    elif kind=='source':plan=replace(plan,data_ready=False)
    else:morning=replace(morning,eligible=False,risk_unit='NONE',max_shares_cap=0,blocked_reasons=('A6_SUSPEND',))
    value,=refresh_execution_envelopes((plan,),morning_envelopes=(morning,),**args)
    assert not value.eligible and value.max_shares_cap==0


def test_probe_batches_and_two_failure_latch_survive_new_current_quote(tmp_path):
    plan,morning,args=setup(tmp_path);morning=conservative_envelope(morning,consecutive_failures=1)
    value,=refresh_execution_envelopes((plan,),morning_envelopes=(morning,),**args)
    assert value.risk_unit=='PROBE' and value.max_batches==1 and value.a6_tightened
    halted,=refresh_execution_envelopes((plan,),morning_envelopes=(morning,),consecutive_failures=2,**args)
    assert not halted.eligible and 'A6_DEGRADED_HALT' in halted.blocked_reasons


def test_first_later_A6_failure_tightens_an_original_KEEP_morning_to_probe(tmp_path):
    plan,morning,args=setup(tmp_path)
    value,=refresh_execution_envelopes((plan,),morning_envelopes=(morning,),consecutive_failures=1,**args)
    assert value.risk_unit=='PROBE' and value.a6_tightened and value.max_batches==1
    assert value.risk_budget<=morning.risk_budget*D('.5')


def test_unreviewed_held_owner_remains_exit_only_not_a_new_entry_slot(tmp_path):
    plan,morning,args=setup(tmp_path);held=replace(plan,plan_id='old-held',status='INVALIDATED')
    value,=refresh_execution_envelopes((held,),morning_envelopes=(morning,),**args)
    assert value.purpose=='EXECUTION' and not value.eligible
    assert 'A6_MORNING_PLAN_NOT_REVIEWED' in value.blocked_reasons


def test_wrong_account_day_or_already_execution_envelope_cannot_be_morning_authority(tmp_path):
    plan,morning,args=setup(tmp_path)
    for bad in (replace(morning,trade_date='2026-10-16'),replace(morning,symbol='000002.SZ'),
            replace(morning,purpose='EXECUTION')):
        with pytest.raises(ValueError,match='EXECUTION_ENVELOPE_SOURCE'):
            refresh_execution_envelopes((plan,),morning_envelopes=(bad,),**args)
