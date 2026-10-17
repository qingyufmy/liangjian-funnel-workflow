from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import pytest

from liangjian_funnel.evaluation.team_v2_replay_batches import chronological_batches
from liangjian_funnel.evaluation.team_v2_replay_source import SIX_DAYS
from liangjian_funnel.evaluation.team_v2_replay_timeline import execute_timeline
from liangjian_funnel.evaluation.team_v2_replay_passes import run_two_passes
from liangjian_funnel.runtime.broker.local_paper import create_broker
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.team_v2_sources import stamp
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore


def fixture(work):
    root=Path(__file__).resolve().parents[1];clock=[stamp(SIX_DAYS[0]+'T08:33:00+08:00')]
    broker=create_broker('local_paper',database=work/'paper.db',paper_config=root/'config/paper_broker.yaml',
        risk_config=root/'config/risk_limits.yaml',clock=lambda:clock[0],calendar=ExchangeTradingCalendar(),run_id='timeline-component')
    store=TeamMeetingStore(work/'team.db');seen=[]
    def set_clock(at):clock[0]=at
    def prepare_day(day,actual,owned):
        assert actual is broker and owned is store;seen.append(('morning',day))
        return {'status':'REPLAY_PM_AND_A6_UNAVAILABLE_NO_ALLOCATION'}
    def minute(row,actual,owned):
        return dict(session_kwargs=dict(broker=actual,store=owned,clock=lambda:clock[0],
            account_id='paper',all_plans={}),closed_bars={},model_enabled=False)
    def close(day,actual,owned):
        assert clock[0].hour==15 and clock[0].minute==1
        seen.append(('close',day));return {'status':'NO_EXPOSURE_NO_CLOSE_MARK_CREATED'}
    return dict(broker=broker,store=store,set_clock=set_clock,prepare_day=prepare_day,
        prepare_minute=minute,consume_close=close,account_id='paper'),seen


def batches():
    return list(chronological_batches([dict(kind='day',trade_date=d,observed_plan_ids=[]) for d in SIX_DAYS],{}))


def test_all_1440_minutes_six_settlements_same_account_and_no_vacuous_model_pass(tmp_path):
    args,seen=fixture(tmp_path);result=execute_timeline(batches(),**args)
    assert result['scheduled_minutes']==1440 and result['regular_model_ticks']==276
    assert result['legal_rate_denominator']==0 and result['model_batch_legal_rate'] is None
    assert len(result['days'])==6 and all(d['settlement']['status']=='COMPLETE' for d in result['days'])
    assert seen==[(kind,day) for day in SIX_DAYS for kind in ('morning','close')]
    assert result['control_release_gate'].startswith('NOT_EVALUATED')


@pytest.mark.parametrize('change',['missing','duplicate','reordered','wrong-clock','entry-expansion'])
def test_partial_timeline_or_foreign_permission_cannot_be_silently_accepted(tmp_path,change):
    args,_=fixture(tmp_path);rows=batches()
    if change=='missing':rows.pop()
    elif change=='duplicate':rows[-1]=deepcopy(rows[-2])
    elif change=='reordered':rows[0],rows[1]=rows[1],rows[0]
    elif change=='wrong-clock':
        original=args['set_clock'];args['set_clock']=lambda at:original(at+timedelta(minutes=1))
    else:
        original=args['prepare_minute']
        def expanded(row,b,s):
            value=original(row,b,s);value['session_kwargs']['active_plan_ids']={'invented'};return value
        args['prepare_minute']=expanded
    with pytest.raises(ValueError,match='TIMELINE|CLOCK_REBIND|ENTRY_SCOPE_EXPANSION'):execute_timeline(rows,**args)


def test_real_six_day_empty_account_driver_double_pass_is_identical_but_not_funded_gate(tmp_path):
    def execute(work,journal,client):
        args,_=fixture(work);execute_timeline(batches(),**args)
    result=run_two_passes(tmp_path/'double',allowed_root=tmp_path,run_pass=execute)
    assert result['ledger_bytes_equal'] and result['curve_bytes_equal']
    assert all(p['settlements']==6 for p in result['passes'])
    assert not result['has_funded_fill_path'] and result['control_release_gate']=='NOT_EVALUATED'
