"""Native A4 consumes both frozen geometry versions; rejected plans never publish."""
from copy import deepcopy
from dataclasses import replace
import pytest
from liangjian_funnel.pipeline.team_v2_plan_geometry import project_team_plan_geometry,project_stop_rr_geometry

@pytest.mark.parametrize('projector',[project_team_plan_geometry,project_stop_rr_geometry],ids=['v1','v2'])
def test_native_a4_reads_zone_invalidation_no_chase_and_hard_stop(projector):
    from test_entry_confirmation import integrated_plan,intraday_minutes,server_context,CFG
    from liangjian_funnel.runtime.entry_confirmation import _zone,_levels
    from liangjian_funnel.runtime.strategies import evaluate_strategy
    raw={**integrated_plan(),'first_resistance':110,'invalidation_level':99}
    before=deepcopy(raw);projected=projector(raw);assert projected['status']=='PUBLISHABLE'
    p=projected['payload'];rows=intraday_minutes();at=rows[-1]['bar_end'];ctx=server_context(at,entry_confirmation_config=CFG)
    result=evaluate_strategy(p,rows,now=at,market_context=ctx).model_dump()
    assert result['action']=='BUY_SIGNAL',result
    assert _zone(p)==(p['trigger_zone']['low'],p['trigger_zone']['high'])
    assert _levels(p)[0]==p['stop_level']==p['invalidation_level']
    assert result['live_stop_level']==p['stop_level']
    assert result['live_no_chase_price']==p['no_chase']
    no_chase={**p,'no_chase':100.4}
    blocked=evaluate_strategy(no_chase,rows,now=at,market_context=ctx).model_dump()
    assert blocked['action']!='BUY_SIGNAL'and'A4_LIVE_NO_CHASE_EXCEEDED'in blocked['reason_codes']
    outside={**p,'trigger_zone':dict(low=100.2,high=100.4)}
    assert evaluate_strategy(outside,rows,now=at,market_context=ctx).model_dump()['action']!='BUY_SIGNAL'
    stopped=deepcopy(rows);stopped[-1]['low']=p['stop_level']-.01
    exited=evaluate_strategy({**p,'position_open':True},stopped,now=at,market_context=ctx).model_dump()
    assert exited['action']=='FORCED_RISK_EXIT'and'HARD_STOP'in exited['reason_codes']
    assert raw==before

def test_v2_rr_rejected_plan_cannot_be_published_or_activated(tmp_path):
    from test_w2_shadow_inputs_publication import publisher_fixture,AT,TARGET
    from liangjian_funnel.pipeline.research import StageAudit
    from liangjian_funnel.workflow import WorkflowApplication
    from liangjian_funnel.runtime.state import RuntimeStore
    app,result,data,raw=publisher_fixture()
    raw.update(trigger_zone=dict(low=100.2,high=100.8),invalidation_level=99,stop_level=99,first_resistance=105,minimum_reward_risk=2.5)
    assert project_team_plan_geometry(raw)['status']=='PUBLISHABLE'
    assert project_stop_rr_geometry(raw)['payload']is None
    stage=StageAudit(lane='lane_1',model='fixture',stage='A3',status='VALIDATED',snapshot_id=result.snapshot_id,
        prompt_hash=None,input_hash=None,output_hash=None,latency_ms=0,attempts=0,thinking_variant='team_v2',symbols=(raw['symbol'],),reason_codes=(),output=result.lanes[0].final_output)
    result=replace(result,lanes=(replace(result.lanes[0],stages=(stage,)),))
    app.store=RuntimeStore(tmp_path/'native-state.sqlite3')
    assert app.store.list_execution_plans()==()
    before=deepcopy(raw)
    summary=WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,minimum_trade_date=TARGET)
    assert summary['created']==[] and summary['observation_pool']
    assert app.store.list_execution_plans()==()
    assert raw==before
