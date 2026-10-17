from copy import deepcopy
from dataclasses import asdict,replace
from datetime import timedelta
from decimal import Decimal as D
import json

import pytest

from liangjian_funnel.runtime.team_v2_trader_inputs import select_model_scope,shard_model_scope,model_closed_context,project_trader_values,digest
from test_team_v2_risk_engine import plan
from test_team_v2_minute_projection import source


def test_window_scope_keeps_all_holdings_events_and_unknown_atr_without_inventing_a_gate():
    plans={str(i):replace(plan(),plan_id=str(i),price=D(12),entry_low=D(10),entry_high=D(11),invalidation_price=D(9)) for i in range(5)}
    plans['1']=replace(plans['1'],price=D('10.5'))
    evaluations={pid:{'action':'NO_ACTION'} for pid in plans};evaluations['2']['action']='START_CONFIRMATION'
    result=select_model_scope(plans,active_plan_ids=set(plans),held_plan_ids={'held':'0'},evaluations=evaluations,atr5m={'3':D('.1')})
    assert result['plan_ids']==['0','1','2','4'] and not result['r1_proximity_complete']
    row=next(r for r in result['decisions'] if r['plan_id']=='3')
    assert row['not_sent_to_model'] and not row['model_judgment_denominator'] and row['reason_code']=='OUT_OF_WINDOW'


def test_near_invalidation_boundary_counts_even_when_far_from_entry():
    p=replace(plan(),price=D('9.1'),entry_low=D(11),entry_high=D(12),invalidation_price=D(9))
    result=select_model_scope({'p1':p},active_plan_ids={'p1'},held_plan_ids={},evaluations={'p1':{'action':'NO_ACTION'}},atr5m={'p1':D('.1')})
    assert result['plan_ids']==['p1'] and result['r1_proximity_complete']


def test_deterministic_eight_plan_shards_put_holdings_in_first_shard_without_dropping_any():
    ids=[f'p{i:02d}' for i in range(43)];shards=shard_model_scope(reversed(ids),{'held':'p42'})
    assert list(map(len,shards))==[8,8,8,8,8,3] and shards[0][0]=='p42'
    assert len({pid for shard in shards for pid in shard})==43
    assert shards==shard_model_scope(ids,{'held':'p42'})
    with pytest.raises(ValueError):shard_model_scope(ids,{str(i):f'p{i:02d}' for i in range(9)})


def test_r3_only_changes_model_bars_not_engine_statistics_or_original_context():
    start=source().bar_end;rows=tuple(source(bar_end=start+timedelta(minutes=i)) for i in range(101))
    original={'moving_averages':{'1m':{'ma20':10,'vwap':10}},'closed_bars':{'1m':[]},'execution_data':{'unchanged':True}}
    before=deepcopy(original);view=model_closed_context(original,rows,cutoff=rows[99].bar_end)
    assert len(view['closed_bars']['1m'])==60 and len(view['closed_bars']['5m'])==20
    assert len(view['closed_bars']['15m'])==6
    assert view['closed_bars']['1m'][-1]['bar_end']==rows[99].bar_end.isoformat()
    assert view['moving_averages']==original['moving_averages'] and original==before


@pytest.mark.parametrize('brief',[None,'Original authored brief'])
def test_projection_binds_raw_sources_without_repeating_payload_or_fabricating_a_brief(brief):
    raw={'symbol':'000001.SZ','source_run_id':'original','cluster_label':'bank','strategy_profile':'TREND_MA5','huge_audit':'x'*10000}
    if brief is not None:raw['trade_brief']=brief
    row=dict(plan_id='p1',symbol='000001.SZ',lane_id='lane_1',status='ACTIVE_TODAY',expires_at='2026-10-19T15:00:00+08:00',payload_json=json.dumps(raw))
    context={'closed_bars':{},'moving_averages':{},'original_tradability_contract':{'status':'KNOWN','halted':False}}
    values=dict(ACTIVE_PLANS=[asdict(plan())],POSITIONS=[],RISK_ENVELOPES={},TECH_REFERENCE={},EVIDENCE_CATALOG=dict(categories=['PLAN','STOCK_TECH'],items=[
        dict(id='PLAN:p1',category='PLAN',value=raw),dict(id='TECH:p1',category='STOCK_TECH',value=context)]))
    before=deepcopy(values);result=project_trader_values(values,plan_rows=[row],contexts={'p1':context})
    assert values==before and result['ACTIVE_PLANS'][0]['trade_brief']==(brief or 'MISSING_ORIGINAL_BRIEF')
    assert result['TRADABILITY']['p1']==context['original_tradability_contract']
    for item in result['EVIDENCE_CATALOG']['items']:
        ref=item['value'];actual=result
        for key in ref['input_ref']:actual=actual[key]
        assert actual is not None and len(ref['original_fact_sha256'])==64
    assert result['EVIDENCE_CATALOG']['items'][0]['value']['original_fact_sha256']==digest(raw)
    assert 'huge_audit' not in result['ACTIVE_PLANS'][0]['publication_facts']
