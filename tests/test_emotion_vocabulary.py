import pytest
import json
from pathlib import Path
from liangjian_funnel.pipeline.emotion_vocabulary import POLICIES,read_emotion_stage
from liangjian_funnel.pipeline.market_aggregates import _emotion_cycle_contract


def test_all_ten_stages_are_shared_exact_fixed_policies():
    from liangjian_funnel.pipeline.team_v2_a2_schema import _POLICIES
    assert _POLICIES==dict(POLICIES) and len(POLICIES)==10
    assert POLICIES['DIVERGENCE']=='WATCH_ONLY' and POLICIES['RETREAT']=='NO_NEW_ENTRY'
    assert POLICIES['LATENT']=='WATCH_ONLY' and POLICIES['ICE_POINT']=='NO_NEW_ENTRY'


def test_weak_high_break_rate_is_retreat_not_divergence():
    args=dict(breadth=.42,limit_up=20,limit_down=3,break_rate=.35,ladder_height=2)
    weak=_emotion_cycle_contract(temperature='WEAK',**args)
    assert weak['stage']=='RETREAT' and weak['new_long_permission']=='NO_NEW_ENTRY'
    diverging=_emotion_cycle_contract(temperature='DIVERGING_WEAK',**args)
    assert diverging['stage']=='DIVERGENCE' and diverging['new_long_permission']=='WATCH_ONLY'


def test_startup_only_dated_read_alias_never_emitted_by_engine():
    assert read_emotion_stage('STARTUP',source_trade_date='2026-10-27')=='IGNITION'
    with pytest.raises(ValueError,match='ALIAS_EXPIRED'):
        read_emotion_stage('STARTUP',source_trade_date='2026-10-28')
    result=_emotion_cycle_contract(temperature='STRONG',breadth=.7,limit_up=55,limit_down=1,
        break_rate=.1,ladder_height=1)
    assert result['stage']=='IGNITION' and result['new_long_permission']=='PROBE_ONLY'


def test_pinned_1016_ice_and_1019_latent_actual_input_branch_regression():
    frozen=json.loads((Path(__file__).parent/'fixtures/emotion_real_inputs_20261016_20261019.json').read_bytes())
    observed=[]
    for source in frozen['sources']:
        old=source['market_emotion_snapshot'];e=old['emotion_cycle_evidence']
        new=_emotion_cycle_contract(temperature=e['temperature'],breadth=e['breadth'],
            limit_up=e['limit_up_count'],limit_down=e['limit_down_count'],
            break_rate=e['break_rate'],ladder_height=e['ladder_height'])
        assert new['stage']==old['emotion_cycle_stage']
        assert new['new_long_permission']==old['new_long_permission']==POLICIES[new['stage']]
        assert len(source['file_sha256'])==64
        observed.append(new['stage'])
    assert observed==['ICE_POINT','LATENT']


def test_divergence_watch_still_blocks_new_emotion_relay_publication(tmp_path):
    from types import SimpleNamespace
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from liangjian_funnel.workflow import WorkflowApplication
    from liangjian_funnel.runtime.state import RuntimeStore
    store=RuntimeStore(tmp_path/'state.db')
    plan=dict(symbol='600001.SH',strategy_profile='LEADER_INTRADAY',stock_behavior_type='EMOTION')
    lane=SimpleNamespace(lane='lane_1',status='READY',final_output={'core_watch_pool':[plan],
        'secondary_watch_pool':[]},stages=[])
    result=WorkflowApplication._publish_plans(SimpleNamespace(store=store),
        SimpleNamespace(run_id='watch-only-contract',lanes=[lane]),'close',
        datetime(2026,10,20,17,tzinfo=ZoneInfo('Asia/Shanghai')),
        snapshot_data={'MARKET_EMOTION_SNAPSHOT':{'available':True,
            'emotion_cycle_stage':'DIVERGENCE','new_long_permission':'WATCH_ONLY'}})
    assert result['created']==[]
    assert result['blocked'][0]['reason']=='A3_EMOTION_RESEARCH_ONLY_NO_ENTRY'
