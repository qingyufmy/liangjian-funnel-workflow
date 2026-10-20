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
