from copy import deepcopy
from pathlib import Path
import json
import pytest
from liangjian_funnel.pipeline.team_v2_a2_normalization import complete_server_policy_demotions
from liangjian_funnel.pipeline.team_v2_rotation_scope import rotation_coverage


def test_actual_0487_only_server_demoted_medicine_is_completed_not_model_compute_error():
    source=json.loads((Path(__file__).parent/'fixtures/a2_0487_policy_demotions_projection.json').read_text(encoding='utf-8'))
    raw={'focus_decisions':source['provider_focus_decisions'],'rotation_reviews':source['provider_rotation_reviews']}
    output={'focus_pool':[{'symbol':s} for s in source['postpolicy_final_focus_symbols']],'rotation_reviews':[]}
    original=deepcopy((raw,output));members=source['direction_members']
    projected,audit=complete_server_policy_demotions(output,provider_raw=raw,direction_members=members)
    assert (raw,output)==original and projected['focus_pool']==output['focus_pool']
    rows=projected['rotation_reviews'];assert len(rows)==1
    assert rows[0]['rotation_direction_id']=='SELECTED_BOARD:INNOVATIVE_MEDICINE_HEALTHCARE'
    assert rows[0]['reason_codes']==['SERVER_POLICY_DEMOTED']
    assert rows[0]['reviewed_symbols']==['300199.SZ','603127.SH']
    assert audit['actions'][0]['demoted_symbols']==source['server_demoted_focus_symbols']
    assert audit['rule_id']=='A2_SERVER_POLICY_DEMOTED_NO_FOCUS_V1'
    # The raw answer's real error is still outstanding: same theme is not membership.
    coverage=rotation_coverage({r['symbol'] for r in raw['focus_decisions']},members)
    assert set(coverage['required_no_focus'])=={'SELECTED_BOARD:AI_COMPUTE_INFRASTRUCTURE'}
    assert all(r['rotation_direction_id']!='SELECTED_BOARD:AI_COMPUTE_INFRASTRUCTURE' for r in rows)


@pytest.mark.parametrize('raw_focus,final_focus,existing,expected',[
    (['A'],[],[],True), (['A','B'],['B'],[],False),
    ([],[],[],False), (['SAME_THEME_NOT_MEMBER'],[],[],False),
    (['A'],['A'],[],False), (['A'],[],[{'rotation_direction_id':'D'}],False),
])
def test_completion_requires_all_real_members_to_be_server_demoted(raw_focus,final_focus,existing,expected):
    raw={'focus_decisions':[{'symbol':s} for s in raw_focus]}
    out={'focus_pool':[{'symbol':s} for s in final_focus],'rotation_reviews':existing}
    result=complete_server_policy_demotions(out,provider_raw=raw,direction_members={'D':['A','B']})
    assert (result is not None)==expected


def test_other_invalid_fields_are_preserved_for_original_validator():
    raw={'focus_decisions':[{'symbol':'A'}],'invented_field':999}
    out={'focus_pool':[],'rotation_reviews':[{'rotation_direction_id':'FOREIGN','bad_field':999}]}
    result,audit=complete_server_policy_demotions(out,provider_raw=raw,direction_members={'D':['A']})
    assert result['rotation_reviews'][0]==out['rotation_reviews'][0]
    assert raw['invented_field']==999 and audit['model_focus_unchanged']
