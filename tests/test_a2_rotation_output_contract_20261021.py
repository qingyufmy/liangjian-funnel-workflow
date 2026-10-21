import json
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.team_v2_rotation_scope import rotation_output_contract
from test_team_v2_a2_research import fixture


def test_exact_conditional_domain_is_generated_not_a_mandatory_row_list():
    directions={'SELECTED_BOARD:AI_CPO':{'300852.SZ','301029.SZ'},'BANKS':{'000001.SZ'}}
    before=deepcopy(directions)
    contract=rotation_output_contract(directions)
    assert contract['allowed_direction_members']=={'BANKS':['000001.SZ'],
        'SELECTED_BOARD:AI_CPO':['300852.SZ','301029.SZ']}
    assert contract['identifier_policy']=='EXACT_CASE_SENSITIVE_NO_ALIAS_NO_FOREIGN_IDS'
    assert directions==before


def test_actual_request_first_round_contains_last_user_bound_contract(tmp_path):
    app,args,seen=fixture(tmp_path,lambda raw,_:None,rotation=True)
    result=app._run_stage(**args)
    assert result.status=='VALIDATED'
    messages=seen[0]['messages']
    assert len(messages)==2
    runtime=json.loads(messages[-1]['content'].removeprefix('RUNTIME_INPUT\n'))
    contract=runtime['server_output_contract']
    assert contract['rule_id']=='A2_ROTATION_COVERAGE_V1'
    assert set(contract['allowed_direction_members'])=={'D'}
    assert result.diagnostics['rotation_output_contract']==contract


@pytest.mark.parametrize('bad',[None,{'D':['000001.SZ',None]}])
def test_bad_server_domain_is_rejected_not_repaired(bad):
    with pytest.raises(ValueError,match='TEAM_A2_ROTATION_SCOPE_INVALID'):
        rotation_output_contract(bad)
