from copy import deepcopy
from liangjian_funnel.pipeline.team_v2_risk_schema import validate_risk_premarket
from test_team_v2_risk_schema import output,CAT,CTX


def test_original_generic_codes_and_exact_subreasons_preserve_rejection():
    raw=output();raw['envelope_adjustments'][0]['reason_code']='A'*81
    raw['capacity_for_tomorrow']['notes']=['one','two','three','four']
    checked=validate_risk_premarket(raw,CAT,**CTX)
    assert not checked.valid
    assert 'A6_ADJUSTMENT_INVALID' in checked.issues and 'A6_CAPACITY_INVALID' in checked.issues
    assert 'A6_ADJUSTMENT_DETAIL:p1:reason_code:REGEX_2_TO_80' in checked.issues
    assert 'A6_CAPACITY_DETAIL:notes:NONEMPTY_MAX3_TEXT' in checked.issues
    raw['envelope_adjustments'][0]['reason_code']='CAPACITY_CHECKED'
    raw['capacity_for_tomorrow']['notes']=['bounded']
    assert validate_risk_premarket(raw,CAT,**CTX).valid


def test_new_cluster_input_only_tightens_original_capacity_assertion():
    raw=output();raw['capacity_for_tomorrow'].update(plans_acceptable=3,cluster_quota={'bank':3})
    checked=validate_risk_premarket(raw,CAT,**CTX)
    assert not checked.valid and 'A6_CAPACITY_DETAIL:cluster_quota:bank:MAX_2' in checked.issues
