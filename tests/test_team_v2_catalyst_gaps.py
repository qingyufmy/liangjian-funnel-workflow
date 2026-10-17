from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.team_v2_a1_schema import validate_a1_response
from liangjian_funnel.pipeline.team_v2_catalyst_gaps import enforce_catalyst_gaps


CATALOG={'categories':['POLICY','FUNDAMENTAL'],'items':[{'id':'p','category':'POLICY'},{'id':'f','category':'FUNDAMENTAL'}]}


def response():
    return dict(envelope={'stage_id':'A1'},handoff_notes=['Verify theme participation'],what_would_change_my_mind=['Official cancellation'],
        industry_chain_graph=[dict(node_id=str(i),theme_ids=['T'],chain_position=segment) for i,segment in enumerate(('UPSTREAM','MIDSTREAM','DOWNSTREAM'))],
        structural_themes=[dict(theme_id='T',display_name='Theme',policy_verifiability='UNVERIFIED',
            falsifiers=['Official cancellation'],handoff_to_a2=['Confirm participation'],supporting_refs=['p'],opposing_refs=['f'],
            catalysts=[dict(event='NO_VERIFIED_CATALYST',source_ref='NO_VERIFIED_CATALYST',expected_date='UNKNOWN',
                priced_in_assessment='UNKNOWN',catalyst_search_scope=['POLICY','FUNDAMENTAL'])])])


def check(output):
    return validate_a1_response(output,CATALOG,required_envelope={'stage_id':'A1'},mode='POLICY_MACRO_DISCOVERY',candidate_symbols=set())


def test_authorized_gap_is_accepted_without_becoming_an_active_theme():
    raw=response();original=deepcopy(raw)
    assert check(raw).valid
    raw['active_research_pool']=[dict(symbol='000001.SZ',primary_theme='T',status='ACTIVE'),
        dict(symbol='000002.SZ',primary_theme='Other',status='ACTIVE')]
    result=enforce_catalyst_gaps(raw)
    assert [r['symbol'] for r in result['active_research_pool']]==['000002.SZ']
    assert result['monitor_pool'][0]['reason_codes']==['CATALYST_GAP']
    assert result['analysis_summary']['catalyst_evidence']['catalyst_gap_rate']==1
    assert raw['structural_themes']==original['structural_themes']


@pytest.mark.parametrize('change',['mixed','verified','one-category','duplicate-category','foreign-category','fake-date','missing-field'])
def test_no_implicit_catalyst_exception_outside_exact_c5_contract(change):
    raw=response();theme=raw['structural_themes'][0];sentinel=theme['catalysts'][0]
    if change=='mixed':theme['catalysts'].append(dict(event='Real',source_ref='p',expected_date='UNKNOWN',priced_in_assessment='UNKNOWN'))
    elif change=='verified':theme['policy_verifiability']='VERIFIED'
    elif change=='one-category':sentinel['catalyst_search_scope']=['POLICY']
    elif change=='duplicate-category':sentinel['catalyst_search_scope']=['POLICY','POLICY']
    elif change=='foreign-category':sentinel['catalyst_search_scope']=['POLICY','invented']
    elif change=='fake-date':sentinel['expected_date']='2026-10-20'
    else:sentinel.pop('priced_in_assessment')
    result=check(raw)
    assert not result.valid and 'A1_CATALYSTS_MISSING' in result.issues


def test_company_batch_omitting_themes_still_uses_frozen_discovery_and_does_not_modify_source():
    themes=response()['structural_themes'];output={'active_research_pool':[dict(symbol='x',primary_theme='Theme',status='ACTIVE')],
        'monitor_pool':[]};before=deepcopy(output)
    result=enforce_catalyst_gaps(output,{'structural_themes':themes})
    assert result['active_research_pool']==[] and result['monitor_pool'][0]['status']=='MONITOR'
    assert output==before and len(themes)==1


@pytest.mark.parametrize('kind',['two-nodes','one-segment'])
def test_total_graph_size_cannot_replace_each_themes_chain_coverage(kind):
    raw=response()
    if kind=='two-nodes':raw['industry_chain_graph'].pop()
    else:
        for row in raw['industry_chain_graph']:row['chain_position']='UPSTREAM'
    assert 'A1_THEME_CHAIN_COVERAGE_INSUFFICIENT' in check(raw).issues
