"""Company economics cannot be substituted with a sector label or score."""
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.company_value_research import (
    FACETS, SCHEMA, TRANSMISSION, company_evidence_scopes, company_research_coverage,
    validate_company_research,
)

SYMBOL = '003021.SZ'
CUTOFF = '2026-10-20T15:00:00+08:00'


def unknown_review(scope):
    return dict(schema=SCHEMA,scope_sha256=scope['scope_sha256'],facets={
        name:dict(status='UNKNOWN',assessment='Insufficient company evidence, no economic conclusion',
            supporting_refs=[],opposing_refs=[],unknowns=['Verified issuer disclosures required'],
            falsifiers=['Contrary dated issuer evidence'],watch_metrics=['Original disclosed operating metric'])
        for name in FACETS})


def inputs():
    return dict(MAIN_BUSINESS_EVIDENCE={SYMBOL:dict(evidence=[
        dict(source_ref='issuer:report',publish_time='2026-08-20T18:00:00+08:00',text='Customer and product disclosure'),
        dict(source_ref='issuer:future',publish_time='2026-10-21T10:00:00+08:00',text='Not yet known'),
        dict(source_ref='issuer:undated',report_period='2026-06-30',text='Period is not disclosure time'),
        dict(source_ref='issuer:other',symbol='300296.SZ',publish_time='2026-08-20T18:00:00+08:00')])},
        MACRO_POLICY_FEED=dict(source_ref='sector:robots',publish_time='2026-10-19T10:00:00+08:00'))


def fixture():
    values=inputs();scope=company_evidence_scopes(values,{SYMBOL},knowledge_cutoff=CUTOFF)[SYMBOL]
    row=dict(symbol=SYMBOL,status='MONITOR',company_value_research=unknown_review(scope),
        financial_transmission=[dict(stage=stage,status='UNVERIFIABLE',source_ref='-') for stage in TRANSMISSION])
    return values,{SYMBOL:scope},dict(active_research_pool=[],monitor_pool=[row])


def test_company_scope_excludes_future_undated_other_issuer_and_macro_substitutes():
    values,scopes,_=fixture();before=deepcopy(values);scope=scopes[SYMBOL]
    assert set(scope['evidence'])=={'issuer:report'}
    assert scope['excluded_evidence']==dict(OTHER_COMPANY=1,PUBLICATION_AFTER_CUTOFF=1,PUBLICATION_TIME_UNVERIFIED=1)
    assert values==before
    assert company_evidence_scopes(values,{SYMBOL},knowledge_cutoff=CUTOFF)==scopes


def test_unknown_is_explicit_research_gap_never_an_investment_pass_or_reject():
    _,scopes,output=fixture();before=deepcopy(output)
    assert validate_company_research(output,scopes,required=True)==[]
    stats=company_research_coverage([output])
    assert all(value=={'UNKNOWN':1} for value in stats['facets'].values())
    assert stats['execution_authority'] is False and stats['semantic_truth_verified'] is False
    assert output==before


def test_date_only_disclosure_known_on_later_day_but_not_invented_before_close():
    values=inputs();values['MAIN_BUSINESS_EVIDENCE'][SYMBOL]['evidence']=[
        dict(source_ref='earlier',disclosure_date='2026-10-19'),
        dict(source_ref='today',disclosure_date='2026-10-20')]
    scope=company_evidence_scopes(values,{SYMBOL},knowledge_cutoff=CUTOFF)[SYMBOL]
    assert set(scope['evidence'])=={'earlier'}
    assert scope['evidence']['earlier'][0]['published_at']=='2026-10-19'
    assert scope['evidence']['earlier'][0]['time_precision']=='DATE'


@pytest.mark.parametrize('change', ['missing','scope','facet','concept_ref','foreign_ref','unsupported','empty_unknown','future'])
def test_company_research_rejects_unbound_or_substituted_economic_claims(change):
    _,scopes,output=fixture();row=output['monitor_pool'][0];value=row['company_value_research']
    facet=value['facets']['competitive_advantage']
    if change=='missing':row.pop('company_value_research')
    if change=='scope':value['scope_sha256']='0'*64
    if change=='facet':value['facets'].pop('growth_drivers')
    if change in {'concept_ref','foreign_ref','future'}:
        facet.update(status='EVIDENCE_SUPPORTED',supporting_refs=[{
            'concept_ref':'sector:robots','foreign_ref':'issuer:other','future':'issuer:future'}[change]],unknowns=[])
    if change=='unsupported':facet.update(status='EVIDENCE_SUPPORTED',unknowns=[])
    if change=='empty_unknown':facet['unknowns']=[]
    assert validate_company_research(output,scopes,required=True)


def test_supported_is_citation_bound_judgment_not_server_verification_of_a_moat():
    _,scopes,output=fixture();facet=output['monitor_pool'][0]['company_value_research']['facets']['competitive_advantage']
    facet.update(status='EVIDENCE_SUPPORTED',assessment='Research judgment: customer qualification may raise replacement costs',
        supporting_refs=['issuer:report'],unknowns=['Competitor qualification times remain unknown'])
    assert not validate_company_research(output,scopes,required=True)
    assert company_research_coverage([output])['semantic_truth_verified'] is False


def test_original_financial_fact_gets_internal_hash_citation_not_a_fake_announcement():
    values=inputs();values['COMPANY_FUNDAMENTALS']={SYMBOL:{'statements':{'INCOME':[
        dict(_dataset='INCOME',report_date_ms=1755619200000,period_end_ms=1751241600000,operating_income=100)]}}}
    scope=company_evidence_scopes(values,{SYMBOL},knowledge_cutoff=CUTOFF)[SYMBOL]
    refs=[key for key in scope['evidence'] if key.startswith('A1:COMPANY_FACT:')]
    assert len(refs)==1 and scope['evidence'][refs[0]][0]['citation_kind']=='FROZEN_FINANCIAL_FACT'


def test_internal_company_facts_cannot_expand_original_policy_source_authority():
    _,scopes,output=fixture()
    output['monitor_pool'][0]['source_refs']=['A1:COMPANY_FACT:'+SYMBOL+':'+('a'*64)]
    assert any('INTERNAL_FACT_USED_AS_OUTER_SOURCE' in issue
        for issue in validate_company_research(output,scopes,required=True))




def test_historical_output_is_not_silently_marked_as_company_researched():
    output=dict(monitor_pool=[dict(symbol=SYMBOL,structural_score=99)])
    assert company_research_coverage([output])['not_assessed_rows']==1
    assert validate_company_research(output,{},required=False)==[]
    assert validate_company_research(output,{},required=True)




@pytest.mark.parametrize('defect',['missing_expense','duplicate','policy_as_revenue','unproven_realized'])
def test_financial_causal_chain_cannot_omit_costs_or_claim_unverified_realization(defect):
    _,scopes,output=fixture();steps=output['monitor_pool'][0]['financial_transmission']
    if defect=='missing_expense':steps[:]=[step for step in steps if step['stage']!='EXPENSE']
    if defect=='duplicate':steps[-1]=deepcopy(steps[0])
    if defect=='policy_as_revenue':steps[3].update(status='REALIZED',source_ref='sector:robots')
    if defect=='unproven_realized':steps[3]['status']='REALIZED'
    assert validate_company_research(output,scopes,required=True)




