"""Actual request edge and strict rejection of invented per-stock gaps."""
from copy import deepcopy
from dataclasses import replace
import json
from decimal import Decimal
import pytest
from test_team_v2_risk_session import fixture
from liangjian_funnel.runtime.team_v2_risk_session import run_risk_premarket
from liangjian_funnel.pipeline.team_v2_effective_risk import nonexistent_gap_ids

def news_case(tmp_path):
    kwargs,seen,raw=fixture(tmp_path)
    catalog=deepcopy(kwargs['replacements']['EVIDENCE_CATALOG'])
    first=kwargs['envelopes'][0];pid=first.plan_id
    item=dict(id='PLAN:'+pid,category='PLAN',value=dict(plan_id=pid,symbol=first.symbol,publication_facts={}))
    healthy=replace(first,plan_id='healthy-p2',symbol='000002.SZ')
    other=dict(id='PLAN:healthy-p2',category='PLAN',value=dict(plan_id=healthy.plan_id,symbol=healthy.symbol,publication_facts={}))
    catalog['categories'].append('PLAN');catalog['items'].extend([item,other])
    item['value'].setdefault('publication_facts',{})['source_data_limits']=dict(
        authority='LJ103_STOCK_NEWS_SOURCE_GAPS',reason_codes=['STOCK_NEWS_SOURCE_UNAVAILABLE'])
    kwargs['replacements']=dict(kwargs['replacements'],EVIDENCE_CATALOG=catalog,PLANS=[item['value'],other['value']])
    kwargs['envelopes']=tuple(replace(e,degradation_tags=tuple(e.degradation_tags)+('DATA_LIMITED','NEWS_DATA_LIMITED'))if e.plan_id==pid else e for e in kwargs['envelopes'])+(healthy,)
    clean=deepcopy(raw['envelope_adjustments'][0]);clean['plan_id']=healthy.plan_id;raw['envelope_adjustments'].append(clean)
    row=next(r for r in raw['envelope_adjustments']if r['plan_id']==pid)
    row.update(adjustment='SUSPEND',gap_review='REJECT',reason_code='DATA_LIMITED_REVIEWED',
        supporting_refs=[item['id']],what_would_change_my_mind=['STOCK_NEWS_SOURCE_UNAVAILABLE; independent geometry rejection remains binding'])
    return kwargs,seen,raw,pid

def test_actual_native_request_lists_gap_and_non_gap_domains_separately(tmp_path):
    kwargs,seen,raw,pid=news_case(tmp_path)
    result=run_risk_premarket(**kwargs)
    assert len(seen)==1
    wire=json.loads(seen[0]['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
    contract=wire['server_output_contract']
    assert contract['source_gap_contract_version']=='source-gap-explicit-review/2'
    assert set(contract['data_limited_plan_reviews'])=={pid}
    assert set(contract['plans_without_declared_source_gaps'])=={e.plan_id for e in kwargs['envelopes']}-{pid}
    assert 'already rejected' in contract['review_instruction'] and 'omit gap_review' in contract['review_instruction']
    # A valid explicit refusal is retained; no source/geometry rejection revived.
    assert not next(e for e in result.envelopes if e.plan_id==pid).eligible

@pytest.mark.parametrize('reason,review',[
    ('DATA_LIMITED_REVIEWED',False),('SOME_OTHER_RISK',True)])
def test_nonexistent_gap_never_evades_validator_by_changing_reason(reason,review):
    raw={'envelope_adjustments':[dict(plan_id='healthy',reason_code=reason)]}
    if review:raw['envelope_adjustments'][0]['gap_review']='KEEP_WITH_GAP'
    assert nonexistent_gap_ids(raw,{'limited':{}})=={'healthy'}

@pytest.mark.parametrize('field',['exception_actions','overnight_reviews','risk_event_attribution'])
def test_missing_inactive_root_field_still_rejects_real_client_reply(tmp_path,field):
    kwargs,seen,raw,pid=news_case(tmp_path)
    del raw[field]
    result=run_risk_premarket(**kwargs)
    assert len(seen)==1 and result.status=='DEGRADED'
    assert not next(e for e in result.envelopes if e.plan_id==pid).eligible
    import sqlite3
    with sqlite3.connect(tmp_path/'model.db') as conn:
        validated,issues=conn.execute('SELECT validated,issues_json FROM model_calls').fetchone()
    assert not validated and 'A6_ROOT_FIELDS_INVALID' in json.loads(issues)

def test_explicit_news_reply_with_invented_gap_cannot_use_server_projection():
    from test_effective_a6_lj099 import original
    from liangjian_funnel.runtime.a6_effective_review import effective_review
    value,args=original();raw=deepcopy(value['raw'])
    # Original real retained reply contains an invented healthy-stock gap;
    # keep the legacy fixture bytes untouched and opt in only the NEWS contract.
    assert nonexistent_gap_ids(raw,args['requirements'])
    next(iter(args['requirements'].values()))['explicit_gap_treatment_required']=True
    assert effective_review(raw,**args) is None
