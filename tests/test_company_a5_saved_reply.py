"""Actual HTTP #12 is a permanent negative, not a response to repair in place."""
from copy import deepcopy
from pathlib import Path
import json
from liangjian_funnel.pipeline.feature_store import content_hash
from liangjian_funnel.pipeline.company_audit_policy import (company_a5_source_policy,
    validate_company_a5_probe,offline_company_a5_review)


def saved():
    value=json.loads((Path(__file__).parent/'fixtures/company_a5_original_20261025.json').read_bytes())
    assert value['original_result_sha256']=='7fb5f201fb47c1bb12bfde9f2af106b2f2eea0a2d3f5b32295d0501e950899d7'
    assert content_hash(value['output'])==value['original_output_hash']=='ae84ee072f3f159974bb100277eaa91b5026689f4034a074f4943cf938dd467a'
    value['legacy_context']['symbols']=set(value['legacy_context']['symbols'])
    return value


def test_saved_reply_rejects_semantics_even_with_correct_current_identities():
    value=saved();result=validate_company_a5_probe(value['output'],value['legacy_context'])
    assert not result.valid
    assert set(result.issues)=={'COMPANY_A5_UNVERIFIED_NOT_UNREALIZED:003021.SZ',
        'COMPANY_A5_UNVERIFIED_NOT_UNREALIZED:300296.SZ','COMPANY_A5_UNAPPROVED_MARKET_TRIGGER:003021.SZ'}
    view=offline_company_a5_review(value['output'],value['legacy_context'])
    assert not view['original_model_admitted'] and view['model_http']==0
    assert view['current_states']['003021.SZ']['a3_cases'][0]['stock_behavior_type']=='TREND'
    assert view['current_states']['300296.SZ']['a3_cases'][0]['technical_status']=='HARD_REJECT'
    assert all(not v['execution_authority'] for v in view['realization_facts'].values())


def test_separate_offline_positive_preserves_current_facts_and_original_reply():
    value=saved();before=content_hash(value['output']);raw=deepcopy(value['output']);context=deepcopy(value['legacy_context'])
    policy=company_a5_source_policy(context['current_bindings']);context.update(policy)
    for row in raw['company_reviews']:
        row['realized_vs_unrealized']=row['realized_vs_unrealized'].replace('机器人细分收入属未实现/未核实状态',
            '机器人细分收入贡献金额与占比尚未独立核实，不能据此认定为零或未实现')
        row['realization_fact']=policy['fact_boundaries'][row['symbol']]
    raw['followup_actions']=[deepcopy(v) for v in policy['followup_catalog'].values() if v['owner'] in ('A1','A2','A3')]
    assert validate_company_a5_probe(raw,context).valid
    assert content_hash(value['output'])==before and raw!=value['output']
    assert raw['envelope']==value['output']['envelope']
    assert [r['current_state'] for r in raw['company_reviews']]==[r['current_state'] for r in value['output']['company_reviews']]
    assert [r['company_quality_vs_theme_strength'] for r in raw['company_reviews']]==[r['company_quality_vs_theme_strength'] for r in value['output']['company_reviews']]
    # No number matching is needed to evade the boundary: even an entirely
    # nonnumeric invented strategy condition is forbidden by the catalog.
    raw['followup_actions'][0]['trigger']='涨停突破后才允许确认机器人收入'
    assert not validate_company_a5_probe(raw,context).valid
