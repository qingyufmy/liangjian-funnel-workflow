from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime.news_data_limits import (stock_news_detail,publication_news_detail,
    merge_news_detail,morning_news_facts,cap_news_envelopes,digest,NEWS_TAG)
from liangjian_funnel.runtime.regeneration_data_limits import a6_gap_requirements,gap_reviewed_ids,restrict_unreviewed
from liangjian_funnel.runtime.risk_engine import RiskEnvelope
from liangjian_funnel.runtime.team_v2_pm_policy import project_pm_values,derive_pm_fallback
from liangjian_funnel.pipeline.team_v2_pm_schema import derive_pm_slots
from liangjian_funnel.pipeline.market_aggregates import build_news_heat_snapshot

SYMBOL='002850.SZ'
def health(available=True,complete=True):
    return dict(source_id='open_news.eastmoney_stock.002850_SZ',available=available,
        reason_code='OK' if available else 'OPEN_NEWS_REQUEST_FAILED',checked_at='2026-10-29T17:00:00+08:00',
        details=dict(channel='eastmoney_stock',complete=complete,usable_item_count=0))

def heat(available=True,complete=True):
    return dict(as_of='2026-10-29T17:00:00+08:00',available=True,items=[],stock_source_health=[health(available,complete)])

def envelope(pid='p',limited=False):
    return RiskEnvelope(pid,SYMBOL,'2026-10-30',True,'PROBE' if limited else 'STANDARD',D(1000),1,1000,(),(),
        purpose='PLANNING',plan_risk_unit='PROBE' if limited else 'STANDARD',
        degradation_tags=('DATA_LIMITED',NEWS_TAG) if limited else ())

def test_actual_news_aggregate_retains_failed_individual_source_despite_healthy_market_news():
    rows=[health(False),dict(source_id='open_news.cls_roll',available=True)]
    result=build_news_heat_snapshot(dict(fact_groups={},source_health=rows),[SYMBOL],as_of=datetime.fromisoformat('2026-10-29T17:00:00+08:00'))
    assert result['available'] is True and result['stock_source_health']==[rows[0]]
    assert stock_news_detail(result,SYMBOL)['reason_codes']==['STOCK_NEWS_SOURCE_UNAVAILABLE']

def test_healthy_empty_is_covered_but_missing_partial_duplicate_health_is_not():
    assert stock_news_detail(heat(),SYMBOL) is None
    for value in ({},dict(available=True,items=[{'symbol':SYMBOL}]),heat(True,False),
            dict(stock_source_health=[health(),health()])):
        assert stock_news_detail(value,SYMBOL)['reason_codes']==['STOCK_NEWS_SOURCE_HEALTH_UNPROVEN']

def test_real_recipe_hash_and_exact_symbol_binding_are_required():
    data=dict(NEWS_HEAT_SNAPSHOT=heat(False),A2_STOCK_NEWS_SCOPE=[SYMBOL]);recipe=dict(base_snapshot_hash='base',stage='A2',**data)
    node=dict(recipe_kind='A2_STOCK_NEWS',status='CAPTURED',data_recipe=data,hash_recipe=recipe,snapshot_hash=digest(recipe))
    audit=SimpleNamespace(stage='A2',snapshot_proof=dict(chain=[node]))
    detail=publication_news_detail([audit],SYMBOL);assert detail['reason_codes']==['STOCK_NEWS_SOURCE_UNAVAILABLE']
    broken=deepcopy(node);broken['data_recipe']['NEWS_HEAT_SNAPSHOT']['stock_source_health'][0]['available']=True
    assert publication_news_detail([SimpleNamespace(stage='A2',snapshot_proof=dict(chain=[broken]))],SYMBOL)['reason_codes']==['STOCK_NEWS_SOURCE_HEALTH_UNPROVEN']
    assert publication_news_detail([audit],'000001.SZ') is None

def test_merge_preserves_original_history_gap_and_raw_payload():
    raw=dict(symbol=SYMBOL,risk_unit='STANDARD',source_data_limits=dict(authority='0589_PLAN_HISTORY_GAPS',reason_codes=['HISTORY_MISSING'],history_readiness={'status':'MISSING'}))
    before=deepcopy(raw);out=merge_news_detail(raw,stock_news_detail(heat(False),SYMBOL))
    assert raw==before and out['risk_unit']=='PROBE' and out['NEWS_DATA_LIMITED']
    assert out['source_data_limits']['reason_codes']==['HISTORY_MISSING','STOCK_NEWS_SOURCE_UNAVAILABLE']
    assert out['source_data_limits']['prior_source_data_limits']==raw['source_data_limits']

def test_pm_projection_keeps_exact_news_gap_and_its_provenance():
    payload=merge_news_detail(dict(symbol=SYMBOL),stock_news_detail(heat(False),SYMBOL))
    values=dict(EVIDENCE_CATALOG=dict(items=[dict(id='plan:p',category='PLAN',value=dict(plan_id='p',publication_facts=payload))]),PLANS=['duplicate'])
    out=project_pm_values(values)
    assert out['EVIDENCE_CATALOG']['items'][0]['value']['publication_facts']['source_data_limits']==payload['source_data_limits']
    assert out['PLANS']=={'source':'EVIDENCE_CATALOG','plan_evidence_ids':['plan:p']}

@pytest.mark.parametrize('treatment,action,accepted',[
    ('KEEP_WITH_GAP','KEEP',True),('DOWNGRADE','TIGHTEN_TO_PROBE',True),('REJECT','SUSPEND',True),
    ('REJECT','KEEP',False),('DOWNGRADE','KEEP',False),('keep_with_gap','KEEP',False),(None,'KEEP',False),([], 'KEEP',False)])
def test_exact_a6_treatment_reason_and_reference_required(treatment,action,accepted):
    payload=merge_news_detail(dict(symbol=SYMBOL),stock_news_detail(heat(False),SYMBOL))
    catalog=dict(items=[dict(id='plan:p',category='PLAN',value=dict(plan_id='p',publication_facts=payload))])
    requirements=a6_gap_requirements(catalog,[envelope(limited=True)])
    row=dict(plan_id='p',adjustment=action,gap_review=treatment,reason_code='DATA_LIMITED_REVIEWED',
        supporting_refs=['plan:p'],what_would_change_my_mind=['STOCK_NEWS_SOURCE_UNAVAILABLE'])
    assert ('p' in gap_reviewed_ids(dict(envelope_adjustments=[row]),requirements)) is accepted
    row['supporting_refs']=[]
    assert not gap_reviewed_ids(dict(envelope_adjustments=[row]),requirements)
    assert not restrict_unreviewed([envelope(limited=True)])[0].eligible

def test_pm_top2_clean_then_news_limited_in_third_without_losing_model_order():
    env=(envelope('gap',True),envelope('clean1'),envelope('clean2'))
    clusters={e.plan_id:e.plan_id for e in env}
    ranked=[dict(rank=i,plan_id=e.plan_id,cluster=e.plan_id,supporting_refs=[],opposing_refs=[]) for i,e in enumerate(env,1)]
    slots=derive_pm_slots(ranked,envelopes=env,plan_clusters=clusters,free_slots=3)['slot_plan']
    assert [s['primary_plan_id'] for s in slots]==['clean1','clean2','gap']
    fallback=derive_pm_fallback(envelopes=env,plan_clusters=clusters,priority=list(clusters),free_slots=3,
        max_new_entries=3,a6_validated=True,utilization_cap=50)
    assert [s['primary_plan_id'] for s in fallback['slot_plan']]==['clean1','clean2','gap']
    assert all(a['intended_risk_unit']=='PROBE' for a in fallback['plan_allocations'])

def test_old_candidate_unknown_is_not_news_healthy_or_a6_eligible_by_assumption():
    facts=[dict(plan_id='p',symbol=SYMBOL,source_row_sha256='original',publication_facts=dict(risk_unit='STANDARD'))]
    original=deepcopy(facts);out=morning_news_facts(facts)
    assert facts==original and out[0]['publication_facts']['source_data_limits']['reason_codes']==['STOCK_NEWS_SOURCE_HEALTH_UNPROVEN']
    capped=cap_news_envelopes([envelope()],{'p'})[0]
    assert capped.risk_unit=='PROBE' and capped.risk_budget<=envelope().risk_budget and capped.max_shares_cap<=1000

def test_later_a1_projection_cannot_erase_news_review_requirement():
    from liangjian_funnel.runtime.regeneration_data_limits import project_data_limited
    raw=merge_news_detail(dict(symbol=SYMBOL),stock_news_detail(heat(False),SYMBOL))
    result=project_data_limited(raw,dict(authority='0589_A1_SOURCE_GAPS',reason_codes=['A1_MISSING']))
    catalog=dict(items=[dict(id='plan:p',category='PLAN',value=dict(plan_id='p',publication_facts=result))])
    req=a6_gap_requirements(catalog,[envelope(limited=True)])
    assert req['p']['explicit_gap_treatment_required'] and 'STOCK_NEWS_SOURCE_UNAVAILABLE' in req['p']['reason_codes']

def test_real_morning_unknown_news_requires_original_a6_gap_review(tmp_path):
    import sqlite3,json
    from test_team_v2_morning import fixture,client,advance,CAL
    from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
    settings,clock=fixture(tmp_path)
    with sqlite3.connect(settings.state_db_path) as db:
        raw=json.loads(db.execute('SELECT payload_json FROM execution_plans').fetchone()[0])
        raw.pop('news_source_health_checked');raw.pop('news_source_health_evidence')
        db.execute('UPDATE execution_plans SET payload_json=?',(json.dumps(raw),))
    api,seen=client(settings)
    frozen=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    assert NEWS_TAG in frozen['envelopes'][0]['degradation_tags']
    advance(clock,8,35)
    result=run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)
    assert result['status']!='READY' and not result['result']['envelopes'][0]['eligible']
    assert len(seen)==1
