"""Attention research is bounded and separate from formal A1/A2/A3 admission."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.a2_theme_research import attention_theme_hypotheses
from liangjian_funnel.pipeline.research.common import _with_daily_emotion_overlay
from liangjian_funnel.pipeline.team_v2_a2_research import prepare_a2_snapshot, _fair_research_slots
from test_team_v2_a2_research import fixture, NOW
from test_wp4_hot100_consumers import full_source


LEAD='603278.SH'


def attention_fixture(tmp_path, mutate=None):
    def reply(raw, attempt):
        raw['theme_reviews'].append(dict(theme_id='NEW_THEME',stage='LATENT',new_entry_policy='WATCH_ONLY',
            penalty_codes=[],support_codes=[],risk_codes=[],supporting_refs=['A2:ATTENTION:'+LEAD],
            opposing_refs=['A2:NEWS'],what_would_change_my_mind=['Dated theme evidence changes']))
        if mutate:mutate(raw,attempt)
    app,args,seen=fixture(tmp_path,reply)
    data=dict(args['snapshot'].data)
    data.update(snapshot_manifest={'as_of':NOW.isoformat()},g0_symbols=[*args['upstream_symbols'],LEAD],
        EASTMONEY_HOT100_SNAPSHOT=full_source(members=[LEAD],at=NOW),
        THS_CONCEPT_MEMBERSHIP=dict(available=True,event_time=NOW.isoformat(),content_hash='a'*64,
            records=[dict(symbol=LEAD,memberships=[dict(concept_thscode='301000.TI',concept_name='新材料概念')])]))
    args['upstream_output']['taxonomy_links']=[dict(theme_id='NEW_THEME',node_id='NEW_THEME:CORE',
        taxonomy='CONCEPT',taxonomy_code='301000.TI',taxonomy_name='新材料概念',
        match_method='MATURE_THEME_REGISTRY_EXACT_NAME')]
    args['upstream_output']['monitor_pool']=[dict(symbol=LEAD,status='MONITOR',reason_codes=['FUNDAMENTAL_UNPROVEN'])]
    args['upstream_output'],_= _with_daily_emotion_overlay(args['upstream_output'],data,set(data['g0_symbols']))
    args['snapshot']=replace(args['snapshot'],data=data)
    return app,args,seen


def test_overlay_keeps_every_original_partition_and_separates_non_robot_attention(tmp_path):
    _,args,_=attention_fixture(tmp_path)
    output=args['upstream_output'];lead=output['independent_research_intake']['candidates'][0]
    assert lead['symbol']==LEAD and lead['original_a1_partition']=='monitor_pool'
    assert lead['formal_a1_member'] is False and lead['execution_permission']=='BLOCKED'
    assert lead['status']=='DEFERRED_A2_RESEARCH_BUDGET'
    assert output['monitor_pool']==[dict(symbol=LEAD,status='MONITOR',reason_codes=['FUNDAMENTAL_UNPROVEN'])]
    assert LEAD not in {r['symbol'] for r in output['active_research_pool']}
    assert output['daily_emotion_overlay']['added_count']==0
    assert {r['theme_id'] for r in attention_theme_hypotheses([lead])[LEAD]}=={'NEW_THEME'}


@pytest.mark.parametrize('defect,reason', [('g0','OUTSIDE_FROZEN_G0'),('risk','A1_EMOTION_MAJOR_RISK'),
    ('tradability','A1_EMOTION_NOT_TRADABLE'),('missing','DATED_EXACT_THEME_MEMBERSHIP_UNPROVEN'),
    ('future_membership','DATED_EXACT_THEME_MEMBERSHIP_UNPROVEN')])
def test_exact_scope_risk_and_dated_membership_fail_closed(tmp_path,defect,reason):
    _,args,_=attention_fixture(tmp_path)
    data=deepcopy(dict(args['snapshot'].data));output=deepcopy(args['upstream_output'])
    if defect=='g0':data['g0_symbols'].remove(LEAD)
    if defect=='risk':data['RISK_EVENTS']=[dict(symbol=LEAD,severity='HIGH')]
    if defect=='tradability':data['TRADABILITY_FLAGS']={LEAD:{'tradable':False}}
    if defect=='missing':data.pop('THS_CONCEPT_MEMBERSHIP')
    if defect=='future_membership':data['THS_CONCEPT_MEMBERSHIP']['event_time']='2026-10-17T00:00:00+08:00'
    result,_=_with_daily_emotion_overlay(output,data,set(data['g0_symbols']))
    assert not result['independent_research_intake']['candidates']
    row=next(r for r in result['independent_research_intake']['dispositions'] if r['symbol']==LEAD)
    assert row['status']=='INELIGIBLE' and row['reason_codes']==[reason]


@pytest.mark.parametrize('defect',['future','hash','count','identity'])
def test_invalid_full_hot100_cannot_be_replaced_by_claimed_intake(tmp_path,defect):
    app,args,_=attention_fixture(tmp_path)
    data=deepcopy(dict(args['snapshot'].data));source=data['EASTMONEY_HOT100_SNAPSHOT']
    if defect=='future':source['as_of']='2026-10-17T00:00:00+08:00'
    if defect=='hash':source['content_hash']='0'*64
    if defect=='count':source['records'].pop()
    if defect=='identity':source['records'][0]['symbol']='FOREIGN'
    args['snapshot']=replace(args['snapshot'],data=data)
    with pytest.raises(ValueError,match='ATTENTION_SOURCE_MISMATCH'):
        prepare_a2_snapshot(app.team_research_adapter,args['snapshot'],args['upstream_symbols'],upstream_output=args['upstream_output'])


def test_actual_adapter_reviews_new_theme_but_never_adds_formal_stock_domain(tmp_path):
    app,args,seen=attention_fixture(tmp_path)
    before=deepcopy((dict(args['snapshot'].data),args['upstream_output']))
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==1
    contract=result.diagnostics['rotation_output_contract']
    assert LEAD not in json.dumps(contract.get('candidate_ids',[]))
    assert LEAD not in {r['symbol'] for pool in ('focus_pool','watch_only_pool','rejected_candidates') for r in result.output[pool]}
    research=next(r for r in result.output['independent_theme_reviews'] if r['theme_id']=='NEW_THEME')
    assert research['execution_authority'] is False and research['review']['theme_score'] is None
    lead=next(r for r in result.output['independent_attention_research_dispositions'] if r['symbol']==LEAD)
    assert lead['status']=='VALIDATED_THEME_RESEARCH' and lead['team_call_ids']
    assert (args['snapshot'].data,args['upstream_output'])==before


def test_raw_model_cannot_put_research_only_symbol_in_focus_even_after_feedback(tmp_path):
    def foreign(raw,attempt):raw['focus_decisions'][0]['symbol']=LEAD
    app,args,seen=attention_fixture(tmp_path,foreign)
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and len(seen)==3
    assert any('DOMAIN' in r or 'SCOPE' in r or 'PARTITION' in r for r in result.reason_codes)
    lead=next(r for r in result.diagnostics['independent_attention_research']['dispositions'] if r['symbol']==LEAD)
    assert lead['status']=='BLOCKED_THEME_RESEARCH'


def test_full_outer_freeze_batches_merge_preserve_independent_review_and_formal_population(tmp_path):
    app,args,seen=attention_fixture(tmp_path)
    prepared=app.team_research_adapter.prepare_a2_snapshot(args['snapshot'],args['upstream_symbols'],upstream_output=args['upstream_output'])
    frozen=prepared.data['_TEAM_A2_SCORE_FREEZE']
    assert set(frozen['review_symbols'])==args['upstream_symbols']
    assert LEAD not in frozen['original_frozen_members']
    assert frozen['themes']['NEW_THEME']['member_count']==0
    result=app._run_a2_batched(**{k:v for k,v in args.items() if k not in ('stage','projection_symbols')})
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==1 and any(r['theme_id']=='NEW_THEME' for r in result.output['independent_theme_reviews'])
    assert next(r for r in result.output['independent_attention_research_dispositions'] if r['symbol']==LEAD)['status']=='VALIDATED_THEME_RESEARCH'


def test_existing_two_batch_plan_delivers_attention_once_without_a_new_call(tmp_path):
    def second_batch(raw,number):
        if number==2:
            raw['theme_reviews']=[r for r in raw['theme_reviews'] if r['theme_id']=='T']
            raw['theme_reviews'][0]['supporting_refs']=['A2:QUANT:000003.SZ']
            raw['focus_decisions'][0].update(symbol='000003.SZ',supporting_refs=['A2:QUANT:000003.SZ'])
    app,args,seen=attention_fixture(tmp_path,second_batch)
    app.settings=app.settings.model_copy(update={'research_a2_batch_size':2})
    result=app._run_a2_batched(**{k:v for k,v in args.items() if k not in ('stage','projection_symbols')})
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==2 and result.diagnostics['batch_count']==2
    assert len([r for r in result.output['independent_theme_reviews'] if r['theme_id']=='NEW_THEME'])==1
    assert len([r for r in result.output['independent_attention_research_dispositions'] if r['symbol']==LEAD])==1


def test_missing_new_theme_review_is_not_counted_as_completed_research(tmp_path):
    app,args,seen=attention_fixture(tmp_path,lambda raw,n:raw.update(theme_reviews=raw['theme_reviews'][:1]))
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and len(seen)==3
    assert any('THEME_COVERAGE' in r for r in result.reason_codes)
    assert next(r for r in result.diagnostics['independent_attention_research']['dispositions'] if r['symbol']==LEAD)['status']=='BLOCKED_THEME_RESEARCH'


def test_prompt_budget_deferral_is_explicit_and_makes_no_request(tmp_path):
    app,args,seen=attention_fixture(tmp_path)
    app.settings=app.settings.model_copy(update={'model_max_input_tokens':1})
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and not seen
    row=next(r for r in result.diagnostics['independent_attention_research']['dispositions'] if r['symbol']==LEAD)
    assert row['status']=='DEFERRED_A2_CALL_NOT_EXECUTED' and row['team_call_ids']==[]


def test_attention_source_recheck_rejects_forged_membership_or_identity(tmp_path):
    app,args,seen=attention_fixture(tmp_path)
    args['upstream_output']['independent_research_intake']['candidates'][0]['symbol']='999999.SZ'
    result=app._run_stage(**args)
    assert result.reason_codes==('TEAM_A2_ATTENTION_SOURCE_MISMATCH',) and not seen


def test_fair_shared_side_cap_preserves_old_public_top30_and_delivers_hot_lead(tmp_path):
    app,args,_=attention_fixture(tmp_path)
    data=dict(args['snapshot'].data);contexts=deepcopy(data['A2_BOTTLENECK_CONTEXT'])
    for i in range(30):
        contexts[f'{100000+i:06d}.SZ']=dict(theme_id='T',deterministic_status='LOCAL_MONITOR',
            deterministic_reason_codes=['EXISTING_MONITOR'],role_breakdown={'relative_strength':50})
    data['A2_BOTTLENECK_CONTEXT']=contexts
    frozen=prepare_a2_snapshot(app.team_research_adapter,replace(args['snapshot'],data=data),args['upstream_symbols'],upstream_output=args['upstream_output'])
    plan=frozen.data['_TEAM_A2_ATTENTION_RESEARCH_FREEZE']
    assert len(frozen.data['A2_QUANT_FILTERED_SAMPLE'])==30
    assert plan['selected_nomination_count']==29 and plan['selected_attention_count']==1
    assert len(plan['selected_slots'])==30 and plan['hypotheses']
    assert next(r for r in plan['dispositions'] if r['symbol']==LEAD)['status']=='PLANNED_EXISTING_A2_CALL'
    assert sum(r['status']=='DEFERRED_A2_SIDE_SAMPLE_CAPACITY' for r in plan['nomination_dispositions'])==1


def two_queues(tmp_path, *, old=30, hot=30, mutate=None):
    app,args,seen=attention_fixture(tmp_path,mutate)
    data=deepcopy(dict(args['snapshot'].data));contexts=data['A2_BOTTLENECK_CONTEXT']
    for i in range(old):
        contexts[f'{100000+i:06d}.SZ']=dict(theme_id='T',deterministic_status='LOCAL_MONITOR',
            deterministic_reason_codes=['EXISTING_MONITOR'],role_breakdown={'relative_strength':100-i})
    symbols=[LEAD]+[f'{610000+i:06d}.SH' for i in range(hot-1)] if hot else []
    data['EASTMONEY_HOT100_SNAPSHOT']=full_source(members=symbols,at=NOW)
    data['g0_symbols']=[*args['upstream_symbols'],*symbols]
    data['THS_CONCEPT_MEMBERSHIP']['records']=[dict(symbol=s,memberships=[
        dict(concept_thscode='301000.TI',concept_name='新材料概念')]) for s in reversed(symbols)]
    args['upstream_output'],_=_with_daily_emotion_overlay(args['upstream_output'],data,set(data['g0_symbols']))
    args['snapshot']=replace(args['snapshot'],data=data)
    return app,args,seen


def test_full_thirty_plus_thirty_queues_really_send_fifteen_each_not_thirty_plus_fifteen(tmp_path):
    from contextlib import closing
    app,args,seen=two_queues(tmp_path)
    result=app._run_a2_batched(**{k:v for k,v in args.items() if k not in ('stage','projection_symbols')})
    assert result.status=='VALIDATED' and len(seen)==1,result.reason_codes
    plan=result.output['independent_research_sampling_plan']
    assert plan['selected_nomination_count']==plan['selected_attention_count']==15
    assert [r['owner_route'] for r in plan['selected_slots']]==['NOMINATION','HOT100']*15
    with closing(app.team_research_adapter.store._connect()) as db:
        raw=db.execute("SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a2-research-source:%'").fetchone()[0]
    values=json.loads(raw)['values']
    assert len(values['A2_QUANT_FILTERED_SAMPLE'])==15
    assert len(values['A2_BOTTLENECK_CONTEXT']['_hot100_independent_research']['candidates'])==15
    assert sum(r['status']=='DEFERRED_A2_SIDE_SAMPLE_CAPACITY' for r in result.output['independent_nomination_research_dispositions'])==15
    assert sum(r['status']=='DEFERRED_A2_SIDE_SAMPLE_CAPACITY' for r in result.output['independent_attention_research_dispositions'])==15


@pytest.mark.parametrize('old,hot,expected',[(0,4,['HOT100']*4),(4,0,['NOMINATION']*4),
    (1,4,['NOMINATION','HOT100','HOT100','HOT100','HOT100'])])
def test_empty_path_fills_from_remaining_original_queue(old,hot,expected):
    slots,_=_fair_research_slots([{'symbol':f'N{i}'} for i in range(old)],
        [{'symbol':f'H{i}'} for i in range(hot)])
    assert [r['owner_route'] for r in slots]==expected


def test_duplicate_symbol_owns_one_slot_first_selected_route_and_tail_backfills():
    original=[{'symbol':f'S{i:02}'} for i in range(45)]
    attention=[{'symbol':f'S{i:02}'} for i in range(30)]
    slots,owners=_fair_research_slots(original,attention)
    assert len(slots)==len(owners)==30
    assert owners['S00']=='NOMINATION'
    assert len({r['symbol'] for r in slots})==30
    # Repeated attention leads do not prevent the old source reaching its tail.
    attention=[{'symbol':'S00'}]*35
    slots,owners=_fair_research_slots(original,attention)
    assert len(slots)==30 and slots[-1]['symbol']=='S29'


def test_fair_plan_records_full_old_tail_and_is_stable_under_source_dict_order(tmp_path):
    app,args,_=two_queues(tmp_path,old=40,hot=30)
    first=prepare_a2_snapshot(app.team_research_adapter,args['snapshot'],args['upstream_symbols'],upstream_output=args['upstream_output'])
    plan=first.data['_TEAM_A2_ATTENTION_RESEARCH_FREEZE']
    assert len(first.data['A2_QUANT_FILTERED_SAMPLE'])==30
    assert plan['nomination_source']['original_candidate_count']==40
    assert len(plan['nomination_dispositions'])==40
    assert next(r for r in plan['nomination_dispositions'] if r['symbol']=='100039.SZ')['status']=='DEFERRED_A2_SIDE_SAMPLE_CAPACITY'
    data=deepcopy(dict(args['snapshot'].data))
    data['A2_BOTTLENECK_CONTEXT']=dict(reversed(list(data['A2_BOTTLENECK_CONTEXT'].items())))
    second=prepare_a2_snapshot(app.team_research_adapter,replace(args['snapshot'],data=data),args['upstream_symbols'],upstream_output=args['upstream_output'])
    assert second.data['_TEAM_A2_ATTENTION_RESEARCH_FREEZE']==plan


def test_unselected_attention_theme_is_not_a_mandatory_model_review(tmp_path):
    app,args,seen=two_queues(tmp_path,old=30,hot=30)
    data=deepcopy(dict(args['snapshot'].data))
    tail=data['EASTMONEY_HOT100_SNAPSHOT']['records'][29]['symbol']
    member=next(r for r in data['THS_CONCEPT_MEMBERSHIP']['records'] if r['symbol']==tail)
    member['memberships']=[dict(concept_thscode='301001.TI',concept_name='仅尾部题材')]
    args['upstream_output']['taxonomy_links'].append(dict(theme_id='UNSELECTED_THEME',node_id='UNSELECTED_THEME:CORE',
        taxonomy='CONCEPT',taxonomy_code='301001.TI',taxonomy_name='仅尾部题材',match_method='MATURE_THEME_REGISTRY_EXACT_NAME'))
    args['upstream_output'],_=_with_daily_emotion_overlay(args['upstream_output'],data,set(data['g0_symbols']))
    args['snapshot']=replace(args['snapshot'],data=data)
    result=app._run_stage(**args)
    assert result.status=='VALIDATED' and len(seen)==1,result.reason_codes
    assert 'UNSELECTED_THEME' not in result.diagnostics['original_theme_review_domain']


def test_full_fair_plan_is_consumed_only_once_across_existing_two_batches(tmp_path):
    from contextlib import closing
    def second_batch(raw,number):
        if number==2:
            raw['theme_reviews']=[r for r in raw['theme_reviews'] if r['theme_id']=='T']
            raw['theme_reviews'][0]['supporting_refs']=['A2:QUANT:000003.SZ']
            raw['focus_decisions'][0].update(symbol='000003.SZ',supporting_refs=['A2:QUANT:000003.SZ'])
    app,args,seen=two_queues(tmp_path,mutate=second_batch)
    app.settings=app.settings.model_copy(update={'research_a2_batch_size':2})
    result=app._run_a2_batched(**{k:v for k,v in args.items() if k not in ('stage','projection_symbols')})
    assert result.status=='VALIDATED' and len(seen)==2,result.reason_codes
    with closing(app.team_research_adapter.store._connect()) as db:
        sources=[json.loads(r[0])['values'] for r in db.execute(
            "SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a2-research-source:%'")]
    assert sorted(len(r['A2_QUANT_FILTERED_SAMPLE']) for r in sources)==[0,15]
    assert sum('_hot100_independent_research' in r['A2_BOTTLENECK_CONTEXT'] for r in sources)==1
    assert len(result.output['independent_nomination_research_dispositions'])==30


def test_upstream_pool_and_link_order_do_not_change_fair_plan_hash(tmp_path):
    app,args,_=two_queues(tmp_path)
    args['upstream_output']['taxonomy_links'].append(dict(theme_id='SECOND_EXACT',node_id='SECOND_EXACT:CORE',
        taxonomy='CONCEPT',taxonomy_code='301000.TI',taxonomy_name='新材料概念',
        match_method='MATURE_THEME_REGISTRY_EXACT_NAME'))
    args['upstream_output'],_=_with_daily_emotion_overlay(args['upstream_output'],args['snapshot'].data,
        set(args['snapshot'].data['g0_symbols']))
    first=prepare_a2_snapshot(app.team_research_adapter,args['snapshot'],args['upstream_symbols'],upstream_output=args['upstream_output'])
    output=deepcopy(args['upstream_output']);output['taxonomy_links'].reverse();output['active_research_pool'].reverse()
    second=prepare_a2_snapshot(app.team_research_adapter,args['snapshot'],args['upstream_symbols'],upstream_output=output)
    assert first.data['_TEAM_A2_ATTENTION_RESEARCH_FREEZE']==second.data['_TEAM_A2_ATTENTION_RESEARCH_FREEZE']


@pytest.mark.parametrize('overlap',[15,30])
def test_shared_slot_keeps_both_proofs_and_non_primary_themes_in_one_transport_row(tmp_path,overlap):
    from contextlib import closing
    app,args,seen=two_queues(tmp_path)
    data=deepcopy(dict(args['snapshot'].data));hot=data['EASTMONEY_HOT100_SNAPSHOT']['records']
    for i in range(overlap):
        old=data['A2_BOTTLENECK_CONTEXT'].pop(f'{100000+i:06d}.SZ')
        data['A2_BOTTLENECK_CONTEXT'][hot[i]['symbol']]=old
    args['snapshot']=replace(args['snapshot'],data=data)
    result=app._run_stage(**args)
    assert result.status=='VALIDATED' and len(seen)==1,result.reason_codes
    plan=result.output['independent_research_sampling_plan']
    assert plan['selected_unique_count']==30
    assert plan['transport_nomination_count']+plan['transport_attention_only_count']==30
    assert len(plan['duplicate_symbols'])==overlap
    with closing(app.team_research_adapter.store._connect()) as db:
        raw=db.execute("SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a2-research-source:%'").fetchone()[0]
    values=json.loads(raw)['values'];sample=values['A2_QUANT_FILTERED_SAMPLE']
    attention=values['A2_BOTTLENECK_CONTEXT']['_hot100_independent_research']
    lead=next(r for r in sample if r['symbol']==LEAD)
    assert lead['theme_id']=='T' and lead['independent_attention_research']['symbol']==LEAD
    assert {r['theme_id'] for r in attention['hypotheses'][LEAD]}=={'NEW_THEME'}
    assert LEAD not in {r['symbol'] for r in attention['candidates']}
    assert len({r['symbol'] for r in sample+attention['candidates']})==len(sample+attention['candidates'])==30
    lead_audit=next(r for r in result.output['independent_attention_research_dispositions'] if r['symbol']==LEAD)
    assert lead_audit['shared_slot'] and lead_audit['status']=='VALIDATED_THEME_RESEARCH'
    nom_audit=next(r for r in result.output['independent_nomination_research_dispositions'] if r['symbol']==LEAD)
    assert nom_audit['status']=='DELIVERED_IN_VALIDATED_A2_CALL'
    assert lead_audit['team_call_ids']==nom_audit['team_call_ids']
    if overlap==30:
        assert len(sample)==30 and attention['candidates']==[]
        assert plan['selected_attention_evidence_count']==30
        assert plan['selected_attention_count']==0  # slots owned by old path, evidence not dropped


def test_unselected_old_candidate_cannot_use_unchanged_public_top30_as_model_nomination_authority(tmp_path):
    def nominate_tail(raw,number):
        raw['model_nominations']=[dict(symbol='100029.SZ',theme_id='T',nomination_thesis='Original evidence',
            quant_filter_reason='EXISTING_MONITOR',why_i_disagree='Frozen fact',falsifiers=['Fact withdrawn'],
            supporting_refs=['A2:NOMINATION:100029.SZ'],opposing_refs=['A2:NEWS'])]
    app,args,seen=two_queues(tmp_path,mutate=nominate_tail)
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and len(seen)==3
    assert any('NOMINATION' in r or 'REFERENCE' in r for r in result.reason_codes)


def test_shared_nomination_slot_cannot_claim_hot100_research_without_required_theme_review(tmp_path):
    app,args,seen=two_queues(tmp_path,mutate=lambda raw,n:raw.update(theme_reviews=raw['theme_reviews'][:1]))
    data=deepcopy(dict(args['snapshot'].data))
    for i,hot in enumerate(data['EASTMONEY_HOT100_SNAPSHOT']['records'][:30]):
        data['A2_BOTTLENECK_CONTEXT'][hot['symbol']]=data['A2_BOTTLENECK_CONTEXT'].pop(f'{100000+i:06d}.SZ')
    args['snapshot']=replace(args['snapshot'],data=data)
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and len(seen)==3
    assert any('THEME_COVERAGE' in r for r in result.reason_codes)
    lead=next(r for r in result.diagnostics['independent_attention_research']['dispositions'] if r['symbol']==LEAD)
    assert lead['shared_slot'] and lead['status']=='BLOCKED_THEME_RESEARCH'


def test_thirty_one_verified_leads_share_one_existing_call_and_preserve_backlog(tmp_path):
    app,args,seen=attention_fixture(tmp_path)
    data=deepcopy(dict(args['snapshot'].data));source=data['EASTMONEY_HOT100_SNAPSHOT']
    symbols=[r['symbol'] for r in source['records'][:31]]
    data['g0_symbols']=[*args['upstream_symbols'],*symbols]
    data['THS_CONCEPT_MEMBERSHIP']['records']=[dict(symbol=s,memberships=[
        dict(concept_thscode='301000.TI',concept_name='新材料概念')]) for s in reversed(symbols)]
    args['upstream_output'],_=_with_daily_emotion_overlay(args['upstream_output'],data,set(data['g0_symbols']))
    args['snapshot']=replace(args['snapshot'],data=data)
    result=app._run_a2_batched(**{k:v for k,v in args.items() if k not in ('stage','projection_symbols')})
    assert result.status=='VALIDATED' and len(seen)==1,result.reason_codes
    rows=result.output['independent_attention_research_dispositions']
    assert sum(r['status']=='VALIDATED_THEME_RESEARCH' for r in rows)==30
    assert next(r for r in rows if r['symbol']==symbols[30])['status']=='DEFERRED_A2_SIDE_SAMPLE_CAPACITY'
    assert not {r['symbol'] for r in result.output['focus_pool']} & set(symbols)


def test_no_formal_call_is_deferred_not_an_extra_model_request(tmp_path):
    app,args,seen=attention_fixture(tmp_path)
    frozen=prepare_a2_snapshot(app.team_research_adapter,args['snapshot'],set(),upstream_output=args['upstream_output'])
    plan=frozen.data['_TEAM_A2_ATTENTION_RESEARCH_FREEZE']
    assert plan['hypotheses']=={} and not seen
    assert next(r for r in plan['dispositions'] if r['symbol']==LEAD)['status']=='DEFERRED_NO_EXISTING_A2_CALL'


def test_taxonomy_order_does_not_change_selected_research_routes(tmp_path):
    _,args,_=attention_fixture(tmp_path)
    data=deepcopy(dict(args['snapshot'].data));output=deepcopy(args['upstream_output'])
    source=data['THS_CONCEPT_MEMBERSHIP']
    source['records'][0]['memberships'].append(dict(concept_thscode='301001.TI',concept_name='其他精确概念'))
    output['taxonomy_links'].append(dict(theme_id='OTHER',node_id='OTHER:CORE',taxonomy='CONCEPT',
        taxonomy_code='301001.TI',taxonomy_name='其他精确概念',match_method='MATURE_THEME_REGISTRY_EXACT_NAME'))
    first,_=_with_daily_emotion_overlay(output,data,set(data['g0_symbols']))
    output['taxonomy_links'].reverse();source['records'][0]['memberships'].reverse()
    second,_=_with_daily_emotion_overlay(output,data,set(data['g0_symbols']))
    def routes(o):return [(r['symbol'],[(x['theme_id'],x['node_id']) for x in r['theme_research_binding']['research_candidate_routes']]) for r in o['independent_research_intake']['candidates']]
    assert routes(first)==routes(second)


def test_real_19_daye_frozen_case_enters_theme_hypothesis_without_monthly_promotion():
    """Exact case replay, not a claim about full historical scheduling/model output."""
    ops=Path(__file__).resolve().parents[2]/'liangjian_team_v2_20261016/artifacts/a4-monitor-20261021'
    fact=ops/'HOT100_19_PUBLIC_ORIGINAL_20261023.json'
    if not fact.exists():pytest.skip('Local immutable real evidence not in portable source checkout')
    assert hashlib.sha256(fact.read_bytes()).hexdigest()=='e400c33a84ef98cd4bb628126ce0e5b8d5178feb2f2f49779d374c0f3ff7ef41'
    sources=sorted(ops.glob('ROBOT_HISTORICAL_PROJECTION_*4a9259fc15c8*'))
    projected=json.loads(sources[-1].read_bytes())
    assert projected['source_file_sha256']=='dc280ae6988a9819992edbf3fc2612cda73b03beaa9e1595b6aa915c2a6a58cf'
    data=deepcopy(projected['projection']['data'])
    data['EASTMONEY_HOT100_SNAPSHOT']=json.loads(fact.read_bytes())['original']
    output={'active_research_pool':[dict(symbol='603728.SH'),dict(symbol='601628.SH')],
        'monitor_pool':[],'rejected_candidates':[]}
    lane_path=ops/'ROBOT_HISTORICAL_RUN_PROJECTION_research_2026-10-19-close-4a9259fc15c8_lane_1_203733.json'
    assert hashlib.sha256(lane_path.read_bytes()).hexdigest()=='30f4ff45721ce849f442748dab31f970fa4988786fa39d05c10b79ab25199693'
    output['taxonomy_links']=json.loads(lane_path.read_bytes())['projection']['stages'][0]['output']['taxonomy_links']
    updated,_=_with_daily_emotion_overlay(output,data,set(data['g0_symbols']))
    leads={r['symbol']:r for r in updated['independent_research_intake']['candidates']}
    assert LEAD in leads,[r['reason_codes'] for r in updated['independent_research_intake']['dispositions'] if r['symbol']==LEAD]
    assert leads[LEAD]['hot100_rank']==70 and leads[LEAD]['formal_a1_member'] is False
    hypotheses=attention_theme_hypotheses([leads[LEAD]])[LEAD]
    assert 'ROBOTICS_ADVANCED_MANUFACTURING' in {h['theme_id'] for h in hypotheses}
    assert all(h['execution_authority'] is False and not h['confirms_catalyst'] for h in hypotheses)
    assert updated['active_research_pool']==output['active_research_pool']
