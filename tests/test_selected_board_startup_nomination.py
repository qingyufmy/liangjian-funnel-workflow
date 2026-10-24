"""Synthetic contract tests, not claims about named stocks' historical signals."""
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
import json

import pytest

from liangjian_funnel.pipeline.feature_store import content_hash
from liangjian_funnel.pipeline.selected_board_nomination import selected_board_startup_candidates,merge_startup_nominations
from liangjian_funnel.pipeline.team_v2_a2_research import _fair_research_slots

DAY='2026-10-19';NOW=DAY+'T16:32:47+08:00';SYM='003021.SZ';THEME='ROBOTICS_ADVANCED_MANUFACTURING'


def inputs(symbol=SYM):
    evidence=dict(bar_end=DAY+'T15:00:00+08:00',setup_bar_end=DAY+'T15:00:00+08:00',
        bar_count=100,adjust_mode='none',close=67.22,ma={'5':63.328,'20':67.3515,'60':65.0},
        previous_ma={'5':62.0,'20':67.0,'60':64.0})
    event=dict(symbol=symbol,signals=['REPAIR_WATCH'],evidence=evidence,as_of=NOW,execution_permission='BLOCKED',
        source_hash='a'*64,review_budget_selected=True,identity_version='early-discovery-json/2')
    event['decision_id']=content_hash(dict(version='early-discovery/1',symbol=symbol,evidence=evidence))
    event['setup_event_id']=content_hash(dict(version='early-discovery/1',symbol=symbol,signals=event['signals'],
        setup_bar_end=evidence['setup_bar_end'],adjust_mode='none'))
    data=dict(snapshot_manifest={'as_of':NOW},g0_symbols=['600000.SH'],
        research_candidates=[dict(symbol=symbol,research_eligible=True,exclusion_reasons=[])],
        EARLY_DISCOVERY_SNAPSHOT=dict(version='early-discovery/1',as_of=NOW,records=[event]),
        THS_CONCEPT_MEMBERSHIP=dict(available=True,content_hash='b'*64,event_time=NOW,
            records=[dict(symbol=symbol,memberships=[dict(concept_thscode='885517.TI',concept_name='机器人概念')])]),
        SELECTED_BOARD_SNAPSHOT=dict(available=True,trade_date=DAY,content_hash='c'*64,
            by_symbol={symbol:[dict(strategy_theme_id=THEME,selected_for_rotation=True,primary_rank=1,
                membership_basis='VENDOR_NATIVE_BOARD',membership_reference_hash='d'*64,
                membership_source_id='HITHINK_THS_API',membership_source_board_id='885517.TI')]}))
    out=dict(taxonomy_links=[dict(theme_id=THEME,node_id='robot:core',taxonomy='CONCEPT',taxonomy_code='885517.TI',
        taxonomy_name='机器人概念',match_method='MATURE_THEME_REGISTRY_EXACT_NAME')],active_research_pool=[],monitor_pool=[],rejected_candidates=[])
    return SimpleNamespace(data=data,snapshot_hash='e'*64),out


def source(s,o,formal=()):return selected_board_startup_candidates(s,o,set(formal))


def test_outside_old_g0_and_hot100_can_be_nominated_without_admission():
    s,o=inputs();before=deepcopy((s.data,o));r=source(s,o)
    assert len(r['candidates'])==1
    c=r['candidates'][0]
    assert c['symbol']==SYM and c['execution_permission']=='BLOCKED' and c['formal_a1_member'] is False
    assert not r['new_model_calls_authorized'] and not r['formal_a1_partitions_changed']
    assert (s.data,o)==before and SYM not in s.data['g0_symbols']


@pytest.mark.parametrize('defect',[
    'late_member','late_board','old_day','unknown_member','not_selected','rank9','composite','missing_security',
    'quality','hard_risk','monthly_hard','stale_bar','future_bar','missing_bar_date','forged_setup','forged_id',
    'unknown_signal','bad_hash','conflict','security_conflict','forming_capture'])
def test_missing_or_conflicting_evidence_and_actual_risks_remain_blocked(defect):
    s,o=inputs();d=s.data;e=d['EARLY_DISCOVERY_SNAPSHOT']['records'][0];b=d['SELECTED_BOARD_SNAPSHOT']
    if defect=='late_member':d['THS_CONCEPT_MEMBERSHIP']['event_time']='2026-10-20T00:00:00+08:00'
    if defect=='late_board':b['as_of']='2026-10-20T00:00:00+08:00'
    if defect=='old_day':b['trade_date']='2026-10-16'
    if defect=='unknown_member':d['THS_CONCEPT_MEMBERSHIP']['records']=[]
    if defect=='not_selected':b['by_symbol'][SYM][0]['selected_for_rotation']=False
    if defect=='rank9':b['by_symbol'][SYM][0]['primary_rank']=9
    if defect=='composite':b['by_symbol'][SYM][0]['membership_basis']='STRATEGY_COMPOSITE_NOT_VENDOR_INDEX'
    if defect=='missing_security':d['research_candidates']=[]
    if defect=='quality':d['research_candidates'][0].update(research_eligible=False,exclusion_reasons=['MINIMUM_TURNOVER_NOT_MET'])
    if defect=='hard_risk':d['RISK_EVENTS']=[dict(symbol=SYM,severity='HIGH')]
    if defect=='monthly_hard':o['monitor_pool']=[dict(symbol=SYM,hard_risk_events=[{'kind':'FRAUD'}])]
    if defect=='stale_bar':e['evidence']['bar_end']='2026-10-16T15:00:00+08:00'
    if defect=='future_bar':e['evidence']['bar_end']='2026-10-20T15:00:00+08:00'
    if defect=='missing_bar_date':e['evidence'].pop('bar_end')
    if defect=='forged_setup':e['setup_event_id']='f'*64
    if defect=='forged_id':e['decision_id']='f'*64
    if defect=='unknown_signal':e['signals']=['POSTHOC_WINNER']
    if defect=='bad_hash':e['source_hash']='unproven'
    if defect=='conflict':d['EARLY_DISCOVERY_SNAPSHOT']['records'].append({**e,'source_hash':'f'*64})
    if defect=='security_conflict':d['research_candidates'].append({**d['research_candidates'][0],'research_eligible':False})
    if defect=='forming_capture':d['snapshot_manifest']['as_of']=DAY+'T14:00:00+08:00'
    r=source(s,o);assert not r['candidates'],(defect,r)


def test_formal_symbols_are_not_claimed_as_new_coverage():
    s,o=inputs();r=source(s,o,{SYM})
    assert r['candidates']==[] and r['dispositions'][0]['status']=='EXISTING_FORMAL_A2_DOMAIN'


def test_formal_a1_member_keeps_original_gate_contract_even_if_outside_a2_batch():
    s,o=inputs();o['active_research_pool']=[dict(symbol=SYM,status='ACTIVE')]
    r=source(s,o)
    assert r['candidates']==[] and r['dispositions'][0]['status']=='EXISTING_FORMAL_A1_CONTRACT'


def test_non_robotics_control_is_not_relabelled_by_price_signal():
    s,o=inputs('601628.SH');s.data['THS_CONCEPT_MEMBERSHIP']['records']=[]
    assert source(s,o)['candidates']==[]


def test_research_eligible_exchange_note_is_not_relabelled_as_quality_failure():
    s,o=inputs('920002.BJ');s.data['research_candidates'][0]['exclusion_reasons']=['BJ_RESEARCH_ONLY']
    c=source(s,o)['candidates'][0]
    assert c['original_security_reasons']==['BJ_RESEARCH_ONLY'] and c['execution_permission']=='BLOCKED'


def test_reordered_duplicate_equal_events_do_not_change_source_or_slot_order():
    s,o=inputs();expected=source(s,o)
    s.data['EARLY_DISCOVERY_SNAPSHOT']['records']*=2
    assert source(s,o)==expected


def test_shared_nomination_keeps_original_reason_and_ranking_but_retains_startup_proof():
    s,o=inputs();startup=source(s,o)
    old=dict(symbol=SYM,theme_id='OLD_THEME',selected_board_theme_match=False,relative_strength='90',
             original_context_sha256='f'*64,quant_filter_reason='OLD_REASON')
    merged=merge_startup_nominations({'candidates':[old]},startup)['candidates']
    assert len(merged)==1 and merged[0]['theme_id']=='OLD_THEME' and merged[0]['quant_filter_reason']=='OLD_REASON'
    assert merged[0]['selected_board_startup_research']['symbol']==SYM


@pytest.mark.parametrize('defect',[None,'future','hash','candidate_only','wrong_day','forming_rank','forming_time','nan'])
def test_full_reference_rank_is_visible_outside_g0_but_never_substituted_when_unproven(defect):
    s,o=inputs()
    reference=dict(schema_version='a2-reference-relative-strength/1',available=True,as_of=NOW,
        evaluation_trade_date=DAY,denominator_scope='FULL_MARKET_REFERENCE',reference_symbol_count=2,
        reference_symbols_hash='f'*64,observed_symbol_count=2,return_window_sessions=20,
        values={SYM:97.5,'601628.SH':2.5})
    reference['values_hash']=content_hash(reference['values'])
    if defect=='future':reference['as_of']='2026-10-20T00:00:00+08:00'
    if defect=='hash':reference['values_hash']='0'*64
    if defect=='candidate_only':reference['denominator_scope']='CANDIDATE_SCOPE_FALLBACK'
    if defect=='wrong_day':reference['evaluation_trade_date']='2026-10-16'
    if defect=='forming_rank':reference['return_window_sessions']=1
    if defect=='forming_time':reference['as_of']=DAY+'T14:00:00+08:00'
    if defect=='nan':reference['values'][SYM]='NaN';reference['values_hash']=content_hash(reference['values'])
    s.data['A2_FACTOR_SNAPSHOT']={'reference_relative_strength':reference}
    row=source(s,o)['candidates'][0]
    assert row['relative_strength']==('97.5' if defect is None else None)
    assert row['execution_permission']=='BLOCKED'


def test_30_slots_remain_two_lane_fair_and_empty_formal_batch_stays_deferred():
    s,o=inputs();startup=source(s,o)
    nom=merge_startup_nominations({'candidates':[]},startup)['candidates']
    assert nom[0]['source_refs']==['A2:STARTUP:'+SYM]
    nom += [dict(symbol=f'{n:06d}.SZ') for n in range(50)]
    hot=[dict(symbol=f'{n+100:06d}.SZ') for n in range(50)]
    slots,_=_fair_research_slots(nom,hot,maximum=30)
    assert len(slots)==30 and sum(r['owner_route']=='NOMINATION' for r in slots)==15
    assert sum(r['owner_route']=='HOT100' for r in slots)==15 and slots[0]['symbol']==SYM
    assert _fair_research_slots(nom,hot,maximum=0)==([],{})


@pytest.mark.parametrize('value',[0.0,0.5,1.0,99.5,100.0])
@pytest.mark.parametrize('reference',[True,False])
def test_explicit_percentile_source_never_guesses_ratio_scale(value,reference):
    s,o=inputs()
    if reference:
        values={SYM:value}
        rank=dict(schema_version='a2-reference-relative-strength/1',available=True,as_of=NOW,
            evaluation_trade_date=DAY,denominator_scope='FULL_MARKET_REFERENCE',reference_symbol_count=1,
            reference_symbols_hash='f'*64,observed_symbol_count=1,return_window_sessions=20,
            values=values,values_hash=content_hash(values))
        s.data['A2_FACTOR_SNAPSHOT']={'reference_relative_strength':rank}
    else:
        leader=dict(available=True,availability_state='OBSERVED_VALUE',source='LOCAL_THEME_CROSS_SECTION',
                    relative_strength_percentile=value)
        s.data['A2_FACTOR_SNAPSHOT']=dict(available=True,content_hash='f'*64,as_of=NOW,
            by_symbol={SYM:dict(leader_structure=leader,stock_trend_structure={'trade_date':DAY})})
    candidate=source(s,o)['candidates'][0]
    assert float(candidate['relative_strength'])==value
    assert candidate['execution_permission']=='BLOCKED'


def test_actual_adapter_delivers_startup_question_in_one_existing_call_without_formal_promotion(tmp_path):
    from test_hot100_independent_research_intake import attention_fixture
    def reply(raw,attempt):
        raw['theme_reviews'].append(dict(theme_id=THEME,stage='LATENT',new_entry_policy='WATCH_ONLY',
            penalty_codes=[],support_codes=[],risk_codes=[],supporting_refs=['A2:STARTUP:'+SYM],
            opposing_refs=['A2:NEWS'],what_would_change_my_mind=['Startup invalidated']))
    app,args,seen=attention_fixture(tmp_path,reply)
    minimal,out=inputs();data=deepcopy(dict(args['snapshot'].data))
    from test_team_v2_a2_research import NOW as clock
    # Use the fixture's real research session, not a fake wall-clock rewrite.
    day=clock.date().isoformat();e=minimal.data['EARLY_DISCOVERY_SNAPSHOT']['records'][0]
    for k in ('bar_end','setup_bar_end'):e['evidence'][k]=day+'T15:00:00+08:00'
    e['as_of']=clock.isoformat();e['decision_id']=content_hash(dict(version='early-discovery/1',symbol=SYM,evidence=e['evidence']))
    e['setup_event_id']=content_hash(dict(version='early-discovery/1',symbol=SYM,signals=e['signals'],setup_bar_end=e['evidence']['setup_bar_end'],adjust_mode='none'))
    minimal.data['EARLY_DISCOVERY_SNAPSHOT']['as_of']=clock.isoformat()
    for key in ('research_candidates','EARLY_DISCOVERY_SNAPSHOT'):data[key]=minimal.data[key]
    data['THS_CONCEPT_MEMBERSHIP']['records']+=minimal.data['THS_CONCEPT_MEMBERSHIP']['records']
    data['SELECTED_BOARD_SNAPSHOT']=minimal.data['SELECTED_BOARD_SNAPSHOT'];data['SELECTED_BOARD_SNAPSHOT']['trade_date']=day
    args['upstream_output']['taxonomy_links']+=out['taxonomy_links']
    # Re-freeze the original Hot100 intake after its membership source changes.
    from liangjian_funnel.pipeline.research.common import _with_daily_emotion_overlay
    args['upstream_output'],_=_with_daily_emotion_overlay(args['upstream_output'],data,set(data['g0_symbols']))
    args['snapshot']=replace(args['snapshot'],data=data)
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==1 and SYM in json.dumps(seen[0]['messages'])
    row=next(r for r in result.output['selected_board_startup_research_dispositions'] if r['symbol']==SYM)
    assert row['status']=='DELIVERED_IN_VALIDATED_A2_CALL' and row['team_call_ids']
    assert SYM not in {r['symbol'] for key in ('focus_pool','watch_only_pool','rejected_candidates') for r in result.output[key]}
