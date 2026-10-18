from copy import deepcopy
from dataclasses import replace

import pytest

from liangjian_funnel.pipeline.team_v2_intraday_schema import (
    validate_intraday_radar,validate_pm_intraday,derive_intraday_acceptance)
from test_team_v2_trader_session import fixture

CAT={'categories':['MARKET','PLAN'],'items':[{'id':'m','category':'MARKET'},{'id':'p','category':'PLAN'}]}
REFS={'supporting_refs':['p'],'opposing_refs':['m']}
CANDIDATES={'000001.SZ':dict(theme_id='t',board_count=2,leader_role='LEADER',eligible=True,hard_rejects=[])}
THEMES={'t':dict(new_entry_policy='ALLOW',stage='CONFIRMATION')}


def radar():
    return dict(envelope={},session='INTRADAY_RADAR',radar_picks=[dict(symbol='000001.SZ',theme_id='t',
        leader_role='LEADER',board_count=2,entry_thesis='confirmed timing',falsifiers=['leader fails'],**REFS)],
        no_pick_reason='',handoff_notes=dict(to_a3=['review'],to_a4=[]),what_would_change_my_mind=['new facts'])


def check_radar(raw,**changes):
    args=dict(required_envelope={},candidates=CANDIDATES,themes=THEMES,emotion_state='ON')
    return validate_intraday_radar(raw,CAT,**(args|changes))


def test_radar_original_refs_and_empty_off_are_valid_and_not_mutated():
    raw=radar();before=deepcopy(raw);result=check_radar(raw)
    assert result.valid and len(result.judgments)==1 and raw==before
    raw.update(radar_picks=[],no_pick_reason='emotion off')
    assert check_radar(raw,emotion_state='OFF').valid


@pytest.mark.parametrize('fault',['outside','duplicate','bool_count','role','theme','refs','sentinel','stage','hard','off','geometry','missing_notes'])
def test_radar_never_upgrades_engine_candidate_or_evidence(fault):
    raw=radar();candidate=deepcopy(CANDIDATES);themes=deepcopy(THEMES);state='ON';row=raw['radar_picks'][0]
    if fault=='outside':row['symbol']='000002.SZ'
    elif fault=='duplicate':raw['radar_picks']*=2
    elif fault=='bool_count':row['board_count']=True
    elif fault=='role':candidate['000001.SZ']['leader_role']='FOLLOWER'
    elif fault=='theme':row['theme_id']='outside'
    elif fault=='refs':row['supporting_refs']=['outside']
    elif fault=='sentinel':row['opposing_refs']=['NO_OPPOSING_EVIDENCE_FOUND']
    elif fault=='stage':themes['t']['stage']='CLIMAX'
    elif fault=='hard':candidate['000001.SZ']['hard_rejects']=['LOCKED_LIMIT_UP']
    elif fault=='off':state='OFF'
    elif fault=='geometry':row['entry_price']=10
    else:raw['handoff_notes']={}
    assert not check_radar(raw,candidates=candidate,themes=themes,emotion_state=state).valid


def pmi():
    return dict(envelope={},session='PM_INTRADAY',intraday_ranking=[dict(rank=1,plan_id='p1',symbol='000001.SZ',
        intended_risk_unit='PROBE',max_batches=1,reason_code='ACCEPT',**REFS)],declined=[],
        handoff_notes=dict(to_a4=['check'],to_a6=[]),what_would_change_my_mind=['new facts'])


def context(tmp_path):
    args,_,_=fixture(tmp_path)
    return dict(required_envelope={},drafts={'p1':dict(symbol='000001.SZ',cluster='bank',eligible=True,blocked_reasons=[])},
        envelopes=args['envelopes'],first_five_days=True)


@pytest.mark.parametrize('fault',['valid','outside','rank','duplicate','standard','batches','ineligible','symbol','refs','missing','declined_overlap','extra'])
def test_pmi_exact_domain_and_probe_first_five_days(tmp_path,fault):
    raw=pmi();ctx=context(tmp_path);row=raw['intraday_ranking'][0]
    if fault=='outside':row['plan_id']='outside'
    elif fault=='rank':row['rank']=True
    elif fault=='duplicate':raw['intraday_ranking']*=2
    elif fault=='standard':row['intended_risk_unit']='STANDARD'
    elif fault=='batches':row['max_batches']=2
    elif fault=='ineligible':ctx['envelopes']['p1']=replace(ctx['envelopes']['p1'],eligible=False)
    elif fault=='symbol':row['symbol']='000002.SZ'
    elif fault=='refs':row['opposing_refs']=['outside']
    elif fault=='missing':raw['intraday_ranking']=[]
    elif fault=='declined_overlap':raw['declined']=[dict(plan_id='p1',reason_code='DECLINE',note='not ready')]
    elif fault=='extra':row['stop_price']=10
    result=validate_pm_intraday(raw,CAT,**ctx)
    assert result.valid is (fault=='valid')


def test_pmi_decline_is_journaled_without_inventing_authored_ref_fields(tmp_path):
    raw=pmi();raw.update(intraday_ranking=[],declined=[dict(plan_id='p1',reason_code='DATA_LIMITED',note='not ready')])
    result=validate_pm_intraday(raw,CAT,**context(tmp_path))
    assert result.valid and result.judgments[0]['path']=='declined[0]'
    assert 'supporting_refs' not in result.judgments[0]


def test_acceptance_shares_slots_cluster_and_daily_two_without_creating_an_order():
    drafts={pid:dict(cluster=cluster) for pid,cluster in [('a','bank'),('b','bank'),('c','tech')]}
    ranked=[dict(plan_id=pid) for pid in drafts]
    result=derive_intraday_acceptance(ranked,drafts=drafts,remaining_daily=2,free_slots=6,
        cluster_used={'bank':2},cluster_caps={'bank':2,'tech':2})
    assert result['accepted_plan_ids']==['c'] and result['remaining_daily']==1
    assert derive_intraday_acceptance(ranked,drafts=drafts,remaining_daily=0,free_slots=6,
        cluster_used={},cluster_caps={'bank':2,'tech':2})['accepted_plan_ids']==[]


def test_new_session_handoffs_keep_authored_two_item_limit(tmp_path):
    raw=radar();raw['handoff_notes']['to_a3']=['a','b','c']
    assert not check_radar(raw).valid
    raw=pmi();raw['handoff_notes']['to_a4']=['a','b','c']
    assert not validate_pm_intraday(raw,CAT,**context(tmp_path)).valid
