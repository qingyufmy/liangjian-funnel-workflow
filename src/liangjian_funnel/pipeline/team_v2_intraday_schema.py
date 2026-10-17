"""0315 radar/PMI contracts only; never publish execution_plans or grant orders.

Candidate/profile/announcement facts and envelopes belong to the engine. Model
rankings have no authority over shared slots, cluster caps or the daily two cap.
"""
from collections.abc import Mapping
from copy import deepcopy

from .team_v2_contract import TeamValidation,validate_team_output
from .team_v2_pm_schema import _item,_root,_result,_text,_REFS,_COMMON


def validate_intraday_radar(output,catalog,*,required_envelope,candidates,themes,emotion_state):
    if (not isinstance(candidates,Mapping) or len(candidates)>20 or not isinstance(themes,Mapping)
            or any(not _text(s) or not isinstance(c,Mapping) or not _text(c.get('theme_id'))
                or type(c.get('board_count')) is not int or c['board_count']<0
                or c.get('leader_role') not in {'LEADER','CORE','FOLLOWER'}
                or type(c.get('eligible')) is not bool or not isinstance(c.get('hard_rejects'),list)
                for s,c in candidates.items())
            or any(not isinstance(v,Mapping) for v in themes.values())):
        raise ValueError('RADAR_SERVER_CONTEXT_INVALID')
    if not isinstance(output,Mapping):return TeamValidation(False,('RADAR_RESPONSE_OBJECT_REQUIRED',),(),0)
    common=validate_team_output('A2',output,catalog,session='INTRADAY_RADAR');issues=list(common.issues)
    if set(output)!=_COMMON|{'radar_picks','no_pick_reason'}:issues.append('RADAR_ROOT_FIELDS_INVALID')
    if output.get('envelope')!=required_envelope:issues.append('RADAR_ENVELOPE_BINDING_MISMATCH')
    if (not isinstance(output.get('handoff_notes'),Mapping) or set(output['handoff_notes'])!={'to_a3','to_a4'}
            or any(isinstance(x,list) and len(x)>2 for x in output.get('handoff_notes',{}).values())):
        issues.append('RADAR_HANDOFF_RECIPIENT_INVALID')
    picks=output.get('radar_picks');seen=[]
    if not isinstance(picks,list) or len(picks)>3:issues.append('RADAR_PICK_LIMIT');picks=[]
    if emotion_state!='ON' and picks:issues.append('RADAR_EMOTION_NOT_ON')
    reason=output.get('no_pick_reason')
    if (not picks and not _text(reason)) or (picks and reason!=''):issues.append('RADAR_EMPTY_REASON_INVALID')
    for row in picks:
        if not _item(row,{'symbol','theme_id','leader_role','board_count','entry_thesis','falsifiers'}|_REFS):
            issues.append('RADAR_PICK_FIELDS_INVALID');continue
        symbol=row['symbol'];candidate=candidates.get(symbol) if isinstance(symbol,str) else None
        if candidate is None:issues.append('RADAR_CANDIDATE_DOMAIN_INVALID');continue
        seen.append(symbol);theme=themes.get(candidate['theme_id'],{})
        if (row['theme_id']!=candidate['theme_id'] or row['leader_role']!=candidate['leader_role']
                or row['leader_role'] not in {'LEADER','CORE'} or type(row['board_count']) is not int
                or row['board_count']!=candidate['board_count']):issues.append('RADAR_SOURCE_FACT_MISMATCH')
        if (candidate['eligible'] is not True or candidate['hard_rejects']
                or theme.get('new_entry_policy') not in {'ALLOW','PROBE_ONLY'}
                or theme.get('stage') in {None,'CLIMAX','DIVERGENCE','RETREAT','FADE'}):
            issues.append('RADAR_HARD_GATE_REJECTED')
        if (not _text(row['entry_thesis']) or not isinstance(row['falsifiers'],list)
                or not 1<=len(row['falsifiers'])<=2 or not all(_text(x) for x in row['falsifiers'])):
            issues.append('RADAR_THESIS_OR_FALSIFIER_INVALID')
    if len(seen)!=len(set(seen)):issues.append('RADAR_DUPLICATE_SYMBOL')
    return _result(issues,common)


def validate_pm_intraday(output,catalog,*,required_envelope,drafts,envelopes,first_five_days):
    if (not isinstance(drafts,Mapping) or not isinstance(envelopes,Mapping) or len(drafts)>3
            or set(drafts)!=set(envelopes) or type(first_five_days) is not bool
            or any(not isinstance(d,Mapping) or not _text(d.get('symbol')) or not _text(d.get('cluster'))
                or type(d.get('eligible')) is not bool or not isinstance(d.get('blocked_reasons'),list)
                or envelopes[pid].plan_id!=pid or envelopes[pid].symbol!=d['symbol'] for pid,d in drafts.items())):
        raise ValueError('PMI_SERVER_CONTEXT_INVALID')
    issues,common=_root(output,catalog,session='PM_INTRADAY',keys=_COMMON|{'intraday_ranking','declined'},
        required_envelope=required_envelope,recipients={'to_a4','to_a6'})
    if not isinstance(output,Mapping):return _result(issues,common)
    if isinstance(output.get('handoff_notes'),Mapping) and any(
            isinstance(x,list) and len(x)>2 for x in output['handoff_notes'].values()):issues.append('PMI_HANDOFF_LIMIT')
    rankings=output.get('intraday_ranking');declined=output.get('declined');seen=[]
    if not isinstance(rankings,list):issues.append('PMI_RANKING_INVALID');rankings=[]
    for rank,row in enumerate(rankings,1):
        if not _item(row,{'rank','plan_id','symbol','intended_risk_unit','max_batches','reason_code'}|_REFS):
            issues.append('PMI_RANKING_FIELDS_INVALID');continue
        pid=row['plan_id'];draft=drafts.get(pid) if isinstance(pid,str) else None
        if draft is None:issues.append('PMI_PLAN_DOMAIN_INVALID');continue
        seen.append(pid);risk=envelopes[pid]
        if type(row['rank']) is not int or row['rank']!=rank:issues.append('PMI_RANK_SEQUENCE_INVALID')
        if (draft['eligible'] is not True or draft['blocked_reasons'] or not risk.eligible
                or row['symbol']!=draft['symbol']):issues.append('PMI_INELIGIBLE_OR_SYMBOL_MISMATCH')
        if (row['intended_risk_unit'] not in {'PROBE','STANDARD'}
                or row['intended_risk_unit']=='STANDARD' and (first_five_days or risk.risk_unit!='STANDARD')
                or type(row['max_batches']) is not int or row['max_batches']!=1 or risk.max_batches<1
                or not _text(row['reason_code'])):issues.append('PMI_RISK_OR_BATCH_ESCALATION')
    judgments=list(common.judgments)
    if not isinstance(declined,list):issues.append('PMI_DECLINED_INVALID');declined=[]
    for index,row in enumerate(declined):
        if not isinstance(row,Mapping) or set(row)!={'plan_id','reason_code','note'}:
            issues.append('PMI_DECLINED_FIELDS_INVALID');continue
        pid=row['plan_id']
        if not isinstance(pid,str) or pid not in drafts:issues.append('PMI_PLAN_DOMAIN_INVALID');continue
        seen.append(pid)
        if not _text(row['reason_code']) or not _text(row['note']) or len(row['note'])>80:issues.append('PMI_DECLINED_REASON_INVALID')
        # Authored declined items have no ref fields. Preserve their bounded
        # plan-domain reason as a judgment, without fabricating evidence refs.
        judgments.append(dict(deepcopy(row),path=f'declined[{index}]',schema_basis='AUTHOR315_DECLINED_REASON'))
    if len(seen)!=len(set(seen)) or set(seen)!=set(drafts):issues.append('PMI_PLAN_COVERAGE_INVALID')
    return TeamValidation(not issues,tuple(dict.fromkeys(issues)),tuple(judgments),common.sentinel_count)


def derive_intraday_acceptance(ranking,*,drafts,remaining_daily,free_slots,cluster_used,cluster_caps):
    """Server-only acceptance, sharing original PM slot/cluster accounting."""
    if (type(remaining_daily) is not int or not 0<=remaining_daily<=2 or type(free_slots) is not int or free_slots<0
            or any(type(v) is not int or v<0 for v in (*cluster_used.values(),*cluster_caps.values()))):
        raise ValueError('PMI_CAPACITY_INVALID')
    accepted=[];counts=dict(cluster_used);limit=min(remaining_daily,free_slots)
    for row in ranking:
        pid=row['plan_id'];cluster=drafts[pid]['cluster']
        if len(accepted)>=limit:break
        if counts.get(cluster,0)>=cluster_caps.get(cluster,0):continue
        if pid in accepted:raise ValueError('PMI_DUPLICATE_PLAN')
        accepted.append(pid);counts[cluster]=counts.get(cluster,0)+1
    return dict(accepted_plan_ids=accepted,remaining_daily=remaining_daily-len(accepted),
        free_slots=free_slots-len(accepted),cluster_used=counts,permission='RESEARCH_PLAN_DOMAIN_ONLY_NOT_ORDER_ADMISSION')
