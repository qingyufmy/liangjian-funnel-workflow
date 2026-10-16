"""Claude's A5 2.0.3 JSON contract; research only, never execution authority."""
from collections.abc import Mapping
import json
import re
from decimal import Decimal

from .team_v2_contract import TeamValidation, validate_team_output

ROOT=set('envelope schema_version review_kind trade_date overall_verdict executive_summary team_scorecard '
    'a2_review a3_review a4_review a6_review trader_vs_deterministic risk_governor_value '
    'model_value_assessment signal_reviews missed_opportunity_reviews core_defects method_feedback '
    'hypotheses handoff_notes what_would_change_my_mind unresolved_questions'.split())
REFS={'evidence_ids','opposing_refs'}
OPTIONAL={'opposing_search_scope'}
ROLES={'A1','A2','A3','A4','A6'}


def _text(value):return isinstance(value,str) and bool(value.strip()) and len(value)<=2000


def _texts(value,limit=8):
    return isinstance(value,list) and len(value)<=limit and all(_text(v) for v in value)


def _item(row,keys,optional=frozenset()):
    return isinstance(row,Mapping) and keys<=set(row) and set(row)<=keys|OPTIONAL|optional


def validate_daily_review(output,catalog,*,required_envelope,trade_date,review_kind,
        signal_domain,counterexample_domain,confirmed_defect_symbols=frozenset(),metrics=None):
    """Domains and numbers come from frozen server facts, not model inference.

    The lexical numeric guard covers the scorecard's numeric claims. Whether a
    narrative conclusion is justified still requires evidence/replay review.
    """
    signal_fields={'symbol','strategy_profile','lifecycle_status'}
    if (review_kind not in {'MIDDAY','POST_CLOSE'} or not isinstance(signal_domain,Mapping)
            or any(not _text(key) or not isinstance(row,Mapping) or not signal_fields<=set(row)
                or not set(row)<=signal_fields|{'plan_id'} or any(not _text(v) for v in row.values())
                for key,row in signal_domain.items())
            or not isinstance(counterexample_domain,set) or not all(_text(x) for x in counterexample_domain)
            or not set(confirmed_defect_symbols)<=counterexample_domain):
        raise ValueError('A5_SERVER_CONTEXT_INVALID')
    common=validate_team_output('A5',output,catalog);issues=list(common.issues)
    def issue(code):issues.append('A5_'+code)
    if not isinstance(output,Mapping):return common
    if set(output)!=ROOT:issue('ROOT_FIELDS_INVALID')
    try:bound=json.dumps(output.get('envelope'),sort_keys=True,allow_nan=False)==json.dumps(required_envelope,sort_keys=True,allow_nan=False)
    except (ValueError,TypeError):bound=False
    if not bound:issue('ENVELOPE_BINDING_MISMATCH')
    if (output.get('schema_version')!='a5-daily-review/2.0.0' or output.get('trade_date')!=trade_date
            or output.get('review_kind')!=review_kind):issue('SESSION_DATE_INVALID')
    if output.get('overall_verdict') not in {'HEALTHY','NEEDS_ATTENTION','DATA_LIMITED','INCIDENT'}:issue('VERDICT_INVALID')
    if not _text(output.get('executive_summary')):issue('SUMMARY_INVALID')
    notes=output.get('handoff_notes')
    if not isinstance(notes,Mapping) or set(notes)!={'to_a1','to_a3','to_a4','to_a6'}:issue('HANDOFF_RECIPIENT_INVALID')
    for name in ('a2_review','a3_review','a4_review','a6_review'):
        row=output.get(name)
        if (not _item(row,{'verdict','summary','strengths','defects','data_limitations'}|REFS)
                or row['verdict'] not in {'HEALTHY','NEEDS_ATTENTION','DATA_LIMITED','NOT_APPLICABLE'}
                or not _text(row['summary']) or any(not _texts(row[key]) for key in ('strengths','defects','data_limitations'))):
            issue('LAYER_REVIEW_INVALID')
    score=output.get('team_scorecard')
    score_keys=set('nav_vs_benchmark utilization_vs_target slot_usage entry_hit_rate drawdown role_kpis attribution'.split())
    if not isinstance(score,Mapping) or set(score)!=score_keys:issue('SCORECARD_INVALID')
    else:
        if (any(not _text(score[key]) for key in score_keys-{'role_kpis','attribution'})
                or not isinstance(score['role_kpis'],Mapping) or set(score['role_kpis'])!=ROLES
                or any(not _text(v) for v in score['role_kpis'].values())):issue('SCORECARD_INVALID')
        rows=score['attribution']
        if (not isinstance(rows,list) or any(not _item(r,{'deviation','owner'}|REFS)
                or r['owner'] not in ROLES|{'ENGINE'} or not _text(r['deviation']) for r in rows)):
            issue('ATTRIBUTION_INVALID')
        def numbers(value):
            if isinstance(value,Mapping):return set().union(set(),*(numbers(v) for v in value.values()))
            if isinstance(value,(list,tuple)):return set().union(set(),*(numbers(v) for v in value))
            if value is None or isinstance(value,bool):return set()
            # Ignore role labels A1..A6, T+1, and reference identifiers.
            return {Decimal(v) for v in re.findall(r'(?<![A-Za-z0-9_:+])[-+]?\d+(?:\.\d+)?',str(value))}
        if not numbers({k:score[k] for k in score_keys-{'attribution'}})<=numbers(metrics or {}):
            issue('SCORECARD_NUMBER_NOT_IN_METRICS')
    schemas={
        'model_value_assessment':({'layer','judgment_type','model_group','control_group','verdict'},
            lambda r:r['layer'] in {'A2','A3','A4','A6'} and r['verdict'] in {'ADDS_VALUE','NO_DIFFERENCE','HARMS','INSUFFICIENT'}),
        'signal_reviews':({'decision_id','symbol','strategy_profile','lifecycle_status','assessment','attribution'},
            lambda r:r['attribution'] in {'GOOD_EXECUTION','SELECTION_ERROR','PLAN_ERROR','CONFIRM_ERROR','RISK_ERROR',
                'DATA_ERROR','MARKET_REVERSAL','DATA_LIMITED','UNCLASSIFIED','NOT_AN_ERROR'}),
        'missed_opportunity_reviews':({'symbol','theme','observed_performance','funnel_drop_stage','assessment','is_confirmed_defect'},
            lambda r:r['funnel_drop_stage'] in ROLES|{'UNRESOLVED'} and type(r['is_confirmed_defect']) is bool
                and (not r['is_confirmed_defect'] or r['symbol'] in confirmed_defect_symbols)),
        'core_defects':({'layer','severity','problem','confidence','blocked_by_data'},
            lambda r:r['layer'] in ROLES|{'ENGINE','BROKER'} and r['severity'] in {'HIGH','MEDIUM','LOW'}
                and r['confidence'] in {'HIGH','MEDIUM','LOW'} and type(r['blocked_by_data']) is bool),
        'hypotheses':({'hypothesis_id','layer','condition','expected_effect','validation_method','success_criteria',
                'falsification_criteria','min_observation_days','automatic_production_change'},
            lambda r:r['layer'] in ROLES|{'ENGINE','BROKER'} and r['validation_method'] in {'REPLAY','PAPER_OBSERVATION'}
                and type(r['min_observation_days']) is int and r['min_observation_days']>=1
                and r['automatic_production_change'] is False)}
    for name,(fields,check) in schemas.items():
        rows=output.get(name)
        if not isinstance(rows,list):issue('JUDGMENT_POOL_INVALID');continue
        for row in rows:
            if not _item(row,fields|REFS,{'plan_id'} if name=='signal_reviews' else frozenset()):
                issue('JUDGMENT_FIELDS_INVALID');continue
            if not check(row) or any(not _text(row[k]) for k in fields if k not in
                    {'is_confirmed_defect','blocked_by_data','min_observation_days','automatic_production_change'}):
                issue('JUDGMENT_VALUE_INVALID')
    for name,domain,key in (('signal_reviews',signal_domain,lambda r:r.get('decision_id')),
            ('missed_opportunity_reviews',counterexample_domain,lambda r:r.get('symbol'))):
        rows=output.get(name)
        if isinstance(rows,list):
            ids=[key(r) for r in rows if isinstance(r,Mapping)]
            encoded=[json.dumps(v,sort_keys=True,default=str) for v in ids]
            if (len(ids)!=len(rows) or len(set(encoded))!=len(ids)
                    or set(encoded)!={json.dumps(v,sort_keys=True) for v in domain}):issue('REVIEW_DOMAIN_INVALID')
    rows=output.get('signal_reviews')
    if isinstance(rows,list):
        for row in rows:
            if not isinstance(row,Mapping) or not isinstance(row.get('decision_id'),str):continue
            expected=signal_domain.get(row['decision_id'])
            if expected is not None and (any(row.get(key)!=expected[key] for key in signal_fields)
                    or ('plan_id' in row and (not _text(row['plan_id']) or row['plan_id']!=expected.get('plan_id')))):
                issue('SIGNAL_METADATA_BINDING_INVALID')
    hypotheses=output.get('hypotheses')
    if isinstance(hypotheses,list):
        ids=[r.get('hypothesis_id') for r in hypotheses if isinstance(r,Mapping)]
        if len(hypotheses)>3 or len(set(map(str,ids)))!=len(ids):issue('HYPOTHESIS_DOMAIN_INVALID')
    for name,fields in (('trader_vs_deterministic',{'summary'}),
            ('risk_governor_value',{'veto_group_vs_pass_group','over_tightening'})):
        row=output.get(name)
        if not _item(row,fields|REFS) or any(not _text(row[k]) for k in fields):issue('COMPARISON_INVALID')
        elif name=='risk_governor_value' and row['over_tightening'] not in {'YES','NO','INSUFFICIENT'}:issue('COMPARISON_INVALID')
    questions=output.get('unresolved_questions')
    if (not isinstance(questions,list) or not questions or any(not isinstance(r,Mapping)
            or set(r)!={'question','reason','resolution'} or not _text(r['question']) or not _text(r['resolution'])
            or r['reason'] not in {'INSUFFICIENT_SAMPLE','MISSING_DATA','CONFOUNDED','REGIME_NOT_OBSERVED'} for r in questions)):
        issue('UNRESOLVED_QUESTIONS_INVALID')
    feedback=output.get('method_feedback')
    if not isinstance(feedback,Mapping) or set(feedback)!=ROLES:issue('METHOD_FEEDBACK_INVALID')
    else:
        # Authored prompt leaves the entry shape open. Accept evidence-bearing
        # judgments, not an unreferenced prose channel around the evidence gate.
        for role,rows in feedback.items():
            if not isinstance(rows,list):issue('METHOD_FEEDBACK_INVALID');continue
            for index,row in enumerate(rows):
                if not isinstance(row,Mapping) or not REFS<=set(row):issue('METHOD_FEEDBACK_INVALID');continue
                if row.get('symbol') is not None:issue('METHOD_FEEDBACK_MUST_NOT_BE_STOCK_SPECIFIC')
    return TeamValidation(not issues,tuple(dict.fromkeys(issues)),common.judgments,common.sentinel_count)
