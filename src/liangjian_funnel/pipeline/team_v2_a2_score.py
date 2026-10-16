"""0263 server-owned A2 theme score from the full frozen quant review domain.

Pure projection, no model/output-selected membership, access or publication.
Caller must freeze this global record before splitting transport/model batches.
Legacy score threshold/TOP5 exception are not implemented or changed here.
"""
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib

from ..runtime.team_v2_store import canonical

FACTORS=frozenset(('breadth','turnover_share','capital_flow','leader_structure','tier_structure',
    'profit_effect','catalyst_freshness','index_chain_resonance','weekly_confirmation','agent_1_quality'))
POLICY=dict(version='FROZEN_QUANT_THEME_MEAN_V1',minimum_valid_members=3,
    maximum_minor_missing_weight=.15,missing_weight_basis='ORIGINAL_WEIGHTS_EXCLUDING_GLOBAL_CAPITAL_GAP',
    penalty_mode='MIDPOINT_FIXED_V1',minimum_penalty_points=-30,
    penalty_points=dict(high_crowding=-12,single_limit_up_driven=-10,laggard_spread_core_weak=-12,
        news_hot_capital_out=-10,volume_up_return_down=-12,insufficient_capacity=-10))


def _hash(body):return hashlib.sha256(canonical(body).encode()).hexdigest()


def _number(value):
    if value is None or isinstance(value,bool) or not isinstance(value,(str,int,float,Decimal)):return None
    try:result=Decimal(str(value))
    except InvalidOperation:return None
    return result if result.is_finite() else None


def freeze_theme_scores(snapshot_data, *, review_symbols, weights, policy, source_snapshot_sha256):
    """review_symbols is the engine's whole quant domain, NOT projection_symbols.

    Fields/refs and raw invalid/missing records remain in the original inputs;
    a missing dimension is NULL, never an invented neutral score or weight.
    """
    if (not isinstance(policy,Mapping) or dict(policy)!=POLICY
            or type(policy['minimum_valid_members']) is not int):raise ValueError('A2_SCORE_POLICY_INVALID')
    if (not isinstance(weights,Mapping) or set(weights)!=FACTORS):raise ValueError('A2_SCORE_WEIGHTS_INVALID')
    numeric={name:_number(value) for name,value in weights.items()}
    if any(v is None or not 0<v<=1 for v in numeric.values()) or sum(numeric.values())!=1:
        raise ValueError('A2_SCORE_WEIGHTS_INVALID')
    if (not isinstance(source_snapshot_sha256,str) or len(source_snapshot_sha256)!=64
            or any(c not in '0123456789abcdef' for c in source_snapshot_sha256)):
        raise ValueError('A2_SCORE_SOURCE_HASH_INVALID')
    contexts=snapshot_data.get('A2_BOTTLENECK_CONTEXT')
    if (not isinstance(review_symbols,(set,frozenset)) or any(not isinstance(s,str) or not s for s in review_symbols)
            or not isinstance(contexts,Mapping) or not review_symbols<=set(contexts)):
        raise ValueError('A2_SCORE_REVIEW_DOMAIN_INVALID')
    groups={};original={}
    for symbol in sorted(review_symbols):
        row=contexts[symbol]
        if not isinstance(row,Mapping) or not isinstance(row.get('theme_id'),str) or not row['theme_id'].strip():
            raise ValueError('A2_SCORE_A1_THEME_BINDING_MISSING')
        original[symbol]=dict(theme_id=row['theme_id'],a2_factor_scores=deepcopy(row.get('a2_factor_scores')))
        groups.setdefault(row['theme_id'],[]).append(symbol)
    capital=snapshot_data.get('CAPITAL_FLOW_SNAPSHOT')
    global_missing=[] if isinstance(capital,Mapping) and capital.get('available') is True else ['capital_flow']
    themes={}
    for theme,symbols in sorted(groups.items()):
        factors={}
        for name in sorted(FACTORS):
            members=[];field_refs=[]
            for symbol in symbols:
                raw=original[symbol]['a2_factor_scores']
                raw=raw.get(name) if isinstance(raw,Mapping) else None
                score=_number(raw.get('score')) if isinstance(raw,Mapping) and raw.get('available') is True else None
                if score is not None and 0<=score<=100:
                    ref='A2_BOTTLENECK_CONTEXT:'+symbol+'.a2_factor_scores.'+name
                    members.append(dict(symbol=symbol,score=str(score),source=raw.get('source'),
                        source_refs=deepcopy(raw.get('source_refs')),field_ref=ref,field_sha256=_hash(raw)))
                    field_refs.append(ref)
            mean=sum((Decimal(r['score']) for r in members),Decimal(0))/len(members) if len(members)>=3 else None
            factors[name]=dict(member_count=len(symbols),effective_member_count=len(members),
                mean_score=str(mean) if mean is not None else None,members=members,field_refs=field_refs,
                member_fields_sha256=_hash({s:original[s]['a2_factor_scores'].get(name)
                    if isinstance(original[s]['a2_factor_scores'],Mapping) else None for s in symbols}))
        missing=sorted(n for n in FACTORS if n not in global_missing and factors[n]['mean_score'] is None)
        missing_weight=sum((numeric[n] for n in missing),Decimal(0))
        available={n:w for n,w in numeric.items() if n not in global_missing and n not in missing}
        total=sum(available.values(),Decimal(0));limited=missing_weight>Decimal('.15') or total<=0
        score=None if limited else sum((Decimal(factors[n]['mean_score'])*w for n,w in available.items()),Decimal(0))/total
        status='SCORE_DATA_LIMITED' if limited else 'SCORE_RENORMALIZED_MINOR' if missing else 'SCORE_COMPLETE'
        themes[theme]=dict(theme_id=theme,member_symbols=symbols,member_count=len(symbols),factors=factors,
            missing_factors=missing,globally_removed_factors=global_missing,missing_weight=str(missing_weight),
            available_weight=str(total),effective_weights={n:str(w/total) for n,w in available.items()} if total else {},
            base_score=str(score) if score is not None else None,status=status,
            focus_policy='WATCH_ONLY' if limited else 'ORIGINAL_THRESHOLD_AND_TOP5_EXCEPTION',
            legacy_global_capital_rule_applied=bool(global_missing))
    body=dict(schema_version='team-a2-theme-score/1',source_snapshot_sha256=source_snapshot_sha256,
        review_symbols=sorted(review_symbols),original_frozen_members=original,weights={n:str(w) for n,w in sorted(numeric.items())},
        policy=deepcopy(dict(policy)),capital_flow_global_source=deepcopy(capital),themes=themes)
    return {**body,'score_snapshot_sha256':_hash(body)}


def score_with_penalties(frozen, theme_id, penalty_codes, penalty_rules):
    """0267 fixed classification deductions, original ranges retained for audit.

    Explicit approved -12/-10 values are authoritative, not Python rounding.
    """
    body={k:v for k,v in frozen.items() if k!='score_snapshot_sha256'}
    if _hash(body)!=frozen.get('score_snapshot_sha256'):raise ValueError('A2_SCORE_FROZEN_HASH_MISMATCH')
    if (not isinstance(penalty_codes,list) or len(penalty_codes)>3
            or any(not isinstance(c,str) or not c for c in penalty_codes) or not isinstance(penalty_rules,Mapping)):
        raise ValueError('A2_SCORE_PENALTY_CODES_INVALID')
    fixed=frozen['policy']['penalty_points']
    unknown=set(penalty_codes)-(set(penalty_rules)&set(fixed))
    if unknown:raise ValueError('A2_SCORE_PENALTY_CODE_UNKNOWN')
    theme=frozen['themes'][theme_id];points=Decimal(0);applied=[];unresolved=[]
    for code in sorted(set(penalty_codes)):
        points+=Decimal(str(fixed[code]));applied.append(code)
    raw_points=points
    points=max(Decimal(str(frozen['policy']['minimum_penalty_points'])),points)
    limited=bool(unresolved or theme['base_score'] is None)
    score=None if limited else max(Decimal(0),min(Decimal(100),Decimal(theme['base_score'])+points))
    return dict(theme_id=theme_id,theme_score=str(score) if score is not None else None,
        base_score=theme['base_score'],penalty_points=str(points),applied_penalty_codes=applied,
        raw_penalty_points=str(raw_points),penalty_mode=frozen['policy']['penalty_mode'],
        original_penalty_rules=deepcopy(dict(penalty_rules)),
        unresolved_penalty_codes=unresolved,penalty_rules_sha256=_hash(penalty_rules),
        score_snapshot_sha256=frozen['score_snapshot_sha256'],
        status='SCORE_DATA_LIMITED' if limited else theme['status'],
        focus_policy='WATCH_ONLY' if limited else theme['focus_policy'])


def canonicalize_team_scores(output,snapshot_data):
    """Server-only score projection; NULL must demote even the TOP5 exception."""
    frozen=snapshot_data['_TEAM_A2_SCORE_FREEZE'];rules=snapshot_data['PENALTY_RULES']
    result=deepcopy(dict(output));themes={}
    for theme in result.get('active_themes',[]):
        proof=score_with_penalties(frozen,theme['theme_id'],theme['team_penalty_codes'],rules)
        record=frozen['themes'][theme['theme_id']]
        theme.update(theme_score=proof['theme_score'],team_score_evidence=proof,
            score_breakdown={n:record['factors'][n]['mean_score'] for n in sorted(FACTORS)},
            penalties=[dict(points=proof['penalty_points'],reason_codes=proof['applied_penalty_codes'])],
            factor_coverage=dict(ratio=float(record['available_weight']),
                available_weight=float(record['available_weight']),total_weight=1,
                unavailable_factors=sorted(set(record['missing_factors']+record['globally_removed_factors'])),
                normalized_over_available_weight=True))
        themes[theme['theme_id']]=theme
    demoted=[];focus=[]
    for pool in ('focus_pool','watch_only_pool'):
        for row in result.get(pool,[]):
            theme=themes.get(row.get('theme_id'))
            if theme is not None:
                row['theme_score']=theme['theme_score'];row['team_score_evidence']=deepcopy(theme['team_score_evidence'])
            if pool=='focus_pool':
                if theme is None or theme['theme_score'] is None:
                    row['status']='WATCH_ONLY'
                    row['reason_codes']=list(dict.fromkeys([*row.get('reason_codes',[]),'SCORE_DATA_LIMITED']))
                    demoted.append(row)
                else:focus.append(row)
    result['focus_pool']=focus
    result['watch_only_pool']=[*result.get('watch_only_pool',[]),*demoted]
    return result,len(themes)+len(demoted)


def team_theme_score_reasons(theme,snapshot_data):
    """Validate the new score contract without inventing legacy zero dimensions."""
    try:
        frozen=snapshot_data['_TEAM_A2_SCORE_FREEZE']
        expected=score_with_penalties(frozen,theme['theme_id'],theme['team_penalty_codes'],snapshot_data['PENALTY_RULES'])
        values={n:frozen['themes'][theme['theme_id']]['factors'][n]['mean_score'] for n in sorted(FACTORS)}
        if (theme.get('team_score_evidence')!=expected or theme.get('theme_score')!=expected['theme_score']
                or theme.get('score_breakdown')!=values):return ['A2_TEAM_SCORE_PROOF_MISMATCH']
        return []
    except (ValueError,KeyError,TypeError):return ['A2_TEAM_SCORE_PROOF_INVALID']
