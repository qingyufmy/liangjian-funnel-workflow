"""0384 explanatory coverage projection; never a model or focus decision."""
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal,InvalidOperation
import hashlib,json

MISSING_PREFIX='A2_ROTATION_FOCUS_COVERAGE_MISSING:'


def complete_server_policy_demotions(output, *, provider_raw, direction_members):
    """0488: own the server's demotion consequence, never a provider omission.

    A direction qualifies only if a provider-selected *member* covered it and
    no member survives server policy. Same-theme non-members never qualify.
    Both the provider answer and its focus choices remain immutable.
    """
    if not isinstance(output,Mapping) or not isinstance(provider_raw,Mapping):return None
    raw_focus={r.get('symbol') for r in provider_raw.get('focus_decisions',[]) if isinstance(r,Mapping) and isinstance(r.get('symbol'),str)}
    final_focus={r.get('symbol') for r in output.get('focus_pool',[]) if isinstance(r,Mapping) and isinstance(r.get('symbol'),str)}
    from .team_v2_rotation_scope import rotation_coverage
    before=rotation_coverage(raw_focus,direction_members);after=rotation_coverage(final_focus,direction_members)
    rows=output.get('rotation_reviews')
    if not isinstance(rows,list) or any(not isinstance(r,Mapping) for r in rows):return None
    existing={r.get('rotation_direction_id') for r in rows};actions=[];additions=[]
    for direction in sorted((set(before['covered_by_final_focus']) & set(after['required_no_focus'])) - existing):
        demoted=sorted(raw_focus.intersection(direction_members[direction])-final_focus)
        if not demoted:continue
        additions.append(dict(rotation_direction_id=direction,decision='NO_FOCUS',reviewed_symbols=demoted[:3],
            reason_codes=['SERVER_POLICY_DEMOTED'],explanation='服务端策略使该方向原重点成员全部降级；仅说明服务端结果，不代表模型改判，不生成计划或交易权限。'))
        actions.append(dict(rule_id='A2_SERVER_POLICY_DEMOTED_NO_FOCUS_V1',rotation_direction_id=direction,
            demoted_symbols=demoted,reason='SERVER_POLICY_DEMOTED',model_selection_unchanged=True))
    if not actions:return None
    projected=deepcopy(dict(output));projected['rotation_reviews'].extend(additions)
    return projected,dict(rule_id='A2_SERVER_POLICY_DEMOTED_NO_FOCUS_V1',authority='0488_SERVER_OWNS_POST_POLICY_CONSEQUENCE',
        actions=actions,provider_raw_sha256=_sha(provider_raw),before_sha256=_sha(output),after_sha256=_sha(projected),
        model_focus_unchanged=True,final_focus_unchanged=projected.get('focus_pool')==output.get('focus_pool'),
        purpose='SERVER_POLICY_EXPLANATION_NOT_MODEL_JUDGMENT')


def _sha(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,
        separators=(',',':'),allow_nan=False).encode()).hexdigest()


def synthesize_final_no_focus(output, *, issues, direction_members, engine_scores,
                              final_attempt, validator, provider_raw):
    """Fill only final-round missing explanations, reusing the caller validator.

    The scope/ranking are frozen engine facts. No score is defaulted, model
    judgment reclassified, focus row promoted/demoted, or trading gate changed.
    The provider raw and the post-policy source remain untouched.
    """
    if (type(final_attempt) is not int or final_attempt!=3 or not isinstance(output,Mapping)
            or not isinstance(direction_members,Mapping) or not isinstance(engine_scores,Mapping)
            or not issues or any(not isinstance(code,str) or not code.startswith(MISSING_PREFIX)
                for code in issues)):
        return None
    original_issues=tuple(validator(output))
    if set(original_issues)!=set(issues):return None
    missing=sorted({code[len(MISSING_PREFIX):] for code in issues})
    from .team_v2_rotation_scope import rotation_coverage
    coverage=rotation_coverage({row['symbol'] for row in output.get('focus_pool',[])
        if isinstance(row,Mapping) and isinstance(row.get('symbol'),str)},direction_members)
    if not set(missing)<=set(coverage['required_no_focus']):return None
    if (any(not direction or direction not in direction_members for direction in missing)
            or not isinstance(output.get('rotation_reviews'),list)):
        return None
    existing=output['rotation_reviews']
    if any(not isinstance(row,Mapping) for row in existing):return None
    if any(row.get('rotation_direction_id') in missing for row in existing):return None
    actions=[];rows=[]
    for direction in missing:
        members=direction_members[direction]
        if (not isinstance(members,(list,tuple,set,frozenset)) or not members
                or any(not isinstance(symbol,str) or not symbol for symbol in members)
                or len(members)!=len(set(members))):return None
        ranked=[]
        for symbol in members:
            value=engine_scores.get(symbol)
            if value is None or isinstance(value,bool):return None
            try:score=Decimal(str(value))
            except (ValueError,InvalidOperation):return None
            if not score.is_finite():return None
            ranked.append((score,symbol))
        ranked.sort(key=lambda pair:(-pair[0],pair[1]))
        selected=[symbol for _,symbol in ranked[:3]]
        row=dict(rotation_direction_id=direction,decision='NO_FOCUS',reviewed_symbols=selected,
            reason_codes=['SERVER_SYNTHESIZED_NO_FOCUS'],
            explanation='模型未覆盖，服务端合成，不代表模型判断；仅补齐最终轮的方向说明，不生成计划或交易权限。')
        rows.append(row)
        actions.append(dict(action='SERVER_SYNTHESIZED_NO_FOCUS',rotation_direction_id=direction,
            reviewed_symbols=selected,engine_ranked_members=[dict(symbol=symbol,score=str(score)) for score,symbol in ranked],
            ranking='FROZEN_ENGINE_SCORE_DESC_SYMBOL_ASC',purpose='EXPLANATORY_COVERAGE_NOT_MODEL_JUDGMENT'))
    projected=deepcopy(dict(output));projected['rotation_reviews'].extend(rows)
    final_issues=tuple(validator(projected))
    if final_issues:return None
    return projected,dict(authority='0384_REPLY_P0_8_FINAL_COVERAGE_ONLY',semantic_attempt=3,
        provider_raw_sha256=_sha(provider_raw),original_postpolicy_sha256=_sha(output),
        projected_postpolicy_sha256=_sha(projected),original_issues=list(original_issues),
        projected_validation_issues=[],actions=actions,coverage_domain=coverage,provider_capture_unchanged=True,
        focus_unchanged=projected.get('focus_pool')==output.get('focus_pool'),
        status='VALIDATED',purpose='EXPLANATORY_COVERAGE_NOT_MODEL_JUDGMENT')
