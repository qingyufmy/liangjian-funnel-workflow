"""Single explanatory rotation domain: final policy FOCUS intersect members."""
from collections.abc import Mapping


# Existing legacy posterior exclusions, not a new reason-code taxonomy.
ENTRY_GATE_ONLY_REASON_CODES = frozenset({
    'MARKET_RISK_OFF_RETREAT', 'MARKET_RISK_OFF', 'RISK_OFF_RETREAT',
    'EMOTION_DIVERGENCE_NO_NEW_ENTRY', 'EMOTION_DIVERGENCE',
    'NO_NEW_ENTRY', 'NO_NEW_ENTRY_REGIME', 'TIER_STRUCTURE_ZERO',
    'TIER_STRUCTURE_ABSENT', 'ROTATION_RANK_4', 'ROTATION_RANK_5',
})


def rotation_reason_contract(direction_members):
    """No positive enum exists in the posterior; expose its actual constraints.

    Codes are model judgments, never synthesized by the service. 0515 aligns
    every frozen direction with the same existing posterior minItems=1 rule.
    """
    rotation_coverage(set(),direction_members)
    return {direction:dict(minItems=1,maxItems=4,uniqueItems=True,
        item_type='nonempty_string',positive_enum=None,
        forbidden_as_only_reasons=sorted(ENTRY_GATE_ONLY_REASON_CODES),
        rule='At least one concrete evidence-based non-selection reason; '
            'a list consisting ONLY of the excluded entry-gate codes is invalid. '
            'The existing posterior has no positive reason-code enum. '
            'The service never invents a model reason.')
        for direction in sorted(direction_members)}


def rotation_reason_issues(direction,reason_codes,direction_members):
    contract=rotation_reason_contract(direction_members).get(direction)
    if contract is None:return ()
    if (not isinstance(reason_codes,list) or not 1<=len(reason_codes)<=4
            or any(not isinstance(code,str) or not code.strip() for code in reason_codes)
            or len(set(reason_codes))!=len(reason_codes)):
        return ('A2_ROTATION_REASON_CODES_INVALID:'+direction,)
    if all(code.strip().upper() in ENTRY_GATE_ONLY_REASON_CODES for code in reason_codes):
        return ('A2_ROTATION_REASON_CODES_ENTRY_GATE_ONLY:'+direction,)
    return ()


def rotation_output_contract(direction_members):
    """Generate the exact catalog from the same projection the validator uses.

    This is a conditional coverage rule, not a list of mandatory NO_FOCUS rows.
    No outside identifier can become legal through normalization.
    """
    coverage=rotation_coverage(set(),direction_members)
    return dict(rule_id='A2_ROTATION_COVERAGE_V1',
        allowed_direction_members=coverage['required_no_focus'],
        rotation_reason_codes_by_direction=rotation_reason_contract(direction_members),
        identifier_policy='EXACT_CASE_SENSITIVE_NO_ALIAS_NO_FOREIGN_IDS',
        algorithm=[
            '先完成 focus_decisions；allowed_direction_members 是全部可用方向，不是必须逐行填写的 NO_FOCUS 表。',
            '逐方向计算成员与模型原始 focus_decisions 股票的交集；只按明确成员，不能以同题材名称代替成员；交集非空则该方向完全不得出现在 rotation_reviews。',
            '仅对交集为空的方向各写恰好一行 NO_FOCUS，reviewed_symbols 从该方向成员取1至3只，不重复、不越界。',
            '若全部方向已有有效 focus，rotation_reviews 必须为 []；服务端不会把非法方向 ID 改成合法方向。',
            '服务端之后若自行降级导致一个方向失去全部重点成员，由服务端补 SERVER_POLICY_DEMOTED 说明；模型不猜测服务端降级结果。',
        ],coverage_authority='0488_PROVIDER_RAW_MEMBERSHIP_THEN_SERVER_POLICY_COMPLETION')


def rotation_coverage(final_focus_symbols,direction_members):
    """No labels, raw-provider priorities, or new trade authority are inferred."""
    if (not isinstance(direction_members,Mapping)
            or not isinstance(final_focus_symbols,(set,frozenset,list,tuple))
            or any(not isinstance(s,str) for s in final_focus_symbols)):
        raise ValueError('TEAM_A2_ROTATION_SCOPE_INVALID')
    focused=set(final_focus_symbols);covered={};required={}
    for direction,members in sorted(direction_members.items()):
        if (not isinstance(direction,str) or not direction
                or not isinstance(members,(set,frozenset,list,tuple))
                or any(not isinstance(s,str) for s in members)):
            raise ValueError('TEAM_A2_ROTATION_SCOPE_INVALID')
        common=sorted(focused.intersection(members))
        if common:covered[direction]=common
        else:required[direction]=sorted(set(members))
    return dict(covered_by_final_focus=covered,required_no_focus=required,
        final_focus_symbols=sorted(focused),authority='0387_POST_POLICY_FOCUS_INTERSECT_FROZEN_MEMBERS')
