"""Single explanatory rotation domain: final policy FOCUS intersect members."""
from collections.abc import Mapping


def rotation_output_contract(direction_members):
    """Generate the exact catalog from the same projection the validator uses.

    This is a conditional coverage rule, not a list of mandatory NO_FOCUS rows.
    No outside identifier can become legal through normalization.
    """
    coverage=rotation_coverage(set(),direction_members)
    return dict(rule_id='A2_ROTATION_COVERAGE_V1',
        allowed_direction_members=coverage['required_no_focus'],
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
