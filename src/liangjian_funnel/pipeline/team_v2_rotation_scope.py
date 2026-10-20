"""Single explanatory rotation domain: final policy FOCUS intersect members."""
from collections.abc import Mapping


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
