"""Audited, conservative A3 close-review projections (0368-reply-c only).

Provider captures and validators remain immutable. This never grants engine
eligibility: quarantined known candidates are explicit non-executable gaps.
"""
from collections.abc import Mapping
from copy import deepcopy

from ..pipeline.team_v2_plan_review_schema import PASS, OPTIONAL, validate_plan_review

ROW_ISSUES = frozenset('A3_PLAN_{}'.format(code) for code in (
    'STRATEGY_PROFILE_CHANGED', 'CLUSTER_CHANGED', 'TRADE_BRIEF_INVALID',
    'ITEM_FALSIFIERS_INVALID', 'REASON_CODES_INVALID', 'SYMBOL_OUTSIDE_DOMAIN'))
RISK_ISSUE = 'A3_PLAN_PASS_RISK_INVALID'
DOMAIN_ISSUE = 'TEAM_CANDIDATE_DOMAIN_MISMATCH'
POOLS = ('core_watch_pool', 'secondary_watch_pool', 'rejected_candidates')


def recover_plan_review(raw, catalog, *, required_envelope, candidate_domain):
    """Return a separately validated projection or None; never edit raw/domain.

    Global failures, evidence defects and incomplete/duplicate source coverage
    cannot disappear because a row was removed. Rank compaction only follows
    an already-valid original global rank partition.
    """
    original = validate_plan_review(raw, catalog, required_envelope=required_envelope,
        candidate_domain=candidate_domain)
    allowed = ROW_ISSUES | {RISK_ISSUE, DOMAIN_ISSUE}
    if original.valid or not isinstance(raw, Mapping) or set(original.issues) - allowed:
        return None
    if any(not isinstance(raw.get(pool), list) for pool in POOLS):
        return None
    all_rows = [row for pool in POOLS for row in raw[pool]]
    if any(not isinstance(row, Mapping) or not isinstance(row.get('symbol'), str) for row in all_rows):
        return None
    symbols = [row['symbol'] for row in all_rows]
    if len(symbols) != len(set(symbols)) or set(symbols) & set(candidate_domain) != set(candidate_domain):
        return None
    ranks = [row.get('plan_priority_rank') for row in raw['core_watch_pool']]
    if (any(type(rank) is not int for rank in ranks)
            or sorted(ranks) != list(range(1, len(ranks) + 1))):
        return None
    effective = deepcopy(dict(raw)); actions = []; retained = []
    for index, original_row in enumerate(raw['core_watch_pool']):
        # A malformed row is not one of the six authorized local failures.
        if not PASS <= set(original_row) or not set(original_row) <= PASS | OPTIONAL:
            return None
        row = deepcopy(dict(original_row)); symbol = row['symbol']
        if symbol not in candidate_domain:
            if row['review_status'] != 'PASS' or row['risk_unit'] not in ('STANDARD', 'PROBE'):
                return None
            local_issues = ['A3_PLAN_SYMBOL_OUTSIDE_DOMAIN']
        else:
            isolated = deepcopy(dict(raw))
            isolated.update(core_watch_pool=[{**row, 'plan_priority_rank': 1}],
                secondary_watch_pool=[], rejected_candidates=[])
            local = validate_plan_review(isolated, catalog, required_envelope=required_envelope,
                candidate_domain={symbol: candidate_domain[symbol]})
            local_issues = list(local.issues)
            if set(local_issues) - (ROW_ISSUES | {RISK_ISSUE}):
                return None
            if RISK_ISSUE in local_issues and not (row['review_status'] == 'PASS'
                    and row['risk_unit'] == 'STANDARD' and candidate_domain[symbol]['risk_unit'] == 'PROBE'):
                return None
        quarantine = sorted(set(local_issues) & ROW_ISSUES)
        if quarantine:
            actions.append(dict(code='SERVER_QUARANTINED_ROW', pool='core_watch_pool',
                index=index, symbol=symbol, original_issues=quarantine, original_row=deepcopy(row)))
            if symbol in candidate_domain:
                source = candidate_domain[symbol]
                diagnostic = dict(symbol=symbol, strategy_profile=source['strategy_profile'],
                    cluster=source['cluster'], review_status='DATA_GAP', risk_unit='NO_ENTRY',
                    reason_codes=['SERVER_QUARANTINED_ROW', *quarantine],
                    supporting_refs=deepcopy(row['supporting_refs']), opposing_refs=deepcopy(row['opposing_refs']))
                if 'opposing_search_scope' in row:
                    diagnostic['opposing_search_scope'] = deepcopy(row['opposing_search_scope'])
                effective['secondary_watch_pool'].append(diagnostic)
            continue
        if RISK_ISSUE in local_issues:
            if (row['review_status'] != 'PASS' or row['risk_unit'] != 'STANDARD'
                    or candidate_domain[symbol]['risk_unit'] != 'PROBE'):
                return None
            row['risk_unit'] = 'PROBE'
            actions.append(dict(code='SERVER_CLAMPED_RISK_UNIT', symbol=symbol,
                original_value='STANDARD', effective_value='PROBE', input_cap='PROBE'))
        retained.append(row)
    if not actions:
        return None
    rank_map = {rank: index + 1 for index, rank in enumerate(sorted(
        row['plan_priority_rank'] for row in retained))}
    for row in retained:
        before = row['plan_priority_rank']; after = rank_map[before]
        if before != after:
            actions.append(dict(code='SERVER_COMPACTED_PRIORITY_AFTER_QUARANTINE',
                symbol=row['symbol'], original_value=before, effective_value=after))
            row['plan_priority_rank'] = after
    effective['core_watch_pool'] = retained
    checked = validate_plan_review(effective, catalog, required_envelope=required_envelope,
        candidate_domain=candidate_domain)
    return effective, dict(authority='0368_REPLY_C_R1_R2', original_issues=list(original.issues),
        actions=actions, effective_validation_issues=list(checked.issues),
        status='VALIDATED' if checked.valid else 'BLOCKED')
