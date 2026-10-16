"""Claude A3 4.0.1 PLAN_REVIEW contract, not an eligibility engine.

The model may disagree with a deterministic verdict. That disagreement is
recorded after blind review; it never creates a strategy or price contract.
"""
from collections.abc import Mapping
import json

from .team_v2_contract import TeamValidation, validate_team_output

ROOT = set('envelope session core_watch_pool secondary_watch_pool rejected_candidates '
    'handoff_notes what_would_change_my_mind unresolved_questions'.split())
BASE = set('symbol strategy_profile review_status risk_unit cluster reason_codes supporting_refs opposing_refs'.split())
PASS = BASE | {'plan_priority_rank', 'trade_brief', 'what_would_change_my_mind'}
OPTIONAL = {'opposing_search_scope'}
PROFILES = {'TREND_MA5', 'MA520_SWING', 'LEADER_INTRADAY'}


def _text(value):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= 2000


def _texts(value):
    return isinstance(value, list) and all(_text(v) for v in value) and len(set(value)) == len(value)


def _enum(value, choices):
    return isinstance(value, str) and value in choices


def validate_plan_review(output, catalog, *, required_envelope, candidate_domain):
    if (not isinstance(candidate_domain, Mapping) or any(not _text(symbol)
            or not isinstance(row, Mapping) or not _enum(row.get('strategy_profile'), PROFILES)
            or not _enum(row.get('risk_unit'), {'STANDARD', 'PROBE'}) or not _text(row.get('cluster'))
            or not _enum(row.get('candidate_origin'), {'FOCUS', 'WATCH_ONLY'})
            for symbol, row in candidate_domain.items())):
        raise ValueError('A3_PLAN_SERVER_DOMAIN_INVALID')
    common = validate_team_output('A3', output, catalog, session='PLAN_REVIEW',
        expected_ids=set(candidate_domain))
    issues = list(common.issues)
    if not isinstance(output, Mapping):
        return common
    def issue(code):
        issues.append('A3_PLAN_' + code)
    if set(output) != ROOT:
        issue('ROOT_FIELDS_INVALID')
    try:
        bound = json.dumps(output.get('envelope'), sort_keys=True, allow_nan=False) == json.dumps(
            required_envelope, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError):
        bound = False
    if not bound:
        issue('ENVELOPE_BINDING_MISMATCH')
    notes = output.get('handoff_notes')
    if not isinstance(notes, Mapping) or set(notes) != {'to_a4', 'to_a6'}:
        issue('HANDOFF_RECIPIENT_INVALID')
    if not _texts(output.get('unresolved_questions')):
        issue('UNRESOLVED_QUESTIONS_INVALID')
    ranks = []
    for pool in ('core_watch_pool', 'secondary_watch_pool', 'rejected_candidates'):
        rows = output.get(pool)
        if not isinstance(rows, list):
            issue('POOL_REQUIRED'); continue
        fields = PASS if pool == 'core_watch_pool' else BASE
        for row in rows:
            if (not isinstance(row, Mapping) or not fields <= set(row)
                    or not set(row) <= fields | OPTIONAL):
                issue('ITEM_FIELDS_INVALID'); continue
            symbol = row['symbol']
            if not isinstance(symbol, str) or symbol not in candidate_domain:
                issue('SYMBOL_OUTSIDE_DOMAIN'); continue
            source = candidate_domain[symbol]
            profile = row['strategy_profile']
            if profile != source['strategy_profile'] and not (
                    pool != 'core_watch_pool' and profile == 'NO_NEXT_DAY_PLAN'):
                issue('STRATEGY_PROFILE_CHANGED')
            if row['cluster'] != source['cluster']:
                issue('CLUSTER_CHANGED')
            if not _texts(row['reason_codes']):
                issue('REASON_CODES_INVALID')
            if pool == 'core_watch_pool':
                if row['review_status'] != 'PASS' or not _enum(row['risk_unit'],
                        {'PROBE'} if source['risk_unit'] == 'PROBE' else {'STANDARD', 'PROBE'}):
                    issue('PASS_RISK_INVALID')
                rank = row['plan_priority_rank']
                if type(rank) is not int or rank < 1:
                    issue('PRIORITY_INVALID')
                else:
                    ranks.append(rank)
                brief = row['trade_brief']
                if (not isinstance(brief, Mapping) or set(brief) != {
                        'likely_failure_path', 'key_levels_note', 'confirm_cadence', 'note_to_trader'}
                        or not all(_text(v) for v in brief.values())
                        or not _enum(brief['confirm_cadence'], {'5m', '15m'})):
                    issue('TRADE_BRIEF_INVALID')
                falsifiers = row['what_would_change_my_mind']
                if not _texts(falsifiers) or not 1 <= len(falsifiers) <= 2:
                    issue('ITEM_FALSIFIERS_INVALID')
            elif not _enum(row['review_status'], {'VETO', 'DATA_GAP'}) or row['risk_unit'] != 'NO_ENTRY':
                issue('NON_PASS_RISK_INVALID')
    # Rank is local to the frozen batch; global display merging is a later step.
    if sorted(ranks) != list(range(1, len(ranks) + 1)):
        issue('PRIORITY_PARTITION_INVALID')
    return TeamValidation(not issues, tuple(dict.fromkeys(issues)), common.judgments, common.sentinel_count)
