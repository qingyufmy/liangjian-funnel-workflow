"""Announcement prefilter, independent of announcements and final A2 output.

Shadow first: a narrow query domain is not activated until frozen-input parity
has been demonstrated. Stock-local uncertainty retains available-channel work;
an explicitly unavailable channel does not retain the entire domain.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from typing import Any

from .a2_features import stock_trend_structure
from .feature_store import content_hash


def _symbols(values: Iterable[str]) -> list[str]:
    return sorted({str(v).strip().upper() for v in values if str(v).strip()})


def event_scope(results: Mapping[str, Any], trade_date: date) -> tuple[list[str], bool]:
    """Read validated raw event rows without claiming unknown absence.

    Do not guess an exchange for a bare ticker. A complete empty event source
    proves absence; a malformed nonempty source does not.
    """
    symbols: set[str] = set()
    complete = True
    for name in ('LIMIT_UP_POOL', 'LIMIT_UP_LADDER'):
        result = results.get(name)
        if result is None or not result.ok or not result.complete:
            complete = False
            continue
        declared_day = result.metadata.get('market_trade_date')
        if str(declared_day or '') != trade_date.isoformat():
            complete = False
        for raw in result.items:
            row = raw.model_dump(mode='json')
            symbol = str(row.get('thscode') or row.get('symbol') or '').strip().upper()
            if len(symbol) == 9 and symbol[:6].isdigit() and symbol[6:] in ('.SH', '.SZ', '.BJ'):
                symbols.add(symbol)
            else:
                complete = False
    return sorted(symbols), complete


def build_disclosure_prefilter(*, symbols: Iterable[str], trade_date: date,
                              daily: Mapping[str, Sequence[Mapping[str, Any]]],
                              selected_board: Mapping[str, Any],
                              event_symbols: Iterable[str], event_sources_complete: bool,
                              hot_symbols: Iterable[str] = (),
                              discovery_symbols: Iterable[str] = (),
                              selected_board_field_present: bool | None = None) -> dict[str, Any]:
    """Keep a conservative superset of the current A2 research channels.

    Reuse A2's stock-local structure predicate verbatim. Board membership,
    HOT100, discovery and ladder/limit events are independent retention paths.
    No rank/count cap, company announcement, model output or A2 final result
    is accepted as an input. Only an observed negative with no other channel
    can be deferred. The caller must verify source freshness/completeness.
    """
    scope = _symbols(symbols)
    hot, discovery, events = (_symbols(v) for v in
                              (hot_symbols, discovery_symbols, event_symbols))
    raw_board = selected_board
    selected_board = selected_board if isinstance(selected_board, Mapping) else {}
    field_present = bool(selected_board) if selected_board_field_present is None else selected_board_field_present
    by_symbol = selected_board.get('by_symbol')
    try:
        declared_board_date = date.fromisoformat(selected_board.get('trade_date'))
    except (TypeError, ValueError):
        declared_board_date = None
    board_known = (selected_board.get('available') is True
                   and isinstance(by_symbol, Mapping) and declared_board_date is not None)
    # screen_a2 currently trusts materialized availability, not trade_date.
    # A date mismatch is audit evidence, not a new blanket retention/block
    # policy. Freshness-based execution permissions belong to board contract.
    board_date_mismatch = (selected_board.get('available') is True
                           and declared_board_date is not None and declared_board_date != trade_date)
    # Match the explicit unavailable-field contract in screen_a2. A missing
    # field is different: historical FULL_MARKET/LEGACY fallbacks still exist.
    trend_channel_blocked = field_present and selected_board.get('available') is not True
    records = []
    for symbol in scope:
        bars = daily.get(symbol, ())
        structure = stock_trend_structure(bars, trade_date)
        reasons = []
        if symbol in hot:
            reasons.append('HOT100')
        if symbol in discovery:
            reasons.append('EARLY_DISCOVERY')
        if symbol in events:
            reasons.append('LADDER_OR_LIMIT_EVENT')
        board_rows = by_symbol.get(symbol, ()) if isinstance(by_symbol, Mapping) else ()
        board_rows_valid = isinstance(board_rows, Sequence) and not isinstance(board_rows, (str, bytes))
        board_rows_valid = board_rows_valid and all(
            isinstance(row, Mapping) and (
                isinstance(row.get('selected_for_rotation'), bool)
                or row.get('rotation_reserve_scope') == 'RESEARCH_ONLY_NO_AUTOMATIC_ENTRY'
            ) for row in board_rows)
        if not trend_channel_blocked and board_rows_valid and any(isinstance(row, Mapping) and (
            row.get('selected_for_rotation') is True
            or row.get('rotation_reserve_scope') == 'RESEARCH_ONLY_NO_AUTOMATIC_ENTRY'
        ) for row in board_rows):
            reasons.append('PRIMARY_OR_RESERVE_BOARD')
        if not trend_channel_blocked and structure.get('structure_confirmed') is True:
            reasons.append('STOCK_TREND_STRUCTURE')
        # Unknown event membership is not an admission channel. Formal close
        # rejects incomplete required market facts before building this scope.
        # Keep stock-local uncertainty only for a still-available trend route.
        uncertain = not trend_channel_blocked and (
            not board_known or not board_rows_valid or structure.get('available') is not True)
        if uncertain:
            reasons.append('UNCERTAIN_FACTS_RETAINED')
        diagnostics = []
        if board_date_mismatch:
            diagnostics.append('BOARD_DATE_MISMATCH')
        if trend_channel_blocked:
            diagnostics.append('BOARD_CHANNEL_UNAVAILABLE')
        elif not board_known or not board_rows_valid:
            diagnostics.append('BOARD_MEMBERSHIP_UNPROVEN')
        if not event_sources_complete:
            diagnostics.append('EVENT_CHANNEL_UNAVAILABLE')
        if structure.get('available') is not True:
            diagnostics.append('STOCK_STRUCTURE_UNAVAILABLE')
        records.append({'symbol': symbol,
                        'status': 'COLLECT_DISCLOSURE' if reasons else 'DEFERRED_DISCLOSURE_NOT_COLLECTED',
                        'reason_codes': reasons or ['NO_OBSERVED_A2_CHANNEL'],
                        'uncertainty_retained': uncertain, 'trend_channel_blocked': trend_channel_blocked,
                        'channel_diagnostics': diagnostics,
                        'trend_structure': structure,
                        'daily_input_hash': content_hash(bars)})
    payload = {'schema_version': 'disclosure-prefilter/1', 'mode': 'SHADOW',
               'execution_authority': False, 'changes_query_scope': False,
               'trade_date': trade_date.isoformat(), 'symbols': scope,
               'source_sets': {'hot100': hot, 'discovery': discovery, 'events': events},
               'event_channel_definition': ['LIMIT_UP_POOL', 'LIMIT_UP_LADDER'],
               'board_input_hash': content_hash(raw_board), 'selected_board_field_present': field_present,
               'event_sources_complete': event_sources_complete,
               'records': records,
               'candidate_symbols': [r['symbol'] for r in records if r['status'] == 'COLLECT_DISCLOSURE'],
               'deferred_symbols': [r['symbol'] for r in records if r['status'] != 'COLLECT_DISCLOSURE']}
    payload['scope_hash'] = content_hash(payload)
    return payload


def audit_disclosure_scope(prefilter: Mapping[str, Any], review_symbols: Iterable[str], *,
                           decisions: Sequence[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """Compare the earlier work-domain with actual quantitative A2 output.

    A miss is evidence of a bad prefilter, not permission to drop an A2 row.
    A2 output is used only for this audit, never to construct the work-domain.
    """
    unhashed = {key: value for key, value in prefilter.items() if key != 'scope_hash'}
    if content_hash(unhashed) != prefilter.get('scope_hash'):
        raise ValueError('DISCLOSURE_PREFILTER_HASH_MISMATCH')
    scope = set(prefilter['symbols'])
    candidate = set(prefilter['candidate_symbols'])
    deferred = set(prefilter['deferred_symbols'])
    records = prefilter['records']
    if (candidate & deferred or candidate | deferred != scope
        or len(records) != len(scope) or {r['symbol'] for r in records} != scope
        or {r['symbol'] for r in records if r['status'] == 'COLLECT_DISCLOSURE'} != candidate
        or {r['symbol'] for r in records if r['status'] == 'DEFERRED_DISCLOSURE_NOT_COLLECTED'} != deferred):
        raise ValueError('DISCLOSURE_PREFILTER_PARTITION_INVALID')
    actual = _symbols(review_symbols)
    rows = list(decisions or ())
    # Pre-ranking eligibility is a stricter coverage domain than the final
    # transport list. Neither this union nor model output builds the prefilter.
    local = _symbols(r['symbol'] for r in rows if r.get('local_eligible_for_review') is True)
    required = set(actual) | set(local)
    missing = sorted(required - candidate)
    route_coverage = {}
    selectors = {
        'EMOTION': lambda r: r.get('a2_pool_channel') == 'EMOTION',
        'PRIMARY_TREND': lambda r: r.get('trend_core_eligible') is True,
        'RESERVE': lambda r: r.get('rotation_reserve_eligible') is True,
        'STRONG_TREND_OBSERVATION': lambda r: (r.get('strong_trend_observation') is True
                                              or r.get('strong_trend_observation_rank') is not None),
        'LEGACY': lambda r: r.get('a2_pool_channel') == 'LEGACY',
        'FULL_MARKET_FALLBACK': lambda r: r.get('rotation_input_source') == 'FULL_MARKET_ROTATION_FALLBACK',
    }
    for name, predicate in selectors.items():
        symbols = _symbols(r['symbol'] for r in rows
                           if r['symbol'] in required and predicate(r))
        route_coverage[name] = {'symbols': symbols, 'count': len(symbols),
                                'missing_symbols': sorted(set(symbols) - candidate)}
    for route in sorted({str(route) for r in rows for route in r.get('eligible_routes', ())}):
        symbols = _symbols(r['symbol'] for r in rows if r['symbol'] in required
                           and route in r.get('eligible_routes', ()))
        route_coverage[f'ROUTE:{route}'] = {'symbols': symbols, 'count': len(symbols),
                                          'missing_symbols': sorted(set(symbols) - candidate)}
    unrepresented = sorted(required - {r['symbol'] for r in rows}) if decisions is not None else []
    blocked = {r['symbol'] for r in records if r.get('trend_channel_blocked') is True}
    conflicts = _symbols(r['symbol'] for r in rows if r['symbol'] in blocked
        and (r.get('trend_core_eligible') is True or r.get('rotation_reserve_eligible') is True
             or r.get('strong_trend_observation') is True))
    return {'schema_version': 'disclosure-prefilter-coverage/2',
            'scope_hash': prefilter['scope_hash'], 'mode': prefilter.get('mode', 'SHADOW'),
            'status': 'SCOPE_MISS' if missing or conflicts else 'DATA_LIMITED' if unrepresented else 'COVERED',
            'coverage_level': 'QUANTITATIVE_PRE_RANK_AND_REVIEW' if decisions is not None else 'REVIEW_SYMBOLS_ONLY',
            'review_symbols': actual, 'missing_symbols': missing,
            'review_missing_symbols': sorted(set(actual) - candidate),
            'local_eligible_symbols': local, 'local_eligible_count': len(local),
            'required_symbols': sorted(required), 'required_count': len(required),
            'outside_scope_symbols': sorted(required - scope),
            'unrepresented_decision_symbols': unrepresented, 'route_coverage': route_coverage,
            'channel_contract_conflict_symbols': conflicts,
            'candidate_count': len(candidate), 'review_count': len(actual),
            'execution_authority': False, 'changes_query_scope': prefilter.get('changes_query_scope') is True}
