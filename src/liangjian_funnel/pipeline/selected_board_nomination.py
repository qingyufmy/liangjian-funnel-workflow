"""Dated early-setup nominations inside the existing 30-row research budget.

This reads frozen facts only. It neither changes A1/rotation membership nor
creates a call, A3 candidate, score, financial admission or execution right.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
from decimal import Decimal,InvalidOperation
from zoneinfo import ZoneInfo
import re

from .feature_store import content_hash

SCHEMA = 'a2-selected-board-startup/1'
ORIGIN = 'SELECTED_BOARD_EARLY_SETUP_RESEARCH_ONLY'
SIGNALS = {'REPAIR_WATCH', 'MA520_CROSS_PREPARATION', 'TREND_RESTART_WATCH'}


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def _time(value):
    stamp = datetime.fromisoformat(value)
    if stamp.utcoffset() is None:
        raise ValueError('STARTUP_AWARE_TIME_REQUIRED')
    return stamp.astimezone(ZoneInfo('Asia/Shanghai'))


def _market_day(data, capture):
    from ..runtime.calendar import ExchangeTradingCalendar
    calendar = ExchangeTradingCalendar()
    if 'CLOSED_SESSION_CAPTURE' in data:
        from ..runtime.closed_session_capture import validate_capture_proof
        proof = validate_capture_proof(data['CLOSED_SESSION_CAPTURE'], captured_at=capture, calendar=calendar)
        if data.get('MARKET_DATA_AS_OF') != proof['source_market_close_at']:
            raise ValueError('STARTUP_CLOSED_SOURCE_MISMATCH')
        return proof['source_trade_date']
    if calendar.is_trading_day(capture.date()) is not True or capture.hour < 15:
        raise ValueError('STARTUP_CLOSED_SOURCE_REQUIRED')
    market = _time(data.get('MARKET_DATA_AS_OF', capture.isoformat()))
    if market > capture or market.date() != capture.date() or market.hour < 15:
        raise ValueError('STARTUP_CLOSED_SOURCE_MISMATCH')
    return market.date().isoformat()


def _membership(data, capture):
    usable = dict(data); proofs = {}
    for kind in ('INDUSTRY', 'CONCEPT'):
        key = 'THS_' + kind + '_MEMBERSHIP'; source = data.get(key)
        valid = (isinstance(source, Mapping) and source.get('available') is True
                 and _sha(source.get('content_hash')) and isinstance(source.get('records'), (list, tuple)))
        clocks = {}
        if valid:
            try:
                clocks = {k: _time(source[k]).isoformat() for k in
                          ('as_of', 'event_time', 'fetch_time', 'ingest_time', 'publish_time') if source.get(k) is not None}
                valid = bool(clocks) and all(_time(v) <= capture for v in clocks.values())
            except (TypeError, ValueError):
                valid = False
        if valid:
            proofs[kind] = dict(declared_content_hash=source['content_hash'], clocks=clocks)
        else:
            usable[key] = {}
    return usable, proofs


def _event_proven(event, day, capture):
    try:
        e = event['evidence']; symbol = event['symbol']; signals = event['signals']
        if (re.fullmatch(r'\d{6}\.(SH|SZ|BJ)',symbol) is None
                or not isinstance(e, Mapping) or not isinstance(signals, list) or not signals
                or len(set(signals)) != len(signals) or not set(signals) <= SIGNALS
                or event.get('execution_permission') != 'BLOCKED'
                or not _sha(event.get('source_hash'))
                or type(e.get('bar_count')) is not int or e['bar_count'] < 61
                or not isinstance(e.get('adjust_mode'), str) or not e['adjust_mode']):
            return False
        bar, setup, observed = _time(e['bar_end']), _time(e['setup_bar_end']), _time(event['as_of'])
        if not (setup <= bar <= observed <= capture and bar.date().isoformat() == day and bar.hour >= 15):
            return False
        from .early_discovery import verify_discovery_event_identity
        return verify_discovery_event_identity(event)
    except (KeyError, TypeError, ValueError):
        return False


def _relative_strength(data,symbol,day,capture):
    source=data.get('A2_FACTOR_SNAPSHOT')
    if not isinstance(source,Mapping):return None,None
    try:
        reference=source.get('reference_relative_strength')
        if reference is not None:
            if (not isinstance(reference,Mapping) or reference.get('schema_version')!='a2-reference-relative-strength/1'
                    or reference.get('available') is not True or reference.get('denominator_scope')!='FULL_MARKET_REFERENCE'
                    or reference.get('evaluation_trade_date')!=day or _time(reference['as_of'])>capture
                    or _time(reference['as_of']).date().isoformat()!=day or _time(reference['as_of']).hour<15
                    or reference.get('return_window_sessions')!=20 or not _sha(reference.get('reference_symbols_hash'))):
                return None,None
            values=reference.get('values')
            if (not isinstance(values,Mapping) or reference.get('values_hash')!=content_hash(values)
                    or type(reference.get('observed_symbol_count')) is not int
                    or reference.get('observed_symbol_count')!=len(values)
                    or type(reference.get('reference_symbol_count')) is not int
                    or not len(values)<=reference['reference_symbol_count']):return None,None
            raw=values.get(symbol)
            if isinstance(raw,bool):return None,None
            value=Decimal(str(raw))
            if not value.is_finite() or not 0<=value<=100:return None,None
            from .deterministic import _relative_strength_score
            value=Decimal(str(round(_relative_strength_score({'relative_strength_percentile':float(value)},default=None),4)))
            return str(value),dict(source_field='A2_FACTOR_SNAPSHOT.reference_relative_strength.values.'+symbol,
                values_hash=reference['values_hash'],reference_symbols_hash=reference['reference_symbols_hash'],
                as_of=reference['as_of'],source_trade_date=day,metric='UNCHANGED_A2_20_SESSION_RETURN_PERCENTILE')
        if (source.get('available') is not True or not _sha(source.get('content_hash'))
                or _time(source['as_of'])>capture or _time(source['as_of']).date().isoformat()!=day
                or _time(source['as_of']).hour<15):return None,None
        row=source['by_symbol'][symbol];leader=row['leader_structure']
        if (row.get('stock_trend_structure',{}).get('trade_date')!=day
                or leader.get('available') is not True or leader.get('availability_state')!='OBSERVED_VALUE'
                or leader.get('source')!='LOCAL_THEME_CROSS_SECTION'):return None,None
        raw=leader['relative_strength_percentile']
        if isinstance(raw,bool):return None,None
        value=Decimal(str(raw))
        if not value.is_finite() or not 0<=value<=100:return None,None
        from .deterministic import _relative_strength_score
        # Use exactly the existing A2 scale and four-decimal ranking semantics.
        value=Decimal(str(round(_relative_strength_score({'leader_structure':leader},default=None),4)))
        return str(value),dict(source_field='A2_FACTOR_SNAPSHOT.by_symbol.'+symbol+'.leader_structure.relative_strength_percentile',
            source_content_hash=source['content_hash'],as_of=source['as_of'],source_trade_date=day,
            original_leader_sha256=content_hash(leader))
    except (KeyError,TypeError,ValueError,InvalidOperation):return None,None


def selected_board_startup_candidates(snapshot, upstream_output, formal_symbols):
    """Independent dated nomination source, not gated on Hot100 popularity.

    Missing evidence gives a visible disposition. It never triggers a fetch or
    substitutes a current membership/bar for missing historical evidence.
    """
    from ..data.hot100_observation import snapshot_decision_as_of
    from .a2_theme_research import _exact_theme_routes
    from .emotion_theme import bind_emotion_themes
    from .research.common import _daily_emotion_hard_risks
    data = snapshot.data
    discovery = data.get('EARLY_DISCOVERY_SNAPSHOT')
    empty = dict(schema=SCHEMA, candidates=[], dispositions=[], execution_authority=False,
                 new_model_calls_authorized=False, formal_a1_partitions_changed=False)
    if not isinstance(discovery, Mapping) or not discovery.get('records'):
        return {**empty, 'source_status': 'NO_FROZEN_EARLY_SETUP_RECORDS'}
    try:
        capture = _time(snapshot_decision_as_of(data)); day = _market_day(data, capture)
        discovery_valid = (discovery.get('version') == 'early-discovery/1'
                           and _time(discovery['as_of']) <= capture)
    except (TypeError, KeyError, ValueError):
        return {**empty, 'source_status': 'CLOSED_SOURCE_UNPROVEN'}
    board = data.get('SELECTED_BOARD_SNAPSHOT')
    board_valid = (isinstance(board, Mapping) and board.get('available') is True
                   and board.get('trade_date') == day and _sha(board.get('content_hash'))
                   and isinstance(board.get('by_symbol'), Mapping))
    if board_valid:
        try:
            board_valid = all(_time(board[k]) <= capture for k in
                ('as_of', 'event_time', 'fetch_time', 'ingest_time') if board.get(k) is not None)
        except (TypeError, ValueError):
            board_valid = False
    usable, proofs = _membership(data, capture)
    events = {}; conflicts = set()
    for event in discovery.get('records', []):
        if not isinstance(event, Mapping) or not isinstance(event.get('symbol'), str):
            continue
        symbol = event['symbol']
        if symbol in events and content_hash(events[symbol]) != content_hash(event):
            conflicts.add(symbol)
        events[symbol] = event
    bindings = bind_emotion_themes(upstream_output, usable, set(events))
    securities = {}; security_conflicts = set()
    for row in data.get('research_candidates', ()):
        if isinstance(row, Mapping) and isinstance(row.get('symbol'), str):
            symbol = row['symbol']
            if symbol in securities and content_hash(securities[symbol]) != content_hash(row):
                security_conflicts.add(symbol)
            securities[symbol] = row
    hard = _daily_emotion_hard_risks(data)
    partitions = {}
    for pool in ('active_research_pool', 'monitor_pool', 'rejected_candidates'):
        for row in upstream_output.get(pool, ()):
            if isinstance(row, Mapping):
                partitions[row.get('symbol')] = pool
                if row.get('hard_risk_events'):
                    hard[row.get('symbol')] = 'A1_EXPLICIT_HARD_RISK'
    candidates = []; dispositions = []
    for symbol in sorted(events):
        if symbol in formal_symbols:
            dispositions.append(dict(symbol=symbol, status='EXISTING_FORMAL_A2_DOMAIN', execution_authority=False)); continue
        if partitions.get(symbol) == 'active_research_pool':
            dispositions.append(dict(symbol=symbol, status='EXISTING_FORMAL_A1_CONTRACT', execution_authority=False)); continue
        event = events[symbol]; security = securities.get(symbol); binding = bindings[symbol]
        routes = _exact_theme_routes(symbol, binding); selected = []
        if board_valid:
            raw_boards = board['by_symbol'].get(symbol, [])
            for row in raw_boards if isinstance(raw_boards, (list, tuple)) else ():
                if not isinstance(row, Mapping): continue
                theme = row.get('strategy_theme_id') or row.get('theme_id')
                matching = [r for r in routes if r['theme_id'] == theme and any(
                    e.get('taxonomy_code') == row.get('membership_source_board_id') for e in r['evidence'])]
                if (row.get('selected_for_rotation') is True and type(row.get('primary_rank')) is int
                        and 1 <= row['primary_rank'] <= 8 and row.get('membership_basis') == 'VENDOR_NATIVE_BOARD'
                        and _sha(row.get('membership_reference_hash')) and row.get('membership_source_id') and matching):
                    selected.append(dict(theme_id=theme, primary_rank=row['primary_rank'],
                        source_board_id=row['membership_source_board_id'], membership_reference_hash=row['membership_reference_hash']))
        selected = sorted({content_hash(r):r for r in selected}.values(), key=lambda r:(r['primary_rank'],r['theme_id'],r['source_board_id']))
        reason = ('CONFLICTING_FROZEN_EVIDENCE' if symbol in conflicts | security_conflicts else
                  'BASE_RESEARCH_QUALIFICATION_UNPROVEN' if not security else
                  'BASE_RESEARCH_QUALITY_EXCLUSION' if security.get('research_eligible') is not True else
                  hard.get(symbol) or
                  ('EARLY_SETUP_EVIDENCE_UNPROVEN' if not discovery_valid or not _event_proven(event, day, capture) else
                   'DATED_SELECTED_NATIVE_BOARD_MEMBERSHIP_UNPROVEN' if not selected else None))
        item = dict(symbol=symbol, status='INELIGIBLE' if reason else 'DEFERRED_EXISTING_A2_RESEARCH_BUDGET',
                    reason_codes=[reason] if reason else [], execution_authority=False)
        if reason is None:
            matched_themes = {r['theme_id'] for r in selected}
            # Retain independent routes for every selected native board, not
            # the stock's possibly unrelated monthly primary identity.
            scoped = deepcopy(binding)
            scoped['research_candidate_routes'] = [r for r in routes if r['theme_id'] in matched_themes]
            relative,relative_proof=_relative_strength(data,symbol,day,capture)
            body = dict(schema=SCHEMA, symbol=symbol, capture_as_of=capture.isoformat(), source_trade_date=day,
                        source_snapshot_sha256=snapshot.snapshot_hash, setup=deepcopy(event), selected_boards=selected,
                        base_security_sha256=content_hash(security), membership_sources=proofs,
                        base_research_eligible=True,original_security_reasons=list(security.get('exclusion_reasons') or []),
                        original_a1_partition=partitions.get(symbol,'NOT_IN_MONTHLY_OUTPUT'),
                        relative_strength=relative,relative_strength_proof=relative_proof,
                        theme_research_binding=scoped, formal_a1_member=False, execution_authority=False,
                        execution_permission='BLOCKED', confirms_catalyst=False, confirms_theme_stage=False)
            body['candidate_id'] = content_hash(body)
            item['candidate_id'] = body['candidate_id']; candidates.append(body)
        dispositions.append(item)
    result = {**empty, 'candidates':candidates, 'dispositions':dispositions,
              'source_status':'FROZEN_SOURCE_EVALUATED', 'source_trade_date':day}
    result['source_sha256'] = content_hash(result)
    return result


def merge_startup_nominations(nomination, startup):
    """Keep the original evidence ordering and Hot100 lane share unchanged."""
    from .team_v2_nomination_source import nomination_order
    rows = {r['symbol']:deepcopy(r) for r in nomination['candidates']}
    for candidate in startup['candidates']:
        symbol = candidate['symbol']
        if symbol in rows:
            rows[symbol]['selected_board_startup_research'] = deepcopy(candidate)
            continue
        reason = 'SELECTED_BOARD_DATED_EARLY_SETUP_RESEARCH_ONLY'
        rows[symbol] = dict(symbol=symbol, theme_id=candidate['selected_boards'][0]['theme_id'],
            quant_filter_reason=reason, filter_reason_codes=[reason], relative_strength=candidate['relative_strength'],
            selected_board_theme_match=True, original_context_sha256=content_hash(candidate),
            original_decision_id=candidate['setup']['decision_id'], source_snapshot_sha256=candidate['source_snapshot_sha256'],
            source_refs=['A2:STARTUP:'+symbol], selected_board_startup_research=deepcopy(candidate),
            scope='RESEARCH_ONLY_NOT_A3_OR_EXECUTION', source_kind=ORIGIN)
    return {**nomination, 'candidates':sorted(rows.values(),key=nomination_order)}
