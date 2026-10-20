"""Pure, scope-aware health assessment of frozen Team minute evidence.

No fetch, model, broker, store or latch calls: suitable for original-packet
replay. The policy concerns source availability, never strategy permission.
"""
from decimal import Decimal
from pathlib import Path

import yaml

from ..data.mootdx import MinuteBar
from .team_v2_minute_projection import project_closed_minute
from .team_v2_sources import stamp

DEFAULT_INVALID_SYMBOL_RATIO = Decimal('0.50')


def load_monitor_health_policy(path):
    path = Path(path)
    # Existing installations without this new file retain the reviewed default;
    # malformed installed policy is a configuration failure, not a weaker gate.
    body = yaml.safe_load(path.read_text(encoding='utf-8')) if path.exists() else {
        'schema_version': 'team-monitor-health/1', 'max_invalid_symbol_ratio': '0.50'}
    if not isinstance(body, dict) or body.get('schema_version') != 'team-monitor-health/1':
        raise ValueError('CONTROL_MONITOR_HEALTH_POLICY_INVALID')
    ratio = Decimal(str(body['max_invalid_symbol_ratio']))
    if not ratio.is_finite() or not Decimal(0) < ratio < Decimal(1):
        raise ValueError('CONTROL_MONITOR_HEALTH_POLICY_INVALID')
    return dict(schema_version=body['schema_version'], max_invalid_symbol_ratio=str(ratio))


def project_packet_sources(packet):
    """Project only original bars; malformed one-stock input stays local."""
    projected = {}
    for symbol in packet['binding']['symbols']:
        packed = packet.get('sources', {}).get(symbol, {})
        try:
            bars = tuple(MinuteBar.model_validate(b) for b in packed.get('bars', ()))
            value = project_closed_minute(bars, (), observed_at=stamp(packet['captured_at']),
                execution_cutoff=stamp(packet['execution_cutoff']))
            if value.get('symbol', symbol) != symbol:
                raise ValueError('CONTROL_SOURCE_SYMBOL_MISMATCH')
            projected[symbol] = {**value, 'bars': bars}
        except (ValueError, TypeError, KeyError):
            projected[symbol] = dict(status='DATA_LIMITED', paper_bar=None, bars=(),
                reason_codes=['CONTROL_SOURCE_MINUTE_INVALID'])
    return projected


def assess_monitor_packet(packet, *, live, policy, projections=None, symbol_failures=None):
    """Return auditable source scope; never infer missing benchmark evidence.

    Live source status is the existing source contract. A benchmark-specific
    failure is used only when explicitly present; absent optional diagnostics
    are not relabelled as a verified benchmark check.
    """
    projections = project_packet_sources(packet) if projections is None else projections
    symbols = sorted(set(packet['binding']['symbols']))
    invalid = []; no_trades = []; ready = []; isolated = []
    for symbol in symbols:
        source = packet.get('sources', {}).get(symbol, {})
        projection = projections[symbol]
        reasons = list((symbol_failures or {}).get(symbol, ()))
        if not source.get('minute_complete'):
            reasons.append('CONTROL_CLOSED_MINUTE_UNAVAILABLE')
        if not source.get('quote', {}).get('complete'):
            reasons.append('CONTROL_CURRENT_QUOTE_UNAVAILABLE')
        if projection.get('status') == 'NO_TRADES':
            no_trades.append(symbol)
        elif projection.get('paper_bar') is None:
            reasons.extend(projection.get('reason_codes') or ['CONTROL_MINUTE_PROJECTION_UNPROVEN'])
        if reasons:
            invalid.append(symbol)
            isolated.append(dict(symbol=symbol, classification='DATA_MISSING_OR_INVALID', reason_codes=reasons))
        elif projection.get('status') == 'NO_TRADES':
            isolated.append(dict(symbol=symbol, classification='NO_TRADES',
                reason_codes=projection['reason_codes'], last_price=projection['last_price']))
        else:
            ready.append(symbol)
    ratio = Decimal(len(invalid)) / Decimal(len(symbols)) if symbols else Decimal(0)
    global_reasons = []
    if live.get('status') not in {'READY', 'READY_DEGRADED'}:
        global_reasons.append('CONTROL_MARKET_SOURCE_UNAVAILABLE')
    if live.get('benchmark_status') in {'MISSING', 'FAILED', 'UNAVAILABLE', 'DATA_BLOCKED'}:
        global_reasons.append('CONTROL_BENCHMARK_UNAVAILABLE')
    if any(row.get('reason_code') == 'CONTROL_CAPTURE_MINUTE_MOVED' for row in packet.get('reason_codes', ())):
        global_reasons.append('CONTROL_CAPTURE_MINUTE_MOVED')
    if symbols and len(invalid) == len(symbols):
        global_reasons.append('CONTROL_ALL_SYMBOL_SOURCES_UNAVAILABLE')
    if ratio > Decimal(policy['max_invalid_symbol_ratio']):
        global_reasons.append('CONTROL_INVALID_SYMBOL_RATIO_EXCEEDED')
    return dict(schema_version='team-monitor-health/1',
        status='GLOBAL_DEGRADED' if global_reasons else 'SYMBOL_ISOLATED' if isolated else 'READY',
        account_record_required=bool(global_reasons), global_reason_codes=global_reasons,
        symbol_count=len(symbols), ready_symbols=ready, invalid_symbols=invalid,
        no_trade_symbols=no_trades, invalid_symbol_ratio=str(ratio),
        max_invalid_symbol_ratio=policy['max_invalid_symbol_ratio'], isolated=isolated,
        benchmark_evidence='EXPLICIT_STATUS' if 'benchmark_status' in live else 'EXISTING_LIVE_SOURCE_CONTRACT_ONLY')
