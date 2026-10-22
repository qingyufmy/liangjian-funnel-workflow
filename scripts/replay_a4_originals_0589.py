"""Bounded pure A4 comparison against hash-pinned original monitor packets.

No stores, model/provider calls or order paths. Recorded originals are the old
side; this does not pretend a current implementation is an old-code replay.

Packet: schema_version=a4-originals-packet/1, trade_date, capture_kind, records.
Each record has event/plan raw row dictionaries, event_sha256/plan_sha256, plus
optional frozen_inputs={source_ref,sha256,body}. body requires minute_snapshot
{snapshot_id,symbol,interval,decision_as_of,payload_json,payload_sha256}, config,
daily_facts and historical_five. Empty historical_five is a captured absence,
not permission to manufacture a seed. Without complete originals report MISSING.
"""
from argparse import ArgumentParser
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path, PurePosixPath
import socket
import sys
import base64
import zlib
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')
MAX_BYTES = 16 * 1024 * 1024


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _time(value):
    result = datetime.fromisoformat(str(value))
    if result.utcoffset() is None: raise ValueError('ORIGINAL_TIME_NAIVE')
    return result.astimezone(TZ)


def _verified_row(record, name):
    row = record.get(name)
    if not isinstance(row, Mapping) or digest(row) != record.get(name + '_sha256'):
        raise ValueError('ORIGINAL_' + name.upper() + '_ROW_HASH_MISMATCH')
    raw = row.get('payload_json')
    if not isinstance(raw, str) or len(raw.encode()) > 1024 * 1024:
        raise ValueError('ORIGINAL_' + name.upper() + '_PAYLOAD_MISSING_OR_OVERSIZE')
    body = json.loads(raw)
    if not isinstance(body, Mapping): raise ValueError('ORIGINAL_PAYLOAD_NOT_OBJECT')
    return dict(row), body, hashlib.sha256(raw.encode()).hexdigest()


def _condition_changes(old, new):
    before, after = old.get('conditions') or {}, new.get('conditions') or {}
    return [dict(name=name, old=before.get(name), new=after.get(name),
        changed=before.get(name) != after.get(name)) for name in sorted(set(before) | set(after))]


def normalize_capture(packet):
    """Adapt the actual 0589 RO packet without changing any original payload."""
    if packet.get('schema_version') != 'a4-original-packet/0589': return packet
    import yaml
    files = {}
    for row in packet.get('files', []):
        raw = row.get('original_utf8')
        if not isinstance(raw, str) or hashlib.sha256(raw.encode()).hexdigest() != row.get('sha256'):
            raise ValueError('ORIGINAL_CAPTURE_FILE_SHA_MISMATCH')
        files[row['path']] = row
    config_file = next((f for path, f in files.items() if path.endswith('/config/entry_confirmation.yaml')), None)
    manifests = [(path, json.loads(f['original_utf8'])) for path, f in files.items() if '/publication_proofs/' in path]
    plans = {row['plan_id']: row for row in packet.get('plans', [])}
    histories = {row['symbol']: row for row in packet.get('history', [])}
    snapshots = {(row['snapshot_id'], row['symbol'], row['interval']): row for row in packet.get('snapshots', [])}
    records = []
    for event in packet.get('events', []):
        payload = json.loads(event['payload_json'])
        if hashlib.sha256(event['payload_json'].encode()).hexdigest() != event.get('payload_sha256'):
            raise ValueError('ORIGINAL_CAPTURE_EVENT_SHA_MISMATCH')
        plan = plans.get(event['plan_id'])
        if plan is None: raise ValueError('ORIGINAL_CAPTURE_PLAN_MISSING')
        if hashlib.sha256(plan['payload_json'].encode()).hexdigest() != plan.get('payload_sha256'):
            raise ValueError('ORIGINAL_CAPTURE_PLAN_SHA_MISMATCH')
        record = dict(event=event, event_sha256=digest(event), plan=plan, plan_sha256=digest(plan))
        symbol = payload['symbol']
        snap = snapshots.get((payload['minute_snapshot_id'], symbol, '1m'))
        hist = histories.get(symbol)
        entries = [(path, entry) for path, manifest in manifests for entry in manifest.get('plans', [])
            if entry.get('plan_id') == event['plan_id']]
        if snap is not None and hist is not None and config_file is not None and len(entries) == 1:
            if digest(hist['rows']) != hist.get('rowset_sha256'):
                raise ValueError('ORIGINAL_CAPTURE_HISTORY_SHA_MISMATCH')
            path, entry = entries[0]
            facts = entry.get('entry_confirmation_daily_facts')
            if not isinstance(facts, Mapping) or digest(facts.get('value')) != facts.get('sha256'):
                raise ValueError('ORIGINAL_CAPTURE_DAILY_FACT_SHA_MISMATCH')
            from liangjian_funnel.runtime.entry_confirmation_sources import _validated_small_facts
            manifest = next(manifest for candidate_path, manifest in manifests if candidate_path == path)
            # Exact runtime pure projection adds its real manifest provenance;
            # it never opens the VM path/base snapshot from this offline helper.
            runtime_facts = _validated_small_facts(entry, symbol=symbol,
                target=_time(event['minute_end']).date(), manifest=manifest,
                manifest_path=PurePosixPath(path), manifest_file_sha=files[path]['sha256'])
            historical = [dict(symbol=row['symbol'], interval=row['interval'], bar_end=row['bar_end'],
                source_id=row['source_id'], adjust_mode=row['adjust_mode'],
                **{key: float(row[key+'_value']) for key in ('open','high','low','close','volume','amount')})
                for row in sorted(hist['rows'], key=lambda row: row['bar_end'])]
            body = dict(minute_snapshot={**snap, 'payload_json': snap['payload_original_utf8']},
                config=yaml.safe_load(config_file['original_utf8'])['entry_confirmation'],
                daily_facts=runtime_facts, historical_five=historical,
                execution_cutoff=payload['strategy']['execution_data']['market_cutoff'],
                expected_execution_input_sha256=payload['strategy']['execution_data']['input_sha256'],
                original_capture_pins=dict(history_rowset_sha256=hist['rowset_sha256'],
                    config_file_sha256=config_file['sha256'], manifest_file_sha256=files[path]['sha256'],
                    daily_fact_sha256=facts['sha256']))
            record['frozen_inputs'] = dict(source_ref='RO minute_decision_snapshots + immutable historical archive + pinned publication manifest',
                sha256=digest(body), body=body)
        records.append(record)
    return dict(schema_version='a4-originals-packet/1', capture_kind='PRODUCTION_RO_ORIGINALS_DECLARED',
        trade_date=packet['requested_event_ends'][0][:10], records=records)


def replay_record(record, *, trade_date):
    event, payload, event_payload_sha = _verified_row(record, 'event')
    plan_row, plan, plan_payload_sha = _verified_row(record, 'plan')
    at = _time(event['minute_end'])
    symbol = payload.get('symbol')
    if (at.date().isoformat() != trade_date or event.get('plan_id', payload.get('plan_id')) != plan_row.get('plan_id')
            or payload.get('plan_id') != plan_row.get('plan_id') or plan_row.get('symbol') != symbol
            or plan.get('symbol') != symbol): raise ValueError('ORIGINAL_PLAN_EVENT_IDENTITY_CONFLICT')
    strategy = payload.get('strategy') if isinstance(payload.get('strategy'), Mapping) else {}
    old = strategy.get('entry_confirmation') if isinstance(strategy.get('entry_confirmation'), Mapping) else {}
    original_buy = event.get('action') in {'BUY_SIGNAL', 'ADD_SIGNAL'} and bool(event.get('effective'))
    out = dict(event_id=event.get('event_id', event.get('id')), symbol=symbol, plan_id=plan_row['plan_id'],
        minute_end=event['minute_end'], original_action=event.get('action'), original_reason=event.get('reason_code'),
        original_effective=event.get('effective'), original_pattern=old,
        original_strategy_reason_codes=strategy.get('reason_codes'), original_buy=original_buy,
        event_row_sha256=record['event_sha256'], plan_row_sha256=record['plan_sha256'],
        event_payload_sha256=event_payload_sha, plan_payload_sha256=plan_payload_sha,
        status='MISSING_ORIGINAL_INPUTS', missing=[], conditions=[],
        actual_buy_impact='UNKNOWN_WITHOUT_COMPLETE_STRATEGY_INPUTS', model_http_calls=0)
    captured = record.get('frozen_inputs')
    if not isinstance(captured, Mapping):
        out['missing'] = ['FROZEN_MINUTE_SNAPSHOT', 'CONFIG', 'DAILY_FACTS', 'HISTORICAL_5M']
        return out
    inputs = captured.get('body')
    if (not isinstance(inputs, Mapping) or digest(inputs) != captured.get('sha256')
            or not isinstance(captured.get('source_ref'), str) or not captured['source_ref']):
        raise ValueError('ORIGINAL_FROZEN_INPUT_HASH_UNPROVEN')
    out.update(frozen_input_sha256=captured['sha256'], frozen_input_source_ref=captured['source_ref'])
    for key in ('minute_snapshot', 'config', 'daily_facts', 'historical_five'):
        if key not in inputs: out['missing'].append(key.upper())
    if out['missing']: return out
    snapshot = inputs['minute_snapshot']
    if not isinstance(snapshot, Mapping): raise ValueError('ORIGINAL_MINUTE_SNAPSHOT_INVALID')
    raw = snapshot.get('payload_json')
    if (not isinstance(raw, str) or len(raw.encode()) > 1024 * 1024
            or hashlib.sha256(raw.encode()).hexdigest() != snapshot.get('payload_sha256')):
        raise ValueError('ORIGINAL_MINUTE_SNAPSHOT_HASH_MISMATCH')
    observed = _time(snapshot['decision_as_of'])
    if (snapshot.get('snapshot_id') != payload.get('minute_snapshot_id') or snapshot.get('symbol') != symbol
            or snapshot.get('interval') != '1m' or observed.replace(second=0, microsecond=0) != at):
        raise ValueError('ORIGINAL_MINUTE_SNAPSHOT_SCOPE_CONFLICT')
    bars = json.loads(raw)
    history = inputs['historical_five']
    if not isinstance(bars, list) or not bars or len(bars) > 500 or not isinstance(history, list) or len(history) > 360:
        raise ValueError('ORIGINAL_BAR_DOMAIN_MISSING_OR_OVERSIZE')
    for group, interval in ((bars, '1m'), (history, '5m')):
        for bar in group:
            if (not isinstance(bar, Mapping) or bar.get('symbol') != symbol or bar.get('interval') != interval
                    or bar.get('evidence_kind') == 'SYNTHETIC_QUOTE' or ':RISK_ONLY' in str(bar.get('source_id')).upper()
                    or bar.get('closed') is False or _time(bar.get('bar_end', bar.get('end'))) > observed):
                raise ValueError('ORIGINAL_SYNTHETIC_FUTURE_OR_WRONG_BAR')
    cutoff = _time(inputs.get('execution_cutoff', observed.isoformat()))
    if cutoff > observed: raise ValueError('ORIGINAL_EXECUTION_CUTOFF_FUTURE')
    from liangjian_funnel.data.session_windows import closed_window_ends
    expected = closed_window_ends(cutoff, '1m')
    bars = [bar for bar in bars if _time(bar['bar_end']).date() == cutoff.date() and _time(bar['bar_end']) <= cutoff]
    if expected: bars = bars[-len(expected):]
    actual_execution_sha = hashlib.sha256(json.dumps(bars, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if inputs.get('expected_execution_input_sha256') not in (None, actual_execution_sha):
        raise ValueError('ORIGINAL_EXECUTION_INPUT_BINDING_MISMATCH')
    from liangjian_funnel.runtime.strategies import aggregate_closed_bars, _vwap, evaluate_strategy, _normalize_bars, _aggregate_sessions
    from liangjian_funnel.runtime.entry_confirmation import evaluate_pattern
    closed = aggregate_closed_bars(bars, as_of=cutoff)
    typed_five, _ = _aggregate_sessions(_normalize_bars(bars, plan), as_of=cutoff)
    new = evaluate_pattern(plan, closed['5m'], closed['15m'], as_of=cutoff,
        config=inputs['config'], daily_facts=inputs['daily_facts'],
        historical_five=history, vwap=_vwap(typed_five))
    out.update(status='PURE_PATTERN_REPLAY', candidate_pattern=new,
        candidate_reason_codes=new.get('reason_codes'), conditions=_condition_changes(old, new),
        minute_snapshot_payload_sha256=snapshot['payload_sha256'],
        execution_input_sha256=actual_execution_sha, execution_as_of=cutoff.isoformat(),
        original_capture_pins=inputs.get('original_capture_pins', {}),
        bar_basis='DERIVED_5M_15M_FROM_EXACT_FROZEN_1M;NO_NEW_MARKET_DATA',
        complete_strategy_replayed=False, missing=['EXACT_MARKET_CONTEXT', 'EXACT_POSITION_OVERLAY'])
    out['numeric_or_provenance_changes'] = [item['name'] for item in out['conditions']
        if any((item['old'] or {}).get(key) != (item['new'] or {}).get(key)
            for key in ('value', 'threshold', 'source_as_of'))]
    if not original_buy and new.get('confirmed') is False:
        out['actual_buy_impact'] = 'NO_RECORDED_BUY_IN_SAMPLE;PATTERN_STILL_UNCONFIRMED;OTHER_GATES_NOT_REPLAYED'
    if 'market_context' in inputs and 'position' in inputs:
        context = inputs['market_context']
        if not isinstance(context, Mapping) or (inputs['position'] is not None and not isinstance(inputs['position'], Mapping)):
            raise ValueError('ORIGINAL_STRATEGY_CONTEXT_INVALID')
        if (context.get('entry_confirmation_config') != inputs['config']
                or context.get('entry_confirmation_daily_facts') != inputs['daily_facts']
                or context.get('historical_5m') != history):
            raise ValueError('ORIGINAL_STRATEGY_CONTEXT_PATTERN_BINDING_CONFLICT')
        evaluated = evaluate_strategy(plan, bars, now=observed,
            position=inputs['position'], market_context=context).model_dump(mode='json')
        out.update(status='PURE_STRATEGY_REPLAY', candidate_strategy=evaluated,
            candidate_reason_codes=evaluated.get('reason_codes'), complete_strategy_replayed=True, missing=[],
            actual_buy_impact=('RECORDED_BUY_QUANT_ACTION_CHANGED' if original_buy
                and evaluated['action'] not in {'BUY_SIGNAL', 'ADD_SIGNAL'} else
                'RECORDED_BUY_QUANT_ACTION_PRESERVED' if original_buy else
                'NO_RECORDED_BUY;NEW_QUANT_BUY_NOT_ACTUAL_ORDER' if evaluated['action'] in {'BUY_SIGNAL', 'ADD_SIGNAL'} else
                'NO_RECORDED_BUY;CANDIDATE_QUANT_NOT_BUY'))
    return out


def replay_packet(packet):
    packet = normalize_capture(packet)
    if packet.get('schema_version') != 'a4-originals-packet/1': raise ValueError('ORIGINAL_PACKET_SCHEMA_INVALID')
    records = packet.get('records')
    if not isinstance(records, list) or not records or len(records) > 48: raise ValueError('ORIGINAL_PACKET_DOMAIN_MISSING_OR_OVERSIZE')
    result = [replay_record(record, trade_date=packet['trade_date']) for record in records]
    return dict(schema_version='a4-originals-replay/1', capture_kind_declared=packet.get('capture_kind', 'UNKNOWN'),
        trade_date=packet['trade_date'], records=result, model_http_calls=0,
        status='REPLAYED_AVAILABLE_ORIGINALS' if all(r['status'] != 'MISSING_ORIGINAL_INPUTS' for r in result) else 'INCOMPLETE_ORIGINALS',
        complete_strategy_count=sum(r.get('complete_strategy_replayed', False) for r in result),
        limitations=['Recorded events are old originals, not a second old-source execution.',
            'Pure pattern replay does not verify market gates, model judgment, risk admission or orders.',
            'Capture provenance is declared by the pinned packet; controlled fixtures are never production evidence.'])


def main(argv=None):
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--packet', required=True, type=Path)
    parser.add_argument('--sha256', required=True)
    args = parser.parse_args(argv)
    if args.packet.stat().st_size > MAX_BYTES: parser.error('packet exceeds bounded 16MiB cap')
    raw = args.packet.read_bytes()
    if hashlib.sha256(raw).hexdigest() != args.sha256: parser.error('packet byte SHA mismatch')
    def denied(*a, **k): raise RuntimeError('OFFLINE_REPLAY_NETWORK_FORBIDDEN')
    socket.getaddrinfo = denied; socket.create_connection = denied; socket.socket.connect = denied
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / 'src'))
    packet = json.loads(raw)
    if packet.get('schema_version') == 'compressed-original/1':
        decompressor = zlib.decompressobj()
        original = decompressor.decompress(base64.b64decode(packet['data'], validate=True), MAX_BYTES+1)
        if (len(original) > MAX_BYTES or not decompressor.eof or len(original) != packet.get('original_bytes')
                or hashlib.sha256(original).hexdigest() != packet.get('original_sha256')):
            raise ValueError('ORIGINAL_COMPRESSED_PACKET_SHA_OR_LIMIT_MISMATCH')
        packet = json.loads(original)
    result = replay_packet(packet)
    result['packet_file_sha256'] = args.sha256
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return 0 if result['status'] == 'REPLAYED_AVAILABLE_ORIGINALS' else 2


if __name__ == '__main__': raise SystemExit(main())
