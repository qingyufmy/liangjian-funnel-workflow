"""Read-only adaptation of the existing frozen A4 audit into WP1 inputs.

All source paths are explicit. No Settings, runtime constructor, SSH, provider,
model, notification or source repair is involved. Trigger windows use exact
decision snapshots; later archive versions are confined to outcome prices.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import runpy
import sqlite3
import zlib
from contextlib import closing
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from ...data.mootdx import MinuteBar
from .dataset import SCHEMA, WINDOW_FIELDS, digest, stamp
from .price_limits import resolve_price_limits


class ExportError(ValueError):
    pass


def _readonly(path: Path) -> sqlite3.Connection:
    path = Path(path).resolve()
    if not path.is_file():
        raise ExportError('SOURCE_SQLITE_MISSING')
    # These are explicit consistent standalone copies, not live databases.
    # mode=ro alone still creates WAL/SHM metadata for a WAL-header copy.
    # Never ignore substantive WAL data when opting into immutable reads.
    wal = Path(str(path) + '-wal')
    if wal.is_file() and wal.stat().st_size:
        raise ExportError('NONEMPTY_WAL_SOURCE_REQUIRES_CONSISTENT_COPY')
    connection = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA query_only=ON')
    connection.execute('BEGIN')
    return connection


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _json(raw: Any) -> dict:
    result = json.loads(raw or '{}')
    if not isinstance(result, dict):
        raise ExportError('SOURCE_OBJECT_REQUIRED')
    return result


def _audit_helpers(path: Path | None):
    path = Path(path) if path else Path(__file__).resolve().parents[4] / 'scripts/audit_frozen_a4_decisions.py'
    if not path.is_file():
        raise ExportError('FROZEN_A4_AUDIT_SOURCE_MISSING')
    # Loading defines the audit helpers; its SSH main is never invoked.
    module = runpy.run_path(str(path), run_name='wp1_frozen_audit_helpers')
    return module['replay_clocks'], module['frozen_market_overlay'], hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot(connection, *, symbol, interval, dispatch, closed, snapshot_id):
    rows = connection.execute(
        'SELECT * FROM minute_decision_snapshots WHERE symbol=? AND interval=? AND decision_as_of=?'
        + (' AND snapshot_id=?' if snapshot_id else ''),
        (symbol, interval, dispatch, *([snapshot_id] if snapshot_id else [])),
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise ExportError('AMBIGUOUS_DECISION_SNAPSHOT')
    row = dict(rows[0])
    try:
        raw = zlib.decompress(row['payload_zlib'])
    except zlib.error as exc:
        raise ExportError('SNAPSHOT_COMPRESSION_INVALID') from exc
    if hashlib.sha256(raw).hexdigest() != row['payload_sha256']:
        raise ExportError('SNAPSHOT_HASH_MISMATCH')
    values = json.loads(raw)
    parsed = [MinuteBar.model_validate(value) for value in values]
    if any(bar.symbol != symbol or bar.interval != interval for bar in parsed):
        raise ExportError('SNAPSHOT_SYMBOL_OR_INTERVAL_MISMATCH')
    if any(bar.bar_end > closed for bar in parsed):
        raise ExportError('FUTURE_BAR_AT_FROZEN_CUTOFF')
    if len({bar.bar_end for bar in parsed}) != len(parsed):
        raise ExportError('DUPLICATE_SNAPSHOT_BAR')
    meta = {key:row[key] for key in ('snapshot_id','decision_as_of','captured_at','payload_sha256')}
    meta['bars_sha256'] = digest(values)
    return values, parsed, meta


def _archive(connection, symbols, *, day, outcome_until, gaps):
    """Outcome-only revision selection; never a substitute for a snapshot."""
    tables = _tables(connection)
    selected: dict[str, dict[tuple[str, str], tuple[str, dict]]] = {symbol:{} for symbol in symbols}
    selection_meta = []
    if 'minute_bar_versions' in tables:
        for symbol in sorted(symbols):
            for row in connection.execute('SELECT * FROM minute_bar_versions WHERE symbol=? AND bar_end>=? AND bar_end<? ORDER BY observed_at,version_id',
                                          (symbol,str(day),str(outcome_until + timedelta(days=1)))):
                item = dict(row)
                bar = _json(item['payload_json'])
                key = (item['interval'], item['bar_end'])
                previous = selected[symbol].get(key)
                if previous and previous[0] == item['observed_at'] and digest(previous[1]) != digest(bar):
                    raise ExportError('AMBIGUOUS_ARCHIVE_REVISION')
                selected[symbol][key] = (item['observed_at'],bar)
                selection_meta.append({k:item[k] for k in ('version_id','symbol','interval','bar_end','observed_at')})
    if 'minute_bars' in tables:
        # First-value table has no unit/finality metadata. Retain its legacy
        # contract rather than labelling inferred shares as observed evidence.
        for symbol in sorted(symbols):
            for row in connection.execute('SELECT * FROM minute_bars WHERE symbol=? AND bar_end>=? AND bar_end<? ORDER BY bar_end',
                                          (symbol,str(day),str(outcome_until + timedelta(days=1)))):
                raw = dict(row)
                key = (raw['interval'], raw['bar_end'])
                if key in selected[symbol]:
                    continue
                value = {k:raw[k] for k in ('symbol','interval','bar_end','source_id','adjust_mode')}
                value.update({k:float(raw[k+'_value']) for k in ('open','high','low','close','volume','amount')})
                value['volume_unit'] = 'legacy_unspecified'
                selected[symbol][key] = ('',value)
                gaps.append({'symbol':symbol,'at':raw['bar_end'],'reason':'ARCHIVE_VOLUME_UNIT_UNPROVEN'})
    archives, outcomes = {}, {}
    for symbol in sorted(symbols):
        ordered = [value for _,value in sorted(selected[symbol].values(), key=lambda pair:(pair[1]['bar_end'],pair[1]['interval']))]
        payload = {'one_minute':[bar for bar in ordered if bar['interval']=='1m' and stamp(bar['bar_end']).date()==day],
                   'five_minute':[bar for bar in ordered if bar['interval']=='5m' and stamp(bar['bar_end']).date()==day]}
        archives[symbol] = {**payload,'sha256':digest(payload)}
        outcomes[symbol] = [bar for bar in ordered if bar['interval']=='1m']
    return archives, outcomes, digest(selection_meta)


def export_day(*, state_db: Path, minute_db: Path, market_state_root: Path,
               trade_date: str | date, outcome_until: str | date | None = None,
               audit_script: Path | None = None) -> dict[str, Any]:
    """Export one historical day from explicit local/readonly source paths.

    The state selection intentionally includes EXPIRED/INVALIDATED rows, using
    recorded valid_from as activation proof. Missing snapshots stay gaps.
    Position days are retained in the manifest but not replayed with position=None.
    """
    from ...workflow import _intraday_market_context
    replay_clocks, overlay, audit_sha = _audit_helpers(audit_script)
    day = date.fromisoformat(str(trade_date))
    until = date.fromisoformat(str(outcome_until or day))
    if until < day:
        raise ExportError('OUTCOME_RANGE_BEFORE_TRADE_DATE')
    gaps: list[dict] = []
    market_files = {}
    with closing(_readonly(state_db)) as state, closing(_readonly(minute_db)) as cache:
        if not {'execution_plans','monitor_events'} <= _tables(state):
            raise ExportError('A4_STATE_TABLES_MISSING')
        if 'minute_decision_snapshots' not in _tables(cache):
            raise ExportError('FROZEN_SNAPSHOT_TABLE_MISSING')
        plan_rows = [dict(row) for row in state.execute(
            'SELECT * FROM execution_plans WHERE (expires_at>=? AND expires_at<?) '
            'OR (valid_from>=? AND valid_from<?) ORDER BY plan_id',
            (str(day),str(day)+'T15:01',str(day),str(day+timedelta(days=1))))]
        events = [dict(row) for row in state.execute(
            'SELECT * FROM monitor_events WHERE minute_end>=? AND minute_end<? ORDER BY minute_end,event_id',
            (str(day),str(day)+'T15:01'))]
        lifecycle_count = state.execute('SELECT count(*) FROM a4_signal_lifecycles WHERE trade_date=?',(str(day),)).fetchone()[0] if 'a4_signal_lifecycles' in _tables(state) else None
        if lifecycle_count is None:
            gaps.append({'reason':'POSITION_LIFECYCLE_TABLE_MISSING'})
        elif lifecycle_count:
            gaps.append({'reason':'POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE','count':lifecycle_count})
        plans, excluded = [], []
        for row in plan_rows:
            if not row.get('valid_from'):
                excluded.append(row['plan_id'])
                continue
            plan = _json(row['payload_json'])
            record = {'plan_id':row['plan_id'],'payload':plan,'sha256':digest(plan),
                      'activated_at':row['valid_from'],'expires_at':row['expires_at'],
                      'source_status':row['status'],'activation_basis':'EXECUTION_PLANS_VALID_FROM',
                      'source_row_sha256':digest(row)}
            invalidated = [event['minute_end'] for event in events if event.get('effective')
                           and event['action']=='PLAN_INVALIDATED'
                           and _json(event['payload_json']).get('plan_id')==row['plan_id']]
            if invalidated:
                record['invalidated_at'] = min(invalidated,key=stamp)
            elif row['status']=='INVALIDATED':
                gaps.append({'plan_id':row['plan_id'],'reason':'INVALIDATION_TIME_UNPROVEN'})
            plans.append(record)
            if not row.get('expires_at'):
                gaps.append({'plan_id':row['plan_id'],'reason':'ACTIVATED_PLAN_EXPIRY_MISSING'})
            elif stamp(row['expires_at']).date() != day:
                gaps.append({'plan_id':row['plan_id'],'reason':'PLAN_VALIDITY_NOT_SINGLE_TARGET_DAY'})
        by_id = {record['plan_id']:record for record in plans}
        windows = []
        seen = set()
        if lifecycle_count == 0:
            for event in events:
                payload = _json(event['payload_json'])
                pid = payload.get('plan_id')
                if pid not in by_id:
                    continue
                if not payload.get('strategy'):
                    gaps.append({'plan_id':pid,'at':event['minute_end'],'reason':'STRATEGY_INPUT_NOT_FROZEN'})
                    continue
                try:
                    closed, dispatch, clock_basis = replay_clocks(payload,event['minute_end'])
                except ValueError as exc:
                    gaps.append({'plan_id':pid,'at':event['minute_end'],'reason':str(exc)})
                    continue
                record = by_id[pid]
                if not record.get('expires_at'):
                    continue
                end = stamp(record.get('invalidated_at') or record['expires_at'])
                if not stamp(record['activated_at']) <= dispatch <= end:
                    continue
                key = (pid,event['minute_end'])
                if key in seen:
                    raise ExportError('DUPLICATE_PLAN_DECISION_WINDOW')
                seen.add(key)
                symbol = record['payload']['symbol']
                snapshot_id = payload.get('minute_snapshot_id') or (payload['strategy'].get('execution_data') or {}).get('minute_snapshot_id')
                one = _snapshot(cache,symbol=symbol,interval='1m',dispatch=event['minute_end'],closed=closed,snapshot_id=snapshot_id)
                five = _snapshot(cache,symbol=symbol,interval='5m',dispatch=event['minute_end'],closed=closed,snapshot_id=snapshot_id)
                if not one or not one[0]:
                    gaps.append({'plan_id':pid,'at':event['minute_end'],'reason':'DECISION_1M_SNAPSHOT_MISSING'})
                    continue
                bucket = dispatch.replace(minute=dispatch.minute//5*5).strftime('%H%M')
                market_path = Path(market_state_root)/str(day)/(bucket+'.json')
                market = {}
                if market_path.is_file():
                    raw_market = market_path.read_bytes()
                    market = _json(raw_market)
                    market_files[str(market_path)] = hashlib.sha256(raw_market).hexdigest()
                else:
                    gaps.append({'plan_id':pid,'at':event['minute_end'],'reason':'FROZEN_MARKET_FILE_MISSING'})
                context = _intraday_market_context(symbol,tuple(one[1]),tuple(five[1]) if five else (),
                                                  current=closed,live_market_state=market)
                context = overlay(context,payload['strategy'])
                captured = max([stamp(one[2]['captured_at']),*([stamp(five[2]['captured_at'])] if five else [])])
                persisted = stamp(event['created_at'])
                if captured > persisted or persisted < dispatch:
                    raise ExportError('SNAPSHOT_CAPTURE_OR_EVENT_CLOCK_CONFLICT')
                window = {'plan_id':pid,'decision_time':event['minute_end'],
                          'observation_time':closed.isoformat(),'frozen_at':captured.isoformat(),
                          'evaluated_at':persisted.isoformat(),'bars':one[0], 'market_context':context,
                          'recorded_action':event['action'],'recorded_reason':event['reason_code'],
                          'recorded_event_id':event['event_id'],'recorded_event_sha256':digest(event),
                          'observation_clock_basis':clock_basis,
                          'knowledge_clock_basis':'EVENT_PERSISTED_AT_UPPER_BOUND_NOT_EXACT_DECISION',
                          'evidence_limitations':['REALTIME_QUOTE_NOT_FROZEN','EXACT_STRATEGY_DECISION_WALL_TIME_NOT_RECORDED'],
                          'source_snapshots':{'1m':one[2],**({'5m':five[2]} if five else {})}}
                window['input_sha256'] = digest({key:window[key] for key in WINDOW_FIELDS})
                windows.append(window)
        symbols = {record['payload']['symbol'] for record in plans}
        archives, outcomes, archive_meta_sha = _archive(cache,symbols,day=day,outcome_until=until,gaps=gaps)
        # Copy only outcome rows. The same original archive objects may also be
        # used by input/coverage validation; never annotate those trigger legs.
        limit_counts: dict[str, int] = {}
        for symbol, values in outcomes.items():
            projected = []
            for raw in values:
                resolution = resolve_price_limits(raw, symbol=symbol, at=stamp(raw['bar_end']))
                status = resolution['status']
                limit_counts[status] = limit_counts.get(status, 0) + 1
                projected.append({**raw, 'price_limit_source_bar_sha256': digest(raw),
                    'derived_limit_prices': resolution['derived_limit_prices'],
                    'price_limit_resolution': {k:v for k,v in resolution.items() if k != 'derived_limit_prices'}})
            outcomes[symbol] = projected
        labels = {symbol:[] for symbol in symbols}
        if 'astock_outcome_labels' in _tables(state):
            for symbol in symbols:
                labels[symbol] = [dict(row) for row in state.execute('SELECT * FROM astock_outcome_labels WHERE symbol=? AND trade_date>=? AND trade_date<=?',(symbol,str(day),str(until)))]
        manifest = {'schema_version':'liangjian-ablation-export-manifest/1',
                    'source_path':'audit_frozen_a4_decisions.py','audit_source_sha256':audit_sha,
                    'context_builder_sha256':hashlib.sha256(inspect.getsource(_intraday_market_context).encode()).hexdigest(),
                    'source_state_db':str(Path(state_db).resolve()),'source_minute_db':str(Path(minute_db).resolve()),
                    'state_selection_sha256':digest({'plans':plan_rows,'events':events}),
                    'archive_selection_sha256':archive_meta_sha,'market_file_sha256':market_files,
                    'activated_plan_ids':sorted(by_id),'not_activated_plan_ids':sorted(excluded),
                    'candidate_plan_count':len(plan_rows),'lifecycle_count':lifecycle_count,
                    'window_count':len(windows),'outcome_until':str(until),'gaps':gaps,
                    'scope':'FROZEN_CLOSED_BAR_STRATEGY_RESEARCH_NO_POSITION_NO_ORIGINAL_REALTIME_QUOTE',
                    'source_mutation':False,'model_calls':0,'provider_calls':0,
                    'price_limit_policy':'SAME_DAY_EXPLICIT_OR_FROZEN_ORDINARY_PRECLOSE_RULE; OUTCOME_ONLY; CONFLICT_BLOCKS',
                    'price_limit_resolution_counts':limit_counts,
                    'price_limit_resolver_sha256':hashlib.sha256(Path(inspect.getfile(resolve_price_limits)).read_bytes()).hexdigest()}
        manifest['sha256'] = digest(manifest)
        data = {'schema_version':SCHEMA,'trade_date':str(day),'plans':plans,'windows':windows,
                'minute_archives':archives,'outcome_bars':outcomes,'outcome_labels':labels,
                'outcome_data_sha256':digest({'outcome_bars':outcomes,'outcome_labels':labels}),
                'export_manifest':manifest}
        return data


def _write_json_file(value: dict, path: Path) -> str:
    """Encode and hash bounded chunks instead of allocating a second day copy."""
    checksum = hashlib.sha256()
    chunks, length = [], 0
    encoder = json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':'))
    with path.open('xb') as handle:
        for chunk in encoder.iterencode(value):
            chunks.append(chunk)
            length += len(chunk)
            if length >= 1_048_576:
                raw = ''.join(chunks).encode('utf-8')
                handle.write(raw)
                checksum.update(raw)
                chunks, length = [], 0
        if chunks:
            raw = ''.join(chunks).encode('utf-8')
            handle.write(raw)
            checksum.update(raw)
    return checksum.hexdigest()


def write_export_bundle(datasets: Iterable[dict], output: Path) -> dict:
    output = Path(output)
    if output.exists():
        raise ExportError('REFUSE_OVERWRITE')
    output.mkdir(parents=True,exist_ok=False)
    files, days, exported, failed, day_receipts = {}, set(), [], [], []
    for dataset in datasets:
        day = dataset['trade_date']
        if day in days:
            raise ExportError('DUPLICATE_DAY_EXPORT')
        days.add(day)
        failure = dataset.get('export_failure')
        if failure:
            name = day+'.failure.json'
            failed.append(day)
            day_receipts.append({'trade_date':day,'status':'DATA_LIMITED',**failure})
        else:
            name = day+'.json'
            exported.append(day)
            manifest = dataset.get('export_manifest') or {}
            day_receipts.append({'trade_date':day,'status':'EXPORTED_PENDING_COVERAGE',
                'activated_plan_count':len(manifest.get('activated_plan_ids') or []),
                'window_count':manifest.get('window_count'),
                'source_gap_count':len(manifest.get('gaps') or []),
                'lifecycle_count':manifest.get('lifecycle_count')})
        files[name] = _write_json_file(dataset,output/name)
        # Release this day's graph before requesting the next generator value.
        del dataset
    receipt = {'schema_version':'liangjian-ablation-export-bundle/1','files':files,
               'source_mutation':False,'trade_dates':sorted(days),
               'exported_trade_dates':exported,'failed_trade_dates':failed,'days':day_receipts,
               'source_gap_count':sum(day.get('source_gap_count',0) for day in day_receipts),
               'status':'DATA_LIMITED' if failed else 'EXPORTED_PENDING_COVERAGE'}
    with (output/'manifest.json').open('x',encoding='utf-8') as handle:
        json.dump(receipt,handle,ensure_ascii=False,indent=2)
    return receipt


__all__ = ['ExportError','export_day','write_export_bundle']
