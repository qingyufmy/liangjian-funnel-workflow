"""Streaming B3 replay of original closed-bar windows, never a source repair.

This deliberately has no Settings/runtime/model/provider constructor. The
database must be an explicit consistent standalone local copy. All event rows
    are accounted for as WINDOW, PRE_KERNEL_BLOCK, GAP or NON_STRATEGY.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime
import hashlib
import inspect
import json
from pathlib import Path
import time

from .ablation.dataset import digest, stamp, session_minutes
from .ablation.exporter import _audit_helpers, _json, _readonly, _snapshot
from ..runtime.strategies import evaluate_strategy


class _SnapshotIndex:
    """Index only metadata in memory; preserve the shared snapshot validator.

    Exported evidence copies intentionally have no mutable SQL indices. This
    keeps each validated snapshot read bounded without modifying that copy.
    """
    def __init__(self, connection):
        self.connection = connection
        self.keys = defaultdict(list)
        for row in connection.execute('SELECT rowid,symbol,interval,decision_as_of,snapshot_id FROM minute_decision_snapshots'):
            self.keys[(row['symbol'],row['interval'],row['decision_as_of'])].append((row['rowid'],row['snapshot_id']))

    def execute(self, query, params):
        assert query.startswith('SELECT * FROM minute_decision_snapshots WHERE ')
        ids = [rowid for rowid,sid in self.keys.get(tuple(params[:3]),()) if len(params)==3 or sid==params[3]]
        placeholders = ','.join('?' for _ in ids) or 'NULL'
        return self.connection.execute('SELECT * FROM minute_decision_snapshots WHERE rowid IN ('+placeholders+')',ids)


def project_monitor_result(result):
    """Mirror the existing monitor warm-up and invalidation presentation."""
    action=result.get('action')
    reason=next(iter(result.get('reason_codes') or ()),None)
    if result.get('state')=='PLAN_INVALIDATED':
        action='PLAN_INVALIDATED'
    if action=='DATA_BLOCK' and reason in {'NO_CLOSED_5M','NO_CLOSED_15M'}:
        return 'START_CONFIRMATION','A4_SESSION_WARMUP'
    return action,reason


def _history_event(event,plan_id):
    """Only fields read by the existing episode function, not megabyte facts."""
    return {key:event[key] for key in ('event_id','minute_end','action','reason_code') if key in event} | {
        'effective':event.get('effective',0),'payload_json':json.dumps({'plan_id':plan_id})}


def iter_frozen_windows(*, state_db: Path, market_state_root: Path,
                        minute_db: Path | None = None, trade_dates=None):
    """Yield one window/gap/other record at a time, chronological original rows.

    WINDOW has ``plan``, ``bars`` (MinuteBar tuple), ``five_minute_bars``,
    ``market_context``, aware ``observation_time``/``decision_time``, original
    ``event``/``payload`` and source hashes. No artificial post-invalidation
    windows and no last-bar substitutions are generated.
    """
    from ..workflow import _intraday_market_context
    replay_clocks,overlay,audit_sha=_audit_helpers(None)
    wanted=set(map(str,trade_dates)) if trade_dates is not None else None
    with closing(_readonly(state_db)) as state, closing(_readonly(minute_db or state_db)) as cache:
        snapshots=_SnapshotIndex(cache)
        plans={row['plan_id']:dict(row) for row in state.execute('SELECT * FROM execution_plans')}
        positions={row[0] for row in state.execute('SELECT DISTINCT trade_date FROM a4_signal_lifecycles')}
        seen=set()
        market_cache={}
        for raw_event in state.execute('SELECT * FROM monitor_events ORDER BY minute_end,event_id'):
            event=dict(raw_event)
            day=event['minute_end'][:10]
            if wanted is not None and day not in wanted:
                continue
            payload=_json(event['payload_json'])
            pid=payload.get('plan_id')
            base={'trade_date':day,'event':event,'payload':payload,'plan_id':pid,
                  'recorded_action':event['action'],'recorded_reason':event['reason_code']}
            if not pid:
                yield {'kind':'NON_STRATEGY',**base}
                continue
            try:
                if day in positions:
                    raise ValueError('POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE')
                if pid not in plans:
                    raise ValueError('EVENT_PLAN_MISSING')
                row=plans[pid]
                plan=_json(row['payload_json'])
                symbol=plan['symbol']
                if symbol!=payload.get('symbol') or symbol!=row['symbol']:
                    raise ValueError('EVENT_PLAN_SYMBOL_MISMATCH')
                if not row.get('valid_from'):
                    raise ValueError('PLAN_ACTIVATION_UNPROVEN')
                closed,dispatch,basis=replay_clocks(payload,event['minute_end'])
                if plan.get('as_of') and stamp(plan['as_of'])>dispatch:
                    raise ValueError('FUTURE_PLAN_FACTS')
                if not stamp(row['valid_from']) <= dispatch <= stamp(row['expires_at']):
                    raise ValueError('EVENT_OUTSIDE_PLAN_VALIDITY')
                if stamp(row['expires_at']).date()!=dispatch.date():
                    raise ValueError('PLAN_TARGET_DAY_MISMATCH')
                if not payload.get('strategy'):
                    raise ValueError('STRATEGY_INPUT_NOT_FROZEN')
                if not payload['strategy'].get('action'):
                    if event['action']!='DATA_BLOCK':
                        raise ValueError('KERNEL_VERDICT_MISSING')
                    yield {'kind':'PRE_KERNEL_BLOCK',**base,'plan':plan,'plan_row':row,
                           'reason':'ORIGINAL_RUNTIME_BLOCK_BEFORE_KERNEL',
                           'kernel_evaluated':False,'provenance':{'event_sha256':digest(event),'plan_row_sha256':digest(row)}}
                    continue
                key=(pid,event['minute_end'])
                if key in seen:
                    raise ValueError('DUPLICATE_PLAN_DECISION_WINDOW')
                seen.add(key)
                sid=payload.get('minute_snapshot_id') or (payload['strategy'].get('execution_data') or {}).get('minute_snapshot_id')
                one=_snapshot(snapshots,symbol=symbol,interval='1m',dispatch=event['minute_end'],closed=closed,snapshot_id=sid)
                five=_snapshot(snapshots,symbol=symbol,interval='5m',dispatch=event['minute_end'],closed=closed,snapshot_id=sid)
                if not one or not one[1]:
                    raise ValueError('DECISION_1M_SNAPSHOT_MISSING')
                captured=max(stamp(value[2]['captured_at']) for value in (one,five) if value)
                persisted=stamp(event['created_at'])
                if captured>persisted or persisted<dispatch:
                    raise ValueError('SNAPSHOT_CAPTURE_OR_EVENT_CLOCK_CONFLICT')
                bucket=dispatch.replace(minute=dispatch.minute//5*5).strftime('%H%M')
                market_key=(day,bucket)
                if market_key not in market_cache:
                    path=Path(market_state_root)/day/(bucket+'.json')
                    raw=path.read_bytes() if path.is_file() else None
                    market_cache[market_key]=(_json(raw) if raw else {},hashlib.sha256(raw).hexdigest() if raw else None)
                market,market_sha=market_cache[market_key]
                context=_intraday_market_context(symbol,tuple(one[1]),tuple(five[1]) if five else (),current=closed,live_market_state=market)
                context=overlay(context,payload['strategy'])
                # The original frozen kernel scalar is an input to live
                # geometry, not proof of a full quote or executable arrival.
                price=payload['strategy'].get('live_entry_price')
                if price is not None:
                    if not isinstance(price,(int,float)) or price<=0:
                        raise ValueError('FROZEN_LIVE_ENTRY_PRICE_INVALID')
                    context['realtime_quote']={'available':True,'price':price,
                        'source':'FROZEN_KERNEL_LIVE_ENTRY_PRICE_NOT_FULL_QUOTE'}
                live=context.get('live_market_state') or {}
                if live.get('as_of') and stamp(live['as_of'])>persisted:
                    raise ValueError('FUTURE_FROZEN_MARKET')
                if live.get('trade_date') and live['trade_date']!=day:
                    raise ValueError('MARKET_WRONG_TRADE_DATE')
                sources={'1m':one[2],**({'5m':five[2]} if five else {})}
                provenance={'event_sha256':digest(event),'plan_row_sha256':digest(row),
                            'plan_sha256':digest(plan),'source_snapshots':sources,
                            'market_file_sha256':market_sha,'audit_source_sha256':audit_sha,
                            'observation_clock_basis':basis,'frozen_at':captured.isoformat(),
                            'persisted_at_upper_bound':persisted.isoformat(),
                            'exact_decision_wall_time_proven':False,'realtime_quote_proven':False}
                yield {'kind':'WINDOW',**base,'plan':plan,'plan_row':row,'bars':tuple(one[1]),
                       'five_minute_bars':tuple(five[1]) if five else (),
                       'market_context':context,'observation_time':closed,'decision_time':dispatch,
                       'kernel_evaluated':True,'provenance':provenance}
            except (ValueError,KeyError,TypeError) as exc:
                yield {'kind':'GAP',**base,'reason':str(exc)}


def run_baseline(*, state_db, market_state_root, output, minute_db=None, trade_dates=None, captured_at=None):
    """Write compact raw comparisons and explicit evidence gaps, no bars dump."""
    output=Path(output)
    output.mkdir(parents=True,exist_ok=False)
    from ..runtime.monitor import MonitorEngine
    start=time.monotonic();days=defaultdict(Counter);gaps=Counter();diffs=[];history=defaultdict(list);by_plan=defaultdict(list)
    with (output/'comparisons.jsonl').open('x',encoding='utf-8') as comparisons, (output/'gaps.jsonl').open('x',encoding='utf-8') as gap_file:
        for item in iter_frozen_windows(state_db=state_db,minute_db=minute_db,market_state_root=market_state_root,trade_dates=trade_dates):
            counts=days[item['trade_date']];counts['records']+=1;counts[item['kind']]+=1
            day_history=history[item['trade_date']]
            compact_event=_history_event(item['event'],item['plan_id'])
            if item['plan_id']:by_plan[item['plan_id']].append(compact_event)
            if item['kind']=='GAP':
                gaps[item['reason']]+=1;gap_file.write(json.dumps(item,ensure_ascii=False)+'\n');day_history.append(compact_event);continue
            if item['kind'] in {'NON_STRATEGY','PRE_KERNEL_BLOCK'}:
                comparisons.write(json.dumps({'kind':item['kind'],'trade_date':item['trade_date'],'event_id':item['event']['event_id'],'plan_id':item['plan_id'],'at':item['event']['minute_end'],'action':item['recorded_action'],'reason':item['recorded_reason']})+'\n');day_history.append(compact_event);continue
            result=evaluate_strategy(item['plan'],item['bars'],now=item['observation_time'],decision_time=item['decision_time'],position=None,market_context=item['market_context']).model_dump(mode='json')
            action,reason=project_monitor_result(result)
            frozen=item['payload']['strategy']
            expected_action,expected_reason=project_monitor_result(frozen)
            # Kernel and final monitor stages are separate: LLM_VETO is not a
            # strategy verdict and may not be erased to manufacture equality.
            kernel_equal=(action,reason)==(expected_action,expected_reason)
            final_action,final_reason=action,reason
            final_basis='DIRECT_KERNEL_MONITOR_PROJECTION'
            if action in {'BUY_SIGNAL','ADD_SIGNAL','SELL_SIGNAL','REDUCE_SIGNAL','FORCED_RISK_EXIT'}:
                if MonitorEngine._resolved_trigger_episode(tuple(day_history),plan_id=item['plan_id'],minute=item['decision_time'],action=action):
                    final_action,final_reason='NO_ACTION','SIGNAL_ALREADY_EMITTED'
                    final_basis='ACTUAL_MONITOR_RESOLVED_TRIGGER_EPISODE'
                elif item['payload'].get('llm_veto') is True and item['payload'].get('llm_reason_code'):
                    final_action,final_reason='LLM_VETO',item['payload']['llm_reason_code']
                    final_basis='FROZEN_EXPLICIT_LLM_VETO_NOT_REINFERRED'
                else:
                    final_action,final_reason=None,None
                    final_basis='POST_KERNEL_FINAL_ACTION_UNVERIFIED'
            monitor_equal=(final_action,final_reason)==(item['recorded_action'],item['recorded_reason'])
            counts['kernel_equal']+=int(kernel_equal);counts['monitor_equal']+=int(monitor_equal)
            row={'kind':'WINDOW','event_id':item['event']['event_id'],'trade_date':item['trade_date'],'plan_id':item['plan_id'],'symbol':item['plan']['symbol'],'at':item['event']['minute_end'],
                 'recorded_action':item['recorded_action'],'recorded_reason':item['recorded_reason'],
                 'frozen_kernel_action':expected_action,'frozen_kernel_reason':expected_reason,'replay_action':action,'replay_reason':reason,
                 'replay_final_action':final_action,'replay_final_reason':final_reason,'final_basis':final_basis,
                 'kernel_equal':kernel_equal,'monitor_equal':monitor_equal,'provenance':item['provenance']}
            comparisons.write(json.dumps(row,ensure_ascii=False)+'\n')
            if not monitor_equal or not kernel_equal:diffs.append(row)
            day_history.append(compact_event)
    coverage=[]
    with closing(_readonly(state_db)) as state:
        for row in state.execute('SELECT * FROM execution_plans ORDER BY expires_at,plan_id'):
            day=row['expires_at'][:10]
            if day not in days:continue
            actual=by_plan[row['plan_id']]
            if not row['valid_from']:
                coverage.append({'trade_date':day,'plan_id':row['plan_id'],'symbol':row['symbol'],'status':'NOT_ACTIVATED','observed_records':len(actual)})
                continue
            start_at=stamp(row['valid_from']);end_at=stamp(row['expires_at'])
            invalid=[stamp(event['minute_end']) for event in actual if event['action']=='PLAN_INVALIDATED']
            if invalid:end_at=min(end_at,min(invalid))
            if captured_at and stamp(captured_at).date()==end_at.date():end_at=min(end_at,stamp(captured_at))
            expected={at for at in session_minutes(day) if start_at<=at<=end_at}
            present={stamp(event['minute_end']) for event in actual}
            missing=sorted(expected-present)
            coverage.append({'trade_date':day,'plan_id':row['plan_id'],'symbol':row['symbol'],
                             'status':'GAPS' if missing else 'COVERED_TO_SOURCE_CUTOFF',
                             'expected_records':len(expected),'observed_records':len(actual),
                             'missing_count':len(missing),'missing_minutes':[at.isoformat() for at in missing],
                             'invalidation_basis':'ORIGINAL_TERMINAL_EVENT' if invalid else 'NO_TERMINAL_EVENT_NOT_UPDATED_AT',
                             'duplicate_count':len(actual)-len(present)})
    summary={'scope':'FROZEN_CLOSED_BAR_NO_POSITION_NO_MODEL_NO_ORDERS','captured_at':captured_at,
             'runtime_seconds':round(time.monotonic()-start,3),'days':{day:dict(c) for day,c in sorted(days.items())},
             'gap_reasons':dict(gaps),'raw_diff_count':len(diffs),'raw_diffs':diffs,
             'plan_coverage':coverage,
             'partial_trade_dates':[day for day in days if captured_at and day==str(stamp(captured_at).date()) and stamp(captured_at).hour<15],
             'strategy_sha256':hashlib.sha256(Path(inspect.getfile(evaluate_strategy)).read_bytes()).hexdigest(),
             'source_mutation':False,'model_calls':0,'provider_calls':0,
             'limitations':['10-15 is PARTIAL if the source capture is before market close.','This proves closed-bar decisions only; LLM/risk/orders/fills and exact quote arrival are separate stages.']}
    with (output/'summary.json').open('x',encoding='utf-8') as handle:json.dump(summary,handle,ensure_ascii=False,indent=2)
    return summary
