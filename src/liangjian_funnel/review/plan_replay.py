"""Read-only A3 plan entry replay. Post-hoc data never proves a missed trade."""
from collections import Counter
from datetime import datetime, timedelta
import hashlib
import json
import time
import inspect
from pathlib import Path

from ..runtime.strategies import evaluate_strategy

PROFILES = ('LEADER_INTRADAY', 'MA520_SWING', 'TREND_MA5')
NAMES = dict(zip(PROFILES, ('龙头策略', '520策略', '趋势五日线')))


def mapping(value):
    if isinstance(value, dict):
        return value
    try:
        result = json.loads(value or '{}')
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}


def stamp(value):
    try:
        result = datetime.fromisoformat(str(value))
        return result if result.tzinfo else None
    except (ValueError, TypeError):
        return None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     default=str).encode()).hexdigest()


def replay_plan_pool(plan_rows, event_rows, *, minute_store, cutoff, market_state_dir=None, max_seconds=20):
    """Replay assigned strategy, never force a trend plan through leader logic.

    Uses only local archived bars and never calls providers, models, orders or
    notification APIs. Missing historical market/position/model context remains
    unverified. All suspicious windows survive model projection in aggregates.
    """
    from ..workflow import _intraday_market_context, _a4_execution_cutoff
    from ..runtime.live_market import _read_cache
    deadline = time.monotonic() + max_seconds
    events = {}
    for row in event_rows:
        payload = mapping(row.get('payload_json'))
        key = str(payload.get('plan_id') or '')
        at = stamp(row.get('minute_end'))
        if at and at <= cutoff:
            events.setdefault(key, []).append(row)
    minutes = []
    for hour, minute in ((9, 31), (13, 1)):
        start = cutoff.replace(hour=hour, minute=minute, second=0, microsecond=0)
        minutes.extend(start + timedelta(minutes=i) for i in range(120)
                       if start + timedelta(minutes=i) <= cutoff)
    results, inputs, windows, cache, frozen_plans = [], {}, [], {}, {}
    market_inputs = {}
    for row in plan_rows:
        plan_id, symbol = str(row.get('plan_id') or ''), str(row.get('symbol') or '')
        plan = mapping(row.get('payload_json'))
        frozen_plans[plan_id] = plan
        profile = str(plan.get('strategy_profile') or '')
        plan_events = events.get(plan_id, [])
        actual = {stamp(e.get('minute_end')): e for e in plan_events}
        start, end = stamp(row.get('valid_from')), stamp(row.get('expires_at'))
        result = {'evidence_id': 'A5R:PLAN:' + plan_id, 'plan_id': plan_id, 'symbol': symbol,
                  'strategy_profile': profile, 'status': 'DATA_LIMITED',
                  'expected_minutes': 0, 'replayed_minutes': 0, 'missing_bars': 0,
                  'missing_decisions': 0, 'technical_trigger_minutes': 0,
                  'context_unverified_minutes': 0,
                  'recorded_signal_not_reproduced': 0,
                  'unconfirmed_trigger_minutes': 0, 'reasons': {}}
        results.append(result)
        if profile not in PROFILES or not start:
            result['reason_code'] = 'UNKNOWN_STRATEGY' if profile not in PROFILES else 'ACTIVATION_NOT_PROVEN'
            continue
        invalidated = [stamp(e.get('minute_end')) for e in plan_events
                       if e.get('effective') and e.get('action') == 'PLAN_INVALIDATED']
        if invalidated:
            end = min([v for v in [end, *invalidated] if v is not None])
        expected = [t for t in minutes if start <= t and (end is None or t <= end)]
        result['expected_minutes'] = len(expected)
        if minute_store is None:
            result['reason_code'] = 'MINUTE_ARCHIVE_UNAVAILABLE'
            continue
        if time.monotonic() >= deadline:
            result['reason_code'] = 'REPLAY_DEADLINE_EXCEEDED'
            continue
        try:
            if symbol not in cache:
                selection = minute_store.latest_decision_snapshot(symbol, '1m', as_of=cutoff)
                one = selection.get('bars', ()) if selection else minute_store.load_latest(symbol, '1m', limit=300)
                five = minute_store.load_latest(symbol, '5m', limit=800)
                one = tuple(sorted((b for b in one if b.bar_end.date() == cutoff.date() and b.bar_end <= cutoff), key=lambda b: b.bar_end))
                five = tuple(sorted((b for b in five if b.bar_end <= cutoff), key=lambda b: b.bar_end))
                cache[symbol] = one, five
                raw = {'one_minute': [b.model_dump(mode='json') for b in one],
                       'five_minute': [b.model_dump(mode='json') for b in five]}
                inputs[symbol] = {'evidence_id': 'A5R:INPUT:' + symbol, 'sha256': digest(raw), **raw}
            one, five = cache[symbol]
            result['input_sha256'] = inputs[symbol]['sha256']
            result['plan_sha256'] = digest(plan)
            entered = False
            reasons = Counter()
            for t in expected:
                if time.monotonic() >= deadline:
                    result['reason_code'] = 'REPLAY_DEADLINE_EXCEEDED'
                    break
                recorded = actual.get(t)
                if entered:
                    reasons['POSITION_LIFECYCLE_NOT_REPLAYED'] += 1
                    continue
                # A recorded lifecycle transition applies even if its bar is absent.
                if recorded and recorded.get('action') in ('BUY_SIGNAL', 'PLAN_INVALIDATED') and recorded.get('effective'):
                    entered = True
                if recorded is None:
                    result['missing_decisions'] += 1
                    windows.append({'evidence_id': f'A5R:MISSING:{plan_id}:{t.isoformat()}',
                                    'plan_id': plan_id, 'minute_end': t.isoformat(),
                                    'recorded_action': None, 'recorded_reason': None,
                                    'classification': 'MISSING_EXECUTION_RECORD'})
                observation_end = _a4_execution_cutoff(t)
                if observation_end is None:
                    reasons['WAIT_FIRST_CLOSED_MINUTE'] += 1
                    continue
                history = tuple(b for b in one if b.bar_end <= observation_end)
                if not history or history[-1].bar_end != observation_end:
                    result['missing_bars'] += 1
                    continue
                bucket = t.replace(minute=t.minute // 5 * 5).strftime('%H%M')
                market = None
                if market_state_dir:
                    market = _read_cache(Path(market_state_dir) / t.date().isoformat() / (bucket + '.json'), current=t)
                if market:
                    market_inputs[digest(market)] = market
                else:
                    result['context_unverified_minutes'] += 1
                context = _intraday_market_context(symbol, history, tuple(b for b in five if b.bar_end <= observation_end), current=t,
                                                  execution_cutoff=observation_end, live_market_state=market)
                evaluation = evaluate_strategy(plan, history, now=observation_end, decision_time=t,
                                               market_context=context).model_dump(mode='json')
                result['replayed_minutes'] += 1
                for reason in evaluation.get('reason_codes', []):
                    reasons[reason] += 1
                action = evaluation.get('action')
                if recorded and recorded.get('effective') and recorded.get('action') == 'BUY_SIGNAL' and action != 'BUY_SIGNAL':
                    result['recorded_signal_not_reproduced'] += 1
                    windows.append({'evidence_id': f'A5R:MISMATCH:{plan_id}:{t.isoformat()}',
                                    'plan_id': plan_id, 'minute_end': t.isoformat(),
                                    'recorded_action': 'BUY_SIGNAL', 'recorded_reason': recorded.get('reason_code'),
                                    'replay_action': action, 'replay_reasons': evaluation.get('reason_codes', []),
                                    'classification': 'RECORDED_SIGNAL_NOT_REPRODUCED'})
                if action == 'BUY_SIGNAL':
                    result['technical_trigger_minutes'] += 1
                    matched = bool(recorded and recorded.get('effective') and recorded.get('action') == action)
                    result['unconfirmed_trigger_minutes'] += int(not matched)
                    windows.append({'evidence_id': f'A5R:WINDOW:{plan_id}:{t.isoformat()}',
                                    'plan_id': plan_id, 'minute_end': t.isoformat(),
                                    'replay_action': action, 'recorded_action': recorded.get('action') if recorded else None,
                                    'recorded_reason': recorded.get('reason_code') if recorded else None,
                                    'classification': 'RECORDED_SIGNAL' if matched else 'REQUIRES_FROZEN_CONTEXT_AUDIT',
                                    'input_sha256': result['input_sha256']})
            result['reasons'] = dict(reasons)
            if not result.get('reason_code'):
                result['status'] = 'DATA_LIMITED' if result['missing_bars'] or result['context_unverified_minutes'] else 'REQUIRES_REVIEW' if result['missing_decisions'] or result['unconfirmed_trigger_minutes'] or result['recorded_signal_not_reproduced'] else 'REPLAYED'
        except Exception as exc:
            result['reason_code'] = 'REPLAY_INPUT_OR_EVALUATION_FAILED'
            result['error_type'] = type(exc).__name__
    coverage = {p: {'plan_count': sum(r['strategy_profile'] == p for r in results),
                    'status': 'COVERED' if any(r['strategy_profile'] == p for r in results) else 'NO_PLAN_NOT_TESTED'} for p in PROFILES}
    return {'schema_version': 'a5-plan-entry-replay/1', 'evidence_id': 'A5R:SUMMARY',
            'cutoff_at': cutoff.isoformat(), 'mode': 'EX_POST_ARCHIVED_TECHNICAL_ENTRY_REPLAY',
            'production_mutation': False, 'model_calls': 0, 'strategy_coverage': coverage,
            'strategy_source_sha256': hashlib.sha256(Path(inspect.getfile(evaluate_strategy)).read_bytes()).hexdigest(),
            'plans': results, 'trigger_windows': windows, 'archived_inputs': inputs, 'frozen_plans': frozen_plans,
            'archived_market_states': market_inputs,
            'confirmed_missed_trade_count': None,
            'limitations': ['回放最终归档K线，不等于当时已收到的数据版本。',
                           '不假设模型通过、市场环境放行、资金可用或能够成交。',
                           '未重建当时实时报价；归档市场状态也可能经过修订，不能用于确认历史成交。',
                           '仅核对入场机会；首次实际入场或失效后不虚构持仓状态继续回放。',
                           '未覆盖的策略、缺失窗口及超时必须标为未验证，不得判定没有漏单。']}


def model_projection(audit):
    result = {k: v for k, v in audit.items() if k not in ('archived_inputs', 'trigger_windows', 'frozen_plans', 'archived_market_states')}
    groups = {}
    for row in audit.get('trigger_windows', []):
        key = (row['plan_id'], row['classification'], row['recorded_action'], row['recorded_reason'])
        groups.setdefault(key, []).append(row)
    result['trigger_groups'] = [
        {'evidence_id': 'A5R:GROUP:' + digest(rows), 'sha256': digest(rows),
         'plan_id': key[0], 'classification': key[1], 'recorded_action': key[2],
         'recorded_reason': key[3], 'count': len(rows),
         'first': min(r['minute_end'] for r in rows), 'last': max(r['minute_end'] for r in rows)}
        for key, rows in groups.items()]
    return result


def markdown_lines(audit):
    if not audit:
        return ['## A3计划池策略回放', '', '本次冻结事实未包含回放证据，未验证。', '']
    lines = ['## A3计划池策略回放', '', '事后技术回放不等于已证实漏单；缺少当时市场、账户或模型证据时仅列待核查。', '']
    for profile, row in audit['strategy_coverage'].items():
        lines.append(f"- {NAMES[profile]}：{row['plan_count']}个计划" + ('，无计划，未验证。' if not row['plan_count'] else '。'))
    rows = audit['plans']
    lines.append(f"- 应检查{sum(r['expected_minutes'] for r in rows)}个计划分钟；已技术回放{sum(r['replayed_minutes'] for r in rows)}个；缺K线{sum(r['missing_bars'] for r in rows)}个；无执行记录{sum(r['missing_decisions'] for r in rows)}个。")
    lines.append(f"- 未对应有效买入信号的技术触发{sum(r['unconfirmed_trigger_minutes'] for r in rows)}次，需要结合模型否决、风控、计划状态和数据版本继续审计。")
    lines.append(f"- 原有效买入信号未复现{sum(r['recorded_signal_not_reproduced'] for r in rows)}次；市场环境未能核验{sum(r['context_unverified_minutes'] for r in rows)}个计划分钟。")
    lines.append(f"- 数据或时间预算不足的计划{sum(r['status'] == 'DATA_LIMITED' for r in rows)}个；完整证据保存在本次冻结事实中。")
    return lines + ['']
