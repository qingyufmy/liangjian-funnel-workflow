"""Offline field sensitivity, not an arrival-time replay or proof of missed fills."""
import argparse
from collections import Counter
from copy import deepcopy
from datetime import datetime
import hashlib
import inspect
import json
from pathlib import Path

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.strategies import evaluate_strategy
from liangjian_funnel.workflow import _a4_execution_cutoff, _intraday_market_context


def replace_field(rows, sample, field):
    rows = deepcopy(rows)
    matches = [row for row in rows if row['bar_end'] == sample['bar_end']]
    if len(matches) != 1 or matches[0][field.lower()] != sample['left']:
        raise ValueError('FROZEN_LEFT_VALUE_NOT_MATCHED')
    matches[0][field.lower()] = sample['right']
    # Tencent's amount is an OHLC estimate, not independent turnover evidence.
    # Keep its documented formula coherent with the modified OHLC/volume.
    row = matches[0]
    if row.get('amount_kind') == 'ohlc_estimate':
        row['amount'] = sum(row[k] for k in ('open', 'high', 'low', 'close')) / 4 * row['volume']
    MinuteBar.model_validate(row)
    return rows


def audit(facts):
    replay = facts['a3_plan_replay']
    market = {v['cache_bucket']: v for v in replay['archived_market_states'].values()}
    output = []
    for summary in replay['plans']:
        pid, symbol = summary['plan_id'], summary['symbol']
        plan = replay['frozen_plans'][pid]
        frozen = replay['archived_inputs'][symbol]
        checks = next(p for p in facts['independent_verification']['a4']['plans'] if p['plan_id'] == pid)
        events = [e for e in facts['a4']['events'] if e.get('plan_id') == pid]
        if any(e.get('effective') and e.get('action') in ('BUY_SIGNAL', 'PLAN_INVALIDATED') for e in events):
            raise ValueError('LIFECYCLE_REPLAY_NOT_SUPPORTED')
        if len({e['minute_end'] for e in events}) != len(events):
            raise ValueError('DUPLICATE_DECISION_WINDOW')
        five = tuple(MinuteBar.model_validate(b) for b in frozen['five_minute'])

        def run(rows):
            one = tuple(MinuteBar.model_validate(b) for b in rows)
            results = {}
            for event in events:
                t = datetime.fromisoformat(event['minute_end'])
                cutoff = _a4_execution_cutoff(t)
                if cutoff is None:
                    continue
                history = tuple(b for b in one if b.bar_end <= cutoff)
                if not history or history[-1].bar_end != cutoff:
                    raise ValueError('MISSING_FROZEN_MINUTE')
                bucket = t.replace(minute=t.minute // 5 * 5, second=0, microsecond=0).isoformat()
                if bucket not in market:
                    raise ValueError('MISSING_FROZEN_MARKET')
                context = _intraday_market_context(symbol, history, tuple(b for b in five if b.bar_end <= cutoff),
                                                  current=t, execution_cutoff=cutoff, live_market_state=market[bucket])
                result = evaluate_strategy(plan, history, now=cutoff, decision_time=t, market_context=context)
                results[t.isoformat()] = result.model_dump(mode='json')
            return results

        baseline = run(frozen['one_minute'])
        baseline_reasons = Counter(r for v in baseline.values() for r in v['reason_codes'])
        if len(baseline) != summary['replayed_minutes'] or dict(baseline_reasons) != summary['reasons']:
            raise ValueError('BASELINE_DOES_NOT_REPRODUCE_ARCHIVED_REPLAY')
        variants = []
        combined = frozen['one_minute']
        for field, check in checks['archived_tdx_field_checks'].items():
            if check['mismatch_count'] != len(check['mismatch_samples']):
                raise ValueError('TRUNCATED_MISMATCH_SAMPLES')
            for sample in check['mismatch_samples']:
                variants.append((field + ':' + sample['bar_end'], replace_field(frozen['one_minute'], sample, field)))
                combined = replace_field(combined, sample, field)
        variants.append(('COMBINED', combined))
        scenarios = []
        for name, rows in variants:
            alternate = run(rows)
            differences = [{'minute_end': t, 'before_action': baseline[t]['action'], 'after_action': v['action'],
                            'before_reasons': baseline[t]['reason_codes'], 'after_reasons': v['reason_codes']}
                           for t, v in alternate.items() if (v['action'], v['reason_codes']) != (baseline[t]['action'], baseline[t]['reason_codes'])]
            scenarios.append({'scenario': name, 'buy_count': sum(v['action'] == 'BUY_SIGNAL' for v in alternate.values()),
                              'action_changes': sum(d['before_action'] != d['after_action'] for d in differences),
                              'differences': differences})
        output.append({'plan_id': pid, 'baseline_minutes': len(baseline), 'baseline_reasons_match': True, 'scenarios': scenarios})
    return {'mode': 'EX_POST_FIELD_SENSITIVITY_NOT_EXECUTION_REPLAY', 'plans': output,
            'strategy_sha256': hashlib.sha256(Path(inspect.getfile(evaluate_strategy)).read_bytes()).hexdigest(),
            'limitations': ['No historical arrival-time/quote/account/model reconstruction.',
                            'Alternate source amount unavailable; OHLC-estimated amount remains an estimate.',
                            'No source correctness verdict; no production mutation or notifications.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--facts', type=Path, required=True)
    args = parser.parse_args()
    raw = args.facts.read_bytes()
    result = audit(json.loads(raw))
    result['facts_sha256'] = hashlib.sha256(raw).hexdigest()
    print(json.dumps(result, ensure_ascii=False, indent=2))
