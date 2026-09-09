"""Independently recompute published daily indicators using read-only fact revisions."""
import argparse
from collections import Counter
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3

from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.settings import Settings


class ReadOnlyFacts(LocalFactCache):
    def __init__(self, path):
        self.path = path

    def _connect(self):
        db = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
        return db


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--as-of', required=True, help='Frozen A3 technical knowledge cutoff, with timezone')
    args = parser.parse_args()
    cutoff = datetime.fromisoformat(args.as_of)
    if cutoff.tzinfo is None:
        parser.error('--as-of must have a timezone')
    settings = Settings.from_env(root=Path.cwd())
    facts = ReadOnlyFacts(settings.fact_cache_db_path)
    with sqlite3.connect(settings.state_db_path.as_uri() + '?mode=ro', uri=True) as db:
        plans = [(symbol, json.loads(raw)) for symbol, raw in db.execute(
            'select symbol,payload_json from execution_plans')]
    plans = [(s, p) for s, p in plans if p.get('source_run_id') == args.run_id]
    checks = []
    for symbol, plan in plans:
        bars = facts.query_daily_bars(symbol, adjust='none', end=cutoff + timedelta(days=1),
                                     as_of=cutoff, limit=800, descending=True)
        closes = [float(r['payload']['close_price']) for r in reversed(bars)
                  if datetime.fromisoformat(r['timestamp']).astimezone(cutoff.tzinfo).date() <= cutoff.date()]
        strategy = plan.get('strategy_facts') or {}
        declared_ma = strategy.get('daily_moving_averages') or {}
        computed_ma = {f'ma{n}': sum(closes[-n:]) / n for n in (5, 20, 60) if len(closes) >= n}
        ma_ok = len(computed_ma) == 3 and all(isinstance(declared_ma.get(k), (float, int))
            and abs(declared_ma[k] - v) <= 0.000002 for k, v in computed_ma.items())
        declared_macd = plan.get('daily_macd') or strategy.get('daily_macd_evidence') or {}
        macd_ok = False
        if len(closes) >= 35:
            fast = slow = closes[0]
            dea = 0.0
            for close in closes[1:]:
                fast += (close - fast) * (2 / 13)
                slow += (close - slow) * (2 / 27)
                dif = fast - slow
                dea += (dif - dea) * (2 / 10)
            values = {'dif': dif, 'dea': dea, 'hist': 2 * (dif - dea)}
            macd_ok = all(isinstance(declared_macd.get(k), (float, int))
                and abs(declared_macd[k] - v) <= 0.000002 for k, v in values.items())
        input_hash = hashlib.sha256(json.dumps(closes, separators=(',', ':')).encode()).hexdigest()
        scenario_values = (plan.get('scenarios') or {}).values()
        checks.append({'symbol': symbol, 'bars': len(closes), 'ma5_20_60_match': ma_ok,
            'macd_match': macd_ok, 'macd_input_hash_match': (strategy.get('daily_macd_evidence') or declared_macd).get('input_hash') == input_hash,
            'four_server_scenarios': len(plan.get('scenarios') or {}) == 4 and all(
                isinstance(v, dict) and v.get('source') == 'DETERMINISTIC_A3_CONTRACT' for v in scenario_values)})
    keys = ('ma5_20_60_match', 'macd_match', 'macd_input_hash_match', 'four_server_scenarios')
    assertions = {key: bool(checks) and all(r[key] for r in checks) for key in keys}
    print(json.dumps({'run_id': args.run_id, 'as_of': args.as_of, 'plan_count': len(plans),
        'strategies': dict(Counter(p.get('strategy_profile') for _, p in plans)),
        'assertions': assertions, 'checks': checks}, ensure_ascii=False))
    if not all(assertions.values()):
        raise SystemExit(2)


if __name__ == '__main__':
    main()
