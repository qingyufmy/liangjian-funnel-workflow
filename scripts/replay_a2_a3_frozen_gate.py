"""Read-only frozen gate audit; no models, registry writes or plan publication."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess

from liangjian_funnel.pipeline.deterministic import screen_a2
from liangjian_funnel.pipeline.factors import FactorEngine
from liangjian_funnel.pipeline.a3_strategy import evaluate_a3_candidate
from liangjian_funnel.pipeline.technical_aggregates import build_technical_aggregates
from liangjian_funnel.workflow import _compact_factor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', required=True)
    parser.add_argument('--research', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raw = Path(args.snapshot).read_bytes()
    frozen = json.loads(raw)
    snapshot = frozen['data']
    research = json.loads(Path(args.research).read_text(encoding='utf8'))
    a1 = next(row['output'] for row in research['stages'] if row['stage'] == 'A1')
    gate = screen_a2(snapshot, a1, review_all_eligible=True)
    candidates = [row for row in gate.decisions if row.get('sent_to_llm')]
    # Use only revisions already received by the frozen snapshot cutoff.
    # SQL connection is explicitly read-only; do not instantiate application stores.
    remote = '''import json,sqlite3
from datetime import datetime,timezone
symbols=SYMBOLS
cutoff=datetime.fromisoformat(CUTOFF).astimezone(timezone.utc).isoformat(timespec='microseconds')
c=sqlite3.connect('file:storage/facts/market_fact_cache.sqlite3?mode=ro',uri=True)
result={}
for symbol in symbols:
 rows=c.execute("WITH ranked AS (SELECT payload_json,bar_timestamp,ROW_NUMBER() OVER (PARTITION BY bar_timestamp ORDER BY fetched_at DESC,content_hash DESC) AS n FROM daily_bars WHERE symbol=? AND adjust='none' AND bar_timestamp<=? AND fetched_at<=?) SELECT payload_json FROM ranked WHERE n=1 ORDER BY bar_timestamp DESC LIMIT 800",(symbol,cutoff,cutoff)).fetchall()
 result[symbol]=[json.loads(r[0]) for r in reversed(rows)]
print(json.dumps(result))
'''.replace('SYMBOLS', repr([r['symbol'] for r in candidates])).replace('CUTOFF', repr(frozen['as_of']))
    process = subprocess.run(['ssh', 'aurum-vm', 'cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -'],
                             input=remote, text=True, capture_output=True, check=True, timeout=90)
    bars = json.loads(process.stdout)
    audit = []
    for row in candidates:
        symbol = row['symbol']
        factor = FactorEngine(symbol).compute(daily_bars=bars[symbol], as_of=datetime.fromisoformat(frozen['as_of']))
        aggregates = build_technical_aggregates(factor)
        compact = _compact_factor(factor.model_dump(mode='json'))
        decision = evaluate_a3_candidate(row, technical_context=compact,
            price_contract=aggregates['PRICE_LEVELS'],
            trading_eligibility=snapshot.get('TRADABILITY_FLAGS', {}).get(symbol),
            kline_labels=aggregates['KLINE_PATTERNS'], snapshot=snapshot, as_of=frozen['as_of'])
        audit.append({'symbol': symbol, 'a2_permission': row.get('execution_permission'),
                      'daily_bars': len(bars[symbol]), 'short_macd': compact['timeframes']['daily']['macd_short'],
                      'decision': decision})
    source = {r['symbol'] for r in a1['active_research_pool']}
    assert set(gate.review_symbols) <= source
    assert len(gate.review_symbols) == len(set(gate.review_symbols))
    result = {'snapshot_hash': frozen['snapshot_hash'], 'file_sha256': hashlib.sha256(raw).hexdigest(),
              'cutoff': frozen['as_of'], 'a2_summary': gate.summary, 'a3_local_decisions': audit,
              'limits': ['Reuses frozen A1 membership; not full-market A1 regeneration.',
                         'Deterministic gate replay only, not LLM review or formal plan publication.',
                         'Historical source gaps remain; no new market collection.']}
    Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf8')
    print(json.dumps({'a2_candidates':len(candidates), 'output':args.output,
                      'sample_decisions':[{'symbol':r['symbol'],
                          **{k:r['decision'].get(k) for k in ('eligibility','route_permission','reason_codes')}}
                          for r in audit if r['symbol'] in ('301071.SZ','688222.SH')]}))


if __name__ == '__main__':
    main()
