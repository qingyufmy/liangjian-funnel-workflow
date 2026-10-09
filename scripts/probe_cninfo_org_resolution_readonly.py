"""Bounded public CNINFO comparison; local evidence only, no DB/model/order."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

import httpx
from liangjian_funnel.data.cninfo import CninfoClient


def run(warm: bool, symbols: list[str], start: str, end: str):
    calls = []
    def on_request(request):
        calls.append({'method': request.method, 'path': request.url.path})
    with httpx.Client(timeout=12, trust_env=False,
                      event_hooks={'request': [on_request]}) as transport:
        with CninfoClient(http_client=transport, min_request_interval_seconds=.5) as c:
            receipt = c.warm_org_catalog(symbols) if warm else None
            results = []
            for symbol in symbols:
                result = c.fetch_announcements(symbol, start, end, max_pages=4)
                facts = sorted([a.model_dump(mode='json') for a in result.announcements],
                               key=lambda a: a['announcement_id'])
                fingerprint = hashlib.sha256(json.dumps(facts, ensure_ascii=False,
                    sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                results.append({'symbol': symbol, 'ok': result.ok,
                    'complete': result.complete, 'reason': result.reason_code,
                    'total': result.total, 'facts_hash': fingerprint,
                    'org_id_source': result.metadata.get('org_id_source'),
                    'org_id_catalog': result.metadata.get('org_id_catalog')})
    return {'requests': calls, 'request_count': len(calls),
            'catalog': receipt, 'results': results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--vm-catalog-scope', action='store_true',
                        help='read active A1 scope over SSH and perform one public identifier GET only')
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_OVERWRITE')
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    if args.vm_catalog_scope:
        remote = """
import json,sqlite3,pathlib
from liangjian_funnel.workflow import _active_a1_downstream_scope
root=pathlib.Path.cwd()
with sqlite3.connect((root/'state/a1_registry.sqlite3').resolve().as_uri()+'?mode=ro',uri=True) as c:
    c.row_factory=sqlite3.Row
    row=c.execute('select * from a1_generations where generation_id=(select generation_id from a1_active_pointer where pointer_name=?)',('A1',)).fetchone()
    print(json.dumps({'generation':row['generation_id'],'symbols':_active_a1_downstream_scope(json.loads(row['payload_json']))}))
"""
        result = subprocess.run(['ssh', 'aurum-vm',
            'cd /www/wwwroot/Agu/liangjian-funnel-workflow && runuser -u www -- .venv/bin/python -B -'],
            input=remote, text=True, capture_output=True, timeout=15, check=True)
        scope = json.loads(result.stdout)
        with CninfoClient(timeout_seconds=12) as client:
            receipt = client.warm_org_catalog(scope['symbols'])
        out = {'captured_at': now.isoformat(), 'mode': 'READ_ONLY_A1_SCOPE_ONE_PUBLIC_GET',
               'generation': scope['generation'], 'scope_symbols': len(scope['symbols']),
               'catalog': receipt}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('x', encoding='utf-8') as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(json.dumps({k: receipt.get(k) for k in
            ['status','matched_symbols','requested_symbols','catalog_rows','reason_code']}))
        return 0 if receipt['status'] == 'READY' else 2
    end, start = now.date().isoformat(), (now.date()-timedelta(days=1)).isoformat()
    symbols = ['600026.SH', '603906.SH', '300308.SZ']
    before, after = run(False, symbols, start, end), run(True, symbols, start, end)
    equivalent = all(a['ok'] and a['complete'] and b['ok'] and b['complete']
        and (a['symbol'], a['total'], a['facts_hash']) ==
            (b['symbol'], b['total'], b['facts_hash'])
        for a, b in zip(before['results'], after['results']))
    out = {'captured_at': now.isoformat(), 'mode': 'READ_ONLY_PUBLIC_NO_MODEL_NO_DB',
           'window': [start, end], 'before': before, 'after': after,
           'equivalent': equivalent,
           'request_reduction': before['request_count']-after['request_count']}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(json.dumps({'output': str(args.output), 'equivalent': equivalent,
                     'before_requests': before['request_count'],
                     'after_requests': after['request_count']}, ensure_ascii=False))
    return 0 if equivalent and (after['catalog'] or {}).get('status') == 'READY' else 2


if __name__ == '__main__':
    raise SystemExit(main())
