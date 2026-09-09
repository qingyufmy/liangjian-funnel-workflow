"""Capture a session read-only, or render captured signal evidence locally.

--capture-only uses three SQLite mode=ro connections, no RuntimeStore init,
provider fetch, model call, notification, scheduling or execution.
"""
import argparse
import hashlib
import json
import sqlite3
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")


def capture(cutoff):
    from liangjian_funnel.settings import Settings
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    from liangjian_funnel.pipeline.local_fact_cache import _timestamp
    s = Settings.from_env(root=Path.cwd())
    def connect(path):
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        return db
    start = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
    with connect(s.state_db_path) as db:
        events = [dict(r) for r in db.execute("SELECT * FROM monitor_events WHERE lane_id='lane_1' AND minute_end>=? AND minute_end<=? AND effective=1 ORDER BY minute_end,event_id", (start.isoformat(), cutoff.isoformat()))]
        plan_ids = {json.loads(e['payload_json']).get('plan_id') for e in events}
        plans = [dict(r) for pid in sorted(p for p in plan_ids if p) for r in db.execute("SELECT * FROM execution_plans WHERE plan_id=?", (pid,))]
        fills = [dict(r) for r in db.execute("SELECT * FROM virtual_fills WHERE bar_end>=? AND bar_end<=?", (start.isoformat(), cutoff.isoformat()))]
        lifecycles = [dict(r) for r in db.execute("SELECT entry_event_key,status,exit_reason,updated_at FROM a4_signal_lifecycles WHERE trade_date=? AND lane_id='lane_1'", (cutoff.date().isoformat(),))]
    symbols = {json.loads(e['payload_json']).get('symbol') for e in events}
    market = {}
    prior = ExchangeTradingCalendar().previous_trading_day(cutoff.date())
    prior_start = datetime.combine(prior, datetime.min.time(), TZ)
    with connect(s.minute_cache_dir / 'minute_bars.sqlite3') as minutes, connect(s.fact_cache_db_path) as daily:
        for symbol in sorted(x for x in symbols if x):
            row = minutes.execute("SELECT snapshot_id,payload_sha256,payload_zlib FROM minute_decision_snapshots WHERE symbol=? AND interval='1m' AND decision_as_of>=? AND decision_as_of<=? ORDER BY decision_as_of DESC LIMIT 1", (symbol, start.isoformat(), cutoff.isoformat())).fetchone()
            bars = []
            if row:
                raw = zlib.decompress(row['payload_zlib'])
                assert hashlib.sha256(raw).hexdigest() == row['payload_sha256']
                bars = json.loads(raw)
            previous = daily.execute("SELECT payload_json FROM daily_bars WHERE symbol=? AND adjust='none' AND bar_timestamp>=? AND bar_timestamp<? AND fetched_at<=? ORDER BY bar_timestamp DESC,fetched_at DESC,content_hash DESC LIMIT 1", (symbol, _timestamp(prior_start), _timestamp(prior_start + timedelta(days=1)), _timestamp(cutoff))).fetchone()
            raw_daily = json.loads(previous[0]) if previous else {}
            previous_close = raw_daily.get('close') or raw_daily.get('close_value') or raw_daily.get('close_price') or (raw_daily.get('payload') or {}).get('close')
            expected = sum(1 for h, m in [(9 + (31+i)//60, (31+i)%60) for i in range(120)] + [(13 + (1+i)//60, (1+i)%60) for i in range(120)] if start.replace(hour=h, minute=m) <= cutoff)
            market[symbol] = {'bars': bars, 'previous_close': previous_close,
                              'source': row['snapshot_id'] if row else 'UNAVAILABLE', 'expected_minutes': expected}
    return {'cutoff_at': cutoff.isoformat(), 'captured_at': datetime.now(TZ).isoformat(),
            'events': events, 'plans': plans, 'fills': fills, 'market': market, 'lifecycles': lifecycles,
            'production_mutation': False}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cutoff')
    p.add_argument('--capture-only', action='store_true')
    p.add_argument('--capture', type=Path)
    p.add_argument('--output', required=True, type=Path)
    args = p.parse_args()
    if args.output.exists():
        p.error('Refusing to overwrite evidence')
    if args.capture_only:
        cutoff = datetime.fromisoformat(args.cutoff)
        if cutoff.tzinfo is None or cutoff > datetime.now(TZ):
            p.error('Cutoff must be timezone-aware and not in the future')
        result = capture(cutoff)
    else:
        from liangjian_funnel.review.signal_audit import build_signal_stock_reviews
        data = json.loads(args.capture.read_text(encoding='utf-8'))
        rows = build_signal_stock_reviews(data['events'], data['plans'], data['fills'], data['market'], datetime.fromisoformat(data['cutoff_at']), lifecycles=data.get('lifecycles', []))
        result = {'cutoff_at': data['cutoff_at'], 'production_mutation': False, 'signal_stock_reviews': rows}
        md = [f"# {data['cutoff_at'][:10]}信号股票：当日表现与入场审计", '', f"事实截止：{data['cutoff_at']}。本地只读审计，不是收盘最终表现，不改生产记录。", '',
              '| 股票 | 当日表现 | 入场审计 |', '|---|---|---|']
        for r in rows:
            md.append(f"| {r['name']} {r['symbol']} | {r['performance_summary']} | {r['entry_audit_summary']} |")
        md += ['', '价格表现未扣费、不是已实现收益。证据齐全只代表冻结记录字段核对，不代表策略重算或买入合理性已独立证明。完整快照编号、成交编号、盈亏比及量化条件见同名JSON。']
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.with_suffix('.md').write_text('\n'.join(md) + '\n', encoding='utf-8')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'output': str(args.output), 'events': len(result.get('events', result.get('signal_stock_reviews', [])))}))
