"""08:00 once-only independent minute-source probes; never writes market bars."""
from datetime import datetime, time
import json
import os

from .calendar import ExchangeTradingCalendar
from .shared_json import atomic_json


def run_market_source_probes(settings, *, now, root, adapters=None):
    from ..data.mootdx import MootdxAdapter, MootdxNode
    from ..data.tencent_minute import TencentIntradayAdapter
    from .provider_health import queue_internal

    prior = ExchangeTradingCalendar().previous_trading_day(now.date())
    cutoff = datetime.combine(prior, time(15), tzinfo=now.tzinfo)
    if adapters is None:
        # One configured TDX node, one bars page. No node rotation or retry.
        # Tencent's normal parser is reused; redirects must not turn one probe
        # into more than one HTTP dispatch.
        def tencent_once(url, params, timeout):
            import requests
            response = requests.get(url, params=dict(params), timeout=(5, timeout),
                allow_redirects=False, headers={'User-Agent': 'LiangjianResearch/2.0'})
            if response.status_code != 200:
                raise ValueError('MARKET_PROBE_HTTP_NON_200')
            return response.json()
        adapters = {
            'MOOTDX': MootdxAdapter(tuple(MootdxNode(host=h, port=p)
                for h, p in settings.mootdx_servers[:1]), page_size=1,
                max_pages=1, timeout_seconds=5),
            'TENCENT': TencentIntradayAdapter(json_fetcher=tencent_once, timeout_seconds=5),
        }
    if set(adapters) != {'MOOTDX', 'TENCENT'}:
        raise ValueError('MARKET_PROBE_BOTH_INDEPENDENT_SOURCES_REQUIRED')
    rows = []
    for name in ('MOOTDX', 'TENCENT'):
        pin = root / ('market-' + name.lower())
        reused = False
        try:
            with pin.with_suffix('.attempt').open('x') as stream:
                stream.write(now.isoformat()); stream.flush(); os.fsync(stream.fileno())
        except FileExistsError:
            reused = True
        if reused:
            row = (json.loads(pin.with_suffix('.json').read_bytes())
                if pin.with_suffix('.json').exists()
                else dict(source=name, status='UNKNOWN_INTERRUPTED'))
        else:
            try:
                result = adapters[name].fetch_bars('000001.SZ', '5m', 1, as_of=cutoff)
                valid = (result.complete and len(result.bars) == 1
                    and result.bars[0].symbol == '000001.SZ'
                    and result.bars[0].interval == '5m'
                    and result.bars[0].bar_end == cutoff
                    and result.bars[0].evidence_kind == 'MARKET_BAR'
                    and result.bars[0].source_id.startswith(name + ':'))
                row = dict(source=name, status='PASS' if valid else 'FAILED',
                    reason_code='OK' if valid else (result.reason_code if not result.complete
                        else 'MARKET_PROBE_WINDOW_UNPROVEN'),
                    expected_bar_end=cutoff.isoformat(),
                    actual_bar_end=result.bars[-1].bar_end.isoformat() if result.bars else None,
                    attempts=[a.model_dump(mode='json') for a in result.attempts],
                    returned_bars=len(result.bars), market_cache_writes=0)
            except Exception as exc:
                row = dict(source=name, status='FAILED',
                    reason_code='MARKET_SOURCE_PROBE_FAILED', error_type=type(exc).__name__)
            atomic_json(pin.with_suffix('.json'), row)
        if row['status'] != 'PASS':
            queue_internal(settings, kind='MARKET_SOURCE_UNAVAILABLE',
                source_id='market-probe:' + now.date().isoformat() + ':' + name,
                text='08:00行情源探针失败：' + name + '；上一交易日闭合5分钟证据未通过，请盘前处理。', now=now)
        rows.append(dict(row, reused=reused))
    return rows
