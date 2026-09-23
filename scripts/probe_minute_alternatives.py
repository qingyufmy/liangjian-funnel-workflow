"""Read-only VM HTTP probes; freeze evidence locally, never change A4 or A5."""
import hashlib
import json
from datetime import datetime
from pathlib import Path
import subprocess

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.data.sina_minute_shadow import normalize_sina_5m, compare_five_minute_bars
from liangjian_funnel.facts.store import FactStore

REMOTE = r'''
import json,httpx
from datetime import datetime
from zoneinfo import ZoneInfo
from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter
tz=ZoneInfo('Asia/Shanghai'); out=[]
with httpx.Client(timeout=8) as c:
 for symbol in ['600519.SH','000001.SZ','688111.SH']:
  start=datetime.now(tz);code,market=symbol.split('.')
  response=c.get('https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData',
      params={'symbol':market.lower()+code,'scale':5,'ma':'no','datalen':100})
  response.raise_for_status();raw=response.json();received=datetime.now(tz)
  tencent=TencentIntradayAdapter(timeout_seconds=8).fetch_bars(symbol,'5m',48,as_of=received)
  out.append({'symbol':symbol,'sina_raw':raw,'started':start.isoformat(),'received':received.isoformat(),
              'tencent_received':datetime.now(tz).isoformat(),'tencent':tencent.model_dump(mode='json')})
print(json.dumps(out,ensure_ascii=True))
'''


def main():
    reply = subprocess.run(['ssh', 'aurum-vm',
        'cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -'],
        input=REMOTE, capture_output=True, text=True, check=True, timeout=60)
    raw = json.loads(reply.stdout)
    digest = hashlib.sha256(reply.stdout.encode()).hexdigest()
    root = Path(__file__).resolve().parents[1] / 'artifacts/minute-alternatives'
    store = FactStore(root)
    store.write_json(root / f'raw-{digest[:16]}.json', raw)
    reports = []
    for item in raw:
        as_of = datetime.fromisoformat(item['received'])
        a = [bar for bar in normalize_sina_5m(item['sina_raw'], symbol=item['symbol'], as_of=as_of)
             if bar.bar_end.date() == as_of.date()]
        b = [MinuteBar.model_validate(bar) for bar in item['tencent']['bars']
             if datetime.fromisoformat(bar['bar_end']).date() == as_of.date()]
        reports.append({'symbol': item['symbol'], 'received_at': item['received'],
                        'raw_evidence_sha256': digest,
                        'hash_scope': 'SSH_STDOUT_UTF8_BEFORE_JSON_RESERIALIZATION',
                        **compare_five_minute_bars(a, b)})
    store.write_json(root / f'comparison-{digest[:16]}.json', reports)
    print(json.dumps({'artifact':str(root / f'comparison-{digest[:16]}.json'), 'results':[
        {k:r[k] for k in ('symbol','common_count','different_bar_count','independent_confirmation')}
        for r in reports]}, ensure_ascii=False))


if __name__ == '__main__':
    main()
