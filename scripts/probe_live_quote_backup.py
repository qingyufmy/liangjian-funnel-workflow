"""Explicit quote-only shadow probe; never dispatches A4, orders or notices."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess

from liangjian_funnel.data.live_quote import normalize_sina_quote, validate_quote
from liangjian_funnel.data.mootdx import map_symbol
from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter, QuoteResult
from liangjian_funnel.reporting import atomic_write_json

REMOTE = r'''
import requests,json,hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
rows=[]
for source,url,referer in URLS:
 started=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
 row={'source':source,'request_started_at':started}
 try:
  response=requests.get(url,timeout=(2.5,2.5),headers={'Referer':referer,'User-Agent':'Mozilla/5.0'})
  response.raise_for_status();response.encoding='gbk';raw=response.text
  row.update(transport='UP',raw=raw,raw_sha256=hashlib.sha256(raw.encode()).hexdigest())
 except Exception as exc:row.update(transport='DOWN',error_type=type(exc).__name__)
 row['response_received_at']=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat();rows.append(row)
print(json.dumps(rows,ensure_ascii=False))
'''


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--symbol', required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    if a.output.exists():
        raise SystemExit('REFUSE_OVERWRITE')
    mapped = map_symbol(a.symbol)
    provider = mapped.exchange.lower() + mapped.code
    urls = [('TENCENT', 'https://qt.gtimg.cn/q?q='+provider, 'https://gu.qq.com/'),
            ('SINA', 'https://hq.sinajs.cn/list='+provider, 'https://finance.sina.com.cn/')]
    r = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8','aurum-vm',
                        'cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -'],
                       input=('URLS='+repr(urls)+'\n'+REMOTE).encode(), capture_output=True, timeout=25)
    if r.returncode:
        raise SystemExit('SSH_PROBE_FAILED')
    rows = json.loads(r.stdout)
    for row in rows:
        if row['transport'] != 'UP':
            continue
        try:
            quote = normalize_sina_quote(row['raw'], mapped.canonical) if row['source']=='SINA' else TencentIntradayAdapter._quote(row['raw'], mapped.canonical)
            result = validate_quote(QuoteResult(symbol=mapped.canonical, quote=quote, reason_code='OK', complete=True),
                                    mapped.canonical, as_of=datetime.fromisoformat(row['response_received_at']))
            row.update(normalized_quote=quote.model_dump(mode='json'), current_quote_valid=result.complete,
                       quote_reason=result.reason_code)
        except ValueError:
            row.update(current_quote_valid=False, quote_reason='RESPONSE_SCHEMA_INVALID')
    out={'symbol':mapped.canonical,'shadow_only':True,'execution_authority':False,
         'scope':'ONE_PUBLIC_SNAPSHOT_PER_SOURCE_NOT_INTRADAY_AVAILABILITY_SLA','observations':rows}
    atomic_write_json(a.output,out)
    print(json.dumps({**out,'observations':[{k:v for k,v in row.items() if k!='raw'} for row in rows]},ensure_ascii=False))


if __name__ == '__main__':
    main()
