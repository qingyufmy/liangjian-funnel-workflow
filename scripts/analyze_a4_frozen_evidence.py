"""Pure strategy comparisons of selected frozen windows, no broker/model/production writes."""
import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.strategies import evaluate_strategy, aggregate_closed_bars
from liangjian_funnel.workflow import _intraday_market_context
from liangjian_funnel.reporting import atomic_write_json


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path);a=p.parse_args();root=a.root
    output=a.output or root/'selected-window-comparison.json'
    if output.exists():raise SystemExit('REFUSE_OVERWRITE')
    read=lambda name:json.loads((root/name).read_text(encoding='utf-8'))
    c=read('capture-1533.json');d=read('decision-evidence.json');markets=sorted(read('market-snapshots.json').values(),key=lambda x:x['as_of'])
    plans={r['symbol']:json.loads(r['payload_json']) for r in c['plans']}
    windows={(w['symbol'],w['decision_as_of'],w['interval']):w for w in d['windows']}
    quotes={(q['symbol'],q['source'],q['interval']):q['bars'] for q in d['quotes']}
    events={(json.loads(e['payload_json']).get('symbol'),e['minute_end']):e for e in c['events']}
    results=[]
    for (symbol,stamp,interval),window in windows.items():
        if interval!='1m' or (symbol,stamp) not in events:continue
        event=events[symbol,stamp];stored=json.loads(event['payload_json']).get('strategy')
        if not stored:continue
        now=datetime.fromisoformat(stamp);market=next((m for m in reversed(markets) if m['as_of']<=stamp),None)
        position=next((r for r in c['positions'] if r['symbol']==symbol and r['updated_at']<=stamp),None)
        for source in ['recorded','tencent','tdx']:
            one=window['bars'] if source=='recorded' else [b for b in quotes[symbol,source,'1m'] if b['bar_end']<=stamp]
            five=windows[symbol,stamp,'5m']['bars'] if source=='recorded' else [b for b in quotes[symbol,source,'5m'] if b['bar_end']<=stamp]
            if not one or not five:
                results.append({'symbol':symbol,'time':stamp,'source':source,'unavailable':True});continue
            context=_intraday_market_context(symbol,tuple(MinuteBar.model_validate(b) for b in one),tuple(MinuteBar.model_validate(b) for b in five),current=now,live_market_state=market)
            value=evaluate_strategy(plans[symbol],one,now=now,position=position,market_context=context).model_dump(mode='json')
            fields=['action','state','reason_codes','met_conditions','unmet_conditions','veto_conditions']
            diff={f:{'recorded':stored.get(f),'recomputed':value.get(f)} for f in fields if stored.get(f)!=value.get(f)}
            results.append({'symbol':symbol,'time':stamp,'source':source,'production_action':event['action'],'action':value['action'],'state':value['state'],'reason_codes':value['reason_codes'],'differences':diff,'indicator_observations':value.get('indicator_observations'),
                'closed_tail':{k:list(v[-3:]) for k,v in aggregate_closed_bars(one,as_of=now).items()} if event['effective'] else {}})
    result={'boundary':'Selected-window deterministic comparison only; final-source replacement is hindsight sensitivity, not contemporaneous trade proof. No model or settlement replay.','capture_sha256':hashlib.sha256((root/'capture-1533.json').read_bytes()).hexdigest(),'results':results}
    atomic_write_json(output,result)
    print(json.dumps({'path':str(output),'rows':len(results),'differences':dict(Counter(r['source'] for r in results if r.get('differences'))),'unavailable':sum(r.get('unavailable',False) for r in results),'effective_samples':[{'symbol':r['symbol'],'time':r['time'],'source':r['source'],'action':r.get('action'),'differences':r.get('differences')} for r in results if r.get('production_action') in ['BUY_SIGNAL','SELL_SIGNAL','REDUCE_SIGNAL','PLAN_INVALIDATED']]},ensure_ascii=False))


if __name__=='__main__':main()
