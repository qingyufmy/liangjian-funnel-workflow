"""Read-only VM release/preopen evidence; output is local and never overwritten."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

REMOTE = r'''
import collections, hashlib, importlib, json, pathlib, sqlite3, subprocess
from datetime import datetime
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter
root = pathlib.Path.cwd()
s = Settings.from_env(root=root)
now = datetime.now(ZoneInfo('Asia/Shanghai'))
day = now.date().isoformat()
def rows(db, query, args=()):
    return [dict(r) for r in db.execute(query, args)]
def readonly(path):
    db = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    return db
out = {'captured_at':now.isoformat(), 'root':str(root), 'host':subprocess.check_output(['hostname'],text=True).strip(),
       'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
       'mode':'READ_ONLY_NO_MODEL_NO_PRODUCTION_WRITE',
       'settings':{'rotation_source':s.rotation_membership_source,'a4_quote_backup_mode':s.a4_quote_backup_mode,
                   'research_models':s.research_models,'monitor_model':s.monitor_model,'review_model':s.review_model,
                   'lark_config_present':s.lark_webhook_path.is_file()}}
out['modules'] = []
for name in ['data.board_reference','data.hithink_board_reference','data.rotation_theme','pipeline.data_source',
             'runtime.auction_base','settings','workflow','data.publication','data.live_fetch',
             'data.tencent_minute','runtime.monitor','pipeline.a2_role_logic', 'pipeline.a3_strategy',
             'pipeline.research', 'pipeline.research.common', 'pipeline.research.a1',
             'pipeline.research.a2', 'pipeline.research.a3', 'cli']:
    try:
        m = importlib.import_module('liangjian_funnel.'+name)
    except ModuleNotFoundError:
        continue
    p = pathlib.Path(m.__file__)
    src = root/'src/liangjian_funnel'/pathlib.Path(*name.split('.')).with_suffix('.py')
    if not src.is_file():
        src = src.with_suffix('')/'__init__.py'
    out['modules'].append({'module':name,'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
                           'matches_src':src.is_file() and p.read_bytes()==src.read_bytes()})
with readonly(s.state_db_path) as c:
    out['today_plans'] = rows(c,'select plan_id,symbol,status,valid_from,expires_at,created_at from execution_plans where expires_at>=? and expires_at<?',(day,day+'T23:59:59'))
    out['positions'] = rows(c,'select symbol,total_qty,sellable_qty from virtual_positions where total_qty>0')
    out['leases'] = rows(c,'select * from scheduler_leases')
    out['current_unexpired_leases'] = [r for r in out['leases'] if r['state']=='ACTIVE' and r['expires_at']>now.isoformat()]
    out['recent_workflows'] = []
    for r in rows(c,'select run_id,trade_date,slot,status,reason_codes_json,updated_at,outcome_json from workflow_runs order by updated_at desc limit 4'):
        o = json.loads(r.pop('outcome_json'))
        r['outcome'] = {k:o.get(k) for k in ['job_status','data_sufficiency_state','actionability_state','counts']}
        r['stages'] = [{k:t.get(k) for k in ['stage','counts','data_sufficiency_state','reason_codes']} for t in o.get('stages',[])]
        out['recent_workflows'].append(r)
    out['today_notifications'] = rows(c,'select kind,status,last_reason_code,sent_at,title from notification_deliveries where created_at>=? order by created_at',(day,))
    out['recent_notification_summary'] = rows(c,'select kind,status,count(*) n,max(sent_at) last_sent from notification_deliveries where created_at>=? group by kind,status',('2026-10-08',))
    out['today_a4_counts'] = rows(c,'select action,reason_code,count(*) n,min(minute_end) first,max(minute_end) last from monitor_events where minute_end>=? group by action,reason_code',(day,))
with readonly(root/'state/a1_registry.sqlite3') as c:
    ptr = rows(c,"select * from a1_active_pointer where pointer_name='A1'")
    out['a1_pointer'] = ptr[0] if ptr else None
    if ptr:
        r = rows(c,'select * from a1_generations where generation_id=?',(ptr[0]['generation_id'],))[0]
        out['a1_generation'] = {k:v for k,v in r.items() if not k.endswith('_json')}
        manifest = json.loads(r.get('manifest_json','{}'))
        out['a1_manifest'] = {k:manifest.get(k) for k in ['schema_version','mode','as_of','g0_count','candidate_record_count','lane_ids','last_full_period','maintenance_week']}
        out['a1_partition_scope_counts'] = {lane:len(symbols) for lane,symbols in manifest.get('partition_symbols_by_lane',{}).items()}
marker = root/'outputs/auction_base'/f'{day}.json'
out['auction_base'] = json.loads(marker.read_text()) if marker.is_file() else None
out['node_business_events'] = []
log = root/'outputs/node'/f'node-{day}.jsonl'
if log.is_file():
    for line in log.read_text().splitlines():
        try: r=json.loads(line)
        except ValueError: continue
        msg=str(r.get('message',''))
        if r.get('stream')=='node' and (r.get('job') is not None or '启动' in msg):
            out['node_business_events'].append({k:r.get(k) for k in ['timestamp','job','level','runId','message']})
out['node_business_events'] = out['node_business_events'][-40:]
out['node_processes'] = []
for entry in pathlib.Path('/proc').iterdir():
    if not entry.name.isdigit(): continue
    try:
        argv=(entry/'cmdline').read_bytes().split(b'\0')
        if b'dist/server/index.js' not in argv: continue
        env=dict(v.split(b'=',1) for v in (entry/'environ').read_bytes().split(b'\0') if b'=' in v)
        out['node_processes'].append({'pid':int(entry.name),'scheduler_enabled':env.get(b'LIANGJIAN_SCHEDULER_ENABLED',b'true').decode(),
                                     'working_directory':str((entry/'cwd').resolve())})
    except (OSError,ValueError): pass
provider = TencentIntradayAdapter(timeout_seconds=3)
out['market_transport_probes'] = []
for symbol in ['600026.SH','603906.SH','600519.SH']:
    q=provider.fetch_quote(symbol,as_of=now)
    b=provider.fetch_bars(symbol,'1m',12,as_of=now)
    out['market_transport_probes'].append({'symbol':symbol,'quote_reason':q.reason_code,'quote_time':q.quote.quote_time.isoformat() if q.quote else None,
         'quote_price':q.quote.price if q.quote else None,'minute_reason':b.reason_code,'bar_count':len(b.bars),
         'last_bar_end':b.bars[-1].bar_end.isoformat() if b.bars else None,
         'preopen_context':'PRIOR_SESSION_QUOTES_ARE_NOT_TODAY_EXECUTABLE' if now.hour<9 else 'VALIDATE_CURRENT_TRADE_DATE'})
print(json.dumps(out,ensure_ascii=False,default=str))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_OVERWRITE')
    result = subprocess.run(['ssh','aurum-vm',
        'cd /www/wwwroot/Agu/liangjian-funnel-workflow && runuser -u www -- .venv/bin/python -B -'],
        input=REMOTE.encode(),capture_output=True,timeout=55)
    if result.returncode:
        raise SystemExit(result.stderr.decode(errors='replace')[-1500:])
    out = json.loads(result.stdout)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    # Exclusive evidence output; production is never opened for writing.
    with args.output.open('x',encoding='utf-8') as f:
        json.dump(out,f,ensure_ascii=False,indent=2)
    print(json.dumps({'output':str(args.output),'head':out['head'],'plans':len(out['today_plans']),
                     'installed_modules_match':all(r['matches_src'] for r in out['modules']),
                     'auction_base_status':(out['auction_base'] or {}).get('status'),
                     'quotes':out['market_transport_probes']},ensure_ascii=False))


if __name__ == '__main__':
    main()
