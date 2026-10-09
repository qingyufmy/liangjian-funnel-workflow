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


REMOTE_CLOSE_SCOPE = r'''
import collections,hashlib,json,pathlib,sqlite3,subprocess
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import _active_a1_downstream_scope
from liangjian_funnel.pipeline.local_fact_cache import canonical_json_hash
root=pathlib.Path.cwd();s=Settings.from_env(root=root);zone=ZoneInfo('Asia/Shanghai')
start=DAY+'T15:10:00+08:00';end=DAY+'T16:40:01+08:00'
def ro(path):
 c=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=5);c.row_factory=sqlite3.Row;c.execute('BEGIN');return c
def file_info(path):
 if not path.is_file():return {'path':str(path),'exists':False}
 h=hashlib.sha256()
 with path.open('rb') as f:
  for part in iter(lambda:f.read(1024*1024),b''):h.update(part)
 return {'path':str(path),'exists':True,'bytes':path.stat().st_size,'sha256':h.hexdigest()}
out={'mode':'READ_ONLY_ORIGINAL_CLOSE_SCOPE_EVIDENCE_NOT_CURRENT_RESEARCH','day':DAY,
 'captured_at':datetime.now(zone).isoformat(),'host':subprocess.check_output(['hostname'],text=True).strip(),
 'head':subprocess.check_output(['git','-c','safe.directory='+str(root),'rev-parse','HEAD'],text=True).strip(),
 'window':{'start':start,'end':end},'production_mutation':False,'network_collection':False}
c=ro(root/'state/a1_registry.sqlite3')
row=c.execute("select generation_id,as_of,created_at,sealed_at,payload_hash,status,payload_json from a1_generations where status='SEALED' and as_of<=? order by as_of desc limit 1",(start,)).fetchone()
if row is None:raise RuntimeError('ORIGINAL_A1_GENERATION_NOT_FOUND')
generation=dict(row);payload=json.loads(generation.pop('payload_json'));a1=set(_active_a1_downstream_scope(payload))
del payload,row
out['a1']={'generation':generation,'symbols':sorted(a1),'count':len(a1)}
c.rollback();c.close()
out['a1']['binding_status']='INFERRED_PRE_CLOSE_SEALED_GENERATION_NOT_RUN_RECEIPT'
c=ro(root/'state/a1_registry.sqlite3')
out['a1_active_pointer']=dict(c.execute("select generation_id,activated_at,previous_generation_id from a1_active_pointer where pointer_name='A1'").fetchone())
out['a1_generation_timeline']=[dict(r) for r in c.execute("select generation_id,as_of,created_at,sealed_at,payload_hash,status from a1_generations where julianday(as_of)>=julianday(?) order by as_of",(DAY+'T00:00:00+08:00',))]
c.rollback();c.close()
receipt=s.workflow_output_dir/'runs'/(DAY+'-close.json')
out['original_close_receipt']=file_info(receipt)
if receipt.is_file():
 v=json.loads(receipt.read_text());out['original_close_receipt']['binding']={k:v.get(k) for k in ('run_id','a1_generation_id','snapshot_id','as_of','status')}
out['progress_file']=file_info(s.workflow_progress_path)
if s.workflow_progress_path.is_file():
 v=json.loads(s.workflow_progress_path.read_text());out['progress_file']['binding']={k:v.get(k) for k in ('run_id','a1_generation_id','phase','started_at','updated_at')}
out['original_candidate_files']=[file_info(p) for folder in (s.research_checkpoint_dir,s.fact_store_dir,s.workflow_output_dir/'data_sync') if folder.is_dir() for p in folder.rglob('*') if p.is_file() and (DAY in p.name or DAY.replace('-','') in p.name) and any(k in p.name.lower() for k in ('discovery','scope','universe','catalog','sync'))]
hotpath=s.fact_store_dir/'eastmoney_hot100'/('eastmoney-guba-hot100-'+DAY+'.json')
out['hot100']=file_info(hotpath)
hot=json.loads(hotpath.read_text()) if hotpath.is_file() else {}
hots={str(r['symbol']) for r in hot.get('records',[]) if isinstance(r,dict) and r.get('symbol')}
out['hot100'].update(symbols=sorted(hots),count=len(hots),available=hot.get('available'),as_of=hot.get('as_of'),content_hash=hot.get('content_hash'))
out['hot100']['hash_valid']=bool(hot) and hashlib.sha256(json.dumps(hot.get('records',[]),ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()==hot.get('content_hash')
marker=s.research_checkpoint_dir/'active_runs'/(DAY+'-close.json');out['resume_marker']=file_info(marker)
if marker.is_file():out['resume_marker']['payload']=json.loads(marker.read_text())
out['same_day_snapshots']=[file_info(p) for p in sorted(s.snapshot_dir.glob('snapshot-'+DAY.replace('-','')+'*.json'))]
out['raw_same_day_snapshots']=[file_info(p) for p in sorted((s.snapshot_dir/'raw').glob('snapshot-'+DAY.replace('-','')+'*.json'))]
log=root/'outputs/node'/('node-'+DAY+'.jsonl');out['log']=file_info(log);progress=[];events=[]
if log.is_file():
 for line in log.read_text().splitlines():
  try:r=json.loads(line)
  except ValueError:continue
  if r.get('job') not in ('close','a1'):continue
  if r.get('stream')=='node':events.append({k:r.get(k) for k in ('timestamp','job','message','runId')})
  else:
   try:p=json.loads(r.get('message',''))
   except ValueError:continue
   if p.get('event')=='WORKFLOW_PROGRESS':progress.append({**p,'observed_at':r.get('timestamp')})
out['job_events']=events;out['last_close_progress']=next((p for p in reversed(progress) if p.get('run_id')==DAY+'-close'),None)
out['original_close_phase_boundaries']=[p for i,p in enumerate(progress) if p.get('run_id')==DAY+'-close' and (i==0 or progress[i-1].get('phase')!=p.get('phase') or p.get('processed')==p.get('total'))]
out['progress_scope_totals']=sorted({p.get('total') for p in progress if p.get('run_id')==DAY+'-close' and p.get('phase')=='CNINFO_SYNC' and isinstance(p.get('total'),int)})
c=ro(s.fact_cache_db_path);queried=collections.defaultdict(list);hash_errors=[]
for r in c.execute("select cache_key,content_hash,payload_json,fetched_at,expires_at from cached_results where namespace='CNINFO_ANNOUNCEMENTS' and julianday(fetched_at)>=julianday(?) and julianday(fetched_at)<=julianday(?) order by fetched_at,cache_key",(start,end)):
 d=dict(r);v=json.loads(d.pop('payload_json'));symbol,semantic=d['cache_key'].split(':',1)
 if canonical_json_hash(v)!=d['content_hash']:hash_errors.append(d);continue
 if v.get('end_date')!=DAY:continue
 queried[symbol].append({**d,'semantic':semantic,'ok':v.get('ok'),'complete':v.get('complete'),'end_date':v.get('end_date'),'announcement_count':len(v.get('announcements',[]))})
out['original_window_cached_queries']={k:v for k,v in sorted(queried.items())};out['original_window_cached_symbol_count']=len(queried)
out['query_cache_hash_errors']=hash_errors;c.rollback();c.close()
c=ro(s.fact_cache_db_path)
out['cache_namespace_inventory']=[dict(r) for r in c.execute('select namespace,count(*) as revisions from cached_results group by namespace')]
out['daily_revision_window']=[dict(r) for r in c.execute('select min(fetched_at) as first_fetch,max(fetched_at) as last_fetch,count(*) as revisions,count(distinct symbol) as symbols from daily_bars where julianday(fetched_at)>=julianday(?) and julianday(fetched_at)<=julianday(?)',(start,end))]
c.rollback();c.close()
out['confirmed_cached_outside_a1']=[{'symbol':symbol,'sources':(['HOT100_FILE'] if symbol in hots else []),'cached_queries':queried[symbol]} for symbol in sorted(set(queried)-a1)]
out['hot100_outside_a1']=sorted(hots-a1)
if RECONSTRUCT_DISCOVERY:
 from liangjian_funnel.pipeline.early_discovery import discover_early_setups
 close_progress=[p for p in progress if p.get('run_id')==DAY+'-close' and p.get('phase')=='EARLY_DISCOVERY_DAILY_SYNC']
 complete=next((p for p in reversed(close_progress) if p.get('processed')==p.get('total') and p.get('total')),None)
 if not complete or not complete.get('observed_at'):raise RuntimeError('ORIGINAL_DISCOVERY_COMPLETION_TIME_REQUIRED')
 received_cutoff=complete['observed_at'];market_cutoff=datetime.fromisoformat(start)
 first_bar=(market_cutoff-timedelta(days=800)).isoformat();last_bar=DAY+'T23:59:59+08:00'
 c=ro(s.fact_cache_db_path);reconstructed=[]
 for symbol in sorted(set(queried)-a1-hots):
  rows=[dict(r) for r in c.execute("with ranked as (select *,row_number() over(partition by bar_timestamp,adjust order by julianday(fetched_at) desc,content_hash desc) as rn from daily_bars where symbol=? and adjust='none' and julianday(fetched_at)<=julianday(?) and julianday(bar_timestamp)>=julianday(?) and julianday(bar_timestamp)<julianday(?)) select bar_timestamp,adjust,fetched_at,content_hash,payload_json from ranked where rn=1 order by julianday(bar_timestamp)",(symbol,received_cutoff,first_bar,last_bar))]
  bars=[];errors=[];refs=[]
  for r in rows:
   payload=json.loads(r['payload_json'])
   if canonical_json_hash(payload)!=r['content_hash']:errors.append(r['content_hash']);continue
   bars.append({**payload,'timestamp':r['bar_timestamp'],'adjust':r['adjust']})
   refs.append({k:r[k] for k in ('bar_timestamp','adjust','fetched_at','content_hash')})
  lead=discover_early_setups({symbol:bars},as_of=market_cutoff,symbols=[symbol]) if not errors else {}
  reconstructed.append({'symbol':symbol,'basis':'EX_POST_ORIGINAL_CACHE_REVISIONS_NOT_FROZEN_SOURCE_SET','received_cutoff':received_cutoff,'input_hash':canonical_json_hash(refs),'input_count':len(refs),'first_bar':refs[0] if refs else None,'last_bar':refs[-1] if refs else None,'hash_errors':errors,'lead':lead.get('records',[]),'data_gaps':lead.get('data_gaps',[])})
 c.rollback();c.close()
 out['discovery_reconstruction']={'status':'NOT_ORIGINAL_REVIEW_BUDGET_PROOF','cutoff_progress':complete,'rows':reconstructed,'matched_predicate_count':sum(bool(r['lead']) for r in reconstructed),'target_count':len(reconstructed),'limitations':['NO_ORIGINAL_G0_OR_GLOBAL_REVIEW_RANKING','PER_SYMBOL_PREDICATE_CANNOT_PROVE_GLOBAL_TOP100_ADMISSION','WINDOW_CUTOFF_NOT_EXACT_PER_SYMBOL_READ_TIME','NO_AFTER_CUTOFF_REVISIONS_OR_NEW_COLLECTION_USED']}
out['exact_scope_status']='PENDING_ORIGINAL_DISCOVERY_AND_G0_SCOPE_EVIDENCE'
out['limitations']=['QUERY_CACHE_PROVES_REQUESTED_SUBSET_NOT_ENTIRE_PREPARED_SCOPE','HOT_FILE_MAY_NOT_PROVE_ORIGINAL_SELECTION_IF_REVISED','DO_NOT_INFER_76_NAMES_FROM_COUNTS','NO_RECOMPUTATION_USING_LATER_DAILY_INPUTS']
print(json.dumps(out,ensure_ascii=False,default=str))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--close-scope-day', help='Export original close scope evidence without market collection.')
    parser.add_argument('--reconstruct-discovery', action='store_true', help='Audit predicates from original received cache versions; not original selection proof.')
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_OVERWRITE')
    remote = REMOTE
    if args.close_scope_day:
        from datetime import date
        date.fromisoformat(args.close_scope_day)
        remote = 'DAY='+repr(args.close_scope_day)+'\nRECONSTRUCT_DISCOVERY='+repr(args.reconstruct_discovery)+'\n'+REMOTE_CLOSE_SCOPE
    result = subprocess.run(['ssh','aurum-vm',
        'cd /www/wwwroot/Agu/liangjian-funnel-workflow && runuser -u www -- .venv/bin/python -B -'],
        input=remote.encode(),capture_output=True,timeout=90 if args.close_scope_day else 55)
    if result.returncode:
        raise SystemExit(result.stderr.decode(errors='replace')[-1500:])
    out = json.loads(result.stdout)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    # Exclusive evidence output; production is never opened for writing.
    with args.output.open('x',encoding='utf-8') as f:
        json.dump(out,f,ensure_ascii=False,indent=2)
    if args.close_scope_day:
        print(json.dumps({'output':str(args.output),'head':out['head'],'a1_count':out['a1']['count'],
                          'scope_totals':out['progress_scope_totals'],'cached_symbols':out['original_window_cached_symbol_count'],
                          'cached_outside_a1':len(out['confirmed_cached_outside_a1']),
                          'same_day_snapshots':len(out['same_day_snapshots']),
                          'exact_scope_status':out['exact_scope_status']},ensure_ascii=False))
        return
    print(json.dumps({'output':str(args.output),'head':out['head'],'plans':len(out['today_plans']),
                     'installed_modules_match':all(r['matches_src'] for r in out['modules']),
                     'auction_base_status':(out['auction_base'] or {}).get('status'),
                     'quotes':out['market_transport_probes']},ensure_ascii=False))


if __name__ == '__main__':
    main()
