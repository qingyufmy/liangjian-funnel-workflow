"""Analyze the already exported evidence locally; never synthesize missing minutes."""
import collections, hashlib, json, pathlib, sys
sys.stdout.reconfigure(encoding='utf-8')
root=pathlib.Path(__file__).resolve().parents[2]
source=pathlib.Path(sys.argv[1]).resolve();data=json.loads(source.read_bytes())
events=data['events'];per={};reasons=collections.Counter();actions=collections.Counter();effective=[];invalid=[]
core=('A3_PULLBACK_ZONE','TREND_15M_PRESSURE_EASING','TREND_PULLBACK_VOLUME_CONTRACTION','TREND_PRIOR_5M_REVERSAL','TREND_SUBSEQUENT_5M_CONFIRMATION','TREND_VWAP_RECLAIMED','TREND_VOLUME_NOT_OVERHEATED')
for row in events:
 p=json.loads(row['payload_json']);s=p.get('strategy') or {};pid=p.get('plan_id')
 actions[row['action']]+=1;reasons[row['reason_code']]+=1
 if row['effective']:effective.append({k:row[k] for k in ('event_id','minute_end','action','reason_code')}|{'plan_id':pid,'symbol':p.get('symbol'),'llm_veto':p.get('llm_veto'),'llm_reason_code':p.get('llm_reason_code'),'entry_contract':p.get('entry_contract')})
 if not pid:continue
 v=per.setdefault(pid,{'symbol':p.get('symbol'),'records':0,'minutes':set(),'first_reasons':collections.Counter(),'max_conditions_met':None,'max_at':None,'failing':None,'start_confirmation_minutes':[],'llm_veto_minutes':[]})
 v['records']+=1;v['minutes'].add(row['minute_end']);v['first_reasons'][row['reason_code']]+=1
 cr=s.get('confirmation_results') or {}
 if cr:
  met=[k for k in core if isinstance(cr.get(k),dict) and cr[k].get('met') is True]
  if v['max_conditions_met'] is None or len(met)>v['max_conditions_met']:
   v.update(max_conditions_met=len(met),max_at=row['minute_end'],failing=[k for k in core if k not in met])
 if s.get('state')=='START_CONFIRMATION' or 'START_CONFIRMATION' in str(s.get('reason_codes')):v['start_confirmation_minutes'].append(row['minute_end'])
 if p.get('llm_veto') or s.get('llm_veto'):v['llm_veto_minutes'].append(row['minute_end'])
 if row['action']=='PLAN_INVALIDATED':invalid.append({'plan_id':pid,'symbol':p.get('symbol'),'at':row['minute_end'],'reason':row['reason_code'],'strategy_reasons':s.get('reason_codes'),'runtime_policy':s.get('runtime_policy')})
plans=[];history=collections.defaultdict(collections.Counter);regimes=collections.defaultdict(set)
for row in data['recent_plan_rows']:
 p=json.loads(row['payload_json']);day=row['expires_at'][:10];history[day][p.get('strategy_profile','UNKNOWN')]+=1
 sf=p.get('strategy_facts') or {};regimes[day].add((sf.get('market_regime'),sf.get('emotion_cycle_stage'),sf.get('emotion_new_long_permission')))
 if day=='2026-10-16':plans.append({k:row.get(k) for k in ('plan_id','symbol','status','valid_from','expires_at','updated_at')}|{'name':p.get('company_name'),'profile':p.get('strategy_profile'),'per_plan':per.get(row['plan_id'])})
for v in per.values():v['minutes']=len(v['minutes']);v['first_reasons']=dict(v['first_reasons'])
jobs=[r for r in data['jobs'] if r['job']=='monitor'];dur=sorted(r['durationMs']/1000 for r in jobs if isinstance(r.get('durationMs'),(int,float)))
def pct(v,p):return v[max(0,__import__('math').ceil(p*len(v))-1)] if v else None
def walk(x):
 if isinstance(x,dict):
  yield x
  for v in x.values():yield from walk(v)
 elif isinstance(x,list):
  for v in x:yield from walk(v)
codes=collections.Counter();phases=[]
for run,bodies in data['monitor_stdout'].items():
 for x in walk(bodies):
  if isinstance(x.get('reason_code'),str):codes[x['reason_code']]+=1
  if 'phase_timings' in x:phases.append({'run_id':run,'value':x['phase_timings']})
for run,trace in data.get('monitor_trace',{}).items():
 if isinstance(trace,dict) and isinstance(trace.get('phase_timings'),dict):phases.append({'run_id':run,'value':trace['phase_timings']})
life=[{k:r.get(k) for k in ('plan_id','symbol','status','signal_time','entry_time','entry_qty','exit_qty','exit_reason','updated_at')} for r in data['lifecycles']]
result={'actual_at':data['actual_at'],'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'source_file':str(source.relative_to(root)),'event_records':len(events),'event_minutes':len({r['minute_end'] for r in events}),'actions':dict(actions),'first_reasons':dict(reasons),'effective_events':effective,'invalidations':invalid,'today_plans':plans,'lifecycles':life,'fills':data['fills'],'monitor_job_counts':dict(collections.Counter(str(r['exitCode']) for r in jobs)),'monitor_duration_seconds':{'p50':pct(dur,.5),'p95':pct(dur,.95),'max':max(dur) if dur else None,'over45':sum(x>45 for x in dur),'over50':sum(x>50 for x in dur)},'monitor_stdout_codes':dict(codes),'phase_records':phases,'daily_plan_profiles':{k:dict(v) for k,v in sorted(history.items())},'daily_plan_contexts':{k:sorted(v,key=str) for k,v in sorted(regimes.items())},'pending_caveats':['Only committed events observable; failed transactions may roll back later plan evaluations.','Daily history counts stored plan rows, not surviving active pool; superseded batches may coexist.','Strong-stock ranking requires an existing frozen full-market price/board dataset; no new provider calls authorized by this audit.']}
result['phase_summary_ms']={}
for name in ('startup','fetch','judge','persist','notify'):
 values=sorted(float(x['value'][name]['duration_ms']) for x in phases if name in x['value'])
 result['phase_summary_ms'][name]={'n':len(values),'p50':pct(values,.5),'p95':pct(values,.95),'max':max(values) if values else None}
result['first_failed_job']=min((r for r in jobs if r.get('exitCode')==2),key=lambda r:r['startedAt'])
funnel_source=sorted(source.parent.glob('*funnel-evidence-stdout.json'))[-1]
funnel=json.loads(funnel_source.read_bytes());rank_receipt=json.loads(funnel_source.with_name(funnel_source.name.replace('-stdout.json','-receipt.json')).read_bytes())
prior=next(v for k,v in funnel['funnel_files'].items() if '2026-10-15-close' in k)
by_stage={stage:{v['symbol']:v for v in prior['items'] if v['stage']==stage} for stage in ('A1','A2','A3')}
result['cached_strong_stock_funnel']=[];fall=collections.Counter()
for item in rank_receipt['selected']:
 symbol=item['symbol'];row={**item,'A1':by_stage['A1'].get(symbol),'A2':by_stage['A2'].get(symbol),'A3':by_stage['A3'].get(symbol)}
 a1=row['A1'];a2=row['A2'];a3=row['A3']
 if not a1:layer='A1_NOT_IN_APPROVED_SET_THEME_EXISTENCE_UNPROVEN'
 elif not a2:layer='A2_NOT_RETAINED_BOARD_VS_REGIME_UNRESOLVED'
 elif not a3:layer='A3_NOT_RETAINED_REASON_UNAVAILABLE'
 else:layer='A3_'+str(a3['pool']).upper()
 row['proven_layer']=layer;fall[layer]+=1;result['cached_strong_stock_funnel'].append(row)
result['cached_rank_scope']={'ranked_symbols':rank_receipt['ranked_symbols'],'formal_full_market_top50':False,'ipo_age_filter_proven':False,'fall_layers':dict(fall),'military_members_examined':len(rank_receipt['military_members']),'limit_up_list_proven':False}
result['A5_originals']=[{k:v for k,v in x.items() if k!='markdown'} for x in funnel['a5_originals']]
result['next_plans']=[{k:v for k,v in x.items() if k!='payload_json'} for x in funnel['next_plans']]
result['funnel_source_sha256']=hashlib.sha256(funnel_source.read_bytes()).hexdigest()
result['notifications']=funnel['notifications']
published={r['run_id']:r for r in data['recent_runs'] if r.get('status')=='PUBLISHED' and r.get('slot')=='CLOSE'}
groups=collections.defaultdict(lambda:collections.defaultdict(list))
for row in data['recent_plan_rows']:
 p=json.loads(row['payload_json']);rid=p.get('source_run_id')
 if rid in published:groups[row['expires_at'][:10]][rid].append(p)
result['latest_close_plan_profiles']={}
for day,batches in sorted(groups.items()):
 latest=max(batches,key=lambda rid:published[rid]['created_at'])
 result['latest_close_plan_profiles'][day]={'source_run_id':latest,'profiles':dict(collections.Counter(x.get('strategy_profile','UNKNOWN') for x in batches[latest])),'stored_plan_count':len(batches[latest]),'not_a_surviving_active_pool':True}
target=source.with_name(source.name.replace('-stdout.json','-analysis-v2.json'))
with target.open('x',encoding='utf-8') as f:json.dump(result,f,ensure_ascii=False,indent=2)
print(json.dumps({k:result[k] for k in ('actual_at','event_records','event_minutes','actions','monitor_job_counts','monitor_duration_seconds')},ensure_ascii=False,indent=2))
print('phases',len(phases),'todayplans',len(plans),'effective',len(effective),'file',target)
