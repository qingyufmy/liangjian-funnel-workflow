"""G-1 original-morning audit + H1-code technical counterfactual, never trades.

All unobserved model votes are rejected. Candidate sightings are not historical
BUY_SIGNALs. The incident plan is excluded; no customer, provider or model I/O.
"""
import argparse
from collections import Counter,defaultdict
from contextlib import closing
from datetime import datetime,timedelta
import gzip,hashlib,json,sqlite3,subprocess,sys,zlib
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];TZ=ZoneInfo('Asia/Shanghai')
SOURCE_PIN='6f1bb2e2a8371c5a65123d8a842f5b2adb7d15508451da39a613acf7457f88cb'
H1='f79abd43e024569578b336ff9a98c7dd737fa915'


def file_hash(path):
 with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def save(path,value):
 with path.open('x',encoding='utf-8') as stream:json.dump(value,stream,ensure_ascii=False,indent=2)


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True)
 p.add_argument('--engine-root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 output=a.output.resolve();engine=a.engine_root.resolve();source=a.source.resolve()
 if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_DIRECTORY_REQUIRED')
 if subprocess.check_output(['git','rev-parse','HEAD'],cwd=engine,text=True).strip()!=H1:raise ValueError('H1_ACCEPTED_SOURCE_REQUIRED')
 sys.path.insert(0,str(engine/'src'))
 from liangjian_funnel.data.mootdx import MinuteBar
 from liangjian_funnel.review.observability import condition_observations
 from liangjian_funnel.runtime.monitor import MonitorEngine
 from liangjian_funnel.runtime.state import RuntimeStore,PlanStatus
 from liangjian_funnel.workflow import _intraday_market_context,_a4_execution_cutoff
 database=source/'frozen.sqlite3'
 if file_hash(database)!=SOURCE_PIN:raise ValueError('G1_ORIGINAL_DATABASE_HASH_INVALID')
 output.mkdir(parents=True);started=datetime.now(TZ).isoformat()
 with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
  db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
  plans=[dict(r) for r in db.execute('SELECT * FROM execution_plans ORDER BY plan_id')]
  events=[dict(r) for r in db.execute('SELECT * FROM monitor_events ORDER BY minute_end,event_id')]
  life=[dict(r) for r in db.execute('SELECT * FROM a4_signal_lifecycles')]
  snapshots=[dict(r) for r in db.execute("SELECT * FROM minute_decision_snapshots WHERE interval='1m' ORDER BY decision_as_of,symbol")]
  db.rollback()
 cutoff=datetime.fromisoformat('2026-10-16T11:19:00+08:00')
 observed=[r for r in events if datetime.fromisoformat(r['minute_end'])<=cutoff]
 morning=condition_observations(plans,observed,cutoff=cutoff)
 by_plan=defaultdict(Counter)
 for row in observed:
  body=json.loads(row['payload_json']);by_plan[body.get('plan_id')][row['reason_code']]+=1
 morning['first_reason_distributions']={k:dict(v) for k,v in by_plan.items() if k is not None}
 save(output/'ORIGINAL_MORNING.json',dict(actual_at=started,source_database_sha256=SOURCE_PIN,
  scope='ORIGINAL_COMMITTED_0930_TO1119_NOT_AFTERNOON',summary=morning,model_calls=0))
 conflicted={r['plan_id'] for r in life if r['symbol']=='600621.SH'}
 if len(conflicted)!=1:raise ValueError('G1_INCIDENT_PLAN_NOT_UNIQUE')
 active=[r for r in plans if r['valid_from'] and datetime.fromisoformat(r['valid_from']).date().isoformat()=='2026-10-16' and r['plan_id'] not in conflicted]
 if len(active)!=12:raise ValueError('G1_OTHER_TWELVE_PLANS_REQUIRED')
 by_symbol={r['symbol']:r for r in active};groups=defaultdict(list);source_receipts=[]
 for row in snapshots:
  if row['symbol'] not in by_symbol:continue
  at=datetime.fromisoformat(row['decision_as_of']);raw=zlib.decompress(row['payload_zlib'])
  if hashlib.sha256(raw).hexdigest()!=row['payload_sha256']:raise ValueError('G1_MINUTE_HASH_INVALID')
  bars=tuple(MinuteBar.model_validate(r) for r in json.loads(raw));ends=[r.bar_end for r in bars]
  if (not bars or ends!=sorted(set(ends)) or any(b.symbol!=row['symbol'] or b.interval!='1m' for b in bars)
    or ends[-1]!=_a4_execution_cutoff(at) or datetime.fromisoformat(row['captured_at'])>=at+timedelta(minutes=1)):
   raise ValueError('G1_ORIGINAL_CLOSED_WINDOW_INVALID')
  groups[at].append((row,bars));source_receipts.append({k:row[k] for k in ('snapshot_id','symbol','decision_as_of','captured_at','payload_sha256')})
 markets=[]
 # Bind the current local files to their immutable original archive hashes.
 with gzip.open(source/'source.jsonl.gz','rt',encoding='utf-8') as stream:
  for line in stream:
   item=json.loads(line)
   if item['kind']!='file' or not item['relative'].startswith('a4_live_market/2026-10-16/'):continue
   relative=Path(item['relative']);raw=(source/'files'/relative).read_bytes()
   if file_hash(source/'files'/relative)!=item['sha256']:raise ValueError('G1_MARKET_ARCHIVE_HASH_INVALID')
   body=json.loads(raw);markets.append((datetime.fromisoformat(body['as_of']),body,item['sha256']))
 markets.sort(key=lambda r:r[0])
 policies={json.dumps(json.loads(r['payload_json']).get('strategy',{}).get('runtime_policy'),sort_keys=True)
  for r in observed if isinstance(json.loads(r['payload_json']).get('strategy'),dict)
  and json.loads(r['payload_json'])['strategy'].get('runtime_policy')}
 if len(policies)!=1:raise ValueError('G1_ORIGINAL_RUNTIME_POLICY_NOT_UNIQUE')
 policy=json.loads(next(iter(policies)));current=[None];sightings=[]
 def reject_unobserved_vote(context):
  for row in context['plans']:
   if row.get('eligible') is True:sightings.append(dict(row,minute=context['minute_end'],
    classification='COUNTERFACTUAL_TECHNICAL_CANDIDATE_MODEL_UNKNOWN',quote_basis='REPLAY_CLOSED_BAR_PRICE'))
  return dict(llm_veto=True,reason_code='COUNTERFACTUAL_MODEL_NOT_OBSERVED')
 store=RuntimeStore(output/'counterfactual.sqlite3')
 try:
  for row in active:store.create_execution_plan(row['plan_id'],row['lane_id'],row['symbol'],status=PlanStatus.ACTIVE_TODAY,
   valid_from=row['valid_from'],expires_at=row['expires_at'],payload=json.loads(row['payload_json']))
  monitor=MonitorEngine(store,llm_veto=reject_unobserved_vote,runtime_policy=policy,
   wall_clock=lambda:current[0],max_seconds=47)
  observed_coverage=defaultdict(set);missing_market=[];callback_ticks=0
  for at,rows in sorted(groups.items()):
   current[0]=at;eligible=[r for r in markets if r[0]<=at]
   market=eligible[-1][1] if eligible else {}
   if not market:missing_market.append(at.isoformat())
   bars={};histories={};contexts={}
   for row,one in rows:
    symbol=row['symbol'];bars[symbol]=one[-1];histories[symbol]=one;observed_coverage[symbol].add(at.isoformat())
    context=_intraday_market_context(symbol,one,(),current=at,execution_cutoff=_a4_execution_cutoff(at),live_market_state=market)
    context.update(replay_classification='COUNTERFACTUAL_NOT_OBSERVED_SIGNAL',quote_basis='REPLAY_CLOSED_BAR_PRICE',
     realtime_quote=dict(available=False,reason_code='HISTORICAL_QUOTE_NOT_ARCHIVED'),
     original_minute_payload_sha256=row['payload_sha256'])
    contexts[symbol]=context
   result=monitor.process_minute('lane_1',bars,minute_snapshot_id='COUNTERFACTUAL:'+at.isoformat(),now=at,
    decision_bar_end=_a4_execution_cutoff(at),bar_histories=histories,market_contexts=contexts,data_ok=True,snapshot_contiguous=True)
   callback_ticks+=result.model_called
  replay=store.list_monitor_events(lane_id='lane_1')
  if store.list_fills('paper:lane_1') or store.list_positions('paper:lane_1') or any(r['action']=='BUY_SIGNAL' for r in replay):
   raise ValueError('G1_NO_COUNTERFACTUAL_BUY_OR_FILL_ALLOWED')
  afternoon=[r for r in replay if datetime.fromisoformat(r['minute_end']).hour>=13]
  afternoon_candidates=[r for r in sightings if datetime.fromisoformat(r['minute']).hour>=13]
  result=dict(classification='COUNTERFACTUAL_H1_CODE_NO_HISTORICAL_REPAIR',actual_started_at=started,
   actual_finished_at=datetime.now(TZ).isoformat(),source_database_sha256=SOURCE_PIN,engine_head=H1,
   driver_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
   incident_plan_ids_excluded=sorted(conflicted),healthy_plan_count=len(active),original_runtime_policy=policy,
   original_window_counts={k:len(v) for k,v in sorted(observed_coverage.items())},original_minute_hashes_verified=len(source_receipts),
   afternoon_original_windows={k:sum(datetime.fromisoformat(v).hour>=13 for v in values) for k,values in sorted(observed_coverage.items())},
   afternoon_committed_decisions=len(afternoon),afternoon_reason_counts=dict(Counter(r['reason_code'] for r in afternoon)),
   afternoon_distinct_candidate_plans=len({r['plan_id'] for r in afternoon_candidates}),
   afternoon_candidate_minutes=len(afternoon_candidates),afternoon_candidates=afternoon_candidates,
   offline_veto_callback_ticks=callback_ticks,model_calls=0,provider_calls=0,production_writes=0,customer_notifications=0,
   effective_buy_signals=0,fills=0,unobserved_model_votes='ALL_REJECTED_NOT_ASSUMED_PASS',
   quote_limitation='EX_POST_CLOSED_BAR_PRICE_NOT_ORIGINAL_REALTIME_QUOTE',
   actual_afternoon_signal_claim='UNPROVEN_MODEL_AND_QUOTE_UNAVAILABLE',cohort_eligible=False,
   missing_market_minutes=missing_market,
   counterfactual_condition_summary=condition_observations(active,replay,cutoff=datetime.fromisoformat('2026-10-16T15:00:00+08:00')))
  save(output/'SOURCE_WINDOWS.json',source_receipts);save(output/'COUNTERFACTUAL_RESULT.json',result)
  print(json.dumps({k:result[k] for k in ('classification','healthy_plan_count','afternoon_committed_decisions',
   'afternoon_distinct_candidate_plans','afternoon_candidate_minutes','effective_buy_signals','fills','model_calls','actual_finished_at')}))
 finally:store.close()
 if file_hash(database)!=SOURCE_PIN:raise ValueError('G1_SOURCE_CHANGED')
 return 0

if __name__=='__main__':sys.exit(main())
