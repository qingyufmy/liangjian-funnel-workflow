"""0308 current deterministic kernel over sealed windows; no models/orders.

Frozen kernel candidates, final monitor actions and regular five-minute review
slots remain separate. Historical PM/A6 eligibility is not invented.
"""
from collections import Counter
from datetime import datetime,timedelta
import argparse,hashlib,json,subprocess,sys
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.evaluation.team_v2_replay_batches import read_sealed_source,chronological_batches
from liangjian_funnel.evaluation.team_v2_replay_materializer import FrozenMinuteMaterializer
from liangjian_funnel.evaluation.ablation.exporter import _audit_helpers
from liangjian_funnel.evaluation.team_v2_replay_source import sha,SIX_DAYS
from liangjian_funnel.runtime.strategies import evaluate_strategy
from liangjian_funnel.runtime.team_v2_sources import stamp
from liangjian_funnel.workflow import _intraday_market_context


def file_hash(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-pack',required=True,type=Path)
    p.add_argument('--market-root',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args()
    output=a.output.absolute();allowed=(ROOT/'artifacts/team-v2-20261016').resolve()
    if (output.resolve()!=output or output.exists() or not output.is_relative_to(allowed)
            or any(p.is_symlink() or getattr(p,'is_junction',lambda:False)() for p in (output,*output.parents))):
        raise ValueError('REPLAY_NEW_OWNED_OUTPUT_REQUIRED')
    receipt,records,plans=read_sealed_source(a.source_pack)
    output.mkdir(parents=True);started=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    source_paths=[Path(__file__),ROOT/'src/liangjian_funnel/runtime/strategies.py',
        ROOT/'src/liangjian_funnel/evaluation/team_v2_replay_materializer.py']
    source_hashes={str(p.relative_to(ROOT)):file_hash(p) for p in source_paths}
    _,overlay,_=_audit_helpers(None)
    markets={};days={day:Counter() for day in SIX_DAYS};candidates=[];all_frozen=[];limited=Counter();mismatches=[]
    digest=hashlib.sha256()
    with FrozenMinuteMaterializer(receipt['source_path'],expected_sha256=receipt['source_database_sha256'],
            plans=plans,settings=SimpleNamespace(root=ROOT)) as reader,(output/'KERNEL_INVENTORY.jsonl').open('x',encoding='utf-8',newline='\n') as stream:
        for batch in chronological_batches(records,plans):
            day=batch['trade_date'];counts=days[day];counts['scheduled_minutes']+=1
            entry_ids={f['plan_id'] for f in batch['entry_source_frames']}
            for frame in batch['source_frames']:
                counts['original_plan_minutes']+=1
                frozen=json.loads(frame['original_events'][0]['payload_json']).get('strategy') or {}
                old=frozen.get('action');counts['frozen_kernel:'+str(old)]+=1
                if old in {'BUY_SIGNAL','ADD_SIGNAL'}:
                    all_frozen.append(dict(plan_id=frame['plan_id'],decision_at=frame['decision_at'],
                        frozen_kernel_action=old,original_final_action=frame['original_action'],frame_sha256=frame['frame_sha256']))
                if frame['status']!='SOURCE_BOUND_RULES_PENDING':
                    counts['source_limited_plan_minutes']+=1;limited.update(frame['reason_codes']);continue
                if old is None:
                    counts['original_pre_kernel_unavailable']+=1;continue
                row=plans[frame['plan_id']];raw=json.loads(row['payload_json']);at=stamp(frame['decision_at']);cutoff=at-timedelta(minutes=1)
                bars,proof=reader._window(frame,row)
                key=(day,at.strftime('%H')+f'{at.minute//5*5:02d}')
                if key not in markets:
                    path=a.market_root/key[0]/(key[1]+'.json')
                    markets[key]=(json.loads(path.read_bytes()) if path.is_file() else {},
                        str(path),file_hash(path) if path.is_file() else None)
                market,path,pin=markets[key]
                if market.get('as_of') and stamp(market['as_of'])>min(stamp(e['created_at']) for e in frame['original_events']):
                    counts['future_market_excluded']+=1;continue
                context=_intraday_market_context(row['symbol'],bars,(),current=cutoff,live_market_state=market)
                context=overlay(context,frozen)
                price=frozen.get('live_entry_price')
                if isinstance(price,(int,float)) and not isinstance(price,bool) and price>0:
                    context['realtime_quote']=dict(available=True,price=price,source='FROZEN_KERNEL_LIVE_ENTRY_PRICE_NOT_FULL_QUOTE')
                result=evaluate_strategy(raw,bars,now=cutoff,decision_time=at,position=None,market_context=context).model_dump(mode='json')
                action=result['action'];counts['current_kernel:'+action]+=1
                record=dict(plan_id=row['plan_id'],decision_at=frame['decision_at'],frozen_kernel_action=old,
                    current_kernel_action=action,original_final_action=frame['original_action'],model_tick=batch['model_tick'],
                    entry_source_eligible=row['plan_id'] in entry_ids,frame_sha256=frame['frame_sha256'],
                    current_result_sha256=sha(result),market_sha256=pin,original_snapshot_sha256=proof['original_snapshot_sha256'])
                if action!=old:mismatches.append(record)
                if action in {'BUY_SIGNAL','ADD_SIGNAL'}:
                    record['historical_pm_a6_eligibility']='UNAVAILABLE_NOT_MINTED'
                    candidates.append(record);counts['buy_add_kernel_candidates']+=1
                    if batch['model_tick'] and row['plan_id'] in entry_ids:counts['regular5m_source_candidates']+=1
                line=json.dumps(record,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n'
                stream.write(line);digest.update(line.encode())
        reads=reader.snapshot_reads
    selected=[]
    for day in SIX_DAYS:
        eligible=[row for row in candidates if row['decision_at'][:10]==day and row['model_tick'] and row['entry_source_eligible']]
        selected.extend(eligible[:1])
    result=dict(status='DETERMINISTIC_KERNEL_CANDIDATE_INVENTORY_NOT_MODEL_OR_CONTROL_PASS',
        actual_started_at=started,actual_finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),head=head,
        source_hashes=source_hashes,source_files_unchanged=all(file_hash(ROOT/p)==pin for p,pin in source_hashes.items()),
        original_database_sha256=receipt['source_database_sha256'],archive_sha256=receipt['archive_sha256'],
        market_files=[dict(path=path,sha256=pin,unchanged=(file_hash(Path(path)) if Path(path).is_file() else None)==pin)
            for _,path,pin in markets.values()],snapshot_reads=reads,days=days,limited_source_reasons=limited,
        frozen_kernel_buy_add_events=all_frozen,current_kernel_buy_add_events=candidates,
        action_mismatch_count=len(mismatches),action_mismatches=mismatches,
        deterministic_probe_selection=selected,selection_rule='FIRST_SOURCE_ELIGIBLE_REGULAR5M_KERNEL_BUY_ADD_PER_DAY',
        inventory_jsonl_sha256=digest.hexdigest(),model_calls=0,orders=0,production_writes=0,
        historical_pm_a6_permission='UNAVAILABLE',control_release_gate='NOT_RUN',returns='NOT_CLAIMED')
    with (output/'INVENTORY.json').open('x',encoding='utf-8',newline='\n') as stream:json.dump(result,stream,ensure_ascii=False,indent=2)
    print(json.dumps(dict(status=result['status'],snapshot_reads=reads,action_mismatches=len(mismatches),
        kernel_buy_add=len(candidates),regular5m_selected=len(selected),receipt_sha256=file_hash(output/'INVENTORY.json'))))
    return 0


if __name__=='__main__':sys.exit(main())
