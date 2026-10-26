"""Controlled originals through the real archive and native minute-cache path."""
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path

from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.candidate_artifacts import prepare_candidate_artifacts
from liangjian_funnel.runtime.history_freshness import session_grid
from liangjian_funnel.runtime.indicator_history import prepare_pattern_history
from liangjian_funnel.runtime.publication_archive import capture_plan_originals, write_publication_manifest
from liangjian_funnel.runtime.publication_receipt import object_hash


def prepare(store, batch, *, run_id, observed_at):
    calendar=ExchangeTradingCalendar()
    target=datetime.fromisoformat(str(batch[0]['expires_at'])).date()
    source=calendar.previous_trading_day(target)
    at=observed_at.replace(year=source.year,month=source.month,day=source.day,hour=16)
    workspace=store.path.parent.parent if store.path.parent.name=='state' else store.path.parent
    root=workspace/'outputs';root.mkdir(exist_ok=True)
    proofs=root/'candidate-contracts';proofs.mkdir(exist_ok=True)
    saved=proofs/(run_id+'.json')
    if saved.exists():return json.loads(saved.read_bytes())
    dates=[]; day=source
    for _ in range(20):
        dates.append(day); day=calendar.previous_trading_day(day)
    daily=[dict(date=d.isoformat(),close=10,high=11) for d in sorted(dates)]
    data={'RECENT_DAILY_BARS':{r['symbol']:deepcopy(daily) for r in batch}}
    raw=[dict(r['payload']) for r in batch]
    a3=dict(stage='A3',lane='lane_1',status='VALIDATED',output={'core_watch_pool':raw})
    a3['output_hash']=object_hash(a3['output'])
    audit=root/(run_id+'-audit.json'); audit.write_text(json.dumps({'stages':[a3]}),encoding='utf-8')
    base=root/(run_id+'-snapshot.json'); base.write_text(json.dumps(dict(snapshot_id='controlled',snapshot_hash=object_hash(data),
        as_of=at.isoformat(),data=data)),encoding='utf-8')
    originals={r['plan_id']:capture_plan_originals(raw=raw[i],pool='core_watch_pool',index=i,a3_audit=a3,authority=None)
               for i,r in enumerate(batch)}
    manifest=write_publication_manifest(output_dir=root/'publication_proofs',run_id=run_id,lane_id='lane_1',
        base_snapshot_path=base,base_snapshot_id='controlled',base_snapshot_hash=object_hash(data),
        audit_path=audit,originals=originals,published=batch,published_at=at,research_as_of=at,
        publication_mode='CANDIDATE_PENDING_REVIEW',minimum_trade_date=target)
    minute=MinuteBarStore(root/'minute')
    minute.write([MinuteBar(symbol=symbol,interval='5m',bar_end=t,open=10,high=11,low=9,close=10,
        volume=1000,amount=10000,source_id='CONTROLLED_TENCENT_NATIVE')
        for symbol in sorted({r['symbol'] for r in batch}) for t in session_grid(source)])
    class NoHttp:
        def fetch_bars(self,*a,**kw):raise AssertionError('fixture cache must not fetch')
    history=prepare_pattern_history(batch,now=at,calendar=calendar,minute_store=minute,provider=NoHttp())
    assert history['status']=='READY'
    result=prepare_candidate_artifacts(batch,workflow_output_dir=root,run_id=run_id,status='READY',
        market_trade_date=source,projection_manifests=[dict(lane_id='lane_1',**manifest)],
        pattern_preparation=history,minute_cache_path=root/'minute/minute_bars.sqlite3',observed_at=at,calendar=calendar)
    saved.write_text(json.dumps(result),encoding='utf-8')
    return result


def stage_natural_candidate(store,batch,*,run_id,observed_at,regeneration=None):
    """CAS fixtures now supply complete controlled artifacts, not a PASS flag."""
    from liangjian_funnel.runtime.reviewed_batch_natural import stage_natural_candidate as stage
    proof=None
    scope=getattr(regeneration,'source_scope',None)
    if scope and (run_id!=regeneration.run_id or any(row['lane_id']!=scope['lane_id'] or
            str(row['expires_at'])[:10]!=scope['target_trade_date'] for row in batch)):
        return stage(store,batch,run_id=run_id,observed_at=observed_at,regeneration=regeneration)
    if batch:
        target=datetime.fromisoformat(str(batch[0]['expires_at'])).date()
        source=ExchangeTradingCalendar().previous_trading_day(target)
        if store.get_workflow_run(run_id,batch[0]['lane_id']) is None:
            store.record_workflow_run(run_id=run_id,lane_id=batch[0]['lane_id'],trade_date=source.isoformat(),
                slot='close',status='READY',model='CONTROLLED',snapshot_hash='a'*64)
        proof=prepare(store,batch,run_id=run_id,observed_at=observed_at)
    return stage(store,batch,run_id=run_id,observed_at=observed_at,regeneration=regeneration,artifact_proof=proof)


def complete_publisher_inputs(app,result,data,*,now,target):
    """Give a real Workflow publisher fixture original files and native cache."""
    from dataclasses import replace
    from types import SimpleNamespace
    from liangjian_funnel.pipeline.research import StageAudit
    calendar=ExchangeTradingCalendar();source=calendar.previous_trading_day(target)
    days=[];day=source
    for _ in range(20):days.append(day);day=calendar.previous_trading_day(day)
    symbols={p['symbol'] for lane in result.lanes for name in ['core_watch_pool','secondary_watch_pool']
             for p in lane.final_output.get(name,[])}
    data['RECENT_DAILY_BARS']={s:[dict(date=d.isoformat(),close=10,high=11) for d in sorted(days)] for s in symbols}
    output=app.settings.workflow_output_dir;output.mkdir(exist_ok=True,parents=True)
    path=output/(result.run_id+'-snapshot.json');snapshot_hash=object_hash(data)
    path.write_text(json.dumps(dict(snapshot_id=result.snapshot_id,snapshot_hash=snapshot_hash,
        as_of=now.isoformat(),data=data)),encoding='utf-8')
    lanes=[]
    for lane in result.lanes:
        audit=StageAudit(lane=lane.lane,model=lane.model,stage='A3',status='VALIDATED',snapshot_id=result.snapshot_id,
            prompt_hash=None,input_hash=None,output_hash=object_hash(lane.final_output),latency_ms=0,attempts=1,
            thinking_variant='team_v2',symbols=tuple(sorted(symbols)),reason_codes=(),output=lane.final_output)
        updated=replace(lane,stages=(audit,),audit_path=output/(result.run_id+'-'+lane.lane+'-audit.json'))
        updated.audit_path.write_text(json.dumps(updated.as_dict()),encoding='utf-8');lanes.append(updated)
    app.settings.minute_cache_dir=output/'minute'
    app.minute_store=MinuteBarStore(app.settings.minute_cache_dir)
    app.minute_store.write([MinuteBar(symbol=s,interval='5m',bar_end=t,open=10,high=11,low=9,close=10,
        volume=1000,amount=10000,source_id='CONTROLLED_TENCENT_NATIVE') for s in symbols for t in session_grid(source)])
    app.market_data=SimpleNamespace(fetch_bars=lambda *a,**kw:(_ for _ in ()).throw(AssertionError('SOURCE_HTTP_FORBIDDEN')))
    app.store.record_workflow_run(run_id=result.run_id,lane_id='lane_1',trade_date=source.isoformat(),
        slot='close',model='CONTROLLED',status='READY_TO_PUBLISH',snapshot_hash=snapshot_hash)
    return replace(result,snapshot_hash=snapshot_hash,lanes=tuple(lanes)),path
