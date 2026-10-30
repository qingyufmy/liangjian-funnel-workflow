from dataclasses import replace
from datetime import datetime
from copy import deepcopy
from types import SimpleNamespace
import json,pytest
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.closed_session_capture import KEY,build_capture_proof
from liangjian_funnel.runtime.publication_receipt import object_hash
from liangjian_funnel.pipeline.research.common import FrozenInputSnapshot,_capture_snapshot_recipe,_freeze_snapshot,ResearchPipeline,StageAudit
from liangjian_funnel.pipeline.team_v2_research_time import research_times
CAL=ExchangeTradingCalendar()
def at(t):return datetime.fromisoformat(t+'+08:00')
def root():
 stamp=at('2026-10-30T08:40:00');p=build_capture_proof(source_trade_date='2026-10-29',started_at=at('2026-10-30T08:35:00'),captured_at=stamp,completed_at=at('2026-10-30T08:50:00'),calendar=CAL)
 return FrozenInputSnapshot('snapshot-capture1211',data={KEY:p,'MARKET_DATA_AS_OF':p['source_market_close_at']},as_of=stamp)
def child(base):
 overlay={'NEWS_HEAT_SNAPSHOT':{},'A2_STOCK_NEWS_SCOPE':[]};recipe={'base_snapshot_hash':base.snapshot_hash,'stage':'A2',**overlay};h=object_hash(recipe)
 x=FrozenInputSnapshot(base.snapshot_id+':a2:'+h[:12],snapshot_hash=h,data={**base.data,**overlay},as_of=at('2026-10-30T08:55:00'))
 return _capture_snapshot_recipe(base,x,recipe_kind='A2_STOCK_NEWS',hash_recipe=recipe,data_recipe=overlay)
def test_native_child_clock_after_capture_and_freeze_keeps_root():
 b=root();c=child(b);before=json.dumps(b.data[KEY],sort_keys=True)
 assert research_times(_freeze_snapshot(c),CAL)==(c.as_of,at('2026-10-29T15:00:00'))
 assert json.dumps(c.data[KEY],sort_keys=True)==before and c.capture_root is b
@pytest.mark.parametrize('case',['missing_root','root_hash','root_data','parent_id','parent_hash','parent_time','recipe','delta','child_time_only','market','proof','target'])
def test_root_recipe_or_time_tamper_cannot_pass(case):
 b=root();c=child(b);chain=deepcopy(c.proof_chain)
 if case=='missing_root':c=replace(c,capture_root=None)
 elif case=='root_hash':c=replace(c,capture_root=replace(b,snapshot_hash='f'*64))
 elif case=='root_data':c=replace(c,capture_root=replace(b,data={**b.data,'OTHER':True}))
 elif case=='parent_id':chain[0]['parent_snapshot_id']='OTHER'
 elif case=='parent_hash':chain[0]['parent_snapshot_hash']='f'*64
 elif case=='parent_time':chain[0]['parent_as_of']=c.as_of.isoformat()
 elif case=='recipe':chain[0]['hash_recipe']['stage']='A3'
 elif case=='delta':chain[0]['data_recipe']['A2_STOCK_NEWS_SCOPE']=['OTHER']
 elif case=='child_time_only':c=replace(c,as_of=at('2026-10-30T08:56:00'))
 elif case=='market':c=replace(c,data={**c.data,'MARKET_DATA_AS_OF':c.as_of.isoformat()})
 elif case in('proof','target'):
  bad=dict(b.data[KEY]);bad['capture_as_of'if case=='proof'else'target_trade_date']='2026-10-30T08:36:00+08:00'if case=='proof'else'2026-11-02'
  c=child(FrozenInputSnapshot(b.snapshot_id,data={**b.data,KEY:bad},as_of=b.as_of))
 if case not in('proof','target'):c=replace(c,proof_chain=chain)
 with pytest.raises(ValueError):research_times(c,CAL)
@pytest.mark.parametrize('case',['future','deadline','stale'])
def test_original_capture_time_guards_remain(case):
 b=root();p=dict(b.data[KEY])
 if case=='stale':
  stamp=at('2026-10-29T05:00:00');p=build_capture_proof(source_trade_date='2026-10-28',started_at=stamp,captured_at=stamp,completed_at=stamp,calendar=CAL);b=FrozenInputSnapshot('snapshot-old',data={KEY:p,'MARKET_DATA_AS_OF':p['source_market_close_at']},as_of=stamp)
 else:
  p['capture_completed_at']='2026-10-30T09:05:00+08:00'if case=='future'else'2026-10-30T09:15:00+08:00';b=FrozenInputSnapshot(b.snapshot_id,data={**b.data,KEY:p},as_of=b.as_of)
 with pytest.raises(ValueError,match='DEADLINE'if case=='deadline'else'CLOSED_SESSION_CAPTURE_STALE_OR_FUTURE'):research_times(child(b),CAL)
def test_real_full_wrapper_entry_binds_actual_parent():
 b=root();c=child(b);pipeline=SimpleNamespace(stage_snapshot_enricher=lambda **kw:dict(snapshot_id=c.snapshot_id,snapshot_hash=c.snapshot_hash,as_of=c.as_of,data=c.data,proof_chain=c.proof_chain))
 x=ResearchPipeline._enrich_stage_snapshot(pipeline,stage='A2',lane_id='lane_1',model='OFFLINE',upstream_symbols=set(),snapshot=b)
 assert x.capture_root.snapshot_hash==b.snapshot_hash and research_times(x,CAL)==(c.as_of,at('2026-10-29T15:00:00'))
def test_a3_original_error_frames_persist(monkeypatch):
 from liangjian_funnel.pipeline.team_v2_research import TeamResearchAdapter
 adapter=TeamResearchAdapter.__new__(TeamResearchAdapter)
 def fail(*a,**k):raise ValueError('CLOSED_SESSION_CAPTURE_BINDING_INVALID')
 monkeypatch.setattr(adapter,'_inputs',fail)
 def blocked(lane,model,stage,sid,reason):return StageAudit(lane,model,stage,'BLOCKED',sid,None,None,None,None,0,None,(),(reason,))
 audit=adapter.run_stage(SimpleNamespace(_blocked_stage=blocked),lane_id='lane_1',model='OFFLINE',stage='A3',snapshot=child(root()),upstream_output={},upstream_symbols=set(),run_id='offline1211')
 v=audit.as_dict();assert v['reason_codes']==['CLOSED_SESSION_CAPTURE_BINDING_INVALID'];f=v['diagnostics']['input_failure'];assert f['exception_type']=='ValueError'and f['frames']and f['fields']['root_snapshot_id']=='snapshot-capture1211'
