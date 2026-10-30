from pathlib import Path
from datetime import datetime
from dataclasses import replace
from types import SimpleNamespace
import json,subprocess
import pytest
from liangjian_funnel.runtime.closed_recovery_resume import AUTHORIZATION,ClosedRecoveryResume,require_window,require_arguments,require_head,require_generation
from liangjian_funnel.runtime.publication_receipt import BoundObject
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
O=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021');S=Path(__file__).parents[1];CAL=ExchangeTradingCalendar();HEAD=subprocess.check_output(['git','-C',str(S),'rev-parse','HEAD'],text=True).strip()
def at(h):return datetime.fromisoformat('2026-10-30T'+h+':00+08:00')
@pytest.fixture(scope='module')
def binding():
 n=json.loads((O/'SOURCE29_R3_FROZEN_A2_NEWS_NODE_1211.json').read_bytes())
 return ClosedRecoveryResume({**AUTHORIZATION,'H':HEAD},'recovery-close-20261029-'+HEAD[:7]+'-1211-r4',BoundObject(n,AUTHORIZATION['a2_news_node_sha256']))
def test_exact_native_grant_and_installed_head(binding):
 assert require_window(binding,CAL,now=at('11:40')).source_trade_date.isoformat()=='2026-10-29';require_head(S,binding)
@pytest.mark.parametrize('h,ok',[('11:30',False),('11:31',True),('13:59',True),('14:00',False),('14:01',False)])
def test_exact_window(binding,h,ok):
 if ok:require_window(binding,CAL,now=at(h))
 else:
  with pytest.raises(ValueError):require_window(binding,CAL,now=at(h))
@pytest.mark.parametrize('field,value',[('authority',''),('source_trade_date','2026-10-28'),('target_trade_date','2026-11-02'),('snapshot_id','OTHER'),('snapshot_hash','0'*64),('snapshot_file_sha256','0'*64),('clock_fix_root','a'*40),('a1_generation_id','OLD'),('a1_manifest_hash','0'*64),('capture_deadline','2026-10-30T09:30:00+08:00')])
def test_wrong_grant_refused(binding,field,value):
 b=replace(binding,authorization={**binding.authorization,field:value})
 with pytest.raises(ValueError):require_window(b,CAL,now=at('11:40'))
def test_missing_grant_and_wrong_head_and_news_refused(binding):
 with pytest.raises(ValueError):require_window(None,CAL,now=at('11:40'))
 with pytest.raises(ValueError):require_head(S,replace(binding,authorization={**binding.authorization,'H':'f'*40}))
 with pytest.raises(ValueError):require_window(replace(binding,news=BoundObject({},binding.news.sha256)),CAL,now=at('11:40'))
def args(b):return dict(slot='close',as_of=None,historical_replay=False,comparison_run=False,primary_only=True,from_active_a1=True,publish_plans=True,record_runtime=True,run_id=b.run_id,snapshot_id=None,snapshot_expected_hash=AUTHORIZATION['snapshot_hash'],active_a1_generation_id=AUTHORIZATION['a1_generation_id'],regeneration=None,research_update=None,frozen_a2_stock_news=None,target_trade_date=None,market_data_as_of=None,schedule_comparison=False,allow_non_trading_source=False,auction_refresh=False,same_day_recovery=False,models=None,primary_lane_ids=None)
@pytest.mark.parametrize('field,value',[('snapshot_id','snapshot-OTHER'),('publish_plans',False),('record_runtime',False),('as_of',at('11:40')),('historical_replay',True),('comparison_run',True),('from_active_a1',False),('snapshot_expected_hash','0'*64),('active_a1_generation_id','OLD'),('frozen_a2_stock_news','OTHER')])
def test_no_general_snapshot_or_publication_escape(binding,field,value):
 kw=args(binding);kw[field]=value
 with pytest.raises(ValueError):require_arguments(binding,**kw)
def test_valid_arguments(binding):require_arguments(binding,**args(binding))
def test_old_sealed_generation_refused(binding):
 app=SimpleNamespace(settings=SimpleNamespace(root=S),trading_calendar=CAL,a1_registry=SimpleNamespace(get_active_generation=lambda:None))
 with pytest.raises(ValueError,match='SEALED_ACTIVE_A1_CHANGED'):require_generation(app,binding,now=at('11:40'))
def test_original_research_only_resume_stays_closed(binding):
 from liangjian_funnel.pipeline.research_overlay_resume import require_resume_arguments
 kw=dict(normalized_slot='close',from_active_a1=True,primary_only=True,reuse_completed_snapshot=True,reuse_resume_snapshot=True,run_id_override=binding.run_id,snapshot_expected_hash=AUTHORIZATION['snapshot_hash'],historical_replay=False,comparison_run=False,schedule_comparison=False,auction_refresh=False,same_day_recovery=False,target_trade_date=None,allow_non_trading_source=False,market_data_as_of=None,snapshot_id=None,models=None,primary_lane_ids=None,publish_plans=True,record_runtime=True)
 with pytest.raises(ValueError,match='A2_NEWS_RESUME_ARGUMENTS_INVALID'):require_resume_arguments(**kw)
def test_normal_close_capture_deadline_still_closed():
 from liangjian_funnel.runtime.calendar import closed_research_session
 with pytest.raises(ValueError,match='DEADLINE'):closed_research_session(at('11:40'),CAL)
