"""1197 controlled native producer -> actual client streams -> journals -> CAS.
All clocks/data are isolated test fixtures; not live model quality or production activation.
"""
from datetime import datetime
from copy import deepcopy
from types import SimpleNamespace
import json
import pytest
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.morning_window_authority import morning_deadline
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
from liangjian_funnel.runtime.reviewed_batch_evidence import _pm_deadline_in_session
from liangjian_funnel.runtime.reviewed_batch_natural import read_natural_candidate
from liangjian_funnel.runtime.reviewed_batch_switch import validate_bundle
from liangjian_funnel.runtime.state import StateTransitionError
DAY='2026-10-30';D=datetime.fromisoformat

def grant(candidate=None):
 return dict(authority='Claude1197',capture_authority='Claude1195',source_trade_date='2026-10-29',trade_date=DAY,
  deadline_at=DAY+'T14:00:00+08:00',candidate_scope_id=candidate['scope_id']if candidate else'controlled',
  candidate_sha256=candidate['body_sha256']if candidate else'a'*64,candidate_revision=candidate['revision']if candidate else 1)

@pytest.mark.parametrize('stage',['engine','risk','pm'])
def test_unmarked_late_actual_entry_rejects_before_source_or_http(tmp_path,stage):
 r=run_morning_stage(SimpleNamespace(root=tmp_path),stage,clock=lambda:D(DAY+'T10:00:00+08:00'),calendar=ExchangeTradingCalendar())
 assert r['reason_code']=='MORNING_STAGE_WINDOW_MISSED'and not list(tmp_path.iterdir())

@pytest.mark.parametrize('key,value',[('authority','unknown'),('capture_authority','unknown'),('trade_date','2026-11-02'),('source_trade_date','2026-10-28'),('candidate_revision',0),('candidate_sha256','bad')])
def test_unknown_or_cross_scope_marker_rejected_before_source(tmp_path,key,value):
 a=grant();a[key]=value
 with pytest.raises(ValueError,match='AUTHORITY'):
  run_morning_stage(SimpleNamespace(root=tmp_path),'engine',clock=lambda:D(DAY+'T10:00:00+08:00'),calendar=ExchangeTradingCalendar(),authorization=a)
 assert not list(tmp_path.iterdir())

@pytest.mark.parametrize('at',['2026-10-30T14:00:00+08:00','2026-10-31T10:00:00+08:00'])
def test_scoped_window_end_closed(at):
 with pytest.raises(ValueError):morning_deadline(D(at).date().isoformat(),'2026-10-29',authorization=grant(),now=D(at))

def test_evidence_deadline_requires_mark_and_exact_candidate():
 a=grant();source=dict(source_namespace='NATURAL_MORNING_CANDIDATE',source_trade_date='2026-10-29',trade_date=DAY,
  **{k:a[k]for k in ['candidate_scope_id','candidate_sha256','candidate_revision']})
 late=D(DAY+'T14:00:00+08:00')
 assert not _pm_deadline_in_session(late,DAY)
 assert _pm_deadline_in_session(late,DAY,authorization=a,source=source)
 bad=dict(source,candidate_revision=2)
 with pytest.raises(ValueError,match='BINDING'):_pm_deadline_in_session(late,DAY,authorization=a,source=bad)

@pytest.mark.parametrize('existing_premarket_baseline,engine_time',[(False,'09:24'),(False,'09:25'),(False,'10:00'),(True,'10:00')])
def test_controlled_native_candidate_real_stream_a6_pm_cas_at1000(tmp_path,monkeypatch,existing_premarket_baseline,engine_time):
 import socket
 monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:(_ for _ in ()).throw(AssertionError('NO_REAL_NETWORK')))
 import test_team_v2_morning as wiring
 import test_natural_reviewed_batch_0595 as native
 cal=ExchangeTradingCalendar();monkeypatch.setattr(wiring,'CAL',cal);monkeypatch.setattr(native,'CAL',cal);monkeypatch.setattr(native,'DAY',DAY)
 original=native.stage_natural_candidate
 def stage(*args,**kwargs):
  kwargs['observed_at']=D('2026-10-29T16:00:00+08:00');return original(*args,**kwargs)
 monkeypatch.setattr(native,'stage_natural_candidate',stage)
 settings,clock,store,_,staged=native.setup(tmp_path,monkeypatch,old=False)
 # Evidence wall time shares this isolated fixture clock, including journal captures.
 # This changes neither OS time nor any production journal.
 import liangjian_funnel.runtime.reviewed_batch_evidence as evidence
 class FixtureDatetime(datetime):
  @classmethod
  def now(cls,tz=None):return clock.now if tz is None else clock.now.astimezone(tz)
 monkeypatch.setattr(evidence,'datetime',FixtureDatetime)
 candidate=read_natural_candidate(settings,day=DAY);a=grant(candidate);api,calls=native.api(settings)
 if existing_premarket_baseline:
  # Only this isolated positive fixture has a genuine native premarket baseline.
  # Production cannot create one retroactively under this morning authorization.
  from liangjian_funnel.runtime.broker.local_paper import create_broker
  clock.now=D(DAY+'T08:30:00+08:00')
  broker=create_broker('local_paper',database=settings.root/'state/team_v2/paper.db',
   paper_config=settings.root/'config/paper_broker.yaml',risk_config=settings.root/'config/risk_limits.yaml',
   clock=lambda:clock.now,calendar=cal,run_id='controlled-premarket-baseline')
  assert broker.open_risk_day(DAY)['opened_at']==clock.now.isoformat()
  from liangjian_funnel.runtime.broker.local_paper import LocalPaperAdapter
  monkeypatch.setattr(LocalPaperAdapter,'open_risk_day',lambda *a,**k:(_ for _ in ()).throw(AssertionError('1201_MUST_REUSE_WITHOUT_SECOND_OPEN')))
 clock.now=D(DAY+'T'+engine_time+':00+08:00')
 if not existing_premarket_baseline and engine_time>='09:25':
  reason='PAPER_RISK_DAY_OPEN_TIME_INVALID'if engine_time>='09:30'else'MORNING_RISK_DAY_CREATION_AFTER0925_1201'
  with pytest.raises(ValueError,match=reason):
   run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=cal,authorization=a)
  assert calls==[] and store.get_execution_plan('p1')is None
  return
 assert run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=cal,authorization=a)['morning_window_authorization']==a
 if engine_time=='09:24':
  from liangjian_funnel.runtime.broker.local_paper import create_broker
  b=create_broker('local_paper',database=settings.root/'state/team_v2/paper.db',paper_config=settings.root/'config/paper_broker.yaml',risk_config=settings.root/'config/risk_limits.yaml',clock=lambda:clock.now,calendar=cal,run_id='controlled-readback')
  assert b.planning_snapshot()['risk_day']['opened_at']==clock.now.isoformat()
 clock.now=D(DAY+'T10:01:00+08:00')
 assert run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=cal,client=api,authorization=a)['status']=='READY'
 clock.now=D(DAY+'T10:02:00+08:00')
 r=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=cal,client=api,authorization=a)
 assert r['model_validated']is True and r['deadline_audit']['window_authorization']==a and calls==['A6','A3']
 assert r['natural_batch_switch']['scope_id']==staged['scope_id']
 assert store.get_execution_plan('p1')['status']=='PENDING_MORNING_REVIEW'
 with store._connect()as db:bundle=json.loads(db.execute('SELECT body_json FROM reviewed_batch_bundles WHERE scope_id=?',(staged['scope_id'],)).fetchone()[0])
 assert bundle['morning_window_authorization']==a
 bad=deepcopy(bundle);bad.pop('morning_window_authorization')
 with pytest.raises(StateTransitionError,match='AUTHORITY'):validate_bundle(bad)
 assert run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=cal,client=api,authorization=a)['natural_batch_switch']['idempotent']
 assert calls==['A6','A3']
