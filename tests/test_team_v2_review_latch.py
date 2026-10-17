from contextlib import closing
from datetime import date,timedelta
import json
import pytest

from liangjian_funnel.runtime.team_v2_review_latch import ReviewDayLatch
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.runtime import team_v2_candidate_review as review
from test_team_v2_candidate_review import prepared


def test_three_actual_failures_latch_across_restart_once_and_next_day_resets(tmp_path):
    args,_,_=prepared(tmp_path);store=args['store'];at=args['decision_at']
    latch=ReviewDayLatch(store)
    def record(i,valid):
        return latch.record(account_id='paper',trade_date=args['trade_date'],decision_id=str(i),valid=valid,observed_at=at)
    assert record(1,False)['consecutive_failures']==1
    assert record(1,False)['consecutive_failures']==1
    assert record(2,True)['consecutive_failures']==0
    assert not record(3,False)['engine_only']
    assert not record(4,False)['engine_only']
    state=record(5,False)
    assert state['engine_only'] and state['consecutive_failures']==3
    latch=ReviewDayLatch(TeamMeetingStore(store.path))
    assert latch.read('paper',args['trade_date'])==state
    assert not latch.read('other',args['trade_date'])['engine_only']
    assert not latch.read('paper',(date.fromisoformat(args['trade_date'])+timedelta(days=1)).isoformat())['engine_only']
    assert latch.ensure_alert('paper',args['trade_date'],state)==latch.ensure_alert('paper',args['trade_date'],state)
    with closing(store._connect()) as db:
        rows=db.execute('SELECT kind,body_json FROM team_notifications').fetchall()
    assert len(rows)==1 and rows[0][0]=='A4_REVIEW_ENGINE_ONLY'
    assert 'A4_REVIEW_ENGINE_ONLY' in json.loads(rows[0][1])['content']['text']


def test_runtime_third_failure_blocks_that_review_next_tick_uses_same_admission(tmp_path,monkeypatch):
    args,_,_=prepared(tmp_path);calls=[];admissions=[]
    def capture(**child):
        calls.append(child)
        child['journal'].claim(child['call_id'],'test-binding')
        child['journal'].mark_failed(child['call_id'],'TEAM_MODEL_RESULT_INVALID')
        return dict(model_validated=False,reason_codes=['TEAM_MODEL_RESULT_INVALID'])
    monkeypatch.setattr(review,'run_trader_session',capture)
    monkeypatch.setattr(review,'admit_trader_signal',lambda **kw:admissions.append(kw) or dict(dispatched=False))
    for i in range(4):
        args.update(decision_id='d'+str(i),call_id='A4:d'+str(i))
        result=review.run_candidate_review(**args)
        if i<3:assert not result['admissions']
    assert len(calls)==3 and len(admissions)==1
    assert result['status']=='ENGINE_ONLY_DAILY_REVIEW_FALLBACK'
    assert result['candidate_review']['actual_model_objects']==0
    assert result['admissions'][0]['origin']=='ENGINE_ONLY_DAILY_REVIEW_FALLBACK'
    assert admissions[0]['envelope']==args['envelopes']['p1']
    assert admissions[0]['pm_allocation'] is not None and admissions[0]['limits']==args['limits']
    assert review.run_candidate_review(**args)==result and len(admissions)==1


def test_no_candidate_and_no_actual_attempt_do_not_advance_failure_count(tmp_path,monkeypatch):
    args,_,_=prepared(tmp_path)
    monkeypatch.setattr(review,'run_trader_session',lambda **kw:dict(model_validated=False,reason_codes=['LOCAL_FAILURE']))
    for i in range(5):
        args.update(decision_id='local'+str(i),call_id='A4:local'+str(i))
        result=review.run_candidate_review(**args)
        assert result['review_day_state']['consecutive_failures']==0
    args.update(decision_id='empty',call_id='A4:empty')
    args['input_projection']['evaluations']['p1']['action']='NO_ACTION'
    assert review.run_candidate_review(**args)['review_day_state']['consecutive_failures']==0


def test_engine_only_still_cannot_buy_without_pm_allocation(tmp_path,monkeypatch):
    args,seen,_=prepared(tmp_path);latch=ReviewDayLatch(args['store'])
    for i in range(3):
        latch.record(account_id=args['account_id'],trade_date=args['trade_date'],decision_id='failed'+str(i),
            valid=False,observed_at=args['decision_at'])
    args['pm_meeting_id']=None
    monkeypatch.setattr(review,'run_trader_session',lambda **kw:pytest.fail('model must remain disabled'))
    result=review.run_candidate_review(**args)
    assert not seen and len(result['admissions'])==1
    assert not result['admissions'][0]['dispatched'] and not args['broker'].get_orders(args['trade_date'])


def test_a5_flags_two_consecutive_trading_days_but_not_two_arbitrary_days():
    from liangjian_funnel.runtime.team_v2_review_facts import review_degradation_metrics
    def row(day):return dict(trade_date=day,body_json=json.dumps(dict(engine_only=True,consecutive_failures=3)))
    assert review_degradation_metrics([row('2026-10-16'),row('2026-10-19')],'2026-10-19')['report_to_claude']
    assert not review_degradation_metrics([row('2026-10-15'),row('2026-10-19')],'2026-10-19')['report_to_claude']


def test_crash_after_third_failure_cannot_reclassify_that_entry_on_restart(tmp_path,monkeypatch):
    args,_,_=prepared(tmp_path);latch=ReviewDayLatch(args['store'])
    for i in range(2):
        latch.record(account_id=args['account_id'],trade_date=args['trade_date'],decision_id='prior'+str(i),
            valid=False,observed_at=args['decision_at'])
    def capture(**child):
        if not child['journal'].has_call_claim(child['call_id']):
            child['journal'].claim(child['call_id'],'test-binding')
            child['journal'].mark_failed(child['call_id'],'TEAM_MODEL_RESULT_INVALID')
        return dict(model_validated=False,reason_codes=['TEAM_MODEL_RESULT_INVALID'])
    monkeypatch.setattr(review,'run_trader_session',capture)
    original=args['store'].record_trader
    monkeypatch.setattr(args['store'],'record_trader',lambda **kw:(_ for _ in ()).throw(RuntimeError('crash')))
    with pytest.raises(RuntimeError,match='crash'):review.run_candidate_review(**args)
    assert latch.read(args['account_id'],args['trade_date'])['engine_only']
    monkeypatch.setattr(args['store'],'record_trader',original)
    result=review.run_candidate_review(**args)
    assert result['status']=='CANDIDATE_REVIEW_FAILED_ENTRIES_BLOCKED' and not result['admissions']
    assert not args['broker'].get_orders(args['trade_date'])


def test_research_capture_buy_never_calls_admission_even_with_engine_envelope(tmp_path,monkeypatch):
    from liangjian_funnel.runtime import team_v2_trader_session as session
    args,seen,_=prepared(tmp_path)
    args.update(capture_only=True,review_candidate_actions={'p1':'BUY_SIGNAL'},review_position_actions={})
    monkeypatch.setattr(session,'admit_trader_signal',lambda **kw:pytest.fail('capture cannot enter admission'))
    result=session.run_trader_session(**args)
    assert result['model_validated'] and len(seen)==1 and not result['admissions']
    assert not args['broker'].get_orders(args['trade_date'])
