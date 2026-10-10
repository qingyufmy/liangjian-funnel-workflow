from datetime import datetime

import pytest

from liangjian_funnel.runtime.shadow_close_schedule import ShadowCloseCoordinator


DAY = '2026-10-12'
def at(clock): return datetime.fromisoformat(DAY+'T'+clock+'+08:00')


def harness():
    calls=[]
    def freeze(**kwargs):
        calls.append(('freeze',kwargs))
        return {'source_ref':'fixture:draft','sha256':'a'*64,'trade_date':DAY,
            'market_cutoff':at('15:00:00').isoformat()}
    def final(**kwargs):
        calls.append(('final',kwargs))
        return {'source_ref':'fixture:report','sha256':'b'*64}
    return ShadowCloseCoordinator(DAY,freeze=freeze,build_final=final),calls


def observation(clock='16:08:00',status='SUCCEEDED'):
    return {'trade_date':DAY,'slot':'A5_POST_CLOSE_1600','status':status,
        'completed_at':at(clock).isoformat(),'source_ref':'fixture:a5','source_sha256':'c'*64}


def test_draft_at_1530_is_frozen_once_not_formal_report():
    engine,calls=harness()
    assert engine.poll(at('15:29:59'))['status']=='WAIT_DRAFT'
    assert engine.poll(at('15:30:00'))['status']=='WAIT_A5'
    assert engine.poll(at('15:31:00'))['status']=='WAIT_A5'
    assert [name for name,_ in calls]==['freeze']


def test_successful_a5_allows_formal_report_once_with_real_clock():
    engine,calls=harness();engine.poll(at('15:30:00'))
    result=engine.poll(at('16:08:01'),a5=observation())
    assert result['status']=='FORMAL_WRITTEN'
    assert calls[-1][1]['a5_status']=='COMPLETE'
    assert calls[-1][1]['as_of']==at('16:08:01')
    assert engine.poll(at('16:10:00'),a5=observation())==result
    assert [name for name,_ in calls]==['freeze','final']


@pytest.mark.parametrize('a5',[None,observation(status='FAILED')])
def test_hard_1645_deadline_does_not_wait_or_change_a5(a5):
    engine,calls=harness();engine.poll(at('15:30:00'))
    assert engine.poll(at('16:44:59'),a5=a5)['status']=='WAIT_A5'
    result=engine.poll(at('16:45:00'),a5=a5)
    assert result['status']=='FORMAL_WRITTEN'
    assert calls[-1][1]['a5_status']=='A5_NOT_COMPLETE'
    assert calls[-1][1]['draft']['sha256']=='a'*64


@pytest.mark.parametrize('change',[
    {'trade_date':'2026-10-09'}, {'slot':'A5_MIDDAY_1135'},
    {'completed_at':at('16:10:00').isoformat()}, {'source_sha256':None},
    {'source_ref':''},
])
def test_unbound_or_future_a5_is_not_formal_completion(change):
    engine,calls=harness();engine.poll(at('15:30:00'))
    value={**observation(),**change}
    assert engine.poll(at('16:08:01'),a5=value)['status']=='WAIT_A5'
    assert len(calls)==1


def test_no_draft_before_cutoff_or_cross_day_backfill():
    engine,calls=harness()
    with pytest.raises(ValueError,match='SHADOW_CLOSE_DAY_MISMATCH'):
        engine.poll(datetime.fromisoformat('2026-10-13T15:30:00+08:00'))
    assert calls==[]


def test_clock_regression_rejected():
    engine,_=harness();engine.poll(at('15:30:00'))
    with pytest.raises(ValueError,match='SHADOW_CLOSE_CLOCK_REGRESSED'):
        engine.poll(at('15:29:59'))


def test_first_poll_after_draft_deadline_does_not_fake_1530_freeze():
    engine,calls=harness();engine.poll(at('16:45:00'))
    assert calls[0][1]['as_of']==at('16:45:00')
    assert calls[0][1]['market_cutoff']==at('15:00:00')
    assert calls[-1][1]['draft_status']=='LATE_DRAFT'


def test_failed_report_does_not_retry_or_call_production():
    count=[]
    def fail(**kwargs):count.append(kwargs);raise OSError('fixture write failed')
    engine=ShadowCloseCoordinator(DAY,freeze=lambda **kwargs:{'source_ref':'fixture:draft',
        'sha256':'a'*64,'trade_date':DAY,'market_cutoff':at('15:00:00').isoformat()},build_final=fail)
    assert engine.poll(at('16:45:00'))['status']=='SHADOW_REPORT_FAILED'
    assert engine.poll(at('16:45:01'))['status']=='SHADOW_REPORT_FAILED'
    assert len(count)==1


def test_late_poll_cannot_reconstruct_a5_availability_before_deadline():
    engine,calls=harness();engine.poll(at('15:30:00'))
    result=engine.poll(at('16:46:00'),a5=observation())
    assert result['a5_status']=='A5_NOT_COMPLETE'
    assert result['deadline_missed'] is True
    assert calls[-1][1]['a5'] is None
