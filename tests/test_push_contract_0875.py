from liangjian_funnel.runtime.lark_notifications import _condition_summary, _time_label


def test_original_utc_is_converted_and_naive_is_not_invented():
    assert "13:25 北京时间" in _time_label("2026-10-27T05:25:00+00:00")
    assert "原件未标时区" in _time_label("2026-10-27T05:25:00")


def test_all_required_conditions_survive_more_than_four():
    conditions = ["B_KEY_LEVEL", "A_STABILIZED", "A3_PULLBACK_ZONE", "A3_RIGHT_SIDE_CONFIRMATION", "A4_LIVE_ENTRY_GEOMETRY_ACCEPTED"]
    text = _condition_summary(conditions)
    assert "共5项" in text and "另" not in text
    assert "本轮入场几何已接受" in text and "原码" not in text


from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from liangjian_funnel.runtime.notification_policy import NotificationPolicy, routes
from liangjian_funnel.runtime.notification_presentation import native_card
import pytest

T=datetime(2026,10,27,10,0,tzinfo=ZoneInfo('Asia/Shanghai'))


def observe(p,key,kind='A4_JUDGE_TIMEOUT',state='WAIT',symbol='000858',minute=0):
    return p.observe(event_key=key,kind=kind,symbol=symbol,state=state,now=T+timedelta(minutes=minute),payload={'source':'original'})


def test_only_same_stock_kind_and_state_merge_and_real_count_and_boundaries(tmp_path):
    p=NotificationPolicy(tmp_path/'n.db')
    assert observe(p,'1')['disposition']=='IMMEDIATE'
    assert observe(p,'2',minute=1)['disposition']=='MERGED'
    assert observe(p,'2',minute=1)['disposition']=='DUPLICATE'
    assert p.claim_summaries(now=T+timedelta(minutes=14))==[]
    rows=p.claim_summaries(now=T+timedelta(minutes=15))
    assert len(rows)==1 and rows[0]['count']==2
    assert rows[0]['first_at']==T.isoformat() and rows[0]['last_at']==(T+timedelta(minutes=1)).isoformat()
    assert p.claim_summaries(now=T+timedelta(minutes=16))==[]


def test_singleton_no_second_post_and_state_changes_always_immediate(tmp_path):
    p=NotificationPolicy(tmp_path/'n.db')
    assert observe(p,'1',state='WAIT')['disposition']=='IMMEDIATE'
    assert observe(p,'2',state='READY',minute=1)['disposition']=='IMMEDIATE'
    assert observe(p,'3',state='WAIT',minute=2)['disposition']=='IMMEDIATE'
    assert p.claim_summaries(now=T+timedelta(minutes=30))==[]


@pytest.mark.parametrize('kind',['A4_EFFECTIVE','A4_EXECUTION_RESULT','A4_STOP_LOSS','A4_POSITION_CHANGED',
    'AUDITED_RESEARCH_FAILED','A4_REVIEW_ENGINE_ONLY','A4_REVIEW_RESTORED','A4_REVIEW_ORIGINAL_BUY_REJECTED'])
def test_never_suppress_trade_position_or_whitelisted_events(tmp_path,kind):
    p=NotificationPolicy(tmp_path/'n.db')
    assert all(observe(p,str(i),kind=kind,minute=i)['disposition']=='IMMEDIATE' for i in range(7))
    assert p.claim_summaries(now=T+timedelta(hours=8),close_day=True)==[]


def test_fourth_system_event_held_for_internal_postclose_summary(tmp_path):
    p=NotificationPolicy(tmp_path/'n.db')
    for i in range(3):
        assert observe(p,str(i),symbol=str(i),minute=i)['disposition']=='IMMEDIATE'
    for i in range(3,6):
        assert observe(p,str(i),symbol=str(i),minute=i)['disposition']=='MERGED'
    assert p.claim_summaries(now=T+timedelta(hours=6))==[]
    rows=p.claim_summaries(now=T+timedelta(hours=8),close_day=True)
    assert len(rows)==1 and rows[0]['mode']=='DAY' and rows[0]['count']==3 and rows[0]['routes']==('INTERNAL',)


def test_routes_no_ops_fallback_to_customer_and_status_dual():
    assert routes('A4_JUDGE_TIMEOUT')==('INTERNAL',)
    assert routes('A4_REVIEW_TRUNCATED')==('INTERNAL',)
    assert routes('NEW_UNKNOWN_OPS_EVENT')==('INTERNAL',)
    assert routes('A4_EFFECTIVE')==('CUSTOMER',)
    assert routes('A4_REVIEW_ENGINE_ONLY')==('CUSTOMER','INTERNAL')
    assert routes('PREMARKET_BATCH_READY')==('CUSTOMER','INTERNAL')


def test_summary_consumers_do_not_claim_each_others_groups(tmp_path):
    p=NotificationPolicy(tmp_path/'n.db')
    for key in ('1','2'):
        p.observe(event_key=key,kind='A4_JUDGE_TIMEOUT',symbol='x',state='WAIT',now=T,payload={'body':{'content':{'text':'a'}}})
    assert p.claim_summaries(now=T+timedelta(minutes=16),owner='workflow')==[]
    assert len(p.claim_summaries(now=T+timedelta(minutes=16),owner='native'))==1


def test_native_card_keeps_code_only_in_note_and_shows_bjt():
    card=native_card(kind='AUDITED_RESEARCH_FAILED',body={'content':{'text':'MODEL_REQUEST_FAILED at 2026-10-27T05:00:00+00:00'}},observed_at=T,event_id='id')['card']
    text=''.join(e.get('text',{}).get('content','') for e in card['elements'])
    assert 'MODEL_REQUEST_FAILED' not in text and '13:00:00 北京时间' in text and '不适用' in text
    assert card['elements'][-1]['tag']=='note'
