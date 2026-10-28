from datetime import datetime
from test_d8_contract_0881 import make_publisher,text


def test_unknown_customer_label_keeps_notice_safe_and_queues_internal_diagnostic(tmp_path):
    publisher,customer,internal=make_publisher(tmp_path)
    result=publisher._send(delivery_key='future-label',kind='PREMARKET_BATCH_READY',source_id='NEW_UNMAPPED_ENGINE_STATE',
        title='盘前状态',lines=['状态：原码：NEW_UNMAPPED_ENGINE_STATE；接下来等待自然激活。'],
        summary={'state':'READY'},now=datetime.fromisoformat('2026-10-28T08:40:00+08:00'))
    assert result['status']=='SENT' and len(customer.cards)==1
    body=text(customer.cards[0])
    assert '系统内部状态，详见内部通道' in body
    assert 'NEW_UNMAPPED_ENGINE_STATE' not in str(customer.cards[0]) and '原码' not in body
    assert any('NEW_UNMAPPED_ENGINE_STATE' in str(row) for row in internal)
    assert '单股当前价' not in body and '不适用' not in body


def test_system_card_does_not_invent_stock_fields_and_has_one_action(tmp_path):
    publisher,customer,internal=make_publisher(tmp_path)
    publisher._send(delivery_key='system-status',kind='PREMARKET_BATCH_READY',source_id='a'*64,
        title='盘前状态',lines=['计划7只已完成自然切换。'],summary={'state':'READY'},
        now=datetime.fromisoformat('2026-10-28T08:40:00+08:00'))
    body=text(customer.cards[0])
    assert '不适用' not in body and '单股' not in body
    assert body.count('接下来')==1 and '此通知不构成放行' not in body


def test_native_customer_unknown_uses_safe_body_internal_keeps_diagnostic():
    from liangjian_funnel.runtime.notification_presentation import native_card
    kwargs=dict(kind='A4_REVIEW_ENGINE_ONLY',body={'content':{'text':'NEW_UNMAPPED_ENGINE_STATE'}},
        observed_at='2026-10-28T14:06:00+08:00',event_id='a'*64)
    customer=native_card(**kwargs,internal=False)
    internal=native_card(**kwargs)
    assert 'NEW_UNMAPPED_ENGINE_STATE' not in str(customer) and '原码' not in str(customer)
    assert '不适用' not in text(customer) and '单股当前价' not in text(customer)
    assert '说明映射告警' in text(internal) and 'NEW_UNMAPPED_ENGINE_STATE' in str(internal)
