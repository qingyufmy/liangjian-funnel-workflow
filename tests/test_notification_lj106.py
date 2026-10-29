"""Known business reasons and actual acknowledgement remain independent."""
import json
from pathlib import Path
import pytest
from liangjian_funnel.runtime.notification_presentation import native_card,delivery_evidence

@pytest.mark.parametrize('code', ['QUOTE_BACKPRESSURE','REALTIME_QUOTE_BACKPRESSURE',
    'DEADLINE_EXCEEDED','REALTIME_QUOTE_DEADLINE_EXCEEDED','MINUTE_PROVIDER_TIMEOUT',
    'SHARED_MARKET_UNAVAILABLE','A5_MODEL_CONTEXT_TOO_LARGE','A4_LIVE_SYSTEMIC_SELL_OFF'])
def test_exact_production_codes_are_explained_with_original_trace(code):
    d={};card=native_card(kind='ALERT',body={'content':{'text':code+' 影响24只 session:s1'}},
        observed_at='2026-10-29T11:16:00+08:00',event_id='event1',diagnostics=d)
    assert d['unmapped_code_counts']=={}
    assert d['business_reason_class']=='KNOWN_REASON_PRESENTATION'
    assert code in json.dumps(card,ensure_ascii=False)
    if code in {'QUOTE_BACKPRESSURE','REALTIME_QUOTE_BACKPRESSURE','DEADLINE_EXCEEDED',
        'REALTIME_QUOTE_DEADLINE_EXCEEDED','MINUTE_PROVIDER_TIMEOUT','SHARED_MARKET_UNAVAILABLE'}:
        text=json.dumps(card,ensure_ascii=False)
        assert '24只股票' in text and '会话：s1' in text

def test_actual_day_originals_have_no_unmapped_codes_and_are_immutable():
    p=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021/Q3-result-table-1730-supplement-alerts-cards-actual-20261029.json')
    before=p.read_bytes();r=json.loads(before)
    for a in r['alerts']:
        d={};native_card(kind=a['kind'],body=a['original_body'],observed_at=a['observed_at'],event_id=a['event_id'],diagnostics=d)
        assert d['unmapped_code_counts']=={},a['event_id']
    assert p.read_bytes()==before
    assert sum(a['delivery_verdict']=='DELIVERY_UNCONFIRMED' for a in r['alerts'])==2

@pytest.mark.parametrize('receipt,expected', [
    ({'http_status':200,'provider_code':0,'message_class':'UNCLASSIFIED'},'DELIVERY_CONFIRMED'),
    ({'http_status':None,'provider_code':None},'DELIVERY_UNCONFIRMED'),
    ({'http_status':200,'provider_code':None},'DELIVERY_UNCONFIRMED'),
    ({'http_status':200,'provider_code':True},'DELIVERY_REJECTED_OR_HTTP_ERROR'),
    ({'http_status':200,'provider_code':1},'DELIVERY_REJECTED_OR_HTTP_ERROR')])
def test_delivery_only_uses_recorded_http_and_provider_code(receipt,expected):
    original=dict(receipt);assert delivery_evidence(receipt)==expected;assert receipt==original

def test_unknown_future_code_stays_unknown_and_missing_scope_not_invented():
    d={};card=native_card(kind='ALERT',body={'content':{'text':'QUOTE_BACKPRESSURE FUTURE_REASON_123'}},
        observed_at='2026-10-29T11:16:00+08:00',event_id='event1',diagnostics=d)
    assert d['unmapped_code_counts']=={'FUTURE_REASON_123':1}
    assert '股票数待原件证明' in json.dumps(card,ensure_ascii=False)

def test_premarket_condition_counts_use_explicit_frozen_facts_not_live_inference():
    from liangjian_funnel.runtime.lark_notifications import _premarket_condition_counts
    assert '满足2 / 未满足1' in _premarket_condition_counts({'deterministic_met_conditions':['a','b'],
        'deterministic_unmet_conditions':['c']})
    assert '不代表盘中确认或成交' in _premarket_condition_counts({'met_conditions':[],'unmet_conditions':[]})
    assert '待原件证明' in _premarket_condition_counts({'required_conditions':['a']})
    assert '状态冲突' in _premarket_condition_counts({'met_conditions':['a'],'unmet_conditions':['a']})

def test_today_original_plan_condition_counts_available_without_rewriting():
    from liangjian_funnel.runtime.lark_notifications import _premarket_condition_counts
    p=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021/C2_TRACK2_1800_ORIGINALS_RO_20261029.json')
    raw=p.read_bytes();facts=json.loads(raw)['result']['A5_original_contexts'][0]['body']['facts']
    plans=facts['plan_causal_chains']['by_plan'];assert len(plans)==25
    for row in plans.values():
        line=_premarket_condition_counts(row['original_frozen_plan'])
        assert '满足' in line and '未满足' in line and '不代表盘中确认或成交' in line
    assert p.read_bytes()==raw
