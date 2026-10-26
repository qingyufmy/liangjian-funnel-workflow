from dataclasses import replace

import pytest

from test_team_v2_execution import fixture
from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal


def test_preview_reuses_real_guard_without_admission_or_order(tmp_path):
    broker,args=fixture(tmp_path)
    before=broker.snapshot_bytes()
    result=admit_trader_signal(**(args|dict(mode='record',preview_only=True)))
    assert result['judgment']['shares']==20000
    assert result['dispatched'] is False and result['order'] is None
    assert result['model_authority'] is False and result['fill_status']=='UNKNOWN'
    assert broker.snapshot_bytes()==before
    assert broker.get_admissions('2026-10-19')==()
    assert broker.get_orders('2026-10-19')==()


@pytest.mark.parametrize('change,reason',[
    ({'pm_allocation':None},'PM_ALLOCATION_UNAVAILABLE'),
    ({'live_market_ready':False},'LIVE_MARKET_NOT_READY'),
    ({'envelope':None},'DATA'),
])
def test_preview_preserves_plan_data_market_pm_guards(tmp_path,change,reason):
    broker,args=fixture(tmp_path)
    if change.get('envelope','other') is None:
        changed=replace(args['plan'],data_ready=False)
        change={'plan':changed,'all_plans':{'p1':changed}}
    before=broker.snapshot_bytes()
    result=admit_trader_signal(**(args|change|dict(mode='record',preview_only=True)))
    assert result['judgment']['status']=='REJECTED'
    assert any(reason in item for item in result['judgment']['reason_codes'])
    assert broker.snapshot_bytes()==before


def test_preview_cannot_be_used_in_control_mode(tmp_path):
    _,args=fixture(tmp_path)
    with pytest.raises(ValueError,match='SHADOW_REQUIRES_RECORD_MODE'):
        admit_trader_signal(**(args|dict(preview_only=True)))


@pytest.mark.parametrize('action,status',[('BUY_SIGNAL','DETERMINISTIC_ENTRY_ELIGIBLE_BEFORE_MODEL'),
    ('DATA_BLOCK','DATA_BLOCK'),('NO_ACTION','NO_TECHNICAL_ENTRY')])
def test_saved_shadow_keeps_missing_evidence_distinct_from_no_action(tmp_path,action,status):
    import time
    from types import SimpleNamespace
    from liangjian_funnel.runtime.team_v2_record_shadow import build_shadow
    broker,args=fixture(tmp_path);saved={}
    def freeze(key,*,source_sha256,body):
        saved.update(key=key,source=source_sha256,body=body);return body
    store=SimpleNamespace(pm_meeting_id='pm-test',load_pm_allocation=lambda *a,**k:args['pm_allocation'],
        freeze_control_context=freeze)
    before=broker.snapshot_bytes()
    result=build_shadow(store=store,broker=broker,plans=args['all_plans'],
        evaluations={'p1':dict(action=action,reason_codes=['DAILY20_MISSING'] if action=='DATA_BLOCK' else [])},
        envelopes={'p1':args['envelope']},active={'p1'},limits=args['limits'],clock=lambda:args['now'],
        cutoff=args['closed_5m_end'],regime=args['regime'],live_market_ready=True,calendar=args['calendar'],
        packet=dict(packet_sha256='a'*64,captured_at=args['now'].isoformat()),utilization_cap=100,
        pattern_bindings={},deadline=time.monotonic()+10,decision='isolated')
    assert result['rows'][0]['status']==status
    assert result['rows'][0]['model_status']=='NOT_RUN_RECORD'
    assert result['rows'][0]['fill_status']=='UNKNOWN' and result['order_authority'] is False
    assert saved['key']=='deterministic-shadow:isolated'
    assert broker.snapshot_bytes()==before
